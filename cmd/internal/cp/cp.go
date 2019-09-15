package cp

import (
	"context"
	"fmt"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3iface"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager/s3manageriface"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/shogo82148/s3cli-mini/internal/fastwalk"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

// maximum object size in a single atomic operation. (5GiB)
// https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjectsExamples.html
// It is a variable, because of tests.
var maxCopyObjectBytes = int64(5 * 1024 * 1024 * 1024)

// chunk size for multipart copying objects
const copyChunkBytes = 5 * 1024 * 1024

var dryrun bool
var parallel int
var includes []string
var excludes []string
var acl string
var recursive bool
var followSymlinks = true
var noFollowSymlinks bool
var noGuessMimeType bool
var contentType string
var cacheControl string
var contentDisposition string
var contentEncoding string
var contentLanguage string
var expires string

// Init initializes flags.
func Init(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.BoolVar(&dryrun, "dryrun", false, "Displays the operations that would be performed using the specified command without actually running them.")
	flags.StringArrayVar(&includes, "include", []string{}, "Don't exclude files or objects in the command that match the specified pattern. See Use of Exclude and Include Filters for details.")
	flags.StringArrayVar(&excludes, "exclude", []string{}, "Exclude all files or objects from the command that matches the specified pattern.")
	flags.StringVar(&acl, "acl", "", "Sets the ACL for the object when the command is performed.")
	flags.BoolVar(&recursive, "recursive", false, "Command is performed on all files or objects under the specified directory or prefix.")
	flags.BoolVar(&followSymlinks, "follow-symlinks", true, "Symbolic links are followed only when uploading to S3 from the local filesystem.")
	flags.BoolVar(&noFollowSymlinks, "no-follow-symlinks", false, "")
	flags.BoolVar(&noGuessMimeType, "no-guess-mime-type", false, "Do not try to guess the mime type for uploaded files. By default the mime type of a file is guessed when it is uploaded.")
	flags.StringVar(&contentType, "content-type", "", "Specify an explicit content type for this operation. This value overrides any guessed mime types.")
	flags.StringVar(&cacheControl, "cache-control", "", "Specifies caching behavior along the request/reply chain.")
	flags.StringVar(&cacheControl, "content-disposition", "", "Specifies presentational information for the object.")
	flags.StringVar(&contentEncoding, "content-encoding", "", "Specifies what content encodings have been applied to the object and thus what decoding mechanisms must be applied to obtain the media-type referenced by the Content-Type header field.")
	flags.StringVar(&contentLanguage, "content-language", "", "The language the content is in.")
	flags.StringVar(&expires, "expires", "", "The date and time at which the object is no longer cacheable.")
}

type client struct {
	ctx            context.Context
	cancel         context.CancelFunc
	cmd            *cobra.Command
	s3             s3iface.ClientAPI
	uploader       s3manageriface.UploaderAPI
	downloader     s3manageriface.DownloaderAPI
	followSymlinks bool
	acl            s3.ObjectCannedACL
	expires        *time.Time
}

// Run runs cp command.
func Run(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(args) != 2 {
		cmd.Usage()
		return
	}

	svc, err := config.NewS3Client()
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}

	c := client{
		ctx:        ctx,
		cancel:     cancel,
		cmd:        cmd,
		s3:         svc,
		uploader:   s3manager.NewUploaderWithClient(svc),
		downloader: s3manager.NewDownloaderWithClient(svc),
	}
	c.followSymlinks = followSymlinks && !noFollowSymlinks
	c.acl, err = parseACL(acl)
	if err != nil {
		c.cmd.PrintErrln("Validation error: ", err)
		os.Exit(1)
	}
	if expires != "" {
		t, err := time.Parse(time.RFC3339, expires)
		if err != nil {
			c.cmd.PrintErrln("Validation error: ", err)
			os.Exit(1)
		}
		c.expires = &t
	}
	c.Run(args[0], args[1])
}

func parseACL(acl string) (s3.ObjectCannedACL, error) {
	switch acl {
	case "":
		return "", nil
	case "private":
		return s3.ObjectCannedACLPrivate, nil
	case "public-read":
		return s3.ObjectCannedACLPublicRead, nil
	case "public-read-write":
		return s3.ObjectCannedACLPublicReadWrite, nil
	case "authenticated-read":
		return s3.ObjectCannedACLAuthenticatedRead, nil
	case "aws-exec-read":
		return s3.ObjectCannedACLAwsExecRead, nil
	case "bucket-owner-read":
		return s3.ObjectCannedACLBucketOwnerRead, nil
	case "bucket-owner-full-control":
		return s3.ObjectCannedACLBucketOwnerFullControl, nil
	}
	return "", fmt.Errorf("unknown acl: %s", acl)
}

func (c *client) Run(src, dist string) {
	if parallel <= 0 {
		parallel = 4
	}

	s3src := strings.HasPrefix(src, "s3://")
	src = strings.TrimPrefix(src, "s3://")
	s3dist := strings.HasPrefix(dist, "s3://")
	dist = strings.TrimPrefix(dist, "s3://")

	if recursive {
		switch {
		case s3src && s3dist:
			if err := c.s3s3recursive(src, dist); err != nil {
				c.cmd.PrintErrln("Copy error: ", err)
				os.Exit(1)
			}
			return
		case !s3src && s3dist:
			if err := c.locals3recursive(src, dist); err != nil {
				c.cmd.PrintErrln("Upload error: ", err)
				os.Exit(1)
			}
			return
		case s3src && !s3dist:
			if err := c.s3localrecursive(src, dist); err != nil {
				c.cmd.PrintErrln("Download error: ", err)
				os.Exit(1)
			}
			return
		}
	} else {
		switch {
		case s3src && s3dist:
			if err := c.s3s3(src, dist); err != nil {
				c.cmd.PrintErrln("Copy error: ", err)
				os.Exit(1)
			}
			return
		case !s3src && s3dist:
			if err := c.locals3(src, dist); err != nil {
				c.cmd.PrintErrln("Upload error: ", err)
				os.Exit(1)
			}
			return
		case s3src && !s3dist:
			if err := c.s3local(src, dist); err != nil {
				c.cmd.PrintErrln("Download error: ", err)
				os.Exit(1)
			}
			return
		}
	}

	c.cmd.PrintErrln("Error: Invalid argument type")
	os.Exit(1)
}

func (c *client) locals3(src, dist string) error {
	bucket, key := parsePath(dist)
	if key == "" || key[len(key)-1] == '/' {
		key += filepath.Base(src)
	}
	if dryrun {
		c.cmd.PrintErrf("upload %s to s3://%s/%s\n", src, bucket, key)
		return nil
	}

	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	input := &s3manager.UploadInput{
		Body:               f,
		Bucket:             aws.String(bucket),
		Key:                aws.String(key),
		ACL:                c.acl,
		ContentType:        getContentType(key),
		CacheControl:       nullableString(cacheControl),
		ContentDisposition: nullableString(contentDisposition),
		ContentEncoding:    nullableString(contentEncoding),
		ContentLanguage:    nullableString(contentLanguage),
		Expires:            c.expires,
	}
	_, err = c.uploader.UploadWithContext(c.ctx, input)
	if err != nil {
		return err
	}
	c.cmd.PrintErrf("upload %s to s3://%s/%s\n", src, bucket, key)
	return nil
}

func (c *client) locals3recursive(src, dist string) error {
	type result struct {
		message string
		err     error
	}
	var wg sync.WaitGroup
	chSource := make(chan string, parallel)
	chResult := make(chan result, parallel)

	// walk local file system
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(chSource)
		if err := fastwalk.Walk(src, func(path string, typ os.FileMode) error {
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			if info.IsDir() {
				if typ == os.ModeSymlink && c.followSymlinks {
					return fastwalk.TraverseLink
				}
				return nil
			}
			select {
			case chSource <- path:
			case <-c.ctx.Done():
				return c.ctx.Err()
			}
			return nil
		}); err != nil {
			select {
			case chResult <- result{err: err}:
			case <-c.ctx.Done():
			}
		}
	}()

	// upload workers
	upload := func(p string) (string, error) {
		bucket, key := parsePath(dist)
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return "", err
		}
		key = path.Join(key, filepath.ToSlash(rel))
		if dryrun {
			return fmt.Sprintf("upload %s to s3://%s/%s", p, bucket, key), nil
		}
		f, err := os.Open(p)
		if err != nil {
			return "", err
		}
		defer f.Close()

		input := &s3manager.UploadInput{
			Body:               f,
			Bucket:             aws.String(bucket),
			Key:                aws.String(key),
			ACL:                c.acl,
			ContentType:        getContentType(key),
			CacheControl:       nullableString(cacheControl),
			ContentDisposition: nullableString(contentDisposition),
			ContentEncoding:    nullableString(contentEncoding),
			ContentLanguage:    nullableString(contentLanguage),
			Expires:            c.expires,
		}
		_, err = c.uploader.UploadWithContext(c.ctx, input)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("upload %s to s3://%s/%s", p, bucket, key), nil
	}
	wg.Add(parallel)
	for i := 0; i < parallel; i++ {
		go func() {
			defer wg.Done()
			for {
				var path string
				var ok bool
				select {
				case path, ok = <-chSource:
					if !ok {
						return
					}
				case <-c.ctx.Done():
					return
				}
				msg, err := upload(path)
				if err != nil {
					select {
					case chResult <- result{err: err}:
					case <-c.ctx.Done():
						return
					}
				}
				select {
				case chResult <- result{message: msg}:
				case <-c.ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(chResult)
	}()

	var err error
	for ret := range chResult {
		if ret.err != nil {
			c.cmd.PrintErrln("Upload error: ", ret.err)
			err = ret.err
			c.cancel()
		} else {
			c.cmd.PrintErrln(ret.message)
		}
	}
	return err
}

func (c *client) s3local(src, dist string) error {
	bucket, key := parsePath(src)
	if key == "" || key[len(key)-1] == '/' {
		c.cmd.PrintErrln("Error: Invalid argument type")
		os.Exit(1)
	}
	if info, err := os.Stat(dist); err == nil && info.IsDir() {
		dist = filepath.Join(dist, path.Base(key))
	}
	if dryrun {
		c.cmd.PrintErrf("download s3://%s/%s to %s\n", bucket, key, dist)
		return nil
	}

	f, err := os.OpenFile(dist, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, err = c.downloader.DownloadWithContext(c.ctx, f, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	c.cmd.PrintErrf("download s3://%s/%s to %s\n", bucket, key, dist)
	return nil
}

func (c *client) s3localrecursive(src, dist string) error {
	bucket, key := parsePath(src)
	if key != "" && key[len(key)-1] != '/' {
		key += "/"
	}
	type result struct {
		message string
		err     error
	}
	var wg sync.WaitGroup
	chSource := make(chan string, parallel)
	chResult := make(chan result, parallel)

	// walk s3
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(chSource)
		req := c.s3.ListObjectsV2Request(&s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(key),
		})
		p := s3.NewListObjectsV2Paginator(req)
		for p.Next(c.ctx) {
			page := p.CurrentPage()
			for _, obj := range page.Contents {
				select {
				case chSource <- aws.StringValue(obj.Key):
				case <-c.ctx.Done():
					return
				}
			}
		}
		if err := p.Err(); err != nil {
			select {
			case chResult <- result{err: err}:
			case <-c.ctx.Done():
				return
			}
		}
	}()

	// download workers
	download := func(p string) (string, error) {
		distPath := filepath.Join(dist, filepath.FromSlash(strings.TrimPrefix(p, key)))
		dir, _ := filepath.Split(distPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return "", err
		}
		f, err := os.OpenFile(distPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			return "", err
		}
		_, err = c.downloader.DownloadWithContext(c.ctx, f, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(p),
		})
		if err != nil {
			return "", err
		}
		if err := f.Close(); err != nil {
			return "", err
		}
		return fmt.Sprintf("download s3://%s/%s to %s", bucket, p, distPath), nil
	}
	wg.Add(parallel)
	for i := 0; i < parallel; i++ {
		go func() {
			defer wg.Done()
			for {
				var key string
				var ok bool
				select {
				case key, ok = <-chSource:
					if !ok {
						return
					}
				case <-c.ctx.Done():
					return
				}
				msg, err := download(key)
				if err != nil {
					select {
					case chResult <- result{err: err}:
					case <-c.ctx.Done():
						return
					}
				}
				select {
				case chResult <- result{message: msg}:
				case <-c.ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(chResult)
	}()

	var err error
	for ret := range chResult {
		if ret.err != nil {
			c.cmd.PrintErrln("Download error: ", ret.err)
			err = ret.err
			c.cancel()
		} else {
			c.cmd.PrintErrln(ret.message)
		}
	}
	return err
}

func (c *client) s3s3(src, dist string) error {
	srcBucket, srcKey := parsePath(src)
	distBucket, distKey := parsePath(dist)
	if distKey == "" || distKey[len(distKey)-1] == '/' {
		distKey += path.Base(srcKey)
	}

	resp, err := c.s3.HeadObjectRequest(&s3.HeadObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(srcKey),
	}).Send(c.ctx)
	if err != nil {
		return err
	}
	if aws.Int64Value(resp.ContentLength) <= maxCopyObjectBytes {
		// https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjectsUsingAPIs.html
		_, err := c.s3.CopyObjectRequest(&s3.CopyObjectInput{
			Bucket:     aws.String(distBucket),
			Key:        aws.String(distKey),
			CopySource: aws.String(srcBucket + "/" + srcKey),
		}).Send(c.ctx)
		if err != nil {
			return err
		}
		c.cmd.PrintErrf("copy s3://%s/%s to s3://%s/%s\n", srcBucket, srcKey, distBucket, distKey)
	} else {
		// https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjctsMPUapi.html
		size := aws.Int64Value(resp.ContentLength)
		resp, err := c.s3.CreateMultipartUploadRequest(&s3.CreateMultipartUploadInput{
			Bucket: aws.String(distBucket),
			Key:    aws.String(distKey),
		}).Send(c.ctx)
		uploadID := resp.UploadId
		if err != nil {
			return err
		}

		// watch incomplete upload
		success := make(chan struct{}) // the size of the channel must be zero to avoid unexpected aborting
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-success:
				return
			case <-c.ctx.Done():
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			c.s3.AbortMultipartUploadRequest(&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(distBucket),
				Key:      aws.String(distKey),
				UploadId: uploadID,
			}).Send(ctx)
		}()

		// upload parts
		g, ctx := errgroup.WithContext(c.ctx)
		parts := make([]s3.CompletedPart, int((size+copyChunkBytes-1)/copyChunkBytes))
		sem := make(chan struct{}, parallel)
	UPLOAD:
		for i, pos := int64(1), int64(0); pos < size; i, pos = i+1, pos+copyChunkBytes {
			i, pos := i, pos
			lastByte := pos + copyChunkBytes - 1
			if lastByte >= size {
				lastByte = size - 1
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				break UPLOAD
			}
			g.Go(func() error {
				defer func() {
					<-sem
				}()
				resp, err := c.s3.UploadPartCopyRequest(&s3.UploadPartCopyInput{
					Bucket:          aws.String(distBucket),
					Key:             aws.String(distKey),
					CopySource:      aws.String(srcBucket + "/" + srcKey),
					CopySourceRange: aws.String(fmt.Sprintf("%d-%d", pos, lastByte)),
					UploadId:        uploadID,
					PartNumber:      aws.Int64(i),
				}).Send(ctx)
				if err != nil {
					return err
				}
				parts[i-1] = s3.CompletedPart{
					ETag:       resp.CopyPartResult.ETag,
					PartNumber: aws.Int64(i),
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			c.cancel()
			wg.Wait()
			return err
		}

		// complete
		_, err = c.s3.CompleteMultipartUploadRequest(&s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(distBucket),
			Key:      aws.String(distKey),
			UploadId: uploadID,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: parts,
			},
		}).Send(c.ctx)
		if err != nil {
			return err
		}

		// notify success via channel
		success <- struct{}{}
		wg.Wait()
	}
	return nil
}

func (c *client) s3s3recursive(src, dist string) error {
	return nil
}

func parsePath(path string) (bucket, key string) {
	path = strings.TrimPrefix(path, "s3://")
	if idx := strings.IndexByte(path, '/'); idx > 0 {
		bucket = path[:idx]
		key = path[idx+1:]
		return
	}
	bucket = path
	return
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func getContentType(path string) *string {
	if contentType != "" {
		return aws.String(contentType)
	}
	if noGuessMimeType {
		return aws.String("application/octet-stream")
	}

	// guess content type
	var t string
	if idx := strings.LastIndex(path, "."); idx >= 0 {
		t = mime.TypeByExtension(path[idx:])
	}
	if t == "" {
		t = "application/octet-stream"
	}
	return aws.String(t)
}
