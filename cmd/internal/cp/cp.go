package cp

import (
	"context"
	"fmt"
	"io"
	"mime"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/shogo82148/s3cli-mini/cmd/internal/interfaces"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/spf13/cobra"
)

// maximum object size in a single atomic operation. (5GiB)
// https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjectsExamples.html
// It is a variable, because of tests.
var maxCopyObjectBytes = int64(5 * 1024 * 1024 * 1024)

// chunk size for multipart copying objects
const copyChunkBytes = 5 * 1024 * 1024

const distStdout = "-"
const srcStdin = "-"

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
	ctxAbort       context.Context
	cancelAbort    context.CancelFunc
	wg             sync.WaitGroup
	semaphore      chan struct{}
	cmd            *cobra.Command
	s3             interfaces.S3Client
	uploader       interfaces.UploaderClient
	downloader     interfaces.DownloaderClient
	followSymlinks bool
	acl            types.ObjectCannedACL
	expires        *time.Time
}

// Run runs cp command.
func Run(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctxAbort, cancelAbort := context.WithCancel(context.Background())
	defer cancelAbort()

	if len(args) != 2 {
		if err := cmd.Usage(); err != nil {
			cmd.PrintErrln("error: ", err)
			os.Exit(1)
		}
		return
	}
	if parallel <= 0 {
		parallel = 4
	}

	c := client{
		ctx:         ctx,
		cancel:      cancel,
		ctxAbort:    ctxAbort,
		cancelAbort: cancelAbort,
		semaphore:   make(chan struct{}, parallel),
		cmd:         cmd,
	}
	var err error
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
	go c.handleSignal()

	c.Run(args[0], args[1])
}

func parseACL(acl string) (types.ObjectCannedACL, error) {
	switch acl {
	case "":
		return "", nil
	case "private":
		return types.ObjectCannedACLPrivate, nil
	case "public-read":
		return types.ObjectCannedACLPublicRead, nil
	case "public-read-write":
		return types.ObjectCannedACLPublicReadWrite, nil
	case "authenticated-read":
		return types.ObjectCannedACLAuthenticatedRead, nil
	case "aws-exec-read":
		return types.ObjectCannedACLAwsExecRead, nil
	case "bucket-owner-read":
		return types.ObjectCannedACLBucketOwnerRead, nil
	case "bucket-owner-full-control":
		return types.ObjectCannedACLBucketOwnerFullControl, nil
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

	if !s3src && !s3dist {
		c.cmd.PrintErrln("Error: Invalid argument type")
		os.Exit(1)
	}

	var bucket string
	if s3dist {
		bucket, _ = parsePath(dist)
	} else if s3src {
		bucket, _ = parsePath(src)
	}
	svc, err := config.NewS3BucketClient(c.ctx, bucket)
	if err != nil {
		c.cmd.PrintErrln("Error: ", err)
		os.Exit(1)
	}
	c.s3 = svc
	c.uploader = manager.NewUploader(svc)
	c.downloader = manager.NewDownloader(svc)

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

	panic("will not reach")
}

// acquire controls parallelism.
// waits for the semaphore and returns true if success.
// the caller should call c.release after the acquire returns true.
func (c *client) acquire() bool {
	select {
	case <-c.ctx.Done():
		return false
	case c.semaphore <- struct{}{}:
		c.wg.Add(1)
		return true
	}
}

func (c *client) release() {
	<-c.semaphore
	c.wg.Done()
}

func (c *client) handleSignal() {
	count := 0
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	for range ch {
		if count == 0 {
			c.cancel()
		} else {
			c.cancelAbort()
		}
		count++
	}
}

func (c *client) s3stdout(bucket, key string) error {
	if dryrun {
		c.cmd.PrintErrf("download s3://%s/%s to STDOUT\n", bucket, key)
		return nil
	}
	res, err := c.s3.GetObject(c.ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	body := res.Body
	defer body.Close()
	if _, err := io.Copy(os.Stdout, body); err != nil {
		return err
	}
	return nil
}

func (c *client) s3local(src, dist string) error {
	bucket, key := parsePath(src)
	if key == "" || key[len(key)-1] == '/' {
		c.cmd.PrintErrln("Error: Invalid argument type")
		os.Exit(1)
	}
	if dist == distStdout {
		return c.s3stdout(bucket, key)
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
	_, err = c.downloader.Download(c.ctx, f, &s3.GetObjectInput{
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
		p := s3.NewListObjectsV2Paginator(c.s3, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(key),
		})
		for p.HasMorePages() {
			page, err := p.NextPage(c.ctx)
			if err != nil {
				select {
				case chResult <- result{err: err}:
				case <-c.ctx.Done():
				}
				return
			}
			for _, obj := range page.Contents {
				select {
				case chSource <- aws.ToString(obj.Key):
				case <-c.ctx.Done():
					return
				}
			}
		}
	}()

	// download workers
	download := func(p string) (string, error) {
		distPath := filepath.Join(dist, filepath.FromSlash(strings.TrimPrefix(p, key)))
		if dryrun {
			return fmt.Sprintf("download s3://%s/%s to %s", bucket, p, distPath), nil
		}
		dir, _ := filepath.Split(distPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return "", err
		}
		f, err := os.OpenFile(distPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			return "", err
		}
		_, err = c.downloader.Download(c.ctx, f, &s3.GetObjectInput{
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
