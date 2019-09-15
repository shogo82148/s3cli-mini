package cp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3iface"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager/s3manageriface"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/spf13/cobra"
)

var dryrun bool
var parallel int
var includes []string
var excludes []string
var acl string
var recursive bool
var followSymlinks = true
var noFollowSymlinks bool
var noGuessMimeType bool

// Init initializes flags.
func Init(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&dryrun, "dryrun", false, "Displays the operations that would be performed using the specified command without actually running them.")
	cmd.Flags().StringArrayVar(&includes, "include", []string{}, "Don't exclude files or objects in the command that match the specified pattern. See Use of Exclude and Include Filters for details.")
	cmd.Flags().StringArrayVar(&excludes, "exclude", []string{}, "Exclude all files or objects from the command that matches the specified pattern.")
	cmd.Flags().StringVar(&acl, "acl", "", "Sets the ACL for the object when the command is performed.")
	cmd.Flags().BoolVar(&recursive, "recursive", false, "Command is performed on all files or objects under the specified directory or prefix.")
	cmd.Flags().BoolVar(&followSymlinks, "follow-symlinks", true, "Symbolic links are followed only when uploading to S3 from the local filesystem.")
	cmd.Flags().BoolVar(&noFollowSymlinks, "no-follow-symlinks", false, "")
	cmd.Flags().BoolVar(&noGuessMimeType, "no-guess-mime-type", false, "Do not try to guess the mime type for uploaded files. By default the mime type of a file is guessed when it is uploaded.")
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
		parallel = 1
	}

	s3src := strings.HasPrefix(src, "s3://")
	src = strings.TrimPrefix(src, "s3://")
	s3dist := strings.HasPrefix(dist, "s3://")
	dist = strings.TrimPrefix(dist, "s3://")

	switch {
	case s3src && s3dist:
		c.s3s3(src, dist)
		return
	case !s3src && s3dist:
		if err := c.locals3(src, dist); err != nil {
			c.cmd.PrintErrln("Upload error: ", err)
			os.Exit(1)
		}
		return
	case s3src && !s3dist:
		c.s3local(src, dist)
		return
	}

	c.cmd.PrintErrln("Error: Invalid argument type")
	os.Exit(1)
}

func (c *client) locals3(src, dist string) error {
	bucket, key := parsePath(dist)
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	if key == "" || key[len(key)-1] == '/' {
		key += filepath.Base(src)
	}

	if !dryrun {
		resp, err := c.uploader.UploadWithContext(c.ctx, &s3manager.UploadInput{
			Body:   f,
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			ACL:    c.acl,
		})
		if err != nil {
			return err
		}
		c.cmd.PrintErrf("upload %s to %s\n", src, resp.Location)
	} else {
		c.cmd.PrintErrf("upload %s to s3://%s/%s\n", src, bucket, key)
	}
	return nil
}

func (c *client) s3local(src, dist string) {}

func (c *client) s3s3(src, dist string) {}

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
