package cp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
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
var followSymlinks bool
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
	ctx        context.Context
	cancel     context.CancelFunc
	cmd        *cobra.Command
	s3         s3iface.ClientAPI
	uploader   s3manageriface.UploaderAPI
	downloader s3manageriface.DownloaderAPI
}

type source interface {
	Name() string
}

type s3source interface {
	source
}

type localSource interface {
	source
	Open() (io.ReadCloser, error)
}

var _ localSource = (*localFileSource)(nil)

type localFileSource struct {
	name string
}

func (s *localFileSource) Open() (io.ReadCloser, error) {
	return os.Open(s.name)
}

func (s *localFileSource) Name() string {
	return s.name
}

type result struct {
	message string
	err     error
}

// Run runs cp command.
func Run(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(args) != 2 {
		cmd.Usage()
		return
	}
	followSymlinks = followSymlinks && !noFollowSymlinks

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
	c.Run(args[0], args[1])
}

func (c *client) Run(src, dist string) {
	if parallel <= 0 {
		parallel = 1
	}
	chSource := make(chan source, parallel)
	go c.parseSource(src, chSource)

	ret := make(chan result, parallel)
	var wg sync.WaitGroup
	wg.Add(parallel)
	for i := 0; i < parallel; i++ {
		go func() {
			defer wg.Done()
			c.runWorker(ret, chSource, dist)
		}()
	}
	go func() {
		wg.Wait()
		close(ret)
	}()

	for r := range ret {
		if r.err != nil {
			c.cmd.PrintErrln("Error: ", r.err)
			c.cancel()
		}
		c.cmd.PrintErrln(r.message)
	}
}

func (c *client) parseSource(src string, ch chan<- source) {
	ch <- &localFileSource{name: src}
	close(ch)
}

func (c *client) runWorker(ret chan<- result, src <-chan source, dist string) {
	for {
		var s source
		var ok bool
		select {
		case s, ok = <-src:
			if !ok {
				return
			}
		case <-c.ctx.Done():
			return
		}
		message, err := c.handleSource(s, dist)
		if err != nil {
			select {
			case ret <- result{err: err}:
			case <-c.ctx.Done():
			}
			return
		}
		select {
		case ret <- result{message: message}:
		case <-c.ctx.Done():
		}
	}
}

func (c *client) handleSource(s source, dist string) (string, error) {
	bucket, key := parsePath(dist)
	switch s := s.(type) {
	case localSource:
		r, err := s.Open()
		if err != nil {
			return "", fmt.Errorf("upload failed: %w", err)
		}
		resp, err := c.uploader.UploadWithContext(c.ctx, &s3manager.UploadInput{
			Body:   r,
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return "", fmt.Errorf("upload failed: %w", err)
		}
		r.Close()
		return fmt.Sprintf("upload: %s to %s", s.Name(), resp.Location), nil
	case s3source:
		return "TODO", nil
	}
	return "", errors.New("upload failed: type missmatch")
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
