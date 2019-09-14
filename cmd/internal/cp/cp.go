package cp

import (
	"context"
	"io"
	"mime"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3iface"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager/s3manageriface"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/spf13/cobra"
)

type client struct {
	s3 s3iface.ClientAPI
}

// Run runs cp command.
func Run(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		return
	}

	svc, err := config.NewS3Client()
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
	c := &client{
		s3: svc,
	}

	src, name, err := c.newReader(args[0])
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
	defer func() {
		src.Close()
	}()

	dest, err := c.newWriter(args[1], name)
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
	defer func() {
		if err := dest.Close(); err != nil {
			cmd.PrintErrln(err)
			os.Exit(1)
		}
	}()

	if _, err := io.Copy(dest, src); err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
}

func (c *client) newReader(s string) (io.ReadCloser, string, error) {
	if strings.HasPrefix(s, "s3://") {
		u, err := url.Parse(s)
		if err != nil {
			return nil, "", err
		}
		req := c.s3.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(u.Host),
			Key:    aws.String(strings.TrimPrefix(u.Path, "/")),
		})
		resp, err := req.Send(context.Background())
		if err != nil {
			return nil, "", err
		}
		return resp.Body, path.Base(u.Path), nil
	}

	f, err := os.Open(s)
	return f, filepath.Base(s), err
}

type uploader struct {
	io.WriteCloser
	wg       sync.WaitGroup
	uploader s3manageriface.UploaderAPI
	err      error
}

func (u *uploader) Close() error {
	err := u.WriteCloser.Close()
	u.wg.Wait()
	if u.err != nil {
		return u.err
	}
	return err
}

func (c *client) newWriter(s, name string) (io.WriteCloser, error) {
	if strings.HasPrefix(s, "s3://") {
		u, err := url.Parse(s)
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(u.Path, "/") {
			u.Path += name
		}
		var t string
		if idx := strings.LastIndex(u.Path, "."); idx >= 0 {
			t = mime.TypeByExtension(u.Path[idx:])
		}
		if t == "" {
			t = "application/octet-stream"
		}
		r, w := io.Pipe()
		uploader := &uploader{
			WriteCloser: w,
			uploader:    s3manager.NewUploaderWithClient(c.s3),
		}
		uploader.wg.Add(1)
		go func() {
			defer r.Close()
			defer uploader.wg.Done()
			_, err := uploader.uploader.Upload(&s3manager.UploadInput{
				Bucket:      aws.String(u.Host),
				Key:         aws.String(strings.TrimPrefix(u.Path, "/")),
				Body:        r,
				ContentType: aws.String(t),
			})
			if err != nil {
				r.CloseWithError(err)
			}
			uploader.err = err
		}()
		return uploader, nil
	}
	if stat, err := os.Stat(s); err == nil {
		if stat.IsDir() {
			s = filepath.Join(s, name)
		}
	}
	return os.OpenFile(s, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
}
