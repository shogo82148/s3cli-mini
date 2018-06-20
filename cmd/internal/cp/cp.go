package cp

import (
	"io"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
)

type client struct {
	s3 *s3.S3
}

// Run runs cp command.
func Run(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		log.Println("usage: cp <LocalPath> <S3Uri> or <S3Uri> <LocalPath> or <S3Uri> <S3Uri>")
		return
	}

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		log.Fatal(err)
	}
	c := &client{
		s3: s3.New(cfg),
	}

	src, name, err := c.newReader(args[0])
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()

	dest, err := c.newWriter(args[1], name)
	if err != nil {
		log.Fatal(err)
	}
	defer dest.Close()

	if _, err := io.Copy(dest, src); err != nil {
		log.Fatal(err)
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
		resp, err := req.Send()
		if err != nil {
			return nil, "", err
		}
		return resp.Body, path.Base(u.Path), nil
	}

	f, err := os.Open(s)
	return f, filepath.Base(s), err
}

func (c *client) newWriter(s, name string) (io.WriteCloser, error) {
	if stat, err := os.Stat(s); err == nil {
		if stat.IsDir() {
			s = filepath.Join(s, name)
		}
	}
	return os.OpenFile(s, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
}
