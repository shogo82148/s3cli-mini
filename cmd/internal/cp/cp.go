package cp

import (
	"io"
	"log"
	"net/url"
	"os"
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

	src, err := c.newReader(args[0])
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()

	dest, err := c.newWriter(args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer dest.Close()

	if _, err := io.Copy(dest, src); err != nil {
		log.Fatal(err)
	}
}

func (c *client) newReader(s string) (io.ReadCloser, error) {
	if strings.HasPrefix(s, "s3://") {
		u, err := url.Parse(s)
		if err != nil {
			return nil, err
		}
		req := c.s3.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(u.Host),
			Key:    aws.String(strings.TrimPrefix(u.Path, "/")),
		})
		resp, err := req.Send()
		if err != nil {
			return nil, err
		}
		return resp.Body, nil
	}
	return os.Open(s)
}

func (c *client) newWriter(s string) (io.WriteCloser, error) {
	return os.OpenFile(s, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
}
