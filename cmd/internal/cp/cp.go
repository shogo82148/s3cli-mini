package cp

import (
	"io"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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

	config := aws.NewConfig().WithRegion("ap-northeast-1")
	session := session.Must(session.NewSession(config))
	c := &client{
		s3: s3.New(session),
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
		out, err := c.s3.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(u.Host),
			Key:    aws.String(u.Path),
		})
		if err != nil {
			return nil, err
		}
		return out.Body, nil
	}
	return os.Open(s)
}

func (c *client) newWriter(s string) (io.WriteCloser, error) {
	return os.OpenFile(s, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
}
