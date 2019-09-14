package ls

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/spf13/cobra"
)

func TestLS_LustBuckets(t *testing.T) {
	if err := config.SetupTest(t); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.CreateBucketRequest(&s3.CreateBucketInput{
		Bucket: aws.String("bucket-for-test"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&buf)
	Run(cmd, []string{})

	if buf.String() != "2006-02-04 01:45:09 bucket-for-test\n" {
		t.Errorf("want 2006-02-04 01:45:09 bucket-for-test, got %s", buf.String())
	}
}
