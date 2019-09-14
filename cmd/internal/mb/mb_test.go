package mb

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/spf13/cobra"
)

func TestMB(t *testing.T) {
	if err := config.SetupTest(t); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		// clean up
		svc.DeleteBucketRequest(&s3.DeleteBucketInput{
			Bucket: aws.String("bucket-for-test"),
		}).Send(ctx)
	}()

	Run(&cobra.Command{}, []string{"s3://bucket-for-test"})

	resp, err := svc.ListBucketsRequest(&s3.ListBucketsInput{}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if aws.StringValue(resp.Buckets[0].Name) != "bucket-for-test" {
		t.Logf("want %s, got %s", "bucket-for-test", aws.StringValue(resp.Buckets[0].Name))
	}
}
