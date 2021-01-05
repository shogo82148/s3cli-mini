package mb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/shogo82148/s3cli-mini/cmd/internal/testutils"
	"github.com/spf13/cobra"
)

func TestMB(t *testing.T) {
	testutils.SkipIfUnitTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	bucketName := os.Getenv("S3CLI_TEST_BUCKET_PREFIX") + hex.EncodeToString(b[:])

	defer testutils.DeleteBucket(ctx, svc, bucketName)

	Run(&cobra.Command{}, []string{"s3://" + bucketName})
	time.Sleep(5 * time.Second)

	// wait for the bucket is visible
	time.Sleep(time.Second)
	for i := 0; i < 60; i++ {
		_, err := svc.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			return
		}
		time.Sleep(10 * time.Second)
	}
	t.Errorf("bucket %s is not found", bucketName)
}
