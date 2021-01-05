package testutils

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shogo82148/s3cli-mini/cmd/internal/interfaces"
)

func bucketPrefix() string {
	return os.Getenv("S3CLI_TEST_BUCKET_PREFIX")
}

// SkipIfUnitTest skips join tests.
func SkipIfUnitTest(t *testing.T) {
	if bucketPrefix() == "" {
		t.Skip("S3CLI_TEST_BUCKET_PREFIX environment value is not set. skip this test.")
	}
}

// CreateTemporaryBucket creates a temporary S3 bucket.
func CreateTemporaryBucket(ctx context.Context, svc interfaces.S3Client) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	bucketName := bucketPrefix() + hex.EncodeToString(b[:])

	_, err := svc.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return "", err
	}

	// wait for the bucket is visible
	time.Sleep(5 * time.Second)
	for i := 0; i < 60; i++ {
		_, err := svc.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			return bucketName, nil
		}
		time.Sleep(10 * time.Second)
	}
	return bucketName, nil
}

// DeleteBucket deletes a S3 bucket.
func DeleteBucket(ctx context.Context, svc interfaces.S3Client, bucketName string) error {
	p := s3.NewListObjectsV2Paginator(svc, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil
		}
		for _, obj := range page.Contents {
			svc.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    obj.Key,
			})
		}
	}

	_, err := svc.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	return err
}
