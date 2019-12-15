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
	"github.com/aws/aws-sdk-go-v2/service/s3/s3iface"
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
func CreateTemporaryBucket(ctx context.Context, svc s3iface.ClientAPI) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	bucketName := bucketPrefix() + hex.EncodeToString(b[:])

	_, err := svc.CreateBucketRequest(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}).Send(ctx)
	if err != nil {
		return "", err
	}

	// wait for the bucket is visible
	time.Sleep(time.Second)
	for i := 0; i < 5; i++ {
		_, err := svc.HeadBucketRequest(&s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		}).Send(ctx)
		if err == nil {
			return "", err
		}
		time.Sleep(time.Second)
	}
	return bucketName, nil
}

// DeleteBucket deletes a S3 bucket.
func DeleteBucket(ctx context.Context, svc s3iface.ClientAPI, bucketName string) error {
	req := svc.ListObjectsV2Request(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	p := s3.NewListObjectsV2Paginator(req)
	for p.Next(ctx) {
		page := p.CurrentPage()
		for _, obj := range page.Contents {
			svc.DeleteObjectRequest(&s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    obj.Key,
			}).Send(ctx)
		}
	}
	if err := p.Err(); err != nil {
		return err
	}

	_, err := svc.DeleteBucketRequest(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	}).Send(ctx)
	return err
}
