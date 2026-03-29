package mb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/shogo82148/s3cli-mini/cmd/internal/testutils"
	"github.com/spf13/cobra"
)

func TestMB_Global(t *testing.T) {
	testutils.SkipIfUnitTest(t)

	bucketNamespace = "global"
	defer func() { bucketNamespace = "" }()

	ctx := t.Context()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	bucketName := testutils.BucketPrefix() + hex.EncodeToString(b[:])

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := testutils.DeleteBucket(ctx, svc, bucketName); err != nil {
			t.Error(err)
		}
	})

	Run(&cobra.Command{}, []string{"s3://" + bucketName})

	// wait for the bucket is visible
	waiter := s3.NewBucketExistsWaiter(svc)
	if err := waiter.Wait(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}, 5*time.Minute); err != nil {
		t.Fatalf("bucket %s is not found: %s", bucketName, err)
	}
}

func TestMB_AccountRegional(t *testing.T) {
	testutils.SkipIfUnitTest(t)

	bucketNamespace = "account-regional"
	defer func() { bucketNamespace = "" }()

	ctx := t.Context()
	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadAWSConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stsSvc := sts.NewFromConfig(cfg)

	// build bucket name
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	out, err := stsSvc.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatal(err)
	}
	bucketName := fmt.Sprintf(
		"%s%s-%s-%s-an",
		testutils.BucketPrefix(),
		hex.EncodeToString(b[:]),
		*out.Account,
		cfg.Region,
	)

	// cleanup bucket after test
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := testutils.DeleteBucket(ctx, svc, bucketName); err != nil {
			t.Error(err)
		}
	})

	// create bucket
	Run(&cobra.Command{}, []string{"s3://" + bucketName})

	// wait for the bucket is visible
	waiter := s3.NewBucketExistsWaiter(svc)
	if err := waiter.Wait(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}, 5*time.Minute); err != nil {
		t.Fatalf("bucket %s is not found: %s", bucketName, err)
	}
}
