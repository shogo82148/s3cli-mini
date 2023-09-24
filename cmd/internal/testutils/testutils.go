package testutils

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

// Bucket is an S3 bucket.
// It is used for integration tests.
type Bucket struct {
	// bucket name
	name string
}

func (b *Bucket) Name() string {
	return b.name
}

type APIClient interface {
	interfaces.BucketCreator
	interfaces.BucketHeader
	interfaces.ObjectListerV2
	interfaces.ObjectDeleter
	interfaces.BucketDeleter
}

// BucketPool is a pool of buckets.
type BucketPool struct {
	svc   APIClient
	input *s3.CreateBucketInput
	mu    sync.Mutex
	pool  map[*Bucket]struct{}
	all   map[*Bucket]struct{}
	sem   chan struct{}
}

func NewBucketPool(input *s3.CreateBucketInput, svc APIClient, size int) *BucketPool {
	if input == nil {
		input = &s3.CreateBucketInput{}
	}
	if size < 1 {
		size = 1
	}
	return &BucketPool{
		svc:   svc,
		input: input,
		pool:  make(map[*Bucket]struct{}),
		all:   make(map[*Bucket]struct{}),
		sem:   make(chan struct{}, size),
	}
}

func (pool *BucketPool) Get(ctx context.Context) (*Bucket, error) {
	pool.sem <- struct{}{}

	if bucket := pool.get(); bucket != nil {
		if err := pool.makeEmpty(ctx, bucket); err == nil {
			return bucket, nil
		}
	}

	bucket, err := pool.createBucket(ctx, pool.input)
	if err != nil {
		return nil, err
	}

	// wait for the bucket is visible
	time.Sleep(5 * time.Second)
	if err := pool.waitForCreating(ctx, bucket); err != nil {
		return nil, err
	}

	return bucket, nil
}

func (pool *BucketPool) Put(bucket *Bucket) {
	<-pool.sem
	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.pool[bucket] = struct{}{}
}

func (pool *BucketPool) Cleanup(ctx context.Context) error {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	for bucket := range pool.all {
		if err := pool.makeEmpty(ctx, bucket); err == nil {
			pool.deleteBucket(ctx, bucket)
		}
	}
	return nil
}

func (pool *BucketPool) get() *Bucket {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	for bucket := range pool.pool {
		delete(pool.pool, bucket)
		return bucket
	}
	return nil
}

func (pool *BucketPool) putBucket(bucket *Bucket) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.all[bucket] = struct{}{}
}

func (pool *BucketPool) createBucket(ctx context.Context, input *s3.CreateBucketInput) (*Bucket, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	bucketName := bucketPrefix() + hex.EncodeToString(b[:])
	bucket := &Bucket{
		name: bucketName,
	}
	pool.putBucket(bucket)

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	in := *input
	in.Bucket = aws.String(bucketName)
	if region != "us-east-1" {
		in.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}
	_, err = pool.svc.CreateBucket(ctx, &in)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

func (pool *BucketPool) waitForCreating(ctx context.Context, bucket *Bucket) error {
	for i := 0; i < 60; i++ {
		_, err := pool.svc.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucket.name),
		})
		if err == nil {
			return nil
		}
		time.Sleep(10 * time.Second)
	}

	return errors.New("creating bucket is timeout")
}

// makeEmpty deletes all objects in the bucket.
func (pool *BucketPool) makeEmpty(ctx context.Context, bucket *Bucket) error {
	p := s3.NewListObjectsV2Paginator(pool.svc, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket.name),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil
		}
		for _, obj := range page.Contents {
			pool.svc.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket.name),
				Key:    obj.Key,
			})
		}
	}
	return nil
}

func (pool *BucketPool) deleteBucket(ctx context.Context, bucket *Bucket) error {
	_, err := pool.svc.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket.name),
	})
	return err
}

type DeleteBucketAPI interface {
	interfaces.BucketDeleter
	interfaces.ObjectListerV2
	interfaces.ObjectDeleter
}

// DeleteBucket deletes a S3 bucket.
func DeleteBucket(ctx context.Context, svc DeleteBucketAPI, bucketName string) error {
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
