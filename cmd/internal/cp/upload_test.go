package cp

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/shogo82148/s3cli-mini/cmd/internal/testutils"
)

func TestUpload(t *testing.T) {
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc, err := config.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	bucketName, err := testutils.CreateTemporaryBucket(ctx, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer testutils.DeleteBucket(context.Background(), svc, bucketName)

	// prepare a test file
	content := []byte("temporary file's content")
	dir, err := ioutil.TempDir("", "s3cli-mini")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	filename := filepath.Join(dir, "tmpfile")
	if err := ioutil.WriteFile(filename, content, 0666); err != nil {
		t.Fatal(err)
	}

	uploader := &uploader{
		ctx:      ctx,
		cancel:   cancel,
		s3:       svc,
		task:     make(chan task),
		parallel: 1,
		partSize: 5 * 1024 * 1024,
	}
	uploader.upload(filename, bucketName)
}

func TestUpload_Multipart(t *testing.T) {
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc, err := config.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	bucketName, err := testutils.CreateTemporaryBucket(ctx, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer testutils.DeleteBucket(context.Background(), svc, bucketName)

	// prepare a test file
	content := make([]byte, 1024*1024*12)
	dir, err := ioutil.TempDir("", "s3cli-mini")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	filename := filepath.Join(dir, "tmpfile")
	if err := ioutil.WriteFile(filename, content, 0666); err != nil {
		t.Fatal(err)
	}

	uploader := &uploader{
		ctx:      ctx,
		cancel:   cancel,
		s3:       svc,
		task:     make(chan task),
		parallel: 5,
		partSize: 5 * 1024 * 1024,
	}
	uploader.upload(filename, bucketName)
}
