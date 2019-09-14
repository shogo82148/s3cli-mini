package ls

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3iface"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/spf13/cobra"
)

func prepareBucket(ctx context.Context, svc s3iface.ClientAPI) (cleanup func(), err error) {
	const bucketName = "bucket-for-test"
	cancelFuncs := []func(){}
	cleanup = func() {
		for i := len(cancelFuncs); i > 0; i-- {
			cancelFuncs[i-1]()
		}
	}
	defer func() {
		if err != nil {
			cleanup()
			cleanup = nil
		}
	}()

	// prepare a bucket for test
	_, err = svc.CreateBucketRequest(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}).Send(ctx)
	if err != nil {
		return
	}

	cancelFuncs = append(cancelFuncs, func() {
		// clean up
		svc.DeleteBucketRequest(&s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		}).Send(ctx)
	})

	keys := []string{
		"a.txt",
		"foo.zip",
		"foo/bar/.baz/a",
		"foo/bar/.baz/b",
		"foo/bar/.baz/c",
		"foo/bar/.baz/d",
		"foo/bar/.baz/e",
		"foo/bar/.baz/hooks/bar",
		"foo/bar/.baz/hooks/foo",
		"z.txt",
	}

	// prepare objects for test
	for _, key := range keys {
		key := key
		_, err = svc.PutObjectRequest(&s3.PutObjectInput{
			Bucket: aws.String("bucket-for-test"),
			Key:    aws.String(key),
			Body:   strings.NewReader(key),
		}).Send(ctx)
		if err != nil {
			return
		}
		cancelFuncs = append(cancelFuncs, func() {
			// clean up
			svc.DeleteObjectRequest(&s3.DeleteObjectInput{
				Bucket: aws.String("bucket-for-test"),
				Key:    aws.String(key),
			}).Send(ctx)
		})
	}

	return
}

func TestLS_ListBuckets(t *testing.T) {
	if err := config.SetupTest(t); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := prepareBucket(ctx, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	// test
	var buf bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&buf)
	Run(cmd, []string{})

	re := regexp.MustCompile(`(?m)^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} bucket-for-test$`)
	if !re.Match(buf.Bytes()) {
		t.Errorf("unexpected result: %s", buf.String())
	}
}

func TestLS_ListObjects(t *testing.T) {
	if err := config.SetupTest(t); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := prepareBucket(ctx, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	// test
	var buf bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&buf)
	Run(cmd, []string{"s3://bucket-for-test"})

	re := regexp.MustCompile(`\A\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}          5 a.txt
\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}          7 foo.zip
                           PRE foo/
\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}          5 z.txt
\z`)
	if !re.Match(buf.Bytes()) {
		t.Errorf("unexpected result: %s", buf.String())
	}
}

func TestLS_recursive(t *testing.T) {
	if err := config.SetupTest(t); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	recursive = true
	defer func() {
		recursive = false
	}()

	svc, err := config.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := prepareBucket(ctx, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	// test
	var buf bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&buf)
	Run(cmd, []string{"s3://bucket-for-test"})

	re := regexp.MustCompile(`\A\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}          5 a.txt
\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}          7 foo.zip
\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}         14 foo/bar/.baz/a
\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}         14 foo/bar/.baz/b
\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}         14 foo/bar/.baz/c
\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}         14 foo/bar/.baz/d
\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}         14 foo/bar/.baz/e
\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}         22 foo/bar/.baz/hooks/bar
\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}         22 foo/bar/.baz/hooks/foo
\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}          5 z.txt
\z`)
	if !re.Match(buf.Bytes()) {
		t.Errorf("unexpected result: %s", buf.String())
	}
}

func TestMakeHumanReadable(t *testing.T) {
	// port of https://github.com/aws/aws-cli/blob/072688cc07578144060aead8b75556fd986e0f2f/tests/unit/customizations/s3/test_utils.py#L50-L68
	cases := []struct {
		in  int64
		out string
	}{
		{0, "0 Bytes"},
		{1, "1 Byte"},
		{1000, "1000 Bytes"},
		{1 << 10, "1.0 KiB"},
		{1 << 20, "1.0 MiB"},
		{1 << 30, "1.0 GiB"},
		{1 << 40, "1.0 TiB"},
		{1 << 50, "1.0 PiB"},
		{1 << 60, "1.0 EiB"},
	}
	for _, tt := range cases {
		got := makeHumanReadable(tt.in)
		if got != tt.out {
			t.Errorf("%d byte(s): want %s, got %s", tt.in, tt.out, got)
		}
	}
}
