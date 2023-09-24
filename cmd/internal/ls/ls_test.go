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
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/shogo82148/s3cli-mini/cmd/internal/interfaces"
	"github.com/shogo82148/s3cli-mini/cmd/internal/testutils"
	"github.com/spf13/cobra"
)

var pool *testutils.BucketPool

func TestMain(m *testing.M) {
	svc, err := config.NewS3Client(context.Background())
	if err != nil {
		panic(err)
	}
	pool = testutils.NewBucketPool(nil, svc, 1)
	defer pool.Cleanup(context.Background())

	m.Run()
}

func prepareBucket(ctx context.Context, svc interfaces.S3Client) (*testutils.Bucket, error) {
	bucket, err := pool.Get(ctx)
	if err != nil {
		return nil, err
	}

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
		_, err = svc.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket.Name()),
			Key:    aws.String(key),
			Body:   strings.NewReader(key),
		})
		if err != nil {
			return nil, err
		}
	}

	return bucket, nil
}

func TestLS_ListObjects(t *testing.T) {
	t.Parallel()
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := prepareBucket(ctx, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	// test
	var buf bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&buf)
	Run(cmd, []string{"s3://" + bucket.Name()})

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
	t.Parallel()
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := prepareBucket(ctx, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	recursive = true
	defer func() {
		recursive = false
	}()

	// test
	var buf bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&buf)
	Run(cmd, []string{"s3://" + bucket.Name()})

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
