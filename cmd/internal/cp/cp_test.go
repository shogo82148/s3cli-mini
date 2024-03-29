package cp

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/shogo82148/s3cli-mini/cmd/internal/testutils"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var pool *testutils.BucketPool
var enabledACLPool *testutils.BucketPool

func TestMain(m *testing.M) {
	svc, err := config.NewS3Client(context.Background())
	if err != nil {
		panic(err)
	}
	pool = testutils.NewBucketPool(nil, svc, 5)
	defer pool.Cleanup(context.Background())

	enabledACLPool = testutils.NewBucketPool(&s3.CreateBucketInput{
		ObjectOwnership: types.ObjectOwnershipBucketOwnerPreferred,
	}, svc, 5)
	defer enabledACLPool.Cleanup(context.Background())

	m.Run()
}

func TestCP_Upload(t *testing.T) {
	t.Parallel()
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	// prepare a test file
	content := []byte("temporary file's content")
	dir, err := os.MkdirTemp("", "s3cli-mini")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	filename := filepath.Join(dir, "tmpfile")
	if err := os.WriteFile(filename, content, 0666); err != nil {
		t.Fatal(err)
	}

	// test
	cmd := &cobra.Command{}
	Run(cmd, []string{filename, "s3://" + bucket.Name() + "/tmpfile.html"})

	resp, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile.html"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// check body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(body))
	}
	if aws.ToString(resp.ContentType) != "text/html; charset=utf-8" {
		t.Errorf("unexpected content-type: want %s, got %s", "text/html; charset-utf-8", aws.ToString(resp.ContentType))
	}

	// check acl
	retACL, err := svc.GetObjectAcl(ctx, &s3.GetObjectAclInput{
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile.html"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range retACL.Grants {
		if g.Grantee.Type != types.TypeCanonicalUser {
			t.Errorf("unexpected grantee type, want %s, got %s", types.TypeCanonicalUser, g.Grantee.Type)
		}
	}
}

func TestCP_Upload_Multipart(t *testing.T) {
	t.Parallel()
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	// prepare a test file
	content := bytes.Repeat([]byte("temporary file's content"), 1024*1024)
	dir, err := os.MkdirTemp("", "s3cli-mini")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	filename := filepath.Join(dir, "tmpfile")
	if err := os.WriteFile(filename, content, 0666); err != nil {
		t.Fatal(err)
	}

	// test
	cmd := &cobra.Command{}
	Run(cmd, []string{filename, "s3://" + bucket.Name() + "/tmpfile.html"})

	resp, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile.html"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// check body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(body))
	}
	if aws.ToString(resp.ContentType) != "text/html; charset=utf-8" {
		t.Errorf("unexpected content-type: want %s, got %s", "text/html; charset-utf-8", aws.ToString(resp.ContentType))
	}

	// check acl
	retACL, err := svc.GetObjectAcl(ctx, &s3.GetObjectAclInput{
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile.html"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range retACL.Grants {
		if g.Grantee.Type != types.TypeCanonicalUser {
			t.Errorf("unexpected grantee type, want %s, got %s", types.TypeCanonicalUser, g.Grantee.Type)
		}
	}
}

func TestCP_Upload_KeyOmitted(t *testing.T) {
	t.Parallel()
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	// prepare a test file
	content := []byte("temporary file's content")
	dir, err := os.MkdirTemp("", "s3cli-mini")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	filename := filepath.Join(dir, "tmpfile")
	if err := os.WriteFile(filename, content, 0666); err != nil {
		t.Fatal(err)
	}

	// test
	cmd := &cobra.Command{}
	Run(cmd, []string{filename, "s3://" + bucket.Name()})

	resp, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile"),
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(body))
	}
}

func TestCP_UploadPublicACL(t *testing.T) {
	// This test overwrites the global variable `acl`.
	// So, this test must be run in parallel.
	// t.Parallel()

	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := enabledACLPool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// allow public access
	_, err = svc.PutPublicAccessBlock(ctx, &s3.PutPublicAccessBlockInput{
		Bucket: aws.String(bucket.Name()),
		PublicAccessBlockConfiguration: &types.PublicAccessBlockConfiguration{
			BlockPublicAcls:       aws.Bool(false),
			BlockPublicPolicy:     aws.Bool(false),
			IgnorePublicAcls:      aws.Bool(false),
			RestrictPublicBuckets: aws.Bool(true),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// prepare a test file
	content := []byte("temporary file's content")
	dir, err := os.MkdirTemp("", "s3cli-mini")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	filename := filepath.Join(dir, "tmpfile")
	if err := os.WriteFile(filename, content, 0666); err != nil {
		t.Fatal(err)
	}

	// test
	acl = "public-read"
	defer func() {
		acl = ""
	}()
	cmd := &cobra.Command{}
	Run(cmd, []string{filename, "s3://" + bucket.Name() + "/tmpfile"})

	resp, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// check body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(body))
	}

	// check acl
	retACL, err := svc.GetObjectAcl(ctx, &s3.GetObjectAclInput{
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile"),
	})
	if err != nil {
		t.Fatal(err)
	}
	var publicRead bool
	for _, g := range retACL.Grants {
		publicRead = publicRead ||
			(g.Grantee.Type == types.TypeGroup &&
				aws.ToString(g.Grantee.URI) == "http://acs.amazonaws.com/groups/global/AllUsers" &&
				g.Permission == types.PermissionRead)
	}
	if !publicRead {
		t.Error("unexpected acl: want public-read, but not")
	}
}

func TestCP_Upload_recursive(t *testing.T) {
	// This test overwrites the global variable `recursive`.
	// So, this test must be run in parallel.
	// t.Parallel()

	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	// prepare test files
	content := []byte("temporary file's content")
	dir, err := os.MkdirTemp("", "s3cli-mini")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
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
	for _, key := range keys {
		filename := filepath.Join(dir, filepath.FromSlash(key))
		dir, _ := filepath.Split(filename)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, content, 0666); err != nil {
			t.Fatal(err)
		}
	}

	// test
	recursive = true
	defer func() {
		recursive = false
	}()
	cmd := &cobra.Command{}
	Run(cmd, []string{dir, "s3://" + bucket.Name()})

	// check body
	for _, key := range keys {
		resp, err := svc.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket.Name()),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if string(body) != string(content) {
			t.Errorf("want %s, got %s", string(content), string(body))
		}
	}
}

func TestCP_Download(t *testing.T) {
	t.Parallel()
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	// prepare a test object
	content := []byte("temporary file's content")
	_, err = svc.PutObject(ctx, &s3.PutObjectInput{
		Body:   bytes.NewReader(content),
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// test
	dir, err := os.MkdirTemp("", "s3cli-mini")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	filename := filepath.Join(dir, "tmpfile")
	cmd := &cobra.Command{}
	Run(cmd, []string{"s3://" + bucket.Name() + "/tmpfile", filename})

	got, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(got))
	}
}

func TestCP_Download_recursive(t *testing.T) {
	// This test overwrites the global variable `recursive`.
	// So, this test must be run in parallel.
	// t.Parallel()

	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	// prepare a test object
	content := []byte("temporary file's content")
	dir, err := os.MkdirTemp("", "s3cli-mini")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
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
	for _, key := range keys {
		_, err = svc.PutObject(ctx, &s3.PutObjectInput{
			Body:   bytes.NewReader(content),
			Bucket: aws.String(bucket.Name()),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// test
	recursive = true
	defer func() {
		recursive = false
	}()
	cmd := &cobra.Command{}
	Run(cmd, []string{"s3://" + bucket.Name() + "/", dir})

	for _, key := range keys {
		filename := filepath.Join(dir, filepath.FromSlash(key))
		data, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != string(content) {
			t.Errorf("key %s: want %s, got %s", key, string(data), string(key))
		}
	}
}

func TestCP_Copy(t *testing.T) {
	t.Parallel()
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	// prepare a test object
	content := []byte("temporary file's content")
	_, err = svc.PutObject(ctx, &s3.PutObjectInput{
		Body:   bytes.NewReader(content),
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile"),
	})
	if err != nil {
		t.Fatal(err)
	}

	cmd := &cobra.Command{}
	Run(cmd, []string{"s3://" + bucket.Name() + "/tmpfile", "s3://" + bucket.Name() + "/tmpfile.copy"})

	// check body
	resp, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile.copy"),
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(body))
	}
}

func TestCP_CopyMultipart(t *testing.T) {
	// This test overwrites the global variable `maxCopyObjectBytes`.
	// So, this test must be run in parallel.
	// t.Parallel()

	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	// prepare a test object
	content := bytes.Repeat([]byte("temporary file's content"), 1024*1024)
	_, err = svc.PutObject(ctx, &s3.PutObjectInput{
		Body:   bytes.NewReader(content),
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile"),
	})
	if err != nil {
		t.Fatal(err)
	}

	original := maxCopyObjectBytes
	maxCopyObjectBytes = 5 * 1024 * 1024
	defer func() {
		maxCopyObjectBytes = original
	}()
	cmd := &cobra.Command{}
	Run(cmd, []string{"s3://" + bucket.Name() + "/tmpfile", "s3://" + bucket.Name() + "/tmpfile.copy"})

	// check body
	resp, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile.copy"),
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(body))
	}
}

func TestCP_CopyRecursive(t *testing.T) {
	// This test overwrites the global variable `recursive` and `maxCopyObjectBytes`.
	// So, this test must be run in parallel.
	// t.Parallel()

	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	// prepare test files
	content := []byte("temporary file's content")
	dir, err := os.MkdirTemp("", "s3cli-mini")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
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
	for _, key := range keys {
		_, err := svc.PutObject(ctx, &s3.PutObjectInput{
			Body:   bytes.NewReader(content),
			Bucket: aws.String(bucket.Name()),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// test
	original := maxCopyObjectBytes
	recursive = true
	defer func() {
		recursive = false
		maxCopyObjectBytes = original
	}()
	cmd := &cobra.Command{}
	Run(cmd, []string{"s3://" + bucket.Name() + "/foo", "s3://" + bucket.Name() + "/fizz"})

	// check body
	expected := []string{
		"fizz/bar/.baz/a",
		"fizz/bar/.baz/b",
		"fizz/bar/.baz/c",
		"fizz/bar/.baz/d",
		"fizz/bar/.baz/e",
		"fizz/bar/.baz/hooks/bar",
		"fizz/bar/.baz/hooks/foo",
	}
	for _, key := range expected {
		resp, err := svc.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket.Name()),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if string(body) != string(content) {
			t.Errorf("want %s, got %s", string(content), string(body))
		}
	}
}

func TestCP_CopyRecursiveMultipart(t *testing.T) {
	// This test overwrites the global variable `recursive` and `maxCopyObjectBytes`.
	// So, this test must be run in parallel.
	// t.Parallel()

	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	// prepare test objects
	content := bytes.Repeat([]byte("temporary"), 1024*1024)
	dir, err := os.MkdirTemp("", "s3cli-mini")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	keys := []string{
		"foo.zip",
		"foo/bar/.baz/a",
		"foo/bar/.baz/hooks/bar",
		"z.txt",
	}
	{
		g, ctx := errgroup.WithContext(ctx)
		for _, key := range keys {
			key := key
			g.Go(func() error {
				_, err := svc.PutObject(ctx, &s3.PutObjectInput{
					Body:   bytes.NewReader(content),
					Bucket: aws.String(bucket.Name()),
					Key:    aws.String(key),
				})
				return err
			})
		}
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	original := maxCopyObjectBytes
	maxCopyObjectBytes = 5 * 1024 * 1024
	recursive = true
	defer func() {
		maxCopyObjectBytes = original
		recursive = false
	}()
	cmd := &cobra.Command{}
	Run(cmd, []string{"s3://" + bucket.Name() + "/foo", "s3://" + bucket.Name() + "/fizz"})

	// check body
	expected := []string{
		"fizz/bar/.baz/a",
		"fizz/bar/.baz/hooks/bar",
	}
	{
		g, ctx := errgroup.WithContext(ctx)
		for _, key := range expected {
			key := key
			g.Go(func() error {
				resp, err := svc.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket.Name()),
					Key:    aws.String(key),
				})
				if err != nil {
					t.Errorf("error while getting %s: %v", key, err)
					return nil
				}
				defer resp.Body.Close()
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Errorf("error while reading %s: %v", key, err)
					return nil
				}
				if string(body) != string(content) {
					t.Errorf("want %s, got %s", string(content), string(body))
				}
				return nil
			})
		}
		g.Wait()
	}
}

func TestCP_DownloadToStdout(t *testing.T) {
	// This test overwrites the global variable `os.Stdout`.
	// So, this test must be run in parallel.
	// t.Parallel()

	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	// prepare a test object
	content := []byte("temporary file's content")
	_, err = svc.PutObject(ctx, &s3.PutObjectInput{
		Body:   bytes.NewReader(content),
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// test
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan struct {
		str string
		err error
	}, 1)
	go func() {
		data, err := io.ReadAll(r)
		ch <- struct {
			str string
			err error
		}{string(data), err}
	}()
	origStdout := os.Stdout
	defer func() { os.Stdout = origStdout }() // restore stdout
	os.Stdout = w

	cmd := &cobra.Command{}
	Run(cmd, []string{"s3://" + bucket.Name() + "/tmpfile", "-"})
	w.Close()

	got := <-ch
	if got.err != nil {
		t.Fatal(err)
	}
	if string(got.str) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(got.str))
	}
}

func TestCP_UploadFromStdin(t *testing.T) {
	// This test overwrites the global variable `os.Stdin`.
	// So, this test must be run in parallel.
	// t.Parallel()

	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := pool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Put(bucket)

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	go func() {
		io.WriteString(w, "temporary file's content")
		w.Close()
	}()
	origStdin := os.Stdin
	defer func() { os.Stdin = origStdin }() // restore stdin
	os.Stdin = r

	cmd := &cobra.Command{}
	Run(cmd, []string{"-", "s3://" + bucket.Name() + "/tmpfile"})

	// check body
	resp, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket.Name()),
		Key:    aws.String("tmpfile"),
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != "temporary file's content" {
		t.Errorf("want %s, got %s", "temporary file's content", string(body))
	}
}
