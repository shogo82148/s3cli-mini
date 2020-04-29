package cp

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/shogo82148/s3cli-mini/cmd/internal/testutils"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func TestCP_Upload(t *testing.T) {
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// test
	cmd := &cobra.Command{}
	Run(cmd, []string{filename, "s3://" + bucketName + "/tmpfile.html"})

	resp, err := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile.html"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// check body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(body))
	}
	if aws.StringValue(resp.ContentType) != "text/html; charset=utf-8" {
		t.Errorf("unexpected content-type: want %s, got %s", "text/html; charset-utf-8", aws.StringValue(resp.ContentType))
	}

	// check acl
	retACL, err := svc.GetObjectAclRequest(&s3.GetObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile.html"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range retACL.Grants {
		if g.Grantee.Type != s3.TypeCanonicalUser {
			t.Errorf("unexpected grantee type, want %s, got %s", s3.TypeCanonicalUser, g.Grantee.Type)
		}
	}
}

func TestCP_Upload_Multipart(t *testing.T) {
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	content := bytes.Repeat([]byte("temporary file's content"), 1024*1024)
	dir, err := ioutil.TempDir("", "s3cli-mini")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	filename := filepath.Join(dir, "tmpfile")
	if err := ioutil.WriteFile(filename, content, 0666); err != nil {
		t.Fatal(err)
	}

	// test
	cmd := &cobra.Command{}
	Run(cmd, []string{filename, "s3://" + bucketName + "/tmpfile.html"})

	resp, err := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile.html"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// check body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(body))
	}
	if aws.StringValue(resp.ContentType) != "text/html; charset=utf-8" {
		t.Errorf("unexpected content-type: want %s, got %s", "text/html; charset-utf-8", aws.StringValue(resp.ContentType))
	}

	// check acl
	retACL, err := svc.GetObjectAclRequest(&s3.GetObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile.html"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range retACL.Grants {
		if g.Grantee.Type != s3.TypeCanonicalUser {
			t.Errorf("unexpected grantee type, want %s, got %s", s3.TypeCanonicalUser, g.Grantee.Type)
		}
	}
}

func TestCP_Upload_KeyOmitted(t *testing.T) {
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// test
	cmd := &cobra.Command{}
	Run(cmd, []string{filename, "s3://" + bucketName})

	resp, err := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(body))
	}
}

func TestCP_UploadPublicACL(t *testing.T) {
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// test
	acl = "public-read"
	defer func() {
		acl = ""
	}()
	cmd := &cobra.Command{}
	Run(cmd, []string{filename, "s3://" + bucketName + "/tmpfile"})

	resp, err := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// check body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(body))
	}

	// check acl
	retACL, err := svc.GetObjectAclRequest(&s3.GetObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var publicRead bool
	for _, g := range retACL.Grants {
		publicRead = publicRead ||
			(g.Grantee.Type == s3.TypeGroup &&
				aws.StringValue(g.Grantee.URI) == "http://acs.amazonaws.com/groups/global/AllUsers" &&
				g.Permission == s3.PermissionRead)
	}
	if !publicRead {
		t.Error("unexpected acl: want public-read, but not")
	}
}

func TestCP_Upload_recursive(t *testing.T) {
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// prepare test files
	content := []byte("temporary file's content")
	dir, err := ioutil.TempDir("", "s3cli-mini")
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
		if err := ioutil.WriteFile(filename, content, 0666); err != nil {
			t.Fatal(err)
		}
	}

	// test
	recursive = true
	defer func() {
		recursive = false
	}()
	cmd := &cobra.Command{}
	Run(cmd, []string{dir, "s3://" + bucketName})

	// check body
	for _, key := range keys {
		resp, err := svc.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}).Send(ctx)
		if err != nil {
			t.Fatal(err)
		}
		body, err := ioutil.ReadAll(resp.Body)
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
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// prepare a test object
	content := []byte("temporary file's content")
	_, err = svc.PutObjectRequest(&s3.PutObjectInput{
		Body:   bytes.NewReader(content),
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// test
	dir, err := ioutil.TempDir("", "s3cli-mini")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	filename := filepath.Join(dir, "tmpfile")
	cmd := &cobra.Command{}
	Run(cmd, []string{"s3://" + bucketName + "/tmpfile", filename})

	got, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(got))
	}
}

func TestCP_DownloadRecursive(t *testing.T) {
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// prepare a test object
	content := []byte("temporary file's content")
	dir, err := ioutil.TempDir("", "s3cli-mini")
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
		_, err = svc.PutObjectRequest(&s3.PutObjectInput{
			Body:   bytes.NewReader(content),
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}).Send(ctx)
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
	Run(cmd, []string{"s3://" + bucketName + "/", dir})

	for _, key := range keys {
		filename := filepath.Join(dir, filepath.FromSlash(key))
		data, err := ioutil.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != string(content) {
			t.Errorf("key %s: want %s, got %s", key, string(data), string(key))
		}
	}
}

func TestCP_Copy(t *testing.T) {
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// prepare a test object
	content := []byte("temporary file's content")
	_, err = svc.PutObjectRequest(&s3.PutObjectInput{
		Body:   bytes.NewReader(content),
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cmd := &cobra.Command{}
	Run(cmd, []string{"s3://" + bucketName + "/tmpfile", "s3://" + bucketName + "/tmpfile.copy"})

	// check body
	resp, err := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile.copy"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(body))
	}
}

func TestCP_CopyMultipart(t *testing.T) {
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// prepare a test object
	content := bytes.Repeat([]byte("temporary file's content"), 1024*1024)
	_, err = svc.PutObjectRequest(&s3.PutObjectInput{
		Body:   bytes.NewReader(content),
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}

	original := maxCopyObjectBytes
	maxCopyObjectBytes = 5 * 1024 * 1024
	defer func() {
		maxCopyObjectBytes = original
	}()
	cmd := &cobra.Command{}
	Run(cmd, []string{"s3://" + bucketName + "/tmpfile", "s3://" + bucketName + "/tmpfile.copy"})

	// check body
	resp, err := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile.copy"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(body))
	}
}

func TestCP_CopyRecursive(t *testing.T) {
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// prepare test files
	content := []byte("temporary file's content")
	dir, err := ioutil.TempDir("", "s3cli-mini")
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
		_, err := svc.PutObjectRequest(&s3.PutObjectInput{
			Body:   bytes.NewReader(content),
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}).Send(ctx)
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
	Run(cmd, []string{"s3://" + bucketName + "/foo", "s3://" + bucketName + "/fizz"})

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
		resp, err := svc.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}).Send(ctx)
		if err != nil {
			t.Fatal(err)
		}
		body, err := ioutil.ReadAll(resp.Body)
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
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// prepare test objects
	content := bytes.Repeat([]byte("temporary"), 1024*1024)
	dir, err := ioutil.TempDir("", "s3cli-mini")
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
				_, err := svc.PutObjectRequest(&s3.PutObjectInput{
					Body:   bytes.NewReader(content),
					Bucket: aws.String(bucketName),
					Key:    aws.String(key),
				}).Send(ctx)
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
	Run(cmd, []string{"s3://" + bucketName + "/foo", "s3://" + bucketName + "/fizz"})

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
				resp, err := svc.GetObjectRequest(&s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(key),
				}).Send(ctx)
				if err != nil {
					t.Errorf("error while getting %s: %v", key, err)
					return nil
				}
				defer resp.Body.Close()
				body, err := ioutil.ReadAll(resp.Body)
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

func TestCP_DownloadStdout(t *testing.T) {
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// prepare a test object
	content := []byte("temporary file's content")
	_, err = svc.PutObjectRequest(&s3.PutObjectInput{
		Body:   bytes.NewReader(content),
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile"),
	}).Send(ctx)
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
		data, err := ioutil.ReadAll(r)
		ch <- struct {
			str string
			err error
		}{string(data), err}
	}()
	origStdout := os.Stdout
	defer func() { os.Stdout = origStdout }() // restore stdout
	os.Stdout = w

	cmd := &cobra.Command{}
	Run(cmd, []string{"s3://" + bucketName + "/tmpfile", "-"})
	w.Close()

	got := <-ch
	if got.err != nil {
		t.Fatal(err)
	}
	if string(got.str) != string(content) {
		t.Errorf("want %s, got %s", string(content), string(got.str))
	}
}

func TestCP_UploadStdin(t *testing.T) {
	testutils.SkipIfUnitTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	Run(cmd, []string{"-", "s3://" + bucketName + "/tmpfile"})

	// check body
	resp, err := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile"),
	}).Send(ctx)
	if err != nil {
		t.Fatal(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(body) != "temporary file's content" {
		t.Errorf("want %s, got %s", "temporary file's content", string(body))
	}
}
