package cp

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3iface"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/spf13/cobra"
)

const bucketName = "bucket-for-test"

func prepareEmptyBucket(ctx context.Context, svc s3iface.ClientAPI) (cleanup func(), err error) {
	// prepare a bucket for test
	_, err = svc.CreateBucketRequest(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}).Send(ctx)
	if err != nil {
		return
	}

	cleanup = func() {
		// clean up
		svc.DeleteBucketRequest(&s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		}).Send(ctx)
	}
	return
}

func TestCP_Upload(t *testing.T) {
	if err := config.SetupTest(t); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := prepareEmptyBucket(ctx, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

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

	// cleanup
	svc.DeleteObjectRequest(&s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile"),
	}).Send(ctx)
}

func TestCP_Upload_KeyOmitted(t *testing.T) {
	if err := config.SetupTest(t); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := prepareEmptyBucket(ctx, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

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

	// cleanup
	svc.DeleteObjectRequest(&s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile"),
	}).Send(ctx)
}

func TestCP_UploadPublicACL(t *testing.T) {
	if err := config.SetupTest(t); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := config.NewS3Client()
	if err != nil {
		t.Fatal(err)
	}
	cleanup, err := prepareEmptyBucket(ctx, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

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

	// cleanup
	svc.DeleteObjectRequest(&s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("tmpfile"),
	}).Send(ctx)
}
