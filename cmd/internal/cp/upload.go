package cp

import (
	"bytes"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shogo82148/s3cli-mini/internal/fastwalk"
)

func (c *client) locals3(src, dist string) error {
	bucket, key := parsePath(dist)
	if key == "" || key[len(key)-1] == '/' {
		key += filepath.Base(src)
	}
	if dryrun {
		c.cmd.PrintErrf("Upload %s to s3://%s/%s\n", src, bucket, key)
		return nil
	}

	f, err := os.Open(src)
	if err != nil {
		return err
	}

	u := &uploader{
		client: c,
		body:   f,
		bucket: bucket,
		key:    key,
	}
	c.cmd.PrintErrf("Upload %s to s3://%s/%s\n", src, bucket, key)
	u.upload()
	c.wg.Wait()
	return c.ctx.Err()
}

func (c *client) locals3recursive(src, dist string) error {
	err := fastwalk.Walk(src, func(p string, typ os.FileMode) error {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}
		if typ.IsDir() {
			if typ == os.ModeSymlink && c.followSymlinks {
				return fastwalk.TraverseLink
			}
			return nil
		}

		bucket, key := parsePath(dist)
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		key = path.Join(key, filepath.ToSlash(rel))
		if dryrun {
			c.cmd.PrintErrf("Upload %s to s3://%s/%s\n", src, bucket, key)
			return nil
		}

		f, err := os.Open(p)
		if err != nil {
			return err
		}

		u := &uploader{
			client: c,
			body:   f,
			bucket: bucket,
			key:    key,
		}
		c.cmd.PrintErrf("Upload %s to s3://%s/%s\n", src, bucket, key)
		u.upload()
		return nil
	})
	c.wg.Wait()
	return err
}

type uploader struct {
	client    *client
	body      io.ReadCloser
	bucket    string
	key       string
	readerPos int64
	totalSize int64

	mu    sync.Mutex
	parts completedParts
}

type completedParts []s3.CompletedPart

func (a completedParts) Len() int      { return len(a) }
func (a completedParts) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a completedParts) Less(i, j int) bool {
	return aws.Int64Value(a[i].PartNumber) < aws.Int64Value(a[j].PartNumber)
}

func (u *uploader) upload() {
	u.initSize()
	r, _, err := u.nextReader()
	if err == io.EOF {
		u.singlePartUpload(r)
		return
	}
	if err != nil {
		u.setError(err)
		return
	}

	// start multipart upload
	resp, err := u.client.s3.CreateMultipartUploadRequest(&s3.CreateMultipartUploadInput{
		Bucket:             aws.String(u.bucket),
		Key:                aws.String(u.key),
		ACL:                u.client.acl,
		ContentType:        getContentType(u.key),
		CacheControl:       nullableString(cacheControl),
		ContentDisposition: nullableString(contentDisposition),
		ContentEncoding:    nullableString(contentEncoding),
		ContentLanguage:    nullableString(contentLanguage),
		Expires:            u.client.expires,
	}).Send(u.client.ctx)
	if err != nil {
		u.setError(err)
		return
	}
	uploadID := aws.StringValue(resp.UploadId)

	var wg sync.WaitGroup
	num := int64(1)
	for {
		if !u.client.acquire() {
			break
		}
		wg.Add(1)
		go func(uploadID string, num int64, r io.ReadSeeker) {
			defer u.client.release()
			defer wg.Done()
			u.uploadChunk(uploadID, num, r)
		}(uploadID, num, r)
		if err == io.EOF {
			break
		}
		num++

		var n int64
		r, n, err = u.nextReader()
		if n == 0 {
			break
		}
	}

	// complete
	u.client.wg.Add(1)
	go func() {
		defer u.client.wg.Done()
		wg.Wait()
		u.body.Close()
		if u.client.ctx.Err() != nil {
			// the request is aborted
			_, err := u.client.s3.AbortMultipartUploadRequest(&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(u.bucket),
				Key:      aws.String(u.key),
				UploadId: aws.String(uploadID),
			}).Send(u.client.ctxAbort)
			if err != nil {
				u.client.cmd.PrintErrln("failed to abort multipart upload ", err)
			}
			return
		}
		sort.Sort(u.parts)
		_, err := u.client.s3.CompleteMultipartUploadRequest(&s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(u.bucket),
			Key:             aws.String(u.key),
			UploadId:        aws.String(uploadID),
			MultipartUpload: &s3.CompletedMultipartUpload{Parts: u.parts},
		}).Send(u.client.ctxAbort)
		if err != nil {
			u.setError(err)
		}
	}()
}

func (u *uploader) initSize() {
	u.totalSize = -1
	switch body := u.body.(type) {
	case interface{ Stat() (os.FileInfo, error) }:
		info, err := body.Stat()
		if err != nil {
			return
		}
		if !info.Mode().IsRegular() {
			// non-regular file, Size is system-dependent.
			return
		}
		u.totalSize = info.Size()
	case interface{ Len() int }:
		u.totalSize = int64(body.Len())
	case io.Seeker:
		current, err := body.Seek(0, io.SeekCurrent)
		if err != nil {
			return
		}
		end, err := body.Seek(0, io.SeekEnd)
		if err != nil {
			return
		}
		_, err = body.Seek(current, io.SeekStart)
		if err != nil {
			return
		}
		u.totalSize = end - current
	}
}

func (u *uploader) nextReader() (io.ReadSeeker, int64, error) {
	switch r := u.body.(type) {
	case io.ReaderAt:
		var err error
		n := int64(copyChunkBytes)
		if u.totalSize >= 0 {
			remain := u.totalSize - u.readerPos
			if remain <= n {
				n = remain
				err = io.EOF
			}
		}
		reader := io.NewSectionReader(r, u.readerPos, n)
		return reader, n, err
	}

	var buf bytes.Buffer
	chunk := &io.LimitedReader{
		R: u.body,
		N: copyChunkBytes,
	}
	n, err := buf.ReadFrom(chunk)
	u.readerPos += n
	return bytes.NewReader(buf.Bytes()), n, err
}

func (u *uploader) singlePartUpload(r io.ReadSeeker) {
	if !u.client.acquire() {
		return
	}
	go func() {
		defer u.client.release()
		defer u.body.Close()
		input := &s3.PutObjectInput{
			Body:               r,
			Bucket:             aws.String(u.bucket),
			Key:                aws.String(u.key),
			ACL:                u.client.acl,
			ContentType:        getContentType(u.key),
			CacheControl:       nullableString(cacheControl),
			ContentDisposition: nullableString(contentDisposition),
			ContentEncoding:    nullableString(contentEncoding),
			ContentLanguage:    nullableString(contentLanguage),
			Expires:            u.client.expires,
		}
		_, err := u.client.s3.PutObjectRequest(input).Send(u.client.ctx)
		if err != nil {
			u.setError(err)
		}
	}()
}

func (u *uploader) uploadChunk(uploadID string, num int64, r io.ReadSeeker) {
	resp, err := u.client.s3.UploadPartRequest(&s3.UploadPartInput{
		Bucket:     aws.String(u.bucket),
		Key:        aws.String(u.key),
		Body:       r,
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int64(num),
	}).Send(u.client.ctx)
	if err != nil {
		u.setError(err)
		return
	}
	part := s3.CompletedPart{ETag: resp.ETag, PartNumber: aws.Int64(num)}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.parts = append(u.parts, part)
}

func (u *uploader) setError(err error) {
	select {
	case <-u.client.ctx.Done():
		return
	default:
	}
	u.client.cancel()
	u.client.cmd.PrintErrln(err)
}
