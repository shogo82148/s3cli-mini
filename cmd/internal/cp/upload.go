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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/shogo82148/s3cli-mini/internal/fastwalk"
)

func (c *client) stdins3(bucket, key string) error {
	if dryrun {
		c.cmd.PrintErrf("upload STDIN to s3://%s/%s\n", bucket, key)
		return nil
	}
	u := &uploader{
		client: c,
		body:   os.Stdin,
		bucket: bucket,
		key:    key,
	}
	u.upload()
	c.wg.Wait()
	return c.ctx.Err()
}

func (c *client) locals3(src, dist string) error {
	bucket, key := parsePath(dist)
	if key == "" || key[len(key)-1] == '/' {
		key += filepath.Base(src)
	}
	if src == srcStdin {
		return c.stdins3(bucket, key)
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
	parts []*types.CompletedPart
}

type completedPartsByPartNumber []*types.CompletedPart

func (a completedPartsByPartNumber) Len() int      { return len(a) }
func (a completedPartsByPartNumber) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a completedPartsByPartNumber) Less(i, j int) bool {
	return aws.ToInt32(a[i].PartNumber) < aws.ToInt32(a[j].PartNumber)
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
	resp, err := u.client.s3.CreateMultipartUpload(u.client.ctx, &s3.CreateMultipartUploadInput{
		Bucket:             aws.String(u.bucket),
		Key:                aws.String(u.key),
		ACL:                u.client.acl,
		ContentType:        getContentType(u.key),
		CacheControl:       nullableString(cacheControl),
		ContentDisposition: nullableString(contentDisposition),
		ContentEncoding:    nullableString(contentEncoding),
		ContentLanguage:    nullableString(contentLanguage),
		Expires:            u.client.expires,
	})
	if err != nil {
		u.setError(err)
		return
	}
	uploadID := aws.ToString(resp.UploadId)

	var wg sync.WaitGroup
	num := int32(1)
	for {
		if !u.client.acquire() {
			break
		}
		wg.Add(1)
		go func(uploadID string, num int32, r io.ReadSeeker) {
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
			_, err := u.client.s3.AbortMultipartUpload(u.client.ctxAbort, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(u.bucket),
				Key:      aws.String(u.key),
				UploadId: aws.String(uploadID),
			})
			if err != nil {
				u.client.cmd.PrintErrln("failed to abort multipart upload ", err)
			}
			return
		}
		sort.Sort(completedPartsByPartNumber(u.parts))
		_, err := u.client.s3.CompleteMultipartUpload(u.client.ctxAbort, &s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(u.bucket),
			Key:             aws.String(u.key),
			UploadId:        aws.String(uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{Parts: u.parts},
		})
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
	if u.totalSize >= 0 {
		switch r := u.body.(type) {
		case io.ReaderAt:
			var err error
			n := int64(copyChunkBytes)
			if remain := u.totalSize - u.readerPos; remain <= n {
				n = remain
				err = io.EOF
			}
			reader := io.NewSectionReader(r, u.readerPos, n)
			u.readerPos += n
			return reader, n, err
		}
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
		_, err := u.client.s3.PutObject(u.client.ctx, input)
		if err != nil {
			u.setError(err)
		}
	}()
}

func (u *uploader) uploadChunk(uploadID string, num int32, r io.ReadSeeker) {
	resp, err := u.client.s3.UploadPart(u.client.ctx, &s3.UploadPartInput{
		Bucket:     aws.String(u.bucket),
		Key:        aws.String(u.key),
		Body:       r,
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(num),
	})
	if err != nil {
		u.setError(err)
		return
	}
	part := &types.CompletedPart{ETag: resp.ETag, PartNumber: aws.Int32(num)}
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
