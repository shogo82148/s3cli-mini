package cp

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3iface"
)

type task interface {
	Send(ctx context.Context) error
}

type uploader struct {
	ctx      context.Context
	cancel   context.CancelFunc
	s3       s3iface.ClientAPI
	task     chan task
	wg       sync.WaitGroup
	parallel int
	partSize int64
}

func (u *uploader) upload(src, dist string) error {
	u.wg.Add(u.parallel)
	for i := 0; i < u.parallel; i++ {
		go u.sendTask()
	}

	bucket, key := parsePath(dist)
	if key == "" || key[len(key)-1] == '/' {
		key += filepath.Base(src)
	}
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	log.Printf("upload: %s to s3://%s/%s", f.Name(), bucket, key)
	if size := stat.Size(); size < u.partSize {
		// single part upload
		input := &s3.PutObjectInput{
			Body:          f,
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			ContentLength: aws.Int64(size),
		}
		u.task <- &singleUploadTask{
			uploader: u,
			input:    input,
			cancel:   u.cancel,
		}
	} else {
		// multipart upload
		resp, err := u.s3.CreateMultipartUploadRequest(&s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}).Send(u.ctx)
		if err != nil {
			return err
		}
		uploadID := resp.UploadId
		uploadContext := &multiportUploadContext{
			Context:  u.ctx,
			cancel:   u.cancel,
			uploader: u,
			uploadID: uploadID,
		}
		for i, pos := int64(1), int64(0); pos < size; i, pos = i+1, pos+u.partSize {
			n := u.partSize
			if bytesLeft := size - pos; bytesLeft < n {
				n = bytesLeft
			}
			input := &s3.UploadPartInput{
				PartNumber: aws.Int64(i),
				Body:       io.NewSectionReader(f, pos, n),
				Bucket:     aws.String(bucket),
				Key:        aws.String(key),
				UploadId:   uploadID,
			}
			uploadContext.wg.Add(1)
			u.task <- &multipartUploadTask{
				ctx:   uploadContext,
				input: input,
			}
		}
		u.wg.Add(1)
		go func() {
			defer u.wg.Done()
			uploadContext.wait()
		}()
	}

	close(u.task)
	u.wg.Wait()
	return nil
}

func (u *uploader) sendTask() {
	defer u.wg.Done()
	for t := range u.task {
		if err := t.Send(u.ctx); err != nil {
			u.cancel()
		}
	}
}

func (u *uploader) abort(ctx context.Context) {
	u.cancel()
}

type singleUploadTask struct {
	uploader *uploader
	input    *s3.PutObjectInput
	cancel   context.CancelFunc
}

func (t *singleUploadTask) Send(ctx context.Context) error {
	_, err := t.uploader.s3.PutObjectRequest(t.input).Send(ctx)
	return err
}

type multiportUploadContext struct {
	context.Context
	cancel   context.CancelFunc
	uploader *uploader
	wg       sync.WaitGroup
	uploadID *string
}

func (ctx *multiportUploadContext) wait() {
	ctx.wg.Wait()
}

type multipartUploadTask struct {
	ctx   *multiportUploadContext
	input *s3.UploadPartInput
}

func (t *multipartUploadTask) Send(ctx context.Context) error {
	defer t.ctx.wg.Done()
	if ctx.Err() != nil {
		return nil
	}
	log.Println("uploading part: ", *t.input.PartNumber)
	_, err := t.ctx.uploader.s3.UploadPartRequest(t.input).Send(ctx)
	return err
}
