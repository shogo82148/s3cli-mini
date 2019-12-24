package cp

import (
	"context"
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
	Abort(ctx context.Context) error
}

type uploader struct {
	ctx      context.Context
	s3       s3iface.ClientAPI
	task     chan task
	wg       sync.WaitGroup
	parallel int
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

	input := &s3.PutObjectInput{
		Body:   f,
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	log.Printf("upload: %s to s3://%s/%s", f.Name(), bucket, key)
	u.task <- &singleUploadTask{
		s3:    u.s3,
		input: input,
	}

	close(u.task)
	u.wg.Wait()
	return nil
}

func (u *uploader) sendTask() {
	defer u.wg.Done()
	for t := range u.task {
		if err := t.Send(u.ctx); err != nil {
			t.Abort(u.ctx)
		}
	}
}

type singleUploadTask struct {
	s3     s3iface.ClientAPI
	input  *s3.PutObjectInput
	cancel context.CancelFunc
}

func (t *singleUploadTask) Send(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	t.cancel = cancel
	_, err := t.s3.PutObjectRequest(t.input).Send(ctx)
	return err
}

func (t *singleUploadTask) Abort(ctx context.Context) error {
	if t.cancel != nil {
		t.cancel()
	}
	return nil
}

type multiUploadTask struct {
	s3     s3iface.ClientAPI
	input  *s3.UploadPartInput
	cancel context.CancelFunc
}

func (t *multiUploadTask) Send(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	t.cancel = cancel
	_, err := t.s3.UploadPartRequest(t.input).Send(ctx)
	return err
}

func (t *multiUploadTask) Abort(ctx context.Context) error {
	if t.cancel != nil {
		t.cancel()
	}
	return nil
}
