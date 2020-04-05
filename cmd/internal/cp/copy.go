package cp

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (c *client) s3s3(src, dist string) error {
	srcBucket, srcKey := parsePath(src)
	distBucket, distKey := parsePath(dist)
	if distKey == "" || distKey[len(distKey)-1] == '/' {
		distKey += path.Base(srcKey)
	}
	c.cmd.PrintErrf("copy s3://%s/%s to s3://%s/%s\n", srcBucket, srcKey, distBucket, distKey)
	if dryrun {
		return nil
	}

	cp := &copier{
		client:     c,
		srcBucket:  srcBucket,
		srcKey:     srcKey,
		distBucket: distBucket,
		distKey:    distKey,
	}
	cp.copy()
	c.wg.Wait()
	return c.ctx.Err()
}

func (c *client) s3s3recursive(src, dist string) error {
	srcBucket, srcKey := parsePath(src)
	distBucket, distKey := parsePath(dist)
	if srcKey != "" && srcKey[len(srcKey)-1] != '/' {
		srcKey += "/"
	}

	// walk s3
	req := c.s3.ListObjectsV2Request(&s3.ListObjectsV2Input{
		Bucket: aws.String(srcBucket),
		Prefix: aws.String(srcKey),
	})
	p := s3.NewListObjectsV2Paginator(req)
	for p.Next(c.ctx) {
		page := p.CurrentPage()
		for _, obj := range page.Contents {
			key := aws.StringValue(obj.Key)
			distKey := path.Join(distKey, strings.TrimPrefix(key, srcKey))
			c.cmd.PrintErrf("copy s3://%s/%s to s3://%s/%s\n", srcBucket, key, distBucket, distKey)
			if dryrun {
				continue
			}
			cp := &copier{
				client:     c,
				srcBucket:  srcBucket,
				srcKey:     key,
				distBucket: distBucket,
				distKey:    distKey,
			}
			cp.copy()
		}
	}
	if err := p.Err(); err != nil {
		c.cancel()
		c.wg.Wait()
		return err
	}
	c.wg.Wait()
	return c.ctx.Err()
}

type copier struct {
	client              *client
	srcBucket, srcKey   string
	distBucket, distKey string
	totalSize           int64

	mu    sync.Mutex
	parts completedParts
}

func (c *copier) copy() {
	if err := c.initSize(); err != nil {
		c.setError(err)
		return
	}
	if c.totalSize <= maxCopyObjectBytes {
		// use CopyObject API for small size object
		// https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjectsUsingAPIs.html
		c.singlePartCopy()
		return
	}

	// multipart copy
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjctsMPUapi.html
	resp, err := c.client.s3.CreateMultipartUploadRequest(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(c.distBucket),
		Key:    aws.String(c.distKey),
	}).Send(c.client.ctx)
	if err != nil {
		c.setError(err)
		return
	}
	uploadID := aws.StringValue(resp.UploadId)
	size := c.totalSize
	var wg sync.WaitGroup
	for i, pos := int64(1), int64(0); pos < size; i, pos = i+1, pos+copyChunkBytes {
		i, pos := i, pos
		lastByte := pos + copyChunkBytes - 1
		if lastByte >= size {
			lastByte = size - 1
		}
		if !c.client.acquire() {
			break
		}
		wg.Add(1)
		go func() {
			defer c.client.release()
			defer wg.Done()
			c.copyChunk(uploadID, i, pos, lastByte)
		}()
	}

	// watch complete
	c.client.wg.Add(1)
	go func() {
		defer c.client.wg.Done()
		wg.Wait()
		if c.client.ctx.Err() != nil {
			// the request is aborted. clean up temporary resources.
			_, err := c.client.s3.AbortMultipartUploadRequest(&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(c.distBucket),
				Key:      aws.String(c.distKey),
				UploadId: aws.String(uploadID),
			}).Send(c.client.ctxAbort)
			if err != nil {
				c.client.cmd.PrintErrln("failed to abort multipart upload ", err)
			}
			return
		}
		sort.Sort(c.parts)
		_, err := c.client.s3.CompleteMultipartUploadRequest(&s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(c.distBucket),
			Key:      aws.String(c.distKey),
			UploadId: aws.String(uploadID),
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: c.parts,
			},
		}).Send(c.client.ctxAbort)
		if err != nil {
			c.setError(err)
		}
	}()
}

func (c *copier) initSize() error {
	resp, err := c.client.s3.HeadObjectRequest(&s3.HeadObjectInput{
		Bucket: aws.String(c.srcBucket),
		Key:    aws.String(c.srcKey),
	}).Send(c.client.ctx)
	if err != nil {
		return err
	}
	c.totalSize = aws.Int64Value(resp.ContentLength)
	return nil
}

func (c *copier) singlePartCopy() {
	if !c.client.acquire() {
		return
	}
	go func() {
		defer c.client.release()
		_, err := c.client.s3.CopyObjectRequest(&s3.CopyObjectInput{
			Bucket:     aws.String(c.distBucket),
			Key:        aws.String(c.distKey),
			CopySource: aws.String(c.srcBucket + "/" + c.srcKey),
		}).Send(c.client.ctx)
		if err != nil {
			c.setError(err)
		}
	}()
}

func (c *copier) copyChunk(uploadID string, num, pos, lastByte int64) {
	resp, err := c.client.s3.UploadPartCopyRequest(&s3.UploadPartCopyInput{
		Bucket:          aws.String(c.distBucket),
		Key:             aws.String(c.distKey),
		CopySource:      aws.String(c.srcBucket + "/" + c.srcKey),
		CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", pos, lastByte)),
		UploadId:        aws.String(uploadID),
		PartNumber:      aws.Int64(num),
	}).Send(c.client.ctx)
	if err != nil {
		c.setError(err)
		return
	}
	part := s3.CompletedPart{ETag: resp.CopyPartResult.ETag, PartNumber: aws.Int64(num)}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.parts = append(c.parts, part)
}

func (c *copier) setError(err error) {
	select {
	case <-c.client.ctx.Done():
		return
	default:
	}
	c.client.cancel()
	c.client.cmd.PrintErrln(err)
}
