package interfaces

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
)

// UploaderClient foobar
type UploaderClient interface{}

// DownloaderClient foobar
type DownloaderClient interface {
	DownloadObject(ctx context.Context, input *transfermanager.DownloadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error)
}
