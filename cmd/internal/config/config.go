package config

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/shogo82148/s3cli-mini/cmd/internal/interfaces"
	"github.com/spf13/cobra"
)

var mu sync.Mutex
var debug bool
var awsConfigLoaded bool
var awsConfig aws.Config
var awsRegion string
var awsProfile string
var endpointURL string

// InitFlag initializes global configure.
func InitFlag(cmd *cobra.Command) {
	flags := cmd.PersistentFlags()
	flags.BoolVar(&debug, "debug", false, "Turn on debug logging.")
	flags.StringVar(&awsRegion, "region", "", "The region to use. Overrides config/env settings.")
	flags.StringVar(&awsProfile, "profile", "", "Use a specific profile from your credential file.")
	flags.StringVar(&endpointURL, "endpoint-url", "", "Overrides command's default URL with the given URL.")
}

// LoadAWSConfig returns aws.Config.
func LoadAWSConfig(ctx context.Context) (aws.Config, error) {
	mu.Lock()
	defer mu.Unlock()

	if awsConfigLoaded {
		return awsConfig.Copy(), nil
	}

	opts := []func(*config.LoadOptions) error{}

	if awsRegion != "" {
		opts = append(opts, config.WithRegion(awsRegion))
	}
	if awsProfile != "" {
		opts = append(opts, config.WithSharedConfigProfile(awsProfile))
	}

	// Load default config
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, err
	}

	if endpointURL != "" {
		cfg.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: endpointURL,
			}, nil
		})
	}
	if debug {
		cfg.ClientLogMode |= aws.LogSigning | aws.LogRetries | aws.LogRequest | aws.LogRequestWithBody | aws.LogResponse | aws.LogResponseWithBody
	}

	awsConfig = cfg
	awsConfigLoaded = true
	return awsConfig.Copy(), nil
}

// NewS3Client returns new S3 client.
func NewS3Client(ctx context.Context) (interfaces.S3Client, error) {
	cfg, err := LoadAWSConfig(ctx)
	if err != nil {
		return nil, err
	}
	svc := s3.NewFromConfig(cfg)
	return svc, nil
}

// NewS3ServiceClient returns new S3 client that is used for getting s3 service level operation, such as ListBucket
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html
func NewS3ServiceClient(ctx context.Context) (interfaces.S3Client, error) {
	cfg, err := LoadAWSConfig(ctx)
	if err != nil {
		return nil, err
	}
	if cfg.Region == "" {
		// fall back to US East (N. Virginia)
		cfg.Region = "us-east-1"
	}
	svc := s3.NewFromConfig(cfg)
	return svc, nil
}

// NewS3BucketClient returns new S3 client that is used for the bucket.
func NewS3BucketClient(ctx context.Context, bucket string) (interfaces.S3Client, error) {
	cfg, err := LoadAWSConfig(ctx)
	if err != nil {
		return nil, err
	}
	svc := s3.NewFromConfig(cfg)
	region, err := getBucketRegion(ctx, svc, bucket)
	if err != nil {
		return nil, err
	}
	cfg.Region = region
	svc = s3.NewFromConfig(cfg)
	return svc, nil
}

const bucketRegionHeader = "X-Amz-Bucket-Region"

// getBucketRegion attempts to get the region for a bucket by calling HeadBucket.
// It uses a deserialize middleware to capture the X-Amz-Bucket-Region response header.
func getBucketRegion(ctx context.Context, svc *s3.Client, bucket string) (string, error) {
	var regionCapture deserializeBucketRegion

	_, err := svc.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, regionCapture.RegisterMiddleware)
		// Use anonymous credentials so the request succeeds even if the
		// current credentials do not have permissions for the bucket, while
		// S3 still returns the X-Amz-Bucket-Region header in the response.
		o.Credentials = nil
	})
	if len(regionCapture.BucketRegion) == 0 && err != nil {
		var httpStatusErr interface {
			HTTPStatusCode() int
		}
		if !errors.As(err, &httpStatusErr) {
			return "", err
		}
		if httpStatusErr.HTTPStatusCode() == http.StatusNotFound {
			return "", fmt.Errorf("bucket not found: %s", bucket)
		}
		return "", err
	}

	return regionCapture.BucketRegion, nil
}

type deserializeBucketRegion struct {
	BucketRegion string
}

func (d *deserializeBucketRegion) RegisterMiddleware(stack *middleware.Stack) error {
	return stack.Deserialize.Add(d, middleware.After)
}

func (d *deserializeBucketRegion) ID() string {
	return "DeserializeBucketRegion"
}

func (d *deserializeBucketRegion) HandleDeserialize(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (
	out middleware.DeserializeOutput, metadata middleware.Metadata, err error,
) {
	out, metadata, err = next.HandleDeserialize(ctx, in)
	if err != nil {
		return out, metadata, err
	}

	resp, ok := out.RawResponse.(*smithyhttp.Response)
	if !ok {
		return out, metadata, fmt.Errorf("unknown transport type %T", out.RawResponse)
	}

	d.BucketRegion = resp.Header.Get(bucketRegionHeader)

	return out, metadata, err
}
