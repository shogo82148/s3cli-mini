package config

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3iface"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
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
	flags.StringVar(&endpointURL, "endpoint-url", "", "UOverride command's default URL with the given URL.")
}

// LoadAWSConfig returns aws.Config.
func LoadAWSConfig() (aws.Config, error) {
	mu.Lock()
	defer mu.Unlock()

	if awsConfigLoaded {
		return awsConfig.Copy(), nil
	}

	configs := []external.Config{}

	if awsRegion != "" {
		configs = append(configs, external.WithRegion(awsRegion))
	}
	if awsProfile != "" {
		configs = append(configs, external.WithSharedConfigProfile(awsProfile))
	}

	// Load default config
	cfg, err := external.LoadDefaultAWSConfig(configs...)
	if err != nil {
		return aws.Config{}, err
	}

	if endpointURL != "" {
		cfg.EndpointResolver = aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: endpointURL,
			}, nil
		})
	}
	if debug {
		cfg.LogLevel = aws.LogDebug
	}

	awsConfig = cfg
	awsConfigLoaded = true
	return awsConfig.Copy(), nil
}

// NewS3Client returns new S3 client.
func NewS3Client() (s3iface.ClientAPI, error) {
	cfg, err := LoadAWSConfig()
	if err != nil {
		return nil, err
	}
	svc := s3.New(cfg)
	return svc, nil
}

// NewS3ServiceClient returns new S3 client that is used for getting s3 service level operation, such as ListBucket
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html
func NewS3ServiceClient() (s3iface.ClientAPI, error) {
	cfg, err := LoadAWSConfig()
	if err != nil {
		return nil, err
	}
	if cfg.Region == "" {
		// fall back to US East (N. Virginia)
		cfg.Region = "us-east-1"
	}
	svc := s3.New(cfg)
	return svc, nil
}

// NewS3BucketClient returns new S3 client that is used for the bucket.
func NewS3BucketClient(ctx context.Context, bucket string) (s3iface.ClientAPI, error) {
	cfg, err := LoadAWSConfig()
	if err != nil {
		return nil, err
	}
	regionHint := cfg.Region
	if regionHint == "" {
		// fall back to US East (N. Virginia)
		regionHint = "us-east-1"
	}
	region, err := s3manager.GetBucketRegion(ctx, cfg, bucket, regionHint)
	if err != nil {
		return nil, err
	}
	cfg.Region = region
	svc := s3.New(cfg)
	return svc, nil
}
