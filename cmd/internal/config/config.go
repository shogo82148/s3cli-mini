package config

import (
	"context"
	"errors"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/defaults"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3iface"
	"github.com/spf13/cobra"
)

var mu sync.Mutex
var awsConfigLoaded bool
var awsConfig aws.Config
var awsRegion string
var inTest bool

// InitFlag initializes global configure.
func InitFlag(cmd *cobra.Command) {
	flags := cmd.PersistentFlags()
	flags.StringVar(&awsRegion, "region", "", "The region to use. Overrides config/env settings.")
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

	// Load default config
	cfg, err := external.LoadDefaultAWSConfig(configs...)
	if err != nil {
		return aws.Config{}, nil
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

	mu.Lock()
	defer mu.Unlock()
	svc.ForcePathStyle = inTest
	return svc, nil
}

// SetupTest sets aws configure for tests.
func SetupTest() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := setupTestConfig(); err != nil {
		return err
	}

	// Wait until the service is ready
	endpoint := os.Getenv("S3MINI_TEST_ENDPOINT")
	for {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		req = req.WithContext(ctx)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			return nil
		}

		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func setupTestConfig() error {
	mu.Lock()
	defer mu.Unlock()

	endpoint := os.Getenv("S3MINI_TEST_ENDPOINT")
	if endpoint == "" {
		return errors.New("S3MINI_TEST_ENDPOINT is not set")
	}

	cfg := defaults.Config()
	cfg.Region = "us-east-1"
	cfg.EndpointResolver = aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		if service == "s3" {
			return aws.Endpoint{
				URL: endpoint,
			}, nil
		}
		return aws.Endpoint{}, errors.New("unknown service")
	})

	// Example Credentials
	cfg.Credentials = aws.NewStaticCredentialsProvider("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "")

	awsConfig = cfg
	awsConfigLoaded = true
	inTest = true
	return nil
}