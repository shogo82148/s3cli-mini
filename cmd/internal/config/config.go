package config

import (
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/spf13/cobra"
)

var mu sync.Mutex
var awsConfigLoaded bool
var awsConfig aws.Config
var awsRegion string

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
	return awsConfig.Copy(), nil
}
