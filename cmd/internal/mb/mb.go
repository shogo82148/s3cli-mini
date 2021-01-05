package mb

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/spf13/cobra"
)

// Run runs mb command.
func Run(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(args) != 1 {
		log.Println("bucket name is missing.")
		if err := cmd.Help(); err != nil {
			log.Fatal(err)
		}
		return
	}

	bucketName := strings.TrimPrefix(args[0], "s3://")

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		log.Fatal(err)
	}

	_, err = svc.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("make_bucket: s3://%s\n", bucketName)
}
