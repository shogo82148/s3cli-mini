package mb

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/spf13/cobra"
)

var bucketNamespace string

// Init initializes mb command.
func Init(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.StringVar(&bucketNamespace, "bucket-namespace", "global", "Specify the namespace for the bucket")
}

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

	var namespace types.BucketNamespace
	switch bucketNamespace {
	case "global":
		namespace = types.BucketNamespaceGlobal
	case "account-regional":
		namespace = types.BucketNamespaceAccountRegional
	default:
		log.Fatalf("invalid bucket namespace: %s, valid values are 'global' or 'account-regional'", bucketNamespace)
	}

	bucketName := strings.TrimPrefix(args[0], "s3://")

	cfg, err := config.LoadAWSConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}
	region := cfg.Region

	svc, err := config.NewS3Client(ctx)
	if err != nil {
		log.Fatal(err)
	}

	input := &s3.CreateBucketInput{
		Bucket:          aws.String(bucketName),
		BucketNamespace: namespace,
	}
	if region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}
	_, err = svc.CreateBucket(ctx, input)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("make_bucket: s3://%s\n", bucketName)
}
