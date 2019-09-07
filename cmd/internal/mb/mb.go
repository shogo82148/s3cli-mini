package mb

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
)

// Run runs cp command.
func Run(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		log.Println("bucket name is missing.")
		if err := cmd.Help(); err != nil {
			log.Fatal(err)
		}
		return
	}

	bucketName := strings.TrimPrefix(args[0], "s3://")

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		log.Fatal(err)
	}
	svc := s3.New(cfg)

	req := svc.CreateBucketRequest(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	_, err = req.Send(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("make_bucket: s3://%s\n", bucketName)
}
