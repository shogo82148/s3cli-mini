package ls

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3iface"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/spf13/cobra"
)

// Run runs mb command.
func Run(cmd *cobra.Command, args []string) {
	svc, err := config.NewS3Client()
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
	if len(args) == 0 {
		listBuckets(cmd, svc)
	}
}

func listBuckets(cmd *cobra.Command, svc s3iface.ClientAPI) {
	out := cmd.OutOrStdout()
	resp, err := svc.ListBucketsRequest(&s3.ListBucketsInput{}).Send(context.Background())
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
	for _, b := range resp.ListBucketsOutput.Buckets {
		creationDate := aws.TimeValue(b.CreationDate).In(time.Local)
		fmt.Fprintf(out, "%s %s\n", creationDate.Format("2006-01-02 15:04:05"), aws.StringValue(b.Name))
	}
}
