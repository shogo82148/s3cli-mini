package ls

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/spf13/cobra"
)

var recursive bool
var humanReadable bool
var summarize bool

// Init initializes flags.
func Init(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&recursive, "recursive", false, "Command is performed on all files or objects under the specified directory or prefix.")
	cmd.Flags().BoolVar(&humanReadable, "human-readable", false, "Displays file sizes in human readable format.")
	cmd.Flags().BoolVar(&summarize, "summarize", false, "Displays summary information (number of objects, total size).")
}

// Run runs mb command.
func Run(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if len(args) == 0 {
		listBuckets(ctx, cmd)
		return
	}
	if len(args) > 1 {
		cmd.PrintErrln("extra options: " + strings.Join(args[1:], " "))
		os.Exit(1)
	}
	listObjects(ctx, cmd, args[0])
}

func listBuckets(ctx context.Context, cmd *cobra.Command) {
	svc, err := config.NewS3ServiceClient()
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
	resp, err := svc.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
	for _, b := range resp.Buckets {
		creationDate := aws.ToTime(b.CreationDate).In(time.Local)
		cmd.Printf("%s %s\n", creationDate.Format("2006-01-02 15:04:05"), aws.ToString(b.Name))
	}
}

func listObjects(ctx context.Context, cmd *cobra.Command, path string) {
	bucket, key := parsePath(path)
	svc, err := config.NewS3BucketClient(ctx, bucket)
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}
	if key != "" {
		input.Prefix = aws.String(key)
	}
	if !recursive {
		input.Delimiter = aws.String("/")
	}

	var objects, totalBytes int64
	for {
		page, err := svc.ListObjectsV2(ctx, input)
		if err != nil {
			cmd.PrintErrln(err)
			os.Exit(1)	
		}
		objects += int64(len(page.Contents))
		// merge Contents and CommonPrefixes
		contents := page.Contents
		prefixes := page.CommonPrefixes
		for len(contents) > 0 && len(prefixes) > 0 {
			if aws.ToString(contents[0].Key) < aws.ToString(prefixes[0].Prefix) {
				printObject(cmd, contents[0])
				totalBytes += aws.ToInt64(contents[0].Size)
				contents = contents[1:]
			} else {
				printPrefix(cmd, prefixes[0])
				prefixes = prefixes[1:]
			}
		}
		for _, obj := range contents {
			printObject(cmd, obj)
			totalBytes += aws.ToInt64(obj.Size)
		}
		for _, prefix := range prefixes {
			printPrefix(cmd, prefix)
		}
		if page.ContinuationToken == nil {
			break
		}
		input.ContinuationToken = page.ContinuationToken
	}

	if summarize {
		cmd.Printf("\nTotal Objects: %d\n", objects)
		if humanReadable {
			cmd.Printf("   Total Size: %s\n", makeHumanReadable(totalBytes))
		} else {
			cmd.Printf("   Total Size: %d\n", totalBytes)
		}
	}
}

func parsePath(path string) (bucket, key string) {
	path = strings.TrimPrefix(path, "s3://")
	if idx := strings.IndexByte(path, '/'); idx > 0 {
		bucket = path[:idx]
		key = path[idx+1:]
		return
	}
	bucket = path
	return
}

func printObject(cmd *cobra.Command, obj *types.Object) {
	date := aws.ToTime(obj.LastModified).In(time.Local).Format("2006-01-02 15:04:05")
	size := aws.ToInt64(obj.Size)
	if humanReadable {
		cmd.Printf("%s %10s %s\n", date, makeHumanReadable(size), aws.ToString(obj.Key))
	} else {
		cmd.Printf("%s %10d %s\n", date, size, aws.ToString(obj.Key))
	}
}

func printPrefix(cmd *cobra.Command, prefix *types.CommonPrefix) {
	cmd.Printf("                           PRE %s\n", aws.ToString(prefix.Prefix))
}

// port of https://github.com/aws/aws-cli/blob/072688cc07578144060aead8b75556fd986e0f2f/awscli/customizations/s3/utils.py#L47-L77
func makeHumanReadable(size int64) string {
	if size == 1 {
		return "1 Byte"
	}
	if size < 1024 {
		return fmt.Sprintf("%d Bytes", size)
	}

	base := 1024.0
	bytes := float64(size)
	for i, suffix := range [...]string{"KiB", "MiB", "GiB", "TiB", "PiB"} {
		unit := float64(int(base*base) << (i * 10))
		if math.Round(bytes/unit*float64(base)) < float64(base) {
			return fmt.Sprintf("%.1f %s", (base*bytes)/unit, suffix)
		}
	}

	return fmt.Sprintf("%.1f EiB", (base*bytes)/float64(1<<70))
}
