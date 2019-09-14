// Copyright © 2019 Shogo Ichinose
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"github.com/shogo82148/s3cli-mini/cmd/internal/ls"
	"github.com/spf13/cobra"
)

// lsCmd represents the ls command
var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List S3 objects and common prefixes under a prefix or all S3 buckets.",
	Long: `List S3 objects and common prefixes under a prefix or all S3 buckets.

Synopsis
ls
<S3Uri> or NONE
[--recursive]
[--page-size <value>]
[--human-readable]
[--summarize]
[--request-payer <value>]

Options
paths (string)

--recursive (boolean) Command is performed on all files or objects under the specified directory or prefix.

--page-size (integer) The number of results to return in each response to a list operation. The default value is 1000 (the maximum allowed). Using a lower value may help if an operation times out.

--human-readable (boolean) Displays file sizes in human readable format.

--summarize (boolean) Displays summary information (number of objects, total size).

--request-payer (string) Confirms that the requester knows that she or he will be charged for the request. Bucket owners need not specify this parameter in their requests. Documentation on downloading objects from requester pays buckets can be found at http://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectsinRequesterPaysBuckets.html

See 'aws help' for descriptions of global parameters.`,
	Run: ls.Run,
}

func init() {
	rootCmd.AddCommand(lsCmd)
}
