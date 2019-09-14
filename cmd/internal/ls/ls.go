package ls

import (
	"log"

	"github.com/shogo82148/s3cli-mini/cmd/internal/config"
	"github.com/spf13/cobra"
)

// Run runs mb command.
func Run(cmd *cobra.Command, args []string) {
	svc, err := config.NewS3Client()
	if err != nil {
		log.Fatal(err)
	}
	_ = svc
}
