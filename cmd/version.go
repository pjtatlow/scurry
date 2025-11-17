package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	// Version is set via ldflags at build time
	Version = "dev"
)

var versionCmd = &cobra.Command{
	Use:  "version",
	Long: `Print the version number of scurry`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("scurry version %s\n", Version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
