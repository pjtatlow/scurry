package cmd

import (
	"github.com/spf13/cobra"
)

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate code from schema definitions",
	Long:  `Generate code artifacts from your schema definition files.`,
}

func init() {
	rootCmd.AddCommand(generateCmd)
}
