package cmd

import (
	"github.com/spf13/cobra"
)

var dataCmd = &cobra.Command{
	Use:   "data",
	Short: "Dump and load database data",
	Long:  `Dump and load database data between CockroachDB instances.`,
}

func init() {
	rootCmd.AddCommand(dataCmd)
}
