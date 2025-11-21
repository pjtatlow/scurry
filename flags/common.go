package flags

import (
	"os"

	"github.com/spf13/cobra"
)

var (
	Verbose       bool
	MigrationDir  string
	DefinitionDir string
	DbUrl         string
)

func AddVerbose(cmd *cobra.Command) {
	cmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "Enable verbose output")
}

func AddMigrationDir(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&MigrationDir, "migrations", coalesceDefaults(os.Getenv("MIGRATION_DIR"), "./migrations"), "Directory containing migration files")
}

func AddDefinitionDir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&DefinitionDir, "definitions", coalesceDefaults(os.Getenv("DEFINITION_DIR"), "./definitions"), "Directory containing schema definition files")
}

func AddDbUrl(cmd *cobra.Command) {
	cmd.Flags().StringVar(&DbUrl, "db-url", coalesceDefaults(os.Getenv("CRDB_URL"), os.Getenv("DB_URL")), "Database connection URL")
}

func coalesceDefaults(defaults ...string) string {
	for _, value := range defaults {
		if value != "" {
			return value
		}
	}
	return ""

}
