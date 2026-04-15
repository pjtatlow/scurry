package flags

import (
	"os"

	"github.com/spf13/cobra"
)

var (
	Verbose        bool
	Force          bool
	NoColor        bool
	MigrationDir   string
	DefinitionDirs []string
	DbUrl          string
)

func AddVerbose(cmd *cobra.Command) {
	cmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "Enable verbose output")
}

func AddForce(cmd *cobra.Command) {
	cmd.PersistentFlags().BoolVar(&Force, "force", false, "Do not prompt user for confirmation or input")
}

func AddNoColor(cmd *cobra.Command) {
	cmd.PersistentFlags().BoolVar(&NoColor, "no-color", false, "Disable colored output (also respects NO_COLOR env var)")
}

func AddMigrationDir(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&MigrationDir, "migrations", coalesceDefaults(os.Getenv("MIGRATION_DIR"), "./migrations"), "Directory containing migration files")
}

func AddDefinitionDirs(cmd *cobra.Command) {
	defaultDirs := []string{"./definitions"}
	if envDir := os.Getenv("DEFINITION_DIR"); envDir != "" {
		defaultDirs = []string{envDir}
	}
	cmd.Flags().StringArrayVar(&DefinitionDirs, "definitions", defaultDirs, "Directories containing schema definition files (can be specified multiple times)")
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
