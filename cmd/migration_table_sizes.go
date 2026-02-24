package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	migrationpkg "github.com/pjtatlow/scurry/internal/migration"
	"github.com/pjtatlow/scurry/internal/ui"
)

var largeTableThreshold int64

var migrationStatPullCmd = &cobra.Command{
	Use:   "table-sizes",
	Short: "Fetch table statistics from the database and write table_sizes.yaml",
	Long: `Query the database for table row counts and sizes, then write
the results to migrations/table_sizes.yaml. This file is used by
'scurry migration gen' to classify migrations as sync or async.`,
	RunE: runMigrationStatPull,
}

func init() {
	migrationCmd.AddCommand(migrationStatPullCmd)

	flags.AddDbUrl(migrationStatPullCmd)
	migrationStatPullCmd.Flags().Int64Var(&largeTableThreshold, "large-table-threshold", int64(migrationpkg.DefaultLargeTableThreshold), "Row count threshold for classifying tables as large")
}

func runMigrationStatPull(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	return doMigrationStatPull(ctx)
}

func doMigrationStatPull(ctx context.Context) error {
	fs := afero.NewOsFs()

	if flags.DbUrl == "" {
		return fmt.Errorf("database URL is required (use --db-url or CRDB_URL env var)")
	}

	if err := validateMigrationsDir(fs); err != nil {
		return err
	}

	// Connect to database
	dbClient, err := db.Connect(ctx, flags.DbUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer dbClient.Close()

	// Fetch table sizes
	tableSizes, err := dbClient.GetTableSizes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get table sizes: %w", err)
	}

	// Build TableSizes struct
	ts := &migrationpkg.TableSizes{
		Threshold: largeTableThreshold,
		Tables:    make(map[string]migrationpkg.TableInfo, len(tableSizes)),
	}

	for _, t := range tableSizes {
		qualifiedName := fmt.Sprintf("%s.%s", t.SchemaName, t.TableName)
		ts.Tables[qualifiedName] = migrationpkg.TableInfo{
			Rows: t.Rows,
		}
	}

	// Save to file
	if err := migrationpkg.SaveTableSizes(fs, flags.MigrationDir, ts); err != nil {
		return fmt.Errorf("failed to save table_sizes.yaml: %w", err)
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Wrote table_sizes.yaml with %d table(s) (threshold: %d rows)", len(tableSizes), largeTableThreshold)))

	return nil
}
