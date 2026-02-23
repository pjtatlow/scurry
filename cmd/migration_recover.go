package cmd

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	migrationpkg "github.com/pjtatlow/scurry/internal/migration"
	"github.com/pjtatlow/scurry/internal/recovery"
	"github.com/pjtatlow/scurry/internal/ui"
)

var migrationRecoverCmd = &cobra.Command{
	Use:   "recover",
	Short: "Recover from a failed migration",
	Long: `Interactively recover from a failed or pending migration.

When a migration fails partway through, it is marked as failed in the database.
This command allows you to:

  - Try again: Re-run all statements from the beginning of the migration
  - Mark as succeeded: Mark the migration as recovered and run remaining statements
  - Run manual SQL: Execute custom SQL to fix the issue, then choose again
  - Abort: Exit without making changes

Examples:
  # Start interactive recovery
  scurry migration recover --db-url="postgresql://user:pass@localhost:26257/db"
`,
	RunE: runMigrationRecover,
}

func init() {
	migrationCmd.AddCommand(migrationRecoverCmd)
	flags.AddDbUrl(migrationRecoverCmd)
}

func runMigrationRecover(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	fs := afero.NewOsFs()

	// Check for interactive terminal
	if !ui.IsInteractive() {
		return fmt.Errorf("migration recover requires an interactive terminal\nRun this command in a terminal with TTY support")
	}

	// Connect to database
	dbClient, err := db.Connect(ctx, flags.DbUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer dbClient.Close()

	// Initialize migration history table
	if err := dbClient.InitMigrationHistory(ctx); err != nil {
		return err
	}

	// Check for failed migration
	failedMigration, err := dbClient.GetFailedMigration(ctx)
	if err != nil {
		return err
	}

	if failedMigration == nil {
		fmt.Println(ui.Success("No failed migrations to recover"))
		return nil
	}

	// Load migration file from disk
	migrationFile := filepath.Join(flags.MigrationDir, failedMigration.Name, "migration.sql")
	exists, err := afero.Exists(fs, migrationFile)
	if err != nil {
		return fmt.Errorf("failed to check migration file: %w", err)
	}
	if !exists {
		return fmt.Errorf("migration file not found: %s\nThe migration file may have been deleted", migrationFile)
	}

	content, err := afero.ReadFile(fs, migrationFile)
	if err != nil {
		return fmt.Errorf("failed to read migration file: %w", err)
	}
	rawSQL := string(content)
	currentChecksum := computeChecksum(rawSQL)
	migrationSQL := migrationpkg.StripHeader(rawSQL)

	// Check for checksum mismatch
	if failedMigration.Checksum != "" && failedMigration.Checksum != currentChecksum {
		fmt.Println(ui.Warning("Migration file has been modified since the failure"))
		fmt.Println(ui.Subtle(fmt.Sprintf("  Stored checksum: %s", recovery.TruncateChecksum(failedMigration.Checksum))))
		fmt.Println(ui.Subtle(fmt.Sprintf("  Current checksum: %s", recovery.TruncateChecksum(currentChecksum))))
		fmt.Println()
	}

	// Display migration info
	displayMigrationInfo(failedMigration, migrationSQL)

	// Warn about pending migrations that could still be running
	if failedMigration.Status == db.MigrationStatusPending {
		fmt.Println()
		fmt.Println(ui.WarningBanner("WARNING: This migration is in 'pending' state.\nIt could still be running in another process right now."))
		fmt.Println()
		fmt.Println(ui.Warning("Before continuing, make sure:"))
		fmt.Println(ui.Warning("  • No other process is currently executing migrations"))
		fmt.Println(ui.Warning("  • The previous migration process has terminated"))
		fmt.Println()

		confirmed, err := ui.ConfirmPrompt("Are you sure no other process is currently running this migration?")
		if err != nil {
			return fmt.Errorf("failed to get confirmation: %w", err)
		}
		if !confirmed {
			fmt.Println(ui.Info("Aborted - verify no other migration process is running before retrying"))
			return nil
		}
	}

	// Create migration struct for execution
	migration := db.Migration{
		Name:     failedMigration.Name,
		SQL:      migrationSQL,
		Checksum: currentChecksum,
	}

	// Run interactive recovery loop
	result, err := recovery.RunRecoveryLoop(ctx, recovery.RecoveryLoopConfig{
		DbClient:            dbClient,
		Migration:           migration,
		FailedMigration:     failedMigration,
		IncludeDropDatabase: false,
		MigrationStatus:     failedMigration.Status,
		OnRetryFailure: func(ctx context.Context, client *db.Client) (*db.AppliedMigration, error) {
			refreshed, err := client.GetFailedMigration(ctx)
			if err != nil {
				fmt.Println(ui.Warning(fmt.Sprintf("Could not refresh migration status: %v", err)))
				return nil, err
			}
			if refreshed != nil {
				displayMigrationInfo(refreshed, migrationSQL)
			}
			return refreshed, nil
		},
	})
	if err != nil {
		return err
	}

	if result == recovery.ResultAbort {
		fmt.Println(ui.Info("Aborted - no changes made"))
	}

	return nil
}

func displayMigrationInfo(failedMigration *db.AppliedMigration, migrationSQL string) {
	fmt.Println()

	if failedMigration.Status == db.MigrationStatusPending {
		fmt.Println(ui.Header("Pending Migration Details"))
	} else {
		fmt.Println(ui.Header("Failed Migration Details"))
	}

	fmt.Println()
	fmt.Printf("  Name: %s\n", failedMigration.Name)
	fmt.Printf("  Status: %s\n", failedMigration.Status)
	if failedMigration.StartedAt != nil {
		fmt.Printf("  Started: %s\n", failedMigration.StartedAt.Format(recovery.DateTimeDisplayFormat))
	}

	if failedMigration.FailedStatement != nil && *failedMigration.FailedStatement != "" {
		fmt.Println()
		fmt.Println(ui.Header("Failed Statement:"))
		fmt.Println(ui.SqlCode(*failedMigration.FailedStatement))
	}

	if failedMigration.ErrorMsg != nil && *failedMigration.ErrorMsg != "" {
		fmt.Println()
		fmt.Println(ui.Error("Error: " + *failedMigration.ErrorMsg))
	}

	fmt.Println()
	fmt.Println(ui.Header("Full Migration Content:"))
	fmt.Println(ui.SqlCode(migrationSQL))
}
