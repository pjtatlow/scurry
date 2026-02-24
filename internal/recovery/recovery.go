// Package recovery provides shared functionality for migration recovery operations.
package recovery

import (
	"context"
	"fmt"

	"github.com/charmbracelet/huh"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/ui"
)

// Recovery option constants
const (
	OptionTryAgain      = "try_again"
	OptionMarkSucceeded = "mark_succeeded"
	OptionDropDatabase  = "drop_database"
	OptionAbort         = "abort"
)

// UI constants
const (
	ChecksumDisplayLength = 16
	DateTimeDisplayFormat = "2006-01-02 15:04:05"
)

// TruncateChecksum shortens a checksum for display purposes.
func TruncateChecksum(checksum string) string {
	if len(checksum) > ChecksumDisplayLength {
		return checksum[:ChecksumDisplayLength] + "..."
	}
	return checksum
}

// TryAgain resets a migration and re-executes all statements from the beginning.
// It updates the migration status based on the outcome.
func TryAgain(ctx context.Context, dbClient *db.Client, migration db.Migration) error {
	fmt.Println()
	fmt.Println(ui.Info("Retrying migration from the beginning..."))

	// Reset migration to pending state
	if err := dbClient.ResetMigrationForRetry(ctx, migration.Name, migration.Checksum); err != nil {
		return fmt.Errorf("failed to reset migration: %w", err)
	}

	// Parse and execute all statements
	statements, err := db.SplitStatements(migration.SQL)
	if err != nil {
		return fmt.Errorf("failed to parse migration: %w", err)
	}

	for i, stmt := range statements {
		fmt.Printf("  Executing statement %d/%d...\n", i+1, len(statements))
		_, err := dbClient.ExecContext(ctx, stmt)
		if err != nil {
			// Update failure info
			if failErr := dbClient.FailMigration(ctx, migration.Name, stmt, err.Error()); failErr != nil {
				return fmt.Errorf("statement failed and could not record failure: %w (original: %v)", failErr, err)
			}
			return fmt.Errorf("statement %d failed: %w", i+1, err)
		}
	}

	// Mark as completed
	if err := dbClient.CompleteMigration(ctx, migration.Name); err != nil {
		return fmt.Errorf("all statements succeeded but failed to mark completed: %w", err)
	}

	return nil
}

// MarkSucceeded marks the migration as recovered without executing any statements.
func MarkSucceeded(ctx context.Context, dbClient *db.Client, migration db.Migration) error {
	fmt.Println()

	if err := dbClient.RecoverMigration(ctx, migration.Name); err != nil {
		return fmt.Errorf("failed to mark migration as recovered: %w", err)
	}

	return nil
}

// RecoveryPromptConfig configures the recovery option prompt.
type RecoveryPromptConfig struct {
	// IncludeDropDatabase includes the "drop database" option
	IncludeDropDatabase bool
	// MigrationStatus is "pending" or "failed" - affects descriptions
	MigrationStatus string
}

// RecoveryResult represents the outcome of a recovery loop.
type RecoveryResult int

const (
	// ResultSuccess indicates the migration succeeded or was recovered
	ResultSuccess RecoveryResult = iota
	// ResultAbort indicates the user chose to abort without changes
	ResultAbort
	// ResultDropDatabase indicates the user chose to drop the database
	ResultDropDatabase
)

// RecoveryLoopConfig configures the recovery loop behavior.
type RecoveryLoopConfig struct {
	// DbClient is the database client
	DbClient *db.Client
	// Migration is the migration to recover
	Migration db.Migration
	// FailedMigration is the record of the failed migration from the database
	FailedMigration *db.AppliedMigration
	// IncludeDropDatabase enables the drop database option
	IncludeDropDatabase bool
	// MigrationStatus is "pending" or "failed" for display purposes
	MigrationStatus string

	// OnRetryFailure is called when a retry fails, allowing the caller to refresh
	// the migration info display. Returns the refreshed migration record.
	// Optional - if nil, no refresh is performed.
	OnRetryFailure func(ctx context.Context, dbClient *db.Client) (*db.AppliedMigration, error)

	// OnDropDatabase is called when the user chooses to drop the database.
	// Required if IncludeDropDatabase is true.
	OnDropDatabase func(ctx context.Context, dbClient *db.Client) error
}

// PromptRecoveryOption displays the recovery options menu and returns the user's choice.
func PromptRecoveryOption(config RecoveryPromptConfig) (string, error) {
	var choice string

	// Different descriptions based on migration status
	var description string

	if config.MigrationStatus == db.MigrationStatusPending {
		description = "Choose how to handle the pending migration"
	} else {
		description = "Choose how to handle the failed migration"
	}

	// Build options list
	options := []huh.Option[string]{
		huh.NewOption("Try again - Re-run all statements from the beginning", OptionTryAgain),
		huh.NewOption("Mark as succeeded - Mark the migration as recovered without re-running", OptionMarkSucceeded),
	}

	if config.IncludeDropDatabase {
		options = append(options, huh.NewOption("Drop database - Drop the entire database and start fresh", OptionDropDatabase))
	}

	options = append(options, huh.NewOption("Abort - Exit without changes", OptionAbort))

	form := huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("Recovery options").
				Description(description).
				Options(options...).
				Value(&choice),
		),
	).WithTheme(ui.HuhTheme())

	err := form.Run()
	if err != nil {
		return "", err
	}

	return choice, nil
}

// RunRecoveryLoop runs the interactive recovery loop, prompting the user for actions
// until the migration is recovered or the user aborts.
func RunRecoveryLoop(ctx context.Context, config RecoveryLoopConfig) (RecoveryResult, error) {
	for {
		choice, err := PromptRecoveryOption(RecoveryPromptConfig{
			IncludeDropDatabase: config.IncludeDropDatabase,
			MigrationStatus:     config.MigrationStatus,
		})
		if err != nil {
			return ResultAbort, fmt.Errorf("failed to get user input: %w", err)
		}

		switch choice {
		case OptionTryAgain:
			if err := TryAgain(ctx, config.DbClient, config.Migration); err != nil {
				fmt.Println(ui.Error(fmt.Sprintf("Retry failed: %v", err)))
				// Allow caller to refresh migration display
				if config.OnRetryFailure != nil {
					config.OnRetryFailure(ctx, config.DbClient)
				}
				fmt.Println()
				continue
			}
			fmt.Println(ui.Success("Migration completed successfully!"))
			return ResultSuccess, nil

		case OptionMarkSucceeded:
			if err := MarkSucceeded(ctx, config.DbClient, config.Migration); err != nil {
				return ResultAbort, fmt.Errorf("failed to mark as succeeded: %w", err)
			}
			fmt.Println(ui.Success("Migration marked as recovered"))
			return ResultSuccess, nil

		case OptionDropDatabase:
			if config.OnDropDatabase == nil {
				return ResultAbort, fmt.Errorf("drop database handler not configured")
			}
			if err := config.OnDropDatabase(ctx, config.DbClient); err != nil {
				return ResultAbort, fmt.Errorf("failed to drop database: %w", err)
			}
			fmt.Println(ui.Success("Database dropped. Please re-run push to start fresh."))
			return ResultDropDatabase, nil

		case OptionAbort:
			return ResultAbort, nil
		}
	}
}
