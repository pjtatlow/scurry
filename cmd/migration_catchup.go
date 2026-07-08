package cmd

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/spf13/afero"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	migrationpkg "github.com/pjtatlow/scurry/internal/migration"
	"github.com/pjtatlow/scurry/internal/recovery"
	"github.com/pjtatlow/scurry/internal/schema"
	"github.com/pjtatlow/scurry/internal/ui"
)

// errMigrationCanceled signals that the user aborted a migration flow (e.g. declined
// to run migrations or aborted recovery). Callers treat it as a clean exit.
var errMigrationCanceled = errors.New("migration canceled")

// catchUpMigrations runs all pending tracked migrations against the database before
// any further work. If a migration is in a failed/pending state it first runs the
// interactive recovery flow. Returns errMigrationCanceled if the user aborts.
//
// It returns baseline=true when adopting a previously untracked, non-empty database:
// the migrations are not run (they would fail against the already-existing schema).
// The caller then records the migrations as the baseline. It also returns the number
// of migrations actually executed during catch-up.
func catchUpMigrations(ctx context.Context, fs afero.Fs, dbClient *db.Client, force, dryRun, verbose bool) (bool, int, error) {
	if err := dbClient.InitMigrationHistory(ctx); err != nil {
		return false, 0, err
	}

	migrations, err := loadMigrations(fs)
	if err != nil {
		return false, 0, err
	}
	if len(migrations) == 0 {
		return false, 0, nil
	}

	// Resolve any failed or pending sync migration before running new ones.
	failed, err := dbClient.GetFailedMigration(ctx)
	if err != nil {
		return false, 0, err
	}
	if failed != nil {
		if err := recoverBeforeLocal(ctx, fs, dbClient, force, migrations, failed); err != nil {
			return false, 0, err
		}
	}

	// Determine which migrations still need to run.
	applied, err := dbClient.GetAppliedMigrations(ctx)
	if err != nil {
		return false, 0, err
	}

	// Adopt an existing, untracked database. If no migrations have ever been
	// recorded but the database already has a schema, treat the current state as
	// the baseline rather than re-running the migrations (which would fail against
	// the existing objects).
	if len(applied) == 0 {
		existing, err := schema.LoadFromDatabase(ctx, dbClient)
		if err != nil {
			return false, 0, err
		}
		if schemaHasObjects(existing) {
			fmt.Println(ui.Info(fmt.Sprintf("Existing untracked database detected; baselining %d migration(s).", len(migrations))))
			return true, 0, nil
		}
	}

	unapplied, warnings, err := filterUnappliedMigrations(migrations, applied)
	if err != nil {
		return false, 0, err
	}
	for _, warning := range warnings {
		fmt.Println(ui.Warning(warning))
	}
	if len(unapplied) == 0 {
		if verbose {
			fmt.Println(ui.Subtle("→ No pending migrations to run"))
		}
		return false, 0, nil
	}

	fmt.Printf("\n%s\n", ui.Header(fmt.Sprintf("Pending migrations to run (%d):", len(unapplied))))
	for i, migration := range unapplied {
		modeLabel := ""
		if migration.Mode == db.MigrationModeAsync {
			modeLabel = " (async)"
		}
		fmt.Printf("  %d. %s%s\n", i+1, migration.Name, modeLabel)
	}
	fmt.Println()

	if dryRun {
		fmt.Println(ui.Info(fmt.Sprintf("ℹ Dry run mode - %d migration(s) would run.", len(unapplied))))
		return false, 0, nil
	}

	fmt.Println(ui.Info("⟳ Running migrations..."))
	executed, skipped, err := runMigrationList(ctx, dbClient, unapplied)
	if err != nil {
		return false, executed, err
	}
	if executed > 0 {
		fmt.Println(ui.Success(fmt.Sprintf("✓ Ran %d migration(s)", executed)))
	}
	if skipped > 0 {
		return false, executed, fmt.Errorf("could not run %d migration(s) due to unmet dependencies or a running async migration", skipped)
	}

	return false, executed, nil
}

// schemaHasObjects reports whether a loaded schema contains any user-defined objects
// (tables, types, routines, sequences, or views). The bare public schema is ignored, so
// a freshly created, empty database reports false.
func schemaHasObjects(s *schema.Schema) bool {
	return len(s.Tables)+len(s.Types)+len(s.Routines)+len(s.Sequences)+len(s.Views) > 0
}

// recoverBeforeLocal runs the interactive recovery flow for a failed/pending migration
// encountered during catch-up, adding a "skip migrations" option that marks all pending
// migrations complete and continues. Returns errMigrationCanceled if the user aborts.
func recoverBeforeLocal(ctx context.Context, fs afero.Fs, dbClient *db.Client, force bool, migrations []db.Migration, failed *db.AppliedMigration) error {
	if force || !ui.IsInteractive() {
		return fmt.Errorf("migration %q is in %s state; run 'scurry migration recover' first", failed.Name, failed.Status)
	}

	migrationFile := filepath.Join(flags.MigrationDir, failed.Name, "migration.sql")
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
	migrationSQL := migrationpkg.StripHeader(rawSQL)

	displayMigrationInfo(failed, migrationSQL)

	migration := db.Migration{
		Name:     failed.Name,
		SQL:      migrationSQL,
		Checksum: computeChecksum(rawSQL),
	}

	result, err := recovery.RunRecoveryLoop(ctx, recovery.RecoveryLoopConfig{
		DbClient:        dbClient,
		Migration:       migration,
		FailedMigration: failed,
		IncludeSkipAll:  true,
		MigrationStatus: failed.Status,
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
		OnSkipAll: func(ctx context.Context, client *db.Client) error {
			return markAllMigrationsComplete(ctx, client, migrations)
		},
	})
	if err != nil {
		return err
	}
	if result == recovery.ResultAbort {
		fmt.Println(ui.Subtle("Canceled."))
		return errMigrationCanceled
	}

	return nil
}
