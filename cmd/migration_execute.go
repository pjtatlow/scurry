package cmd

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	migrationpkg "github.com/pjtatlow/scurry/internal/migration"
	"github.com/pjtatlow/scurry/internal/ui"
)

var (
	executeDryRun           bool
	executeForce            bool
	executeIncludeAsync     bool
	executeStatementTimeout time.Duration
)

var migrationExecuteCmd = &cobra.Command{
	Use:   "execute",
	Short: "Execute pending migrations against the database",
	Long: `Execute all unapplied migrations against the database in order.

Migrations are tracked in the _scurry_.migrations table. Only migrations that
haven't been applied yet will be executed. If a migration fails, execution stops
and the error is reported.

Examples:
  # Execute pending migrations with confirmation prompt
  scurry migration execute --db-url="postgresql://user:pass@localhost:26257/db"

  # Preview what would be executed without applying
  scurry migration execute --dry-run

  # Execute without confirmation prompt
  scurry migration execute --force
`,
	RunE: runMigrationExecute,
}

func init() {
	migrationCmd.AddCommand(migrationExecuteCmd)

	flags.AddDbUrl(migrationExecuteCmd)

	migrationExecuteCmd.Flags().BoolVar(&executeDryRun, "dry-run", false, "Show what would be executed without applying")
	migrationExecuteCmd.Flags().BoolVar(&executeForce, "force", false, "Skip confirmation prompt")
	migrationExecuteCmd.Flags().BoolVar(&executeIncludeAsync, "include-async", false, "Include async migrations in execution")
	migrationExecuteCmd.Flags().DurationVar(&executeStatementTimeout, "statement-timeout", 0, "Set statement timeout (e.g., 30s, 5m, 1h)")
}

func runMigrationExecute(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if flags.DbUrl == "" {
		return fmt.Errorf("database URL is required (use --db-url or CRDB_URL env var)")
	}

	// Load all migrations from disk
	migrations, err := loadMigrations(afero.NewOsFs())
	if err != nil {
		return err
	}

	if len(migrations) == 0 {
		fmt.Println(ui.Info(fmt.Sprintf("No migrations found in %s", flags.MigrationDir)))
		return nil
	}

	// Connect to database
	dbClient, err := db.Connect(ctx, flags.DbUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer dbClient.Close()

	// Set statement timeout if specified
	if executeStatementTimeout > 0 {
		if err := dbClient.SetStatementTimeout(ctx, executeStatementTimeout); err != nil {
			return fmt.Errorf("failed to set statement timeout: %w", err)
		}
	}

	// Initialize migration history table
	if err := dbClient.InitMigrationHistory(ctx); err != nil {
		return err
	}

	// Check for failed or pending sync migrations that need recovery
	failedMigration, err := dbClient.GetFailedMigration(ctx)
	if err != nil {
		return err
	}
	if failedMigration != nil {
		if failedMigration.Status == db.MigrationStatusFailed {
			fmt.Println(ui.Error(fmt.Sprintf("Migration %q is in failed state", failedMigration.Name)))
			if failedMigration.ErrorMsg != nil {
				fmt.Println(ui.Error(fmt.Sprintf("Error: %s", *failedMigration.ErrorMsg)))
			}
		} else {
			fmt.Println(ui.Error(fmt.Sprintf("Migration %q is in pending state (may have crashed during execution)", failedMigration.Name)))
		}
		fmt.Println()
		fmt.Println(ui.Info("Run 'scurry migration recover' to resolve this before executing new migrations"))
		return fmt.Errorf("cannot execute migrations while a migration is in %s state", failedMigration.Status)
	}

	// Get applied migrations
	appliedMigrations, err := dbClient.GetAppliedMigrations(ctx)
	if err != nil {
		return err
	}

	// Filter to get unapplied migrations
	unappliedMigrations, warnings, err := filterUnappliedMigrations(migrations, appliedMigrations)
	if err != nil {
		return err
	}

	// Show warnings about modified migrations
	for _, warning := range warnings {
		fmt.Println(ui.Warning(warning))
	}

	if len(unappliedMigrations) == 0 {
		fmt.Println(ui.Success("All migrations have been applied"))
		return nil
	}

	// Build execution list preserving timestamp order, skipping async unless --include-async
	var migrationsToExecute []db.Migration
	var skippedAsync []db.Migration
	for _, m := range unappliedMigrations {
		if m.Mode == db.MigrationModeAsync && !executeIncludeAsync {
			skippedAsync = append(skippedAsync, m)
			continue
		}
		migrationsToExecute = append(migrationsToExecute, m)
	}

	if len(skippedAsync) > 0 {
		fmt.Printf("\n%s\n", ui.Warning(fmt.Sprintf("Skipping %d async migration(s):", len(skippedAsync))))
		for _, m := range skippedAsync {
			fmt.Printf("  - %s\n", m.Name)
		}
		fmt.Println(ui.Info("Use --include-async to execute all migrations"))
	}

	if len(migrationsToExecute) == 0 {
		fmt.Println()
		fmt.Println(ui.Success("No sync migrations to execute"))
		return nil
	}

	// Display migrations to be executed
	fmt.Printf("\n%s\n", ui.Header("Migrations to execute:"))
	for i, migration := range migrationsToExecute {
		modeLabel := ""
		if migration.Mode == db.MigrationModeAsync {
			modeLabel = " (async)"
		}
		fmt.Printf("  %d. %s%s\n", i+1, migration.Name, modeLabel)
	}
	fmt.Println()

	// Dry run mode - just show what would be executed
	if executeDryRun {
		fmt.Println(ui.Info("Dry run mode - no changes will be made"))
		return nil
	}

	// Confirmation prompt
	if !executeForce {
		confirmed, err := ui.ConfirmPrompt("Execute these migrations?")
		if err != nil {
			return err
		}
		if !confirmed {
			fmt.Println(ui.Info("Aborted"))
			return nil
		}
	}

	// Execute migrations one by one
	fmt.Println()
	executed := 0
	skipped := 0
	for i, migration := range migrationsToExecute {
		// Check depends_on dependencies
		if len(migration.DependsOn) > 0 {
			unmet, err := dbClient.CheckDependenciesMet(ctx, migration.DependsOn)
			if err != nil {
				return fmt.Errorf("failed to check dependencies for %s: %w", migration.Name, err)
			}
			if len(unmet) > 0 {
				fmt.Println(ui.Warning(fmt.Sprintf("Skipping %s (%d/%d): unmet dependencies: %s",
					migration.Name, i+1, len(migrationsToExecute),
					strings.Join(unmet, ", "))))
				skipped++
				continue
			}
		}

		// If this is an async migration, check if another async is already running
		if migration.Mode == db.MigrationModeAsync {
			running, err := dbClient.HasRunningAsyncMigration(ctx)
			if err != nil {
				return fmt.Errorf("failed to check for running async migration: %w", err)
			}
			if running != nil {
				fmt.Println(ui.Warning(fmt.Sprintf("Skipping %s (%d/%d): async migration %q is still running",
					migration.Name, i+1, len(migrationsToExecute), running.Name)))
				skipped++
				continue
			}
		}

		fmt.Printf("Executing %s (%d/%d)...\n", migration.Name, i+1, len(migrationsToExecute))

		err := dbClient.ExecuteMigrationWithTracking(ctx, migration)
		if err != nil {
			// Migration failed - report the error and stop
			fmt.Println(ui.Error(fmt.Sprintf("\nMigration failed: %s", migration.Name)))
			fmt.Println(ui.Error(fmt.Sprintf("Error: %v", err)))
			fmt.Println()

			// Show progress
			if executed > 0 {
				fmt.Printf("%s\n", ui.Success(fmt.Sprintf("Successfully applied %d migration(s) before failure", executed)))
				fmt.Println()
			}

			fmt.Printf("%s\n", ui.Error(fmt.Sprintf("Failed migration: %s", migration.Name)))
			fmt.Printf("%s\n", ui.Info(fmt.Sprintf("Remaining migrations not executed: %d", len(migrationsToExecute)-i-1)))
			fmt.Println()
			fmt.Println(ui.Info("Run 'scurry migration recover' to resolve this failure"))

			return fmt.Errorf("migration execution stopped due to error")
		}

		fmt.Printf("  %s\n", ui.Success("✓ Success"))
		executed++
	}

	// Summary
	fmt.Println()
	if executed > 0 {
		fmt.Println(ui.Success(fmt.Sprintf("Applied %d migration(s)", executed)))
	}
	if skipped > 0 {
		fmt.Println(ui.Warning(fmt.Sprintf("Skipped %d migration(s) due to unmet dependencies or running async", skipped)))
	}
	if skipped == 0 && executed > 0 {
		fmt.Println(ui.Success("All migrations executed successfully!"))
	}

	return nil
}

// computeChecksum computes the SHA-256 checksum of a migration's SQL content.
// Headers are stripped before hashing so that header-only edits don't change the checksum.
func computeChecksum(sql string) string {
	stripped := migrationpkg.StripHeader(sql)
	hash := sha256.Sum256([]byte(stripped))
	return fmt.Sprintf("%x", hash)
}

func filterUnappliedMigrations(allMigrations []db.Migration, appliedMigrations []db.AppliedMigration) ([]db.Migration, []string, error) {
	appliedMap := make(map[string]db.AppliedMigration)
	for _, m := range appliedMigrations {
		appliedMap[m.Name] = m
	}

	var unapplied []db.Migration
	var warnings []string

	for _, migration := range allMigrations {
		if applied, exists := appliedMap[migration.Name]; exists {
			// Migration has been applied - verify checksum hasn't changed
			// Skip warning if stored checksum is empty (marked during creation, not execution)
			if applied.Checksum != "" && applied.Checksum != migration.Checksum {
				warnings = append(warnings, fmt.Sprintf(
					"WARNING: Migration %s has been modified after being applied (checksum mismatch)",
					migration.Name,
				))
			}
		} else {
			// Migration hasn't been applied yet
			unapplied = append(unapplied, migration)
		}
	}

	// Sort unapplied migrations by name to ensure consistent ordering
	sort.Slice(unapplied, func(i, j int) bool {
		return unapplied[i].Name < unapplied[j].Name
	})

	return unapplied, warnings, nil
}
