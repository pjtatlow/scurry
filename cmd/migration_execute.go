package cmd

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	"github.com/pjtatlow/scurry/internal/ui"
)

var (
	executeDryRun           bool
	executeForce            bool
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
	migrationExecuteCmd.Flags().DurationVar(&executeStatementTimeout, "statement-timeout", 0, "Set statement timeout (e.g., 30s, 5m, 1h)")
}

func runMigrationExecute(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Load all migrations from disk
	migrations, err := loadMigrationsForExecution(afero.NewOsFs())
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

	// Display migrations to be executed
	fmt.Printf("\n%s\n", ui.Header("Migrations to execute:"))
	for i, migration := range unappliedMigrations {
		fmt.Printf("  %d. %s\n", i+1, migration.Name)
	}
	fmt.Println()

	// Dry run mode - just show what would be executed
	if executeDryRun {
		fmt.Println(ui.Info("Dry run mode - no changes will be made"))
		return nil
	}

	// Confirmation prompt
	if !executeForce {
		fmt.Printf("%s ", ui.Header("Execute these migrations?"))
		fmt.Print("[y/N]: ")
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println(ui.Info("Aborted"))
			return nil
		}
	}

	// Execute migrations one by one
	fmt.Println()
	for i, migration := range unappliedMigrations {
		fmt.Printf("Executing %s (%d/%d)...\n", migration.Name, i+1, len(unappliedMigrations))

		err := dbClient.ExecuteMigration(ctx, migration)
		if err != nil {
			// Migration failed - report the error and stop
			fmt.Println(ui.Error(fmt.Sprintf("\nMigration failed: %s", migration.Name)))
			fmt.Println(ui.Error(fmt.Sprintf("Error: %v", err)))
			fmt.Println()

			// Show progress
			if i > 0 {
				fmt.Printf("%s\n", ui.Success(fmt.Sprintf("Successfully applied %d migration(s) before failure:", i)))
				for j := range i {
					fmt.Printf("  ✓ %s\n", unappliedMigrations[j].Name)
				}
				fmt.Println()
			}

			fmt.Printf("%s\n", ui.Error(fmt.Sprintf("Failed migration: %s", migration.Name)))
			fmt.Printf("%s\n", ui.Info(fmt.Sprintf("Remaining migrations not executed: %d", len(unappliedMigrations)-i-1)))

			return fmt.Errorf("migration execution stopped due to error")
		}

		fmt.Printf("  %s\n", ui.Success("✓ Success"))
	}

	// All migrations completed successfully
	fmt.Println()
	fmt.Println(ui.Success("All migrations executed successfully!"))
	fmt.Printf("%s\n", ui.Success(fmt.Sprintf("Applied %d migration(s)", len(unappliedMigrations))))

	return nil
}

// loadMigrationsForExecution loads all migration files from the migrations directory
// and returns them with checksums computed
func loadMigrationsForExecution(fs afero.Fs) ([]db.Migration, error) {
	// Read migrations directory
	entries, err := afero.ReadDir(fs, flags.MigrationDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []db.Migration{}, nil
		}
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	// Filter and sort migration directories
	var migrationDirs []string
	for _, entry := range entries {
		// Skip schema.sql and non-directories
		if !entry.IsDir() {
			continue
		}

		// Migration directories should have the format: YYYYMMDDHHMMSS_name
		name := entry.Name()
		if len(name) >= 14 {
			migrationDirs = append(migrationDirs, name)
		}
	}

	// Sort by timestamp (directory name starts with timestamp)
	sort.Strings(migrationDirs)

	// Read migration.sql from each directory
	var allMigrations []db.Migration
	for _, dir := range migrationDirs {
		migrationFile := filepath.Join(flags.MigrationDir, dir, "migration.sql")

		// Check if migration.sql exists
		exists, err := afero.Exists(fs, migrationFile)
		if err != nil {
			return nil, fmt.Errorf("failed to check migration file %s: %w", migrationFile, err)
		}
		if !exists {
			continue
		}

		// Read migration.sql content
		content, err := afero.ReadFile(fs, migrationFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read migration file %s: %w", migrationFile, err)
		}

		sql := string(content)
		checksum := computeChecksum(sql)

		// Add the migration
		allMigrations = append(allMigrations, db.Migration{
			Name:     dir,
			SQL:      sql,
			Checksum: checksum,
		})
	}

	return allMigrations, nil
}

// ComputeChecksum computes the SHA-256 checksum of a migration's SQL content
func computeChecksum(sql string) string {
	hash := sha256.Sum256([]byte(sql))
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
			if applied.Checksum != migration.Checksum {
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
