package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	migrationpkg "github.com/pjtatlow/scurry/internal/migration"
	"github.com/pjtatlow/scurry/internal/schema"
	"github.com/pjtatlow/scurry/internal/ui"
)

var (
	validateOverwrite    bool
	validateNoCheckpoint bool
)

var migrationValidateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate that migrations produce the expected schema",
	Long: `Validate migrations by applying them to a clean database and comparing
the result with schema.sql. Use --overwrite to update schema.sql instead of comparing.`,
	RunE: migrationValidate,
}

func init() {
	migrationCmd.AddCommand(migrationValidateCmd)
	migrationValidateCmd.Flags().BoolVar(&validateOverwrite, "overwrite", false, "Overwrite schema.sql with the result instead of comparing")
	migrationValidateCmd.Flags().BoolVar(&validateNoCheckpoint, "no-checkpoint", false, "Skip checkpoint generation after successful validation")
}

func migrationValidate(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	err := doMigrationValidate(ctx)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return nil
}

func doMigrationValidate(ctx context.Context) error {
	fs := afero.NewOsFs()

	// Validate migrations directory
	if err := validateMigrationsDir(fs); err != nil {
		return err
	}

	// 1. Load all migration files in order
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Loading migrations..."))
	}

	migrations, err := loadMigrations(fs)
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}

	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Found %d migration(s)", len(migrations))))
	}

	// 2. Apply migrations to empty shadow database
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Applying migrations to clean database..."))
	}

	resultSchema, err := applyMigrationsToCleanDatabase(ctx, migrations, flags.Verbose)
	if err != nil {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Result: %d tables, %d types, %d routines, %d sequences, %d views",
			len(resultSchema.Tables), len(resultSchema.Types), len(resultSchema.Routines), len(resultSchema.Sequences), len(resultSchema.Views))))
	}

	// 3. Handle overwrite flag
	if validateOverwrite {
		if flags.Verbose {
			fmt.Println(ui.Subtle("→ Overwriting schema.sql..."))
		}

		err = dumpProductionSchema(ctx, fs, resultSchema)
		if err != nil {
			return fmt.Errorf("failed to write schema.sql: %w", err)
		}

		fmt.Println()
		fmt.Println(ui.Success(fmt.Sprintf("✓ Updated %s", getSchemaFilePath())))

		// Create checkpoint for the last migration if it doesn't exist
		if !validateNoCheckpoint && len(migrations) > 0 {
			if err := ensureCheckpointForLastMigration(fs, migrations, resultSchema, flags.Verbose); err != nil {
				return fmt.Errorf("failed to create checkpoint: %w", err)
			}
		}

		return nil
	}

	// 4. Load expected schema from schema.sql
	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading expected schema from %s...", getSchemaFilePath())))
	}

	expectedSchema, err := loadProductionSchema(ctx, fs)
	if err != nil {
		return fmt.Errorf("failed to load schema.sql: %w", err)
	}

	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Expected: %d tables, %d types, %d routines, %d sequences, %d views",
			len(expectedSchema.Tables), len(expectedSchema.Types), len(expectedSchema.Routines), len(expectedSchema.Sequences), len(expectedSchema.Views))))
	}

	// 5. Compare schemas
	if flags.Verbose {
		fmt.Println()
		fmt.Println(ui.Subtle("→ Comparing schemas..."))
	}

	diffResult := schema.Compare(resultSchema, expectedSchema)

	// 6. Check for differences
	if !diffResult.HasChanges() {
		fmt.Println()
		fmt.Println(ui.Success("✓ Migrations match schema.sql"))

		// Create checkpoint for the last migration if it doesn't exist
		if !validateNoCheckpoint && len(migrations) > 0 {
			if err := ensureCheckpointForLastMigration(fs, migrations, resultSchema, flags.Verbose); err != nil {
				return fmt.Errorf("failed to create checkpoint: %w", err)
			}
		}

		return nil
	}

	// Show differences
	fmt.Println()
	fmt.Println(ui.Error("✗ Migrations do not match schema.sql"))
	fmt.Println()
	fmt.Println(ui.Header("Differences found:"))
	fmt.Println(diffResult.Summary())
	fmt.Println()

	// Prompt user to overwrite schema.sql
	shouldOverwrite, err := ui.ConfirmPrompt("Do you want to overwrite schema.sql with the migration results?")
	if err != nil {
		return fmt.Errorf("confirmation prompt failed: %w", err)
	}

	if !shouldOverwrite {
		return fmt.Errorf("validation failed: schema mismatch")
	}

	// User chose to overwrite
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Overwriting schema.sql..."))
	}

	err = dumpProductionSchema(ctx, fs, resultSchema)
	if err != nil {
		return fmt.Errorf("failed to write schema.sql: %w", err)
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Updated %s", getSchemaFilePath())))

	// Create checkpoint for the last migration if it doesn't exist
	if !validateNoCheckpoint && len(migrations) > 0 {
		if err := ensureCheckpointForLastMigration(fs, migrations, resultSchema, flags.Verbose); err != nil {
			return fmt.Errorf("failed to create checkpoint: %w", err)
		}
	}

	return nil
}

// loadMigrations loads all migration files from the migrations directory in order.
// Headers are parsed for mode/depends_on and stripped from the SQL content.
// Checksums are computed on the header-stripped SQL.
func loadMigrations(fs afero.Fs) ([]db.Migration, error) {
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

		// Parse header and determine mode
		mode := db.MigrationModeSync
		header, headerErr := migrationpkg.ParseHeader(sql)
		if headerErr != nil {
			fmt.Println(ui.Warning(fmt.Sprintf("Invalid header in %s: %v (defaulting to sync)", dir, headerErr)))
		}
		if header != nil {
			mode = string(header.Mode)
		}

		// Strip header from SQL before execution
		strippedSQL := migrationpkg.StripHeader(sql)

		// Collect depends_on from header
		var dependsOn []string
		if header != nil && len(header.DependsOn) > 0 {
			dependsOn = header.DependsOn
		}

		allMigrations = append(allMigrations, db.Migration{
			Name:      dir,
			SQL:       strippedSQL,
			Checksum:  checksum,
			Mode:      mode,
			DependsOn: dependsOn,
		})
	}

	return allMigrations, nil
}

// applyMigrationsToCleanDatabase creates a clean shadow database and applies all migrations
// It uses checkpoints to optimize validation when available
func applyMigrationsToCleanDatabase(ctx context.Context, migrations []db.Migration, showProgress bool) (*schema.Schema, error) {
	fs := afero.NewOsFs()

	// Try to find a valid checkpoint to skip some migrations
	checkpoint, checkpointIdx, err := findLatestValidCheckpoint(fs, migrations)
	if err != nil && showProgress {
		fmt.Println(ui.Warning(fmt.Sprintf("  Warning: error finding checkpoint: %v", err)))
	}

	var client *db.Client
	var startIndex int

	if checkpoint != nil {
		// Use checkpoint as starting point
		if showProgress {
			fmt.Println(ui.Subtle(fmt.Sprintf("  Using checkpoint from %s (skipping %d migration(s))",
				checkpoint.MigrationName, checkpointIdx+1)))
		}

		// Parse checkpoint schema content
		checkpointStatements, parseErr := schema.ParseSQL(checkpoint.SchemaContent)
		if parseErr != nil {
			// Fall back to full validation if checkpoint can't be parsed
			if showProgress {
				fmt.Println(ui.Warning(fmt.Sprintf("  Warning: failed to parse checkpoint, starting from scratch: %v", parseErr)))
			}
			checkpoint = nil
		} else {
			// Convert parsed statements to string slice for GetShadowDB
			var stmtStrings []string
			for _, stmt := range checkpointStatements {
				stmtStrings = append(stmtStrings, stmt.String())
			}

			client, err = db.GetShadowDB(ctx, stmtStrings...)
			if err != nil {
				return nil, err
			}
			startIndex = checkpointIdx + 1
		}
	}

	if checkpoint == nil {
		// No valid checkpoint, start from scratch
		if showProgress && len(migrations) > 0 {
			fmt.Println(ui.Subtle("  No valid checkpoint found, starting from empty database"))
		}

		client, err = db.GetShadowDB(ctx)
		if err != nil {
			return nil, err
		}
		startIndex = 0
	}
	defer client.Close()

	// Keep autocommit_before_ddl enabled (production behavior) so we catch
	// migrations that would fail due to transaction boundary issues.
	client.SetDisableAutocommitDDL(false)

	// Apply remaining migrations
	for i := startIndex; i < len(migrations); i++ {
		mig := migrations[i]
		if showProgress {
			fmt.Println(ui.Subtle(fmt.Sprintf("  Applying migration %d/%d: %s", i+1, len(migrations), mig.Name)))
		}

		start := time.Now()
		err = client.ExecuteBulkDDL(ctx, mig.SQL)
		duration := time.Since(start)

		if err != nil {
			return nil, fmt.Errorf("failed to apply migration %s: %w", mig.Name, err)
		}

		if showProgress {
			fmt.Println(ui.Success(fmt.Sprintf("    ✓ Completed in %v", duration.Round(time.Millisecond))))
		}
	}

	// Get the schema from the database
	return schema.LoadFromDatabase(ctx, client)
}

// ensureCheckpointForLastMigration creates a checkpoint for the last migration if one doesn't exist
func ensureCheckpointForLastMigration(fs afero.Fs, migrations []db.Migration, resultSchema *schema.Schema, showProgress bool) error {
	if len(migrations) == 0 {
		return nil
	}

	lastMigration := migrations[len(migrations)-1]
	migrationDir := filepath.Join(flags.MigrationDir, lastMigration.Name)

	// Check if checkpoint already exists
	checkpoint, err := loadCheckpoint(fs, migrationDir)
	if err != nil {
		// Error loading checkpoint, try to create a new one
		if showProgress {
			fmt.Println(ui.Subtle(fmt.Sprintf("→ Checkpoint error for %s, regenerating...", lastMigration.Name)))
		}
	} else if checkpoint != nil {
		// Checkpoint exists, validate it
		expectedHash := computeMigrationsHash(migrations)
		if checkpoint.Header.MigrationsHash == expectedHash {
			if err := validateCheckpoint(checkpoint); err == nil {
				// Checkpoint is valid, nothing to do
				return nil
			}
		}
		// Checkpoint exists but is invalid, regenerate it
		if showProgress {
			fmt.Println(ui.Subtle(fmt.Sprintf("→ Checkpoint invalid for %s, regenerating...", lastMigration.Name)))
		}
	} else {
		// No checkpoint exists
		if showProgress {
			fmt.Println(ui.Subtle(fmt.Sprintf("→ Creating checkpoint for %s...", lastMigration.Name)))
		}
	}

	// Create the checkpoint
	if err := createCheckpointForMigration(fs, migrations, resultSchema, migrationDir); err != nil {
		return err
	}

	if showProgress {
		fmt.Println(ui.Success("  ✓ Checkpoint created"))
	}

	return nil
}
