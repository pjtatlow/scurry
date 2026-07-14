package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	migrationpkg "github.com/pjtatlow/scurry/internal/migration"
	"github.com/pjtatlow/scurry/internal/schema"
	"github.com/pjtatlow/scurry/internal/ui"
)

const (
	signaturesNoVerify = "no-verify"
	signaturesVerify   = "verify"
	signaturesRequire  = "require"
	signaturesFix      = "fix"
)

var (
	validateOverwrite    bool
	validateNoCheckpoint bool
	validateSignatures   string
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
	migrationValidateCmd.Flags().StringVar(&validateSignatures, "signatures", signaturesNoVerify, "Signature handling mode: no-verify, verify, require, or fix")
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
	if err := validateSignaturesMode(validateSignatures); err != nil {
		return err
	}

	// Validate migrations directory
	if err := validateMigrationsDir(fs); err != nil {
		return err
	}

	// Validate schema.sql exists and is parseable (skip in overwrite mode)
	if !validateOverwrite {
		if flags.Verbose {
			fmt.Println(ui.Subtle("→ Validating schema.sql..."))
		}
		if err := validateSchemaFile(ctx, fs); err != nil {
			return err
		}
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

	signaturesOnly, err := handleMigrationSignatures(fs, migrations, validateSignatures)
	if err != nil {
		return err
	}
	if signaturesOnly {
		return nil
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

// validateSchemaFile checks that schema.sql exists, contains valid SQL, and
// can be applied to a clean database (catching semantic errors like duplicate types).
func validateSchemaFile(ctx context.Context, fs afero.Fs) error {
	schemaPath := getSchemaFilePath()

	exists, err := afero.Exists(fs, schemaPath)
	if err != nil {
		return fmt.Errorf("failed to check schema.sql: %w", err)
	}
	if !exists {
		return fmt.Errorf("schema.sql not found at %s\nRun 'scurry migration validate --overwrite' to create it", schemaPath)
	}

	content, err := afero.ReadFile(fs, schemaPath)
	if err != nil {
		return fmt.Errorf("failed to read schema.sql: %w", err)
	}

	sql := strings.TrimSpace(string(content))
	if sql == "" {
		return fmt.Errorf("schema.sql is empty at %s\nRun 'scurry migration validate --overwrite' to populate it", schemaPath)
	}

	statements, err := schema.ParseSQL(sql)
	if err != nil {
		return fmt.Errorf("schema.sql contains invalid SQL: %w", err)
	}

	// Apply to a clean database to catch semantic errors (e.g. duplicate types, missing references)
	var stmtStrings []string
	for _, stmt := range statements {
		stmtStrings = append(stmtStrings, stmt.String())
	}

	client, err := db.GetShadowDB(ctx, stmtStrings...)
	if err != nil {
		return fmt.Errorf("schema.sql cannot be applied to a clean database: %w", err)
	}
	client.Close()

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

		squash := header != nil && header.Squash

		allMigrations = append(allMigrations, db.Migration{
			Name:      dir,
			SQL:       strippedSQL,
			Checksum:  checksum,
			Mode:      mode,
			DependsOn: dependsOn,
			Squash:    squash,
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

	// Apply remaining migrations
	for i := startIndex; i < len(migrations); i++ {
		mig := migrations[i]
		if showProgress {
			fmt.Println(ui.Subtle(fmt.Sprintf("  Applying migration %d/%d: %s", i+1, len(migrations), mig.Name)))
		}

		start := time.Now()

		// Split the migration SQL into individual statements before executing.
		// ExecuteBulkDDL expects individual statements, not a single blob.
		statements, splitErr := db.SplitStatements(mig.SQL)
		if splitErr != nil {
			return nil, fmt.Errorf("failed to parse migration %s: %w", mig.Name, splitErr)
		}

		// Squash migrations contain clean CREATE statements (a schema snapshot),
		// so they can safely run with autocommit disabled for speed. Real
		// migrations run with autocommit enabled (production behavior) to catch
		// transaction boundary issues.
		if mig.Squash {
			client.SetDisableAutocommitDDL(true)
		} else {
			client.SetDisableAutocommitDDL(false)
		}

		err = client.ExecuteBulkDDL(ctx, statements...)
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

// sigStatus is the signature state of a single migration's header.
type sigStatus int

const (
	sigOK      sigStatus = iota // present and matches
	sigMissing                  // no header, or header without a signature
	sigInvalid                  // signature present but does not match (edited/forged/malformed)
)

// checkMigrationSignature reads a migration file and classifies its header signature.
func checkMigrationSignature(fs afero.Fs, name string) (sigStatus, error) {
	raw, err := afero.ReadFile(fs, filepath.Join(flags.MigrationDir, name, "migration.sql"))
	if err != nil {
		return sigInvalid, fmt.Errorf("failed to read migration %s: %w", name, err)
	}
	content := string(raw)

	header, err := migrationpkg.ParseHeader(content)
	if err != nil {
		// A malformed header can't have a valid signature.
		return sigInvalid, nil
	}
	if header == nil || header.Sig == "" {
		return sigMissing, nil
	}
	sig, err := migrationpkg.ComputeSig(header, migrationpkg.StripHeader(content))
	if err != nil || sig != header.Sig {
		return sigInvalid, nil
	}
	return sigOK, nil
}

func validateSignaturesMode(mode string) error {
	switch mode {
	case signaturesNoVerify, signaturesVerify, signaturesRequire, signaturesFix:
		return nil
	default:
		return fmt.Errorf("invalid --signatures value %q: must be one of no-verify, verify, require, or fix", mode)
	}
}

// handleMigrationSignatures applies the requested signature mode. The returned bool is
// true for fix mode, which is a distinct operation that exits after rewriting headers.
func handleMigrationSignatures(fs afero.Fs, migrations []db.Migration, mode string) (bool, error) {
	switch mode {
	case signaturesNoVerify:
		return false, nil
	case signaturesVerify:
		return false, verifyAndReportSignatures(fs, migrations, false)
	case signaturesRequire:
		return false, verifyAndReportSignatures(fs, migrations, true)
	case signaturesFix:
		return true, signMigrationHeaders(fs, migrations)
	default:
		return false, fmt.Errorf("invalid --signatures value %q", mode)
	}
}

// verifyAndReportSignatures verifies every migration's header signature. An invalid
// signature (edited or hand-authored header) is always a failure; a missing signature is
// a warning unless require is true. Backfill existing migrations with --signatures=fix.
func verifyAndReportSignatures(fs afero.Fs, migrations []db.Migration, require bool) error {
	var invalid, missing []string
	for _, m := range migrations {
		status, err := checkMigrationSignature(fs, m.Name)
		if err != nil {
			return err
		}
		switch status {
		case sigInvalid:
			invalid = append(invalid, m.Name)
		case sigMissing:
			missing = append(missing, m.Name)
		}
	}

	for _, name := range invalid {
		fmt.Println(ui.Error(fmt.Sprintf("✗ %s: invalid scurry header signature — the header was hand-authored or edited. Regenerate it with scurry (never hand-author the '-- scurry:' header).", name)))
	}
	for _, name := range missing {
		if require {
			fmt.Println(ui.Error(fmt.Sprintf("✗ %s: missing scurry header signature", name)))
		} else {
			fmt.Println(ui.Warning(fmt.Sprintf("⚠ %s: missing scurry header signature (run 'scurry migration validate --signatures=fix' to backfill)", name)))
		}
	}

	if len(invalid) > 0 || (require && len(missing) > 0) {
		return fmt.Errorf("migration header signature check failed")
	}
	return nil
}

// signMigrationHeaders (re)writes the scurry signature on every migration. A migration
// that already has a header is blessed as-is (its mode/depends_on are preserved and
// signed); a header-less migration is classified from its body so it gains a valid
// header. The result is reviewable in version control before committing.
func signMigrationHeaders(fs afero.Fs, migrations []db.Migration) error {
	changed := 0
	for i, m := range migrations {
		path := filepath.Join(flags.MigrationDir, m.Name, "migration.sql")
		raw, err := afero.ReadFile(fs, path)
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", m.Name, err)
		}
		content := string(raw)
		body := migrationpkg.StripHeader(content)

		header, err := migrationpkg.ParseHeader(content)
		if err != nil {
			return fmt.Errorf("migration %s has an unparseable header; fix it before signing: %w", m.Name, err)
		}
		if header == nil {
			// No header at all: classify the body so it gains a valid one.
			header, err = deriveHeaderForBody(fs, body, migrations[:i])
			if err != nil {
				return fmt.Errorf("failed to derive header for %s: %w", m.Name, err)
			}
		}

		if err := migrationpkg.SignHeader(header, body); err != nil {
			return fmt.Errorf("failed to sign migration %s: %w", m.Name, err)
		}
		newContent := migrationpkg.FormatHeader(header) + "\n" + body
		if newContent == content {
			continue
		}
		if err := afero.WriteFile(fs, path, []byte(newContent), 0644); err != nil {
			return fmt.Errorf("failed to write migration %s: %w", m.Name, err)
		}
		changed++
	}

	fmt.Println(ui.Success(fmt.Sprintf("✓ Signed %d migration(s) (%d already up to date)", changed, len(migrations)-changed)))
	return nil
}

// headerForStatements builds the canonical header scurry should write for a set of
// migration statements: it classifies them sync/async against table sizes and detects
// dependencies on prior migrations. When announce is true and the result is async, the
// classification reasons are printed. This is the single place custom/manually-authored
// migrations get their header, so it can never be hand-supplied.
func headerForStatements(fs afero.Fs, stmts []tree.Statement, prior []db.Migration, announce bool) (*migrationpkg.Header, error) {
	tableSizes, err := migrationpkg.LoadTableSizes(fs, flags.MigrationDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load table_sizes.yaml: %w", err)
	}

	result := migrationpkg.ClassifyStatements(stmts, tableSizes)
	if announce && result.Mode == migrationpkg.ModeAsync {
		fmt.Println()
		fmt.Println(ui.Warning("Migration classified as async:"))
		for _, reason := range result.Reasons {
			fmt.Printf("  - %s\n", reason)
		}
		fmt.Println()
	}

	header := &migrationpkg.Header{Mode: result.Mode}

	migInfos := make([]migrationpkg.MigrationInfo, len(prior))
	for i, m := range prior {
		migInfos[i] = migrationpkg.MigrationInfo{Name: m.Name, SQL: m.SQL}
	}
	header.DependsOn = migrationpkg.FindDependencies(stmts, migInfos)

	return header, nil
}

// deriveHeaderForBody builds a canonical header for a header-less migration body by
// parsing it and classifying its statements.
func deriveHeaderForBody(fs afero.Fs, body string, prior []db.Migration) (*migrationpkg.Header, error) {
	parsed, err := parser.Parse(body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse migration body: %w", err)
	}
	stmts := make([]tree.Statement, len(parsed))
	for i, p := range parsed {
		stmts[i] = p.AST
	}
	return headerForStatements(fs, stmts, prior, false)
}
