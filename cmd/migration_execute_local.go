package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	migrationpkg "github.com/pjtatlow/scurry/internal/migration"
	"github.com/pjtatlow/scurry/internal/recovery"
	"github.com/pjtatlow/scurry/internal/schema"
	"github.com/pjtatlow/scurry/internal/ui"
)

var (
	migrationLocalName    string
	migrationLocalSQLPath string
	migrationLocalDryRun  bool
	migrationLocalStrict  bool
)

// errMigrationLocalNotConverged is returned under --strict when the schema definitions,
// the migration snapshot, and the database have not fully converged.
var errMigrationLocalNotConverged = errors.New("schema, migrations, and database are not in sync")

var migrationLocalCmd = &cobra.Command{
	Use:   "execute-local",
	Short: "Author, apply, and reconcile a migration against a dev database",
	Long: `Author the next migration and apply it to a development database in one step.

This is the migration-first local dev loop: it catches up any pending migrations,
authors a new migration (auto-generated from the schema diff, or supplied via
--migration-sql for custom SQL), applies it to the database, records it, advances the
schema.sql snapshot, and reports any remaining drift between the schema definitions,
the migrations, and the database.

Examples:
  # Auto-generate a migration from the schema diff and apply it
  scurry migration execute-local --db-url=... --definitions=./definitions

  # Supply a custom migration (e.g. a data backfill) from stdin
  echo "UPDATE users SET status='active' WHERE status IS NULL;" | \
    scurry migration execute-local --migration-sql - --name backfill_status --db-url=...
`,
	RunE: runMigrationLocal,
}

func init() {
	migrationCmd.AddCommand(migrationLocalCmd)

	flags.AddDbUrl(migrationLocalCmd)
	flags.AddDefinitionDirs(migrationLocalCmd)

	migrationLocalCmd.Flags().StringVar(&migrationLocalName, "name", "", "Name for the migration (skips prompt)")
	migrationLocalCmd.Flags().StringVar(&migrationLocalSQLPath, "migration-sql", "",
		"Path to a .sql file (or - for stdin) to use as the entire migration body instead of diffing")
	migrationLocalCmd.Flags().BoolVar(&migrationLocalDryRun, "dry-run", false, "Preview without writing files or touching the database")
	migrationLocalCmd.Flags().BoolVar(&migrationLocalStrict, "strict", false, "Exit non-zero if schema, migrations, and database have not converged")
}

func runMigrationLocal(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	if flags.DbUrl == "" {
		return fmt.Errorf("database URL is required (use --db-url or CRDB_URL env var)")
	}
	if len(flags.DefinitionDirs) == 0 {
		return fmt.Errorf("definition directory is required (use --definitions)")
	}

	fs := afero.NewOsFs()

	// Resolve the supplied migration body here (file or stdin) so the core stays
	// free of process I/O and remains unit-testable.
	var suppliedSQL string
	var useSupplied bool
	forceFromStdin := false
	if migrationLocalSQLPath != "" {
		useSupplied = true
		if migrationLocalSQLPath == "-" {
			b, err := io.ReadAll(os.Stdin)
			if err != nil {
				return fmt.Errorf("failed to read migration SQL from stdin: %w", err)
			}
			suppliedSQL = string(b)
			// stdin has been consumed; there is no channel left for prompts.
			forceFromStdin = true
		} else {
			b, err := afero.ReadFile(fs, migrationLocalSQLPath)
			if err != nil {
				return fmt.Errorf("failed to read migration SQL file: %w", err)
			}
			suppliedSQL = string(b)
		}
	}

	client, err := db.Connect(ctx, flags.DbUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer client.Close()

	opts := MigrationLocalOptions{
		Fs:             fs,
		DefinitionDirs: flags.DefinitionDirs,
		DbClient:       client,
		SuppliedSQL:    suppliedSQL,
		UseSuppliedSQL: useSupplied,
		Name:           migrationLocalName,
		Force:          flags.Force || forceFromStdin,
		DryRun:         migrationLocalDryRun,
		Strict:         migrationLocalStrict,
		Verbose:        flags.Verbose,
	}

	errCtx := &ErrorContext{}
	_, err = executeMigrationLocal(ctx, opts, errCtx)
	if err != nil {
		reportPath, reportErr := writeErrorReport(errCtx, err)
		if reportErr != nil {
			fmt.Println(ui.Warning(fmt.Sprintf("Failed to write error report: %s", reportErr)))
		} else if reportPath != "" {
			fmt.Println(ui.Info(fmt.Sprintf("Error report written to: %s", reportPath)))
		}
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return nil
}

// MigrationLocalOptions contains options for the migration execute-local operation.
type MigrationLocalOptions struct {
	Fs             afero.Fs
	DefinitionDirs []string
	DbClient       *db.Client // live dev database
	SuppliedSQL    string     // raw body already read from file/stdin
	UseSuppliedSQL bool       // true when --migration-sql was given (an empty body is invalid)
	Name           string
	Force          bool
	DryRun         bool
	Strict         bool
	Verbose        bool
}

// MigrationLocalResult contains the outcome of a migration execute-local operation.
type MigrationLocalResult struct {
	CaughtUp      int  // migrations run during catch-up
	Baselined     bool // an untracked, populated database was adopted
	Authored      bool // a new migration file was created
	MigrationDir  string
	Applied       bool // the new migration was applied (or recorded as already satisfied)
	SchemaDrift   bool // definitions vs snapshot differ
	DatabaseDrift bool // snapshot vs database differ
	Converged     bool // !SchemaDrift && !DatabaseDrift
}

func executeMigrationLocal(ctx context.Context, opts MigrationLocalOptions, errCtx *ErrorContext) (*MigrationLocalResult, error) {
	result := &MigrationLocalResult{}
	fs := opts.Fs
	dbClient := opts.DbClient

	if err := validateMigrationsDir(fs); err != nil {
		return result, err
	}

	// 1. Catch up pending migrations first, so the database matches the migration
	// history before we author on top of it.
	preAuthorMigs, err := loadMigrations(fs)
	if err != nil {
		return result, err
	}
	baseline, caughtUp, err := catchUpMigrations(ctx, fs, dbClient, opts.Force, opts.DryRun, opts.Verbose)
	if err != nil {
		if errors.Is(err, errMigrationCanceled) {
			return result, nil
		}
		return result, err
	}
	result.Baselined = baseline
	result.CaughtUp = caughtUp

	// 2. Load the snapshot and resolve the migration body.
	prodSchema, err := loadProductionSchema(ctx, fs)
	if err != nil {
		return result, fmt.Errorf("failed to load production schema: %w", err)
	}
	errCtx.RemoteSchema = prodSchema

	var statements []string
	var rawBody string
	var header *migrationpkg.Header

	if opts.UseSuppliedSQL {
		// --- Supplied-SQL path: the body is used verbatim, no diffing. ---
		// Scurry owns the `-- scurry:` header (mode/depends_on); users author only the
		// migration body, so reject a hand-written header rather than honoring it.
		if hdr, hdrErr := migrationpkg.ParseHeader(opts.SuppliedSQL); hdr != nil || hdrErr != nil {
			return result, fmt.Errorf("do not include a '-- scurry:' header in supplied SQL; scurry manages migration metadata (mode is classified automatically from table sizes)")
		}

		rawBody = opts.SuppliedSQL
		if strings.TrimSpace(rawBody) == "" {
			return result, fmt.Errorf("supplied migration SQL is empty")
		}

		// Parse once: the string statements drive validation/dependency detection, and
		// the ASTs drive sync/async classification.
		parsed, err := parser.Parse(rawBody)
		if err != nil {
			return result, fmt.Errorf("failed to parse supplied migration SQL: %w", err)
		}
		stmtAST := make([]tree.Statement, len(parsed))
		for i, p := range parsed {
			statements = append(statements, p.AST.String())
			stmtAST[i] = p.AST
		}
		errCtx.Statements = statements

		// Classify the supplied statements against table sizes, the same rules the diff
		// path uses. With no table_sizes.yaml this is always sync.
		tableSizes, err := migrationpkg.LoadTableSizes(fs, flags.MigrationDir)
		if err != nil {
			return result, fmt.Errorf("failed to load table_sizes.yaml: %w", err)
		}
		classifyResult := migrationpkg.ClassifyStatements(stmtAST, tableSizes)
		if classifyResult.Mode == migrationpkg.ModeAsync {
			fmt.Println()
			fmt.Println(ui.Warning("Migration classified as async:"))
			for _, reason := range classifyResult.Reasons {
				fmt.Printf("  - %s\n", reason)
			}
			fmt.Println()
		}
		header = &migrationpkg.Header{Mode: classifyResult.Mode}
	} else {
		// --- Diff path: author from the definitions-vs-snapshot diff. ---
		localSchema, err := loadDefinitionsSchema(ctx, fs, opts.DefinitionDirs)
		if err != nil {
			return result, fmt.Errorf("failed to load local schema: %w", err)
		}
		errCtx.LocalSchema = localSchema

		diffResult := schema.Compare(localSchema, prodSchema)
		if !diffResult.HasChanges() {
			// Nothing new to author. Catch-up may still have advanced the DB, so
			// reconcile and report rather than treating this as an error.
			if caughtUp == 0 && !baseline {
				fmt.Println(ui.Success("✓ No schema changes to author"))
			} else {
				fmt.Println(ui.Info("No schema changes to author."))
			}
			if baseline && !opts.DryRun {
				if err := markAllMigrationsComplete(ctx, dbClient, preAuthorMigs); err != nil {
					return result, fmt.Errorf("failed to baseline migrations: %w", err)
				}
			}
			return finishReconcile(ctx, opts, result)
		}

		// Prompt for USING expressions on column type changes (interactive only).
		if !opts.Force {
			if err := promptForUsingExpressionsGen(diffResult); err != nil {
				return result, err
			}
		}

		statements, _, err = diffResult.GenerateMigrations(true)
		if err != nil {
			return result, fmt.Errorf("failed to generate migrations: %w", err)
		}
		errCtx.Statements = statements

		tableSizes, err := migrationpkg.LoadTableSizes(fs, flags.MigrationDir)
		if err != nil {
			return result, fmt.Errorf("failed to load table_sizes.yaml: %w", err)
		}
		classifyResult := migrationpkg.ClassifyDifferences(diffResult.Differences, tableSizes)
		if classifyResult.Mode == migrationpkg.ModeAsync {
			fmt.Println()
			fmt.Println(ui.Warning("Migration classified as async:"))
			for _, reason := range classifyResult.Reasons {
				fmt.Printf("  - %s\n", reason)
			}
			fmt.Println()
		}
		header = &migrationpkg.Header{Mode: classifyResult.Mode}
		rawBody = ""
	}

	// 3. Author the migration file (validate on shadow, resolve name, write).
	dirName, newSchema, err := finalizeAuthoredMigration(ctx, fs, prodSchema, statements, rawBody, header, opts.Name, opts.Force, opts.DryRun, opts.Verbose)
	if err != nil {
		return result, err
	}
	if opts.DryRun {
		fmt.Println(ui.Info("ℹ Dry run mode - no migration written, nothing applied."))
		return result, nil
	}
	result.Authored = true
	result.MigrationDir = dirName

	// 4. Baseline: record the pre-existing migrations before applying the new one,
	// so the newly authored migration is the only unapplied one.
	if baseline {
		if err := markAllMigrationsComplete(ctx, dbClient, preAuthorMigs); err != nil {
			return result, fmt.Errorf("failed to baseline migrations: %w", err)
		}
		fmt.Println(ui.Success(fmt.Sprintf("✓ Marked %d existing migration(s) as applied (baseline)", len(preAuthorMigs))))
	}

	// 5. Apply the new migration via the execute core.
	all, err := loadMigrations(fs)
	if err != nil {
		return result, err
	}
	applied, err := dbClient.GetAppliedMigrations(ctx)
	if err != nil {
		return result, err
	}
	unapplied, warnings, err := filterUnappliedMigrations(all, applied)
	if err != nil {
		return result, err
	}
	for _, w := range warnings {
		fmt.Println(ui.Warning(w))
	}

	fmt.Println()
	fmt.Println(ui.Info("⟳ Applying migration..."))
	_, _, applyErr := runMigrationList(ctx, dbClient, unapplied)
	if applyErr == nil {
		result.Applied = true
	}

	// 6. Advance the snapshot from the shadow-validated schema (the snapshot tracks
	// the sum of migration files; reconcile below surfaces any gap versus the DB).
	if err := dumpProductionSchema(ctx, fs, newSchema); err != nil {
		return result, fmt.Errorf("failed to update schema.sql: %w", err)
	}
	fmt.Println(ui.Success(fmt.Sprintf("✓ Updated %s", getSchemaFilePath())))

	// 7. Three-way reconcile (compute before reporting so apply-failure handling can
	// consult the database drift).
	if err := computeReconcile(ctx, opts, result); err != nil {
		return result, err
	}

	// 8. Apply-failure semantics: auto-complete if the database already matches.
	if applyErr != nil {
		if result.DatabaseDrift {
			return result, fmt.Errorf("failed to apply migration %s: %w\nRun 'scurry migration recover' or 'scurry migration execute-local' again", dirName, applyErr)
		}
		if err := markAllMigrationsComplete(ctx, dbClient, unapplied); err != nil {
			return result, fmt.Errorf("failed to record already-satisfied migration: %w", err)
		}
		result.Applied = true
		fmt.Println(ui.Warning("Migration already satisfied by the database; recorded as applied."))
	}

	// 9. Report and enforce --strict.
	reportReconcile(result)
	if opts.Strict && !result.Converged {
		return result, errMigrationLocalNotConverged
	}

	return result, nil
}

// loadDefinitionsSchema loads the schema definitions (the "true" desired schema) via
// an ephemeral shadow database, mirroring how migration gen loads them.
func loadDefinitionsSchema(ctx context.Context, fs afero.Fs, dirs []string) (*schema.Schema, error) {
	shadow, err := db.GetShadowDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get shadow database client: %w", err)
	}
	defer shadow.Close()
	return schema.LoadFromDirectories(ctx, fs, dirs, shadow)
}

// computeReconcile loads the three schema states and records how they diverge:
//   - SchemaDrift:   definitions (T) vs snapshot (S) — migrations don't produce the schema
//   - DatabaseDrift: snapshot (S) vs database (D)    — the DB drifted from the migrations
func computeReconcile(ctx context.Context, opts MigrationLocalOptions, result *MigrationLocalResult) error {
	t, err := loadDefinitionsSchema(ctx, opts.Fs, opts.DefinitionDirs)
	if err != nil {
		return fmt.Errorf("failed to load schema definitions for reconcile: %w", err)
	}
	s, err := loadProductionSchema(ctx, opts.Fs)
	if err != nil {
		return fmt.Errorf("failed to load snapshot for reconcile: %w", err)
	}
	d, err := schema.LoadFromDatabase(ctx, opts.DbClient)
	if err != nil {
		return fmt.Errorf("failed to load database schema for reconcile: %w", err)
	}

	result.SchemaDrift = schema.Compare(t, s).HasChanges()
	result.DatabaseDrift = schema.Compare(s, d).HasChanges()
	result.Converged = !result.SchemaDrift && !result.DatabaseDrift
	return nil
}

// reportReconcile prints the reconcile outcome with targeted next steps.
func reportReconcile(result *MigrationLocalResult) {
	fmt.Println()
	if result.SchemaDrift {
		fmt.Println(ui.Warning("Your migrations do not fully produce your declared schema."))
		fmt.Println(ui.Info("  Run 'scurry migration execute-local' again to author the remaining changes."))
	}
	if result.DatabaseDrift {
		fmt.Println(ui.Warning("Your database has drifted from the sum of your migrations."))
		fmt.Println(ui.Info("  Run 'scurry push' to reconcile the database."))
	}
	if result.Converged {
		fmt.Println(ui.Success("✓ Schema, migrations, and database are in sync."))
	}
}

// finishReconcile computes, reports, and enforces --strict for paths that authored
// nothing new (e.g. catch-up only, or no schema changes).
func finishReconcile(ctx context.Context, opts MigrationLocalOptions, result *MigrationLocalResult) (*MigrationLocalResult, error) {
	if err := computeReconcile(ctx, opts, result); err != nil {
		return result, err
	}
	reportReconcile(result)
	if opts.Strict && !result.Converged {
		return result, errMigrationLocalNotConverged
	}
	return result, nil
}

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
