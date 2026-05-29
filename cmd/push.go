package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	migrationpkg "github.com/pjtatlow/scurry/internal/migration"
	"github.com/pjtatlow/scurry/internal/recovery"
	"github.com/pjtatlow/scurry/internal/schema"
	"github.com/pjtatlow/scurry/internal/ui"
)

var pushCmd = &cobra.Command{
	Use:   "push",
	Short: "Push local schema changes to the database",
	Long: `Push local schema changes to the database by applying the necessary migrations.
This will compare the local schema with the database schema and apply the differences.
All non-system schemas will be pushed automatically.`,
	RunE: push,
}

var (
	pushDryRun         bool
	pushWithMigrations bool
)

// errPushCanceled signals that the user aborted the push (e.g. declined to run
// migrations or aborted recovery). It is handled in executePush as a clean exit.
var errPushCanceled = errors.New("push canceled")

func init() {
	rootCmd.AddCommand(pushCmd)

	flags.AddDbUrl(pushCmd)
	flags.AddDefinitionDirs(pushCmd)
	flags.AddMigrationDir(pushCmd)

	pushCmd.Flags().BoolVar(&pushDryRun, "dry-run", false, "Show what would be executed without applying changes")
	pushCmd.Flags().BoolVar(&pushWithMigrations, "with-migrations", false, "Run pending migrations before computing and applying the schema diff")
}

func push(cmd *cobra.Command, args []string) error {
	// Validate required flags
	if flags.DbUrl == "" {
		return fmt.Errorf("database URL is required (use --db-url or CRDB_URL env var)")
	}
	if len(flags.DefinitionDirs) == 0 {
		return fmt.Errorf("definition directory is required (use --definitions)")
	}

	err := doPush(cmd.Context())
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return nil
}

// PushOptions contains options for the push operation
type PushOptions struct {
	Fs             afero.Fs
	DefinitionDirs []string
	DbClient       *db.Client
	Verbose        bool
	DryRun         bool
	Force          bool
	RunMigrations  bool
}

// PushResult contains the result of a push operation
type PushResult struct {
	Statements []string
	HasChanges bool
}

// ErrorContext tracks schema and migration state for error reporting
type ErrorContext struct {
	LocalSchema  *schema.Schema
	RemoteSchema *schema.Schema
	Statements   []string
}

// ErrorReportFile represents the YAML structure of an error report
type ErrorReportFile struct {
	Generated    string   `yaml:"generated"`
	Error        string   `yaml:"error"`
	LocalSchema  []string `yaml:"local_schema"`
	RemoteSchema []string `yaml:"remote_schema"`
	Migrations   []string `yaml:"migrations,omitempty"`
}

// generateErrorReportContent generates the YAML content for an error report
func generateErrorReportContent(errCtx *ErrorContext, err error) ([]byte, error) {
	report := ErrorReportFile{
		Generated:    time.Now().Format(time.RFC3339),
		Error:        err.Error(),
		LocalSchema:  errCtx.LocalSchema.OriginalStatements,
		RemoteSchema: errCtx.RemoteSchema.OriginalStatements,
		Migrations:   errCtx.Statements,
	}

	return yaml.Marshal(report)
}

// writeErrorReport writes a YAML error report to a temp file
func writeErrorReport(errCtx *ErrorContext, err error) (string, error) {
	if errCtx.LocalSchema == nil || errCtx.RemoteSchema == nil {
		return "", nil
	}

	content, yamlErr := generateErrorReportContent(errCtx, err)
	if yamlErr != nil {
		return "", fmt.Errorf("failed to generate error report: %w", yamlErr)
	}

	tmpFile, fileErr := os.CreateTemp("", "scurry-push-error-*.yaml")
	if fileErr != nil {
		return "", fmt.Errorf("failed to create error report file: %w", fileErr)
	}
	defer tmpFile.Close()

	if _, fileErr := tmpFile.Write(content); fileErr != nil {
		return "", fmt.Errorf("failed to write error report: %w", fileErr)
	}

	return tmpFile.Name(), nil
}

func doPush(ctx context.Context) error {

	client, err := db.Connect(ctx, flags.DbUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer client.Close()

	opts := PushOptions{
		Fs:             afero.NewOsFs(),
		DefinitionDirs: flags.DefinitionDirs,
		DbClient:       client,
		Verbose:        flags.Verbose,
		DryRun:         pushDryRun,
		Force:          flags.Force,
		RunMigrations:  pushWithMigrations,
	}

	errCtx := &ErrorContext{}
	_, err = executePush(ctx, opts, errCtx)
	if err != nil {
		reportPath, reportErr := writeErrorReport(errCtx, err)
		if reportErr != nil {
			fmt.Println(ui.Warning(fmt.Sprintf("Failed to write error report: %s", reportErr)))
		} else if reportPath != "" {
			fmt.Println(ui.Info(fmt.Sprintf("Error report written to: %s", reportPath)))
		}
	}
	return err
}

func executePush(ctx context.Context, opts PushOptions, errCtx *ErrorContext) (*PushResult, error) {
	// Run pending tracked migrations against the live database first, so that the
	// diff captures any custom changes other developers committed as migrations
	// (data backfills, DDL the auto-diff can't express) instead of reverting them.
	baseline := false
	if opts.RunMigrations {
		var err error
		baseline, err = runMigrationsBeforePush(ctx, opts)
		if err != nil {
			if errors.Is(err, errPushCanceled) {
				return &PushResult{HasChanges: false, Statements: []string{}}, nil
			}
			return nil, err
		}
	}

	// finalizeBaseline marks all on-disk migrations as applied. It is only armed
	// when adopting a previously untracked, non-empty database: the migrations
	// aren't run (they'd fail against the existing schema), the normal push
	// reconciles the schema, and then we record the migrations as the baseline.
	finalizeBaseline := func() error {
		if !baseline || opts.DryRun {
			return nil
		}
		migs, err := loadMigrations(opts.Fs)
		if err != nil {
			return err
		}
		if err := markAllMigrationsComplete(ctx, opts.DbClient, migs); err != nil {
			return fmt.Errorf("failed to baseline migrations: %w", err)
		}
		fmt.Println(ui.Success(fmt.Sprintf("✓ Marked %d existing migration(s) as applied (baseline)", len(migs))))
		return nil
	}

	// Load local schema from files
	if opts.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading local schema from %s...", strings.Join(opts.DefinitionDirs, ", "))))
	}

	dbClient, err := db.GetShadowDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer dbClient.Close()

	localSchema, err := schema.LoadFromDirectories(ctx, opts.Fs, opts.DefinitionDirs, dbClient)
	if err != nil {
		return nil, fmt.Errorf("failed to load local schema: %w", err)
	}
	errCtx.LocalSchema = localSchema

	if opts.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Found %d tables, %d types, %d routines, %d sequences, %d views locally",
			len(localSchema.Tables), len(localSchema.Types), len(localSchema.Routines), len(localSchema.Sequences), len(localSchema.Views))))
	}

	// Load remote schema from database (all schemas)
	if opts.Verbose {
		fmt.Println(ui.Subtle("→ Loading database schema..."))
	}

	remoteSchema, err := schema.LoadFromDatabase(ctx, opts.DbClient)
	if err != nil {
		return nil, fmt.Errorf("failed to load database schema: %w", err)
	}
	errCtx.RemoteSchema = remoteSchema

	if opts.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Found %d tables, %d types, %d routines, %d sequences, %d views in database",
			len(remoteSchema.Tables), len(remoteSchema.Types), len(remoteSchema.Routines), len(remoteSchema.Sequences), len(remoteSchema.Views))))
	}

	// Compare schemas
	if opts.Verbose {
		fmt.Println()
		fmt.Println(ui.Subtle("→ Comparing schemas..."))
	}

	diffResult := schema.Compare(localSchema, remoteSchema)

	if !diffResult.HasChanges() {
		if baseErr := finalizeBaseline(); baseErr != nil {
			return nil, baseErr
		}
		if opts.Verbose {
			fmt.Println()
			fmt.Println(ui.Success("✓ No changes"))
		}
		return &PushResult{HasChanges: false, Statements: []string{}}, nil
	}

	// Show differences
	fmt.Println(ui.Header("\nDifferences found:"))
	fmt.Println(diffResult.Summary())

	// Prompt for USING expressions on column type changes
	if !opts.Force {
		if err := promptForUsingExpressions(diffResult); err != nil {
			return nil, err
		}
	}

	// Get migration statements
	statements, warnings, err := diffResult.GenerateMigrations(true)
	if err != nil {
		return nil, fmt.Errorf("failed to generate migrations: %w", err)
	}
	errCtx.Statements = statements

	if opts.Verbose {
		fmt.Println()
		fmt.Println(ui.Header(fmt.Sprintf("Generated %d migration statement(s) with %d warning(s):", len(statements), len(warnings))))

		for i, stmt := range statements {
			fmt.Printf("%s %s\n\n", ui.Info(fmt.Sprintf("%d.", i+1)), ui.SqlCode(stmt))
		}
	}
	for i, warning := range warnings {
		fmt.Printf("WARNING: %s \n\n", ui.Warning(fmt.Sprintf("%d. %s", i+1, warning)))
	}

	if opts.DryRun {
		if opts.Verbose {
			fmt.Println()
			fmt.Println(ui.Info("ℹ Dry run mode - no changes applied."))
		}
		return &PushResult{HasChanges: true, Statements: statements}, nil
	}

	if !opts.Force {
		fmt.Println()
		confirmed, err := ui.ConfirmPrompt("Do you want to apply these changes?")
		if err != nil {
			return nil, fmt.Errorf("confirmation prompt failed: %w", err)
		}

		if !confirmed {
			fmt.Println(ui.Subtle("Push canceled."))
			return &PushResult{HasChanges: true, Statements: statements}, nil
		}
	}

	// Apply migrations
	fmt.Println()
	fmt.Println(ui.Info("⟳ Applying migrations..."))

	if err := opts.DbClient.ExecuteBulkDDL(ctx, statements...); err != nil {
		fmt.Println()
		fmt.Println(ui.Warning("⚠ Bulk apply failed, retrying statements one-by-one to identify the failure..."))
		fmt.Println()

		// Re-load remote schema to capture any partial progress
		retryRemoteSchema, reloadErr := schema.LoadFromDatabase(ctx, opts.DbClient)
		if reloadErr != nil {
			return nil, fmt.Errorf("%s: %w (additionally, failed to reload schema for retry: %s)", ui.Error("✗ Failed to apply migrations"), err, reloadErr)
		}

		// Re-compare with local schema
		retryDiff := schema.Compare(localSchema, retryRemoteSchema)
		if !retryDiff.HasChanges() {
			fmt.Println(ui.Warning("⚠ Despite the error, all changes appear to have been applied."))
			fmt.Println(ui.Subtle(fmt.Sprintf("  Original error: %s", err)))
			if baseErr := finalizeBaseline(); baseErr != nil {
				return nil, baseErr
			}
			return &PushResult{HasChanges: true, Statements: statements}, nil
		}

		// Re-generate migration statements from the current state
		retryStatements, _, genErr := retryDiff.GenerateMigrations(true)
		if genErr != nil {
			return nil, fmt.Errorf("%s: %w (additionally, failed to regenerate migrations for retry: %s)", ui.Error("✗ Failed to apply migrations"), err, genErr)
		}

		fmt.Println(ui.Info(fmt.Sprintf("⟳ Retrying %d remaining statement(s) individually:", len(retryStatements))))
		fmt.Println()

		for i, stmt := range retryStatements {
			fmt.Printf("%s %s\n", ui.Info(fmt.Sprintf("%d/%d:", i+1, len(retryStatements))), ui.SqlCode(stmt))
			if stmtErr := opts.DbClient.ExecuteBulkDDL(ctx, stmt); stmtErr != nil {
				fmt.Println()
				fmt.Println(ui.Error(fmt.Sprintf("✗ Statement %d failed:", i+1)))
				fmt.Println(ui.SqlCode(stmt))
				return nil, fmt.Errorf("%s: %w", ui.Error("✗ Failed to apply migrations"), stmtErr)
			}
			fmt.Println(ui.Success(fmt.Sprintf("  ✓ Statement %d applied", i+1)))
			fmt.Println()
		}

		fmt.Println(ui.Success("✓ All remaining statements applied individually."))
		if baseErr := finalizeBaseline(); baseErr != nil {
			return nil, baseErr
		}
		return &PushResult{HasChanges: true, Statements: statements}, nil
	}

	fmt.Println()
	fmt.Println(ui.Success("✓ Successfully applied all migrations!"))
	if baseErr := finalizeBaseline(); baseErr != nil {
		return nil, baseErr
	}
	return &PushResult{HasChanges: true, Statements: statements}, nil
}

// runMigrationsBeforePush runs all pending tracked migrations against the live database
// before the schema diff. If a migration is in a failed/pending state it first runs the
// interactive recovery flow. Returns errPushCanceled if the user aborts.
//
// It returns baseline=true when adopting a previously untracked, non-empty database: the
// migrations are not run (they would fail against the already-existing schema). The caller
// then reconciles the schema with a normal push and marks the migrations as the baseline.
func runMigrationsBeforePush(ctx context.Context, opts PushOptions) (bool, error) {
	dbClient := opts.DbClient

	if err := dbClient.InitMigrationHistory(ctx); err != nil {
		return false, err
	}

	migrations, err := loadMigrations(opts.Fs)
	if err != nil {
		return false, err
	}
	if len(migrations) == 0 {
		return false, nil
	}

	// Resolve any failed or pending sync migration before running new ones.
	failed, err := dbClient.GetFailedMigration(ctx)
	if err != nil {
		return false, err
	}
	if failed != nil {
		if err := recoverBeforePush(ctx, opts, migrations, failed); err != nil {
			return false, err
		}
	}

	// Determine which migrations still need to run.
	applied, err := dbClient.GetAppliedMigrations(ctx)
	if err != nil {
		return false, err
	}

	// Adopt an existing, untracked database. If no migrations have ever been
	// recorded but the database already has a schema, treat the current state as
	// the baseline rather than re-running the migrations (which would fail against
	// the existing objects).
	if len(applied) == 0 {
		existing, err := schema.LoadFromDatabase(ctx, dbClient)
		if err != nil {
			return false, err
		}
		if schemaHasObjects(existing) {
			fmt.Println(ui.Info(fmt.Sprintf("Existing untracked database detected; baselining %d migration(s) after the push.", len(migrations))))
			return true, nil
		}
	}

	unapplied, warnings, err := filterUnappliedMigrations(migrations, applied)
	if err != nil {
		return false, err
	}
	for _, warning := range warnings {
		fmt.Println(ui.Warning(warning))
	}
	if len(unapplied) == 0 {
		if opts.Verbose {
			fmt.Println(ui.Subtle("→ No pending migrations to run"))
		}
		return false, nil
	}

	fmt.Printf("\n%s\n", ui.Header(fmt.Sprintf("Pending migrations to run before pushing (%d):", len(unapplied))))
	for i, migration := range unapplied {
		modeLabel := ""
		if migration.Mode == db.MigrationModeAsync {
			modeLabel = " (async)"
		}
		fmt.Printf("  %d. %s%s\n", i+1, migration.Name, modeLabel)
	}
	fmt.Println()

	if opts.DryRun {
		fmt.Println(ui.Info(fmt.Sprintf("ℹ Dry run mode - %d migration(s) would run before the diff.", len(unapplied))))
		return false, nil
	}

	fmt.Println(ui.Info("⟳ Running migrations..."))
	executed, skipped, err := runMigrationList(ctx, dbClient, unapplied)
	if err != nil {
		return false, err
	}
	if executed > 0 {
		fmt.Println(ui.Success(fmt.Sprintf("✓ Ran %d migration(s)", executed)))
	}
	if skipped > 0 {
		return false, fmt.Errorf("could not run %d migration(s) due to unmet dependencies or a running async migration", skipped)
	}

	return false, nil
}

// schemaHasObjects reports whether a loaded schema contains any user-defined objects
// (tables, types, routines, sequences, or views). The bare public schema is ignored, so
// a freshly created, empty database reports false.
func schemaHasObjects(s *schema.Schema) bool {
	return len(s.Tables)+len(s.Types)+len(s.Routines)+len(s.Sequences)+len(s.Views) > 0
}

// recoverBeforePush runs the interactive recovery flow for a failed/pending migration
// encountered during push, adding a "skip migrations" option that marks all pending
// migrations complete and lets the declarative diff bring the schema to its final state.
func recoverBeforePush(ctx context.Context, opts PushOptions, migrations []db.Migration, failed *db.AppliedMigration) error {
	if opts.Force || !ui.IsInteractive() {
		return fmt.Errorf("migration %q is in %s state; run 'scurry migration recover' before pushing", failed.Name, failed.Status)
	}

	migrationFile := filepath.Join(flags.MigrationDir, failed.Name, "migration.sql")
	exists, err := afero.Exists(opts.Fs, migrationFile)
	if err != nil {
		return fmt.Errorf("failed to check migration file: %w", err)
	}
	if !exists {
		return fmt.Errorf("migration file not found: %s\nThe migration file may have been deleted", migrationFile)
	}
	content, err := afero.ReadFile(opts.Fs, migrationFile)
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
		DbClient:        opts.DbClient,
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
		fmt.Println(ui.Subtle("Push canceled."))
		return errPushCanceled
	}

	return nil
}

// promptForUsingExpressions checks for column type changes and prompts the user
// to optionally provide a USING expression for each one.
func promptForUsingExpressions(diffResult *schema.ComparisonResult) error {
	for i := range diffResult.Differences {
		diff := &diffResult.Differences[i]
		if diff.Type != schema.DiffTypeColumnTypeChanged {
			continue
		}

		// Find the AlterTableAlterColumnType command in the migration statements
		for _, stmt := range diff.MigrationStatements {
			alterTable, ok := stmt.(*tree.AlterTable)
			if !ok {
				continue
			}
			for _, cmd := range alterTable.Cmds {
				alterColType, ok := cmd.(*tree.AlterTableAlterColumnType)
				if !ok {
					continue
				}

				// Prompt user if they want to add a USING expression
				fmt.Println()
				confirmed, err := ui.ConfirmPrompt(fmt.Sprintf("Add a USING expression for: %s?", diff.Description))
				if err != nil {
					return fmt.Errorf("confirmation prompt failed: %w", err)
				}

				if !confirmed {
					continue
				}

				// Prompt for the expression
				var exprStr string
				form := huh.NewForm(
					huh.NewGroup(
						huh.NewInput().
							Title("USING expression").
							Description("Enter the expression to convert the column value").
							Placeholder(alterColType.Column.String()).
							Value(&exprStr).
							Validate(func(s string) error {
								if strings.TrimSpace(s) == "" {
									return fmt.Errorf("expression cannot be empty")
								}
								_, err := parser.ParseExpr(s)
								if err != nil {
									return fmt.Errorf("invalid expression: %w", err)
								}
								return nil
							}),
					),
				).WithTheme(ui.HuhTheme())

				if err := form.Run(); err != nil {
					return fmt.Errorf("expression input failed: %w", err)
				}

				// Parse and set the expression
				expr, err := parser.ParseExpr(exprStr)
				if err != nil {
					return fmt.Errorf("failed to parse expression: %w", err)
				}
				alterColType.Using = expr
			}
		}
	}
	return nil
}
