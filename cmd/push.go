package cmd

import (
	"context"
	"fmt"
	"os"
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
	pushDryRun bool
)

func init() {
	rootCmd.AddCommand(pushCmd)

	flags.AddDbUrl(pushCmd)
	flags.AddDefinitionDir(pushCmd)

	pushCmd.Flags().BoolVar(&pushDryRun, "dry-run", false, "Show what would be executed without applying changes")
}

func push(cmd *cobra.Command, args []string) error {
	// Validate required flags
	if flags.DbUrl == "" {
		return fmt.Errorf("database URL is required (use --db-url or CRDB_URL env var)")
	}
	if flags.DefinitionDir == "" {
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
	Fs            afero.Fs
	DefinitionDir string
	DbClient      *db.Client
	Verbose       bool
	DryRun        bool
	Force         bool
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
		Fs:            afero.NewOsFs(),
		DefinitionDir: flags.DefinitionDir,
		DbClient:      client,
		Verbose:       flags.Verbose,
		DryRun:        pushDryRun,
		Force:         flags.Force,
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
	// Load local schema from files
	if opts.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading local schema from %s...", opts.DefinitionDir)))
	}

	dbClient, err := db.GetShadowDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer dbClient.Close()

	localSchema, err := schema.LoadFromDirectory(ctx, opts.Fs, opts.DefinitionDir, dbClient)
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
		return nil, fmt.Errorf("%s: %w", ui.Error("✗ Failed to apply migrations"), err)
	}

	fmt.Println()
	fmt.Println(ui.Success("✓ Successfully applied all migrations!"))
	return &PushResult{HasChanges: true, Statements: statements}, nil
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
