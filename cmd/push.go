package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/flags"
	"github.com/pjtatlow/scurry/internal/db"
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
	pushForce  bool
)

func init() {
	rootCmd.AddCommand(pushCmd)

	flags.AddDbUrl(pushCmd)
	flags.AddDefinitionDir(pushCmd)

	pushCmd.Flags().BoolVar(&pushDryRun, "dry-run", false, "Show what would be executed without applying changes")
	pushCmd.Flags().BoolVar(&pushForce, "force", false, "Skip confirmation prompt")
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
		Force:         pushForce,
	}

	_, err = executePush(ctx, opts)
	return err
}

func executePush(ctx context.Context, opts PushOptions) (*PushResult, error) {
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

	if opts.Verbose {
		// Show differences
		fmt.Println(ui.Header("\nDifferences found:"))
		fmt.Println(diffResult.Summary())
	}

	// Get migration statements
	statements, err := diffResult.GenerateMigrations(true)
	if err != nil {
		return nil, fmt.Errorf("failed to generate migrations: %w", err)
	}

	if opts.Verbose {
		fmt.Println()
		fmt.Println(ui.Header(fmt.Sprintf("Generated %d migration statement(s):", len(statements))))

		for i, stmt := range statements {
			fmt.Printf("%s %s\n\n", ui.Info(fmt.Sprintf("%d.", i+1)), ui.SqlCode(stmt))
		}
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
	if opts.Verbose {
		fmt.Println()
		fmt.Println(ui.Info("⟳ Applying migrations..."))
	}

	if err := opts.DbClient.ExecuteBulkDDL(ctx, statements...); err != nil {
		return nil, fmt.Errorf("%s: %w", ui.Error("✗ Failed to apply migrations"), err)
	}

	if opts.Verbose {
		fmt.Println()
		fmt.Println(ui.Success("✓ Successfully applied all migrations!"))
	}
	return &PushResult{HasChanges: true, Statements: statements}, nil
}
