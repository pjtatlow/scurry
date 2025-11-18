package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/flags"
	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/schema"
	"github.com/pjtatlow/scurry/internal/ui"
)

var dumpCmd = &cobra.Command{
	Use:   "dump <output-file>",
	Short: "Dump the database schema to a file",
	Long: `Dump the database schema to a file by generating CREATE statements for all objects.
This creates a file similar to migrations/schema.sql that can recreate the entire database.`,
	Args: cobra.ExactArgs(1),
	RunE: dump,
}

var (
	dumpForce bool
)

func init() {
	rootCmd.AddCommand(dumpCmd)
	dumpCmd.Flags().StringVar(&dbURL, "db-url", os.Getenv("CRDB_URL"), "Database connection URL (defaults to CRDB_URL env var)")
	dumpCmd.Flags().BoolVar(&dumpForce, "force", false, "Overwrite the output file without confirmation")
}

func dump(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	outputFile := args[0]

	// Validate required flags
	if dbURL == "" {
		return fmt.Errorf("database URL is required (use --db-url or CRDB_URL env var)")
	}

	// Execution errors print and exit
	err := doDump(ctx, outputFile)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return nil
}

func doDump(ctx context.Context, outputFile string) error {
	fs := afero.NewOsFs()

	// Check if output file exists
	exists, err := afero.Exists(fs, outputFile)
	if err != nil {
		return fmt.Errorf("failed to check output file: %w", err)
	}

	if exists && !dumpForce {
		confirmed, err := ui.ConfirmPrompt(fmt.Sprintf("File %s already exists. Overwrite?", outputFile))
		if err != nil {
			return fmt.Errorf("confirmation prompt failed: %w", err)
		}
		if !confirmed {
			fmt.Println(ui.Subtle("Dump canceled."))
			return nil
		}
	}

	// Connect to database
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Connecting to database..."))
	}

	client, err := db.Connect(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer client.Close()

	// Load schema from database
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Loading database schema..."))
	}

	dbSchema, err := schema.LoadFromDatabase(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to load database schema: %w", err)
	}

	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Found %d tables, %d types, %d routines, %d sequences, %d views in database",
			len(dbSchema.Tables), len(dbSchema.Types), len(dbSchema.Routines), len(dbSchema.Sequences), len(dbSchema.Views))))
	}

	// Generate CREATE statements
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Generating CREATE statements..."))
	}

	statements, err := schema.Compare(dbSchema, schema.NewSchema()).GenerateMigrations(true)
	if err != nil {
		return fmt.Errorf("failed to generate CREATE statements: %w", err)
	}

	// Write to output file
	content := strings.Join(statements, ";\n\n\n") + ";\n"
	err = afero.WriteFile(fs, outputFile, []byte(content), 0644)
	if err != nil {
		return fmt.Errorf("failed to write output file: %w", err)
	}

	if flags.Verbose {
		fmt.Println()
		fmt.Println(ui.Success(fmt.Sprintf("✓ Successfully dumped schema to %s", outputFile)))
	} else {
		fmt.Println(ui.Success(fmt.Sprintf("Schema dumped to %s", outputFile)))
	}

	return nil
}
