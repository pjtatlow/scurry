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

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate local schema",
	Long: `Validate local schema.
This will try to execute the local schema in a new shadow database to ensure it is valid.`,
	RunE: validate,
}

func init() {
	rootCmd.AddCommand(validateCmd)

	flags.AddDefinitionDir(validateCmd)
}

func validate(cmd *cobra.Command, args []string) error {
	// Validate required flags
	if flags.DefinitionDir == "" {
		return fmt.Errorf("definition directory is required (use --definitions)")
	}

	err := doValidate(cmd.Context())
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return nil
}

func doValidate(ctx context.Context) error {

	// Load local schema from files
	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading local schema from %s...", flags.DefinitionDir)))
	}

	dbClient, err := db.GetShadowDB(ctx)
	if err != nil {
		return fmt.Errorf("failed to get shadow database client: %w", err)
	}
	defer dbClient.Close()

	localSchema, err := schema.LoadFromDirectory(ctx, afero.NewOsFs(), flags.DefinitionDir, dbClient)
	if err != nil {
		return fmt.Errorf("failed to load local schema: %w", err)
	}

	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Found %d tables, %d types, %d routines, %d sequences, %d views locally",
			len(localSchema.Tables), len(localSchema.Types), len(localSchema.Routines), len(localSchema.Sequences), len(localSchema.Views))))
	}

	fmt.Println()
	fmt.Println(ui.Success("✓ Successfully validated local schema!"))
	return nil
}
