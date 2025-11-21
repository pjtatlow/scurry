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

var testserverCmd = &cobra.Command{
	Use:   "testserver",
	Short: "Start a test database with the current schema",
	Long: `Start a test database, push the current schema to it, and write the database URL to a file.
The database will stay running until the process is killed (Ctrl+C).`,
	RunE: runTestserver,
}

var (
	urlFile string
)

func init() {
	rootCmd.AddCommand(testserverCmd)

	flags.AddDefinitionDir(testserverCmd)

	testserverCmd.Flags().StringVar(&urlFile, "url-file", "", "File to write the database URL to when it's ready")
	testserverCmd.Flags().StringVar(&db.TestServerHost, "host", "", "Host address for the test database server")
	testserverCmd.Flags().IntVar(&db.TestServerPort, "port", 0, "Port for the test database server")
	testserverCmd.Flags().IntVar(&db.TestServerHTTPPort, "http-port", 0, "HTTP port for the test database server")
}

func runTestserver(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Validate required flags
	if flags.DefinitionDir == "" {
		return fmt.Errorf("definition directory is required (use --definitions)")
	}
	if urlFile == "" {
		return fmt.Errorf("url file is required (use --url-file)")
	}

	err := doTestserver(ctx, urlFile)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return nil
}

func doTestserver(ctx context.Context, urlFile string) error {
	// Start test server
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Starting CRDB test server..."))
	}

	dbClient, err := db.GetShadowDB(ctx)
	if err != nil {
		return fmt.Errorf("failed to get shadow database client: %w", err)
	}
	defer dbClient.Close()

	testServerUrl := dbClient.ConnectionString()

	// Load local schema
	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading local schema from %s...", flags.DefinitionDir)))
	}
	testSchema, err := schema.LoadFromDirectory(ctx, afero.NewOsFs(), flags.DefinitionDir, dbClient)
	if err != nil {
		return fmt.Errorf("failed to load local schema: %w", err)
	}
	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("  Found %d tables, %d types, %d routines, %d sequences, %d views locally",
			len(testSchema.Tables), len(testSchema.Types), len(testSchema.Routines), len(testSchema.Sequences), len(testSchema.Views))))
	}

	// Write URL to file
	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Writing database URL to %s...", urlFile)))
	}

	err = os.WriteFile(urlFile, []byte(testServerUrl), 0644)
	if err != nil {
		return fmt.Errorf("failed to write URL file: %w", err)
	}

	if flags.Verbose {
		fmt.Println(ui.Success(fmt.Sprintf("✓ Database URL written to %s", urlFile)))
	}

	// Print success message
	fmt.Println()
	fmt.Println(ui.Success("✓ Test database is ready!"))
	fmt.Println(ui.Info(fmt.Sprintf("  Database URL: %s", testServerUrl)))
	fmt.Println(ui.Info(fmt.Sprintf("  URL file: %s", urlFile)))
	fmt.Println()
	fmt.Println(ui.Subtle("Press Ctrl+C to stop the test server..."))

	// Wait for interrupt signal
	<-ctx.Done()

	fmt.Println()
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Stopping test server..."))
	}

	// Clean up URL file
	os.Remove(urlFile)

	if flags.Verbose {
		fmt.Println(ui.Success("✓ Test server stopped"))
	}

	return nil
}
