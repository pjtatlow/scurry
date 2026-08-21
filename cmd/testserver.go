package cmd

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
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

// The store fraction reaches CockroachDB rounded to two decimal places, and
// CockroachDB rejects 0% and 100%, so those are the fractions worth accepting.
const (
	minStoreSize = 0.01
	maxStoreSize = 0.99
)

var (
	urlFile     string
	cacheSize   string
	maxGoMemory string
)

func init() {
	rootCmd.AddCommand(testserverCmd)

	flags.AddDefinitionDirs(testserverCmd)

	testserverCmd.Flags().StringVar(&urlFile, "url-file", "", "File to write the database URL to when it's ready")
	testserverCmd.Flags().StringVar(&db.TestServerHost, "host", "", "Host address for the test database server")
	testserverCmd.Flags().IntVar(&db.TestServerPort, "port", 0, "Port for the test database server")
	testserverCmd.Flags().IntVar(&db.TestServerHTTPPort, "http-port", 0, "HTTP port for the test database server")
	testserverCmd.Flags().Float64Var(&db.TestServerStoreSize, "store-size", 0, "Fraction of available memory for the in-memory store, e.g. 0.05 (CockroachDB --store=type=mem,size, defaults to 0.2)")
	testserverCmd.Flags().StringVar(&cacheSize, "cache-size", "", "Size of the block cache, e.g. 512MiB (CockroachDB --cache, defaults to 10% of available memory)")
	testserverCmd.Flags().StringVar(&maxGoMemory, "max-go-memory", "", "Soft limit on the CockroachDB Go heap, e.g. 2GiB (sets GOMEMLIMIT, defaults to 2.25x CockroachDB's --max-sql-memory)")
}

// applyMemoryLimits validates the optional memory limit flags and hands the
// parsed values to the db package. Flags that were not set are left alone, so
// CockroachDB's defaults apply.
func applyMemoryLimits() error {
	if db.TestServerStoreSize != 0 && (db.TestServerStoreSize < minStoreSize || db.TestServerStoreSize > maxStoreSize) {
		return fmt.Errorf("invalid --store-size %v: expected a fraction of available memory between %v and %v (e.g. 0.05)", db.TestServerStoreSize, minStoreSize, maxStoreSize)
	}

	if cacheSize != "" {
		size, err := parseMemorySize("cache-size", cacheSize)
		if err != nil {
			return err
		}
		db.TestServerCacheSize = size
	}

	if maxGoMemory != "" {
		size, err := parseMemorySize("max-go-memory", maxGoMemory)
		if err != nil {
			return err
		}
		db.TestServerMaxGoMemory = size
	}

	return nil
}

// parseMemorySize parses a byte size the way CockroachDB does, e.g. 512MiB,
// 2GB or a plain byte count.
func parseMemorySize(name, value string) (int64, error) {
	size, err := humanize.ParseBytes(value)
	if err != nil || size == 0 || size > math.MaxInt64 {
		return 0, fmt.Errorf("invalid --%s %q: expected a byte size such as 512MiB or 2GiB", name, value)
	}
	return int64(size), nil
}

func runTestserver(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Validate required flags
	if len(flags.DefinitionDirs) == 0 {
		return fmt.Errorf("definition directory is required (use --definitions)")
	}
	if urlFile == "" {
		return fmt.Errorf("url file is required (use --url-file)")
	}
	if err := applyMemoryLimits(); err != nil {
		return err
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
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Loading local schema from %s...", strings.Join(flags.DefinitionDirs, ", "))))
	}
	testSchema, err := schema.LoadFromDirectories(ctx, afero.NewOsFs(), flags.DefinitionDirs, dbClient)
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
