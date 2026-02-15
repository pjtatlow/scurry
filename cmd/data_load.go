package cmd

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/data"
	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	"github.com/pjtatlow/scurry/internal/ui"
)

var (
	dataLoadDryRun        bool
	dataLoadTruncateFirst bool
	dataLoadCreateSchema  bool
)

var dataLoadCmd = &cobra.Command{
	Use:   "load <input-file>",
	Short: "Load data from a dump file into a database",
	Long: `Load data from a scurry dump file into a CockroachDB database.

The dump file's schema is compared against the target database to verify
compatibility before loading. Use --create-schema to create the schema from
the dump file (for restoring into an empty database).

Examples:
  scurry data load backup.sql --db-url="postgresql://user:pass@localhost:26257/mydb"
  scurry data load backup.sql --db-url="..." --truncate-first
  scurry data load backup.sql --db-url="..." --create-schema
  scurry data load backup.sql --db-url="..." --dry-run`,
	Args: cobra.ExactArgs(1),
	RunE: runDataLoad,
}

func init() {
	dataCmd.AddCommand(dataLoadCmd)

	flags.AddDbUrl(dataLoadCmd)

	dataLoadCmd.Flags().BoolVar(&dataLoadDryRun, "dry-run", false, "Parse and check compatibility without loading data")
	dataLoadCmd.Flags().BoolVar(&dataLoadTruncateFirst, "truncate-first", false, "Truncate all tables before loading data")
	dataLoadCmd.Flags().BoolVar(&dataLoadCreateSchema, "create-schema", false, "Create the schema from the dump file before loading data")
}

func runDataLoad(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	inputFile := args[0]

	if flags.DbUrl == "" {
		return fmt.Errorf("database URL is required (use --db-url or CRDB_URL env var)")
	}

	err := doDataLoad(ctx, inputFile)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return nil
}

func doDataLoad(ctx context.Context, inputFile string) error {
	fs := afero.NewOsFs()

	// Read dump file
	content, err := afero.ReadFile(fs, inputFile)
	if err != nil {
		return fmt.Errorf("failed to read input file: %w", err)
	}

	// Parse dump file
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Parsing dump file..."))
	}

	var reader io.Reader = bytes.NewReader(content)
	if strings.HasSuffix(inputFile, ".gz") {
		gzReader, err := gzip.NewReader(bytes.NewReader(content))
		if err != nil {
			return fmt.Errorf("failed to decompress gzip file: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	dumpFile, err := data.ParseDumpFile(reader)
	if err != nil {
		return fmt.Errorf("failed to parse dump file: %w", err)
	}

	if flags.Verbose {
		totalRows := 0
		for _, td := range dumpFile.TableData {
			totalRows += td.RowCount
		}
		fmt.Println(ui.Subtle(fmt.Sprintf("  Found %d tables, %d total rows, %d sequences",
			len(dumpFile.Tables), totalRows, len(dumpFile.Sequences))))
	}

	// Connect to database
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Connecting to database..."))
	}

	client, err := db.Connect(ctx, flags.DbUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer client.Close()

	// Load data
	if flags.Verbose {
		if dataLoadDryRun {
			fmt.Println(ui.Subtle("→ Running compatibility check (dry run)..."))
		} else {
			fmt.Println(ui.Subtle("→ Loading data..."))
		}
	}

	result, err := data.Load(ctx, client, dumpFile, data.LoadOptions{
		DryRun:        dataLoadDryRun,
		TruncateFirst: dataLoadTruncateFirst,
		CreateSchema:  dataLoadCreateSchema,
	})
	if err != nil {
		var compatErr *data.CompatibilityError
		if errors.As(err, &compatErr) {
			fmt.Println(ui.Error("Schema compatibility check failed:"))
			for _, issue := range compatErr.Issues {
				if issue.Severity == "error" {
					fmt.Printf("  %s %s\n", ui.Error("[ERROR]"), issue.Description)
				} else {
					fmt.Printf("  %s %s\n", ui.Warning("[WARN]"), issue.Description)
				}
			}
			return fmt.Errorf("aborting due to compatibility errors")
		}
		return err
	}

	if dataLoadDryRun {
		fmt.Println(ui.Info(fmt.Sprintf("Dry run: would load %d tables, %d rows",
			result.TablesLoaded, result.RowsInserted)))
	} else {
		fmt.Println(ui.Success(fmt.Sprintf("Data loaded successfully (%d tables, %d rows)",
			result.TablesLoaded, result.RowsInserted)))
	}

	return nil
}
