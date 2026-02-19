package cmd

import (
	"bufio"
	"compress/gzip"
	"context"
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
	dataDumpOverwrite bool
	dataDumpBatchSize int
)

var dataDumpCmd = &cobra.Command{
	Use:   "dump <output-file>",
	Short: "Dump all database data to a file",
	Long: `Dump all table data from a CockroachDB database to a single SQL file.

Tables are exported in foreign key-safe insertion order. The output file includes
schema metadata for compatibility checking when loading.

Examples:
  scurry data dump backup.sql --db-url="postgresql://user:pass@localhost:26257/mydb"
  scurry data dump backup.sql --db-url="..." --overwrite --batch-size=500`,
	Args: cobra.ExactArgs(1),
	RunE: runDataDump,
}

func init() {
	dataCmd.AddCommand(dataDumpCmd)

	flags.AddDbUrl(dataDumpCmd)

	dataDumpCmd.Flags().BoolVar(&dataDumpOverwrite, "overwrite", false, "Overwrite the output file without confirmation")
	dataDumpCmd.Flags().IntVar(&dataDumpBatchSize, "batch-size", 100, "Number of rows per INSERT statement")
}

func runDataDump(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	outputFile := args[0]

	if flags.DbUrl == "" {
		return fmt.Errorf("database URL is required (use --db-url or CRDB_URL env var)")
	}

	err := doDataDump(ctx, outputFile)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return nil
}

func doDataDump(ctx context.Context, outputFile string) error {
	fs := afero.NewOsFs()

	// Check if output file exists
	exists, err := afero.Exists(fs, outputFile)
	if err != nil {
		return fmt.Errorf("failed to check output file: %w", err)
	}

	if exists && !dataDumpOverwrite {
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

	client, err := db.Connect(ctx, flags.DbUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer client.Close()

	// Dump data
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Dumping database data..."))
	}

	dumpFile, err := data.Dump(ctx, client, dataDumpBatchSize)
	if err != nil {
		return fmt.Errorf("failed to dump data: %w", err)
	}

	// Write to output file
	f, err := fs.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer f.Close()

	bw := bufio.NewWriter(f)

	var w io.Writer = bw
	var gzw *gzip.Writer
	if strings.HasSuffix(outputFile, ".gz") {
		gzw = gzip.NewWriter(bw)
		w = gzw
	}

	if err := dumpFile.Write(w); err != nil {
		return fmt.Errorf("failed to write dump: %w", err)
	}

	// Close gzip first (writes footer), then flush bufio to disk
	if gzw != nil {
		if err := gzw.Close(); err != nil {
			return fmt.Errorf("failed to close gzip writer: %w", err)
		}
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("failed to flush output file: %w", err)
	}

	// Summary
	totalRows := 0
	for _, td := range dumpFile.TableData {
		totalRows += td.RowCount
	}

	fmt.Println(ui.Success(fmt.Sprintf("Data dumped to %s (%d tables, %d rows, %d sequences)",
		outputFile, len(dumpFile.Tables), totalRows, len(dumpFile.Sequences))))

	return nil
}
