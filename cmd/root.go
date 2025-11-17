package cmd

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
)

var (
	// Global flags
	verbose bool

	// Shared command flags
	dbURL     string
	schemaDir string

	logOutput bytes.Buffer

	// Global context for signal handling
	rootContext context.Context
	cancelFunc  context.CancelFunc
)

var rootCmd = &cobra.Command{
	Use:   "scurry",
	Short: "CockroachDB schema migration tool",
	Long: `Scurry is a CLI tool for managing CockroachDB database schemas.
It allows you to define your database schema in SQL files and keep them in sync with your database.`,
}

func Execute() error {
	// Create context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	rootContext = ctx
	cancelFunc = cancel

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start goroutine to handle signals
	go func() {
		<-sigChan
		if verbose {
			fmt.Fprintln(os.Stderr, "\nReceived interrupt signal, canceling...")
		}
		// Stop shared test server on interrupt
		db.StopShadowDbServer()
		cancel()
	}()

	// Ensure cleanup on exit
	defer func() {
		cancel()
		db.StopShadowDbServer()
	}()

	return rootCmd.ExecuteContext(ctx)
}

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&db.CrdbVersion, "crdb-version", os.Getenv("CRDB_VERSION"), "CockroachDB version, defaults to latest.")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")

	// Hide log output from cockroachdb testserver package
	log.SetOutput(&logOutput)
}
