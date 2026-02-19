package cmd

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	"github.com/pjtatlow/scurry/internal/schema"
	"github.com/pjtatlow/scurry/internal/ui"
)

const (
	checkpointFileName        = "checkpoint.sql"
	checkpointHeaderPrefix    = "-- scurry:migrations="
	checkpointCacheInterval   = 50
)

// CheckpointHeader holds the parsed header information from a checkpoint file
type CheckpointHeader struct {
	MigrationsHash string // SHA-256 of all migration contents up to this point
	CheckpointHash string // SHA-256 of the schema content in this file
}

// Checkpoint represents a parsed checkpoint.sql file
type Checkpoint struct {
	MigrationName string
	Header        CheckpointHeader
	SchemaContent string // The raw SQL content after the header
}

var checkpointRegenCmd = &cobra.Command{
	Use:   "checkpoint-regen",
	Short: "Regenerate checkpoint.sql files for all migrations",
	Long: `Regenerate checkpoint.sql files by replaying all migrations in order.
This is useful after manually editing migrations or fixing checkpoint issues.`,
	RunE: runCheckpointRegen,
}

func init() {
	migrationCmd.AddCommand(checkpointRegenCmd)
}

// computeMigrationsHash computes SHA-256 of concatenated migration contents
// migrations must be sorted by name (timestamp order)
func computeMigrationsHash(migrations []migration) string {
	var combined strings.Builder
	for _, m := range migrations {
		combined.WriteString(m.sql)
	}
	hash := sha256.Sum256([]byte(combined.String()))
	return fmt.Sprintf("%x", hash)
}

// computeContentHash computes SHA-256 of content string
func computeContentHash(content string) string {
	hash := sha256.Sum256([]byte(content))
	return fmt.Sprintf("%x", hash)
}

// collapseWhitespace replaces all whitespace sequences (including newlines) with a single space
var whitespaceRegex = regexp.MustCompile(`\s+`)

func collapseWhitespace(s string) string {
	return strings.TrimSpace(whitespaceRegex.ReplaceAllString(s, " "))
}

// formatCheckpointHeader formats the header line for checkpoint.sql
func formatCheckpointHeader(migrationsHash, checkpointHash string) string {
	return fmt.Sprintf("%s%s,checkpoint=%s",
		checkpointHeaderPrefix, migrationsHash, checkpointHash)
}

// parseCheckpointHeader parses the first line of checkpoint.sql
// Returns error if the line doesn't match expected format
func parseCheckpointHeader(line string) (*CheckpointHeader, error) {
	if !strings.HasPrefix(line, checkpointHeaderPrefix) {
		return nil, fmt.Errorf("invalid checkpoint header: missing prefix")
	}

	// Pattern: -- scurry:migrations=<hash>,checkpoint=<hash>
	pattern := regexp.MustCompile(
		`^-- scurry:migrations=([a-f0-9]{64}),checkpoint=([a-f0-9]{64})$`)
	matches := pattern.FindStringSubmatch(line)
	if matches == nil {
		return nil, fmt.Errorf("invalid checkpoint header format")
	}

	return &CheckpointHeader{
		MigrationsHash: matches[1],
		CheckpointHash: matches[2],
	}, nil
}

// generateCheckpointContent generates the full checkpoint.sql content
func generateCheckpointContent(sch *schema.Schema, migrationsHash string) (string, error) {
	// Generate schema SQL as compact single-line statements to minimize line count
	statements, _, err := schema.Compare(sch, schema.NewSchema()).GenerateMigrations(false)
	if err != nil {
		return "", fmt.Errorf("failed to generate schema statements: %w", err)
	}

	// Collapse all whitespace in each statement to ensure single-line output
	for i, stmt := range statements {
		statements[i] = collapseWhitespace(stmt)
	}

	schemaContent := strings.Join(statements, ";") + ";"
	checkpointHash := computeContentHash(schemaContent)
	header := formatCheckpointHeader(migrationsHash, checkpointHash)

	return header + "\n" + schemaContent, nil
}

// loadCheckpoint loads and parses a checkpoint.sql file from a migration directory
func loadCheckpoint(fs afero.Fs, migrationDir string) (*Checkpoint, error) {
	checkpointPath := filepath.Join(migrationDir, checkpointFileName)

	exists, err := afero.Exists(fs, checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check checkpoint file: %w", err)
	}
	if !exists {
		return nil, nil // No checkpoint exists
	}

	content, err := afero.ReadFile(fs, checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	// Split into header and content
	contentStr := string(content)
	lines := strings.SplitN(contentStr, "\n", 2)
	if len(lines) < 1 {
		return nil, fmt.Errorf("checkpoint file is empty")
	}

	header, err := parseCheckpointHeader(lines[0])
	if err != nil {
		return nil, err
	}

	schemaContent := ""
	if len(lines) > 1 {
		schemaContent = lines[1]
	}

	// Extract migration name from directory path
	migrationName := filepath.Base(migrationDir)

	return &Checkpoint{
		MigrationName: migrationName,
		Header:        *header,
		SchemaContent: schemaContent,
	}, nil
}

// writeCheckpoint writes a checkpoint.sql file to a migration directory
func writeCheckpoint(fs afero.Fs, migrationDir string, content string) error {
	checkpointPath := filepath.Join(migrationDir, checkpointFileName)
	return afero.WriteFile(fs, checkpointPath, []byte(content), 0644)
}

// createCheckpointForMigration creates checkpoint.sql for a specific migration
// migrationsUpTo should be sorted and include all migrations up to and including target
func createCheckpointForMigration(fs afero.Fs, migrationsUpTo []migration, resultSchema *schema.Schema, targetMigrationDir string) error {
	migrationsHash := computeMigrationsHash(migrationsUpTo)
	content, err := generateCheckpointContent(resultSchema, migrationsHash)
	if err != nil {
		return err
	}
	return writeCheckpoint(fs, targetMigrationDir, content)
}

// validateCheckpoint verifies checkpoint content hash integrity
func validateCheckpoint(checkpoint *Checkpoint) error {
	actualHash := computeContentHash(checkpoint.SchemaContent)
	if actualHash != checkpoint.Header.CheckpointHash {
		return fmt.Errorf(
			"checkpoint content hash mismatch: expected %s, got %s",
			checkpoint.Header.CheckpointHash, actualHash)
	}
	return nil
}

// findLatestValidCheckpoint finds the most recent valid checkpoint
// Returns the checkpoint, its index in allMigrations, and any error
// Returns nil, -1, nil if no valid checkpoint found
func findLatestValidCheckpoint(fs afero.Fs, allMigrations []migration) (*Checkpoint, int, error) {
	// Iterate from newest to oldest migration
	for i := len(allMigrations) - 1; i >= 0; i-- {
		migDir := filepath.Join(flags.MigrationDir, allMigrations[i].name)
		checkpoint, err := loadCheckpoint(fs, migDir)
		if err != nil {
			continue // Skip invalid checkpoints
		}
		if checkpoint == nil {
			continue // No checkpoint in this directory
		}

		// Compute expected migrations hash for this point
		migrationsUpTo := allMigrations[:i+1]
		expectedHash := computeMigrationsHash(migrationsUpTo)

		// Validate content hash
		if err := validateCheckpoint(checkpoint); err != nil {
			continue
		}

		// Validate migrations hash
		if checkpoint.Header.MigrationsHash != expectedHash {
			continue
		}

		return checkpoint, i, nil
	}

	return nil, -1, nil
}

// getCheckpointCache creates a CheckpointCache from the SCHEMA_CACHE_URL env var.
// On any error, prints a warning and returns nil. Returns nil if the env var is empty.
func getCheckpointCache(ctx context.Context) *db.CheckpointCache {
	cache, err := db.NewCheckpointCache(ctx, flags.SchemaCacheUrl)
	if err != nil {
		fmt.Println(ui.Warning(fmt.Sprintf("  Warning: failed to connect to checkpoint cache: %v", err)))
		return nil
	}
	if cache == nil {
		return nil
	}

	if err := cache.InitTable(ctx); err != nil {
		fmt.Println(ui.Warning(fmt.Sprintf("  Warning: failed to initialize checkpoint cache table: %v", err)))
		cache.Close()
		return nil
	}

	return cache
}

// storeToCacheIfAvailable generates schema SQL and stores it in the remote cache.
// Logs warnings on error, never fails.
func storeToCacheIfAvailable(ctx context.Context, cache *db.CheckpointCache, migrationsHash string, sch *schema.Schema) {
	if cache == nil {
		return
	}

	content, err := generateCheckpointContent(sch, migrationsHash)
	if err != nil {
		fmt.Println(ui.Warning(fmt.Sprintf("  Warning: failed to generate checkpoint content for cache: %v", err)))
		return
	}

	// Extract just the schema SQL (after the header line)
	lines := strings.SplitN(content, "\n", 2)
	schemaSQL := ""
	if len(lines) > 1 {
		schemaSQL = lines[1]
	}

	if err := cache.Put(ctx, migrationsHash, schemaSQL); err != nil {
		fmt.Println(ui.Warning(fmt.Sprintf("  Warning: failed to store checkpoint in cache: %v", err)))
	}
}

// lookupFromCache retrieves a cached checkpoint from the remote cache.
// Returns nil on miss or error.
func lookupFromCache(ctx context.Context, cache *db.CheckpointCache, migrationsHash string) *Checkpoint {
	if cache == nil {
		return nil
	}

	schemaSQL, found, err := cache.Get(ctx, migrationsHash)
	if err != nil {
		fmt.Println(ui.Warning(fmt.Sprintf("  Warning: failed to lookup checkpoint in cache: %v", err)))
		return nil
	}
	if !found {
		return nil
	}

	// Validate by parsing the SQL
	_, parseErr := schema.ParseSQL(schemaSQL)
	if parseErr != nil {
		fmt.Println(ui.Warning(fmt.Sprintf("  Warning: cached checkpoint has invalid SQL: %v", parseErr)))
		return nil
	}

	checkpointHash := computeContentHash(schemaSQL)

	return &Checkpoint{
		Header: CheckpointHeader{
			MigrationsHash: migrationsHash,
			CheckpointHash: checkpointHash,
		},
		SchemaContent: schemaSQL,
	}
}

// runCheckpointRegen regenerates all checkpoint.sql files
func runCheckpointRegen(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	fs := afero.NewOsFs()

	// Validate migrations directory
	if err := validateMigrationsDir(fs); err != nil {
		return err
	}

	// Load all migrations
	migrations, err := loadMigrations(fs)
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}

	if len(migrations) == 0 {
		fmt.Println(ui.Info("No migrations found"))
		return nil
	}

	cache := getCheckpointCache(ctx)
	if cache != nil {
		defer cache.Close()
	}

	fmt.Println(ui.Header(fmt.Sprintf("Regenerating checkpoints for %d migrations...", len(migrations))))

	// Start with empty database
	client, err := db.GetShadowDB(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	// Apply migrations one by one and generate checkpoints
	for i, mig := range migrations {
		fmt.Printf("Processing %s (%d/%d)...\n", mig.name, i+1, len(migrations))

		start := time.Now()

		// Apply this migration
		err = client.ExecuteBulkDDL(ctx, mig.sql)
		if err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", mig.name, err)
		}

		// Get current schema state
		currentSchema, err := schema.LoadFromDatabase(ctx, client)
		if err != nil {
			return fmt.Errorf("failed to load schema after %s: %w", mig.name, err)
		}

		// Generate checkpoint for this migration
		migrationsUpTo := migrations[:i+1]
		migDir := filepath.Join(flags.MigrationDir, mig.name)

		err = createCheckpointForMigration(fs, migrationsUpTo, currentSchema, migDir)
		if err != nil {
			return fmt.Errorf("failed to create checkpoint for %s: %w", mig.name, err)
		}

		// Also store to remote cache
		migrationsHash := computeMigrationsHash(migrationsUpTo)
		storeToCacheIfAvailable(ctx, cache, migrationsHash, currentSchema)

		duration := time.Since(start)
		fmt.Println(ui.Success(fmt.Sprintf("  Checkpoint created in %v", duration.Round(time.Millisecond))))
	}

	fmt.Println()
	fmt.Println(ui.Success(fmt.Sprintf("Regenerated %d checkpoint(s)", len(migrations))))

	return nil
}
