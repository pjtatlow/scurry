package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/pjtatlow/scurry/internal/flags"
	migrationpkg "github.com/pjtatlow/scurry/internal/migration"
	"github.com/pjtatlow/scurry/internal/ui"
)

var squashBeforeStr string

var migrationSquashCmd = &cobra.Command{
	Use:   "squash",
	Short: "Squash old migrations into a single migration",
	Long: `Squash all migrations older than the specified duration into a single migration.

The squashed migration contains the combined SQL of all replaced migrations and is
marked with a special header so that the migration system skips it during execution.
Existing databases already have these migrations applied, so the squash migration
serves as a historical record and is used only during validation.

Supports Go duration syntax (e.g., 720h) as well as shorthand units:
  d = days, w = weeks, m = months (30 days)

Examples:
  # Squash migrations older than 30 days
  scurry migration squash --before=30d

  # Squash migrations older than 3 months
  scurry migration squash --before=3m

  # Squash migrations older than 2 weeks
  scurry migration squash --before=2w

  # Squash without confirmation prompt
  scurry migration squash --before=30d --force
`,
	RunE: runMigrationSquash,
}

func init() {
	migrationCmd.AddCommand(migrationSquashCmd)
	migrationSquashCmd.Flags().StringVar(&squashBeforeStr, "before", "", "Squash migrations older than this duration (e.g., 30d, 2w, 3m, 720h)")
	_ = migrationSquashCmd.MarkFlagRequired("before")
}

func runMigrationSquash(cmd *cobra.Command, args []string) error {
	squashBefore, err := parseDuration(squashBeforeStr)
	if err != nil {
		return fmt.Errorf("invalid --before value %q: %w", squashBeforeStr, err)
	}
	err = doMigrationSquash(afero.NewOsFs(), squashBefore)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	return nil
}

// parseDuration parses a duration string with support for shorthand units:
// d (days), w (weeks), m (months/30 days), in addition to standard Go duration syntax.
func parseDuration(s string) (time.Duration, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("empty duration")
	}

	suffix := s[len(s)-1]
	switch suffix {
	case 'd':
		n, err := strconv.Atoi(s[:len(s)-1])
		if err != nil {
			return 0, fmt.Errorf("invalid number before 'd': %w", err)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	case 'w':
		n, err := strconv.Atoi(s[:len(s)-1])
		if err != nil {
			return 0, fmt.Errorf("invalid number before 'w': %w", err)
		}
		return time.Duration(n) * 7 * 24 * time.Hour, nil
	case 'm':
		n, err := strconv.Atoi(s[:len(s)-1])
		if err != nil {
			return 0, fmt.Errorf("invalid number before 'm': %w", err)
		}
		return time.Duration(n) * 30 * 24 * time.Hour, nil
	default:
		return time.ParseDuration(s)
	}
}

func doMigrationSquash(fs afero.Fs, squashBefore time.Duration) error {

	// Validate migrations directory
	if err := validateMigrationsDir(fs); err != nil {
		return err
	}

	// Load all migrations
	migrations, err := loadMigrations(fs)
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}

	if len(migrations) < 2 {
		return fmt.Errorf("need at least 2 migrations to squash, found %d", len(migrations))
	}

	// Calculate cutoff time
	cutoff := time.Now().Add(-squashBefore)

	if flags.Verbose {
		fmt.Println(ui.Subtle(fmt.Sprintf("→ Cutoff time: %s", cutoff.Format(time.RFC3339))))
	}

	// Find migrations before cutoff by parsing timestamps from directory names
	var toSquash []int // indices into migrations slice
	for i, mig := range migrations {
		ts, err := parseMigrationTimestamp(mig.Name)
		if err != nil {
			if flags.Verbose {
				fmt.Println(ui.Warning(fmt.Sprintf("  Skipping %s: could not parse timestamp: %v", mig.Name, err)))
			}
			continue
		}
		if ts.Before(cutoff) {
			toSquash = append(toSquash, i)
		}
	}

	if len(toSquash) < 2 {
		return fmt.Errorf("need at least 2 migrations before cutoff to squash, found %d", len(toSquash))
	}

	// Display what will be squashed
	fmt.Println(ui.Header(fmt.Sprintf("Migrations to squash (%d):", len(toSquash))))
	for _, idx := range toSquash {
		fmt.Printf("  - %s\n", migrations[idx].Name)
	}
	fmt.Println()

	// Confirm unless --force
	if !flags.Force {
		if !ui.IsInteractive() {
			return fmt.Errorf("squash requires an interactive terminal for confirmation\nUse --force to skip the confirmation prompt")
		}
		confirmed, err := ui.ConfirmPrompt(fmt.Sprintf("Squash these %d migrations into one?", len(toSquash)))
		if err != nil {
			return err
		}
		if !confirmed {
			fmt.Println(ui.Info("Aborted"))
			return nil
		}
	}

	// Build combined SQL from squashed migrations (headers already stripped by loadMigrations)
	var combinedParts []string
	for _, idx := range toSquash {
		sql := strings.TrimSpace(migrations[idx].SQL)
		if sql != "" {
			combinedParts = append(combinedParts, sql)
		}
	}
	combinedSQL := strings.Join(combinedParts, "\n\n")

	// Use the timestamp of the last squashed migration for the new name
	lastSquashed := migrations[toSquash[len(toSquash)-1]]
	lastTimestamp := lastSquashed.Name[:14]

	squashName := lastTimestamp + "_squash"
	squashDir := filepath.Join(flags.MigrationDir, squashName)

	// Build the header
	header := &migrationpkg.Header{
		Mode:   migrationpkg.ModeSync,
		Squash: true,
	}
	content := migrationpkg.FormatHeader(header) + "\n" + combinedSQL

	// Create squash migration directory and file
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Creating squash migration..."))
	}

	if err := fs.MkdirAll(squashDir, 0755); err != nil {
		return fmt.Errorf("failed to create squash migration directory: %w", err)
	}

	squashFile := filepath.Join(squashDir, "migration.sql")
	if err := afero.WriteFile(fs, squashFile, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write squash migration: %w", err)
	}

	// Delete old migration directories
	if flags.Verbose {
		fmt.Println(ui.Subtle("→ Removing squashed migrations..."))
	}

	for _, idx := range toSquash {
		oldDir := filepath.Join(flags.MigrationDir, migrations[idx].Name)
		if err := fs.RemoveAll(oldDir); err != nil {
			return fmt.Errorf("failed to remove migration directory %s: %w", migrations[idx].Name, err)
		}
		if flags.Verbose {
			fmt.Println(ui.Subtle(fmt.Sprintf("  Removed %s", migrations[idx].Name)))
		}
	}

	fmt.Println()
	fmt.Println(ui.Success(fmt.Sprintf("✓ Squashed %d migrations into %s", len(toSquash), squashName)))
	fmt.Println()
	fmt.Println(ui.Info("Run 'scurry migration validate --overwrite' to update schema.sql and checkpoints"))

	return nil
}

// parseMigrationTimestamp extracts the timestamp from a migration directory name.
// Migration names have the format YYYYMMDDHHMMSS_description.
func parseMigrationTimestamp(name string) (time.Time, error) {
	if len(name) < 14 {
		return time.Time{}, fmt.Errorf("name too short")
	}
	return time.Parse("20060102150405", name[:14])
}
