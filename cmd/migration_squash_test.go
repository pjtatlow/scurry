package cmd

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
	migrationpkg "github.com/pjtatlow/scurry/internal/migration"
)

func TestParseMigrationTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr bool
		wantTS  time.Time
	}{
		{
			name:   "valid timestamp",
			input:  "20250115120000_create_users",
			wantTS: time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC),
		},
		{
			name:   "valid timestamp with complex name",
			input:  "20240601093045_add_email_to_users",
			wantTS: time.Date(2024, 6, 1, 9, 30, 45, 0, time.UTC),
		},
		{
			name:    "name too short",
			input:   "12345",
			wantErr: true,
		},
		{
			name:    "invalid timestamp digits",
			input:   "abcdefghijklmn_bad",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseMigrationTimestamp(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantTS, got)
		})
	}
}

func TestDoMigrationSquash(t *testing.T) {
	// Not parallel: subtests modify shared globals (flags.Force, squashBefore)

	// Helper to create a migration directory with SQL content
	createMigrationDir := func(t *testing.T, fs afero.Fs, name, sql string) {
		t.Helper()
		dir := filepath.Join(flags.MigrationDir, name)
		err := fs.MkdirAll(dir, 0755)
		require.NoError(t, err)
		err = afero.WriteFile(fs, filepath.Join(dir, "migration.sql"), []byte(sql), 0644)
		require.NoError(t, err)
	}

	t.Run("squash combines migrations and removes originals", func(t *testing.T) {
		fs := afero.NewMemMapFs()

		// Create migrations directory
		err := fs.MkdirAll(flags.MigrationDir, 0755)
		require.NoError(t, err)

		// Create old migrations (well in the past)
		createMigrationDir(t, fs, "20240101000000_create_users", "CREATE TABLE users (id INT PRIMARY KEY);")
		createMigrationDir(t, fs, "20240201000000_add_email", "ALTER TABLE users ADD COLUMN email STRING;")

		// Create a recent migration (should not be squashed)
		recentTimestamp := time.Now().Add(-1 * time.Hour).Format("20060102150405")
		recentName := recentTimestamp + "_add_posts"
		createMigrationDir(t, fs, recentName, "CREATE TABLE posts (id INT PRIMARY KEY);")

		// Load migrations to verify setup
		migrations, err := loadMigrations(fs)
		require.NoError(t, err)
		require.Len(t, migrations, 3)

		// Set squash params and run
		squashBefore = 720 * time.Hour // 30 days
		flags.Force = true
		defer func() { flags.Force = false }()

		err = doMigrationSquash(fs)
		require.NoError(t, err)

		// Verify: old migration dirs should be gone
		exists, err := afero.DirExists(fs, filepath.Join(flags.MigrationDir, "20240101000000_create_users"))
		require.NoError(t, err)
		assert.False(t, exists, "old migration should be deleted")

		exists, err = afero.DirExists(fs, filepath.Join(flags.MigrationDir, "20240201000000_add_email"))
		require.NoError(t, err)
		assert.False(t, exists, "old migration should be deleted")

		// Verify: squash migration should exist with last squashed timestamp
		squashDir := filepath.Join(flags.MigrationDir, "20240201000000_squash")
		exists, err = afero.DirExists(fs, squashDir)
		require.NoError(t, err)
		assert.True(t, exists, "squash migration should be created")

		// Verify squash migration content
		content, err := afero.ReadFile(fs, filepath.Join(squashDir, "migration.sql"))
		require.NoError(t, err)
		contentStr := string(content)

		// Should have squash header
		assert.Contains(t, contentStr, "squash=true")
		assert.Contains(t, contentStr, "mode=sync")

		// Should contain both original migration SQLs
		assert.Contains(t, contentStr, "CREATE TABLE users")
		assert.Contains(t, contentStr, "ALTER TABLE users ADD COLUMN email")

		// Verify: recent migration should still exist
		exists, err = afero.DirExists(fs, filepath.Join(flags.MigrationDir, recentName))
		require.NoError(t, err)
		assert.True(t, exists, "recent migration should not be affected")
	})

	t.Run("error when fewer than 2 migrations before cutoff", func(t *testing.T) {
		fs := afero.NewMemMapFs()

		err := fs.MkdirAll(flags.MigrationDir, 0755)
		require.NoError(t, err)

		// Create only 1 old migration
		createMigrationDir(t, fs, "20240101000000_create_users", "CREATE TABLE users (id INT PRIMARY KEY);")

		// Create a recent migration
		recentTimestamp := time.Now().Add(-1 * time.Hour).Format("20060102150405")
		createMigrationDir(t, fs, recentTimestamp+"_add_posts", "CREATE TABLE posts (id INT PRIMARY KEY);")

		squashBefore = 720 * time.Hour
		flags.Force = true
		defer func() { flags.Force = false }()

		err = doMigrationSquash(fs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "need at least 2 migrations before cutoff")
	})

	t.Run("error when fewer than 2 total migrations", func(t *testing.T) {
		fs := afero.NewMemMapFs()

		err := fs.MkdirAll(flags.MigrationDir, 0755)
		require.NoError(t, err)

		createMigrationDir(t, fs, "20240101000000_create_users", "CREATE TABLE users (id INT PRIMARY KEY);")

		squashBefore = 720 * time.Hour
		flags.Force = true
		defer func() { flags.Force = false }()

		err = doMigrationSquash(fs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "need at least 2 migrations to squash")
	})

	t.Run("squash migration has squash flag set when loaded", func(t *testing.T) {
		fs := afero.NewMemMapFs()

		err := fs.MkdirAll(flags.MigrationDir, 0755)
		require.NoError(t, err)

		// Create old migrations
		createMigrationDir(t, fs, "20240101000000_create_users", "CREATE TABLE users (id INT PRIMARY KEY);")
		createMigrationDir(t, fs, "20240201000000_add_email", "ALTER TABLE users ADD COLUMN email STRING;")

		// Create a recent migration
		recentTimestamp := time.Now().Add(-1 * time.Hour).Format("20060102150405")
		createMigrationDir(t, fs, recentTimestamp+"_add_posts", "CREATE TABLE posts (id INT PRIMARY KEY);")

		squashBefore = 720 * time.Hour
		flags.Force = true
		defer func() { flags.Force = false }()

		err = doMigrationSquash(fs)
		require.NoError(t, err)

		// Reload migrations and verify squash flag
		migrations, err := loadMigrations(fs)
		require.NoError(t, err)

		// Should have 2: the squash migration and the recent one
		require.Len(t, migrations, 2)

		// First should be the squash
		assert.True(t, migrations[0].Squash, "squash migration should have Squash=true")
		assert.Equal(t, "20240201000000_squash", migrations[0].Name)

		// Second should be the recent one, not squash
		assert.False(t, migrations[1].Squash, "recent migration should have Squash=false")
	})
}

func TestSquashMigrationHeaderRoundTrip(t *testing.T) {
	t.Parallel()

	header := &migrationpkg.Header{
		Mode:   migrationpkg.ModeSync,
		Squash: true,
	}

	formatted := migrationpkg.FormatHeader(header)
	assert.Contains(t, formatted, "squash=true")

	parsed, err := migrationpkg.ParseHeader(formatted + "\nCREATE TABLE t (id INT);")
	require.NoError(t, err)
	require.NotNil(t, parsed)
	assert.True(t, parsed.Squash)
	assert.Equal(t, migrationpkg.ModeSync, parsed.Mode)
}

func TestFilterUnappliedMigrationsWithSquash(t *testing.T) {
	t.Parallel()

	allMigrations := []db.Migration{
		{Name: "20240101000000_squash", SQL: "CREATE TABLE users (id INT PRIMARY KEY);", Checksum: "abc123", Mode: db.MigrationModeSync, Squash: true},
		{Name: "20240601000000_add_posts", SQL: "CREATE TABLE posts (id INT PRIMARY KEY);", Checksum: "def456", Mode: db.MigrationModeSync},
	}

	// No applied migrations - both should be unapplied
	unapplied, warnings, err := filterUnappliedMigrations(allMigrations, nil)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Len(t, unapplied, 2)

	// Squash flag should be preserved through filtering
	assert.True(t, unapplied[0].Squash)
	assert.False(t, unapplied[1].Squash)
}
