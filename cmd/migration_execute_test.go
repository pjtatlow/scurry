package cmd

import (
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/db"
	"github.com/pjtatlow/scurry/internal/flags"
)

func TestLoadMigrationsForExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		files      map[string]string // path -> content relative to migration dir
		wantCount  int
		wantNames  []string
		wantModes  []string
		wantDepsOn [][]string
	}{
		{
			name:      "empty directory",
			files:     nil,
			wantCount: 0,
		},
		{
			name: "single sync migration",
			files: map[string]string{
				"20250101000000_create_users/migration.sql": "CREATE TABLE users (id INT PRIMARY KEY);",
			},
			wantCount: 1,
			wantNames: []string{"20250101000000_create_users"},
			wantModes: []string{"sync"},
		},
		{
			name: "async migration with header",
			files: map[string]string{
				"20250101000000_add_index/migration.sql": "-- scurry:mode=async\nCREATE INDEX idx ON users (name);",
			},
			wantCount: 1,
			wantNames: []string{"20250101000000_add_index"},
			wantModes: []string{"async"},
		},
		{
			name: "async migration with depends_on",
			files: map[string]string{
				"20250101000000_create_users/migration.sql": "CREATE TABLE users (id INT PRIMARY KEY);",
				"20250102000000_add_index/migration.sql":    "-- scurry:mode=async,depends_on=20250101000000_create_users\nCREATE INDEX idx ON users (id);",
			},
			wantCount:  2,
			wantNames:  []string{"20250101000000_create_users", "20250102000000_add_index"},
			wantModes:  []string{"sync", "async"},
			wantDepsOn: [][]string{nil, {"20250101000000_create_users"}},
		},
		{
			name: "sorted by timestamp",
			files: map[string]string{
				"20250103000000_third/migration.sql": "CREATE TABLE c (id INT PRIMARY KEY);",
				"20250101000000_first/migration.sql": "CREATE TABLE a (id INT PRIMARY KEY);",
				"20250102000000_second/migration.sql": "CREATE TABLE b (id INT PRIMARY KEY);",
			},
			wantCount: 3,
			wantNames: []string{"20250101000000_first", "20250102000000_second", "20250103000000_third"},
		},
		{
			name: "skips non-directory files",
			files: map[string]string{
				"20250101000000_create_users/migration.sql": "CREATE TABLE users (id INT PRIMARY KEY);",
				"schema.sql": "-- schema file, not a migration",
			},
			wantCount: 1,
			wantNames: []string{"20250101000000_create_users"},
		},
		{
			name: "skips directories without migration.sql",
			files: map[string]string{
				"20250101000000_empty/.gitkeep": "",
				"20250102000000_valid/migration.sql": "CREATE TABLE t (id INT PRIMARY KEY);",
			},
			wantCount: 1,
			wantNames: []string{"20250102000000_valid"},
		},
		{
			name: "header stripped from SQL",
			files: map[string]string{
				"20250101000000_async/migration.sql": "-- scurry:mode=async\nCREATE INDEX idx ON t (col);",
			},
			wantCount: 1,
			wantNames: []string{"20250101000000_async"},
			wantModes: []string{"async"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fs := afero.NewMemMapFs()

			err := fs.MkdirAll(flags.MigrationDir, 0755)
			require.NoError(t, err)

			for path, content := range tt.files {
				fullPath := filepath.Join(flags.MigrationDir, path)
				err := fs.MkdirAll(filepath.Dir(fullPath), 0755)
				require.NoError(t, err)
				err = afero.WriteFile(fs, fullPath, []byte(content), 0644)
				require.NoError(t, err)
			}

			migrations, err := loadMigrationsForExecution(fs)
			require.NoError(t, err)
			assert.Len(t, migrations, tt.wantCount)

			if tt.wantNames != nil {
				for i, name := range tt.wantNames {
					assert.Equal(t, name, migrations[i].Name)
				}
			}

			if tt.wantModes != nil {
				for i, mode := range tt.wantModes {
					assert.Equal(t, mode, migrations[i].Mode)
				}
			}

			if tt.wantDepsOn != nil {
				for i, deps := range tt.wantDepsOn {
					assert.Equal(t, deps, migrations[i].DependsOn)
				}
			}

			// Verify headers are stripped from SQL
			for _, m := range migrations {
				assert.NotContains(t, m.SQL, "-- scurry:")
			}
		})
	}
}

func TestFilterUnappliedMigrations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		allMigrations    []db.Migration
		applied          []db.AppliedMigration
		wantUnapplied    []string
		wantWarningCount int
	}{
		{
			name: "all new migrations",
			allMigrations: []db.Migration{
				{Name: "20250101_a", Checksum: "aaa"},
				{Name: "20250102_b", Checksum: "bbb"},
			},
			applied:       nil,
			wantUnapplied: []string{"20250101_a", "20250102_b"},
		},
		{
			name: "all already applied",
			allMigrations: []db.Migration{
				{Name: "20250101_a", Checksum: "aaa"},
			},
			applied: []db.AppliedMigration{
				{Name: "20250101_a", Checksum: "aaa"},
			},
			wantUnapplied: nil,
		},
		{
			name: "checksum mismatch produces warning",
			allMigrations: []db.Migration{
				{Name: "20250101_a", Checksum: "new_checksum"},
			},
			applied: []db.AppliedMigration{
				{Name: "20250101_a", Checksum: "old_checksum"},
			},
			wantUnapplied:    nil,
			wantWarningCount: 1,
		},
		{
			name: "empty stored checksum does not warn",
			allMigrations: []db.Migration{
				{Name: "20250101_a", Checksum: "aaa"},
			},
			applied: []db.AppliedMigration{
				{Name: "20250101_a", Checksum: ""},
			},
			wantUnapplied:    nil,
			wantWarningCount: 0,
		},
		{
			name: "mix of applied and unapplied",
			allMigrations: []db.Migration{
				{Name: "20250101_a", Checksum: "aaa"},
				{Name: "20250102_b", Checksum: "bbb"},
				{Name: "20250103_c", Checksum: "ccc"},
			},
			applied: []db.AppliedMigration{
				{Name: "20250101_a", Checksum: "aaa"},
			},
			wantUnapplied: []string{"20250102_b", "20250103_c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			unapplied, warnings, err := filterUnappliedMigrations(tt.allMigrations, tt.applied)
			require.NoError(t, err)

			var names []string
			for _, m := range unapplied {
				names = append(names, m.Name)
			}
			assert.Equal(t, tt.wantUnapplied, names)
			assert.Len(t, warnings, tt.wantWarningCount)
		})
	}
}
