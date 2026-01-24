package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseErrorReport(t *testing.T) {
	tests := []struct {
		name                   string
		content                string
		expectedError          string
		expectedLocalCount     int
		expectedRemoteCount    int
		expectedMigrationCount int
		expectedMigrations     []string
	}{
		{
			name: "complete error report",
			content: `generated: "2024-01-15T10:30:00Z"
error: "failed to apply migrations: pq: column \"email\" does not exist"
local_schema:
  - |
    CREATE TABLE users (
        id INT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL
    );
remote_schema:
  - |
    CREATE TABLE users (
        id INT PRIMARY KEY,
        name TEXT NOT NULL
    );
migrations:
  - "ALTER TABLE users ADD COLUMN email TEXT NOT NULL;"
`,
			expectedError:          "failed to apply migrations: pq: column \"email\" does not exist",
			expectedLocalCount:     1,
			expectedRemoteCount:    1,
			expectedMigrationCount: 1,
			expectedMigrations:     []string{"ALTER TABLE users ADD COLUMN email TEXT NOT NULL;"},
		},
		{
			name: "multiple statements",
			content: `generated: "2024-01-15T10:30:00Z"
error: "test error"
local_schema:
  - "CREATE TABLE users (id INT PRIMARY KEY);"
  - "CREATE TABLE posts (id INT PRIMARY KEY);"
remote_schema:
  - "CREATE TABLE users (id INT PRIMARY KEY);"
migrations:
  - "CREATE TABLE posts (id INT PRIMARY KEY);"
  - "ALTER TABLE users ADD COLUMN name TEXT;"
`,
			expectedError:          "test error",
			expectedLocalCount:     2,
			expectedRemoteCount:    1,
			expectedMigrationCount: 2,
		},
		{
			name: "no migrations generated",
			content: `generated: "2024-01-15T10:30:00Z"
error: "failed to load schema"
local_schema:
  - "CREATE TABLE users (id INT PRIMARY KEY);"
remote_schema:
  - "CREATE TABLE users (id INT PRIMARY KEY);"
`,
			expectedError:          "failed to load schema",
			expectedLocalCount:     1,
			expectedRemoteCount:    1,
			expectedMigrationCount: 0,
		},
		{
			name: "empty schemas",
			content: `generated: "2024-01-15T10:30:00Z"
error: "error message"
local_schema: []
remote_schema: []
migrations: []
`,
			expectedError:          "error message",
			expectedLocalCount:     0,
			expectedRemoteCount:    0,
			expectedMigrationCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write content to temp file
			tmpDir := t.TempDir()
			reportPath := filepath.Join(tmpDir, "error-report.yaml")
			err := os.WriteFile(reportPath, []byte(tt.content), 0644)
			require.NoError(t, err)

			// Parse the report
			report, err := parseErrorReport(reportPath)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedError, report.Error)
			assert.Len(t, report.LocalStatements, tt.expectedLocalCount,
				"Expected %d local statements, got %d: %v",
				tt.expectedLocalCount, len(report.LocalStatements), report.LocalStatements)
			assert.Len(t, report.RemoteStatements, tt.expectedRemoteCount,
				"Expected %d remote statements, got %d: %v",
				tt.expectedRemoteCount, len(report.RemoteStatements), report.RemoteStatements)
			assert.Len(t, report.Migrations, tt.expectedMigrationCount,
				"Expected %d migrations, got %d: %v",
				tt.expectedMigrationCount, len(report.Migrations), report.Migrations)

			// Check specific migrations if expected
			for i, expected := range tt.expectedMigrations {
				if i < len(report.Migrations) {
					assert.Equal(t, expected, report.Migrations[i])
				}
			}
		})
	}
}

func TestParseErrorReport_FileNotFound(t *testing.T) {
	_, err := parseErrorReport("/nonexistent/path/to/file.yaml")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to open error report")
}

func TestParseErrorReport_InvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	reportPath := filepath.Join(tmpDir, "invalid.yaml")
	err := os.WriteFile(reportPath, []byte("not: valid: yaml: content"), 0644)
	require.NoError(t, err)

	_, err = parseErrorReport(reportPath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse error report")
}
