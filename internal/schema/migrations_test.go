package schema

import (
	"strings"
	"testing"
)

func TestWarningCommentsInMigrations(t *testing.T) {
	tests := []struct {
		name             string
		localTables      []string
		remoteTables     []string
		wantWarning      string
		wantCommentInSQL bool
	}{
		{
			name: "non-nullable column without default generates warning comment",
			localTables: []string{
				"CREATE TABLE users (id INT PRIMARY KEY, email STRING NOT NULL)",
			},
			remoteTables: []string{
				"CREATE TABLE users (id INT PRIMARY KEY)",
			},
			wantWarning:      "Column 'public.users.email' is non-nullable but has no default value",
			wantCommentInSQL: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSchema := createSchemaWithTables(tt.localTables)
			remoteSchema := createSchemaWithTables(tt.remoteTables)

			diffResult := Compare(localSchema, remoteSchema)

			if !diffResult.HasChanges() {
				t.Fatal("expected changes but got none")
			}

			// Generate migrations in pretty mode
			migrations, warnings, err := diffResult.GenerateMigrations(true)
			if err != nil {
				t.Fatalf("GenerateMigrations() error: %v", err)
			}

			// Verify warning was returned
			if len(warnings) == 0 {
				t.Error("expected at least one warning, got none")
			}

			// Check that the warning message contains expected text
			foundWarning := false
			for _, w := range warnings {
				if strings.Contains(w, tt.wantWarning) {
					foundWarning = true
					break
				}
			}
			if !foundWarning {
				t.Errorf("expected warning containing %q, got: %v", tt.wantWarning, warnings)
			}

			// Verify warning comment appears in SQL
			if tt.wantCommentInSQL {
				allSQL := strings.Join(migrations, "\n")

				// Print the actual SQL for demonstration
				t.Logf("Generated SQL with warning comment:\n%s", allSQL)

				if !strings.Contains(allSQL, "-- WARNING:") {
					t.Errorf("expected SQL to contain '-- WARNING:' comment, got:\n%s", allSQL)
				}
				if !strings.Contains(allSQL, tt.wantWarning) {
					t.Errorf("expected SQL to contain warning text %q, got:\n%s", tt.wantWarning, allSQL)
				}

				// Verify comment appears before ALTER TABLE statement
				warningIndex := strings.Index(allSQL, "-- WARNING:")
				alterIndex := strings.Index(allSQL, "ALTER TABLE")
				if warningIndex == -1 || alterIndex == -1 || warningIndex >= alterIndex {
					t.Errorf("expected '-- WARNING:' to appear before 'ALTER TABLE', got:\n%s", allSQL)
				}
			}
		})
	}
}

func TestFormatWarningComment(t *testing.T) {
	tests := []struct {
		name    string
		warning string
		want    string
	}{
		{
			name:    "single line warning",
			warning: "This is a warning",
			want:    "-- WARNING: This is a warning",
		},
		{
			name:    "multi-line warning",
			warning: "Line 1\nLine 2",
			want:    "-- WARNING: Line 1\n-- WARNING: Line 2",
		},
		{
			name:    "empty warning",
			warning: "",
			want:    "",
		},
		{
			name:    "warning with empty lines",
			warning: "Line 1\n\nLine 3",
			want:    "-- WARNING: Line 1\n-- WARNING: Line 3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatWarningComment(tt.warning)
			if got != tt.want {
				t.Errorf("formatWarningComment() = %q, want %q", got, tt.want)
			}
		})
	}
}
