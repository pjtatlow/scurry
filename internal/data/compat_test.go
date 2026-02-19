package data

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pjtatlow/scurry/internal/schema"
)

func parseTestSchema(t *testing.T, sql string) *schema.Schema {
	t.Helper()
	stmts, err := schema.ParseSQL(sql)
	require.NoError(t, err)
	return schema.NewSchema(stmts...)
}

func TestCheckCompatibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		dumpSQL      string
		targetSQL    string
		wantErrors   []string // substrings expected in error-severity issues
		wantWarnings []string // substrings expected in warning-severity issues
	}{
		{
			name:      "identical schemas",
			dumpSQL:   "CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
			targetSQL: "CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
		},
		{
			name:       "missing table in target",
			dumpSQL:    "CREATE TABLE public.users (id INT8 PRIMARY KEY)",
			targetSQL:  "CREATE TABLE public.other (id INT8 PRIMARY KEY)",
			wantErrors: []string{"Table 'public.users' exists in dump but not in target"},
		},
		{
			name:       "missing column in target",
			dumpSQL:    "CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL, email STRING)",
			targetSQL:  "CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
			wantErrors: []string{"Column 'public.users.email' exists in dump but not in target"},
		},
		{
			name:       "new required column in target",
			dumpSQL:    "CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
			targetSQL:  "CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL, age INT8 NOT NULL)",
			wantErrors: []string{"NOT NULL column 'public.users.age' without DEFAULT"},
		},
		{
			name:         "extra nullable column in target",
			dumpSQL:      "CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
			targetSQL:    "CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL, bio STRING)",
			wantWarnings: []string{"column 'public.users.bio' not present in dump"},
		},
		{
			name:         "extra column with default in target",
			dumpSQL:      "CREATE TABLE public.users (id INT8 PRIMARY KEY)",
			targetSQL:    "CREATE TABLE public.users (id INT8 PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now())",
			wantWarnings: []string{"column 'public.users.created_at' not present in dump"},
		},
		{
			name:         "type mismatch",
			dumpSQL:      "CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
			targetSQL:    "CREATE TABLE public.users (id INT8 PRIMARY KEY, name INT8 NOT NULL)",
			wantWarnings: []string{"type differs"},
		},
		{
			name:      "computed column in target ignored",
			dumpSQL:   "CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)",
			targetSQL: "CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL, full_name STRING AS (name) STORED)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dumpSchema := parseTestSchema(t, tt.dumpSQL)
			targetSchema := parseTestSchema(t, tt.targetSQL)

			issues := CheckCompatibility(dumpSchema, targetSchema)

			var errors []CompatibilityIssue
			var warnings []CompatibilityIssue
			for _, issue := range issues {
				switch issue.Severity {
				case "error":
					errors = append(errors, issue)
				case "warning":
					warnings = append(warnings, issue)
				default:
					t.Fatalf("unexpected severity: %s", issue.Severity)
				}
			}

			// Check errors
			assert.Len(t, errors, len(tt.wantErrors), "expected %d errors, got %d: %v", len(tt.wantErrors), len(errors), errors)
			for i, wantSub := range tt.wantErrors {
				if i < len(errors) {
					assert.Contains(t, errors[i].Description, wantSub)
				}
			}

			// Check warnings
			assert.Len(t, warnings, len(tt.wantWarnings), "expected %d warnings, got %d: %v", len(tt.wantWarnings), len(warnings), warnings)
			for i, wantSub := range tt.wantWarnings {
				if i < len(warnings) {
					assert.Contains(t, warnings[i].Description, wantSub)
				}
			}
		})
	}
}

func TestExtractColumns(t *testing.T) {
	t.Parallel()

	stmts, err := schema.ParseSQL("CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL, bio STRING)")
	require.NoError(t, err)
	ct := stmts[0].(*tree.CreateTable)
	ct.HoistConstraints()

	cols := extractColumns(ct)

	assert.Len(t, cols, 3)
	assert.Contains(t, cols, "id")
	assert.Contains(t, cols, "name")
	assert.Contains(t, cols, "bio")
}
