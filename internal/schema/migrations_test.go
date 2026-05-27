package schema

import (
	"context"
	"strings"
	"testing"

	"github.com/pjtatlow/scurry/internal/db"
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

// TestForeignKeyDependsOnNewUniqueConstraint reproduces the bug where
// adding a new unique index (or unique constraint) to an existing table
// and creating a new table with a foreign key that references those
// unique columns produces statements in the wrong order: the new table
// is created before the unique constraint exists, so CockroachDB rejects
// the foreign key with "there is no unique constraint matching given keys
// for referenced table".
//
// The expected order is: the unique index (CREATE INDEX ... UNIQUE or
// ALTER TABLE ... ADD CONSTRAINT ... UNIQUE) must come before the
// CREATE TABLE that references it.
func TestForeignKeyDependsOnNewUniqueConstraint(t *testing.T) {
	tests := []struct {
		name         string
		remoteTables []string
		localTables  []string
		wantOrder    []string // substrings that must appear in this relative order
	}{
		{
			name: "new table FK references newly added UNIQUE INDEX on parent",
			remoteTables: []string{
				`CREATE TABLE public.parent (
					id INT8 NOT NULL,
					code STRING NOT NULL,
					CONSTRAINT parent_pkey PRIMARY KEY (id ASC)
				)`,
			},
			localTables: []string{
				`CREATE TABLE public.parent (
					id INT8 NOT NULL,
					code STRING NOT NULL,
					CONSTRAINT parent_pkey PRIMARY KEY (id ASC),
					UNIQUE INDEX parent_code_key (code ASC)
				)`,
				`CREATE TABLE public.child (
					id INT8 NOT NULL,
					parent_code STRING NOT NULL,
					CONSTRAINT child_pkey PRIMARY KEY (id ASC),
					CONSTRAINT child_parent_code_fkey FOREIGN KEY (parent_code) REFERENCES public.parent (code)
				)`,
			},
			wantOrder: []string{"CREATE UNIQUE INDEX parent_code_key", "CREATE TABLE public.child"},
		},
		{
			name: "new table FK references newly added UNIQUE constraint (named, multi-column) on parent",
			remoteTables: []string{
				`CREATE TABLE public.parent (
					id INT8 NOT NULL,
					tenant_id INT8 NOT NULL,
					code STRING NOT NULL,
					CONSTRAINT parent_pkey PRIMARY KEY (id ASC)
				)`,
			},
			localTables: []string{
				`CREATE TABLE public.parent (
					id INT8 NOT NULL,
					tenant_id INT8 NOT NULL,
					code STRING NOT NULL,
					CONSTRAINT parent_pkey PRIMARY KEY (id ASC),
					CONSTRAINT parent_tenant_code_key UNIQUE (tenant_id, code)
				)`,
				`CREATE TABLE public.child (
					id INT8 NOT NULL,
					tenant_id INT8 NOT NULL,
					parent_code STRING NOT NULL,
					CONSTRAINT child_pkey PRIMARY KEY (id ASC),
					CONSTRAINT child_parent_code_fkey FOREIGN KEY (tenant_id, parent_code) REFERENCES public.parent (tenant_id, code)
				)`,
			},
			wantOrder: []string{"parent_tenant_code_key", "CREATE TABLE public.child"},
		},
		{
			name: "new FK constraint (via ALTER TABLE) on existing table references newly added UNIQUE INDEX",
			remoteTables: []string{
				`CREATE TABLE public.parent (
					id INT8 NOT NULL,
					code STRING NOT NULL,
					CONSTRAINT parent_pkey PRIMARY KEY (id ASC)
				)`,
				`CREATE TABLE public.child (
					id INT8 NOT NULL,
					parent_code STRING NOT NULL,
					CONSTRAINT child_pkey PRIMARY KEY (id ASC)
				)`,
			},
			localTables: []string{
				`CREATE TABLE public.parent (
					id INT8 NOT NULL,
					code STRING NOT NULL,
					CONSTRAINT parent_pkey PRIMARY KEY (id ASC),
					UNIQUE INDEX parent_code_key (code ASC)
				)`,
				`CREATE TABLE public.child (
					id INT8 NOT NULL,
					parent_code STRING NOT NULL,
					CONSTRAINT child_pkey PRIMARY KEY (id ASC),
					CONSTRAINT child_parent_code_fkey FOREIGN KEY (parent_code) REFERENCES public.parent (code)
				)`,
			},
			wantOrder: []string{"CREATE UNIQUE INDEX parent_code_key", "child_parent_code_fkey"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localSchema := createSchemaWithTypesAndTables(nil, tt.localTables)
			remoteSchema := createSchemaWithTypesAndTables(nil, tt.remoteTables)

			diffResult := Compare(localSchema, remoteSchema)
			if !diffResult.HasChanges() {
				t.Fatal("expected changes but got none")
			}

			migrations, _, err := diffResult.GenerateMigrations(false)
			if err != nil {
				t.Fatalf("GenerateMigrations() error: %v", err)
			}

			allDDL := strings.Join(migrations, "\n")

			lastIndex := -1
			for _, want := range tt.wantOrder {
				index := strings.Index(allDDL[lastIndex+1:], want)
				if index == -1 {
					t.Errorf("expected %q to appear after position %d.\nGot:\n%s", want, lastIndex, allDDL)
					return
				}
				lastIndex = lastIndex + 1 + index
			}
		})
	}
}

// TestForeignKeyDependsOnNewUniqueConstraintApplies is an end-to-end check
// that the migration generated for "new FK references newly added unique
// constraint" actually applies to a CockroachDB instance — verifying the
// ordering fix produces SQL the database accepts.
func TestForeignKeyDependsOnNewUniqueConstraintApplies(t *testing.T) {
	tests := []struct {
		name       string
		remoteSQL  []string
		localSQL   []string
		exerciseFK string // INSERT SQL that should succeed if the FK points at a real unique constraint
	}{
		{
			name: "new child table FK references newly added UNIQUE INDEX on parent",
			remoteSQL: []string{
				"CREATE TABLE parent (id INT PRIMARY KEY, code STRING NOT NULL)",
			},
			localSQL: []string{
				"CREATE TABLE parent (id INT PRIMARY KEY, code STRING NOT NULL, UNIQUE INDEX parent_code_key (code))",
				"CREATE TABLE child (id INT PRIMARY KEY, parent_code STRING NOT NULL REFERENCES parent (code))",
			},
		},
		{
			name: "new FK constraint (ALTER TABLE) on existing child references newly added UNIQUE INDEX",
			remoteSQL: []string{
				"CREATE TABLE parent (id INT PRIMARY KEY, code STRING NOT NULL)",
				"CREATE TABLE child (id INT PRIMARY KEY, parent_code STRING NOT NULL)",
			},
			localSQL: []string{
				"CREATE TABLE parent (id INT PRIMARY KEY, code STRING NOT NULL, UNIQUE INDEX parent_code_key (code))",
				"CREATE TABLE child (id INT PRIMARY KEY, parent_code STRING NOT NULL, CONSTRAINT child_parent_code_fkey FOREIGN KEY (parent_code) REFERENCES parent (code))",
			},
		},
		{
			name: "new FK references newly added multi-column UNIQUE constraint",
			remoteSQL: []string{
				"CREATE TABLE parent (id INT PRIMARY KEY, tenant_id INT NOT NULL, code STRING NOT NULL)",
			},
			localSQL: []string{
				"CREATE TABLE parent (id INT PRIMARY KEY, tenant_id INT NOT NULL, code STRING NOT NULL, CONSTRAINT parent_tenant_code_key UNIQUE (tenant_id, code))",
				"CREATE TABLE child (id INT PRIMARY KEY, tenant_id INT NOT NULL, parent_code STRING NOT NULL, CONSTRAINT child_parent_code_fkey FOREIGN KEY (tenant_id, parent_code) REFERENCES parent (tenant_id, code))",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Build remote-state schema from the live DB and load local
			// schema from raw SQL to mirror the real diff path.
			remoteSchema := createSchemaWithTables(tt.remoteSQL)
			localSchema := createSchemaWithTables(tt.localSQL)

			diff := Compare(localSchema, remoteSchema)
			migrations, _, err := diff.GenerateMigrations(false)
			if err != nil {
				t.Fatalf("GenerateMigrations() error: %v", err)
			}

			client, err := db.GetShadowDB(ctx, tt.remoteSQL...)
			if err != nil {
				t.Fatalf("GetShadowDB failed: %v", err)
			}
			defer client.Close()

			if err := client.ExecuteBulkDDL(ctx, migrations...); err != nil {
				t.Fatalf("generated migration failed to apply: %v\n\nDDL:\n%s", err, strings.Join(migrations, ";\n"))
			}
		})
	}
}
