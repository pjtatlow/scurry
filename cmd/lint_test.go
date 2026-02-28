package cmd

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/stretchr/testify/assert"
)

func TestCheckTableForeignKeyIndexes(t *testing.T) {
	tests := []struct {
		name       string
		tableSQL   string
		wantIssues int
	}{
		{
			name: "FK with explicit index",
			tableSQL: `CREATE TABLE orders (
				id INT PRIMARY KEY,
				user_id INT,
				FOREIGN KEY (user_id) REFERENCES users(id),
				INDEX idx_user_id (user_id)
			)`,
			wantIssues: 0,
		},
		{
			name: "FK without index",
			tableSQL: `CREATE TABLE orders (
				id INT PRIMARY KEY,
				user_id INT,
				FOREIGN KEY (user_id) REFERENCES users(id)
			)`,
			wantIssues: 1,
		},
		{
			name: "FK covered by primary key",
			tableSQL: `CREATE TABLE order_items (
				order_id INT,
				item_id INT,
				quantity INT,
				PRIMARY KEY (order_id, item_id),
				FOREIGN KEY (order_id) REFERENCES orders(id)
			)`,
			wantIssues: 0,
		},
		{
			name: "FK covered by unique constraint",
			tableSQL: `CREATE TABLE orders (
				id INT PRIMARY KEY,
				user_id INT,
				order_number STRING,
				FOREIGN KEY (user_id) REFERENCES users(id),
				UNIQUE (user_id, order_number)
			)`,
			wantIssues: 0,
		},
		{
			name: "composite FK with matching index",
			tableSQL: `CREATE TABLE line_items (
				id INT PRIMARY KEY,
				order_id INT,
				product_id INT,
				FOREIGN KEY (order_id, product_id) REFERENCES order_products(order_id, product_id),
				INDEX idx_order_product (order_id, product_id)
			)`,
			wantIssues: 0,
		},
		{
			name: "composite FK without index",
			tableSQL: `CREATE TABLE line_items (
				id INT PRIMARY KEY,
				order_id INT,
				product_id INT,
				FOREIGN KEY (order_id, product_id) REFERENCES order_products(order_id, product_id)
			)`,
			wantIssues: 1,
		},
		{
			name: "composite FK with partial index coverage",
			tableSQL: `CREATE TABLE line_items (
				id INT PRIMARY KEY,
				order_id INT,
				product_id INT,
				FOREIGN KEY (order_id, product_id) REFERENCES order_products(order_id, product_id),
				INDEX idx_order (order_id)
			)`,
			wantIssues: 1,
		},
		{
			name: "FK covered by prefix of larger index",
			tableSQL: `CREATE TABLE orders (
				id INT PRIMARY KEY,
				user_id INT,
				created_at TIMESTAMP,
				FOREIGN KEY (user_id) REFERENCES users(id),
				INDEX idx_user_created (user_id, created_at)
			)`,
			wantIssues: 0,
		},
		{
			name: "FK not covered by suffix of index",
			tableSQL: `CREATE TABLE orders (
				id INT PRIMARY KEY,
				user_id INT,
				created_at TIMESTAMP,
				FOREIGN KEY (user_id) REFERENCES users(id),
				INDEX idx_created_user (created_at, user_id)
			)`,
			wantIssues: 1,
		},
		{
			name: "multiple FKs some covered some not",
			tableSQL: `CREATE TABLE orders (
				id INT PRIMARY KEY,
				user_id INT,
				store_id INT,
				FOREIGN KEY (user_id) REFERENCES users(id),
				FOREIGN KEY (store_id) REFERENCES stores(id),
				INDEX idx_user (user_id)
			)`,
			wantIssues: 1,
		},
		{
			name: "no foreign keys",
			tableSQL: `CREATE TABLE users (
				id INT PRIMARY KEY,
				name STRING
			)`,
			wantIssues: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmts, err := parser.Parse(tt.tableSQL)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}

			createTable, ok := stmts[0].AST.(*tree.CreateTable)
			if !ok {
				t.Fatalf("expected CreateTable, got %T", stmts[0].AST)
			}

			issues := checkTableForeignKeyIndexes("test_table", createTable)

			if len(issues) != tt.wantIssues {
				t.Errorf("expected %d issues, got %d: %+v", tt.wantIssues, len(issues), issues)
			}
		})
	}
}

func TestCheckTableNullableUniqueColumns(t *testing.T) {
	tests := []struct {
		name       string
		tableSQL   string
		wantIssues int
	}{
		{
			name: "unique constraint with all NOT NULL columns",
			tableSQL: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email STRING NOT NULL,
				UNIQUE (email)
			)`,
			wantIssues: 0,
		},
		{
			name: "unique constraint with nullable column",
			tableSQL: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email STRING,
				UNIQUE (email)
			)`,
			wantIssues: 1,
		},
		{
			name: "unique constraint with multiple columns one nullable",
			tableSQL: `CREATE TABLE users (
				id INT PRIMARY KEY,
				tenant_id INT NOT NULL,
				email STRING,
				UNIQUE (tenant_id, email)
			)`,
			wantIssues: 1,
		},
		{
			name: "unique constraint with multiple nullable columns",
			tableSQL: `CREATE TABLE users (
				id INT PRIMARY KEY,
				first_name STRING,
				last_name STRING,
				UNIQUE (first_name, last_name)
			)`,
			wantIssues: 2,
		},
		{
			name: "primary key columns are not flagged",
			tableSQL: `CREATE TABLE users (
				id INT PRIMARY KEY,
				name STRING
			)`,
			wantIssues: 0,
		},
		{
			name: "composite primary key not flagged",
			tableSQL: `CREATE TABLE order_items (
				order_id INT,
				item_id INT,
				PRIMARY KEY (order_id, item_id)
			)`,
			wantIssues: 0,
		},
		{
			name: "multiple unique constraints mixed",
			tableSQL: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email STRING NOT NULL,
				phone STRING,
				UNIQUE (email),
				UNIQUE (phone)
			)`,
			wantIssues: 1,
		},
		{
			name: "no unique constraints",
			tableSQL: `CREATE TABLE users (
				id INT PRIMARY KEY,
				name STRING,
				INDEX idx_name (name)
			)`,
			wantIssues: 0,
		},
		{
			name: "explicitly NULL column in unique constraint",
			tableSQL: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email STRING NULL,
				UNIQUE (email)
			)`,
			wantIssues: 1,
		},
		{
			name: "nullable column with WHERE IS NOT NULL predicate",
			tableSQL: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email STRING,
				UNIQUE INDEX idx_email (email) WHERE email IS NOT NULL
			)`,
			wantIssues: 0,
		},
		{
			name: "multiple nullable columns all guarded by WHERE IS NOT NULL",
			tableSQL: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email STRING,
				phone STRING,
				UNIQUE INDEX idx_both (email, phone) WHERE email IS NOT NULL AND phone IS NOT NULL
			)`,
			wantIssues: 0,
		},
		{
			name: "multiple nullable columns only one guarded by WHERE IS NOT NULL",
			tableSQL: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email STRING,
				phone STRING,
				UNIQUE INDEX idx_both (email, phone) WHERE email IS NOT NULL
			)`,
			wantIssues: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmts, err := parser.Parse(tt.tableSQL)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}

			createTable, ok := stmts[0].AST.(*tree.CreateTable)
			if !ok {
				t.Fatalf("expected CreateTable, got %T", stmts[0].AST)
			}

			issues := checkTableNullableUniqueColumns("test_table", createTable)

			if len(issues) != tt.wantIssues {
				t.Errorf("expected %d issues, got %d: %+v", tt.wantIssues, len(issues), issues)
			}
			for _, issue := range issues {
				if issue.Rule != "nullable-unique" {
					t.Errorf("expected rule %q, got %q", "nullable-unique", issue.Rule)
				}
			}
		})
	}
}

func TestAllPrefixes(t *testing.T) {
	tests := []struct {
		name string
		cols []string
		want [][]string
	}{
		{
			name: "single column",
			cols: []string{"a"},
			want: [][]string{{"a"}},
		},
		{
			name: "two columns",
			cols: []string{"a", "b"},
			want: [][]string{{"a"}, {"a", "b"}},
		},
		{
			name: "three columns",
			cols: []string{"a", "b", "c"},
			want: [][]string{{"a"}, {"a", "b"}, {"a", "b", "c"}},
		},
		{
			name: "empty",
			cols: []string{},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := allPrefixes(tt.cols)

			if len(got) != len(tt.want) {
				t.Fatalf("expected %d prefixes, got %d", len(tt.want), len(got))
			}

			for i := range got {
				if !columnsMatch(got[i], tt.want[i]) {
					t.Errorf("prefix %d: expected %v, got %v", i, tt.want[i], got[i])
				}
			}
		})
	}
}

func TestCheckTableTTLIndexes(t *testing.T) {
	tests := []struct {
		name       string
		tableSQL   string
		wantIssues int
	}{
		{
			name: "no TTL expression",
			tableSQL: `CREATE TABLE users (
				id INT PRIMARY KEY,
				created_at TIMESTAMPTZ
			)`,
			wantIssues: 0,
		},
		{
			name: "TTL expression with covering index",
			tableSQL: `CREATE TABLE sessions (
				id INT PRIMARY KEY,
				created_at TIMESTAMPTZ,
				INDEX idx_created_at (created_at)
			) WITH (
				ttl_expiration_expression = 'created_at + INTERVAL ''7 days''',
				ttl_job_cron = '@hourly'
			)`,
			wantIssues: 0,
		},
		{
			name: "TTL expression without covering index",
			tableSQL: `CREATE TABLE sessions (
				id INT PRIMARY KEY,
				created_at TIMESTAMPTZ
			) WITH (
				ttl_expiration_expression = 'created_at + INTERVAL ''7 days''',
				ttl_job_cron = '@hourly'
			)`,
			wantIssues: 1,
		},
		{
			name: "TTL expression column covered by composite index prefix",
			tableSQL: `CREATE TABLE sessions (
				id INT PRIMARY KEY,
				created_at TIMESTAMPTZ,
				team_id INT,
				INDEX idx_created_team (created_at, team_id)
			) WITH (
				ttl_expiration_expression = 'created_at + INTERVAL ''7 days''',
				ttl_job_cron = '@hourly'
			)`,
			wantIssues: 0,
		},
		{
			name: "TTL expression column not first in composite index",
			tableSQL: `CREATE TABLE sessions (
				id INT PRIMARY KEY,
				created_at TIMESTAMPTZ,
				team_id INT,
				INDEX idx_team_created (team_id, created_at)
			) WITH (
				ttl_expiration_expression = 'created_at + INTERVAL ''7 days''',
				ttl_job_cron = '@hourly'
			)`,
			wantIssues: 1,
		},
		{
			name: "TTL expression column covered by unique constraint",
			tableSQL: `CREATE TABLE sessions (
				id INT PRIMARY KEY,
				expires_at TIMESTAMPTZ NOT NULL,
				UNIQUE (expires_at)
			) WITH (
				ttl_expiration_expression = 'expires_at',
				ttl_job_cron = '@hourly'
			)`,
			wantIssues: 0,
		},
		{
			name: "TTL expression column covered by primary key",
			tableSQL: `CREATE TABLE events (
				created_at TIMESTAMPTZ NOT NULL,
				event_id INT NOT NULL,
				PRIMARY KEY (created_at, event_id)
			) WITH (
				ttl_expiration_expression = 'created_at + INTERVAL ''30 days''',
				ttl_job_cron = '@daily'
			)`,
			wantIssues: 0,
		},
		{
			name: "TTL with simple column reference no interval",
			tableSQL: `CREATE TABLE tokens (
				id INT PRIMARY KEY,
				expires_at TIMESTAMPTZ
			) WITH (
				ttl_expiration_expression = 'expires_at',
				ttl_job_cron = '@hourly'
			)`,
			wantIssues: 1,
		},
		{
			name: "TTL expression with unrelated indexes",
			tableSQL: `CREATE TABLE sessions (
				id INT PRIMARY KEY,
				created_at TIMESTAMPTZ,
				team_id INT,
				user_id INT,
				INDEX idx_team (team_id),
				INDEX idx_user (user_id)
			) WITH (
				ttl_expiration_expression = 'created_at + INTERVAL ''7 days''',
				ttl_job_cron = '@hourly'
			)`,
			wantIssues: 1,
		},
		{
			name: "partial index on TTL column does not count",
			tableSQL: `CREATE TABLE sessions (
				id INT PRIMARY KEY,
				created_at TIMESTAMPTZ,
				active BOOL,
				INDEX idx_created_at (created_at) WHERE active = true
			) WITH (
				ttl_expiration_expression = 'created_at + INTERVAL ''7 days''',
				ttl_job_cron = '@hourly'
			)`,
			wantIssues: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmts, err := parser.Parse(tt.tableSQL)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}

			createTable, ok := stmts[0].AST.(*tree.CreateTable)
			if !ok {
				t.Fatalf("expected CreateTable, got %T", stmts[0].AST)
			}

			issues := checkTableTTLIndexes("test_table", createTable)

			if len(issues) != tt.wantIssues {
				t.Errorf("expected %d issues, got %d: %+v", tt.wantIssues, len(issues), issues)
			}
		})
	}
}

func TestExtractColumnsFromExpression(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		wantCols []string
	}{
		{
			name:     "simple column reference",
			expr:     "expires_at",
			wantCols: []string{"expires_at"},
		},
		{
			name:     "column plus interval",
			expr:     "created_at + INTERVAL '7 days'",
			wantCols: []string{"created_at"},
		},
		{
			name:     "function wrapping column",
			expr:     "COALESCE(deleted_at, created_at + INTERVAL '30 days')",
			wantCols: []string{"deleted_at", "created_at"},
		},
		{
			name:     "invalid expression",
			expr:     "???invalid???",
			wantCols: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := extractColumnsFromExpression(tt.expr)

			if len(got) != len(tt.wantCols) {
				t.Fatalf("expected %d columns, got %d: %v", len(tt.wantCols), len(got), got)
			}

			for i := range got {
				if got[i] != tt.wantCols[i] {
					t.Errorf("column %d: expected %q, got %q", i, tt.wantCols[i], got[i])
				}
			}
		})
	}
}

func TestColumnsMatch(t *testing.T) {
	tests := []struct {
		name string
		a    []string
		b    []string
		want bool
	}{
		{
			name: "equal single",
			a:    []string{"a"},
			b:    []string{"a"},
			want: true,
		},
		{
			name: "equal multiple",
			a:    []string{"a", "b", "c"},
			b:    []string{"a", "b", "c"},
			want: true,
		},
		{
			name: "different length",
			a:    []string{"a", "b"},
			b:    []string{"a"},
			want: false,
		},
		{
			name: "different values",
			a:    []string{"a", "b"},
			b:    []string{"a", "c"},
			want: false,
		},
		{
			name: "different order",
			a:    []string{"a", "b"},
			b:    []string{"b", "a"},
			want: false,
		},
		{
			name: "both empty",
			a:    []string{},
			b:    []string{},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := columnsMatch(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

func TestParseLintDisables(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []lintDisable
	}{
		{
			name: "no directives",
			sql:  "CREATE TABLE users (id INT PRIMARY KEY);",
			want: nil,
		},
		{
			name: "rule only",
			sql: `-- scurry:lint-disable=nullable-unique
CREATE TABLE users (id INT PRIMARY KEY);`,
			want: []lintDisable{
				{Rule: "nullable-unique"},
			},
		},
		{
			name: "rule with table",
			sql: `-- scurry:lint-disable=nullable-unique:users
CREATE TABLE users (id INT PRIMARY KEY);`,
			want: []lintDisable{
				{Rule: "nullable-unique", Table: "users"},
			},
		},
		{
			name: "rule with table and constraint",
			sql: `-- scurry:lint-disable=nullable-unique:users.phone_key
CREATE TABLE users (id INT PRIMARY KEY);`,
			want: []lintDisable{
				{Rule: "nullable-unique", Table: "users", Constraint: "phone_key"},
			},
		},
		{
			name: "multiple directives",
			sql: `-- scurry:lint-disable=nullable-unique:users.phone_key
-- scurry:lint-disable=fk-missing-index:orders.fk_user_id
CREATE TABLE users (id INT PRIMARY KEY);`,
			want: []lintDisable{
				{Rule: "nullable-unique", Table: "users", Constraint: "phone_key"},
				{Rule: "fk-missing-index", Table: "orders", Constraint: "fk_user_id"},
			},
		},
		{
			name: "stops at non-comment line",
			sql: `-- scurry:lint-disable=nullable-unique
CREATE TABLE users (id INT PRIMARY KEY);
-- scurry:lint-disable=fk-missing-index`,
			want: []lintDisable{
				{Rule: "nullable-unique"},
			},
		},
		{
			name: "skips regular comments",
			sql: `-- This is a regular comment
-- scurry:lint-disable=nullable-unique
CREATE TABLE users (id INT PRIMARY KEY);`,
			want: []lintDisable{
				{Rule: "nullable-unique"},
			},
		},
		{
			name: "skips blank lines between directives",
			sql: `-- scurry:lint-disable=nullable-unique

-- scurry:lint-disable=fk-missing-index
CREATE TABLE users (id INT PRIMARY KEY);`,
			want: []lintDisable{
				{Rule: "nullable-unique"},
				{Rule: "fk-missing-index"},
			},
		},
		{
			name: "inline comment after directive",
			sql: `-- scurry:lint-disable=nullable-unique -- phone can be null
CREATE TABLE users (id INT PRIMARY KEY);`,
			want: []lintDisable{
				{Rule: "nullable-unique"},
			},
		},
		{
			name: "empty value ignored",
			sql: `-- scurry:lint-disable=
CREATE TABLE users (id INT PRIMARY KEY);`,
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := parseLintDisables(tt.sql)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsSuppressed(t *testing.T) {
	tests := []struct {
		name     string
		issue    LintIssue
		disables map[string][]lintDisable
		want     bool
	}{
		{
			name: "no disables",
			issue: LintIssue{
				Rule: "nullable-unique", Table: "users", Constraint: "phone_key",
			},
			disables: map[string][]lintDisable{},
			want:     false,
		},
		{
			name: "rule-only suppresses all",
			issue: LintIssue{
				Rule: "nullable-unique", Table: "users", Constraint: "phone_key",
			},
			disables: map[string][]lintDisable{
				"users": {{Rule: "nullable-unique"}},
			},
			want: true,
		},
		{
			name: "table-level suppresses all constraints",
			issue: LintIssue{
				Rule: "nullable-unique", Table: "users", Constraint: "phone_key",
			},
			disables: map[string][]lintDisable{
				"users": {{Rule: "nullable-unique", Table: "users"}},
			},
			want: true,
		},
		{
			name: "constraint-level exact match",
			issue: LintIssue{
				Rule: "nullable-unique", Table: "users", Constraint: "phone_key",
			},
			disables: map[string][]lintDisable{
				"users": {{Rule: "nullable-unique", Table: "users", Constraint: "phone_key"}},
			},
			want: true,
		},
		{
			name: "constraint-level no match",
			issue: LintIssue{
				Rule: "nullable-unique", Table: "users", Constraint: "phone_key",
			},
			disables: map[string][]lintDisable{
				"users": {{Rule: "nullable-unique", Table: "users", Constraint: "email_key"}},
			},
			want: false,
		},
		{
			name: "different rule not suppressed",
			issue: LintIssue{
				Rule: "fk-missing-index", Table: "users", Constraint: "fk_org_id",
			},
			disables: map[string][]lintDisable{
				"users": {{Rule: "nullable-unique"}},
			},
			want: false,
		},
		{
			name: "different table not suppressed",
			issue: LintIssue{
				Rule: "nullable-unique", Table: "orders", Constraint: "phone_key",
			},
			disables: map[string][]lintDisable{
				"users": {{Rule: "nullable-unique", Table: "users"}},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isSuppressed(tt.issue, tt.disables)
			assert.Equal(t, tt.want, got)
		})
	}
}
