package cmd

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
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
