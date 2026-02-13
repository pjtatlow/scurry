package schema

import (
	"slices"
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"

	"github.com/pjtatlow/scurry/internal/set"
)

func TestGetExprDeps(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		wantDeps []string
	}{
		{
			name:     "simple literal",
			expr:     "42",
			wantDeps: []string{},
		},
		{
			name:     "string literal",
			expr:     "'hello'",
			wantDeps: []string{},
		},
		{
			name:     "boolean literal",
			expr:     "true",
			wantDeps: []string{},
		},
		{
			name:     "unqualified name",
			expr:     "foo",
			wantDeps: []string{"public.foo"},
		},
		{
			name:     "schema qualified name",
			expr:     "myschema.bar",
			wantDeps: []string{"myschema.bar"},
		},
		{
			name:     "function call without schema",
			expr:     "now()",
			wantDeps: []string{"public.now"},
		},
		{
			name:     "function call with schema",
			expr:     "pg_catalog.now()",
			wantDeps: []string{"pg_catalog.now"},
		},
		{
			name:     "cast to custom type",
			expr:     "'value'::myschema.mytype",
			wantDeps: []string{"myschema.mytype", "myschema.mytype.value"},
		},
		{
			name:     "cast to custom type without schema",
			expr:     "'value'::mytype",
			wantDeps: []string{"public.mytype", "public.mytype.value"},
		},
		{
			name:     "type annotation to custom type",
			expr:     "'value':::myschema.mytype",
			wantDeps: []string{"myschema.mytype", "myschema.mytype.value"},
		},
		{
			name:     "type annotation to custom type without schema",
			expr:     "'value':::mytype",
			wantDeps: []string{"public.mytype", "public.mytype.value"},
		},
		{
			name:     "cast to builtin type",
			expr:     "'42'::INT",
			wantDeps: []string{},
		},
		{
			name:     "array of custom type",
			expr:     "ARRAY[]::myschema.mytype[]",
			wantDeps: []string{"myschema.mytype"},
		},
		{
			name:     "binary expression with names",
			expr:     "a + b",
			wantDeps: []string{"public.a", "public.b"},
		},
		{
			name:     "function with arguments",
			expr:     "concat(first_name, last_name)",
			wantDeps: []string{"public.concat", "public.first_name", "public.last_name"},
		},
		{
			name:     "nested function calls",
			expr:     "my_outer(my_inner(name))",
			wantDeps: []string{"public.my_outer", "public.my_inner", "public.name"},
		},
		{
			name:     "case expression",
			expr:     "CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END",
			wantDeps: []string{"public.status"},
		},
		{
			name:     "coalesce expression",
			expr:     "COALESCE(nickname, name)",
			wantDeps: []string{"public.nickname", "public.name"},
		},
		{
			name:     "comparison expression",
			expr:     "age > 18",
			wantDeps: []string{"public.age"},
		},
		{
			name:     "and expression",
			expr:     "a AND b",
			wantDeps: []string{"public.a", "public.b"},
		},
		{
			name:     "or expression",
			expr:     "a OR b",
			wantDeps: []string{"public.a", "public.b"},
		},
		{
			name:     "not expression",
			expr:     "NOT active",
			wantDeps: []string{"public.active"},
		},
		{
			name:     "is null expression",
			expr:     "deleted_at IS NULL",
			wantDeps: []string{"public.deleted_at"},
		},
		{
			name:     "is not null expression",
			expr:     "created_at IS NOT NULL",
			wantDeps: []string{"public.created_at"},
		},
		{
			name:     "in expression",
			expr:     "status IN ('active', 'pending')",
			wantDeps: []string{"public.status"},
		},
		{
			name:     "between expression",
			expr:     "age BETWEEN 18 AND 65",
			wantDeps: []string{"public.age"},
		},
		{
			name:     "array expression",
			expr:     "ARRAY[a, b, c]",
			wantDeps: []string{"public.a", "public.b", "public.c"},
		},
		{
			name:     "multiple casts to custom types",
			expr:     "val::type1 + val::type2",
			wantDeps: []string{"public.val", "public.type1", "public.type2"},
		},
		{
			name:     "nextval sequence reference",
			expr:     "nextval('myseq')",
			wantDeps: []string{"public.myseq"},
		},
		{
			name:     "nextval with schema qualified sequence",
			expr:     "nextval('myschema.myseq')",
			wantDeps: []string{"myschema.myseq"},
		},
		{
			name:     "currval sequence reference",
			expr:     "currval('myseq')",
			wantDeps: []string{"public.myseq"},
		},
		{
			name:     "setval sequence reference",
			expr:     "setval('myseq', 100)",
			wantDeps: []string{"public.myseq"},
		},
		{
			name:     "nextval with regclass cast",
			expr:     "nextval('public.order_id_seq'::REGCLASS)",
			wantDeps: []string{"public.order_id_seq"},
		},
		{
			name:     "nextval with regclass cast unqualified",
			expr:     "nextval('myseq'::REGCLASS)",
			wantDeps: []string{"public.myseq"},
		},
		{
			name:     "complex expression",
			expr:     "COALESCE(custom_func(a::mytype), b::othertype, 0)",
			wantDeps: []string{"public.custom_func", "public.a", "public.mytype", "public.b", "public.othertype"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := parser.ParseExpr(tt.expr)
			if err != nil {
				t.Fatalf("failed to parse expression %q: %v", tt.expr, err)
			}

			got := getExprDeps(expr)
			want := set.New(tt.wantDeps...)

			// Check that got contains all expected deps
			for _, dep := range tt.wantDeps {
				if !got.Contains(dep) {
					t.Errorf("getExprDeps(%q) missing expected dependency %q", tt.expr, dep)
				}
			}

			// Check that got doesn't contain unexpected deps
			for dep := range got.Values() {
				if !want.Contains(dep) {
					t.Errorf("getExprDeps(%q) has unexpected dependency %q", tt.expr, dep)
				}
			}

			// Check size matches
			if got.Size() != want.Size() {
				gotSlice := slices.Collect(got.Values())
				t.Errorf("getExprDeps(%q) returned %d deps %v, want %d deps %v",
					tt.expr, got.Size(), gotSlice, want.Size(), tt.wantDeps)
			}
		})
	}
}

func TestGetExprColumnDeps(t *testing.T) {
	tests := []struct {
		name            string
		schemaTableName string
		tableName       string
		expr            string
		wantDeps        []string
	}{
		{
			name:            "column reference gets table prefix",
			schemaTableName: "public",
			tableName:       "users",
			expr:            "name",
			wantDeps:        []string{"public.name", "public.users.name"},
		},
		{
			name:            "multiple columns get table prefix",
			schemaTableName: "public",
			tableName:       "users",
			expr:            "first_name || ' ' || last_name",
			wantDeps:        []string{"public.first_name", "public.users.first_name", "public.last_name", "public.users.last_name"},
		},
		{
			name:            "function calls also get prefixed",
			schemaTableName: "public",
			tableName:       "users",
			expr:            "upper(name)",
			wantDeps:        []string{"public.upper", "public.users.upper", "public.name", "public.users.name"},
		},
		{
			name:            "schema qualified names not duplicated",
			schemaTableName: "public",
			tableName:       "users",
			expr:            "myschema.myfunc()",
			wantDeps:        []string{"myschema.myfunc"},
		},
		{
			name:            "custom schema table",
			schemaTableName: "myschema",
			tableName:       "accounts",
			expr:            "balance",
			wantDeps:        []string{"public.balance", "myschema.accounts.balance"},
		},
		{
			name:            "cast to type with column",
			schemaTableName: "public",
			tableName:       "users",
			expr:            "status::mytype",
			wantDeps:        []string{"public.status", "public.users.status", "public.mytype", "public.users.mytype"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := parser.ParseExpr(tt.expr)
			if err != nil {
				t.Fatalf("failed to parse expression %q: %v", tt.expr, err)
			}

			got := getExprColumnDeps(tt.schemaTableName, tt.tableName, expr)
			want := set.New(tt.wantDeps...)

			// Check that got contains all expected deps
			for _, dep := range tt.wantDeps {
				if !got.Contains(dep) {
					t.Errorf("getExprColumnDeps(%q, %q, %q) missing expected dependency %q",
						tt.schemaTableName, tt.tableName, tt.expr, dep)
				}
			}

			// Check that got doesn't contain unexpected deps
			for dep := range got.Values() {
				if !want.Contains(dep) {
					t.Errorf("getExprColumnDeps(%q, %q, %q) has unexpected dependency %q",
						tt.schemaTableName, tt.tableName, tt.expr, dep)
				}
			}

			// Check size matches
			if got.Size() != want.Size() {
				gotSlice := slices.Collect(got.Values())
				t.Errorf("getExprColumnDeps(%q, %q, %q) returned %d deps %v, want %d deps %v",
					tt.schemaTableName, tt.tableName, tt.expr, got.Size(), gotSlice, want.Size(), tt.wantDeps)
			}
		})
	}
}
