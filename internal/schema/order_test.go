package schema

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTable(schemaName, tableName, sql string) ObjectSchema[*tree.CreateTable] {
	stmts, err := parseSQL(sql)
	if err != nil {
		panic(err)
	}
	ct := stmts[0].(*tree.CreateTable)
	ct.HoistConstraints()
	return ObjectSchema[*tree.CreateTable]{
		Name:   tableName,
		Schema: schemaName,
		Ast:    ct,
	}
}

func TestComputeTableInsertionOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		tables          []ObjectSchema[*tree.CreateTable]
		expectedOrder   []string
		expectedSelfRef map[string][]string
		expectError     bool
	}{
		{
			name: "single table",
			tables: []ObjectSchema[*tree.CreateTable]{
				makeTable("public", "users", `CREATE TABLE public.users (id INT8 PRIMARY KEY, name STRING NOT NULL)`),
			},
			expectedOrder:   []string{"public.users"},
			expectedSelfRef: map[string][]string{},
		},
		{
			name: "independent tables",
			tables: []ObjectSchema[*tree.CreateTable]{
				makeTable("public", "users", `CREATE TABLE public.users (id INT8 PRIMARY KEY)`),
				makeTable("public", "products", `CREATE TABLE public.products (id INT8 PRIMARY KEY)`),
			},
			expectedOrder:   []string{"public.products", "public.users"},
			expectedSelfRef: map[string][]string{},
		},
		{
			name: "linear FK chain",
			tables: []ObjectSchema[*tree.CreateTable]{
				makeTable("public", "comments", `CREATE TABLE public.comments (id INT8 PRIMARY KEY, post_id INT8 REFERENCES public.posts(id))`),
				makeTable("public", "posts", `CREATE TABLE public.posts (id INT8 PRIMARY KEY, user_id INT8 REFERENCES public.users(id))`),
				makeTable("public", "users", `CREATE TABLE public.users (id INT8 PRIMARY KEY)`),
			},
			expectedOrder:   []string{"public.users", "public.posts", "public.comments"},
			expectedSelfRef: map[string][]string{},
		},
		{
			name: "diamond dependency",
			tables: []ObjectSchema[*tree.CreateTable]{
				makeTable("public", "a", `CREATE TABLE public.a (id INT8 PRIMARY KEY)`),
				makeTable("public", "b", `CREATE TABLE public.b (id INT8 PRIMARY KEY, a_id INT8 REFERENCES public.a(id))`),
				makeTable("public", "c", `CREATE TABLE public.c (id INT8 PRIMARY KEY, a_id INT8 REFERENCES public.a(id))`),
				makeTable("public", "d", `CREATE TABLE public.d (id INT8 PRIMARY KEY, b_id INT8 REFERENCES public.b(id), c_id INT8 REFERENCES public.c(id))`),
			},
			expectedOrder:   []string{"public.a", "public.b", "public.c", "public.d"},
			expectedSelfRef: map[string][]string{},
		},
		{
			name: "self-referential FK",
			tables: []ObjectSchema[*tree.CreateTable]{
				makeTable("public", "categories", `CREATE TABLE public.categories (
					id INT8 PRIMARY KEY,
					name STRING NOT NULL,
					parent_id INT8 REFERENCES public.categories(id)
				)`),
			},
			expectedOrder: []string{"public.categories"},
			expectedSelfRef: map[string][]string{
				"public.categories": {"parent_id"},
			},
		},
		{
			name: "many-to-one",
			tables: []ObjectSchema[*tree.CreateTable]{
				makeTable("public", "parent", `CREATE TABLE public.parent (id INT8 PRIMARY KEY)`),
				makeTable("public", "child1", `CREATE TABLE public.child1 (id INT8 PRIMARY KEY, parent_id INT8 REFERENCES public.parent(id))`),
				makeTable("public", "child2", `CREATE TABLE public.child2 (id INT8 PRIMARY KEY, parent_id INT8 REFERENCES public.parent(id))`),
				makeTable("public", "child3", `CREATE TABLE public.child3 (id INT8 PRIMARY KEY, parent_id INT8 REFERENCES public.parent(id))`),
			},
			expectedOrder:   []string{"public.parent", "public.child1", "public.child2", "public.child3"},
			expectedSelfRef: map[string][]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := ComputeTableInsertionOrder(tt.tables)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedOrder, result.Order)

			// Compare self-ref columns
			if len(tt.expectedSelfRef) == 0 {
				assert.Empty(t, result.SelfRefColumns)
			} else {
				assert.Equal(t, tt.expectedSelfRef, result.SelfRefColumns)
			}
		})
	}
}
