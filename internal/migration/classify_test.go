package migration

import (
	"testing"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/types"
	"github.com/stretchr/testify/assert"

	"github.com/pjtatlow/scurry/internal/schema"
)

func makeTableName(schemaName, tableName string) tree.TableName {
	tn := tree.MakeTableNameWithSchema("", tree.Name(schemaName), tree.Name(tableName))
	return tn
}

func largeTableSizes() *TableSizes {
	return &TableSizes{
		Threshold: 100000,
		Tables: map[string]TableInfo{
			"public.posts": {Rows: 15000000},
			"public.users": {Rows: 500000},
		},
	}
}

func smallTableSizes() *TableSizes {
	return &TableSizes{
		Threshold: 100000,
		Tables: map[string]TableInfo{
			"public.small_table": {Rows: 50},
		},
	}
}

func TestClassifyDifferences(t *testing.T) {
	t.Parallel()

	postsTable := makeTableName("public", "posts")
	smallTable := makeTableName("public", "small_table")

	tests := []struct {
		name       string
		diffs      []schema.Difference
		tableSizes *TableSizes
		wantMode   MigrationMode
		wantAsync  bool
	}{
		{
			name: "new table is sync",
			diffs: []schema.Difference{
				{Type: schema.DiffTypeTableAdded},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeSync,
		},
		{
			name: "drop table is sync",
			diffs: []schema.Difference{
				{Type: schema.DiffTypeTableRemoved},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeSync,
		},
		{
			name: "CREATE INDEX on small table is sync",
			diffs: []schema.Difference{
				{
					Type: schema.DiffTypeTableModified,
					MigrationStatements: []tree.Statement{
						&tree.CreateIndex{
							Name:  "idx_small",
							Table: smallTable,
						},
					},
				},
			},
			tableSizes: smallTableSizes(),
			wantMode:   ModeSync,
		},
		{
			name: "CREATE INDEX on large table is async",
			diffs: []schema.Difference{
				{
					Type: schema.DiffTypeTableModified,
					MigrationStatements: []tree.Statement{
						&tree.CreateIndex{
							Name:  "idx_posts",
							Table: postsTable,
						},
					},
				},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeAsync,
			wantAsync:  true,
		},
		{
			name: "ADD COLUMN NOT NULL DEFAULT on large table is async",
			diffs: []schema.Difference{
				{
					Type: schema.DiffTypeTableColumnModified,
					MigrationStatements: []tree.Statement{
						&tree.AlterTable{
							Table: postsTable.ToUnresolvedObjectName(),
							Cmds: tree.AlterTableCmds{
								&tree.AlterTableAddColumn{
									ColumnDef: &tree.ColumnTableDef{
										Name: "view_count",
										Type: types.Int,
										Nullable: struct {
											Nullability    tree.Nullability
											ConstraintName tree.Name
										}{
											Nullability: tree.NotNull,
										},
										DefaultExpr: struct {
											Expr           tree.Expr
											ConstraintName tree.Name
										}{
											Expr: tree.NewDInt(0),
										},
									},
								},
							},
						},
					},
				},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeAsync,
			wantAsync:  true,
		},
		{
			name: "ADD COLUMN nullable on large table is sync",
			diffs: []schema.Difference{
				{
					Type: schema.DiffTypeTableColumnModified,
					MigrationStatements: []tree.Statement{
						&tree.AlterTable{
							Table: postsTable.ToUnresolvedObjectName(),
							Cmds: tree.AlterTableCmds{
								&tree.AlterTableAddColumn{
									ColumnDef: &tree.ColumnTableDef{
										Name: "description",
										Type: types.String,
										Nullable: struct {
											Nullability    tree.Nullability
											ConstraintName tree.Name
										}{
											Nullability: tree.Null,
										},
									},
								},
							},
						},
					},
				},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeSync,
		},
		{
			name: "SET NOT NULL on large table is async",
			diffs: []schema.Difference{
				{
					Type: schema.DiffTypeTableColumnModified,
					MigrationStatements: []tree.Statement{
						&tree.AlterTable{
							Table: postsTable.ToUnresolvedObjectName(),
							Cmds: tree.AlterTableCmds{
								&tree.AlterTableSetNotNull{
									Column: "title",
								},
							},
						},
					},
				},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeAsync,
			wantAsync:  true,
		},
		{
			name: "ADD FK on large table is async",
			diffs: []schema.Difference{
				{
					Type: schema.DiffTypeTableModified,
					MigrationStatements: []tree.Statement{
						&tree.AlterTable{
							Table: postsTable.ToUnresolvedObjectName(),
							Cmds: tree.AlterTableCmds{
								&tree.AlterTableAddConstraint{
									ConstraintDef: &tree.ForeignKeyConstraintTableDef{
										Name:     "fk_user",
										Table:    makeTableName("public", "users"),
										FromCols: tree.NameList{"user_id"},
										ToCols:   tree.NameList{"id"},
									},
									ValidationBehavior: tree.ValidationDefault,
								},
							},
						},
					},
				},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeAsync,
			wantAsync:  true,
		},
		{
			name: "ADD CHECK on large table is async",
			diffs: []schema.Difference{
				{
					Type: schema.DiffTypeTableModified,
					MigrationStatements: []tree.Statement{
						&tree.AlterTable{
							Table: postsTable.ToUnresolvedObjectName(),
							Cmds: tree.AlterTableCmds{
								&tree.AlterTableAddConstraint{
									ConstraintDef: &tree.CheckConstraintTableDef{
										Name: "check_positive",
										Expr: tree.NewDInt(1),
									},
									ValidationBehavior: tree.ValidationDefault,
								},
							},
						},
					},
				},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeAsync,
			wantAsync:  true,
		},
		{
			name: "ADD CONSTRAINT with ValidationSkip on large table is sync",
			diffs: []schema.Difference{
				{
					Type: schema.DiffTypeTableModified,
					MigrationStatements: []tree.Statement{
						&tree.AlterTable{
							Table: postsTable.ToUnresolvedObjectName(),
							Cmds: tree.AlterTableCmds{
								&tree.AlterTableAddConstraint{
									ConstraintDef: &tree.ForeignKeyConstraintTableDef{
										Name: "fk_user",
									},
									ValidationBehavior: tree.ValidationSkip,
								},
							},
						},
					},
				},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeSync,
		},
		{
			name: "ALTER COLUMN TYPE on large table is async",
			diffs: []schema.Difference{
				{
					Type: schema.DiffTypeColumnTypeChanged,
					MigrationStatements: []tree.Statement{
						&tree.AlterTable{
							Table: postsTable.ToUnresolvedObjectName(),
							Cmds: tree.AlterTableCmds{
								&tree.AlterTableAlterColumnType{
									Column: "status",
									ToType: types.String,
								},
							},
						},
					},
				},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeAsync,
			wantAsync:  true,
		},
		{
			name: "mixed sync and async diffs results in async",
			diffs: []schema.Difference{
				{Type: schema.DiffTypeTableAdded}, // sync
				{
					Type: schema.DiffTypeTableModified,
					MigrationStatements: []tree.Statement{
						&tree.CreateIndex{
							Name:  "idx_posts",
							Table: postsTable,
						},
					},
				}, // async
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeAsync,
			wantAsync:  true,
		},
		{
			name: "no table_sizes means everything is sync",
			diffs: []schema.Difference{
				{
					Type: schema.DiffTypeTableModified,
					MigrationStatements: []tree.Statement{
						&tree.CreateIndex{
							Name:  "idx_posts",
							Table: postsTable,
						},
					},
				},
			},
			tableSizes: nil,
			wantMode:   ModeSync,
		},
		{
			name: "table not in table_sizes is sync",
			diffs: []schema.Difference{
				{
					Type: schema.DiffTypeTableModified,
					MigrationStatements: []tree.Statement{
						&tree.CreateIndex{
							Name:  "idx_unknown",
							Table: makeTableName("public", "unknown_table"),
						},
					},
				},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeSync,
		},
		{
			name: "schema operations are sync",
			diffs: []schema.Difference{
				{Type: schema.DiffSchemaAdded},
				{Type: schema.DiffSchemaRemoved},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeSync,
		},
		{
			name: "routine operations are sync",
			diffs: []schema.Difference{
				{Type: schema.DiffTypeRoutineAdded},
				{Type: schema.DiffTypeRoutineModified},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeSync,
		},
		{
			name: "view operations are sync",
			diffs: []schema.Difference{
				{Type: schema.DiffTypeViewAdded},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeSync,
		},
		{
			name: "sequence operations are sync",
			diffs: []schema.Difference{
				{Type: schema.DiffTypeSequenceAdded},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeSync,
		},
		{
			name:       "empty diffs is sync",
			diffs:      []schema.Difference{},
			tableSizes: largeTableSizes(),
			wantMode:   ModeSync,
		},
		{
			name: "ADD UNIQUE constraint on large table is sync",
			diffs: []schema.Difference{
				{
					Type: schema.DiffTypeTableModified,
					MigrationStatements: []tree.Statement{
						&tree.AlterTable{
							Table: postsTable.ToUnresolvedObjectName(),
							Cmds: tree.AlterTableCmds{
								&tree.AlterTableAddConstraint{
									ConstraintDef:      &tree.UniqueConstraintTableDef{},
									ValidationBehavior: tree.ValidationDefault,
								},
							},
						},
					},
				},
			},
			tableSizes: largeTableSizes(),
			wantMode:   ModeSync,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ClassifyDifferences(tt.diffs, tt.tableSizes)
			assert.Equal(t, tt.wantMode, result.Mode)
			if tt.wantAsync {
				assert.NotEmpty(t, result.Reasons)
			} else {
				assert.Empty(t, result.Reasons)
			}
		})
	}
}
