package schema

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/spf13/afero"

	"github.com/pjtatlow/scurry/internal/db"
)

type CreateObjectStatement interface {
	tree.Statement
	*tree.CreateTable | *tree.CreateType | *tree.CreateSequence | *tree.CreateView | *tree.CreateRoutine | *tree.CreateSchema
}

// Schema represents the complete database schema
type Schema struct {
	Routines           []ObjectSchema[*tree.CreateRoutine]
	Schemas            []ObjectSchema[*tree.CreateSchema]
	Sequences          []ObjectSchema[*tree.CreateSequence]
	Tables             []ObjectSchema[*tree.CreateTable]
	Types              []ObjectSchema[*tree.CreateType]
	Views              []ObjectSchema[*tree.CreateView]
	OriginalStatements []string // Original SQL statement strings in order
}

// TableSchema represents a table definition
type ObjectSchema[T CreateObjectStatement] struct {
	Name   string
	Schema string
	Ast    T
}

func (o *ObjectSchema[T]) ResolvedName() string {
	if o.Schema == "" {
		return o.Name
	}
	return fmt.Sprintf("%s.%s", o.Schema, o.Name)
}

func NewSchema(statements ...tree.Statement) *Schema {
	schema := &Schema{
		Tables:             make([]ObjectSchema[*tree.CreateTable], 0),
		Types:              make([]ObjectSchema[*tree.CreateType], 0),
		Schemas:            make([]ObjectSchema[*tree.CreateSchema], 0),
		Sequences:          make([]ObjectSchema[*tree.CreateSequence], 0),
		Views:              make([]ObjectSchema[*tree.CreateView], 0),
		Routines:           make([]ObjectSchema[*tree.CreateRoutine], 0),
		OriginalStatements: make([]string, 0, len(statements)),
	}
	for _, stmt := range statements {
		// Store the statement string
		schema.OriginalStatements = append(schema.OriginalStatements, stmt.String())

		switch stmt := stmt.(type) {
		case *tree.CreateSchema:
			obj := ObjectSchema[*tree.CreateSchema]{
				Name: stmt.Schema.Schema(),
				Ast:  stmt,
			}
			schema.Schemas = append(schema.Schemas, obj)

		case *tree.CreateTable:
			stmt.HoistConstraints()

			schemaName, tableName := getTableName(stmt.Table)
			obj := ObjectSchema[*tree.CreateTable]{
				Name:   tableName,
				Schema: schemaName,
				Ast:    stmt,
			}
			schema.Tables = append(schema.Tables, obj)

		case *tree.CreateType:
			schemaName, typeName := getObjectName(stmt.TypeName)
			obj := ObjectSchema[*tree.CreateType]{
				Name:   typeName,
				Schema: schemaName,
				Ast:    stmt,
			}
			schema.Types = append(schema.Types, obj)

		case *tree.CreateSequence:
			schemaName, sequenceName := getTableName(stmt.Name)
			obj := ObjectSchema[*tree.CreateSequence]{
				Name:   sequenceName,
				Schema: schemaName,
				Ast:    stmt,
			}
			schema.Sequences = append(schema.Sequences, obj)

		case *tree.CreateView:
			schemaName, viewName := getTableName(stmt.Name)
			obj := ObjectSchema[*tree.CreateView]{
				Name:   viewName,
				Schema: schemaName,
				Ast:    stmt,
			}
			schema.Views = append(schema.Views, obj)

		case *tree.CreateRoutine:
			schemaName, functionName := getRoutineName(stmt.Name)
			obj := ObjectSchema[*tree.CreateRoutine]{
				Name:   functionName,
				Schema: schemaName,
				Ast:    stmt,
			}
			schema.Routines = append(schema.Routines, obj)
		}
	}

	return schema
}

// LoadFromDirectory loads schema from SQL files in a directory
func LoadFromDirectory(ctx context.Context, fs afero.Fs, dirPath string, dbClient *db.Client) (*Schema, error) {

	// 1. Load raw schemas from fs
	allStatements := make([]tree.Statement, 0)
	err := afero.Walk(fs, dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(path), ".sql") {
			return nil
		}

		content, err := afero.ReadFile(fs, path)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", path, err)
		}

		sql := string(content)
		statements, err := parseSQL(sql)
		if err != nil {
			return fmt.Errorf("in file %s: %w", path, err)
		}

		allStatements = append(allStatements, statements...)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 2. Load schemas into a new database
	rawSchema := NewSchema(allStatements...)
	diff := Compare(rawSchema, NewSchema())
	statements, err := diff.GenerateMigrations(false)
	if err != nil {
		return nil, err
	}

	if err := dbClient.ExecuteBulkDDL(ctx, statements...); err != nil {
		return nil, err
	}

	// 3. Get standardized create statements from the database
	return LoadFromDatabase(ctx, dbClient)
}

// LoadFromDatabase loads schema from all non-system schemas in the database
func LoadFromDatabase(ctx context.Context, dbClient *db.Client) (*Schema, error) {
	statements, err := dbClient.GetAllCreateStatements(ctx)
	if err != nil {
		return nil, err
	}

	allStatements := make([]tree.Statement, 0, len(statements))
	for _, s := range statements {
		stmt, err := parseSQL(s)
		if err != nil {
			return nil, err
		}
		allStatements = append(allStatements, stmt...)
	}

	schema := NewSchema(allStatements...)

	return schema, nil
}

// ParseSQL parses SQL string into statements (exported for use in migrate command)
func ParseSQL(sql string) ([]tree.Statement, error) {
	return parseSQL(sql)
}

func parseSQL(sql string) ([]tree.Statement, error) {
	statements, err := parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	var results []tree.Statement
	for _, stmt := range statements {
		// Validate that only DDL statements are present
		if stmt.AST.StatementType() != tree.TypeDDL {
			return nil, fmt.Errorf("non-DDL statement found: %s (type: %s). Schema files should only contain DDL statements (CREATE TABLE, CREATE TYPE, etc.)",
				stmt.AST.StatementTag(), stmt.AST.StatementType())
		}

		// Determine object type and name
		switch stmt.AST.(type) {
		case *tree.CreateTable:
		case *tree.CreateType:
		case *tree.CreateRoutine:
		case *tree.CreateSequence:
		case *tree.CreateView:
		case *tree.CreateSchema:
		default:
			return nil, fmt.Errorf("unsupported DDL statement: %s.\nscurry currently supports:\n\tCREATE SCHEMA\n\tCREATE TABLE\n\tCREATE TYPE\n\tCREATE SEQUENCE\n\tCREATE (MATERIALIZED) VIEW\n\tCREATE FUNCTION\n\tCREATE PROCEDURE\nIndexes should be defined inline within CREATE TABLE statements",
				stmt.AST.StatementTag(),
			)
		}

		results = append(results, stmt.AST)
	}

	return results, nil
}
