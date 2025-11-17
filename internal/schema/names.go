package schema

import (
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

func getTableName(name tree.TableName) (string, string) {
	schemaName := "public"
	if name.ExplicitSchema {
		schemaName = name.SchemaName.Normalize()
	}
	tableName := name.ObjectName.Normalize()
	return schemaName, tableName
}

func getRoutineName(name tree.RoutineName) (string, string) {
	schemaName := "public"
	if name.ExplicitSchema {
		schemaName = name.SchemaName.Normalize()
	}
	routineName := name.ObjectName.Normalize()
	return schemaName, routineName
}

func getObjectName(name *tree.UnresolvedObjectName) (string, string) {
	schemaName := "public"
	if name.HasExplicitSchema() {
		schemaName = strings.ToLower(name.Schema())
	}
	objectName := strings.ToLower(name.Object())

	return schemaName, objectName
}
