package db

import (
	"context"
	"fmt"
)

// TableSizeInfo holds size information for a single table from the database
type TableSizeInfo struct {
	SchemaName string
	TableName  string
	Rows       int64
}

// GetTableSizes queries the database for table sizes.
// Uses crdb_internal.table_row_statistics for row estimates and
// range stats for approximate disk size.
func (c *Client) GetTableSizes(ctx context.Context) ([]TableSizeInfo, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT schema_name, table_name, estimated_row_count
		FROM [SHOW TABLES]
		WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', '_scurry_')
		ORDER BY estimated_row_count DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query table sizes: %w", err)
	}
	defer rows.Close()

	var tables []TableSizeInfo
	for rows.Next() {
		var t TableSizeInfo
		if err := rows.Scan(&t.SchemaName, &t.TableName, &t.Rows); err != nil {
			return nil, fmt.Errorf("failed to scan table size: %w", err)
		}
		tables = append(tables, t)
	}

	return tables, rows.Err()
}
