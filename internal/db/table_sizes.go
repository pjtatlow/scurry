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
	SizeBytes  int64
}

// GetTableSizes queries the database for table sizes.
// Uses crdb_internal.table_row_statistics for row estimates and
// range stats for approximate disk size.
func (c *Client) GetTableSizes(ctx context.Context) ([]TableSizeInfo, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT
			t.schema_name,
			t.name AS table_name,
			COALESCE(s.estimated_row_count, 0)::INT8 AS estimated_rows,
			COALESCE(
				(SELECT sum((crdb_internal.range_stats(start_key)->'key_bytes')::INT8 +
				            (crdb_internal.range_stats(start_key)->'val_bytes')::INT8)
				 FROM crdb_internal.ranges_no_leases r
				 WHERE r.table_id = t.table_id),
				0
			)::INT8 AS size_bytes
		FROM crdb_internal.tables t
		LEFT JOIN crdb_internal.table_row_statistics s ON s.table_id = t.table_id
		WHERE t.database_name = current_database()
		  AND t.schema_name NOT IN ('pg_catalog', 'information_schema', 'crdb_internal', '_scurry_')
		  AND t.drop_time IS NULL
		ORDER BY estimated_rows DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query table sizes: %w", err)
	}
	defer rows.Close()

	var tables []TableSizeInfo
	for rows.Next() {
		var t TableSizeInfo
		if err := rows.Scan(&t.SchemaName, &t.TableName, &t.Rows, &t.SizeBytes); err != nil {
			return nil, fmt.Errorf("failed to scan table size: %w", err)
		}
		tables = append(tables, t)
	}

	return tables, rows.Err()
}
