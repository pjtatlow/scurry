package migration

import (
	"path/filepath"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

const (
	tableSizesFileName        = "table_sizes.yaml"
	DefaultLargeTableThreshold int64 = 100000
)

// TableInfo holds size information for a single table
type TableInfo struct {
	Rows      int64 `yaml:"rows"`
	SizeBytes int64 `yaml:"size_bytes"`
}

// TableSizes holds table size data loaded from table_sizes.yaml
type TableSizes struct {
	Threshold int64                `yaml:"threshold"`
	Tables    map[string]TableInfo `yaml:"tables"`
}

// LoadTableSizes reads table_sizes.yaml from the migrations directory.
// Returns nil if the file does not exist.
func LoadTableSizes(fs afero.Fs, migrationsDir string) (*TableSizes, error) {
	path := filepath.Join(migrationsDir, tableSizesFileName)

	exists, err := afero.Exists(fs, path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}

	data, err := afero.ReadFile(fs, path)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	var ts TableSizes
	if err := yaml.Unmarshal(data, &ts); err != nil {
		return nil, err
	}

	return &ts, nil
}

// SaveTableSizes writes table_sizes.yaml to the migrations directory.
func SaveTableSizes(fs afero.Fs, migrationsDir string, ts *TableSizes) error {
	data, err := yaml.Marshal(ts)
	if err != nil {
		return err
	}

	path := filepath.Join(migrationsDir, tableSizesFileName)
	return afero.WriteFile(fs, path, data, 0644)
}

// IsLargeTable returns true if the table's row count meets or exceeds the threshold.
// Returns false if ts is nil, the table is not found, or the table is below threshold.
func (ts *TableSizes) IsLargeTable(tableName string) bool {
	if ts == nil {
		return false
	}
	info, ok := ts.Tables[tableName]
	if !ok {
		return false
	}
	threshold := ts.Threshold
	if threshold <= 0 {
		threshold = DefaultLargeTableThreshold
	}
	return info.Rows >= threshold
}
