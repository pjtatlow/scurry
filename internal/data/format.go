package data

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

const (
	headerMarker      = "-- scurry:data-dump"
	versionPrefix     = "-- version: "
	createdAtPrefix   = "-- created_at: "
	tablesPrefix      = "-- tables: "
	schemaBeginMarker = "-- BEGIN SCHEMA"
	schemaEndMarker   = "-- END SCHEMA"
	tablePrefix       = "-- Table: "
	sequencePrefix    = "-- Sequence: "
)

// DumpFile represents the complete dump file structure.
type DumpFile struct {
	Version   int
	CreatedAt time.Time
	Tables    []string        // table names in insertion order
	SchemaSQL string          // all CREATE statements
	TableData []TableDump     // per-table INSERT/UPDATE statements
	Sequences []SequenceValue // sequence current values
}

// TableDump holds the SQL statements for a single table's data.
type TableDump struct {
	QualifiedName string
	RowCount      int
	Statements    []string // INSERT and UPDATE SQL statements
}

// SequenceValue holds the current value for a sequence.
type SequenceValue struct {
	QualifiedName string
	Value         int64
}

// Write serializes the DumpFile to the given writer.
func (d *DumpFile) Write(w io.Writer) error {
	bw := bufio.NewWriter(w)

	// Header
	fmt.Fprintln(bw, headerMarker)
	fmt.Fprintf(bw, "%s%d\n", versionPrefix, d.Version)
	fmt.Fprintf(bw, "%s%s\n", createdAtPrefix, d.CreatedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(bw, "%s%s\n", tablesPrefix, strings.Join(d.Tables, ","))

	// Schema section
	fmt.Fprintln(bw)
	fmt.Fprintln(bw, schemaBeginMarker)
	if d.SchemaSQL != "" {
		fmt.Fprintln(bw, d.SchemaSQL)
	}
	fmt.Fprintln(bw, schemaEndMarker)

	// Table data sections
	for _, td := range d.TableData {
		fmt.Fprintln(bw)
		fmt.Fprintf(bw, "%s%s (%d rows)\n", tablePrefix, td.QualifiedName, td.RowCount)
		for _, stmt := range td.Statements {
			fmt.Fprintln(bw, stmt)
			fmt.Fprintln(bw)
		}
	}

	// Sequence sections
	for _, seq := range d.Sequences {
		fmt.Fprintln(bw)
		fmt.Fprintf(bw, "%s%s\n", sequencePrefix, seq.QualifiedName)
		fmt.Fprintf(bw, "SELECT setval('%s', %d);\n", seq.QualifiedName, seq.Value)
	}

	return bw.Flush()
}

// ParseDumpFile parses a dump file from the given reader.
func ParseDumpFile(r io.Reader) (*DumpFile, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	// Read all lines into a slice so we can index freely
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading dump file: %w", err)
	}

	if len(lines) == 0 {
		return nil, fmt.Errorf("empty dump file")
	}

	df := &DumpFile{}
	i := 0

	// Parse header
	if strings.TrimSpace(lines[i]) != headerMarker {
		return nil, fmt.Errorf("invalid dump file: missing header marker")
	}
	i++

	// Parse version
	if i >= len(lines) {
		return nil, fmt.Errorf("unexpected end of file: expected version")
	}
	if !strings.HasPrefix(lines[i], versionPrefix) {
		return nil, fmt.Errorf("invalid dump file: expected version line, got: %s", lines[i])
	}
	version, err := strconv.Atoi(strings.TrimPrefix(lines[i], versionPrefix))
	if err != nil {
		return nil, fmt.Errorf("invalid version: %w", err)
	}
	df.Version = version
	i++

	// Parse created_at
	if i >= len(lines) {
		return nil, fmt.Errorf("unexpected end of file: expected created_at")
	}
	if !strings.HasPrefix(lines[i], createdAtPrefix) {
		return nil, fmt.Errorf("invalid dump file: expected created_at line, got: %s", lines[i])
	}
	createdAt, err := time.Parse(time.RFC3339, strings.TrimPrefix(lines[i], createdAtPrefix))
	if err != nil {
		return nil, fmt.Errorf("invalid created_at: %w", err)
	}
	df.CreatedAt = createdAt
	i++

	// Parse tables list
	if i >= len(lines) {
		return nil, fmt.Errorf("unexpected end of file: expected tables")
	}
	if !strings.HasPrefix(lines[i], tablesPrefix) {
		return nil, fmt.Errorf("invalid dump file: expected tables line, got: %s", lines[i])
	}
	tablesStr := strings.TrimPrefix(lines[i], tablesPrefix)
	if tablesStr != "" {
		df.Tables = strings.Split(tablesStr, ",")
	}
	i++

	// Find schema begin
	foundSchema := false
	for i < len(lines) {
		if strings.TrimSpace(lines[i]) == schemaBeginMarker {
			i++
			foundSchema = true
			break
		}
		i++
	}
	if !foundSchema {
		return nil, fmt.Errorf("invalid dump file: missing schema section")
	}

	// Read schema until end marker
	var schemaLines []string
	for i < len(lines) {
		if strings.TrimSpace(lines[i]) == schemaEndMarker {
			i++
			break
		}
		schemaLines = append(schemaLines, lines[i])
		i++
	}
	df.SchemaSQL = strings.TrimSpace(strings.Join(schemaLines, "\n"))

	// Parse remaining sections (table data and sequences)
	for i < len(lines) {
		line := lines[i]

		if strings.HasPrefix(line, tablePrefix) {
			td, nextI, err := parseTableLines(lines, i)
			if err != nil {
				return nil, err
			}
			df.TableData = append(df.TableData, td)
			i = nextI
		} else if strings.HasPrefix(line, sequencePrefix) {
			seq, nextI, err := parseSequenceLines(lines, i)
			if err != nil {
				return nil, err
			}
			df.Sequences = append(df.Sequences, seq)
			i = nextI
		} else {
			i++
		}
	}

	return df, nil
}

// parseTableLines parses a table section starting at lines[start].
// Returns the parsed TableDump and the index of the next unprocessed line.
func parseTableLines(lines []string, start int) (TableDump, int, error) {
	headerLine := lines[start]
	rest := strings.TrimPrefix(headerLine, tablePrefix)

	parenIdx := strings.LastIndex(rest, " (")
	if parenIdx == -1 {
		return TableDump{}, start + 1, fmt.Errorf("invalid table header: %s", headerLine)
	}

	qualifiedName := rest[:parenIdx]
	rowCountStr := rest[parenIdx+2 : len(rest)-len(" rows)")]
	rowCount, err := strconv.Atoi(rowCountStr)
	if err != nil {
		return TableDump{}, start + 1, fmt.Errorf("invalid row count in table header: %s", headerLine)
	}

	td := TableDump{
		QualifiedName: qualifiedName,
		RowCount:      rowCount,
	}

	i := start + 1
	var currentStmt strings.Builder

	for i < len(lines) {
		line := lines[i]

		// Stop if we hit another section marker
		if strings.HasPrefix(line, tablePrefix) || strings.HasPrefix(line, sequencePrefix) {
			break
		}

		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			if currentStmt.Len() > 0 {
				td.Statements = append(td.Statements, strings.TrimSpace(currentStmt.String()))
				currentStmt.Reset()
			}
			i++
			continue
		}

		if currentStmt.Len() > 0 {
			currentStmt.WriteByte('\n')
		}
		currentStmt.WriteString(line)
		i++
	}

	if currentStmt.Len() > 0 {
		td.Statements = append(td.Statements, strings.TrimSpace(currentStmt.String()))
	}

	return td, i, nil
}

// parseSequenceLines parses a sequence section starting at lines[start].
// Returns the parsed SequenceValue and the index of the next unprocessed line.
func parseSequenceLines(lines []string, start int) (SequenceValue, int, error) {
	qualifiedName := strings.TrimPrefix(lines[start], sequencePrefix)
	sv := SequenceValue{QualifiedName: qualifiedName}

	i := start + 1
	for i < len(lines) {
		line := strings.TrimSpace(lines[i])
		i++

		if line == "" {
			continue
		}

		if !strings.HasPrefix(line, "SELECT setval(") {
			return sv, i, fmt.Errorf("expected SELECT setval statement, got: %s", line)
		}

		commaIdx := strings.LastIndex(line, ", ")
		closeIdx := strings.LastIndex(line, ")")
		if commaIdx == -1 || closeIdx == -1 || closeIdx <= commaIdx {
			return sv, i, fmt.Errorf("invalid setval statement: %s", line)
		}
		valStr := line[commaIdx+2 : closeIdx]
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return sv, i, fmt.Errorf("invalid sequence value: %w", err)
		}
		sv.Value = val
		break
	}

	return sv, i, nil
}
