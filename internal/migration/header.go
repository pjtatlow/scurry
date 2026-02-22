package migration

import (
	"fmt"
	"strings"
)

// MigrationMode represents whether a migration is sync or async
type MigrationMode string

const (
	ModeSync  MigrationMode = "sync"
	ModeAsync MigrationMode = "async"
)

const headerPrefix = "-- scurry:"

// Header holds parsed migration header metadata
type Header struct {
	Mode      MigrationMode
	DependsOn []string
}

// ParseHeader parses the first line of a migration SQL string for a scurry header.
// Returns nil if no header is present.
func ParseHeader(sql string) (*Header, error) {
	line, _, _ := strings.Cut(sql, "\n")
	line = strings.TrimSpace(line)

	if !strings.HasPrefix(line, headerPrefix) {
		return nil, nil
	}

	rest := strings.TrimPrefix(line, headerPrefix)
	h := &Header{}

	for _, part := range strings.Split(rest, ",") {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("invalid header field: %q", part)
		}
		switch key {
		case "mode":
			switch MigrationMode(value) {
			case ModeSync, ModeAsync:
				h.Mode = MigrationMode(value)
			default:
				return nil, fmt.Errorf("invalid mode: %q", value)
			}
		case "depends_on":
			h.DependsOn = strings.Split(value, ";")
		default:
			return nil, fmt.Errorf("unknown header field: %q", key)
		}
	}

	if h.Mode == "" {
		return nil, fmt.Errorf("header missing required field: mode")
	}

	return h, nil
}

// FormatHeader serializes a Header to a SQL comment line.
func FormatHeader(h *Header) string {
	var sb strings.Builder
	sb.WriteString(headerPrefix)
	sb.WriteString("mode=")
	sb.WriteString(string(h.Mode))

	if len(h.DependsOn) > 0 {
		sb.WriteString(",depends_on=")
		sb.WriteString(strings.Join(h.DependsOn, ";"))
	}

	return sb.String()
}

// StripHeader removes the scurry header line from the top of SQL content.
// If no header is present, the original string is returned unchanged.
func StripHeader(sql string) string {
	line, rest, found := strings.Cut(sql, "\n")
	if !strings.HasPrefix(strings.TrimSpace(line), headerPrefix) {
		return sql
	}
	if !found {
		return ""
	}
	return rest
}

// PrependHeader strips any existing header and prepends a new one.
func PrependHeader(sql string, h *Header) string {
	stripped := StripHeader(sql)
	return FormatHeader(h) + "\n" + stripped
}
