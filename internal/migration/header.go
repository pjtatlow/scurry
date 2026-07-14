package migration

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"

	"github.com/pjtatlow/scurry/internal/schema"
	"github.com/pjtatlow/scurry/internal/set"
)

// MigrationMode represents whether a migration is sync or async
type MigrationMode string

const (
	ModeSync  MigrationMode = "sync"
	ModeAsync MigrationMode = "async"
)

const headerPrefix = "-- scurry:"

// sigLength is the number of hex characters kept from the header signature. This is a
// tamper/authorship tripwire, not a cryptographic guarantee, so a short prefix is fine.
const sigLength = 12

// Header holds parsed migration header metadata
type Header struct {
	Mode      MigrationMode
	DependsOn []string
	Squash    bool
	// Sig is a short signature scurry writes over the header fields and body. It lets
	// scurry detect a hand-authored or edited header (which won't carry a valid sig),
	// forcing header generation through scurry (which classifies correctly). It is NOT
	// part of the signed content.
	Sig string
}

// ComputeSig returns the canonical signature for a header's semantic fields bound to its
// migration body. It is deterministic: dependencies are sorted, the body is parsed and
// formatted from its AST, and the sig itself is excluded. Both generation and
// verification call this, so a matching sig means the header is consistent with the
// body under scurry's algorithm.
func ComputeSig(h *Header, body string) (string, error) {
	canonicalBody, err := canonicalizeBody(body)
	if err != nil {
		return "", err
	}

	deps := append([]string(nil), h.DependsOn...)
	sort.Strings(deps)

	var sb strings.Builder
	sb.WriteString("mode=")
	sb.WriteString(string(h.Mode))
	sb.WriteString(";depends_on=")
	sb.WriteString(strings.Join(deps, ";"))
	if h.Squash {
		sb.WriteString(";squash=true")
	}
	sb.WriteString(";body=")
	sb.WriteString(canonicalBody)

	sum := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(sum[:])[:sigLength], nil
}

// SignHeader sets h.Sig to the canonical signature over the header and the given body.
// Call this immediately before formatting/writing a migration file.
func SignHeader(h *Header, body string) error {
	sig, err := ComputeSig(h, body)
	if err != nil {
		return err
	}
	h.Sig = sig
	return nil
}

// canonicalizeBody parses a migration body and serializes each statement from its AST.
// This makes signatures independent of source formatting while preserving statement
// order and semantic details such as literals and quoted identifiers.
func canonicalizeBody(body string) (string, error) {
	statements, err := parser.Parse(body)
	if err != nil {
		return "", fmt.Errorf("failed to parse migration body: %w", err)
	}

	var sb strings.Builder
	for _, stmt := range statements {
		sb.WriteString(stmt.AST.String())
		sb.WriteString(";\n")
	}
	return sb.String(), nil
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
			if value == "" {
				return nil, fmt.Errorf("depends_on must not be empty")
			}
			h.DependsOn = strings.Split(value, ";")
		case "squash":
			if value != "true" {
				return nil, fmt.Errorf("squash must be \"true\"")
			}
			h.Squash = true
		case "sig":
			if value == "" {
				return nil, fmt.Errorf("sig must not be empty")
			}
			h.Sig = value
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

	if h.Squash {
		sb.WriteString(",squash=true")
	}

	if h.Sig != "" {
		sb.WriteString(",sig=")
		sb.WriteString(h.Sig)
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

// MigrationInfo holds the name and SQL content of an existing migration.
type MigrationInfo struct {
	Name string
	SQL  string
}

// FindDependencies detects which existing migrations share object-level overlaps
// with the new migration statements, returning only the most recent migration(s)
// that touch overlapping objects.
func FindDependencies(newStatements []tree.Statement, existingMigrations []MigrationInfo) []string {
	// Compute all names touched by the new migration (provides + dependencies),
	// excluding schema-level names (e.g. "schema:public") which are too generic.
	newNames := set.New[string]()
	for _, stmt := range newStatements {
		for name := range schema.GetProvidedNames(stmt, false).Values() {
			if !strings.HasPrefix(name, "schema:") {
				newNames.Add(name)
			}
		}
		for name := range schema.GetDependencyNames(stmt, false).Values() {
			if !strings.HasPrefix(name, "schema:") {
				newNames.Add(name)
			}
		}
	}

	if newNames.Size() == 0 {
		return nil
	}

	covered := set.New[string]()
	var deps []string

	// Iterate existing migrations in reverse chronological order (most recent first)
	for i := len(existingMigrations) - 1; i >= 0; i-- {
		mig := existingMigrations[i]

		// Strip header before parsing
		sql := StripHeader(mig.SQL)
		parsed, err := parser.Parse(sql)
		if err != nil {
			continue
		}

		migNames := set.New[string]()
		for _, stmt := range parsed {
			for name := range schema.GetProvidedNames(stmt.AST, false).Values() {
				if !strings.HasPrefix(name, "schema:") {
					migNames.Add(name)
				}
			}
			for name := range schema.GetDependencyNames(stmt.AST, false).Values() {
				if !strings.HasPrefix(name, "schema:") {
					migNames.Add(name)
				}
			}
		}

		overlap := newNames.Intersection(migNames)
		// Only count names we haven't already covered
		uncoveredOverlap := overlap.Difference(covered)
		if uncoveredOverlap.Size() > 0 {
			deps = append(deps, mig.Name)
			for name := range uncoveredOverlap.Values() {
				covered.Add(name)
			}
		}

		// Once all names are covered, stop
		if covered.Size() >= newNames.Size() {
			break
		}
	}

	return deps
}
