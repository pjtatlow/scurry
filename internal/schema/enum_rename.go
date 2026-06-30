package schema

import (
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/types"
)

// enumRename is a detected enum rename. scurry has no rename concept, so without
// this a renamed enum becomes drop+create + a per-column cast — and CRDB rejects a
// direct enum->enum cast. A detected rename emits one `ALTER TYPE from RENAME TO to`.
type enumRename struct {
	from ObjectSchema[*tree.CreateType] // present only in remote
	to   ObjectSchema[*tree.CreateType] // present only in local
}

func (r enumRename) oldName() string { return r.from.ResolvedName() }
func (r enumRename) newName() string { return r.to.ResolvedName() }

// detectEnumRenames pairs a dropped and an added enum as a rename only when the
// evidence is unambiguous — a false positive renames the wrong type and corrupts
// data. Required: identical, identically-ordered labels; that label set maps to
// exactly one dropped and one added enum in the same schema; and at least one
// column actually switched from the old type to the new one. Anything else falls
// back to the safe drop+create path.
func detectEnumRenames(local, remote *Schema) []enumRename {
	localEnums := enumTypesByName(local)
	remoteEnums := enumTypesByName(remote)

	type bucket struct {
		olds []ObjectSchema[*tree.CreateType]
		news []ObjectSchema[*tree.CreateType]
	}
	buckets := make(map[string]*bucket)
	bucketFor := func(sig string) *bucket {
		if buckets[sig] == nil {
			buckets[sig] = &bucket{}
		}
		return buckets[sig]
	}

	for name, e := range remoteEnums {
		if _, inLocal := localEnums[name]; !inLocal {
			b := bucketFor(enumLabelSignature(e.Ast))
			b.olds = append(b.olds, e)
		}
	}
	for name, e := range localEnums {
		if _, inRemote := remoteEnums[name]; !inRemote {
			b := bucketFor(enumLabelSignature(e.Ast))
			b.news = append(b.news, e)
		}
	}

	switched := columnsSwitchedBetween(local, remote)

	var renames []enumRename
	for _, b := range buckets {
		if len(b.olds) != 1 || len(b.news) != 1 {
			continue
		}
		from, to := b.olds[0], b.news[0]
		if from.Schema != to.Schema {
			continue
		}
		if !switched[switchKey{old: from.ResolvedName(), new: to.ResolvedName()}] {
			continue
		}
		renames = append(renames, enumRename{from: from, to: to})
	}

	sort.Slice(renames, func(i, j int) bool { return renames[i].oldName() < renames[j].oldName() })
	return renames
}

func enumTypesByName(s *Schema) map[string]ObjectSchema[*tree.CreateType] {
	out := make(map[string]ObjectSchema[*tree.CreateType])
	for _, t := range s.Types {
		if t.Ast.Variety == tree.Enum {
			out[t.ResolvedName()] = t
		}
	}
	return out
}

// enumLabelSignature is an order-sensitive signature of an enum's labels; labels
// are quoted so adjacent boundaries are unambiguous.
func enumLabelSignature(ct *tree.CreateType) string {
	labels := getEnumValues(ct)
	parts := make([]string, len(labels))
	for i, l := range labels {
		parts[i] = strconv.Quote(l)
	}
	return strings.Join(parts, ",")
}

type switchKey struct {
	old string
	new string
}

// columnsSwitchedBetween returns the (oldType -> newType) UDT-name pairs for which
// some column shared by both schema versions changed its declared type.
func columnsSwitchedBetween(local, remote *Schema) map[switchKey]bool {
	out := make(map[switchKey]bool)
	remoteTables := make(map[string]ObjectSchema[*tree.CreateTable])
	for _, t := range remote.Tables {
		remoteTables[t.ResolvedName()] = t
	}
	for _, lt := range local.Tables {
		rt, ok := remoteTables[lt.ResolvedName()]
		if !ok {
			continue
		}
		remoteCols := columnUDTNames(rt.Ast)
		for col, localType := range columnUDTNames(lt.Ast) {
			if remoteType, ok := remoteCols[col]; ok && remoteType != localType {
				out[switchKey{old: remoteType, new: localType}] = true
			}
		}
	}
	return out
}

func columnUDTNames(ct *tree.CreateTable) map[string]string {
	out := make(map[string]string)
	for _, def := range ct.Defs {
		if cd, ok := def.(*tree.ColumnTableDef); ok {
			if name, ok := getResolvableTypeReferenceDepName(cd.Type); ok {
				out[cd.Name.Normalize()] = name
			}
		}
	}
	return out
}

// enumChangeContext carries cross-object enum info into the per-column diff: which
// target types are enums (so a rewrite can bridge through text) and which column
// type differences are fully explained by a detected rename (so they need no DDL).
type enumChangeContext struct {
	localEnumNames map[string]bool
	renameByOld    map[string]string
}

func newEnumChangeContext(local, remote *Schema) *enumChangeContext {
	ctx := &enumChangeContext{
		localEnumNames: make(map[string]bool),
		renameByOld:    make(map[string]string),
	}
	for _, t := range local.Types {
		if t.Ast.Variety == tree.Enum {
			ctx.localEnumNames[t.ResolvedName()] = true
		}
	}
	for _, r := range detectEnumRenames(local, remote) {
		ctx.renameByOld[r.oldName()] = r.newName()
	}
	return ctx
}

// isEnumTarget reports whether a column's target type is an enum. SQL-parsed enum
// columns are unresolved object names, not resolved *types.T, so we consult the
// locally-defined enum names (and handle a resolved *types.T for completeness).
func (c *enumChangeContext) isEnumTarget(t tree.ResolvableTypeReference) bool {
	if tt, ok := t.(*types.T); ok {
		return tt.Family() == types.EnumFamily
	}
	if name, ok := getResolvableTypeReferenceDepName(t); ok {
		return c.localEnumNames[name]
	}
	return false
}

func (c *enumChangeContext) explainedByRename(remoteType, localType tree.ResolvableTypeReference) bool {
	if len(c.renameByOld) == 0 {
		return false
	}
	oldN, ok1 := getResolvableTypeReferenceDepName(remoteType)
	newN, ok2 := getResolvableTypeReferenceDepName(localType)
	if !ok1 || !ok2 {
		return false
	}
	mapped, ok := c.renameByOld[oldN]
	return ok && mapped == newN
}
