package schema

import (
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/types"
)

// enumRename describes a safe, unambiguous enum rename detected between the
// remote (current DB) and local (desired) schemas. scurry has no first-class
// rename concept, so without this detection a renamed enum is modeled as
// drop-old-type + create-new-type + a per-column cast — and CockroachDB rejects
// a direct enum->enum cast. When the rename is unambiguous we instead emit a
// single `ALTER TYPE <old> RENAME TO <new>`, which CRDB supports and which
// repoints every referencing column for free.
type enumRename struct {
	old ObjectSchema[*tree.CreateType] // enum present only in remote
	new ObjectSchema[*tree.CreateType] // enum present only in local
}

func (r enumRename) oldName() string { return r.old.ResolvedName() }
func (r enumRename) newName() string { return r.new.ResolvedName() }

// detectEnumRenames finds enum renames between the remote (current) and local
// (desired) schemas.
//
// The detection is intentionally conservative: a false positive silently
// renames the wrong type and corrupts data, so we only treat a drop+add pair as
// a rename when the evidence is unambiguous. The rule is:
//
//  1. Candidate OLD enums are those present in remote but absent from local;
//     candidate NEW enums are those present in local but absent from remote.
//  2. A candidate pair must have an IDENTICAL, identically-ordered label set
//     (same enum kind, same labels in the same order). To avoid ambiguity when
//     several enums coincidentally share a label set, a given label set must map
//     to EXACTLY ONE old candidate and EXACTLY ONE new candidate; otherwise none
//     of them are treated as a rename and they fall back to drop+create.
//  3. As an extra guard against two coincidentally same-labeled but unrelated
//     enums, we additionally require concrete evidence that the rename happened:
//     at least one table column whose type switched from the old enum to the new
//     enum (remote column type == old, local column type == new). A genuine
//     rename always carries its referencing columns along; an unrelated
//     drop-then-add of a same-labeled enum does not. Enums with no referencing
//     column are left to the safe drop+create path (dropping/creating an unused
//     enum is harmless).
func detectEnumRenames(local, remote *Schema) []enumRename {
	localEnums := enumTypesByName(local)
	remoteEnums := enumTypesByName(remote)

	type bucket struct {
		olds []ObjectSchema[*tree.CreateType]
		news []ObjectSchema[*tree.CreateType]
	}
	buckets := make(map[string]*bucket)

	bucketFor := func(sig string) *bucket {
		b := buckets[sig]
		if b == nil {
			b = &bucket{}
			buckets[sig] = b
		}
		return b
	}

	for name, e := range remoteEnums {
		if _, inLocal := localEnums[name]; inLocal {
			continue
		}
		b := bucketFor(enumLabelSignature(e.Ast))
		b.olds = append(b.olds, e)
	}
	for name, e := range localEnums {
		if _, inRemote := remoteEnums[name]; inRemote {
			continue
		}
		b := bucketFor(enumLabelSignature(e.Ast))
		b.news = append(b.news, e)
	}

	switched := columnsSwitchedBetween(local, remote)

	var renames []enumRename
	for _, b := range buckets {
		// Require a one-to-one match for this label set.
		if len(b.olds) != 1 || len(b.news) != 1 {
			continue
		}
		old, knew := b.olds[0], b.news[0]
		// Require concrete evidence: a column actually switched old -> new.
		if !switched[switchKey{old: old.ResolvedName(), new: knew.ResolvedName()}] {
			continue
		}
		renames = append(renames, enumRename{old: old, new: knew})
	}

	sort.Slice(renames, func(i, j int) bool { return renames[i].oldName() < renames[j].oldName() })
	return renames
}

// enumTypesByName returns the enum-variety types of a schema keyed by their
// resolved (schema-qualified) name.
func enumTypesByName(s *Schema) map[string]ObjectSchema[*tree.CreateType] {
	out := make(map[string]ObjectSchema[*tree.CreateType])
	for _, t := range s.Types {
		if t.Ast.Variety == tree.Enum {
			out[t.ResolvedName()] = t
		}
	}
	return out
}

// enumLabelSignature returns an order-sensitive signature of an enum's labels.
// Labels are quoted so the boundary between adjacent labels is unambiguous.
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

// columnsSwitchedBetween returns the set of (oldType -> newType) UDT name pairs
// for which at least one column (present in both the remote and local version
// of the same table) changed its declared type from oldType to newType. Only
// user-defined (named) types are recorded; scalar `*types.T` columns are
// ignored because they can never be an enum rename target.
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
		localCols := columnUDTNames(lt.Ast)
		remoteCols := columnUDTNames(rt.Ast)
		for col, localType := range localCols {
			remoteType, ok := remoteCols[col]
			if !ok {
				continue
			}
			if localType != remoteType {
				out[switchKey{old: remoteType, new: localType}] = true
			}
		}
	}
	return out
}

// columnUDTNames maps each column with a user-defined (named) type to that
// type's resolved name. Columns with scalar types are omitted.
func columnUDTNames(ct *tree.CreateTable) map[string]string {
	out := make(map[string]string)
	for _, def := range ct.Defs {
		cd, ok := def.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		if name, ok := getResolvableTypeReferenceDepName(cd.Type); ok {
			out[cd.Name.Normalize()] = name
		}
	}
	return out
}

// enumChangeContext carries cross-object enum information into the per-table
// column comparison. It answers two questions the per-column diff needs:
//   - is a column's target type an enum? (so a rewrite cast can be bridged
//     through text: `col::STRING::newenum`, since CRDB rejects a direct
//     enum->enum cast); and
//   - is a column's type difference fully explained by a detected enum rename?
//     (so the column needs no DDL at all — the ALTER TYPE ... RENAME repoints it).
type enumChangeContext struct {
	localEnumNames map[string]bool   // resolved name -> true for enums defined locally
	renameByOld    map[string]string // old resolved name -> new resolved name
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

// isEnumTarget reports whether the given (target/local) column type is an enum.
// Enum columns parsed from SQL are unresolved object names rather than resolved
// `*types.T` values, so we consult the set of locally-defined enum type names;
// we also handle a resolved enum `*types.T` for completeness.
func (c *enumChangeContext) isEnumTarget(t tree.ResolvableTypeReference) bool {
	if tt, ok := t.(*types.T); ok {
		return tt.Family() == types.EnumFamily
	}
	if name, ok := getResolvableTypeReferenceDepName(t); ok {
		return c.localEnumNames[name]
	}
	return false
}

// explainedByRename reports whether the only difference between a remote and a
// local column type is a detected enum rename (remote uses the old name, local
// uses the new name). Such a column needs no type-change DDL.
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
