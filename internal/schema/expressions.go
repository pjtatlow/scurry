package schema

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"

	"github.com/pjtatlow/scurry/internal/set"
)

func getExprDeps(expr tree.Expr) set.Set[string] {
	v := newExprVisitor()
	v.visitNode(expr)
	expr.Walk(v)
	return v.deps
}

// Attempts to find any names that were automatically prefixed with "public." that might have actually been column names,
// and adds a "tableName." prefix to them instead.
func getExprColumnDeps(schemaTableName, tableName string, expr tree.Expr) set.Set[string] {
	v := newExprVisitor()
	v.visitNode(expr)
	expr.Walk(v)
	deps := slices.Collect(v.deps.Values())
	for _, name := range deps {
		if strings.HasPrefix(name, "public.") {
			v.deps.Add(fmt.Sprintf("%s.%s.%s", schemaTableName, tableName, name[7:]))
		}
	}
	return v.deps
}

type exprVisitor struct {
	deps set.Set[string]
}

func newExprVisitor() *exprVisitor {
	return &exprVisitor{
		deps: set.New[string](),
	}
}

func (v *exprVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	v.visitNode(expr)
	return true, expr
}

func (v *exprVisitor) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

func (v *exprVisitor) visitNode(expr tree.Expr) {
	switch expr := expr.(type) {
	case *tree.AllColumnsSelector:
	case *tree.AndExpr:
	case *tree.AnnotateTypeExpr:
	case *tree.Array:
	case *tree.ArrayFlatten:
	case *tree.BinaryExpr:
	case *tree.CaseExpr:
	case *tree.CastExpr:
		{
			if name, ok := getResolvableTypeReferenceDepName(expr.Type); ok {
				v.deps.Add(name)
			}

		}
	case *tree.CoalesceExpr:
	case *tree.CollateExpr:
	case *tree.ColumnAccessExpr:
	case *tree.ColumnItem:
	case *tree.ComparisonExpr:
	case *tree.DArray:
	case *tree.DBitArray:
	case *tree.DBool:
	case *tree.DBox2D:
	case *tree.DBytes:
	case *tree.DCollatedString:
	case *tree.DDate:
	case *tree.DDecimal:
	case *tree.DEncodedKey:
	case *tree.DEnum:
	case *tree.DFloat:
	case *tree.DGeography:
	case *tree.DGeometry:
	case *tree.DIPAddr:
	case *tree.DInt:
	case *tree.DInterval:
	case *tree.DJSON:
	case *tree.DJsonpath:
	case *tree.DOid:
	case *tree.DOidWrapper:
	case *tree.DPGLSN:
	case *tree.DPGVector:
	case *tree.DString:
	case *tree.DTSQuery:
	case *tree.DTSVector:
	case *tree.DTime:
	case *tree.DTimeTZ:
	case *tree.DTimestamp:
	case *tree.DTimestampTZ:
	case *tree.DTuple:
	case *tree.DUuid:
	case *tree.DVoid:
	case *tree.DefaultVal:
	case *tree.FuncExpr:
		if expr.Func.ReferenceByName != nil {
			schema, objectName := getObjectName(expr.Func.ReferenceByName)
			v.deps.Add(fmt.Sprintf("%s.%s", schema, objectName))
		}
		if name, ok := expr.Func.FunctionReference.(*tree.UnresolvedName); ok {
			v.visitNode(name)
		}
	case *tree.IfErrExpr:
	case *tree.IfExpr:
	case *tree.IndirectionExpr:
	case *tree.IsNotNullExpr:
	case *tree.IsNullExpr:
	case *tree.IsOfTypeExpr:
	case *tree.NotExpr:
	case *tree.NullIfExpr:
	case *tree.NumVal:
	case *tree.OrExpr:
	case *tree.ParenExpr:
	case *tree.PartitionMaxVal:
	case *tree.PartitionMinVal:
	case *tree.Placeholder:
	case *tree.RangeCond:
	case *tree.StrVal:
	case *tree.Subquery:
	case *tree.Tuple:
	case *tree.TupleStar:
	case *tree.UnaryExpr:
	case *tree.UnqualifiedStar:
	case *tree.UnresolvedName:
		{
			name, err := expr.ToUnresolvedObjectName(tree.NoAnnotation)
			if err != nil {
				return
			}
			schema, objectName := getObjectName(&name)
			v.deps.Add(fmt.Sprintf("%s.%s", schema, objectName))
		}
	}
}

func getResolvableTypeReferenceDepName(name tree.ResolvableTypeReference) (string, bool) {
	switch name := name.(type) {
	case *tree.UnresolvedObjectName:
		schemaName, objectName := getObjectName(name)
		return fmt.Sprintf("%s.%s", schemaName, objectName), true
	case *tree.ArrayTypeReference:
		return getResolvableTypeReferenceDepName(name.ElementType)
	default:
		return "", false
	}
}
