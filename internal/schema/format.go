package schema

import (
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
)

func formatNode(node tree.NodeFormatter) string {
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	node.Format(fmtCtx)
	return fmtCtx.CloseAndGetString()
}
