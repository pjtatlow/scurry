package schema

import (
	"cmp"
	"maps"
	"slices"
)

func sortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	return slices.Sorted(maps.Keys(m))
}
