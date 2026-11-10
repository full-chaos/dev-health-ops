package admin

import (
	"slices"
	"strings"
)

// rowsByKey returns the rows sorted by key, so a category list has one order whatever the store returns.
func rowsByKey(rows []setting) []setting {
	out := slices.Clone(rows)
	slices.SortFunc(out, func(a, b setting) int { return strings.Compare(a.Key, b.Key) })
	return out
}
