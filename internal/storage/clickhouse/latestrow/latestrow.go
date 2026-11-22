// Package latestrow is ops' name for the ONE definition of which stored row
// of a work unit is "the latest" in work_unit_investments (CHAOS-8908). The
// definition itself lives in github.com/full-chaos/dev-health-go/latestrow,
// which ops, acr and the dev-health-go readers all read, so no repository
// keeps its own copy of the key. This package only forwards to it; a key
// change is a dev-health-go tag and a bump here.
//
// The table is ReplacingMergeTree(computed_at) ORDER BY (org_id, work_unit_id),
// so before a merge a unit can hold several rows. Every reader that picks one
// (the investment skip check, the served reader, the evidence and
// distribution reads) must pick the SAME row, and it must be the row a merge
// keeps, so a merge never changes an answer: computed_at first, then the
// insert order.
package latestrow

import golatestrow "github.com/full-chaos/dev-health-go/latestrow"

// OrderKey is the ordering expression of the latest row.
const OrderKey = golatestrow.OrderKey

// ArgMax returns argMax(column, OrderKey).
func ArgMax(column string) string {
	return golatestrow.ArgMax(column)
}

// ArgMaxKeepNull returns the first element of argMax(tuple(column), OrderKey):
// unlike ArgMax it keeps a NULL of the latest row instead of skipping it.
func ArgMaxKeepNull(column string) string {
	return golatestrow.ArgMaxKeepNull(column)
}

// ArgMaxKeepNullBy returns the latest value of a Nullable column of a table
// other than work_unit_investments, ordered by version, with the value as the
// tiebreak for equal versions.
func ArgMaxKeepNullBy(column, version string) string {
	return golatestrow.ArgMaxKeepNullBy(column, version)
}
