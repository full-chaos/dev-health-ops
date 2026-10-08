// Package latestrow is the ONE definition of which stored row of a work unit
// is "the latest" in work_unit_investments (CHAOS-8908).
//
// The table is ReplacingMergeTree(computed_at) ORDER BY (org_id, work_unit_id),
// so before a merge a unit can hold several rows. Every reader that picks one
// (the investment skip check, the served reader, the evidence and
// distribution reads) must pick the SAME row, and it must be the row a merge
// keeps, so a merge never changes an answer. OrderKey is therefore the
// merge's own order: computed_at first; then, for equal computed_at, the
// INSERT order, which is what ReplacingMergeTree keeps for equal versions
// (the last row of the later part, the later row of one part).
//
// Insert order is read from the row's physical position: the highest block
// number in the part name (all_<min>_<max>_<level>; the table has no
// partition) and the row's offset in the part.
package latestrow

// OrderKey is the ordering expression of the latest row.
//
// The columns are table-qualified on purpose: a reader that outputs a column
// under its own name (argMax(x, ...) AS categorization_run_id) would otherwise
// see that alias inside the next aggregate (ILLEGAL_AGGREGATION, code 184).
const OrderKey = "(work_unit_investments.computed_at, toUInt64OrZero(splitByChar('_', work_unit_investments._part)[3]), work_unit_investments._part_offset)"

// ArgMax returns argMax(column, OrderKey).
func ArgMax(column string) string {
	return "argMax(" + column + ", " + OrderKey + ")"
}

// ArgMaxKeepNull returns the first element of argMax(tuple(column), OrderKey):
// unlike ArgMax it keeps a NULL of the latest row instead of skipping it.
func ArgMaxKeepNull(column string) string {
	return "(argMax(tuple(" + column + "), " + OrderKey + ")).1"
}
