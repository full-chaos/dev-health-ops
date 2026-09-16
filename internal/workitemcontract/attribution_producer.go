package workitemcontract

// Three independent paths write work_item_team_attributions: the daily
// `work_item_attribution` metrics family, the remaining-family staleness
// backstop, and providersync's sync-time deriver. They resolve the same
// cascade over the same keys into a ReplacingMergeTree(computed_at) whose
// sorting key carries no producer column, so two producers' rows for one key
// are indistinguishable once they are stored.
//
// These names are the vocabulary the `writer` column stores. They live here,
// in the package that already owns this table's destination declaration,
// because the writing packages do not import each other -- a second spelling
// in one of them would silently split the column's value space, and a query
// filtering on one spelling would report zero rows for a path that is
// writing normally.
const (
	// AttributionWriterDaily is the daily metrics family, scoped to a
	// partition's (org, repos, day) window.
	AttributionWriterDaily = "daily"
	// AttributionWriterBackstop is the remaining-family backstop, scoped to
	// the work items whose ownership changed since its last run.
	AttributionWriterBackstop = "backstop"
	// AttributionWriterSync is providersync's sync-time deriver, scoped to the
	// work items one sync claim just ingested.
	AttributionWriterSync = "sync"
)

// AttributionWriters returns the writer names in declaration order. A reader
// grouping rows by producer uses this rather than a literal list, so a fourth
// path added above cannot be missed by a dashboard or a divergence check.
func AttributionWriters() []string {
	return []string{
		AttributionWriterDaily,
		AttributionWriterBackstop,
		AttributionWriterSync,
	}
}
