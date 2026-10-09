package prrework

// Table is the ClickHouse table that holds the daily counts.
const Table = "repo_metrics_daily"

// The count columns of Table, in Counts field order. prs_merged is the count
// the table always had; the three others hold NULL on a row that was written
// before the counts existed.
const (
	ColumnMerged   = "prs_merged"
	ColumnReviewed = "prs_merged_reviewed"
	ColumnRework   = "prs_merged_rework"
	ColumnNoSignal = "prs_merged_no_rework_signal"
)

// CountColumns are the count columns in Counts field order.
var CountColumns = []string{ColumnMerged, ColumnReviewed, ColumnRework, ColumnNoSignal}

// DayRatioColumn is the one-day value for a reader of one row: NULL when the
// day is not measured.
const DayRatioColumn = "pr_rework_ratio_reviewed"

// DeprecatedRatioColumn is the old ratio, rework / ALL merged pull requests
// with 0 for a day with no merged pull request. It is still written for older
// readers; no reader of this release reads it.
const DeprecatedRatioColumn = "pr_rework_ratio"

// WindowRateSQL is Rate as a ClickHouse aggregate expression over rows of
// Table that are already deduplicated to the newest computed_at per
// (org_id, repo_id, day). It is NULL when the view is not measured, so a
// reader must scan it into a nullable destination. A row with no stored
// counts adds nothing to either sum.
const WindowRateSQL = `if(sum(ifNull(prs_merged_reviewed, 0)) = 0, NULL, toFloat64(sum(ifNull(prs_merged_rework, 0))) / toFloat64(sum(ifNull(prs_merged_reviewed, 0))))`

// ViewSumsSQL selects, from rows of Table already deduplicated to the newest
// computed_at per (org_id, repo_id, day), the summed counts in CountColumns
// order and the number of rows that hold counts: the columns ViewScanDest
// scans. prs_merged is summed over the rows that hold counts only, so the
// coverage is reviewed / merged of the same rows.
const ViewSumsSQL = `toUInt64(sumIf(prs_merged, prs_merged_reviewed IS NOT NULL)), toUInt64(sum(ifNull(prs_merged_reviewed, 0))), toUInt64(sum(ifNull(prs_merged_rework, 0))), toUInt64(sum(ifNull(prs_merged_no_rework_signal, 0))), toUInt64(countIf(prs_merged_reviewed IS NOT NULL))`

// ViewScanDest returns the scan destinations for one ViewSumsSQL row.
func ViewScanDest(view *View) []any {
	return []any{&view.Merged, &view.Reviewed, &view.Rework, &view.NoSignal, &view.StoredRows}
}
