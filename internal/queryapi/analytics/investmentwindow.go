package analytics

// InvestmentWindowClause is the one window predicate of every work-unit read of Investment (CHAOS-9016, D6022): a
// unit belongs to the window [start, end) that holds its END day (to_ts), whole, in exactly one window, so adjacent
// windows add up to the whole period. datedByEnd false is the older overlap of [from_ts, to_ts] with the window.
func InvestmentWindowClause(datedByEnd bool) string {
	if datedByEnd {
		return `work_unit_investments.to_ts >= {start_date:Date}
  AND work_unit_investments.to_ts < {end_date:Date}`
	}
	return `work_unit_investments.from_ts < {end_date:Date}
  AND work_unit_investments.to_ts >= {start_date:Date}`
}
