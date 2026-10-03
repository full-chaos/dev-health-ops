package providerfoundation

// costClassBudgetLimits is the ONE table of concurrent provider requests the
// worker serves per (provider, org, host, cost class) budget key, and the
// number of units the dispatcher may admit per (org, provider, cost class)
// bucket (CHAOS-7434). Two maps carrying the same numbers let the dispatcher
// admit eight heavy units while the worker budget served one, so seven of
// them looped in 1-2 s budget-contention snoozes for hours: both consumers
// read this table instead.
var costClassBudgetLimits = map[string]int{
	"light":  4,
	"medium": 2,
	"heavy":  1,
}

// CostClassBudgetLimitSource names where every limit of the table comes from, for the start lines that print them: the
// table above, never an environment variable or a config value (CHAOS-8201).
const CostClassBudgetLimitSource = "table"

// CostClassBudgetLimit returns the concurrent-request limit of a cost class.
// ok is false for a class outside the table; callers decide what an unknown
// class means (the dispatch guard keeps its configured clamp, the worker
// refuses to build a budget key).
func CostClassBudgetLimit(costClass string) (limit int, ok bool) {
	limit, ok = costClassBudgetLimits[costClass]
	return limit, ok
}
