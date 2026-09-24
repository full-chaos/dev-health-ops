// Package opportunities is the Go port of GET+POST /api/v1/opportunities
// (api/services/opportunities.py's build_opportunities_response).
//
// build_opportunities_response reads NO ClickHouse/Postgres table of its
// own: it calls build_home_response and derives its cards purely
// in-process from HomeResponse.deltas (metric/label/delta_pct). This
// package mirrors that shape exactly -- FromHomeResponse is a pure
// function over an already-built home.Response, and BuildResponse is a
// thin wrapper that calls home.BuildResponse (with a nil Postgres
// client, matching build_opportunities_response's own call, which never
// threads semantic_session through) and then FromHomeResponse.
//
// Whatever home.BuildResponse filters, dedups or declares becomes this
// route's behaviour too -- see internal/home's own package doc comment
// and internal/goapiproof/home_corpus.go for the specific citations;
// none of it is re-declared here.
package opportunities

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
)

// Card is the wire shape of OpportunityCard (schemas.py:197-202).
type Card struct {
	ID                   string   `json:"id"`
	Title                string   `json:"title"`
	Rationale            string   `json:"rationale"`
	EvidenceLinks        []string `json:"evidence_links"`
	SuggestedExperiments []string `json:"suggested_experiments"`
}

// Response is the wire shape of OpportunitiesResponse (schemas.py:205-206).
type Response struct {
	Items []Card `json:"items"`
}

// defaultSuggestedExperiments ports _DEFAULT_SUGGESTED_EXPERIMENTS
// (services/opportunities.py:8-11) -- the fallback for any metric with
// no dedicated entry below (pr_rework_ratio is one of the eleven
// _METRICS entries in services/home.py but has no entry in
// _METRIC_SUGGESTED_EXPERIMENTS, so it falls through to this default).
var defaultSuggestedExperiments = []string{
	"Triage the top 10 longest-running work items.",
	"Introduce a rotating on-call reviewer for stalled PRs.",
}

// metricSuggestedExperiments ports _METRIC_SUGGESTED_EXPERIMENTS
// (services/opportunities.py:13-54).
var metricSuggestedExperiments = map[string][]string{
	"cycle_time": {
		"Trace the oldest active items to their current waiting state.",
		"Split one long-running item into the next reviewable slice.",
	},
	"review_latency": {
		"Reserve a daily review block for PRs waiting longest for first response.",
		"Pair authors with likely reviewers before opening complex PRs.",
	},
	"throughput": {
		"Audit recently completed items for the smallest repeatable delivery pattern.",
		"Pause new starts until the team finishes the highest-value active work.",
	},
	"deploy_freq": {
		"Identify the smallest safe release candidate and ship it behind existing controls.",
		"Review deploy blockers from the last cycle and remove one manual handoff.",
	},
	"churn": {
		"Review the files with the largest churn increase for unclear ownership or scope.",
		"Timebox a design checkpoint before the next high-churn change expands.",
	},
	"wip_saturation": {
		"Set a short-term WIP limit and finish active items before starting more.",
		"Reassign one blocked item owner to unblock or explicitly park it today.",
	},
	"blocked_work": {
		"Escalate the top blocked item with the dependency owner and a target unblock date.",
		"Convert recurring blocked states into explicit dependency tickets.",
	},
	"change_failure_rate": {
		"Review recent failed changes for the earliest detectable signal before release.",
		"Add one pre-release check to the riskiest deployment path.",
	},
	"rework_ratio": {
		"Compare reopened or rewritten work against its original acceptance criteria.",
		"Add a short pre-implementation alignment review for similar upcoming work.",
	},
	"ci_success": {
		"Classify the latest CI failures by flaky test, environment, or product defect.",
		"Fix or quarantine the highest-frequency flaky check before adding new coverage.",
	},
}

// suggestedExperimentsFor ports _suggested_experiments_for
// (services/opportunities.py:118-119).
func suggestedExperimentsFor(metric string) []string {
	if v, ok := metricSuggestedExperiments[metric]; ok {
		return v
	}
	return defaultSuggestedExperiments
}

// primaryScopeID ports _primary_scope_id (services/opportunities.py:112-115).
func primaryScopeID(f home.Filters) string {
	if len(f.Scope.IDs) > 0 {
		return f.Scope.IDs[0]
	}
	return ""
}

// FromHomeResponse ports build_opportunities_response's own card-building
// logic (services/opportunities.py:57-109) over an already-built
// home.Response -- the boundary build_opportunities_response itself
// composes at (it imports and calls build_home_response, never a
// ClickHouse/Postgres reader directly).
func FromHomeResponse(h *home.Response, f home.Filters) *Response {
	// "negative" is build_opportunities_response's own variable name
	// (services/opportunities.py:71) for deltas with delta_pct > 0 --
	// kept as a doc note here, not a Go identifier, since the Python name
	// does not describe what it holds.
	var positive []home.MetricDelta
	for _, d := range h.Deltas {
		if d.DeltaPct > 0 {
			positive = append(positive, d)
		}
	}
	// sorted(..., reverse=True) is stable: equal delta_pct values keep
	// their original _METRICS order (services/home.py) -- sort.SliceStable
	// preserves the same guarantee.
	sort.SliceStable(positive, func(i, j int) bool {
		return positive[i].DeltaPct > positive[j].DeltaPct
	})

	n := len(positive)
	if n > 4 {
		n = 4
	}
	ranked := positive[:n]

	scopeID := primaryScopeID(f)
	cards := make([]Card, 0, len(ranked))
	for idx, delta := range ranked {
		cards = append(cards, Card{
			ID:        fmt.Sprintf("opp-%d", idx+1),
			Title:     fmt.Sprintf("Reduce %s", delta.Label),
			Rationale: fmt.Sprintf("%s climbed %.0f%% in the last %d days.", delta.Label, delta.DeltaPct, f.Time.RangeDays),
			EvidenceLinks: []string{fmt.Sprintf(
				"/api/v1/explain?metric=%s&scope_type=%s&scope_id=%s&range_days=%d&compare_days=%d",
				delta.Metric, f.Scope.Level, scopeID, f.Time.RangeDays, f.Time.CompareDays,
			)},
			SuggestedExperiments: suggestedExperimentsFor(delta.Metric),
		})
	}

	if len(cards) == 0 {
		cards = append(cards, Card{
			ID:        "opp-0",
			Title:     "Maintain steady flow",
			Rationale: "Key metrics are stable. Focus on sustaining current practices.",
			EvidenceLinks: []string{fmt.Sprintf(
				"/api/v1/home?scope_type=%s&scope_id=%s",
				f.Scope.Level, scopeID,
			)},
			SuggestedExperiments: []string{"Share the current playbook with new teams."},
		})
	}

	return &Response{Items: cards}
}

// BuildResponse ports build_opportunities_response (services/opportunities.py:57-109),
// composing internal/home's own exported builder exactly as the Python
// function composes build_home_response -- including never threading a
// Postgres client through (build_opportunities_response's own call to
// build_home_response never passes semantic_session, so
// latest_successful_sync_at is always unresolved on this route; that
// field is not read anywhere on this route's own response, so it is not
// a divergence).
//
// NO CACHING PORTED, matching internal/home's own BuildResponse (see its
// doc comment): Python's build_opportunities_response passes its own
// `cache` argument straight through to build_home_response
// (services/opportunities.py:64-69) -- main.py wires this to HOME_CACHE,
// the SAME epoch-scoped 60s-TTL cache instance /api/v1/home itself
// reads and writes, keyed by ("home", org_id, filters) with NO
// route-specific component (services/home.py's own
// epoch_cache_key(cache, "home", org_id, filters)). So in Python, a
// recent /api/v1/home call and a /api/v1/opportunities call for the SAME
// org+filters can serve one another's cached result, and either can
// return a home response up to 60s stale rather than a fresh read. This
// Go port never caches at all -- every call recomputes from ClickHouse,
// same as internal/home.BuildResponse's own port. A cache hit and a
// cache miss return byte-identical JSON in Python for STABLE underlying
// data, so this is not a wire-shape divergence; it IS a genuine
// data-freshness/timing gap this port leaves uncovered rather than
// shaped -- see this package's own RISK-NOTES citation.
func BuildResponse(ctx context.Context, chClient home.QueryClient, orgID string, f home.Filters, now time.Time) (*Response, error) {
	h, err := home.BuildResponse(ctx, chClient, nil, orgID, f, now)
	if err != nil {
		return nil, err
	}
	return FromHomeResponse(h, f), nil
}
