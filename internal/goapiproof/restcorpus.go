package goapiproof

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
)

// This file is the REST sibling of operations.go: a committed, per-route
// request corpus, refused-by-name when a route this table does not cover
// is live (see AssertRESTPathCoverage), exactly like operationSpecs refuses an
// uncovered GraphQL operation.
//
// A route's corpus is keyed by its own routeswitch operation name (e.g.
// "REST:GET:/api/v1/quadrant") -- the same string cmd/query-api's own
// route file declares as a package constant and registers on its
// DynamicSwitch. internal/migrationmatrix's RESTOperationName renders the
// SAME string from a receipt's own (method, path) columns
// (go_api_rest_proof_run has no operation-name column of its own --
// alembic 0134's own doc comment on why it is keyed by method+path
// instead), so a receipt and its corpus entry are matched by re-deriving
// one string the same way on both sides, never by storing it twice.
// AssertRESTPathCoverage checks the corpus's PATHS (not the operation
// strings, which a live query-api has no way to report; see that
// function's own doc comment) against the mounted mux.

// RESTBodyMode selects how a corpus entry's two response bodies are
// treated once status-code admission holds.
type RESTBodyMode string

const (
	// RESTBodyModeJSON decodes both bodies as JSON and runs Compare over
	// them under the entry's own Parity -- the normal case.
	RESTBodyModeJSON RESTBodyMode = "json"
	// RESTBodyModeStatusOnly never decodes or compares either body. Used
	// where at least one leg's body is not JSON by design (a plain-text
	// http.Error body) or where this table does not yet declare the
	// body's content field-by-field -- see restEndpointSpecs' own entries
	// for which and why.
	RESTBodyModeStatusOnly RESTBodyMode = "status_only"
)

// RESTRequest is one corpus entry: a single, fully-specified HTTP request
// sent to both planes, plus what admissible and matching mean for it.
type RESTRequest struct {
	// Name distinguishes this request from its siblings under the same
	// route, in operator output and in RequestIdentity. Never empty --
	// ValidateRESTCorpus refuses an entry without one.
	Name string
	// Query is the request's query-string parameters, nil for none.
	Query url.Values
	// Body is JSON-marshalled as the request body for POST/PUT/PATCH, and
	// must be nil for GET.
	Body any

	// WantCandidateStatus/WantBaselineStatus are the status EACH plane
	// must answer for this request to be admissible (RESTAdmit refuses
	// otherwise). Equal for the overwhelming majority of entries; see
	// StatusDivergenceReason for the declared exception.
	WantCandidateStatus int
	WantBaselineStatus  int
	// StatusDivergenceReason states why WantCandidateStatus and
	// WantBaselineStatus are allowed to differ -- a genuine, ACCEPTED
	// Go-side status-code choice documented at the route's own request
	// parser (e.g. quadrant_route.go's parseQuadrantDate: Go answers 400
	// where Python's Pydantic answers 422 for the same malformed date,
	// and the route's own doc comment calls this "a documented,
	// Go-side-only status-code divergence; the DATA contract ... is
	// unaffected"). This is never how a Python DEFECT is declared -- that
	// is Parity.BaselineDefects, which never touches the status code.
	// Required when the two Want values differ; forbidden when they
	// agree (ValidateRESTCorpus checks both directions).
	StatusDivergenceReason string

	// BodyMode controls whether the two bodies are compared at all.
	BodyMode RESTBodyMode
	// Parity is this request's declared comparator configuration -- the
	// SAME Options type a GraphQL OperationSpec declares, reused rather
	// than duplicated: FloatTierB/VolatileFields/BaselineDefects/
	// OrderInsensitiveLists all mean exactly what they mean there.
	Parity Options

	// DedupListPath/DedupKeyFields, when both set, run
	// InjectRESTDedupKeys on both snapshots before Compare -- see that
	// function's doc comment. Leave both zero for a request with no
	// dedup-shaped baseline defect.
	DedupListPath  string
	DedupKeyFields []string

	// Produces lists ids this request's BASELINE response makes
	// available to a LATER request's own IDBindings -- see
	// restidbind.go's package doc comment. Empty for every request that
	// supplies none (the overwhelming majority).
	Produces []RESTIDProducer

	// IDBindings lists ids an EARLIER request (per RESTRunOrder) must
	// have Produced -- resolved at run time and applied to BOTH legs of
	// THIS request before either is sent. ValidateRESTIDBindingOrder
	// refuses a binding whose Producer is not Produced by a request
	// strictly earlier in RESTRunOrder: a consumer before its producer is
	// a corpus error, caught at startup, never discovered as an
	// always-refused request in a live run.
	IDBindings []RESTIDBinding
}

// RESTEndpointSpec is one REST route's committed corpus.
type RESTEndpointSpec struct {
	Method   string
	Path     string
	Requests []RESTRequest
	// PublicNoAuth marks a route that carries NO bearer-envelope check on
	// either plane -- meta is the only one today (see meta.go's own
	// "Auth: PUBLIC, verified from source, not assumed" doc comment). A
	// request under a PublicNoAuth spec is sent with no Authorization
	// header on EITHER leg, so the public code path is what is actually
	// measured, not an authenticated path that merely happens to also
	// succeed unauthenticated.
	PublicNoAuth bool
}

// investmentExplainProseFields names investment/explain's LLM-authored
// paths -- text the model writes fresh per call, never reproducible
// across two calls let alone across two planes calling two different
// model deployments. Declared VolatileFields, not skipped entirely: the
// STRUCTURED fields around them (the numeric evidence, confidence.level's
// own deterministic band thresholds are NOT here -- only the model's
// free-text judgement is) still compare normally. Index-free, so one
// entry each covers every element of top_findings/what_to_check_next.
var investmentExplainProseFields = map[string]string{
	"data.summary":                   "LLM-generated prose paragraph: not reproducible across two calls, let alone two planes calling two model deployments",
	"data.top_findings.finding":      "LLM-generated prose finding statement",
	"data.confidence.drivers":        "LLM-generated prose list of what drove its own confidence judgement",
	"data.what_to_check_next.action": "LLM-generated prose",
	"data.what_to_check_next.why":    "LLM-generated prose",
	"data.what_to_check_next.where":  "LLM-generated prose",
	"data.anti_claims":               "LLM-generated prose list",
}

// investmentExplainDeterministicFloats names investment/explain's
// ClickHouse-derived numeric fields declared Tier B: each is a merged
// floating-point aggregate, order-nondeterministic across two runs on the
// SAME plane, let alone two planes -- the same rule every sibling
// GraphQL operation's own FloatTierB table already applies, not a new
// one invented for this route.
var investmentExplainDeterministicFloats = map[string]string{
	"data.confidence.quality_mean":                     "avgIf(evidence_quality)-shaped ClickHouse float aggregate over the request's attributed work units",
	"data.confidence.quality_stddev":                   "stddevPopIf(evidence_quality)-shaped ClickHouse float aggregate",
	"data.top_findings.evidence.share_pct":             "derived from a ClickHouse float aggregate over attributed work-unit effort",
	"data.top_findings.evidence.delta_pct_points":      "derived from the same float aggregates as share_pct",
	"data.top_findings.evidence.evidence_quality_mean": "avgIf(evidence_quality)-shaped ClickHouse float aggregate",
}

// quadrantPointFloats declares quadrant.Point's x/y as Tier B: both are
// wrapped in toFloat64(...) over a ClickHouse aggregate expression
// (quadrant.go's own package doc comment: "every value_expr is wrapped in
// toFloat64(...)"), and the underlying aggregate is the same class of
// merged, order-nondeterministic floating-point value every sibling
// FloatTierB table in this service already declares.
var quadrantPointFloats = map[string]string{
	"data.points.x": "quadrant.go's toFloat64(...)-wrapped ClickHouse aggregate expression",
	"data.points.y": "quadrant.go's toFloat64(...)-wrapped ClickHouse aggregate expression",
}

// drilldownPRsParity is shared by every admissible (2xx) drilldown/prs
// request, GET and POST alike: both routes call the same
// BuildPRsResponse, so both carry the same two declared Python-plane
// defects.
var drilldownPRsParity = Options{
	BaselineDefects: []BaselineDefect{
		{
			Ticket: "CHAOS-5803",
			Reason: "git_pull_requests' created_at/merged_at/first_review_at columns are ClickHouse DateTime64(3, 'UTC') (000_raw_tables.sql); Python's clickhouse_connect driver returns them NAIVE (no tzinfo), so the Pydantic response serializes them with no offset, while Go's driver attaches UTC location and this port's PRItem (encoding/json's default time.Time marshaling) emits RFC 3339 with an explicit offset -- the same class of divergence already declared for the operations.go GraphQL corpus's own capacityForecasts and pr entries. Go is correct.",
			Paths: []string{
				"data.items.created_at",
				"data.items.merged_at",
				"data.items.first_review_at",
			},
			Intermittent:       true,
			IntermittentReason: "present only while this request's live result actually contains at least one returned PR with a non-null value under one of these three fields; an empty items list, or a window whose only PRs happen to leave every one of these fields null, shows no divergence under these paths and is not a bug in this citation -- the same live-data-dependence every other Intermittent entry in this table declares, just triggered by result content here rather than by ClickHouse's own merge state",
		},
		{
			Ticket:             "CHAOS-5803",
			Reason:             "git_pull_requests and repos are both ReplacingMergeTree(last_synced) (000_raw_tables.sql); fetch_pull_requests (api/queries/drilldown.py) reads neither with FINAL or any other merge-time dedup, so an unmerged physical version of the same logical PR ((repo_id, number)) can surface as two rows sharing that identity, spending one extra slot of the page limit and shifting every later element's position -- the same mechanism WorkGraphEdgeDedupShape already covers for the GraphQL workGraphEdges operation. This port's drilldown.Reader reads both tables FINAL. Go is correct.",
			Paths:              []string{"data.items"},
			Intermittent:       true,
			IntermittentReason: "present only while the source tables hold an unmerged physical version of some PR or repo row; a comparison taken after the next background merge shows no repeated (repo_id, number) on the baseline side either",
			WorkGraphEdgeDedupShape: &WorkGraphEdgeDedupShape{
				EdgesListPath: "data.items",
				IDField:       RESTDedupKeyField,
			},
		},
	},
}

// drilldownPRsDedup is every admissible drilldown/prs request's dedup
// declaration -- see InjectRESTDedupKeys and drilldownPRsParity's own
// WorkGraphEdgeDedupShape entry above.
var drilldownPRsDedup = struct {
	ListPath  string
	KeyFields []string
}{ListPath: "items", KeyFields: []string{"repo_id", "number"}}

// drilldownIssuesParity is shared by every admissible (2xx) drilldown/
// issues request, GET and POST alike: both routes call the same
// BuildIssuesResponse, so both carry the same declared Python-plane
// defect. Unlike drilldownPRsParity there is only one BaselineDefect here:
// work_item_cycle_times is already read FINAL on both planes
// (fetchIssuesQuery's own doc comment in internal/drilldown/issues.go), so
// the RMT-dedup shape drilldownPRsParity's own second entry declares has
// no counterpart on this route.
var drilldownIssuesParity = Options{
	BaselineDefects: []BaselineDefect{
		{
			Ticket: "CHAOS-5808",
			Reason: "work_item_cycle_times' started_at/completed_at columns are ClickHouse Nullable(DateTime('UTC')) (001_metrics_v2.sql); Python's clickhouse_connect driver returns them NAIVE (no tzinfo), so the Pydantic response serializes them with no offset, while Go's driver attaches UTC location and this port's IssueItem (encoding/json's default time.Time marshaling) emits RFC 3339 with an explicit offset -- the same class of divergence drilldown/prs' own corpus entry already declares for created_at/merged_at/first_review_at. Go is correct.",
			Paths: []string{
				"data.items.started_at",
				"data.items.completed_at",
			},
			Intermittent:       true,
			IntermittentReason: "present only while this request's live result actually contains at least one returned issue with a non-null started_at or completed_at -- both columns are themselves Nullable, so an empty items list, or a window whose only issues have not yet started/completed, shows no divergence under these paths and is not a bug in this citation",
		},
	},
}

// explainParity is shared by every admissible (2xx) explain request, GET
// and POST alike: both routes call the same BuildExplainResponse, so both
// carry the same four declared, Intermittent Python-plane defects (see
// restEndpointSpecs' own doc comment for the fifth, undeclared one).
var explainParity = Options{
	BaselineDefects: []BaselineDefect{
		{
			Ticket: "CHAOS-5813",
			Reason: "repos and teams are both ReplacingMergeTree (repos since 000_raw_tables.sql, teams since 002_teams.sql); resolveScopeDisplayNames' own Python source (identity.py) reads neither with FINAL or any argMax dedup, so an unmerged physical version of a driver's or contributor's repo/team row can surface as a stale display_name for that id (or, transiently, an extra physical row read as part of the same scan). This port reads both FINAL. Go is correct.",
			Paths: []string{
				"data.drivers.display_name",
				"data.drivers.label",
				"data.contributors.display_name",
				"data.contributors.label",
			},
			Intermittent:       true,
			IntermittentReason: "present only while repos or teams holds an unmerged physical version for an id this response actually names; a comparison taken after the next background merge shows no divergence under these paths",
		},
		{
			Ticket: "CHAOS-5813",
			Reason: "work_item_metrics_daily.cycle_time_p50_hours and repo_metrics_daily.pr_first_review_p50_hours are both Nullable(Float64) (001_metrics_v2.sql); Python's own reader (api/queries/metrics.py's metric-value read) takes a bare argMax(col, computed_at) over them, which can return a STALE non-null version instead of the row at the TRUE latest version whenever that latest version's own value is NULL -- argMax's own candidate selection skips a NULL arg rather than tracking \"the value at the max version\". This port's metricValueProjection wraps the column in a tuple, (argMax(tuple(col), computed_at)).1, so the true latest version always wins, NULL included -- reaching every numeric field this route derives from that column (the headline value/delta and every driver/contributor value) for the cycle_time and review_latency metrics, the only two this route reaches whose column is Nullable. Go is correct.",
			Paths: []string{
				"data.value",
				"data.delta_pct",
				"data.drivers.value",
				"data.drivers.delta_pct",
				"data.contributors.value",
			},
			Intermittent:       true,
			IntermittentReason: "present only while the metric's own source table holds an unmerged physical version whose latest row is NULL for this column, and only for the cycle_time/review_latency metrics (the only two Nullable(Float64) columns this route reaches); a comparison taken after the next merge, or for any other metric, shows no divergence under these paths",
		},
		{
			Ticket: "CHAOS-5818",
			Reason: "fetch_metric_contributors and fetch_metric_driver_delta (api/queries/explain.py) rank every metric with a hardcoded avg(column), regardless of that metric's own aggregator in _METRIC_CONFIG -- this table's own headline reader, fetch_metric_value (api/queries/metrics.py), already keys off the metric's configured aggregator, so an avg-aggregator metric's ranking agrees with its own headline while a sum-aggregator metric's ranking (throughput, deploy_freq, churn, blocked_work) silently averages a quantity the metric's own label, unit and headline all present as a total. This port's fetchMetricContributors/fetchMetricDriverDelta (cmd/query-api/internal/explain/metrics.go) take the metric's own config.Aggregator, matching the headline read. Go is correct.",
			Paths: []string{
				"data.drivers.value",
				"data.drivers.delta_pct",
				"data.contributors.value",
			},
			Intermittent:       true,
			IntermittentReason: "present only for a request whose metric resolves to a sum-aggregator config (throughput, deploy_freq, churn, blocked_work) and whose ranked group has more than one contributing daily row with differing values in the request window -- a single-row or uniform-value group leaves sum and avg equal, and an avg-aggregator metric (cycle_time, review_latency, wip_saturation, change_failure_rate) never reaches this path at all",
		},
		{
			Ticket: "CHAOS-5819",
			Reason: "blocked_work's table/column read (work_item_state_durations_daily.duration_hours) carries no status predicate in fetch_metric_value/fetch_metric_contributors/fetch_metric_driver_delta (api/queries/metrics.py, api/queries/explain.py), so the 'Blocked Work' headline, its drivers and its contributors sum/rank duration_hours across every status the table records (backlog/todo/in_progress/in_review/blocked/done/canceled/unknown) -- only this table's OTHER, unrelated reader (fetch_blocked_hours, used by home.py, never by /explain) restricts to status = 'blocked'. This port's blocked_work config carries a StatusFilter of 'blocked' (cmd/query-api/internal/explain/metricconfig.go), reaching every numeric field this route derives from that column for this one metric. Go is correct.",
			Paths: []string{
				"data.value",
				"data.delta_pct",
				"data.drivers.value",
				"data.drivers.delta_pct",
				"data.contributors.value",
			},
			Intermittent:       true,
			IntermittentReason: "present only for a blocked_work request whose window/scope has at least one non-blocked-status row contributing duration_hours for a natural key (day, provider, work_scope_id, team_id) that also carries a blocked-status row; a window with no non-blocked duration recorded, or no data at all, shows no divergence under these paths, and no other metric ever reaches this path",
		},
	},
}

// explainScopeDropDefect is scope_filter_for_metric's own org_id-
// omission bug (cmd/query-api/internal/explain/response.go's own
// scopeFilterForMetric doc comment): explain.py's one call site for this
// chain omits the org_id keyword entirely, so for a "repo"-scoped metric
// (review_latency, deploy_freq, churn, change_failure_rate --
// metricconfig.go's own Scope: "repo" entries; GroupBy repo_id) EVERY
// repo/team scope the caller asks for resolves to zero repo ids on the
// Python side and is silently dropped -- the metric aggregates over the
// WHOLE ORG instead of the requested scope, still correctly org-bounded
// by the metric query's own separate org_id filter (a scope-filter
// defect, not a cross-tenant leak). This port passes the real orgID
// (scopeFilterForMetric's own doc comment: "Go is correct"), so a
// genuinely scoped request narrows on Go and does not on Python. Declared
// only on the entries below that actually request a non-default scope
// for one of these four metrics: a default (org-scoped) request never
// reaches this branch with a scope to drop, so explainParity's own
// entries carry no citation for it.
var explainScopeDropDefect = BaselineDefect{
	Ticket:             "CHAOS-5813",
	Reason:             "explain.py's own call site for scope_filter_for_metric (api/services/explain.py:150-152) is the only caller anywhere in the Python source that omits the org_id keyword -- it silently defaults to \"\". For a repo-scoped metric (review_latency/deploy_freq/churn/change_failure_rate) that flows into resolve_repo_id's own org_id filter, which no real org's repos row ever matches: every repo/team scope ref fails to resolve, repo_ids ends up [], and the scope filter is silently dropped, so the headline value/delta and every driver/contributor value are computed over the whole org rather than the requested scope. This port passes the real org id throughout. Go is correct.",
	Paths:              []string{"data.value", "data.delta_pct", "data.drivers.value", "data.drivers.delta_pct", "data.contributors.value"},
	Intermittent:       true,
	IntermittentReason: "present only while the requested repo/team scope's own aggregate actually differs from the whole org's aggregate for this metric and window; a scope whose narrowed value happens to equal the org-wide one shows no divergence under these paths",
}

// explainRepoTeamScopedParity is explainParity's own two declared
// defects plus explainScopeDropDefect -- shared by every explain entry
// below that requests an explicit, live repo or team scope for one of
// the four metrics explainScopeDropDefect names.
var explainRepoTeamScopedParity = Options{
	BaselineDefects: append(append([]BaselineDefect{}, explainParity.BaselineDefects...), explainScopeDropDefect),
}

// explainScopedRequest builds one repo- or team-scoped explain request
// for a metric explainScopeDropDefect covers, its scope_id bound at run
// time to producerName's own live id (see restidbind.go).
func explainScopedRequest(name, metric, scopeType, producerName string) RESTRequest {
	return RESTRequest{
		Name:                name,
		Query:               url.Values{"metric": {metric}, "scope_type": {scopeType}},
		WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode:   RESTBodyModeJSON,
		Parity:     explainRepoTeamScopedParity,
		IDBindings: []RESTIDBinding{{Producer: producerName, QueryParam: "scope_id"}},
	}
}

// peopleParity is shared by every admissible (2xx) people request whose
// query string actually reaches ClickHouse -- a genuinely empty q
// short-circuits BEFORE the client is touched (BuildSearchResponse's own
// early return), so the "default" entry below carries no Parity at all:
// there is nothing this citation could ever cover there.
var peopleParity = Options{
	BaselineDefects: []BaselineDefect{
		{
			Ticket:             "CHAOS-5812",
			Reason:             "user_metrics_daily is ReplacingMergeTree(computed_at) (migration 096, sorting key org_id/repo_id/author_email/day); the reference people/people_search.sql reads it with no FINAL/argMax dedup at all -- only its sibling work_item_user_metrics_daily branch already carries FINAL. identity_id defaults to author_email but is not itself part of the sorting key, so a later write correcting it for an existing key is possible: an unmerged physical version can then surface BOTH the stale and the corrected identity as separate search results for the same person -- a LIST-LENGTH divergence. This port reads BOTH UNION branches FINAL (searchPeopleQuery's own doc comment), matching quadrant/identity.go's resolvePersonIdentity for the same two tables. Go is correct. This citation's Paths reach the divergence but, by this package's own leaf-only coverage rule (BaselineDefect's doc comment), never silently admit it: a length difference stays a real, uncovered finding on the receipt whenever it fires -- the same declared shape as the filters/options citation above.",
			Paths:              []string{"data"},
			Intermittent:       true,
			IntermittentReason: "present only while user_metrics_daily holds an unmerged physical version whose identity_id changed since the last merge for some (org_id, repo_id, author_email, day) key this request's org/text filter reaches; a comparison taken after the next background merge shows no divergence",
		},
	},
}

// peopleDetailParity is shared by GET /api/v1/people/{person_id}/summary
// and /metric's own live (person_id-bound, 200) entries below. Both
// routes read repos, user_metrics_daily, work_item_user_metrics_daily and
// work_item_cycle_times -- every one a ReplacingMergeTree table the
// Python reference reads raw and this port reads FINAL (summary.go's own
// per-query doc comments; resolve.go's own identity read carries the
// identical fix peopleParity's own citation already covers for search).
// An unmerged physical version on any of them can surface either a
// LIST-LENGTH divergence (an extra stale row in a sparkline/work-mix
// list) or a single STALE VALUE (a headline/coverage/freshness field read
// from the wrong physical version) -- Paths names the whole payload
// ("data") for the same reason peopleParity's own citation does: the
// affected field or list element is not fixed to one path, it depends on
// which of the several source tables happens to hold an unmerged version
// for this specific person_id at request time.
var peopleDetailParity = Options{
	BaselineDefects: []BaselineDefect{
		{
			Ticket:             "CHAOS-5812",
			Reason:             "repos, user_metrics_daily, work_item_user_metrics_daily and work_item_cycle_times are each ReplacingMergeTree (migrations 000/096/055); build_person_summary_response and build_person_metric_response (services/people.py) read every one of them without FINAL or argMax dedup, where this port's own summary.go/metric.go/resolve.go read all four FINAL, matching the identical fix peopleParity's own citation already declares for GET /api/v1/people's search read of the same two UNION branches. An unmerged physical version can surface a stale headline/coverage/freshness value or an extra, stale element in a sparkline or work-mix list for this person_id. Go is correct. This citation's Paths reach the divergence but, by this package's own leaf-only coverage rule (BaselineDefect's doc comment), never silently admit a length or structural difference: it stays a real, uncovered finding on the receipt whenever it fires.",
			Paths:              []string{"data"},
			Intermittent:       true,
			IntermittentReason: "present only while one of the four source tables holds an unmerged physical version for THIS person_id's own rows since the last merge; a comparison taken after the next background merge shows no divergence",
		},
	},
}

// personDrilldownPRsParity is shared by GET
// /api/v1/people/{person_id}/drilldown/prs's own live (person_id-bound,
// 200) entry below.
var personDrilldownPRsParity = Options{
	BaselineDefects: []BaselineDefect{
		{
			Ticket: "CHAOS-5803",
			Reason: "git_pull_requests' created_at/merged_at/first_review_at columns are ClickHouse DateTime64(3, 'UTC') (000_raw_tables.sql); Python's clickhouse_connect driver returns them NAIVE (no tzinfo), so the Pydantic response serializes them with no offset, while Go's driver attaches UTC location and this port's PullRequestRow (encoding/json's default time.Time marshaling) emits RFC 3339 with an explicit offset -- the same class of divergence drilldown/prs' own corpus entry (drilldownPRsParity) already declares for the sibling scope-based route reading the same three columns. Go is correct.",
			Paths: []string{
				"data.items.created_at",
				"data.items.merged_at",
				"data.items.first_review_at",
			},
			Intermittent:       true,
			IntermittentReason: "present only while this request's live result actually contains at least one returned PR with a non-null value under one of these three fields; an empty items list shows no divergence under these paths",
		},
		{
			Ticket:             "CHAOS-5803",
			Reason:             "git_pull_requests and repos are both ReplacingMergeTree(last_synced) (000_raw_tables.sql, org_id added to both sorting keys by migration 027); sql/people/person_drilldown_prs.sql reads git_pull_requests with no FINAL/argMax dedup and joins repos (also unFINALed) with the org filter only in the JOIN's outer WHERE, evaluated after the merge. This port's fetchPersonPullRequestsQuery reads BOTH tables FINAL, with the org boundary resolved through an org-scoped repos subquery bound to git_pull_requests.repo_id (drilldownprs.go's own doc comment, matching internal/drilldown/prs.go's identical fix for the sibling scope-based route). An unmerged physical version on either table can surface a stale field value or drop/duplicate a row among this identity's own pull requests. Go is correct. This citation's Paths reach the divergence but, by this package's own leaf-only coverage rule, never silently admit a length or structural difference.",
			Paths:              []string{"data"},
			Intermittent:       true,
			IntermittentReason: "present only while git_pull_requests or repos holds an unmerged physical version for a PR this identity authored, since the last merge; a comparison taken after the next background merge shows no divergence",
		},
	},
}

// personDrilldownIssuesParity is shared by GET
// /api/v1/people/{person_id}/drilldown/issues's own live (person_id-bound,
// 200) entry below. Unlike personDrilldownPRsParity there is only one
// BaselineDefect here: work_item_cycle_times is already read FINAL on both
// planes for this route (sql/people/person_drilldown_issues.sql:10,
// drilldownissues.go's own doc comment), so the RMT-dedup shape
// personDrilldownPRsParity's own second entry declares has no counterpart.
var personDrilldownIssuesParity = Options{
	BaselineDefects: []BaselineDefect{
		{
			Ticket: "CHAOS-5808",
			Reason: "work_item_cycle_times' started_at/completed_at columns are ClickHouse Nullable(DateTime('UTC')) (001_metrics_v2.sql); Python's clickhouse_connect driver returns them NAIVE (no tzinfo), so the Pydantic response serializes them with no offset, while Go's driver attaches UTC location and this port's IssueRow (encoding/json's default time.Time marshaling) emits RFC 3339 with an explicit offset -- the same class of divergence drilldown/issues' own corpus entry (drilldownIssuesParity) already declares for the sibling scope-based route reading the same two columns. Go is correct.",
			Paths: []string{
				"data.items.started_at",
				"data.items.completed_at",
			},
			Intermittent:       true,
			IntermittentReason: "present only while this request's live result actually contains at least one returned issue with a non-null started_at or completed_at -- both columns are themselves Nullable, so an empty items list, or a window whose only issues have not yet started/completed, shows no divergence under these paths",
		},
	},
}

// restEndpointSpecs is the committed per-route corpus, keyed by
// routeswitch operation name.
//
// filters/options has no validation-error entry: its own package doc
// comment states the route "has no query parameters and no per-request
// branching" and its handler "wraps its whole body in a single
// try/except that maps ANY exception to 503" -- there is no caller input
// this route rejects with a 4xx at all, so a "validation-error request"
// does not exist for it to cover. Declared here rather than left silently
// absent.
//
// investment/explain's corpus is otherwise the coarsest of the eight
// routes: its success response is majority LLM-authored prose
// (investmentExplainProseFields), and its route streams a keep-alive
// prefix ahead of the final JSON value (writeKeepAliveJSON, cmd/
// query-api) -- insignificant JSON whitespace a standard decoder already
// skips, not a shape this table needs to special-case. Its
// invalid_scope_level entry below IS confirmed-live: decodeInvestmentExplain
// RequestBody routes "filters" through the same validateMetricFilter
// drilldown/prs's own POST already uses (pydantic_metric_filter.go), so
// the two routes share one validator and one error envelope, not two
// independently-guessed ones.
//
// meta has no validation-error entry: main.py's meta() takes no
// parameters at all (meta.go's own doc comment), so there is no caller
// input for it to reject.
//
// explain declares four of its five known Python-plane divergences via
// explainParity below (the display-name FINAL-dedup defect, the
// Nullable-column tuple-argMax defect, the ranking-aggregator defect and
// the blocked_work status-filter defect, all Intermittent -- their own
// Reason strings have the citation trail). Its FIFTH divergence --
// scope_filter_for_metric's own org_id omission silently dropping a
// requested repo/team scope filter for review_latency/deploy_freq/churn/
// change_failure_rate -- is declared nowhere in this table: like
// quadrant's and drilldown/prs's own uncovered non-org scope branches, it
// only fires for an explicitly repo/team-scoped request, and exercising
// it needs a real, live repo/team id from the target org's own data,
// which this corpus does not have. A default (org-scoped) request never
// reaches the branch that drops the filter, so no entry below is
// affected either way.
//
// people declares its one known Python-plane divergence via peopleParity
// below: a LIST-LENGTH shape (an extra, stale search result), the same
// declared-but-never-silently-admitted class the filters/options citation
// above establishes -- Paths names the whole payload ("data") because the
// extra element can appear anywhere in the result list, not under one
// fixed field. No validation-error entry duplicates the two already-
// captured 422 fixtures (cmd/query-api/testdata/people_422): this table's
// two 422 entries below reproduce them by request shape, not by re-typing
// a body a live comparison already gets from the real route.
//
// drilldown/issues' own started_at/completed_at naive-datetime divergence
// is declared via drilldownIssuesParity above, the same shape drilldown/
// prs's own corpus entry already cites for its created_at/merged_at/
// first_review_at fields.
//
// people/{person_id}/summary, people/{person_id}/metric,
// people/{person_id}/drilldown/prs and people/{person_id}/drilldown/issues
// each declare negative-path entries whose person_id literally resolves to
// the un-templated text "{person_id}" (never a real identity's md5 digest,
// so both planes 404 deterministically, or 422/400 ahead of identity
// resolution -- see each entry's own doc comment) PLUS one 200-path entry
// whose person_id is bound at run time to GET /api/v1/people's own live
// person_id (restidbind.go's PathParam binding, the person_id
// RESTIDBinding peopleDetailParity/personDrilldownParity's own
// BaselineDefect below names). The 200 entry is what actually reaches each
// route's own several declared FINAL-dedup fixes (user_metrics_daily,
// work_item_user_metrics_daily, work_item_cycle_times, git_pull_requests
// and repos all read raw in the reference and FINAL in this port, matching
// resolvePersonIdentity's own declared fix for the identical
// user_metrics_daily gap) -- unreachable before the id-binding mechanism
// existed, for the same reason the GraphQL corpus's own `pr` operation
// stays refused: an invented id is worse than no entry at all
// (operations.go's own `pr` doc comment).
//
// flame's corpus is REFUSED-BY-NAME only, even with id binding now
// available: every entry below is a deterministic 4xx that
// build_flame_response/internal/flame.BuildResponse answers BEFORE any
// ClickHouse call, never a live 200. The flame entity_id needs a PR
// number, a work_item_id, or a deployment_id (a repo_id:number,
// repo_id:deployment_id pair, or a bare work_item_id) -- none of which
// any producer in this table supplies yet (GET /api/v1/people produces
// person_id; GET /api/v1/filters/options produces team_id/repo_id; no
// request anywhere in restRunOrder produces a PR number, work_item_id or
// deployment_id). Until a producer for one of those exists, this table
// cannot construct a request that reaches a row this org's live data
// actually has, so it covers only the entity_type/entity_id validation
// surface and the entity-id-shape guards, which need no live id at all.
// AssertRESTPathCoverage is satisfied by these entries' PATH regardless.
var restEndpointSpecs = map[string]RESTEndpointSpec{
	"REST:GET:/api/v1/quadrant": {
		Method: "GET",
		Path:   "/api/v1/quadrant",
		Requests: []RESTRequest{
			quadrantOrgRequest("churn_throughput_org", "churn_throughput"),
			quadrantOrgRequest("cycle_throughput_org", "cycle_throughput"),
			quadrantOrgRequest("wip_throughput_org", "wip_throughput"),
			quadrantOrgRequest("review_load_latency_org", "review_load_latency"),
			{
				// team scope, scope_id bound at run time to
				// filters/options' own live team_id -- one of the
				// uncovered non-org scope branches this file's package
				// doc comment used to name as a standing gap
				// (restidbind.go closes it).
				Name:                "cycle_throughput_team_scoped",
				Query:               url.Values{"type": {"cycle_throughput"}, "scope_type": {"team"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     Options{FloatTierB: quadrantPointFloats},
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
			},
			{
				// repo scope, scope_id bound to filters/options' own
				// live repo_id -- churn_throughput specifically, to also
				// exercise the forced-repo-grain override this package's
				// own doc comment names for that type.
				Name:                "churn_throughput_repo_scoped",
				Query:               url.Values{"type": {"churn_throughput"}, "scope_type": {"repo"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     Options{FloatTierB: quadrantPointFloats},
				IDBindings: []RESTIDBinding{{Producer: "repo_id", QueryParam: "scope_id"}},
			},
			{
				// person scope, scope_id bound to people's own live
				// person_id (GET /api/v1/people's query_string_search
				// entry).
				Name:                "wip_throughput_person_scoped",
				Query:               url.Values{"type": {"wip_throughput"}, "scope_type": {"person"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     Options{FloatTierB: quadrantPointFloats},
				IDBindings: []RESTIDBinding{{Producer: "person_id", QueryParam: "scope_id"}},
			},
			{
				Name: "custom_window_org",
				Query: url.Values{
					"type":       {"churn_throughput"},
					"scope_type": {"org"},
					"bucket":     {"month"},
					"range_days": {"90"},
					"start_date": {"2026-06-01"},
					"end_date":   {"2026-09-01"},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Parity:   Options{FloatTierB: quadrantPointFloats},
			},
			{
				// quadrant.go's own package doc comment states this port
				// matches Python's behaviour exactly, so no declared
				// BaselineDefect exists for quadrant's happy path.
				// newQuadrantWorkHandler aggregates every invalid query
				// param into ONE 422, byte-shaped like Pydantic's own
				// aggregation -- a missing "type" answers the same
				// missingFieldError shape drilldown/prs and
				// investment/explain already use, so this compares the
				// full JSON body rather than status alone.
				Name:                "missing_type",
				Query:               url.Values{},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// parseISODateQueryParam replaced quadrant's own
				// parseQuadrantDate (which answered a Go-only 400 here);
				// this route now shares the exact date-error shape
				// drilldown/prs's GET already declares, so there is no
				// longer a status divergence to declare for this case.
				Name: "malformed_start_date",
				Query: url.Values{
					"type":       {"churn_throughput"},
					"start_date": {"not-a-date"},
				},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:GET:/api/v1/filters/options": {
		Method: "GET",
		Path:   "/api/v1/filters/options",
		Requests: []RESTRequest{
			{
				Name:                "options",
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Parity: Options{BaselineDefects: []BaselineDefect{{
					Ticket:             "CHAOS-5798",
					Reason:             "teams (the user_metrics_daily UNION branch), developers (author_email), repos and flow_stage (work_item_state_durations_daily) are each read from a ReplacingMergeTree table without FINAL (api/queries/filters.py) where this port's five reads all apply FINAL, bounded to the requesting org in the same statement (filteroptions package doc comment). An unmerged physical version whose VALUE changed since the last merge (a team rename, a re-attributed author_email) can leave an extra, stale distinct value in Python's list that this port's FINAL read excludes -- a LIST-LENGTH divergence, which this citation's Paths reach but, by this package's own leaf-only coverage rule (BaselineDefect's doc comment), never silently admit: it stays a real, uncovered finding on the receipt whenever it fires. Go is correct.",
					Paths:              []string{"data.teams", "data.developers", "data.repos", "data.flow_stage"},
					Intermittent:       true,
					IntermittentReason: "present only while the affected source tables hold an unmerged physical row whose value changed since the last merge; absent once merged, which is the common case",
				}}},
				// Produces team_id/repo_id from this response's OWN
				// teams/repos lists (both plain []string -- see
				// filteroptions.Response) for quadrant's team/repo-scoped
				// consumer entries and explain's own repo/team-scoped
				// entries below -- see restidbind.go.
				Produces: []RESTIDProducer{
					{Name: "team_id", ListPath: "teams"},
					{Name: "repo_id", ListPath: "repos"},
				},
			},
		},
	},
	"REST:GET:/api/v1/drilldown/prs": {
		Method: "GET",
		Path:   "/api/v1/drilldown/prs",
		Requests: []RESTRequest{
			{
				Name:                "default_window",
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownPRsParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
			},
			{
				Name:                "range_days_90",
				Query:               url.Values{"range_days": {"90"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownPRsParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
			},
			{
				// scope_type=repo, scope_id bound at run time to
				// filters/options' own live repo_id (restidbind.go) --
				// the repo-scoped branch quadrant_route.go's own sibling
				// doc comment on this table's uncovered non-org scope
				// gap named, now actually exercised.
				Name:                "repo_scoped",
				Query:               url.Values{"scope_type": {"repo"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownPRsParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
				IDBindings: []RESTIDBinding{{Producer: "repo_id", QueryParam: "scope_id"}},
			},
			{
				// Confirmed live (pydantic_validation_error.go's own doc
				// comment): a non-numeric query param produces byte-parity
				// 422 int_parsing envelopes on both planes.
				Name:                "invalid_range_days",
				Query:               url.Values{"range_days": {"not-a-number"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:POST:/api/v1/drilldown/prs": {
		Method: "POST",
		Path:   "/api/v1/drilldown/prs",
		Requests: []RESTRequest{
			{
				Name:                "default_filters",
				Body:                map[string]any{"filters": map[string]any{}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownPRsParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
			},
			{
				Name: "explicit_scope_and_sort",
				Body: map[string]any{
					"filters": map[string]any{
						"scope": map[string]any{"level": "org"},
						"time":  map[string]any{"range_days": 30},
					},
					"sort":  "created_at",
					"limit": 25,
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownPRsParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
			},
			{
				// Confirmed live (pydantic_validation_error.go): a body
				// with no "filters" key produces byte-parity 422 `missing`
				// envelopes on both planes.
				Name:                "missing_filters",
				Body:                map[string]any{},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:POST:/api/v1/investment/explain": {
		Method: "POST",
		Path:   "/api/v1/investment/explain",
		Requests: []RESTRequest{
			{
				Name:                "default_filters",
				Body:                map[string]any{"filters": map[string]any{}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Parity: Options{
					VolatileFields: investmentExplainProseFields,
					FloatTierB:     investmentExplainDeterministicFloats,
				},
			},
			{
				// decodeInvestmentExplainRequestBody (investment_explain_
				// body_validation.go) routes "filters" through the SAME
				// validateMetricFilter drilldown/prs's POST already uses
				// -- an invalid scope.level answers the shared
				// literal_error envelope byte-for-byte on both planes.
				Name: "invalid_scope_level",
				Body: map[string]any{"filters": map[string]any{
					"scope": map[string]any{"level": "not-a-real-scope-level"},
				}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:GET:/api/v1/drilldown/issues": {
		Method: "GET",
		Path:   "/api/v1/drilldown/issues",
		Requests: []RESTRequest{
			{
				Name:                "default_window",
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownIssuesParity,
			},
			{
				Name:                "range_days_90",
				Query:               url.Values{"range_days": {"90"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownIssuesParity,
			},
			{
				// Same shared int_parsing validator drilldown/prs's own
				// GET already exercises (both routes share
				// pydantic_validation_error.go unchanged).
				Name:                "invalid_range_days",
				Query:               url.Values{"range_days": {"not-a-number"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:POST:/api/v1/drilldown/issues": {
		Method: "POST",
		Path:   "/api/v1/drilldown/issues",
		Requests: []RESTRequest{
			{
				Name:                "default_filters",
				Body:                map[string]any{"filters": map[string]any{}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownIssuesParity,
			},
			{
				// This route's OWN documented asymmetry from drilldown/prs
				// (internal/drilldown/issues.go's own IssueParams doc
				// comment): scope_filter_for_metric's team-only branch list
				// means a "repo" scope level applies NO filter at all here
				// -- a real behavioural difference, not a port defect, so
				// no declaration covers it; this entry exercises exactly
				// that no-op path (an org-level default scope) rather than
				// a team scope this table has no live team id for.
				Name: "explicit_scope_and_sort",
				Body: map[string]any{
					"filters": map[string]any{
						"scope": map[string]any{"level": "org"},
						"time":  map[string]any{"range_days": 30},
					},
					"sort":  "started_at",
					"limit": 25,
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownIssuesParity,
			},
			{
				// Same shared "missing filters key" validator drilldown/
				// prs's own POST already exercises.
				Name:                "missing_filters",
				Body:                map[string]any{},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:GET:/api/v1/meta": {
		Method: "GET",
		Path:   "/api/v1/meta",
		Requests: []RESTRequest{
			{
				// meta.go: every field is either a server-level
				// ClickHouse call both planes make identically
				// (`SELECT version()` against the SAME server) or a
				// static literal ported verbatim -- no declared
				// divergence, no query params, no validation-error shape
				// to cover (main.py's meta() takes none).
				Name:                "meta",
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
			},
		},
		PublicNoAuth: true,
	},
	"REST:GET:/api/v1/explain": {
		Method: "GET",
		Path:   "/api/v1/explain",
		Requests: []RESTRequest{
			{
				// review_latency reaches every declared divergence
				// explainParity covers: it is one of the four repo-scoped
				// metrics AND one of the two Nullable-column metrics.
				Name:                "review_latency_default",
				Query:               url.Values{"metric": {"review_latency"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: explainParity,
			},
			{
				// cycle_time is the OTHER Nullable-column metric
				// explainParity's tuple-argMax entry covers, team- rather
				// than repo-scoped, exercised here with a non-default
				// range_days/compare_days pair.
				Name:                "cycle_time_range_days_90",
				Query:               url.Values{"metric": {"cycle_time"}, "range_days": {"90"}, "compare_days": {"30"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: explainParity,
			},
			// Repo/team-scoped entries for the four metrics
			// explainScopeDropDefect covers (metricconfig.go's own
			// Scope: "repo" entries), scope_id bound at run time to
			// filters/options' own live repo_id/team_id -- this table's
			// own package doc comment used to name this as explain's
			// third, undeclared divergence for want of a live id;
			// restidbind.go closes that gap.
			explainScopedRequest("review_latency_repo_scoped", "review_latency", "repo", "repo_id"),
			explainScopedRequest("review_latency_team_scoped", "review_latency", "team", "team_id"),
			explainScopedRequest("deploy_freq_repo_scoped", "deploy_freq", "repo", "repo_id"),
			explainScopedRequest("deploy_freq_team_scoped", "deploy_freq", "team", "team_id"),
			explainScopedRequest("churn_repo_scoped", "churn", "repo", "repo_id"),
			explainScopedRequest("churn_team_scoped", "churn", "team", "team_id"),
			explainScopedRequest("change_failure_rate_repo_scoped", "change_failure_rate", "repo", "repo_id"),
			explainScopedRequest("change_failure_rate_team_scoped", "change_failure_rate", "team", "team_id"),
			{
				// throughput is a sum-aggregator, team-scoped metric --
				// explainParity's ranking-aggregator entry (ranking by
				// the metric's own aggregator) can only reach a
				// divergence from a sum-aggregator metric's request,
				// which neither review_latency nor cycle_time above is.
				Name:                "throughput_default",
				Query:               url.Values{"metric": {"throughput"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: explainParity,
			},
			{
				// metric has no default (api/main.py:542) -- the same
				// missingFieldError shape quadrant's own missing_type
				// entry already exercises for a required query param.
				Name:                "missing_metric",
				Query:               url.Values{},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:POST:/api/v1/explain": {
		Method: "POST",
		Path:   "/api/v1/explain",
		Requests: []RESTRequest{
			{
				Name:                "default_filters",
				Body:                map[string]any{"metric": "review_latency", "filters": map[string]any{}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: explainParity,
			},
			{
				Name: "explicit_compare_window",
				Body: map[string]any{
					"metric": "cycle_time",
					"filters": map[string]any{
						"scope": map[string]any{"level": "org"},
						"time":  map[string]any{"range_days": 30, "compare_days": 30},
					},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: explainParity,
			},
			{
				// blocked_work is the only metric explainParity's
				// status-filter entry (status-restricted duration_hours)
				// can ever reach; also a sum-aggregator metric, so it can
				// reach the ranking-aggregator entry too.
				Name: "blocked_work_default",
				Body: map[string]any{
					"metric":  "blocked_work",
					"filters": map[string]any{},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: explainParity,
			},
			{
				// ExplainRequest requires both "metric" and "filters"
				// (neither carries a Field default); an empty body
				// aggregates both missing-field errors, the same
				// "missing filters key" validator drilldown/prs's own
				// POST already exercises for "filters" alone.
				Name:                "missing_metric_and_filters",
				Body:                map[string]any{},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:GET:/api/v1/flame": {
		Method: "GET",
		Path:   "/api/v1/flame",
		Requests: []RESTRequest{
			{
				// Both entity_type and entity_id are required (no
				// default) in main.py's flame() signature -- an empty
				// query string aggregates both missing-field errors,
				// the same shape quadrant's own missing_type entry
				// already exercises for a single required query param.
				Name:                "missing_entity_type_and_id",
				Query:               url.Values{},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// _reject_comparative_params (main.py:791, 202-208):
				// checked AFTER FastAPI's own required-field
				// validation already passed -- confirmed live for
				// /api/v1/people (people_route.go's own doc comment,
				// the same ordering this route's port reuses).
				Name: "comparative_param_rejected",
				Query: url.Values{
					"entity_type": {"issue"}, "entity_id": {"whatever"}, "rank": {"1"},
				},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// _parse_repo_entity's own missing-separator branch
				// (services/flame.py:52-55) -- deterministic, no
				// ClickHouse call, no live id needed.
				Name: "pr_entity_id_missing_repo_prefix",
				Query: url.Values{
					"entity_type": {"pr"}, "entity_id": {"not-prefixed-42"},
				},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// _parse_repo_entity's own invalid-UUID branch
				// (services/flame.py:57-58).
				Name: "pr_entity_id_invalid_repo_uuid",
				Query: url.Values{
					"entity_type": {"pr"}, "entity_id": {"not-a-uuid:42"},
				},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// build_flame_response's own `int(suffix)` guard
				// (services/flame.py:373-378), reached only after a
				// syntactically valid (but not necessarily existing)
				// repo UUID -- still deterministic, no ClickHouse call:
				// the PR-id-numeric check runs before fetch_pull_request.
				Name: "pr_entity_id_non_numeric_suffix",
				Query: url.Values{
					"entity_type": {"pr"}, "entity_id": {"00000000-0000-0000-0000-000000000000:not-a-number"},
				},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// The final fallthrough (services/flame.py:426) -- no
				// entity_type branch matches, no ClickHouse call at all.
				Name: "unknown_entity_type",
				Query: url.Values{
					"entity_type": {"not-a-real-entity-type"}, "entity_id": {"whatever"},
				},
				WantCandidateStatus: 404, WantBaselineStatus: 404,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:GET:/api/v1/people": {
		Method: "GET",
		Path:   "/api/v1/people",
		Requests: []RESTRequest{
			{
				// `if not trimmed: return []` (services/people.py) fires
				// BEFORE ClickHouse is ever touched -- deterministically
				// `[]` on both planes regardless of data state, so this
				// entry carries no Parity: there is nothing peopleParity's
				// citation could ever cover here.
				Name:                "default_empty_query",
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// A non-empty q reaches searchPeopleQuery -- the one
				// request shape peopleParity's citation actually applies
				// to.
				Name:                "query_string_search",
				Query:               url.Values{"q": {"e"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: peopleParity,
				// Produces person_id from this response's own FIRST
				// result -- the body is a bare JSON array (no wrapper
				// key: people.SearchResult's own json tags, written
				// straight through by writePeopleSearchResponse), so
				// ListPath is empty and IDField reads person_id off each
				// element. Consumed by quadrant's person-scoped entry
				// below.
				Produces: []RESTIDProducer{{Name: "person_id", IDField: "person_id"}},
			},
			{
				// Exercises boundedSearchLimit's own clamp (maxSearchLimit
				// = 50): this port applies the bound ONCE where Python
				// applies the same bound TWICE (SearchParams.Limit's own
				// doc comment) -- both are documented as equivalent for
				// every input, and this entry is what actually proves it
				// rather than merely asserting it.
				Name:                "limit_above_max_is_clamped",
				Query:               url.Values{"q": {"e"}, "limit": {"1000"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: peopleParity,
			},
			{
				// _reject_comparative_params (main.py): ANY forbidden key
				// present is a 400, {"detail": "Comparative parameters are
				// not supported."} -- confirmed live (people_route.go's
				// own writePeopleDetailError doc comment), so this compares
				// the full JSON body, not status alone.
				Name:                "comparative_param_rejected",
				Query:               url.Values{"compare_to": {"1"}},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// Captured live: cmd/query-api/testdata/people_422/
				// get_non_numeric_limit.json.
				Name:                "invalid_limit",
				Query:               url.Values{"limit": {"not-a-number"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// Captured live: cmd/query-api/testdata/people_422/
				// get_non_numeric_limit_with_comparative_param.json --
				// byte-identical to invalid_limit's own body, proving
				// FastAPI's query-parameter validation (422) runs BEFORE
				// _reject_comparative_params's handler-body check (400)
				// ever does, even with a forbidden param on the same
				// request (people_route.go's own ordering doc comment).
				Name:                "invalid_limit_with_comparative_param",
				Query:               url.Values{"limit": {"not-a-number"}, "compare_to": {"1"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:GET:/api/v1/people/{person_id}/summary": {
		Method: "GET",
		Path:   "/api/v1/people/{person_id}/summary",
		Requests: []RESTRequest{
			{
				// person_id is a PATH segment, and RESTRequest has no
				// per-request path override (doREST sends spec.Path
				// verbatim) -- so every request under this spec resolves
				// person_id to the LITERAL text "{person_id}", url-escaped
				// on the wire and decoded back to that same literal string
				// by both planes' own path routing before either ever
				// looks at it. That string can never equal a real
				// identity's md5 digest (resolve_person_identity/
				// resolvePersonIdentity's WHERE clause), so it
				// deterministically 404s on both planes -- this table's
				// no-real-id constraint (see this file's package doc
				// comment) turns INTO the negative-path proof here rather
				// than blocking it, matching main.py's own literal
				// `HTTPException(404, "Person not found")` string.
				Name:                "person_not_found",
				WantCandidateStatus: 404, WantBaselineStatus: 404,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// FastAPI/Pydantic resolves range_days at the framework
				// level before the route body (hence before identity
				// resolution) ever runs -- people_summary_route.go's own
				// doc comment on this precedence -- so this 422 fires
				// regardless of person_id, the same "validation beats
				// business logic" shape this table's people/quadrant
				// entries already establish.
				Name:                "invalid_range_days",
				Query:               url.Values{"range_days": {"not-a-number"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// person_id bound at run time to GET /api/v1/people's own
				// live person_id (restidbind.go's PathParam binding),
				// replacing the literal "{person_id}" text every OTHER
				// entry in this spec resolves to -- this is the 200 path
				// the rest of this spec's entries cannot reach at all
				// (this file's package doc comment on this spec).
				Name:                "summary_default",
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     peopleDetailParity,
				IDBindings: []RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
			},
		},
	},
	"REST:GET:/api/v1/people/{person_id}/metric": {
		Method: "GET",
		Path:   "/api/v1/people/{person_id}/metric",
		Requests: []RESTRequest{
			{
				// _metric_config(metric)/personMetricConfig(metric) both
				// run BEFORE identity resolution (build_person_metric_
				// response's own call order, metric.go's own doc comment)
				// -- so an unsupported metric answers 400 regardless of
				// person_id, the same literal-path-segment reasoning the
				// summary spec's own person_not_found entry documents.
				Name:                "unsupported_metric",
				Query:               url.Values{"metric": {"not-a-real-metric"}},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// A SUPPORTED metric reaches identity resolution, which
				// 404s for the same literal-path-segment reason the
				// summary spec's own person_not_found entry documents.
				Name:                "person_not_found",
				Query:               url.Values{"metric": {"churn"}},
				WantCandidateStatus: 404, WantBaselineStatus: 404,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// metric is a REQUIRED query param (`metric: str`, no
				// default, main.py) -- its absence is Pydantic's own
				// "missing" 422 detail, aggregated the same way this
				// table's other missing-required-field entries
				// (quadrant's missing_type) already are. Confirmed live
				// (this route's own PR TEST-EVIDENCE).
				Name:                "missing_metric",
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// person_id bound at run time to GET /api/v1/people's own
				// live person_id, same binding the summary spec's own
				// "summary_default" entry uses -- "churn" is a supported
				// metric (metricconfig.go's own personMetricConfigs), so
				// this reaches identity resolution and the 200 path.
				Name:                "metric_default",
				Query:               url.Values{"metric": {"churn"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     peopleDetailParity,
				IDBindings: []RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
			},
		},
	},
	"REST:GET:/api/v1/people/{person_id}/drilldown/prs": {
		Method: "GET",
		Path:   "/api/v1/people/{person_id}/drilldown/prs",
		Requests: []RESTRequest{
			{
				// Same literal-path-segment reasoning the summary spec's
				// own person_not_found entry documents: this table has no
				// live person_id, so every request's person_id resolves to
				// the un-templated text "{person_id}", which never equals a
				// real identity's md5 digest -- deterministically 404 on
				// both planes.
				Name:                "person_not_found",
				WantCandidateStatus: 404, WantBaselineStatus: 404,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// FastAPI/Pydantic resolves range_days at the framework
				// level before identity resolution ever runs -- same
				// precedence the summary/metric specs' own invalid_range_days
				// entries document -- so this 422 fires regardless of
				// person_id.
				Name:                "invalid_range_days",
				Query:               url.Values{"range_days": {"not-a-number"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// cursor is a `datetime | None` query param
				// (people_drilldown_prs_route.go's own
				// parseISODateTimeQueryParam/classifyDateTimeParseError,
				// confirmed live against the real Pydantic TypeAdapter) --
				// a malformed value answers 422 before identity resolution
				// too, same precedence as range_days above.
				Name:                "malformed_cursor",
				Query:               url.Values{"cursor": {"not-a-date"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// person_id bound at run time to GET /api/v1/people's own
				// live person_id, same binding the summary/metric specs'
				// own 200 entries use -- this reaches identity resolution
				// and the 200 path.
				Name:                "drilldown_prs_default",
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     personDrilldownPRsParity,
				IDBindings: []RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
			},
		},
	},
	"REST:GET:/api/v1/people/{person_id}/drilldown/issues": {
		Method: "GET",
		Path:   "/api/v1/people/{person_id}/drilldown/issues",
		Requests: []RESTRequest{
			{
				// Same literal-path-segment reasoning as drilldown/prs'
				// own person_not_found entry above.
				Name:                "person_not_found",
				WantCandidateStatus: 404, WantBaselineStatus: 404,
				BodyMode: RESTBodyModeJSON,
			},
			{
				Name:                "invalid_range_days",
				Query:               url.Values{"range_days": {"not-a-number"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// Same shared cursor validator drilldown/prs's own GET
				// already exercises (both routes share
				// pydantic_validation_error.go unchanged).
				Name:                "malformed_cursor",
				Query:               url.Values{"cursor": {"not-a-date"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// Same live person_id binding as drilldown/prs' own
				// "drilldown_prs_default" entry above.
				Name:                "drilldown_issues_default",
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     personDrilldownIssuesParity,
				IDBindings: []RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
			},
		},
	},
}

// quadrantOrgRequest builds one of quadrant's four QuadrantDefinitions
// types at the default (org) scope -- the branch every type shares, and
// the one this table can exercise without a live team/repo/person id (see
// this file's package doc comment on the corpus's known scope gap).
func quadrantOrgRequest(name, quadrantType string) RESTRequest {
	return RESTRequest{
		Name:                name,
		Query:               url.Values{"type": {quadrantType}, "scope_type": {"org"}},
		WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode: RESTBodyModeJSON,
		Parity:   Options{FloatTierB: quadrantPointFloats},
	}
}

// SpecForREST returns the committed corpus for a REST route, or an error
// naming the operation -- the REST sibling of SpecFor. A missing spec is
// a refusal, never a skip, for the same reason SpecFor's own doc comment
// gives.
func SpecForREST(operation string) (RESTEndpointSpec, error) {
	spec, ok := restEndpointSpecs[operation]
	if !ok {
		return RESTEndpointSpec{}, fmt.Errorf("goapiproof: no committed REST corpus for operation %q -- add one to restcorpus.go rather than skipping it", operation)
	}
	return spec, nil
}

// KnownRESTOperations lists every REST route this table covers, sorted.
func KnownRESTOperations() []string {
	names := make([]string, 0, len(restEndpointSpecs))
	for name := range restEndpointSpecs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// KnownRESTPaths lists the distinct paths this table covers, sorted --
// the REST corpus's coverage unit for AssertRESTPathCoverage, which
// cross-checks against a MECHANICALLY PARSED mux (no live "which REST
// operations are registered" endpoint exists the way GET /registry
// answers for GraphQL), and restendpoints.go's own doc comment already
// establishes path-level (not (method, path)) matching as this service's
// convention for exactly that reason.
func KnownRESTPaths() []string {
	seen := map[string]bool{}
	for _, spec := range restEndpointSpecs {
		seen[spec.Path] = true
	}
	paths := make([]string, 0, len(seen))
	for path := range seen {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// AssertRESTPathCoverage checks this table's paths against `mounted`, the
// paths a live query-api actually registers (migrationmatrix.
// LoadQueryAPIMuxRoutes' own output, read by the caller so this package
// need not import migrationmatrix -- see restcorpus.go's package doc
// comment on the import direction). Both directions are errors, mirroring
// AssertCoverage's own two-sided check.
func AssertRESTPathCoverage(mounted []string) error {
	mountedSet := make(map[string]bool, len(mounted))
	for _, path := range mounted {
		mountedSet[path] = true
	}
	known := KnownRESTPaths()
	knownSet := make(map[string]bool, len(known))
	for _, path := range known {
		knownSet[path] = true
	}

	var uncovered, stale []string
	for _, path := range mounted {
		if !knownSet[path] {
			uncovered = append(uncovered, path)
		}
	}
	for _, path := range known {
		if !mountedSet[path] {
			stale = append(stale, path)
		}
	}
	sort.Strings(uncovered)
	sort.Strings(stale)

	switch {
	case len(uncovered) > 0 && len(stale) > 0:
		return fmt.Errorf("goapiproof: REST corpus disagrees with the mounted mux: uncovered=%v stale=%v", uncovered, stale)
	case len(uncovered) > 0:
		return fmt.Errorf("goapiproof: query-api mounts REST paths this corpus has no entry for: %v", uncovered)
	case len(stale) > 0:
		return fmt.Errorf("goapiproof: this corpus covers REST paths query-api no longer mounts: %v", stale)
	}
	return nil
}

// ValidateRESTCorpus checks restEndpointSpecs' own internal consistency:
// every request named, every GET carrying no body, every POST/PUT/PATCH
// declaring a status pair with StatusDivergenceReason set if and only if
// the pair differs, and every declared BaselineDefect vocabulary-valid
// (validateBaselineDefects). Run by TestRESTCorpusIsValid so a future
// entry that violates one of these cannot merge silently.
func ValidateRESTCorpus() error {
	for operation, spec := range restEndpointSpecs {
		if len(spec.Requests) == 0 {
			return fmt.Errorf("goapiproof: REST corpus entry %q declares zero requests", operation)
		}
		seenNames := map[string]bool{}
		for _, req := range spec.Requests {
			if req.Name == "" {
				return fmt.Errorf("goapiproof: REST corpus entry %q has a request with no Name", operation)
			}
			if seenNames[req.Name] {
				return fmt.Errorf("goapiproof: REST corpus entry %q declares request %q twice", operation, req.Name)
			}
			seenNames[req.Name] = true
			if spec.Method == "GET" && req.Body != nil {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q is a GET carrying a body", operation, req.Name)
			}
			diverges := req.WantCandidateStatus != req.WantBaselineStatus
			hasReason := req.StatusDivergenceReason != ""
			if diverges && !hasReason {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q declares different candidate/baseline statuses (%d/%d) with no StatusDivergenceReason", operation, req.Name, req.WantCandidateStatus, req.WantBaselineStatus)
			}
			if !diverges && hasReason {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q gives a StatusDivergenceReason but declares equal candidate/baseline statuses", operation, req.Name)
			}
			if req.BodyMode != RESTBodyModeJSON && req.BodyMode != RESTBodyModeStatusOnly {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q has an unrecognised BodyMode %q", operation, req.Name, req.BodyMode)
			}
			if err := validateBaselineDefects(req.Parity.BaselineDefects); err != nil {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q: %w", operation, req.Name, err)
			}
			if (req.DedupListPath == "") != (len(req.DedupKeyFields) == 0) {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q sets DedupListPath and DedupKeyFields inconsistently -- both or neither", operation, req.Name)
			}
			if len(req.Produces) > 0 && req.BodyMode != RESTBodyModeJSON {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q declares Produces with BodyMode %q -- an id can only be extracted from a decoded JSON body", operation, req.Name, req.BodyMode)
			}
			for _, prod := range req.Produces {
				if prod.Name == "" {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q declares a Produces entry with no Name", operation, req.Name)
				}
			}
			for _, binding := range req.IDBindings {
				if binding.Producer == "" {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q declares an IDBinding with no Producer", operation, req.Name)
				}
				if (binding.QueryParam == "") == (binding.PathParam == "") {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q binds id %q with QueryParam=%q PathParam=%q -- exactly one must be set", operation, req.Name, binding.Producer, binding.QueryParam, binding.PathParam)
				}
			}
		}
	}
	if err := ValidateRESTIDBindingOrder(); err != nil {
		return err
	}
	return nil
}

// restRunOrder is go-api-rest-prove's OWN request-loop order -- it
// replaces KnownRESTOperations' alphabetical order for that one purpose,
// because alphabetical order does not, in general, place a producer
// ahead of every operation that consumes an id it Produces:
// "REST:GET:/api/v1/explain" sorts before
// "REST:GET:/api/v1/filters/options", which is exactly the wrong way
// around for explain's own repo/team-scoped entries below. Every entry
// here must appear in restEndpointSpecs exactly once --
// ValidateRESTIDBindingOrder checks that, plus that every declared
// IDBindings.Producer is Produced by a request strictly earlier in this
// order (an earlier request in the SAME operation counts, since
// restEndpointSpecs' own Requests slice order is preserved within an
// operation).
var restRunOrder = []string{
	"REST:GET:/api/v1/filters/options",
	"REST:GET:/api/v1/people",
	"REST:GET:/api/v1/people/{person_id}/summary",
	"REST:GET:/api/v1/people/{person_id}/metric",
	"REST:GET:/api/v1/people/{person_id}/drilldown/prs",
	"REST:GET:/api/v1/people/{person_id}/drilldown/issues",
	"REST:GET:/api/v1/drilldown/issues",
	"REST:POST:/api/v1/drilldown/issues",
	"REST:GET:/api/v1/drilldown/prs",
	"REST:POST:/api/v1/drilldown/prs",
	"REST:GET:/api/v1/explain",
	"REST:POST:/api/v1/explain",
	"REST:GET:/api/v1/flame",
	"REST:POST:/api/v1/investment/explain",
	"REST:GET:/api/v1/meta",
	"REST:GET:/api/v1/quadrant",
}

// RESTRunOrder returns a fresh copy of restRunOrder -- cmd/go-api-rest-
// prove's own request-loop order (see restRunOrder's own doc comment for
// why this differs from KnownRESTOperations' alphabetical order).
func RESTRunOrder() []string {
	out := make([]string, len(restRunOrder))
	copy(out, restRunOrder)
	return out
}

// ValidateRESTIDBindingOrder checks restRunOrder against restEndpointSpecs
// and every declared Produces/IDBindings pair: restRunOrder must be
// exactly a permutation of restEndpointSpecs' own keys, no id name may be
// Produced twice, and every IDBindings.Producer must already have been
// Produced by a request strictly earlier in restRunOrder (or earlier in
// the same operation's own Requests slice) -- a consumer placed ahead of
// its producer is refused here, at startup, rather than discovered as an
// always-refused request in a live run. Run by TestRESTCorpusIsValid via
// ValidateRESTCorpus, which calls this directly.
func ValidateRESTIDBindingOrder() error {
	if len(restRunOrder) != len(restEndpointSpecs) {
		return fmt.Errorf("goapiproof: restRunOrder has %d entries, restEndpointSpecs has %d -- every corpus operation must appear in the run order exactly once", len(restRunOrder), len(restEndpointSpecs))
	}
	seenOps := make(map[string]bool, len(restRunOrder))
	for _, op := range restRunOrder {
		if seenOps[op] {
			return fmt.Errorf("goapiproof: restRunOrder lists operation %q twice", op)
		}
		seenOps[op] = true
		if _, ok := restEndpointSpecs[op]; !ok {
			return fmt.Errorf("goapiproof: restRunOrder names operation %q, which restEndpointSpecs does not declare", op)
		}
	}
	for op := range restEndpointSpecs {
		if !seenOps[op] {
			return fmt.Errorf("goapiproof: restEndpointSpecs declares operation %q, which restRunOrder never lists", op)
		}
	}

	produced := map[string]string{} // producer Name -> "operation/request" that declares it
	for _, op := range restRunOrder {
		spec := restEndpointSpecs[op]
		for _, req := range spec.Requests {
			for _, binding := range req.IDBindings {
				if _, ok := produced[binding.Producer]; !ok {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q binds id %q, which no earlier request in restRunOrder Produces -- a consumer must run strictly after its producer", op, req.Name, binding.Producer)
				}
				if binding.PathParam != "" && !strings.Contains(spec.Path, "{"+binding.PathParam+"}") {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q binds id %q to PathParam %q, but %q has no {%s} placeholder", op, req.Name, binding.Producer, binding.PathParam, spec.Path, binding.PathParam)
				}
			}
			for _, prod := range req.Produces {
				if existing, dup := produced[prod.Name]; dup {
					return fmt.Errorf("goapiproof: id %q is Produced by both %s and %s/%s -- give it one producer", prod.Name, existing, op, req.Name)
				}
				produced[prod.Name] = op + "/" + req.Name
			}
		}
	}
	return nil
}
