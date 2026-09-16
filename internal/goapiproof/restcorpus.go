package goapiproof

import (
	"fmt"
	"net/url"
	"sort"
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
// people/{person_id}/summary and people/{person_id}/metric carry NO 200
// (success) entry, and consequently NO BaselineDefect citation for either
// route's own several declared FINAL-dedup fixes (their own route/package
// doc comments: user_metrics_daily, work_item_cycle_times and repos all
// read raw in the reference and FINAL in this port) -- unlike quadrant's
// type=person scope or explain's repo/team-scoped branches (both reachable
// but simply unexercised here), a 200 from EITHER of these two routes is
// only reachable through a REAL identity that resolves in the target
// org's own ClickHouse data (resolveIdentityContext's own contract: ""
// canonical means 404, full stop), which this corpus cannot know any more
// than the GraphQL corpus's own `pr` operation can know a real stored pull
// request id (operations.go's own `pr` entry: "an invented [id] is worse
// than no entry at all"). Both routes' person_id is a PATH segment with no
// per-request override in this table's own request shape (RESTRequest has
// no path field; doREST sends spec.Path verbatim) -- so, rather than
// leaving these two paths uncovered entirely (which AssertRESTPathCoverage
// would refuse), each spec below turns that same constraint into its
// negative-path coverage instead: every request's person_id literally
// resolves to the un-templated text "{person_id}", which can never equal a
// real identity's md5 digest, so it deterministically 404s (or, ahead of
// identity resolution, still validates/400s) on both planes. If a live
// person_id ever becomes available to this table (a flag, or a row read
// from the target org's own data, matching InstanceVariable's own resolution
// story for `pr`), the 200 entries and their BaselineDefect citations
// belong here, not invented now.
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
		}
	}
	return nil
}
