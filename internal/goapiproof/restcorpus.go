package goapiproof

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
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
	// RESTBodyModeCandidateShape decodes ONLY the candidate body -- never
	// the baseline's, and never a Compare between them -- and asserts it
	// is live (a non-null JSON value of the declared kind, non-empty when
	// it is an object) via AssertRESTCandidateShape. For a route whose
	// baseline answers a single, fixed, uninteresting status regardless
	// of input (see restdeletedbody.go), there is no second leg worth
	// admitting a body from, let alone comparing against; this mode
	// proves the candidate answered something real instead of proving
	// nothing at all the way RESTBodyModeStatusOnly does for the SAME
	// shape of entry.
	RESTBodyModeCandidateShape RESTBodyMode = "candidate_shape"
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

	// Timeout overrides go-api-rest-prove's own run-wide default
	// (-timeout) for BOTH legs of this one request. Zero -- the value
	// every entry that does not set this field carries -- means "use the
	// run's default"; this field exists so ONE corpus entry with a
	// legitimately slow baseline can declare its own longer budget
	// without moving the ceiling every other request in the corpus is
	// measured against. It can only ever ask for MORE time than the
	// run's default, never less by a positive value, and nothing reads
	// it to raise that default itself.
	Timeout time.Duration
	// StatusDivergenceReason states why WantCandidateStatus and
	// WantBaselineStatus are allowed to differ. Two shapes exist today:
	// a genuine ACCEPTED Go-side status-code choice documented at the
	// route's own request parser (e.g. quadrant_route.go's
	// parseQuadrantDate: Go answers 400 where Python's Pydantic answers
	// 422 for the same malformed date, and the route's own doc comment
	// calls this "a documented, Go-side-only status-code divergence; the
	// DATA contract ... is unaffected"); and a baseline-only failing
	// status, where the Python plane answers a non-2xx status for a
	// request the candidate answers with real data (e.g. the quadrant
	// route's wip_throughput entries, and the issue-drilldown routes'
	// own 503 entries below). A baseline-only failing status is always
	// paired with BodyMode: RESTBodyModeStatusOnly, since there is no
	// baseline body worth decoding. Required when the two Want values
	// differ; forbidden when they agree (ValidateRESTCorpus checks both
	// directions).
	StatusDivergenceReason string

	// BodyMode controls whether the two bodies are compared at all.
	BodyMode RESTBodyMode
	// CandidateShapeArray is meaningful only under RESTBodyModeCandidateShape:
	// true when this request's declared response is JSON-array-shaped at
	// its root (the route's Python response_model is a list[...]); false
	// (the default, and the overwhelming majority) for an object-shaped
	// root. AssertRESTCandidateShape reads this to know which JSON kind
	// -- and, for an object, non-emptiness -- counts as "live".
	CandidateShapeArray bool
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

	// PathLiterals substitutes fixed, non-secret values into the route's own
	// {placeholders} (for example the one schema version, or an id that must
	// not exist), where no earlier response produces the value and an
	// operator-supplied -bind would only restate a constant. Every key must
	// be a placeholder of the entry's Path (ValidateRESTCorpus checks it).
	PathLiterals map[string]string

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

	// BaselineTimeoutDeclared, when set, admits this request on the
	// candidate's answer alone in a run whose baseline leg produced no
	// response within its timeout -- see BaselineTimeoutDeclaration
	// (restbaselinetimeout.go) for every condition and what is written.
	// A baseline that answers leaves the request on the ordinary comparison.
	BaselineTimeoutDeclared *BaselineTimeoutDeclaration
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
	// PythonForwarder marks an endpoint the Python app can forward to
	// query-api (investment_explain_dispatcher.py for investment/explain):
	// when its switch is on, the Python app relays query-api's answer --
	// only a 200, it falls back to its own path on any other status --
	// with query-api's provenance headers dropped. A 200 baseline on such
	// an endpoint cannot be shown to be Python's own answer, so RESTAdmit
	// refuses it by name; any other baseline status is Python's own,
	// because request validation and authentication reject before the
	// handler reaches the forwarder.
	PythonForwarder bool
	// Service is the Go service the candidate leg is sent to. Empty means
	// query-api; see RESTService.
	Service RESTService
	// Credential is which bearer this entry's legs send; empty means the
	// run's own. See RESTCredentialKind.
	Credential RESTCredentialKind
}

// investmentBaselineDefects is shared by GET and POST /api/v1/investment
// (and, by inheritance, investmentSunburstBaselineDefects below -- see
// its own doc comment for why that inheritance is harmless there).
// Both read internal/investment's shared source (the same
// LATEST_WORK_UNIT_INVESTMENTS_CTE-derived query every other reader in
// internal/queryapi/analytics already composes from), which
// excludes any work unit a later regrouping run has recorded in
// work_unit_supersessions -- a table the Python query this route ports
// has no knowledge of at all. Whenever a live org holds a superseded
// work unit still inside the request's time window, Python keeps
// counting that unit's effort into theme_distribution/
// subcategory_distribution/evidence_quality_distribution/
// evidence_quality_stats; Go does not -- making candidate's own row
// population a STRICT SUBSET of baseline's, the SAME structural fact
// DictKeyDirectionShape/ScalarDirectionShape's own doc comments state.
//
// SPLIT ACROSS FIVE ENTRIES, one per aggregate, because each is a
// different JSON shape: theme_distribution/subcategory_distribution/
// evidence_quality_distribution are Go maps (response.go's own
// map[string]float64 fields) -> DictKeyDirectionShape;
// evidence_quality_stats.band_counts is the SAME map shape one level
// deeper -> its own DictKeyDirectionShape entry; evidence_quality_stats.
// total is a single scalar count -> ScalarDirectionShape.
//
// evidence_quality_stats.mean/.stddev are DELIBERATELY OUTSIDE every
// entry below, even though they sit in the SAME struct: they are
// AVERAGES, not sums or counts, and removing rows from a population
// being averaged has no guaranteed direction (a removed value below the
// mean raises it, one above lowers it) -- the same "no honest shape"
// problem investmentFlowRepoDedupParity's own chosen_mode/label/
// description note states for a threshold flip. investmentQualityStats
// Floats already declares them Tier B for the UNRELATED engine-rounding
// mechanism (this file's own doc comment above that table); a
// supersession-driven shift beyond that tolerance is knowingly left
// uncovered rather than folded into this citation by path proximity.
//
// quality_drivers is ALSO outside every entry below for the same
// reason: computeQualityStats (response.go) appends each driver string
// from an independent THRESHOLD test over totalCount/bandCounts/mean/
// stddev (e.g. "missing_evidence_metadata" when
// bandCounts["unknown"]/totalCount > 0.3) -- a threshold crossing is a
// categorical flip, not a direction or a bound, the identical problem
// chosen_mode's own drop documents on the sibling route. A real
// quality_drivers divergence surfaces as an ordinary, uncovered finding;
// that is correct and intended.
//
// NEVER OBSERVED FIRING ON A REAL CAPTURE: none of the three available
// production deployed-vs-deployed prove runs (_records/deploy-71/r78/
// step73, deploy-70/r77/step72, deploy-69/r76/step71) shows a genuine
// instance of this mechanism on GET/POST /api/v1/investment -- step73's
// only finding on this route (range_days_90) is evidence_quality_stats.
// mean's own already-Tier-B-declared rounding difference, not this
// mechanism; the other two runs show a clean match. Every shape below is
// proven only by a deliberately injected fixture fault
// (dictkeydirection_test.go, scalardirection_test.go), the same standard
// sankeyInvestmentParity's own argMax-null-skip/ConservationShape entry
// already documents. Zero firings across these three runs does not
// establish the mechanism cannot occur here -- it is equally consistent
// with these three captures simply not containing a supersession inside
// the requested window; the shapes are unproven against real data
// either way.
var investmentBaselineDefects = []BaselineDefect{
	{
		Ticket:             "CHAOS-4441",
		Reason:             "Go's read of work_unit_investments excludes any work unit a later regrouping run has recorded in work_unit_supersessions; the Python query this route ports has no knowledge of that table at all, so it can still count a superseded work unit's effort into theme_distribution -- a Go map keyed by theme name, summing row.Value per theme (response.go's own BuildResponse). Candidate's row population is a strict subset of baseline's, so a theme's own sum can only be pulled up by an excluded row's effort, never down -- direction only, no magnitude bound.",
		Paths:              []string{"data.theme_distribution"},
		Intermittent:       true,
		IntermittentReason: "present only for an org that currently holds at least one superseded work unit still inside the request's time window; an org with no supersession rows (or none inside that window) shows no divergence under this path, which is expected, not stale",
		DictKeyDirectionShape: &DictKeyDirectionShape{
			DictPath:  "data.theme_distribution",
			ValuePath: "data.theme_distribution",
		},
	},
	{
		Ticket:             "CHAOS-4441",
		Reason:             "the same work_unit_supersessions exclusion as this ticket's data.theme_distribution entry, over subcategory_distribution (the SAME per-row summation, keyed by subcategory instead of theme).",
		Paths:              []string{"data.subcategory_distribution"},
		Intermittent:       true,
		IntermittentReason: "present only for an org that currently holds at least one superseded work unit still inside the request's time window; an org with no supersession rows (or none inside that window) shows no divergence under this path, which is expected, not stale",
		DictKeyDirectionShape: &DictKeyDirectionShape{
			DictPath:  "data.subcategory_distribution",
			ValuePath: "data.subcategory_distribution",
		},
	},
	{
		Ticket:             "CHAOS-4441",
		Reason:             "the same work_unit_supersessions exclusion, over evidence_quality_distribution: response.go's own BuildResponse counts qualityStats.BandCounts per band into this map. A superseded work unit's own evidence-quality band, if it has one, can only be counted extra by baseline, never dropped by it -- direction only, no magnitude bound.",
		Paths:              []string{"data.evidence_quality_distribution"},
		Intermittent:       true,
		IntermittentReason: "present only for an org that currently holds at least one superseded work unit still inside the request's time window; an org with no supersession rows (or none inside that window) shows no divergence under this path, which is expected, not stale",
		DictKeyDirectionShape: &DictKeyDirectionShape{
			DictPath:  "data.evidence_quality_distribution",
			ValuePath: "data.evidence_quality_distribution",
		},
	},
	{
		Ticket:             "CHAOS-4441",
		Reason:             "the same work_unit_supersessions exclusion, over evidence_quality_stats.band_counts -- the SAME BandCounts map evidence_quality_distribution's own entry reads, one level deeper in the response.",
		Paths:              []string{"data.evidence_quality_stats.band_counts"},
		Intermittent:       true,
		IntermittentReason: "present only for an org that currently holds at least one superseded work unit still inside the request's time window; an org with no supersession rows (or none inside that window) shows no divergence under this path, which is expected, not stale",
		DictKeyDirectionShape: &DictKeyDirectionShape{
			DictPath:  "data.evidence_quality_stats.band_counts",
			ValuePath: "data.evidence_quality_stats.band_counts",
		},
	},
	{
		Ticket:             "CHAOS-4441",
		Reason:             "the same work_unit_supersessions exclusion, over evidence_quality_stats.total: computeQualityStats (response.go) sets it from the SAME strictly-subset row population's own count. Counting rows over a strict SUBSET of a population can only read less than or equal to the same count over the full population -- a structural fact, not an empirical skew -- so baseline is admitted only when strictly greater than candidate.",
		Paths:              []string{"data.evidence_quality_stats.total"},
		Intermittent:       true,
		IntermittentReason: "present only for an org that currently holds at least one superseded work unit still inside the request's time window; an org with no supersession rows (or none inside that window) shows no divergence under this path, which is expected, not stale",
		ScalarDirectionShape: &ScalarDirectionShape{
			Path:                  "data.evidence_quality_stats.total",
			BaselineMustBeGreater: true,
		},
	},
}

// investmentQualityStatsFloats declares evidence_quality_stats.mean/
// .stddev as Tier B: qualitystats.go's own avgIf(evidence_quality)/
// stddevPopIf(evidence_quality) query is the SAME merged, order-
// nondeterministic ClickHouse aggregate class investmentExplainDeterministicFloats
// already declares for investment/explain's own confidence.quality_mean/
// .quality_stddev -- undeclared here until now, which let a last-digit
// engine rounding difference on this route surface as a Tier-A exact
// mismatch that investmentBaselineDefects' own citation (unshaped,
// covering the whole evidence_quality_stats subtree by path proximity)
// still admitted, by the wrong mechanism rather than the right
// tolerance.
//
// This table also names data.theme_distribution/
// data.subcategory_distribution: FetchInvestmentBreakdown
// (investmentexplain/reader.go, shared with investment/explain and
// investment/sunburst) selects sum(theme_kv.2 * effort_value)/
// sum(subcategory_kv.2 * effort_value) AS value over Float64
// effort_value and Float32 theme/subcategory weights -- the identical
// genuine merged ClickHouse float aggregate class sankeyInvestmentParity's
// own data.links.value and investmentSunburstParity's own data.value
// declare for the SAME underlying query shape. Both were undeclared
// (Tier A exact) before this ticket.
var investmentQualityStatsFloats = map[string]string{
	"data.evidence_quality_stats.mean":   "avgIf(evidence_quality)-shaped ClickHouse float aggregate over the request's attributed work units",
	"data.evidence_quality_stats.stddev": "stddevPopIf(evidence_quality)-shaped ClickHouse float aggregate",
	"data.theme_distribution":            "sum(theme_kv.2 * effort_value) (investmentexplain/reader.go FetchInvestmentBreakdown) -- genuine merged ClickHouse float aggregate over Float64 effort_value/Float32 theme weight",
	"data.subcategory_distribution":      "sum(subcategory_kv.2 * effort_value) (investmentexplain/reader.go FetchInvestmentBreakdown) -- genuine merged ClickHouse float aggregate over Float64 effort_value/Float32 subcategory weight",
}

// investmentIntegerLeaves declares GET/POST /api/v1/investment's
// count-shaped numeric leaves. data.evidence_quality_distribution is Go's
// own float64(count) cast (investment/response.go BuildResponse) from an
// int band count -- not a ClickHouse aggregate at all, wire-typed
// float64 purely as a map-value convenience; total/band_counts are
// already Go int, straight from qualitystats.go's own count()/countIf().
var investmentIntegerLeaves = map[string]string{
	"data.evidence_quality_distribution":      "Go's own float64(count) cast (investment/response.go BuildResponse) from an int band count -- not a ClickHouse float aggregate, wire-typed float64 only as a map-value convenience.",
	"data.evidence_quality_stats.total":       "count() (investment/qualitystats.go) -- a bare row count.",
	"data.evidence_quality_stats.band_counts": "countIf() per quality band (investment/qualitystats.go) -- bare row counts.",
}

// investmentParity is GET and POST /api/v1/investment's own shared Parity.
var investmentParity = Options{
	BaselineDefects:       investmentBaselineDefects,
	NumericLeavesDeclared: true,
	FloatTierB:            investmentQualityStatsFloats,
	IntegerLeaves:         investmentIntegerLeaves,
}

// investmentTeamScopeDictSubsetDefects extends FOUR of
// investmentBaselineDefects' own five existing dict/scalar-direction
// citations (theme_distribution, subcategory_distribution,
// evidence_quality_distribution, evidence_quality_stats.band_counts) with
// a SIBLING declaration for the team-scope mechanism this ticket adds:
// BuildResponse (investment/response.go:186-197) splices
// teamscope.RepoCondition(orgID,"repo_id",params.ScopeIDs,...) for a
// team scope, resolving a team's repositories from team_repo_ownership,
// while the reference resolves the same scope from user_metrics_daily.
// team_id (resolve_repo_filter_ids -> resolve_repo_ids_for_teams,
// api/queries/scopes.py:72-89) -- empty for this org's real team, so the
// reference answers organization-wide while the candidate answers over
// the team's own repositories.
//
// A MATCHED key's own value shrinking (or staying equal) under this
// narrower population is ALREADY admitted by the existing sibling
// DictKeyDirectionShape entries above with no change at all:
// DictKeyDirectionShape's own claim (baseline >= candidate at a matched
// key) is a structural fact about ANY strict-subset population, not
// specific to the work_unit_supersessions mechanism those entries were
// first written for -- the SAME reasoning explainContributorsSubsetDefect/
// explainDriversSubsetDefect (restcorpus.go, above) already establish for
// reusing a mechanism-agnostic shape across two unrelated root causes.
//
// What the EXISTING entries cannot admit is a theme/subcategory/quality-
// band that exists organization-wide but has ZERO effort inside the
// team's own repositories: BuildResponse's own per-row accumulation
// (`if row.Theme != "" && row.Value > 0`, response.go) never writes a Go
// map entry for a value that never appears at all, so the WHOLE KEY is
// absent from the candidate map, not merely smaller -- a ShapePresence
// finding, which DictKeyDirectionShape's own pre-existing gate refuses
// for every declaration that never opts in (dictkeydirection.go's own
// AdmitBaselineOnlyKeys doc comment). These four sibling entries opt
// their own Paths into that admission for exactly this ticket's own
// mechanism; the four EXISTING sibling entries above are left untouched
// (AdmitBaselineOnlyKeys stays false there), so their own behaviour for
// the supersession mechanism is unchanged.
var investmentTeamScopeDictSubsetDefects = []BaselineDefect{
	{
		Ticket:                "CHAOS-5940",
		Reason:                "GET/POST /api/v1/investment's team scope resolves a team's repositories from team_repo_ownership through teamscope.RepoCondition on the candidate, user_metrics_daily.team_id (empty for this org's real team) on the reference -- the reference answers organization-wide, the candidate over the team's own repositories. A theme with zero effort inside the team's own repositories is absent from the candidate map entirely, not merely smaller (BuildResponse's own `row.Value > 0` guard, response.go). Go is correct.",
		Paths:                 []string{"data.theme_distribution"},
		Intermittent:          true,
		IntermittentReason:    "present only while the requested team's own repositories are a PROPER subset of the organization's AND at least one theme's own effort falls entirely outside that subset; a team owning every repository, or a window whose every theme touches the team's own repositories, shows no divergence under this path",
		DictKeyDirectionShape: &DictKeyDirectionShape{DictPath: "data.theme_distribution", ValuePath: "data.theme_distribution", AdmitBaselineOnlyKeys: true},
	},
	{
		Ticket:                "CHAOS-5940",
		Reason:                "the same team-repository-resolution mechanism as this ticket's data.theme_distribution entry, over subcategory_distribution. Go is correct.",
		Paths:                 []string{"data.subcategory_distribution"},
		Intermittent:          true,
		IntermittentReason:    "present only while the requested team's own repositories are a PROPER subset of the organization's AND at least one subcategory's own effort falls entirely outside that subset; a team owning every repository, or a window whose every subcategory touches the team's own repositories, shows no divergence under this path",
		DictKeyDirectionShape: &DictKeyDirectionShape{DictPath: "data.subcategory_distribution", ValuePath: "data.subcategory_distribution", AdmitBaselineOnlyKeys: true},
	},
	{
		Ticket:                "CHAOS-5940",
		Reason:                "the same team-repository-resolution mechanism as this ticket's data.theme_distribution entry, over evidence_quality_distribution: a quality band with zero attributed work inside the team's own repositories is absent from the candidate map entirely. Go is correct.",
		Paths:                 []string{"data.evidence_quality_distribution"},
		Intermittent:          true,
		IntermittentReason:    "present only while the requested team's own repositories are a PROPER subset of the organization's AND at least one quality band's own attributed work falls entirely outside that subset; a team owning every repository, or a window whose every band touches the team's own repositories, shows no divergence under this path",
		DictKeyDirectionShape: &DictKeyDirectionShape{DictPath: "data.evidence_quality_distribution", ValuePath: "data.evidence_quality_distribution", AdmitBaselineOnlyKeys: true},
	},
	{
		Ticket:                "CHAOS-5940",
		Reason:                "the same team-repository-resolution mechanism as this ticket's data.theme_distribution entry, over evidence_quality_stats.band_counts -- the SAME BandCounts map evidence_quality_distribution's own entry reads, one level deeper. Go is correct.",
		Paths:                 []string{"data.evidence_quality_stats.band_counts"},
		Intermittent:          true,
		IntermittentReason:    "present only while the requested team's own repositories are a PROPER subset of the organization's AND at least one quality band's own attributed work falls entirely outside that subset; a team owning every repository, or a window whose every band touches the team's own repositories, shows no divergence under this path",
		DictKeyDirectionShape: &DictKeyDirectionShape{DictPath: "data.evidence_quality_stats.band_counts", ValuePath: "data.evidence_quality_stats.band_counts", AdmitBaselineOnlyKeys: true},
	},
}

// investmentTeamScopeQualityStatsDefects cover the three
// evidence_quality_stats leaves investmentBaselineDefects leaves outside
// on purpose (mean, stddev, quality_drivers -- see that table's own doc
// comment), for a team-scoped request only. Under a team scope the
// baseline aggregates the whole organisation and the candidate the
// team's own repositories (investmentTeamScopeDictSubsetDefects' own
// doc comment states both resolutions), so the two legs average
// different populations and apply the driver thresholds to different
// inputs. Neither mechanism has a direction. Each entry below checks
// its leaves against the two bodies' own contents instead:
// QualityDriversRecomputeShape recomputes each leg's list from that
// leg's own counts, mean and stddev; BandMomentSubsetShape bounds each
// (mean, E[x^2]) pair jointly by each leg's own band counts and by the
// excluded population's band counts. See qualitystatsrecompute.go.
var investmentTeamScopeQualityStatsDefects = []BaselineDefect{
	{
		Ticket:             "CHAOS-5967",
		Reason:             "GET/POST /api/v1/investment's team scope resolves a team's repositories from team_repo_ownership through teamscope.RepoCondition on the candidate, user_metrics_daily.team_id (empty for this org's real team) on the reference -- the reference aggregates the whole organisation, the candidate the team's own repositories. evidence_quality_stats.mean/stddev are avgIf/stddevPopIf over those two different populations (investment/qualitystats.go, api/queries/investment.py fetch_investment_quality_stats), so they differ with no direction. The check bounds both leaves from the two bodies, jointly: for each leg and for the baseline-minus-candidate excluded population, the mean lies inside the interval its own band counts allow and E[x^2] lies between the exact least and the exact greatest E[x^2] that mean allows; stddev >= 0 and the excluded variance >= 0. Each bound allows only the float64 rounding avgIf/stddevPopIf and the check's own arithmetic can carry, derived in the variance domain: 4*gamma_(n+3) for n >= 2 (4.6e-13 at n = 1029, 7.7e-13 at n = 1732) and exactly 0 for n = 1, whose variance x*x - x*x is computed exactly; so a one-row leg must report stddev 0, and near variance 0 a stddev up to about 1.3e-6 (n = 1029) or 1.6e-6 (n = 1732) is admitted because the planes' own cancellation produces values of that size. A correct pair on a bound, such as a zero-variance excluded set, is admitted on every run. It requires unknown == 0 on both legs and candidate band counts <= baseline band counts. It does not exclude a wrong stddev that stays inside that exact range, such as a sample stddev in place of a population stddev on a large population. Go is correct.",
		Paths:              []string{"data.evidence_quality_stats.mean", "data.evidence_quality_stats.stddev"},
		Intermittent:       true,
		IntermittentReason: "present only while the requested team's own repositories are a PROPER subset of the organization's AND the work units outside them move the average; a team owning every repository leaves both populations identical.",
		BandMomentSubsetShape: &BandMomentSubsetShape{
			StatsPath:  "data.evidence_quality_stats",
			MeanPath:   "data.evidence_quality_stats.mean",
			StddevPath: "data.evidence_quality_stats.stddev",
		},
	},
	{
		Ticket:             "CHAOS-5967",
		Reason:             "the same team-repository-resolution mechanism as this ticket's mean/stddev entry, over evidence_quality_stats.quality_drivers: computeQualityStats (investment/response.go) and _compute_quality_stats (api/services/investment.py) apply the same five thresholds to each plane's own total, band_counts, mean and stddev, so different populations can cross a threshold on one plane only. Admitted only when each leg's list equals, in order, the list recomputed from that leg's own body. Go is correct.",
		Paths:              []string{"data.evidence_quality_stats.quality_drivers"},
		Intermittent:       true,
		IntermittentReason: "present only while the requested team's own repositories are a PROPER subset of the organization's AND the two populations fall on different sides of a driver threshold; a team owning every repository leaves both lists identical",
		QualityDriversRecomputeShape: &QualityDriversRecomputeShape{
			StatsPath:   "data.evidence_quality_stats",
			DriversPath: "data.evidence_quality_stats.quality_drivers",
		},
	},
}

// investmentTeamScopedParity is investmentParity plus
// investmentTeamScopeDictSubsetDefects and
// investmentTeamScopeQualityStatsDefects. evidence_quality_stats.total
// needs no new entry here: investmentBaselineDefects' own existing
// ScalarDirectionShape citation for it is a single scalar (no "whole key
// vanishes" case exists for a value with no keys at all) and already
// admits team scope's own baseline->candidate direction with no change,
// the same mechanism-agnostic reasoning as the four dict entries above.
var investmentTeamScopedParity = Options{
	BaselineDefects:       append(append(append([]BaselineDefect{}, investmentParity.BaselineDefects...), investmentTeamScopeDictSubsetDefects...), investmentTeamScopeQualityStatsDefects...),
	NumericLeavesDeclared: true,
	FloatTierB:            investmentParity.FloatTierB,
	IntegerLeaves:         investmentParity.IntegerLeaves,
}

// investmentSunburstBaselineDefects extends investmentBaselineDefects
// with the sunburst route's own additional divergence: its repo-name
// join reads `repos` (ReplacingMergeTree, sorting key org_id/id) with
// FINAL and an org_id predicate on Go's side; Python's own join carries
// neither, so an unmerged physical version of a repo row can fan a
// slice's value out by however many versions are still live for it --
// the same mechanism class already declared for sankey/heatmap's own
// repos joins.
//
// SHAPED as a direction-only claim (KeyedDirectionShape), not a
// multiplier: unlike the sankey/investment-flow graph, a sunburst row
// carries no incident-edge structure to verify a uniform k against, only
// its own (theme, subcategory, scope) key and value -- the SAME
// row-population argument DictKeyDirectionShape's own doc comment
// states (dictkeydirection.go) applies here too, just over a keyed LIST
// rather than a plain object: Python's join can only ADD extra physical
// copies of a matching repos row to a slice's sum, never remove one, so
// baseline can only be pulled UP relative to candidate at a given key,
// never down.
//
// Paths names data.value alone, not data.scope: once rows are paired by
// (theme, subcategory, scope) -- investmentSunburstOrderInsensitiveLists,
// below -- two paired elements share that SAME key by construction
// (orderInsensitiveKey builds the pairing key from exactly those three
// field values), so their own scope fields can never disagree at a
// paired element; a genuine scope difference (a rename splitting one
// repos row's contribution across two labels) always shows up as a
// missing/extra KEY instead, a structural finding no BaselineDefect
// shape ever covers (compare.go's leafDifference gate). Citing
// data.scope covered nothing real and only advertised a guard this
// citation cannot provide.
//
// NEVER OBSERVED FIRING ON A REAL CAPTURE: none of the three available
// production deployed-vs-deployed prove runs (_records/deploy-71/r78/
// step73, deploy-70/r77/step72, deploy-69/r76/step71) shows a genuine
// instance of this mechanism -- step73's only sunburst mismatch is pure
// position-shift noise (candidate[N] equals baseline[N+1] field-for-
// field), the exact ordering artifact investmentSunburstOrderInsensitive
// Lists above already resolves, not a value change; the other two runs
// show a clean match. This shape is proven here only by a deliberately
// injected fixture fault (investmentsunburst_direction_test.go), the same standard
// sankeyInvestmentParity's own argMax-null-skip/ConservationShape entry
// already documents. Zero firings across these three runs is consistent
// with two different explanations -- the mechanism cannot occur on this
// route, or it can and these three captures simply did not contain an
// unmerged repos row -- and this shape does not distinguish between
// them; it is unproven against real data either way.
//
// What this shape CANNOT catch: a real Go regression that under-counts
// a slice's value (moving the SAME direction the mechanism itself
// produces) is indistinguishable from a genuine instance from the two
// response bodies alone -- only the sign is checked, never disproved,
// the same known limit every direction-only shape in this package
// documents.
var investmentSunburstBaselineDefects = append(append([]BaselineDefect{}, investmentBaselineDefects...), BaselineDefect{
	Ticket:             "CHAOS-4773",
	Reason:             "the sunburst repo-name join reads repos with FINAL and an org_id predicate on Go's side; Python's own join has neither, so an unmerged physical version of a repo row can fan a slice's value out by however many versions are still live for it. Direction only, no magnitude bound: Python's join can only add extra physical copies to a slice's sum, never remove one, so baseline is admitted only when strictly greater than candidate at the same (theme, subcategory, scope) key.",
	Paths:              []string{"data.value"},
	Intermittent:       true,
	IntermittentReason: "present only while a repo row referenced by this org's investment data still holds 2+ unmerged physical versions; once ClickHouse merges them the two planes agree and this citation covers nothing, which is expected, not stale",
	KeyedDirectionShape: &KeyedDirectionShape{
		ListPath:   "data",
		ValueField: "value",
		ValuePath:  "data.value",
		KeyFields:  []string{"theme", "subcategory", "scope"},
	},
}, BaselineDefect{
	Ticket:             "CHAOS-5923",
	Reason:             "fetch_investment_sunburst/FetchInvestmentSunburst both GROUP BY theme, subcategory, scope, ORDER BY value DESC and LIMIT :limit identically on both planes (investmentSunburstOrderInsensitiveLists' own doc comment). The sibling repos-join fan-out entry above can move a slice's rank in that ordering without either plane's own SQL changing which (theme, subcategory, scope) keys exist, so a row can cross the LIMIT boundary on one plane only, appearing on baseline's list while the row it displaces -- whichever one now ranks just past the limit -- disappears from it. Go's list is the correct one: it is EITHER plane's own true, undoubled ranking, and Python's is inflated by the same fan-out the sibling entry already establishes is a Python-only defect.",
	Paths:              []string{"data"},
	Intermittent:       true,
	IntermittentReason: "present only while the sibling repos-join fan-out entry's own mechanism is live AND the affected repository has a row close enough to the requested limit's own boundary to cross it; a comparison with no such boundary crossing agrees on both planes",
	LimitDisplacementShape: &LimitDisplacementShape{
		ListPath:     "data",
		KeyFields:    []string{"theme", "subcategory", "scope"},
		RepoKeyField: "scope",
		ValueField:   "value",
		ValuePath:    "data.value",
	},
})

// investmentSunburstOrderInsensitiveLists declares the sunburst list's
// own row order: fetch_investment_sunburst (api/queries/investment.py)
// and FetchInvestmentSunburst (sunburst.go) both GROUP BY theme,
// subcategory, scope and ORDER BY value DESC with no secondary sort,
// IDENTICALLY on both planes. The ordering is UNCONSTRAINED IN BOTH
// PLANES' OWN SQL, so it is a property of the engine's result for a
// query with no secondary sort, not a divergence between the planes.
// It matters here specifically because investmentSunburstBaselineDefects'
// own repos-join fan-out (above) inflates one slice's value, which can
// move that slice's RANK in the value-DESC order and shift every later
// slice's own position -- a positional comparison then reports every
// field of every shifted slice as a difference, not only the one slice
// whose value actually changed, and the fan-out's own citation (Paths
// naming only scope/value) has no way to explain a shifted theme or
// subcategory. (theme, subcategory, scope) is the query's own GROUP BY
// key, unique per row on each plane's own result set, and safe to pair
// on for exactly that reason.
var investmentSunburstOrderInsensitiveLists = []OrderInsensitiveList{
	{
		Path:      "data",
		KeyFields: []string{"theme", "subcategory", "scope"},
		Reason:    "fetch_investment_sunburst and FetchInvestmentSunburst both GROUP BY theme, subcategory, scope and ORDER BY value DESC with no secondary sort, identically on both planes; the ordering is unconstrained in both planes' own SQL, not a Go-vs-Python defect",
		Ticket:    "CHAOS-5874",
	},
}

// investmentSunburstParity is GET /api/v1/investment/sunburst's own
// shared Parity.
//
// data.value (SunburstSlice.Value) is FetchInvestmentSunburst's
// own sum(subcategory_kv.2 * effort_value) (investment/sunburst.go) --
// the SAME genuine merged ClickHouse float aggregate class
// investmentQualityStatsFloats' own data.theme_distribution/
// data.subcategory_distribution declare for the identical query shape.
// Undeclared (Tier A exact) before this ticket.
//
// Its own LimitDisplacementShape entry carries no usable Limit: every
// corpus request must go through investmentSunburstParityWithLimit
// instead, which fills in THAT request's own effective limit. A caller
// reaching for this value directly gets a limit-displacement entry that
// never admits anything (Limit's zero value matches no real list
// length), which is the safe default for a Parity nothing has
// specialised yet.
var investmentSunburstParity = Options{
	BaselineDefects:       investmentSunburstBaselineDefects,
	OrderInsensitiveLists: investmentSunburstOrderInsensitiveLists,
	NumericLeavesDeclared: true,
	FloatTierB: map[string]string{
		"data.value": "sum(subcategory_kv.2 * effort_value) (investment/sunburst.go FetchInvestmentSunburst) -- genuine merged ClickHouse float aggregate.",
	},
}

// investmentSunburstDefaultLimit is investment_sunburst's own `limit:
// int = 500` default (api/main.py, the investment_sunburst route
// signature) mirrored by newInvestmentSunburstGetHandler's own `limit
// := 500` (internal/queryapi/server/investment_route.go): applies only while the
// request's own query carries no `limit` parameter at all. A PRESENT
// value (including 0 or a negative number) passes straight through
// unclamped on both planes; an invalid one is a 422 on both (this
// route's own "invalid_limit" corpus entry), never JSON-compared at
// all.
const investmentSunburstDefaultLimit = 500

// investmentSunburstParityWithLimit returns investmentSunburstParity
// with a fresh copy of its own LimitDisplacementShape entry whose Limit
// is set to THIS request's effective limit -- the value its own query
// sends, or investmentSunburstDefaultLimit when it sends none. Every
// other entry in BaselineDefects is the SAME slice element (copied by
// value, unchanged), so this only ever changes what the limit-
// displacement entry itself checks.
func investmentSunburstParityWithLimit(limit int) Options {
	opts := investmentSunburstParity
	defects := make([]BaselineDefect, len(investmentSunburstParity.BaselineDefects))
	copy(defects, investmentSunburstParity.BaselineDefects)
	for i, defect := range defects {
		if defect.LimitDisplacementShape == nil {
			continue
		}
		shape := *defect.LimitDisplacementShape
		shape.Limit = limit
		defect.LimitDisplacementShape = &shape
		defects[i] = defect
	}
	opts.BaselineDefects = defects
	return opts
}

// investmentSunburstTeamScopeSubsetDefect declares data as a bounded
// subset (membership only -- see below) for a team-scoped GET
// /api/v1/investment/sunburst request: BuildSunburstResponse
// (investment/response.go:294-298) splices teamscope.RepoCondition(orgID,
// "repo_id",params.ScopeIDs,...) for a team scope, resolving a team's
// repositories from team_repo_ownership, while the reference resolves
// the same scope from user_metrics_daily.team_id (resolve_repo_filter_ids
// -> resolve_repo_ids_for_teams, api/queries/scopes.py:72-89) -- empty
// for this org's real team, so the reference answers organization-wide
// while the candidate answers over the team's own repositories.
//
// EqualLeaves/BoundedLeaves left EMPTY (membership only), the SAME
// pattern explainContributorsSubsetDefect/explainDriversSubsetDefect
// use above: data.value at a matched (theme, subcategory, scope) key is
// ALREADY covered by this route's own two sibling entries
// (investmentSunburstBaselineDefects' own KeyedDirectionShape/
// LimitDisplacementShape, both declared for the repos-join fan-out/LIMIT-
// boundary mechanism) -- FetchInvestmentSunburst's own GROUP BY theme,
// subcategory, scope already names one specific repository per key
// (scope = r.repo, sunburst.go), so a matched key's own value is
// untouched by a narrower repo scope in the first place; this entry adds
// only the membership admission neither sibling shape reaches (both are
// gated to leaf/value findings, never a whole missing key -- compare.go's
// own shape-field gate). Go is correct.
var investmentSunburstTeamScopeSubsetDefect = BaselineDefect{
	Ticket:              "CHAOS-5940",
	Reason:              "GET /api/v1/investment/sunburst's team scope resolves a team's repositories from team_repo_ownership through teamscope.RepoCondition on the candidate, user_metrics_daily.team_id (empty for this org's real team) on the reference -- the reference answers organization-wide, the candidate over the team's own repositories. GROUP BY theme, subcategory, scope already names one specific repository per key (scope = r.repo), so a matched key's own value is untouched by a narrower repo scope; only whole keys for a repository outside the team disappear. Go is correct.",
	Paths:               []string{"data"},
	Intermittent:        true,
	IntermittentReason:  "present only while the requested team's own repositories are a PROPER subset of the organization's; a team owning every repository leaves the two lists identical",
	TeamRepoSubsetShape: &TeamRepoSubsetShape{ListPath: "data", KeyFields: []string{"theme", "subcategory", "scope"}},
}

// investmentSunburstTeamScopedParityWithLimit is
// investmentSunburstParityWithLimit plus
// investmentSunburstTeamScopeSubsetDefect.
func investmentSunburstTeamScopedParityWithLimit(limit int) Options {
	opts := investmentSunburstParityWithLimit(limit)
	opts.BaselineDefects = append(append([]BaselineDefect{}, opts.BaselineDefects...), investmentSunburstTeamScopeSubsetDefect)
	return opts
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

// investmentExplainIntegerLeaves declares investment/explain's one
// count-shaped numeric leaf: EncodeInvestmentMixExplanation
// (investmentexplain/explanation.go) writes confidence.band_mix as an
// OBJECT keyed by band name ("high", "moderate", ...), each value a
// BandCount.Count Go int -- a plain count of work units in that band,
// never a ClickHouse aggregate. Declared at the MAP's own path, like
// investmentIntegerLeaves' evidence_quality_distribution
// (restcorpus.go), because BandMix's own doc comment (explanation.go)
// states its keys are whichever bands were actually ENCOUNTERED in this
// request's work units, not a fixed enumerable set -- a per-band-name
// declaration would need updating every time a new band value appeared
// in the data, and silently under-declare until then.
var investmentExplainIntegerLeaves = map[string]string{
	"data.confidence.band_mix": "BandCount.Count (investmentexplain/explanation.go), one entry per evidence-quality band this request's work units actually reached -- a plain Go int count, never a ClickHouse aggregate.",
}

// quadrantPointFloats declares quadrant.Point's x/y as Tier B: both are
// wrapped in toFloat64(...) over a ClickHouse aggregate expression
// (quadrant.go's own package doc comment: "every value_expr is wrapped in
// toFloat64(...)"), and the underlying aggregate is the same class of
// merged, order-nondeterministic floating-point value every sibling
// FloatTierB table in this service already declares. data.points.
// trajectory.x/.y carry the SAME per-window value data.points.x/.y expose
// as the latest window only -- one tolerance covering two paths, not a
// second one.
var quadrantPointFloats = map[string]string{
	"data.points.x":            "quadrant.go's toFloat64(...)-wrapped ClickHouse aggregate expression",
	"data.points.y":            "quadrant.go's toFloat64(...)-wrapped ClickHouse aggregate expression",
	"data.points.trajectory.x": "the same toFloat64(...)-wrapped ClickHouse aggregate value as data.points.x -- trajectory carries the identical per-window value, data.points.x is just its latest window",
	"data.points.trajectory.y": "the same toFloat64(...)-wrapped ClickHouse aggregate value as data.points.y -- trajectory carries the identical per-window value, data.points.y is just its latest window",
}

// cycleBreakdownFloats declares the three fixed leaf-depths
// buildCycleBreakdownTree's root/category/status-leaf shape can ever
// produce as Tier B -- see the flame/aggregated spec's own doc comment
// for why this mode (unlike code_hotspots) both needs a declaration
// (genuine Float64 sum(duration_hours) merge) and can express it exactly
// (a FIXED two-level tree, never deeper).
var cycleBreakdownFloats = map[string]string{
	"data.root.value":                   "sum(duration_hours)-shaped ClickHouse float aggregate, merged across category nodes",
	"data.root.children.value":          "sum(duration_hours)-shaped ClickHouse float aggregate, merged across a category's status rows",
	"data.root.children.children.value": "sum(duration_hours)-shaped ClickHouse float aggregate for one status row",
}

// aggFlameThroughputInts declares throughput mode's own three fixed leaf
// depths as integer: buildThroughputTree (aggflame.go) is root -> work
// type -> team, exactly two levels deep like cycle_breakdown's own tree
// (never deeper -- the team level's own Children is always []Node{}),
// and every value at every depth is uniqExact(wct.work_item_id) (an exact
// distinct count, clickhouse.go) summed in Go (typeValue/rootValue) --
// integer arithmetic throughout, never a merged float aggregate.
var aggFlameThroughputInts = map[string]string{
	"data.root.value":                   "sum of uniqExact(work_item_id) across every work-type node -- an exact count sum",
	"data.root.children.value":          "sum of uniqExact(work_item_id) across a work type's own team rows -- an exact count sum",
	"data.root.children.children.value": "uniqExact(work_item_id) for one team's own row -- an exact distinct count",
}

var aggFlameThroughputParity = Options{
	NumericLeavesDeclared: true,
	IntegerLeaves:         aggFlameThroughputInts,
}

// drilldownPRsIntegerLeaves declares this route's two numeric leaves,
// both integer domain despite Go's own wire typing choice for one of
// them: data.items.number is a plain PR number column; data.items.
// review_latency_hours is `dateDiff('hour', created_at, first_review_at)`
// (drilldown.py, drilldown/prs.go) -- always a whole number of hours on
// both planes. This port types it *int64 (prs.go); Python's Pydantic
// model types the SAME quantity `float`, so the wire emits "5" on one
// plane and "5.0" on the other -- a WIRE-SHAPE difference the JSON number
// comparator already treats as equal, never a provenance one: dateDiff
// performs no cross-row arithmetic either plane's merge order could
// perturb, so this is integer, not float.
var drilldownPRsIntegerLeaves = map[string]string{
	"data.items.number":               "a plain PR number column, never aggregated.",
	"data.items.review_latency_hours": "dateDiff('hour', created_at, first_review_at) -- always a whole number of hours; Go types it int64 where Python's pydantic model types the same quantity float, a wire-shape difference, not a provenance one.",
}

// pullRequestRowCopyRule names the git_pull_requests response fields a
// later write of the same (repo_id, number) can store a different value
// for, read off every writer of the table, and gives none of them a
// direction. title is written verbatim from the upstream's current title
// on every provider sync (internal/providersync normalizeGitHubPullRequest
// and its GitLab counterpart), so an upstream edit rewrites it either way.
// Every writer refuses a null over a held merged_at or first_review_at
// (the provider sync's guardPullRequestMergedAtRegressions and
// guardPullRequestReviewRegressions, the stream-ingest writers' terminal
// columns in internal/streamhandlers stored_version.go), but each guard
// reads the held version and inserts in two statements with no lock, a
// physical version written before those guards stays until the
// background merge, and first_review_at is recomputed from the reviews
// each sync fetches; so neither gets a direction. review_latency_hours
// is a pure function of first_review_at and the shared created_at. The
// author (author on the person route, author_name on the scope route)
// is the upstream login written on every sync, "Unknown" when the
// upstream carries none, so a renamed or vanished login rewrites it. Every other response field is
// left out: repo_id and number are the key; created_at is the route's
// sort key, and a copy pair that differs in it (the writer's sync-time
// fallback for a pull request with no upstream created_at) stays outside;
// link is null on both planes.
var pullRequestRowCopyRule = DuplicateCopyRule{
	RewrittenFields: []string{"title", "author", "author_name", "merged_at", "first_review_at", "review_latency_hours"},
}

// pullRequestAccounting is the pull-request drilldown family's shared
// candidate-side check (CandidateAccounting): every candidate row equals
// one whole baseline copy under pullRequestRowCopyRule, the candidate is
// ordered created_at DESC as both routes' ORDER BY, and only a page-cut
// baseline leaves candidate rows beyond its reach. limit is the request's
// own effective limit; parityWithPageCutLimit sets it per request.
func pullRequestAccounting(limit int) *CandidateAccounting {
	return &CandidateAccounting{ListPath: "data.items", IDField: RESTDedupKeyField, SortField: "created_at", CopyRule: &pullRequestRowCopyRule, PageLimit: limit}
}

// pullRequestListPaths are the response paths a pull-request drilldown
// declaration can admit a finding under that depend on which rows the
// list carries: the list itself and the cursor copied from its last row.
var pullRequestListPaths = []string{"data.items", "data.next_cursor"}

// reachesPath reports whether a cited path covers, or is covered by, one
// of paths: a citation of the list, of a leaf inside it, or of an
// ancestor of it.
func reachesPath(cited string, paths []string) bool {
	for _, path := range paths {
		if cited == path || strings.HasPrefix(cited, path+".") || strings.HasPrefix(path, cited+".") {
			return true
		}
	}
	return false
}

// gatePullRequestDefects is the pull-request drilldown family's Options
// builder step that makes the candidate accounting structural: every
// declaration whose Paths reach the list or its cursor carries
// pullRequestAccounting, whatever its shape, so no declaration of the
// family admits under the list without it. A declaration whose mechanism
// is a narrower candidate population (TeamRepoSubsetShape) permits
// dropped rows and nothing else.
func gatePullRequestDefects(defects []BaselineDefect, limit int) []BaselineDefect {
	gated := make([]BaselineDefect, len(defects))
	for i, defect := range defects {
		for _, cited := range defect.Paths {
			if reachesPath(cited, pullRequestListPaths) {
				accounting := pullRequestAccounting(limit)
				accounting.AllowDropped = defect.TeamRepoSubsetShape != nil
				defect.Accounting = accounting
				break
			}
		}
		gated[i] = defect
	}
	return gated
}

// pullRequestDrilldownRoute carries the only declaration inputs that
// differ between the pull-request drilldown routes.
type pullRequestDrilldownRoute struct {
	// cursorPath is the response's own next-page cursor leaf, the
	// created_at of the page's last row, or empty for a route with none.
	cursorPath string
}

// pullRequestDrilldownDefects is the one definition of the reference-plane
// defects every pull-request drilldown route declares: GET/POST
// /api/v1/drilldown/prs (fetch_pull_requests, api/queries/drilldown.py;
// internal/drilldown/prs.go) and GET /api/v1/people/{person_id}/
// drilldown/prs (fetch_person_pull_requests, sql/people/
// person_drilldown_prs.sql; internal/queryapi/people/
// drilldownprs.go). Both reference readers select the same
// git_pull_requests row fields through an INNER JOIN on repos with no
// FINAL on either table, ORDER BY created_at DESC LIMIT; both ports read
// both tables FINAL. A mechanism declared for one route is therefore a
// mechanism the other can hit, and neither route's Parity lists these
// entries by hand.
func pullRequestDrilldownDefects(route pullRequestDrilldownRoute) []BaselineDefect {
	datetimePaths := []string{
		"data.items.created_at",
		"data.items.merged_at",
		"data.items.first_review_at",
	}
	pageCutPaths := []string{"data.items"}
	if route.cursorPath != "" {
		datetimePaths = append(datetimePaths, route.cursorPath)
		pageCutPaths = append(pageCutPaths, route.cursorPath)
	}
	return gatePullRequestDefects([]BaselineDefect{
		{
			Ticket:                  "CHAOS-5803",
			Reason:                  "git_pull_requests' created_at/merged_at/first_review_at columns are ClickHouse DateTime64(3, 'UTC') (000_raw_tables.sql); Python's clickhouse_connect driver returns them NAIVE (no tzinfo), so the Pydantic response serializes them with no offset -- and so does a next-page cursor copied from the last row's created_at -- while Go's driver attaches UTC location and each port's row type (encoding/json's default time.Time marshaling) emits RFC 3339 with an explicit offset -- the same class of divergence already declared for the operations.go GraphQL corpus's own capacityForecasts and pr entries. Go is correct. The same naive-vs-aware rendering recurs on GET /api/v1/people/{person_id}/summary's deltas spark series ts field, reading a ClickHouse Date column through a different query path and declared separately, in peopleSummarySparkTimestampDefect below.",
			Paths:                   datetimePaths,
			TimestampRenderingShape: &TimestampRenderingShape{},
			Intermittent:            true,
			IntermittentReason:      "present only while this request's live result actually contains at least one returned PR with a non-null value under one of these fields; an empty items list, or a window whose only PRs happen to leave every one of these fields null, shows no divergence under these paths and is not a bug in this citation -- the same live-data-dependence every other Intermittent entry in this table declares, just triggered by result content here rather than by ClickHouse's own merge state",
		},
		{
			Ticket:             "CHAOS-5803",
			Reason:             "git_pull_requests and repos are both ReplacingMergeTree(last_synced) (000_raw_tables.sql); the reference readers (fetch_pull_requests in api/queries/drilldown.py, fetch_person_pull_requests over sql/people/person_drilldown_prs.sql) read neither with FINAL or any other merge-time dedup, so an unmerged physical version of the same logical PR ((repo_id, number)) can surface as two rows sharing that identity, spending one extra slot of the page limit and shifting every later element's position -- the same mechanism WorkGraphEdgeDedupShape already covers for the GraphQL workGraphEdges operation. Both ports read both tables FINAL. Go is correct.",
			Paths:              []string{"data.items"},
			Intermittent:       true,
			IntermittentReason: "present only while the source tables hold an unmerged physical version of some PR or repo row; a comparison taken after the next background merge shows no repeated (repo_id, number) on the baseline side either",
			WorkGraphEdgeDedupShape: &WorkGraphEdgeDedupShape{
				EdgesListPath:      "data.items",
				IDField:            RESTDedupKeyField,
				TrailingCursorPath: route.cursorPath,
				// The page's last id can have copies past the limit; its
				// on-page copies are judged by pullRequestRowCopyRule's
				// fields (pageboundarycopy.go).
				PageBoundary: &PageBoundary{SortField: "created_at", Limit: drilldownPRsDefaultLimit, CopyRule: &pullRequestRowCopyRule},
			},
		},
		{
			Ticket:             "CHAOS-5897",
			Reason:             "Mechanism: git_pull_requests is ReplacingMergeTree(last_synced) keyed by (org_id, repo_id, number), and the reference readers read it without FINAL, so while an older physical version of a pull request is unmerged the baseline page carries both an older copy and a newer copy of the same (repo_id, number), differing on whichever fields a later write stored differently. pullRequestRowCopyRule names those fields from every writer of the table -- title, the author login, merged_at, first_review_at and review_latency_hours (CHAOS-6001) -- and gives none of them a direction: a provider sync rewrites title and the author login from upstream; every writer refuses a null over a held merged_at or first_review_at, but each guard reads the held version and inserts in two statements with no lock, a version written before those guards stays until the background merge, and first_review_at is recomputed from the reviews each sync fetches. A FINAL read returns one whole physical version, and the unFINALed read returns every version still unmerged, the kept one included, so the candidate row must equal one whole baseline copy field for field. Scope: data.items only; admits an id whose baseline copies disagree only when every copy carries the candidate's key set, every field outside those agrees across the copies, and the candidate row is present and equal to one copy. A candidate value that no copy carries, a candidate mixing fields of two copies, a disagreement on any other field, or an id absent from the candidate stays uncovered. Blind spot: which copy is newest is the table's last_synced, which neither body carries, so a Go read that served an older physical version is not told apart from one that served the newest.",
			Paths:              []string{"data.items"},
			Intermittent:       true,
			IntermittentReason: "present only while git_pull_requests holds an unmerged older physical version of a pull request that a later write stored with a different title, merged_at or first_review_at, and that pull request falls within the requested page; after the background merge the baseline carries one copy per pull request and this entry has nothing to admit",
			WorkGraphEdgeDedupShape: &WorkGraphEdgeDedupShape{
				EdgesListPath:      "data.items",
				IDField:            RESTDedupKeyField,
				TrailingCursorPath: route.cursorPath,
				RewrittenFields:    pullRequestRowCopyRule.RewrittenFields,
			},
		},
		{
			Ticket:             "CHAOS-5959",
			Reason:             "the same repos-join fan-out mechanism as this table's own CHAOS-5803 duplicate-row entry above, over data.items' own LENGTH: a physically-duplicated (repo_id, number) row spends an extra slot of the page limit, so the reference plane's own items list carries more elements than the candidate's for the SAME logical set of pull requests. Copies of one id are judged by pullRequestRowCopyRule, the same rule the CHAOS-5897 entry above applies per id. Confirmed against real production captures: on GET /api/v1/drilldown/prs team_scoped, every one of 6 duplicated ids in a 42-item baseline page was exactly 2 byte-identical copies, all for the same physically-duplicated repository, and collapsing them id for id reproduced the candidate's own 36-item page exactly, content included; on GET /api/v1/people/{person_id}/drilldown/prs, 9 pull requests arrived as 18 rows (each 2 or 4 copies, git_pull_requests and repos both unmerged), one pull request's two physical versions carrying different titles (CHAOS-6001), and the candidate's 9 rows each equal one copy. Go is correct.",
			Paths:              []string{"data.items"},
			Intermittent:       true,
			IntermittentReason: "present only while git_pull_requests or repos holds an unmerged physical version that duplicates a returned pull request's own identity within the requested page; a comparison taken after the next background merge shows no length divergence",
			DuplicateCollapseLengthShape: &DuplicateCollapseLengthShape{
				ListPath: "data.items",
				IDField:  RESTDedupKeyField,
				CopyRule: &pullRequestRowCopyRule,
			},
		},
		{
			Ticket:             "CHAOS-5968",
			Reason:             "the same repos-join fan-out mechanism as this table's own CHAOS-5959 length entry above, over the case CHAOS-5959's own exact-collapse rule cannot reach: the reference readers' own INNER JOIN repos reads repos WITHOUT FINAL, so while one or more repos rows sit unmerged (ReplacingMergeTree(last_synced), any repo a sync has recently written to, not confined to one particular repo or a fixed cadence) the join doubles every pull request belonging to one of those repos; both routes' ORDER BY created_at DESC LIMIT means each doubled row spends one extra slot a genuinely distinct, later-ranked pull request would otherwise have occupied, so the candidate page -- reading the same table FINAL, never spending a slot on a duplicate -- reaches further and lists distinct ids the baseline's own page never got to. Copies of one id are judged by pullRequestRowCopyRule, the same rule the CHAOS-5897 entry above applies per id. Confirmed against two real production captures (team_scoped), both outside any single fixed sync cadence: a PARTIAL capture, baseline 50 items at the route's own limit, 28 distinct ids, 22 of them duplicated exactly 2x each, byte-identical, candidate 36 items with 8 candidate-only ids in its own tail (the true, deduplicated population, 36, itself under the route's own limit, 50); and an ALL-DOUBLED capture, baseline 50 items, 25 distinct ids ACROSS TWO repos, every one of the 25 duplicated exactly 2x each, byte-identical (the window's own population entirely covered by repos with an unmerged repos row at capture time, not merely some of it), candidate 36 items with dedup(baseline)'s own 25 ids as its own literal prefix and 11 candidate-only ids in the tail. No property of a candidate-only tail row is a content check -- uniqueness and ordering are the only structural properties available for a row the baseline page never reached, never a check on whether that row's own id or timestamp is correct. On a route whose response carries a next-page cursor copied from the page's last row's created_at, the same spent slots move that last row: the reference pages over undeduplicated copies, so its page ends earlier and its cursor names a later instant than the candidate's. Confirmed against a real production capture of GET /api/v1/people/{person_id}/drilldown/prs: baseline 50 rows carrying 37 distinct pull requests (13 of them as two physical copies), candidate 50 distinct rows with dedup(baseline) as its literal prefix, each cursor equal to its own leg's last created_at, the candidate's the earlier. A cursor difference is admitted only as that computed consequence: every page-cut rule holds, each leg's cursor names its own last row's created_at instant, and the candidate's is not later than the baseline's. Go is correct.",
			Paths:              pageCutPaths,
			Intermittent:       true,
			IntermittentReason: "present only while the baseline page is both truncated at the route's own limit AND holds an unmerged physical version that duplicates a returned pull request's own identity; a comparison taken after the next background merge, or one whose baseline page never reaches the limit at all, shows no length or cursor divergence for this entry",
			DuplicateCollapsePageCutShape: &DuplicateCollapsePageCutShape{
				ListPath:  "data.items",
				IDField:   RESTDedupKeyField,
				SortField: "created_at",
				Limit:     drilldownPRsDefaultLimit,
				CopyRule:  &pullRequestRowCopyRule,
				// Empty on a route with no cursor.
				CursorPath: route.cursorPath,
			},
		},
	}, drilldownPRsDefaultLimit)
}

// drilldownPRsParity is shared by every admissible (2xx) drilldown/prs
// request, GET and POST alike: both routes call the same
// BuildPRsResponse, so both carry the same declared Python-plane
// defects.
var drilldownPRsParity = Options{
	NumericLeavesDeclared: true,
	IntegerLeaves:         drilldownPRsIntegerLeaves,
	BaselineDefects:       pullRequestDrilldownDefects(pullRequestDrilldownRoute{}),
}

// drilldownPRsDefaultLimit is fetchPullRequestsQuery's own page size when
// a request sends no `limit` at all -- drilldown.py's own `limit: int =
// 50` keyword default (api/queries/drilldown.py) and drilldown/prs.go's
// own doc comment stating the identical GET-hardcoded/POST-`or 50`
// resolution (PRParams.Limit's own doc comment, prs.go:33-38). GET never
// accepts a `limit` query param at all, so every GET request in this
// corpus reaches this same value; POST's own "explicit_scope_and_sort"
// entry is the one request in this corpus that sends a different value,
// and reaches drilldownPRsParityWithLimit instead.
const drilldownPRsDefaultLimit = 50

// drilldownPRsParityWithLimit returns drilldownPRsParity with its page-cut
// entry's Limit set to THIS request's effective limit -- see
// parityWithPageCutLimit.
func drilldownPRsParityWithLimit(limit int) Options {
	return parityWithPageCutLimit(drilldownPRsParity, limit)
}

// parityWithPageCutLimit returns opts with every page limit set to one
// request's own effective limit: a DuplicateCollapsePageCutShape's Limit,
// a WorkGraphEdgeDedupShape's PageBoundary.Limit
// and every declaration's CandidateAccounting.PageLimit -- the same
// convention investmentSunburstParityWithLimit already establishes for
// the identical class of per-request LIMIT field. Every other field of
// every entry is copied unchanged.
func parityWithPageCutLimit(opts Options, limit int) Options {
	defects := make([]BaselineDefect, len(opts.BaselineDefects))
	copy(defects, opts.BaselineDefects)
	for i, defect := range defects {
		if defect.DuplicateCollapsePageCutShape != nil {
			shape := *defect.DuplicateCollapsePageCutShape
			shape.Limit = limit
			defect.DuplicateCollapsePageCutShape = &shape
		}
		if defect.WorkGraphEdgeDedupShape != nil && defect.WorkGraphEdgeDedupShape.PageBoundary != nil {
			shape := *defect.WorkGraphEdgeDedupShape
			boundary := *shape.PageBoundary
			boundary.Limit = limit
			shape.PageBoundary = &boundary
			defect.WorkGraphEdgeDedupShape = &shape
		}
		if defect.Accounting != nil {
			accounting := *defect.Accounting
			accounting.PageLimit = limit
			defect.Accounting = &accounting
		}
		defects[i] = defect
	}
	opts.BaselineDefects = defects
	return opts
}

// drilldownPRsDedup is every admissible drilldown/prs request's dedup
// declaration -- see InjectRESTDedupKeys and drilldownPRsParity's own
// WorkGraphEdgeDedupShape entry above.
var drilldownPRsDedup = struct {
	ListPath  string
	KeyFields []string
}{ListPath: "items", KeyFields: []string{"repo_id", "number"}}

// drilldownPRsTeamScopeSubsetDefect declares data.items as a bounded
// subset for a team-scoped GET/POST /api/v1/drilldown/prs request:
// BuildPRsResponse (drilldown/prs.go:164-182) splices
// teamscope.RepoCondition(orgID,"pr.repo_id",params.ScopeIDs,...) for a
// team scope, the same shared team_repo_ownership condition every
// repo-keyed route in this corpus reads; the reference resolves the
// identical scope through resolve_repo_filter_ids (metric_scope="repo"
// hardcoded at both GET/POST call sites, api/main.py:918-919,956-957) ->
// resolve_repo_ids_for_teams (api/queries/scopes.py:72-89) -- empty for
// this org's real team, so the reference answers organization-wide while
// the candidate answers over the team's own repositories.
//
// RANK-TRUNCATION GUARD: fetchPullRequestsQuery's own ORDER BY
// created_at DESC LIMIT 50 (drilldown/prs.go:134-155) means a
// team-scoped top-50 slice and an org-scoped top-50 slice are only
// safely comparable as a subset when the ORG-WIDE row count for the
// requested window stays under the limit on BOTH legs -- otherwise a row
// present in the team's own top 50 could be crowded out of the org's own
// top 50 by a non-team repository's more recent PR, an ordinary
// consequence of the LIMIT boundary, not a real divergence, and
// indistinguishable from one by this shape alone (teamreposubset.go's
// own rule 3: a single candidate-only key refuses the WHOLE plan).
// Verified against live production counts (read-only, FINAL) rather than
// assumed: every already-captured window (the default window, 90 days,
// a single repository) saturates at exactly 50/50 on both legs, and
// range_days alone never drops the org-wide count below 50 within the
// first two weeks. The team-scoped case below instead binds a FIXED
// three-day window (2026-08-15 through 2026-08-17, half-open at
// 2026-08-18) that a live count confirms carries 36 organization-wide
// rows, none of them near the boundary.
//
// EqualLeaves, not bounded: every field besides the (repo_id, number)
// key is an intrinsic, per-PR property this route's own assembly reads
// verbatim (fetchPullRequestsQuery's own SELECT, never a cross-PR
// aggregate a narrower repository set could resum), the same "no
// per-unit sum-type leaf a narrower scope could legitimately shrink"
// reasoning workUnitsTeamScopeSubsetDefect's own doc comment (above)
// already establishes for the identical class of route. created_at/
// merged_at/first_review_at are DELIBERATELY left unnamed here: those
// three already carry their own unshaped, always-live citation for the
// naive-vs-aware wire-format rendering (drilldownPRsParity's own first
// entry, above), and naming them in EqualLeaves here would test raw
// string equality against a wire form that citation already knows can
// legitimately differ -- poisoning this shape's own admission on every
// comparison that entry alone would have covered cleanly.
//
// On the window this ticket verified, the team owns every repository
// with any pull request at all inside it: team_prs equals org_prs for
// every day recorded, so the "subset" this run actually observes is the
// FULL set, not a proper one -- this shape's own rules 1-4 hold
// unconditionally for an equal population (nothing is missing, nothing
// needs bounding), so it is admitted the same way a genuine narrowing
// would be, but this run's own IntermittentReason states plainly that it
// does not itself demonstrate narrowing on this data.
var drilldownPRsTeamScopeSubsetDefect = BaselineDefect{
	Ticket:             "CHAOS-5940",
	Reason:             "GET/POST /api/v1/drilldown/prs' team scope resolves a team's repositories from team_repo_ownership through teamscope.RepoCondition on the candidate, user_metrics_daily.team_id (empty for this org's real team) on the reference -- the reference answers organization-wide, the candidate over the team's own repositories. Every field besides (repo_id, number) is a per-PR property read verbatim, never resummed across a repository set. Go is correct.",
	Paths:              []string{"data.items"},
	Intermittent:       true,
	IntermittentReason: "present only while the requested window's own organization-wide row count stays under fetchPullRequestsQuery's own LIMIT 50 on both legs (verified for the one fixed window this corpus binds; not assumed to hold for range_days or an unverified window) AND the requested team's own repositories are a PROPER subset of the organization's; on the verified window the team happens to own every repository with a pull request at all, so this citation is exercised as an EQUAL-population case, never observed narrowing a real list on this org's own data",
	TeamRepoSubsetShape: &TeamRepoSubsetShape{
		ListPath:    "data.items",
		KeyFields:   []string{"repo_id", "number"},
		EqualLeaves: []string{"title", "author_name", "review_latency_hours"},
	},
}

// drilldownPRsTeamScopeUncutDefect names the ONE further consequence of
// the SAME repos-join duplicate-row mechanism this table's own CHAOS-
// 5959/CHAOS-5968 entries already declare, over the case neither of
// them content-verifies: a candidate-only tail row's own field, id
// included (CHAOS-5968's own doc comment states why -- no baseline
// value for it exists to check against once the page is genuinely cut).
// GET carries no limit parameter at all -- its own effective limit is
// fixed at 50, on both planes, immovable by any request. POST's own
// limit has no ceiling on either plane. So a request whose effective
// limit is high enough, or whose window is short enough, can keep the
// true team population -- doubled by the fan-out or not -- entirely
// under that limit; when it does, collapsing baseline's own duplicate
// rows content-verifies every field of every row, byte for byte, under
// the SAME exact-collapse rule CHAOS-5959 already applies to the
// unbounded case. RequestLimit's own precondition (DuplicateCollapse
// LengthShape's own doc comment) refuses this entry outright, rather
// than admit a match, whenever the baseline's own raw length reaches
// the request's own effective limit -- that reach is the one signal
// available from the response bodies alone that candidate's own leg
// might be independently truncated at the identical boundary, which
// would make an exact-collapse match prove only that the two leading
// pages agree, never that the whole population was compared.
var drilldownPRsTeamScopeUncutDefect = BaselineDefect{
	Ticket:             "CHAOS-5988",
	Reason:             "the same repos-join fan-out mechanism this table's own CHAOS-5959/CHAOS-5968 entries declare, over the request shapes -- an explicit high limit, or a short enough window -- that keep a team's own true population strictly under the request's own effective limit even fully doubled: GET's own limit is fixed at 50 on both planes (no limit parameter exists at all); POST's own limit has no ceiling on either plane. When the true population fits, every field of every row is content-verified by the exact-collapse rule, id included -- the one property CHAOS-5968's own page-cut case cannot ever check. A baseline whose own raw length reaches the request's own effective limit goes idle under this entry instead, never falling back to CHAOS-5968's own admission: reaching the limit is the one signal available from the response bodies alone that candidate's own leg might share the identical truncation boundary, which would make an exact-collapse match prove only that the two leading pages agree. Go is correct.",
	Paths:              []string{"data.items"},
	Intermittent:       true,
	IntermittentReason: "present only while the requested team's own true population, doubled by an in-flight repos-join fan-out or not, fits strictly under the request's own effective limit; a baseline whose own raw length reaches that limit goes idle under this entry (\"page possibly cut\") rather than risk admitting a match neither leg's own truncation boundary can rule out",
	DuplicateCollapseLengthShape: &DuplicateCollapseLengthShape{
		ListPath:     "data.items",
		IDField:      RESTDedupKeyField,
		RequestLimit: drilldownPRsDefaultLimit,
		CopyRule:     &pullRequestRowCopyRule,
	},
}

// drilldownPRsTeamScopedParity is drilldownPRsParity plus
// drilldownPRsTeamScopeSubsetDefect and drilldownPRsTeamScopeUncutDefect.
var drilldownPRsTeamScopedParity = Options{
	NumericLeavesDeclared: true,
	IntegerLeaves:         drilldownPRsIntegerLeaves,
	BaselineDefects:       gatePullRequestDefects(append(append([]BaselineDefect{}, drilldownPRsParity.BaselineDefects...), drilldownPRsTeamScopeSubsetDefect, drilldownPRsTeamScopeUncutDefect), drilldownPRsDefaultLimit),
}

// drilldownPRsTeamScopedParityWithLimit returns drilldownPRsTeamScopedParity
// with a fresh copy of its own CHAOS-5968 (DuplicateCollapsePageCutShape)
// and CHAOS-5988 (DuplicateCollapseLengthShape, by ticket -- never the
// table's OTHER DuplicateCollapseLengthShape entry, CHAOS-5959, which
// stays RequestLimit-unset) entries, both set to THIS request's own
// effective limit -- the same per-request-limit convention
// drilldownPRsParityWithLimit already establishes for CHAOS-5968 alone.
func drilldownPRsTeamScopedParityWithLimit(limit int) Options {
	opts := parityWithPageCutLimit(drilldownPRsTeamScopedParity, limit)
	for i, defect := range opts.BaselineDefects {
		if defect.Ticket == "CHAOS-5988" && defect.DuplicateCollapseLengthShape != nil {
			shape := *defect.DuplicateCollapseLengthShape
			shape.RequestLimit = limit
			defect.DuplicateCollapseLengthShape = &shape
			opts.BaselineDefects[i] = defect
		}
	}
	return opts
}

// drilldownIssuesFloats declares this route's two numeric leaves:
// cycle_time_hours/lead_time_hours are `wct.cycle_time_hours`/
// `wct.lead_time_hours`, PLAIN per-row column reads off work_item_cycle_
// times (issues.go) -- Nullable(Float64) (001_metrics_v2.sql), never
// summed or averaged, so two planes reading the SAME latest version must
// agree bit for bit, the same "one row's own stored value, no cross-row
// arithmetic" class work-units' own workUnitsFloatExact already declares.
// Float-DECLARED (named here, satisfying validateNumericLeaves' own rule
// that a FloatExactLeaves key also appear in FloatTierB) but opted back
// to Tier A exact by drilldownIssuesFloatExact below.
var drilldownIssuesFloats = map[string]string{
	"data.items.cycle_time_hours": "wct.cycle_time_hours, a plain per-row Nullable(Float64) column read (issues.go) -- no cross-row arithmetic, opted back to exact below",
	"data.items.lead_time_hours":  "wct.lead_time_hours, the same plain per-row read as cycle_time_hours, opted back to exact below",
}

// drilldownIssuesFloatExact opts both leaves back to Tier A: each is one
// physical row's own stored value, never merged across rows.
var drilldownIssuesFloatExact = map[string]string{
	"data.items.cycle_time_hours": "a plain per-row column read; no cross-row arithmetic on either plane",
	"data.items.lead_time_hours":  "a plain per-row column read; no cross-row arithmetic on either plane",
}

// drilldownIssuesParity is shared by every admissible (2xx) drilldown/
// issues request, GET and POST alike: both routes call the same
// BuildIssuesResponse, so both carry the same declared Python-plane
// defect. Unlike drilldownPRsParity there is only one BaselineDefect here:
// work_item_cycle_times is already read FINAL on both planes
// (fetchIssuesQuery's own doc comment in internal/drilldown/issues.go), so
// the RMT-dedup shape drilldownPRsParity's own second entry declares has
// no counterpart on this route. Set on GET/POST's own "team_scoped"
// entries below -- the one shape on this route whose baseline actually
// answers 200, so its body (and both numeric leaves above) is genuinely
// compared; every other candidate-200 entry on this route stays declared
// baseline-only 503 (StatusDivergenceReason), so those numeric leaves stay
// unreached there. Also exercised directly, by name, by
// TestDrilldownIssuesParity_EmptyItemsIsAStructuralRefusalNotStale and
// TestDrilldownIssuesParityDatetimeCitation_NonVacuousMatchIsIdleNotStale.
// drilldownIssuesItemsOrderInsensitiveLists declares GET/POST
// /api/v1/drilldown/issues' own data.items list keyed by work_item_id:
// fetch_issues/fetchIssuesQuery both ORDER BY wct.completed_at DESC with
// no secondary sort, identically on both planes (fetchIssuesQuery's own
// doc comment, internal/drilldown/issues.go) -- the ordering is
// UNCONSTRAINED IN BOTH PLANES' OWN SQL for any group of rows sharing
// the same completed_at, the same "unconstrained, not a Go-vs-Python
// defect" property investmentSunburstOrderInsensitiveLists already
// documents for its own unordered-tie list. Pairing by work_item_id
// means a pure reordering of a SHARED subset of items (the common case)
// compares equal with no declaration needed at all; drilldownIssuesBoundaryTie
// below covers what keyed pairing still reports on its own: a
// work_item_id present on only one side, at the request's own LIMIT
// boundary.
var drilldownIssuesItemsOrderInsensitiveLists = []OrderInsensitiveList{
	{
		Path:      "data.items",
		KeyFields: []string{"work_item_id"},
		Reason:    "fetch_issues and fetchIssuesQuery both ORDER BY wct.completed_at DESC with no secondary sort, identically on both planes; the ordering is unconstrained in both planes' own SQL for any tied completed_at, not a Go-vs-Python defect",
		Ticket:    "CHAOS-5957",
	},
}

// drilldownIssuesBoundaryTie declares the ONE structural consequence of
// that same unordered tie: when strictly more rows share the list's own
// trailing completed_at value than remain before LIMIT cuts the list
// off, each plane's own ClickHouse execution can admit a DIFFERENT
// subset of that tied group, not merely a different order of the same
// subset -- a work_item_id present on only one side. See
// BoundaryTieShape's own doc comment (boundarytie.go) for the full rule
// set this citation's admission follows.
var drilldownIssuesBoundaryTie = BaselineDefect{
	Ticket:             "CHAOS-5957",
	Reason:             "fetch_issues (api/queries/drilldown.py) and fetchIssuesQuery (internal/drilldown/issues.go) both carry ORDER BY wct.completed_at DESC with no secondary sort key, then LIMIT :limit, identically on both planes -- when the tied group at that trailing completed_at value is larger than the number of slots remaining under the limit, each plane's own execution admits an unspecified subset of it, never guaranteed the same one. This is a genuine SQL-level tie, not a Go-vs-Python defect: neither plane's own query text differs for this clause. BoundaryTieShape admits ONLY a baseline-only/candidate-only work_item_id pair whose own completed_at agrees, on both sides, with the single shared tie value the swap actually happened at -- never a presence difference at any other completed_at, and never a leaf mismatch on a key both planes still return.",
	Paths:              []string{"data.items"},
	Intermittent:       true,
	IntermittentReason: "present only while the tied group at this request's own LIMIT boundary is strictly larger than the number of slots remaining under LIMIT -- a run whose boundary completed_at is held by exactly enough rows to fill those slots, or fewer, shows no divergence under this mechanism and is not a bug in this citation",
	BoundaryTieShape: &BoundaryTieShape{
		ListPath:  "data.items",
		KeyFields: []string{"work_item_id"},
		TieField:  "completed_at",
		Limit:     50,
	},
}

var drilldownIssuesParity = Options{
	NumericLeavesDeclared: true,
	FloatTierB:            drilldownIssuesFloats,
	FloatExactLeaves:      drilldownIssuesFloatExact,
	OrderInsensitiveLists: drilldownIssuesItemsOrderInsensitiveLists,
	BaselineDefects: []BaselineDefect{
		{
			Ticket: "CHAOS-5808",
			Reason: "work_item_cycle_times' started_at/completed_at columns are ClickHouse Nullable(DateTime('UTC')) (001_metrics_v2.sql); Python's clickhouse_connect driver returns them NAIVE (no tzinfo), so the Pydantic response serializes them with no offset, while Go's driver attaches UTC location and this port's IssueItem (encoding/json's default time.Time marshaling) emits RFC 3339 with an explicit offset -- the same class of divergence drilldown/prs' own corpus entry already declares for created_at/merged_at/first_review_at. Go is correct.",
			Paths: []string{
				"data.items.started_at",
				"data.items.completed_at",
			},
			TimestampRenderingShape: &TimestampRenderingShape{},
			Intermittent:            true,
			IntermittentReason:      "present only while this request's live result actually contains at least one returned issue with a non-null started_at or completed_at -- both columns are themselves Nullable, so an empty items list, or a window whose only issues have not yet started/completed, shows no divergence under these paths and is not a bug in this citation",
		},
		drilldownIssuesBoundaryTie,
	},
}

// explainParity is shared by every admissible (2xx) explain request, GET
// and POST alike: both routes call the same BuildExplainResponse, so both
// carry the same five declared, Intermittent Python-plane defects (see
// restEndpointSpecs' own doc comment for the fifth, undeclared one).

// explainAggregateFloats declares explain's own ClickHouse-derived
// numeric leaves as Tier B: metricValueProjection's headline read and
// fetchMetricContributors/fetchMetricDriverDelta's ranking read
// (internal/queryapi/explain/metrics.go) are each a merged Float64
// ClickHouse aggregate over work_item_metrics_daily/repo_metrics_daily/
// work_item_state_durations_daily, the same class every sibling
// FloatTierB table in this service already declares (CHAOS-5451).
// Undeclared here until now, which let a last-digit engine rounding
// difference surface as a Tier-A exact mismatch that explainParity's own
// CHAOS-5813 citation (unshaped, covering these same leaves by path
// proximity) still admitted, by the wrong mechanism rather than the
// right tolerance -- the flap cycle_time_range_days_90 showed between
// runs is this: ULP-scale noise crossing the exact-equality boundary
// run to run, not a change in the underlying data.
var explainAggregateFloats = map[string]string{
	"data.value":                  "metricValueProjection's argMax(tuple(col), computed_at) read, a merged Float64 ClickHouse aggregate",
	"data.delta_pct":              "derived from the same float aggregate as data.value",
	"data.drivers.value":          "fetchMetricContributors'/fetchMetricDriverDelta's own aggregate read (sum() or avg() depending on the metric's configured aggregator), a merged Float64 ClickHouse aggregate",
	"data.drivers.delta_pct":      "derived from the same float aggregates as data.drivers.value",
	"data.contributors.value":     "the same aggregate read as data.drivers.value",
	"data.contributors.delta_pct": "derived from the same float aggregates as data.contributors.value -- the same Contributor struct backs both drivers and contributors (schemas.py's Contributor model), so this leaf shares data.drivers.delta_pct's own provenance.",
}

// explainDriverRankOrderInsensitive declares data.drivers/data.contributors
// order-insensitive for THIS ticket only, and only for as long as the
// mechanism it is tied to exists: CHAOS-5818's own avg-vs-sum ranking
// divergence means the two planes can rank a sum-aggregator metric's
// contributing rows by DIFFERENT functions, so the top-N SET a plane
// selects -- not merely each element's value -- can differ. Comparing
// such a list positionally would not be a weaker check, it would be a
// meaningless one: row 3 on one plane is not necessarily the same
// logical driver/contributor as row 3 on the other, so a per-key
// direction claim requires pairing by id first. BLIND SPOT, stated
// plainly because it differs from every other OrderInsensitiveLists
// entry in this file: drivers/contributors are RANKED lists and their
// order is USER-VISIBLE -- it is the feature, not an internal
// representation detail. This declaration means a genuine Go-side
// ranking regression (the right rows, in the wrong order) is NOT caught
// on this route while it stands. It is justified only for as long as
// CHAOS-5818's own ranking divergence exists; once the two planes rank
// by the same function, this declaration should come out along with it,
// not be left behind as a permanent relaxation.
var explainDriverRankOrderInsensitive = []OrderInsensitiveList{
	{Path: "data.drivers", KeyFields: []string{"id"}, Reason: "CHAOS-5818's own avg-vs-sum ranking divergence can select a different top-N set, not just different values at the same rank -- see explainDriverRankOrderInsensitive's own doc comment", Ticket: "CHAOS-5818"},
	{Path: "data.contributors", KeyFields: []string{"id"}, Reason: "the same ranking divergence as data.drivers, over contributors", Ticket: "CHAOS-5818"},
}

var explainParity = Options{
	NumericLeavesDeclared: true,
	FloatTierB:            explainAggregateFloats,
	OrderInsensitiveLists: explainDriverRankOrderInsensitive,
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
			Reason: "fetch_metric_contributors and fetch_metric_driver_delta (api/queries/explain.py) rank every metric with a hardcoded avg(column), regardless of that metric's own aggregator in _METRIC_CONFIG -- this table's own headline reader, fetch_metric_value (api/queries/metrics.py), already keys off the metric's configured aggregator, so an avg-aggregator metric's ranking agrees with its own headline while a sum-aggregator metric's ranking (throughput, deploy_freq, churn, blocked_work) silently averages a quantity the metric's own label, unit and headline all present as a total. This port's fetchMetricContributors/fetchMetricDriverDelta (internal/queryapi/explain/metrics.go) take the metric's own config.Aggregator, matching the headline read. Go is correct. DIRECTION: a sum over N>=1 non-negative values is >= their average, equal only at N=1 -- every one of the four sum-aggregator metrics this entry covers reads a structurally non-negative column (metricconfig.go: throughput/items_completed and deploy_freq/deployments_count are counts, churn/total_loc_touched is touched-lines, blocked_work/duration_hours is a duration; none can go negative), so candidate (Go, sum) is admitted only when strictly greater than baseline (Python, avg) at the same driver/contributor id -- the reverse of every OTHER KeyedDirectionShape in this file, which is why this entry sets CandidateMustBeGreater. The non-negativity is this claim's own premise, not incidental: the same argument reverses for a signed quantity (see data.drivers.delta_pct's own drop below), and no metric this entry reaches carries one. Pairing is by id, over data.drivers/data.contributors declared order-insensitive under this ticket (explainDriverRankOrderInsensitive above) -- see that declaration's own doc comment for why positional pairing cannot be used here and what leaving it in costs.",
			Paths: []string{
				"data.drivers.value",
				"data.contributors.value",
			},
			Intermittent:       true,
			IntermittentReason: "present only for a request whose metric resolves to a sum-aggregator config (throughput, deploy_freq, churn, blocked_work) and whose ranked group has more than one contributing daily row with differing values in the request window -- a single-row or uniform-value group leaves sum and avg equal, and an avg-aggregator metric (cycle_time, review_latency, wip_saturation, change_failure_rate) never reaches this path at all. data.drivers.delta_pct is deliberately NOT in Paths: verified against a real captured instance, delta_pct's own baseline value was LESS than candidate's at the same driver while value's own baseline was GREATER -- opposite signs under the SAME comparison, because a percentage-change ratio does not inherit sum-vs-avg's monotonic guarantee when the two compare windows' own row counts differ. No direction is provable for it, so it is left uncovered rather than blanket-admitted; a real divergence there surfaces as an ordinary, visible finding.",
			KeyedDirectionShape: &KeyedDirectionShape{
				ListPath:               "data.drivers",
				ValueField:             "value",
				ValuePath:              "data.drivers.value",
				KeyFields:              []string{"id"},
				CandidateMustBeGreater: true,
			},
		},
		{
			Ticket:             "CHAOS-5818",
			Reason:             "the same avg-vs-sum mechanism and the same non-negativity premise as this ticket's data.drivers.value entry, over contributors. Go is correct.",
			Paths:              []string{"data.contributors.value"},
			Intermittent:       true,
			IntermittentReason: "the same condition as this ticket's data.drivers.value entry, over the contributors ranking",
			KeyedDirectionShape: &KeyedDirectionShape{
				ListPath:               "data.contributors",
				ValueField:             "value",
				ValuePath:              "data.contributors.value",
				KeyFields:              []string{"id"},
				CandidateMustBeGreater: true,
			},
		},
		{
			Ticket: "CHAOS-5819",
			Reason: "blocked_work's table/column read (work_item_state_durations_daily.duration_hours) carries no status predicate in fetch_metric_value/fetch_metric_contributors/fetch_metric_driver_delta (api/queries/metrics.py, api/queries/explain.py), so the 'Blocked Work' headline, its drivers and its contributors sum/rank duration_hours across every status the table records (backlog/todo/in_progress/in_review/blocked/done/canceled/unknown) -- only this table's OTHER, unrelated reader (fetch_blocked_hours, used by home.py, never by /explain) restricts to status = 'blocked'. This port's blocked_work config carries a StatusFilter of 'blocked' (internal/queryapi/explain/metricconfig.go), reaching every numeric field this route derives from that column for this one metric. Go is correct. This same divergence can also manifest as a LIST-LENGTH difference: a window/scope whose blocked-status rows are fewer than its non-blocked ones leaves data.drivers/data.contributors shorter on the candidate side, and a window with no blocked-status row at all leaves them empty against a populated baseline. Paths above cannot reach that manifestation and no addition to them would: classifyBaselineDefects (compare.go) admits a finding only when leafDifference(shape) holds, which is exactly ShapeValue/ShapeNull/ShapeScalarType -- a length, presence or structure difference is categorically outside every BaselineDefect's coverage, independent of what its Paths name. A list-length instance of this divergence is therefore knowingly left uncovered and stays a visible, real finding on the receipt whenever it fires.",
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
		{
			Ticket:             "CHAOS-5819",
			Reason:             "the same status-filter divergence as this ticket's own value entry above (restcorpus.go), taken to its full consequence on the drivers list: a window with no status='blocked' row leaves the candidate's blocked-only fetchMetricDriverDelta GROUP BY empty (metrics.go), so data.value/data.delta_pct read 0 on the candidate (covered by the sibling value entry) while data.drivers, ranked from the SAME all-status avg() the value entry's own baseline reads (explain.py), still carries whatever driver a blocked_work window with non-blocked duration surfaces -- a baseline-only PRESENCE difference this entry's own ZeroValueEmptyListShape admits, categorically outside the sibling entry's leaf-only Paths (leafDifference, compare.go). Go is correct.",
			Paths:              []string{"data.drivers"},
			Intermittent:       true,
			IntermittentReason: "present only under the exact condition the sibling value entry's own IntermittentReason states, and only when that window's candidate blocked-only GROUP BY resolves to zero rows for every group_by key (an empty drivers list, not merely a smaller one) -- a window whose blocked-status rows still populate at least one group_by key shows a value/leaf divergence there instead, still covered by the sibling entry, never by this one",
			ZeroValueEmptyListShape: &ZeroValueEmptyListShape{
				ValuePath: "data.value",
				ListPath:  "data.drivers",
			},
		},
		{
			Ticket:             "CHAOS-5819",
			Reason:             "the same status-filter divergence and the same value-collapse consequence as this ticket's own drivers entry above, over contributors (fetchMetricContributors, metrics.go). Go is correct.",
			Paths:              []string{"data.contributors"},
			Intermittent:       true,
			IntermittentReason: "the same condition as this ticket's own drivers entry above, over the contributors ranking",
			ZeroValueEmptyListShape: &ZeroValueEmptyListShape{
				ValuePath: "data.value",
				ListPath:  "data.contributors",
			},
		},
	},
}

// explainScopeDropDefect is scope_filter_for_metric's own org_id-
// omission bug (internal/queryapi/explain/response.go's own
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
	Reason:             "explain.py's own call site for scope_filter_for_metric (api/services/explain.py:150-152) is the only caller anywhere in the Python source that omits the org_id keyword -- it silently defaults to \"\". For a repo-scoped metric (review_latency/deploy_freq/churn/change_failure_rate) that flows into resolve_repo_id's own org_id filter, which no real org's repos row ever matches: every repo/team scope ref fails to resolve, repo_ids ends up [], and the scope filter is silently dropped, so the headline value/delta and every driver/contributor value are computed over the whole org rather than the requested scope. This port passes the real org id throughout. Go is correct. The reference's own team-to-repo resolver for this scope (resolve_repo_ids_for_teams, api/queries/scopes.py:72-89) reads DISTINCT repo_id straight off user_metrics_daily.team_id -- it bridges through the metrics table, never through team ownership -- while this port resolves a team's repositories from team_repo_ownership (internal/queryapi/teamscope), so for a team scope the two planes read different tables: see this file's own TEAM SCOPE paragraph above for why that difference is a knowingly uncovered finding rather than a declared defect. This same divergence can also manifest as a LIST-LENGTH difference: data.drivers/data.contributors come back shorter on the candidate side than the baseline's substituted org-wide list, and a scope with no matching rows in the window leaves them empty against a populated baseline, with data.value/data.delta_pct then reading 0 against a real number rather than a differently-scoped one -- a structural difference, never covered by ANY BaselineDefect shape (see compare.go's leafDifference gate), knowingly left uncovered and a real, visible finding on the receipt whenever a requested scope's resolved repo set is small or has no overlap with the metric's own source table. The same gap can also surface one layer earlier, before any field comparison runs: a resolved scope narrow enough widens the two response bodies past the proof comparator's own size-disagreement threshold, and the request is REFUSED as legs_do_not_overlap instead of reaching an admitted mismatch -- the same fallout, not a second defect.",
	Paths:              []string{"data.value", "data.delta_pct", "data.drivers.value", "data.drivers.delta_pct", "data.contributors.value"},
	Intermittent:       true,
	IntermittentReason: "present only while the requested repo/team scope's own aggregate actually differs from the whole org's aggregate for this metric and window; a scope whose narrowed value happens to equal the org-wide one shows no divergence under these paths",
}

// explainRepoTeamScopedParity is explainParity's own two declared
// defects plus explainScopeDropDefect -- shared by every explain entry
// below that requests an explicit, live repo or team scope for one of
// the four metrics explainScopeDropDefect names.
var explainRepoTeamScopedParity = Options{
	NumericLeavesDeclared: true,
	FloatTierB:            explainAggregateFloats,
	OrderInsensitiveLists: explainDriverRankOrderInsensitive,
	BaselineDefects:       append(append([]BaselineDefect{}, explainParity.BaselineDefects...), explainScopeDropDefect),
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

// explainContributorsSubsetDefect/explainDriversSubsetDefect declare
// data.contributors/data.drivers as containing only a bounded-subset
// PRESENCE admission for a team-scoped request on one of
// explainScopeDropDefect's four metrics: a genuinely narrower team's
// candidate list is missing exactly the contributors/drivers whose work
// falls outside the team's own repositories, which the candidate resolves
// through team_repo_ownership via one shared condition
// (internal/queryapi/teamscope.RepoCondition) while the reference
// resolves the same scope from user_metrics_daily.team_id
// (resolve_repo_ids_for_teams, api/queries/scopes.py) -- explainScopeDropDefect's
// own Reason states the two tables. Neither declares an EqualLeaves or
// BoundedLeaves entry: a matched contributor/driver's own "value"/
// "delta_pct" leaf is already reached by explainScopeDropDefect's own
// blanket Paths citation above (data.contributors.value, data.drivers.value)
// for every author BOTH planes return, so this pair of declarations adds
// only the membership admission that citation's leaf-only coverage
// cannot reach (compare.go's leafDifference gate) -- it never re-cites,
// or narrows, a leaf explainScopeDropDefect already explains.
var explainContributorsSubsetDefect = BaselineDefect{
	Ticket:              "CHAOS-5920",
	Reason:              "GET /api/v1/explain's team scope resolves a team's repositories from team_repo_ownership through one shared condition, while the reference plane resolves the same scope from user_metrics_daily.team_id -- a per-author-attribution table, not a repository-ownership one (explainScopeDropDefect's own Reason, above, states the two tables in full). For a genuinely narrower team the candidate's contributors list is therefore a subset of the baseline's own organization-wide list, paired by id (author). Go is correct.",
	Paths:               []string{"data.contributors"},
	TeamRepoSubsetShape: &TeamRepoSubsetShape{ListPath: "data.contributors", KeyFields: []string{"id"}},
	Intermittent:        true,
	IntermittentReason:  "present only while the requested team's own repositories are a PROPER subset of the organization's and at least one contributor's own work falls entirely outside that subset; a team owning every repository, or a window whose every contributing author touches only repositories the team owns, leaves the two lists identical, and shows no divergence under this Path",
}

var explainDriversSubsetDefect = BaselineDefect{
	Ticket:              "CHAOS-5920",
	Reason:              "The same mechanism and citation as explainContributorsSubsetDefect above, over data.drivers rather than data.contributors. Go is correct.",
	Paths:               []string{"data.drivers"},
	TeamRepoSubsetShape: &TeamRepoSubsetShape{ListPath: "data.drivers", KeyFields: []string{"id"}},
	Intermittent:        true,
	IntermittentReason:  "the same condition as explainContributorsSubsetDefect's own IntermittentReason, over the drivers ranking",
}

// explainTeamScopedParity is explainRepoTeamScopedParity plus
// explainContributorsSubsetDefect/explainDriversSubsetDefect -- shared by
// every explain team-scoped entry below. Kept separate from
// explainRepoTeamScopedParity (which repo-scoped entries still use
// unchanged) because the membership admission these two declarations add
// is specific to a team's repository resolution, not a repo-scoped
// request's own single, directly-bound repository.
var explainTeamScopedParity = Options{
	FloatTierB:            explainAggregateFloats,
	OrderInsensitiveLists: explainDriverRankOrderInsensitive,
	BaselineDefects:       append(append(append([]BaselineDefect{}, explainRepoTeamScopedParity.BaselineDefects...), explainContributorsSubsetDefect), explainDriversSubsetDefect),
}

// explainTeamScopedRequest builds one team-scoped explain request for a
// metric explainScopeDropDefect covers, its scope_id bound at run time to
// producerName's own live id (see restidbind.go).
func explainTeamScopedRequest(name, metric, producerName string) RESTRequest {
	return RESTRequest{
		Name:                name,
		Query:               url.Values{"metric": {metric}, "scope_type": {"team"}},
		WantCandidateStatus: 200, WantBaselineStatus: 200,
		BodyMode:   RESTBodyModeJSON,
		Parity:     explainTeamScopedParity,
		IDBindings: []RESTIDBinding{{Producer: producerName, QueryParam: "scope_id"}},
	}
}

// peopleParity is shared by every admissible (2xx) people request whose
// query string actually reaches ClickHouse -- a genuinely empty q
// short-circuits BEFORE the client is touched (BuildSearchResponse's own
// early return), so the "default" entry below carries no Parity at all:
// there is nothing this citation could ever cover there.
//
// NumericLeavesDeclared is set with both numeric tables absent: SearchResult
// (schemas.py's PersonSearchResult -- person_id/display_name/identities/
// active) carries zero numeric leaves, so a future field added to this
// shape becomes a reported, undeclared leaf rather than a silent exact
// comparison.
var peopleParity = Options{
	NumericLeavesDeclared: true,
	BaselineDefects: []BaselineDefect{
		{
			Ticket: "CHAOS-5812",
			Reason: "user_metrics_daily is ReplacingMergeTree(computed_at) (migration 096, sorting key org_id/repo_id/author_email/day); the reference people/people_search.sql reads it with no FINAL/argMax dedup at all -- only its sibling work_item_user_metrics_daily branch already carries FINAL. identity_id defaults to author_email but is not itself part of the sorting key, so a later write correcting it for an existing key is possible: an unmerged physical version can then surface BOTH the stale and the corrected identity as separate search results for the same person -- a LIST-LENGTH divergence, and this list is compared POSITIONALLY (no OrderInsensitiveLists entry for it), so the inserted element also shifts every later element's own person_id/display_name/identities/active into an individual leaf mismatch. This port reads BOTH UNION branches FINAL (searchPeopleQuery's own doc comment), matching quadrant/identity.go's resolvePersonIdentity for the same two tables. Go is correct. This citation's Paths reach the divergence but, by this package's own leaf-only coverage rule (BaselineDefect's doc comment), never silently admit the length difference itself: it stays a real, uncovered finding on the receipt whenever it fires -- the same declared shape as the filters/options citation above.",
			Paths: []string{
				// SearchResult's whole field set (search.go): a
				// positional-list insertion can shift any one of a
				// later element's own fields, never a field this type
				// does not carry.
				"data.person_id",
				"data.display_name",
				"data.identities",
				"data.active",
			},
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
// from the wrong physical version).
//
// Paths is the union of every field either route actually derives from
// one of the four tables' own read, NOT the whole payload: repos and
// repo_metrics_daily are read FINAL only for countDistinct/maxOrNull
// aggregates (fetchCoverage/fetchLastIngestedAt's own doc comments state
// this is correctness-neutral for those specific aggregates -- an
// unmerged duplicate changes neither a distinct count nor a max), so
// freshness.last_ingested_at, freshness.sources (derived from it) and
// freshness.coverage.repos_covered_pct carry no citable divergence from
// this mechanism, and neither does data.person (resolvePersonIdentity's
// own UNION DISTINCT collapses a duplicate identity string regardless of
// which physical row it came from). deltas[].spark[].ts is excluded
// too: it is peopleSummarySparkTimestampDefect's own, separately
// declared divergence (a naive-vs-aware timestamp rendering, present or
// absent by result content, never by merge state), and folding it into
// this blanket citation would let this defect's own live/idle state
// stand in for that one's, silently absorbing that divergence even on a
// run where this table's own trigger is live for an unrelated reason.
// peopleDetailParity is a BASELINE-DEFECTS-ONLY template: GET .../summary
// and .../metric's own six per-metric live entries each derive their own
// Options from it BY VALUE (the same split heatmapDedupParity's own four
// per-scenario Options and investmentFlowRepoDedupParity's own three
// per-shape Options already establish), adding their own numeric-leaf
// declaration on top. It is never assigned directly to a request's Parity
// itself: .../summary's six personMetrics entries share ONE wire path per
// field (data.deltas.value etc, safe to declare once since exactly one
// request reaches it), but .../metric's six entries are SIX SEPARATE
// requests, one per metric, each populating a DIFFERENT subset of
// data.breakdowns.by_repo/by_work_type/by_stage (metricconfig.go's own
// ByRepo/ByWorkType/ByStage configs never all three together) -- a
// FloatTierB/IntegerLeaves entry has no Intermittent escape the way
// BaselineDefect does (Options.FloatTierB's own doc comment), so a
// breakdown path declared but never reached by a GIVEN metric's own
// request would report UnusedTierB and refuse that request by name. Each
// metric's own Options below declares only the leaves that metric's own
// response can actually carry.
var peopleDetailParity = Options{
	BaselineDefects: []BaselineDefect{
		{
			Ticket: "CHAOS-5812",
			Reason: "repos, user_metrics_daily, work_item_user_metrics_daily and work_item_cycle_times are each ReplacingMergeTree (migrations 000/096/055); build_person_summary_response and build_person_metric_response (services/people.py) read every one of them without FINAL or argMax dedup, where this port's own summary.go/metric.go/resolve.go read all four FINAL, matching the identical fix peopleParity's own citation already declares for GET /api/v1/people's search read of the same two UNION branches. An unmerged physical version can surface a stale headline/coverage/freshness value or an extra, stale element in a sparkline or work-mix list for this person_id. Go is correct. This citation's Paths reach the divergence but, by this package's own leaf-only coverage rule (BaselineDefect's doc comment), never silently admit a length or structural difference: it stays a real, uncovered finding on the receipt whenever it fires.",
			Paths: []string{
				// GET .../summary fields sourced from work_item_cycle_times
				// (fetchCoverage's non-neutral counts).
				"data.freshness.coverage.prs_linked_to_issues_pct",
				"data.freshness.coverage.issues_with_cycle_states_pct",
				// GET .../summary: fetchIdentityCoverage (user_metrics_daily/
				// work_item_user_metrics_daily).
				"data.identity_coverage_pct",
				// GET .../summary: fetchPersonMetricValue/fetchPersonMetricSeries
				// (personMetrics' own per-metric table), plus the narrative
				// sentences derived from deltas' own values.
				"data.deltas.value",
				"data.deltas.delta_pct",
				"data.deltas.spark.value",
				"data.narrative",
				// GET .../summary: fetchPersonWorkMix/fetchPersonFlowBreakdown
				// (work_item_cycle_times), fetchPersonCollaboration
				// (user_metrics_daily/work_item_user_metrics_daily).
				"data.sections.work_mix",
				"data.sections.flow_breakdown",
				"data.sections.collaboration",
				// GET .../metric: fetchPersonMetricSeries/fetchPersonBreakdown
				// (personMetricConfig's own per-metric table), plus the
				// driver sentence derived from breakdowns' own values.
				"data.timeseries.value",
				"data.breakdowns.by_repo.value",
				"data.breakdowns.by_work_type.value",
				"data.breakdowns.by_stage.value",
				"data.drivers.text",
			},
			Intermittent:       true,
			IntermittentReason: "present only while one of the four source tables holds an unmerged physical version for THIS person_id's own rows since the last merge; a comparison taken after the next background merge shows no divergence",
		},
	},
}

// peopleSummarySparkTimestampDefect declares GET .../summary's own
// deltas[].spark[].ts divergence: user_metrics_daily.day and
// work_item_user_metrics_daily.day (001_metrics_v2.sql) are ClickHouse
// Date columns, not DateTime64. _spark_points (services/people.py) sets
// SparkPoint.ts directly from the driver's row value; clickhouse_connect
// returns a Date column as a naive python date, and SparkPoint.ts's own
// pydantic type (models/schemas.py: `ts: datetime`) coerces it to
// midnight with no tzinfo, so the response serializes with no UTC
// offset. This port's fetchPersonMetricSeries (metricqueries.go) scans
// the same Date column into a time.Time the ClickHouse Go driver
// locates in UTC, and encoding/json's default time.Time marshaling
// always writes an explicit offset -- the same naive-vs-aware rendering
// drilldownPRsParity's own datetime entry declares for git_pull_requests'
// DateTime64 columns, recurring here on a Date column reached through a
// different query path. Go is correct. Unlike peopleDetailParity's own
// citation, this divergence does not depend on merge state -- every REST
// citation in this table is Intermittent (TestEveryRESTBaselineDefect
// IsIntermittent), but on RESULT CONTENT, not ClickHouse's: it is
// present whenever this person's deltas actually carry a spark point,
// the same content-triggered reading drilldownPRsParity's own datetime
// entry already uses, and it goes idle (never stale) for a person whose
// series happens to be empty this request -- never idle because a
// background merge happened to run.
var peopleSummarySparkTimestampDefect = BaselineDefect{
	Ticket:             "CHAOS-5859",
	Reason:             "user_metrics_daily.day and work_item_user_metrics_daily.day (001_metrics_v2.sql) are ClickHouse Date columns; _spark_points (services/people.py) assigns SparkPoint.ts from the driver's row value, and SparkPoint.ts's pydantic type (models/schemas.py: `ts: datetime`) coerces the naive python date clickhouse_connect returns for a Date column into a naive datetime with no tzinfo, so the response serializes with no UTC offset. This port's fetchPersonMetricSeries (metricqueries.go) scans the same Date column into a time.Time the ClickHouse Go driver locates in UTC, and encoding/json's default time.Time marshaling always writes an explicit offset. Go is correct.",
	Paths:              []string{"data.deltas.spark.ts"},
	Intermittent:       true,
	IntermittentReason: "present only while this person's deltas actually carry at least one non-empty spark series; a person whose window returns an empty series for every metric shows no divergence under this path and is not a bug in this citation -- a result-content trigger, not ClickHouse merge state: a comparison taken after a background merge shows the identical divergence, because the underlying naive-vs-aware rendering the Reason describes never depends on which physical row version was read",
}

// peopleSummaryLastIngestedAtTimestampDefect declares GET .../summary's
// own freshness.last_ingested_at divergence: the SAME naive-vs-aware
// rendering class peopleSummarySparkTimestampDefect declares for a Date
// column and drilldownPRsParity's own datetime entry declares for
// DateTime64 columns, recurring here on repo_metrics_daily.computed_at
// (migration 096), a plain ClickHouse DateTime column reached through
// freshness's own query rather than either of those. fetch_last_ingested_at
// (api/queries/freshness.py) returns the driver's row value with no
// explicit timezone handling, and clickhouse_connect returns a DateTime
// column NAIVE (no tzinfo), so the Pydantic response serializes it with
// no UTC offset. This port's fetchLastIngestedAt (summary.go) scans the
// same column into a time.Time the ClickHouse Go driver locates in UTC,
// and encoding/json's default time.Time marshaling always writes an
// explicit offset. Go is correct. A distinct declaration from the two
// above because it is a different column on a different query, not a
// restatement of either -- peopleDetailParity's own citation excludes
// this path precisely because repos/repo_metrics_daily's FINAL read is
// correctness-neutral for this aggregate (fetchCoverage/
// fetchLastIngestedAt's own doc comments), so the merge-lag mechanism
// never explains this divergence; only the rendering does.
var peopleSummaryLastIngestedAtTimestampDefect = BaselineDefect{
	Ticket:             "CHAOS-5871",
	Reason:             "repo_metrics_daily.computed_at (migration 096) is a ClickHouse DateTime column; fetch_last_ingested_at (api/queries/freshness.py) returns the driver's row value through query_dicts with no explicit timezone handling, and Python's clickhouse_connect driver returns a DateTime column NAIVE (no tzinfo), so the response serializes it with no UTC offset. This port's fetchLastIngestedAt (summary.go) scans the same column into a time.Time the ClickHouse Go driver locates in UTC, and encoding/json's default time.Time marshaling always writes an explicit offset -- the same naive-vs-aware rendering class this table's own Date- and DateTime64-column entries already declare, recurring here on a plain DateTime column reached through freshness's own query. Go is correct.",
	Paths:              []string{"data.freshness.last_ingested_at"},
	Intermittent:       true,
	IntermittentReason: "present only while this org's freshness reading actually carries a non-null last_ingested_at; an org with no repo_metrics_daily rows at all shows no divergence under this path -- a result-content trigger, not ClickHouse merge state, the same reading peopleSummarySparkTimestampDefect's own IntermittentReason uses",
}

// peopleSummaryCollaborationOrderInsensitiveLists declares GET
// .../summary's own sections.collaboration row-order gap: fetchPersonCollaboration's
// query (summary.go) and person_summary_collaboration.sql are both a
// plain ClickHouse UNION ALL with no outer ORDER BY, over four review_load
// branches and two handoff_points branches respectively. The row order
// this produces is UNCONSTRAINED IN BOTH PLANES' OWN SQL -- a property of
// the engine's result for a query with no secondary sort, not a
// divergence between the planes, the same class investmentFull's own
// sankey nodes/edges OrderInsensitiveLists (operations.go) already
// declare for an identical unordered UNION ALL shape. label uniquely
// identifies an element within each of the two lists (review_load's four
// branches and handoff_points' two branches never repeat a label), so
// pairing on it, rather than position, is sound.
var peopleSummaryCollaborationOrderInsensitiveLists = []OrderInsensitiveList{
	{
		Path:      "data.sections.collaboration.review_load",
		KeyFields: []string{"label"},
		Reason:    "fetchPersonCollaboration's query (summary.go) and person_summary_collaboration.sql are both an unordered ClickHouse UNION ALL over review_load's four branches; the row order is unconstrained in both planes' own SQL, not a Go-vs-Python defect",
		Ticket:    "CHAOS-5872",
	},
	{
		Path:      "data.sections.collaboration.handoff_points",
		KeyFields: []string{"label"},
		Reason:    "handoff_points shares the same unordered-UNION shape as review_load, over its own two branches with no cross-branch ordering guarantee either",
		Ticket:    "CHAOS-5872",
	},
}

// peopleSummaryParity is GET /api/v1/people/{person_id}/summary's own
// live (person_id-bound, 200) entry: peopleDetailParity's own citation
// plus the two content-triggered timestamp citations above and the
// collaboration ordering relaxation. Not shared with /metric's own six
// entries below (each its own per-metric Options, peopleMetricChurnParity
// etc.): /metric's response carries no deltas/spark/sections shape at all
// (MetricResponse.Timeseries' own day field is already a plain string,
// metric.go), so a citation or relaxation naming a path that route can
// never produce would cover nothing there and refuse the run as stale --
// the identical reasoning that also splits /metric's OWN numeric-leaf
// declaration one Options per metric, since a breakdown path one metric
// never populates is the same kind of gap for FloatTierB/IntegerLeaves.
// peopleSummaryNumericFloats/peopleSummaryNumericInts declare GET
// .../summary's OWN numeric leaves -- exactly one request
// (summary_default) ever carries this Options value, so declaring the
// full set together, once, is safe (unlike .../metric below): every path
// here is reachable in that single response.
//   - data.freshness.coverage.*: fetchCoverage's toFloat64(countDistinct/
//     count) ratios (summary.go) -- genuine ratios, the same fetchCoverage
//     shape home_corpus.go's own homeNumericLeaves already declares float.
//   - data.identity_coverage_pct: coverage_sources/2.0*100.0
//     (services/people.py:497) -- a genuine ratio over an exact
//     countDistinct (summary.go's fetchIdentityCoverage).
//   - data.deltas.value/delta_pct/spark.value: ONE wire path shared by
//     all six personMetrics entries WITHIN THIS SINGLE RESPONSE
//     (metricconfig.go), whose producing aggregates are NOT uniform --
//     cycle_time/review_latency/wip_overlap use avg() (always Float64 in
//     ClickHouse regardless of the underlying column's own type), while
//     throughput/churn/blocked_work sum() an integer-domain column
//     (items_completed/loc_touched/if(status='blocked',1,0)). Declared
//     float for the whole shared path -- the same "declare the more
//     permissive classification" convention homeNumericLeaves already
//     uses for its own identically mixed data.deltas.value/spark.value.
//   - data.sections.flow_breakdown.value (summary.go's
//     fetchPersonFlowBreakdown): coalesce(toFloat64(avg(cycle_time_hours)
//     ),0) / avg(lead_time_hours-cycle_time_hours) -- genuine avg()
//     aggregates over Float64 columns.
var peopleSummaryNumericFloats = map[string]string{
	"data.freshness.coverage.repos_covered_pct":            "fetchCoverage's covered/total ratio *100 (summary.go) -- a genuine ratio.",
	"data.freshness.coverage.prs_linked_to_issues_pct":     "fetchCoverage's linked/total ratio *100 -- a genuine ratio.",
	"data.freshness.coverage.issues_with_cycle_states_pct": "fetchCoverage's with_cycle/total ratio *100 -- a genuine ratio.",
	"data.identity_coverage_pct":                           "coverage_sources/2.0*100.0 (services/people.py:497) -- a genuine ratio over fetchIdentityCoverage's exact countDistinct.",
	"data.deltas.value":                                    "shared by all six personMetrics entries within this one response, whose producing aggregates mix avg() over Float64 columns and sum() over integer-domain columns on this ONE wire path -- declared float for the whole path, the same mixed-provenance convention homeNumericLeaves uses for its own identical shape.",
	"data.deltas.delta_pct":                                "derived from the same mixed-provenance aggregate as data.deltas.value.",
	"data.deltas.spark.value":                              "the same per-row mixed provenance as data.deltas.value, one point per day.",
	"data.sections.flow_breakdown.value":                   "fetchPersonFlowBreakdown's avg(cycle_time_hours) / avg(lead_time_hours-cycle_time_hours) (summary.go) -- genuine Float64 avg() aggregates.",
}

// peopleSummaryNumericInts declares GET .../summary's own integer leaves:
// each is a `toFloat64(...)`-wrapped count/sum whose cast exists only so
// the ClickHouse driver can scan the result into *float64, never because
// the quantity is fractional -- the same "cast for scan" class heatmap's
// own repo_touchpoints/active_hours and home's own
// rework_theme_allocation sums already declare integer.
var peopleSummaryNumericInts = map[string]string{
	"data.sections.work_mix.value":                     "fetchPersonWorkMix's toFloat64(count()) (summary.go) -- an exact row count, cast for scan only.",
	"data.sections.collaboration.review_load.value":    "fetchPersonCollaboration's toFloat64(sum(reviews_given/reviews_received/prs_authored/prs_merged)) (summary.go) -- sums of integer-domain count columns, cast for scan only.",
	"data.sections.collaboration.handoff_points.value": "fetchPersonCollaboration's toFloat64(sum(items_started/items_completed)) (summary.go) -- sums of integer-domain count columns, cast for scan only.",
}

var peopleSummaryParity = Options{
	NumericLeavesDeclared: true,
	FloatTierB:            peopleSummaryNumericFloats,
	IntegerLeaves:         peopleSummaryNumericInts,
	BaselineDefects: append(append([]BaselineDefect{}, peopleDetailParity.BaselineDefects...),
		peopleSummarySparkTimestampDefect, peopleSummaryLastIngestedAtTimestampDefect),
	OrderInsensitiveLists: peopleSummaryCollaborationOrderInsensitiveLists,
}

// The six Options below are GET .../metric's own per-metric live
// entries: personMetricConfig (metricconfig.go) resolves "metric" to
// exactly one of six personMetrics rows, so each request is its OWN
// comparison reaching its OWN subset of data.timeseries.value and
// data.breakdowns.by_repo/by_work_type/by_stage -- never all three
// breakdowns together, and wip_overlap reaches none of them at all
// (metric.go's own cfg.ByRepo/ByWorkType/ByStage all nil check leaves
// breakdowns the static empty-lists value). Each Options here inherits
// peopleDetailParity's own BaselineDefects BY VALUE and adds only the
// numeric declarations that metric's own request can actually reach.

// peopleMetricChurnInts: metric_default requests "churn" -- sum(loc_touched)
// over a UInt32 count column (005_ic_metrics.sql) for both the series and
// its ByRepo breakdown -- integer, never a merged float aggregate.
var peopleMetricChurnInts = map[string]string{
	"data.timeseries.value":         "personMetrics' churn entry: sum(loc_touched), a UInt32 count column (005_ic_metrics.sql) -- integer.",
	"data.breakdowns.by_repo.value": "churn's own ByRepo breakdown: the same sum(loc_touched) shape, grouped by repo, joined to repos FINAL.",
}

var peopleMetricChurnParity = Options{
	NumericLeavesDeclared: true,
	IntegerLeaves:         peopleMetricChurnInts,
	BaselineDefects:       append([]BaselineDefect{}, peopleDetailParity.BaselineDefects...),
}

// peopleMetricCycleTimeFloats: metric_cycle_time -- avg(cycle_time_p50_hours)
// / avg(cycle_time_hours), both Nullable(Float64) columns (001_metrics_v2.
// sql) -- avg() is always Float64 in ClickHouse regardless of the source
// column's own type, so this is float by aggregator, not merely by column.
var peopleMetricCycleTimeFloats = map[string]string{
	"data.timeseries.value":              "personMetrics' cycle_time entry: avg(cycle_time_p50_hours) over work_item_user_metrics_daily -- avg() is always Float64.",
	"data.breakdowns.by_work_type.value": "cycle_time's own ByWorkType breakdown: avg(cycle_time_hours) over work_item_cycle_times, grouped by type.",
	"data.breakdowns.by_stage.value":     "cycle_time's own ByStage breakdown: the same avg(cycle_time_hours) shape, grouped by status.",
}

var peopleMetricCycleTimeParity = Options{
	NumericLeavesDeclared: true,
	FloatTierB:            peopleMetricCycleTimeFloats,
	BaselineDefects:       append([]BaselineDefect{}, peopleDetailParity.BaselineDefects...),
}

// peopleMetricReviewLatencyFloats: metric_review_latency --
// avg(pr_first_review_p50_hours), a Nullable(Float64) column
// (001_metrics_v2.sql) -- avg() is always Float64.
var peopleMetricReviewLatencyFloats = map[string]string{
	"data.timeseries.value":         "personMetrics' review_latency entry: avg(pr_first_review_p50_hours) over user_metrics_daily -- avg() is always Float64.",
	"data.breakdowns.by_repo.value": "review_latency's own ByRepo breakdown: the same avg(pr_first_review_p50_hours) shape, joined to repos FINAL.",
}

var peopleMetricReviewLatencyParity = Options{
	NumericLeavesDeclared: true,
	FloatTierB:            peopleMetricReviewLatencyFloats,
	BaselineDefects:       append([]BaselineDefect{}, peopleDetailParity.BaselineDefects...),
}

// peopleMetricThroughputInts: metric_throughput -- sum(items_completed),
// a UInt32 count column (001_metrics_v2.sql), and its ByWorkType
// breakdown's sum(if(completed_at IS NULL,0,1)) -- both integer, never a
// merged float aggregate.
var peopleMetricThroughputInts = map[string]string{
	"data.timeseries.value":              "personMetrics' throughput entry: sum(items_completed), a UInt32 count column (001_metrics_v2.sql) -- integer.",
	"data.breakdowns.by_work_type.value": "throughput's own ByWorkType breakdown: sum(if(completed_at IS NULL,0,1)) over work_item_cycle_times -- a plain 0/1 count sum, integer.",
}

var peopleMetricThroughputParity = Options{
	NumericLeavesDeclared: true,
	IntegerLeaves:         peopleMetricThroughputInts,
	BaselineDefects:       append([]BaselineDefect{}, peopleDetailParity.BaselineDefects...),
}

// peopleMetricWipOverlapFloats: metric_wip_overlap -- avg(wip_count_end_of_day)
// over a UInt32 column (001_metrics_v2.sql) -- avg() is always Float64 in
// ClickHouse regardless of the source column's own integer type. This
// metric configures NO breakdown at all (metricconfig.go), so
// by_repo/by_work_type/by_stage stay the static empty-lists value and
// carry no leaf to declare -- declaring one here would report it unused
// and refuse this request by name.
var peopleMetricWipOverlapFloats = map[string]string{
	"data.timeseries.value": "personMetrics' wip_overlap entry: avg(wip_count_end_of_day) over work_item_user_metrics_daily -- avg() is always Float64.",
}

var peopleMetricWipOverlapParity = Options{
	NumericLeavesDeclared: true,
	FloatTierB:            peopleMetricWipOverlapFloats,
	BaselineDefects:       append([]BaselineDefect{}, peopleDetailParity.BaselineDefects...),
}

// peopleMetricBlockedWorkInts: metric_blocked_work --
// sum(if(status='blocked',1,0)) over work_item_cycle_times, for both the
// series and its ByWorkType breakdown -- a plain 0/1 count sum, integer.
var peopleMetricBlockedWorkInts = map[string]string{
	"data.timeseries.value":              "personMetrics' blocked_work entry: sum(if(status='blocked',1,0)) over work_item_cycle_times -- integer.",
	"data.breakdowns.by_work_type.value": "blocked_work's own ByWorkType breakdown: the same sum(if(status='blocked',1,0)) shape, grouped by type.",
}

var peopleMetricBlockedWorkParity = Options{
	NumericLeavesDeclared: true,
	IntegerLeaves:         peopleMetricBlockedWorkInts,
	BaselineDefects:       append([]BaselineDefect{}, peopleDetailParity.BaselineDefects...),
}

// personDrilldownPRsIntegerLeaves declares this route's two numeric
// leaves, both integer domain: data.items.number is a plain PR number
// column (uint32, drilldownprs.go); data.items.review_latency_hours is
// `toFloat64(dateDiff('hour', pr.created_at, pr.first_review_at))`
// (drilldownprs.go) -- dateDiff is always a whole number of hours, and
// the toFloat64 wrap exists only so this port types the field *float64
// on the wire (unlike drilldown/prs' own *int64 choice for the identical
// quantity) -- a wire-shape choice, not a provenance one.
var personDrilldownPRsIntegerLeaves = map[string]string{
	"data.items.number":               "a plain PR number column (uint32), never aggregated.",
	"data.items.review_latency_hours": "dateDiff('hour', created_at, first_review_at) -- always a whole number of hours; toFloat64()-wrapped for this port's own *float64 wire typing, a wire-shape choice, not a provenance one.",
}

// personDrilldownPRsParity is GET /api/v1/people/{person_id}/drilldown/
// prs' own live (person_id-bound, 200) entries' Parity: the same
// pullRequestDrilldownDefects drilldownPRsParity binds, with the route's
// own next_cursor -- the created_at of the page's last row on both planes
// (services/people.py's `rows[-1].get("created_at")`, drilldownprs.go) --
// as the trailing cursor.
var personDrilldownPRsParity = Options{
	NumericLeavesDeclared: true,
	IntegerLeaves:         personDrilldownPRsIntegerLeaves,
	BaselineDefects:       pullRequestDrilldownDefects(pullRequestDrilldownRoute{cursorPath: "data.next_cursor"}),
}

// personDrilldownPRsCeilingLimit is the route's own limit ceiling on both
// planes: main.py's `_bounded_limit_param(limit, 200)` and drilldownprs.go's
// maxDrilldownLimit. A request above it is served a page of this size.
const personDrilldownPRsCeilingLimit = 200

// personDrilldownIssuesParity is the Parity GET /api/v1/people/
// {person_id}/drilldown/issues' own live entries would compare under.
// Both of that route's readers (sql/people/person_drilldown_issues.sql,
// drilldownissues.go) read work_item_cycle_times FINAL and ORDER BY
// wct.completed_at DESC LIMIT with no secondary sort, exactly as the
// scope-based drilldown/issues readers do, so it binds the same
// declarations drilldownIssuesParity binds. No corpus entry sets it
// today: that route's live entries are declared baseline-only 503
// (StatusDivergenceReason), so no body is compared until that status
// divergence clears.
var personDrilldownIssuesParity = drilldownIssuesParity

// flamePRIDBoundParity is GET /api/v1/flame's own live (pr_id-bound, 200)
// "pr" entity_type entry below. flame_route.go's own package doc comment
// (internal/queryapi/flame) states the citation this mirrors: both
// git_pull_requests and git_pull_request_reviews are ReplacingMergeTree(
// last_synced) (000_raw_tables.sql, sort-keyed by migration 027), and
// api/queries/flame.py's fetch_pull_request reads a bare `LIMIT 1` (no
// ORDER BY, no FINAL) while fetch_pull_request_reviews reads `ORDER BY
// submitted_at` with no FINAL either -- neither dedups an unmerged
// physical version of the same logical row. This is the same declared
// defect class drilldown/prs' own corpus entry (drilldownPRsParity)
// already tracks for git_pull_requests, at a different query site over
// the same table plus its review sibling. This port reads both FINAL
// (flame.go's own doc comment). Go is correct.
// flamePRIDBoundEntityInts declares the "pr" entity_type's own
// data.entity.number leaf: services/flame.py's _build_pr_flame_response
// (entity["number"] = number) and this port's buildPRFlameResponse
// (internal/queryapi/flame/flame.go's entity map, line 360) both
// assign it a plain int parsed off the "<repo_id>:<number>" entity_id --
// never aggregated -- so it is integer, not float. "issue"/"deployment"
// entity dicts carry no numeric field at all (services/flame.py's own
// entity dict literals for those two branches).
var flamePRIDBoundEntityInts = map[string]string{
	"data.entity.number": "the PR number parsed off the entity_id's own <repo_id>:<number> suffix, a plain int on both planes, never aggregated",
}

var flamePRIDBoundParity = Options{
	NumericLeavesDeclared: true,
	IntegerLeaves:         flamePRIDBoundEntityInts,
	BaselineDefects: []BaselineDefect{
		{
			Ticket: "CHAOS-5803",
			Reason: "git_pull_requests and git_pull_request_reviews are both ReplacingMergeTree(last_synced) (000_raw_tables.sql, org_id added to both sorting keys by migration 027); api/queries/flame.py's fetch_pull_request reads git_pull_requests with a bare LIMIT 1 (no ORDER BY, no FINAL) and fetch_pull_request_reviews reads git_pull_request_reviews ordered by submitted_at with no FINAL either -- neither dedups an unmerged physical version of the same logical row. This port's fetchPullRequest/fetchPullRequestReviews (internal/queryapi/flame) read both tables FINAL. An unmerged physical version can surface a stale PR field (entity/timeline/frame values derived from it) or a stale/duplicated review that shifts the rework-window frames this entity_type builds. Go is correct. This citation's Paths reach the divergence but, by this package's own leaf-only coverage rule (BaselineDefect's doc comment), never silently admit a length or structural difference: it stays a real, uncovered finding on the receipt whenever it fires.",
			// flame.Response's whole field set (flame.go): the Reason
			// names all three -- entity, timeline and frame values -- as
			// directly derived from the one PR/review read this
			// mechanism can serve a stale or duplicated physical version
			// of.
			Paths: []string{
				"data.entity",
				"data.timeline",
				"data.frames",
			},
			Intermittent:       true,
			IntermittentReason: "present only while git_pull_requests or git_pull_request_reviews holds an unmerged physical version for this PR's own rows since the last merge; a comparison taken after the next background merge shows no divergence",
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
// TEAM SCOPE ON A REPO-KEYED ROUTE IS NOT PROVABLE BY EQUALITY. The two
// planes resolve a team's repositories from different tables. This port
// reads team_repo_ownership through one shared condition
// (internal/queryapi/teamscope.RepoCondition), the source
// migrations/clickhouse/081_team_cognitive_load_daily.sql names as the one
// that defines a team's repositories. The reference reads DISTINCT repo_id
// off user_metrics_daily.team_id (resolve_repo_ids_for_teams,
// api/queries/scopes.py), which is per-author membership attribution and
// falls back to author membership whenever repository-ownership resolution
// misses. For the organization this proof runs against, none of the ids
// that resolver produces for a real team exists in repos, so the
// reference's verified list comes back EMPTY -- and an empty list is also
// how the reference expresses "no filter". The baseline therefore answers
// ORG-WIDE for a team-scoped request while the candidate answers over that
// team's repositories.
//
// home and opportunities reach the same outcome by a second, independent
// road: services/home.py's own scope_filter_for_metric call sites for a
// metric delta and for the top delta's drivers pass no org_id, which
// defaults to "", so every repo/team ref fails to resolve there too.
//
// A CHANGED AGGREGATE VALUE stays outside every citation on purpose: a
// scoped average is not a subset of an unscoped one, it is a different
// number, and no shape in this package claims otherwise for a ratio or
// average leaf (TeamRepoSubsetShape's own doc comment, teamreposubset.go,
// states this for the leaves it declares; explainScopeDropDefect below
// already blanket-admits explain's own data.value/data.delta_pct and
// matched-author data.contributors.value/data.drivers.value for a MATCHED
// author regardless of direction, which is a wider admission than a
// direction-only shape would grant and is unchanged by this file).
//
// A LIST MEMBERSHIP difference -- an element the organization-wide
// baseline carries that a genuinely narrower team-scoped candidate does
// not -- IS admissible: TeamRepoSubsetShape (teamreposubset.go) certifies
// the candidate's list as a bounded subset of the baseline's, by key,
// wherever a route's own BaselineDefect declares it. Declared today for
// GET/POST work-units' whole list (workUnitsTeamScopeSubsetDefect,
// workunits_corpus.go), GET/POST opportunities' items list
// (opportunitiesTeamScopeSubsetDefect, opportunities_corpus.go) and
// explain's data.contributors/data.drivers on review_latency/deploy_freq/
// churn/change_failure_rate's own team-scoped entries
// (explainContributorsSubsetDefect/explainDriversSubsetDefect above).
// home_team_scoped and work-units team_scoped (GET and POST) carry a
// BaselineTimeoutDeclared (restbaselinetimeout.go): while the baseline leg
// answers nothing within 180 s the request is admitted on the candidate's
// non-empty answer alone, and while it answers the ordinary comparison runs.
// NOT declared for opportunities_team_scoped's own GET leg: its baseline leg
// currently answers nothing at all (a request timeout, see the entry's own
// Timeout field), so there is no live comparison to establish a declaration
// against, and none is guessed. quadrant's cycle_throughput_team_scoped
// and flame/aggregated's two team_id entries bind a team COLUMN directly,
// resolve no repositories, and are unaffected. investment/explain's
// team_scoped entry below resolves a team the same new way but its
// response is majority LLM-authored prose with few numeric leaves, and
// carries no declared list membership shape either. heatmap, sankey,
// drilldown/prs, investment, investment/sunburst and investment/flow
// resolve a team the same new way but carry no team-scoped entry here, so
// this corpus sees nothing change for them; the same finding applies the
// moment one is added.
//
// explain declares five of its five known Python-plane divergences via
// explainParity/explainScopeDropDefect below (the display-name FINAL-dedup
// defect, the Nullable-column tuple-argMax defect, the ranking-aggregator
// defect and the blocked_work status-filter defect, all Intermittent --
// their own Reason strings have the citation trail; and
// scope_filter_for_metric's own org_id omission for
// review_latency/deploy_freq/churn/change_failure_rate, explainScopeDropDefect,
// not Intermittent -- it fires on every explicitly repo/team-scoped
// request for one of those four metrics, not only while some table holds
// an unmerged row). explainScopeDropDefect's own list-membership
// manifestation (data.contributors/data.drivers coming back shorter, or
// empty, on the candidate side) is declared, for the team-scoped entries
// only, via explainContributorsSubsetDefect/explainDriversSubsetDefect
// above; its repo-scoped entries carry no such membership shape.
//
// people declares its one known Python-plane divergence via peopleParity
// below: a LIST-LENGTH shape (an extra, stale search result), the same
// declared-but-never-silently-admitted class the filters/options citation
// above establishes -- Paths names the whole payload ("data") because the
// extra element can appear anywhere in the result list, not under one
// fixed field. No validation-error entry duplicates the two already-
// captured 422 fixtures (internal/queryapi/server/testdata/people_422): this table's
// two 422 entries below reproduce them by request shape, not by re-typing
// a body a live comparison already gets from the real route.
//
// drilldown/issues' own started_at/completed_at naive-datetime divergence
// (drilldownIssuesParity above, the same shape drilldown/prs's own corpus
// entry already cites for its created_at/merged_at/first_review_at fields)
// is declared on GET/POST /api/v1/drilldown/issues' own "team_scoped"
// entries: the team-scoped statement avoids the org-wide exception, so its
// baseline answers 200 and its body is genuinely compared. Every OTHER
// candidate-200 entry on this route (default_window, range_days_90,
// explicit_window and their POST twins) stays declared baseline-only 503
// (StatusDivergenceReason), so those bodies are still never compared.
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
// (operations.go's own `pr` doc comment). The one exception is
// people/{person_id}/drilldown/issues' own 200 entry, presently declared
// baseline-only 503 (StatusDivergenceReason): its body is never decoded,
// so it reaches none of this route's declared checks right now -- see
// its own doc comment.
//
// flame's corpus covers both deterministic-refusal requests (every 4xx
// build_flame_response/internal/flame.BuildResponse answers BEFORE any
// ClickHouse call, or after a ClickHouse call whose zero-row answer is
// itself deterministic for a guaranteed-absent id -- pr_entity_id_bound_
// not_found) AND, now that a producer exists for two of its three
// entity_id shapes, two live 200 entries:
// pr_entity_id_bound_200 (entity_id bound to pr_id, produced by GET
// /api/v1/drilldown/prs' own default_window entry as "<repo_id>:<number>",
// parseRepoEntity's own shape) and issue_entity_id_bound_200 (entity_id
// bound to work_item_id, produced by GET /api/v1/drilldown/issues' own
// default_window entry, a bare string -- the flame "issue" entity_id IS
// the work_item_id, no repo prefix). Both producers run strictly before
// flame in restRunOrder. issue_entity_id_not_found shares the same
// declared-failing baseline as issue_entity_id_bound_200 (its own doc
// comment above) rather than the deterministic zero-row shape
// pr_entity_id_bound_not_found uses: the "issue" statement fails in the
// planner before any row lookup happens, found or not.
//
// The THIRD entity_type, "deployment": its own three deterministic-refusal
// branches (missing repo prefix, invalid repo uuid, missing suffix -- the
// same parseRepoEntity, services/flame.py:51-61, the "pr" entries above
// already exercise, reached here via the "deployment" call site instead)
// are covered by deployment_entity_id_missing_repo_prefix/_invalid_repo_
// uuid/_missing_suffix, needing no id at all. entity_id is a
// "<repo_id>:<deployment_id>" pair, and no route this corpus covers
// exposes a deployment_id anywhere in its response body -- not GET
// /api/v1/heatmap (Cell/Evidence carry PRs and commits, no deployments),
// not GET /api/v1/quadrant or /api/v1/explain (metric values only), and
// no drilldown or people route reads the deployments table at all: this
// corpus cannot Produce this id itself (operations.go's own `pr` doc
// comment states the identical rule for the GraphQL corpus's own
// unproducible id). Its declared 200 path, deployment_entity_id_bound_200,
// instead binds entity_id to deployment_entity_id, a
// restOperatorSuppliedProducers entry (this file's own doc comment above
// ValidateRESTIDBindingOrder) resolved from the run invocation's own
// -bind flag rather than from any request here -- a run whose invocation
// omits it refuses that one request by name, the same as any other
// unresolved id. On production data, 0 of the proof organisation's
// 1135 deployments satisfy end > start: every one carries only
// deployed_at (started_at, merged_at, finished_at all NULL) with a
// terminal status, so this entry's own -bind has no live value today,
// and deployment_gap_entity_id_bound_422 below is the deployment
// branch's only live case.
// AssertRESTPathCoverage is satisfied by these entries' PATH regardless.
//
// Branches this corpus cannot provably reach, and why:
//   - The candidate-side 503 (flame_route.go:201-208, the generic
//     fallback the route layer answers for any internal/flame.BuildResponse
//     error that is not one of its own typed *RequestErrors) for every
//     entity_type: this requires an actual ClickHouse read failure, which
//     no request this corpus can construct triggers deterministically.
//   - validateFlameFrames' own "Flame frames have gaps" 422 (services/
//     flame.py:64-82 and 201/265/351; internal/flame/flame.go's own
//     validateFlameFrames, called at flame.go:413, 453, 529) IS reached
//     for all three entity_types now, each by its own gap entry below
//     (deployment_gap_entity_id_bound_422, pr_gap_entity_id_bound_status_
//     divergence, issue_gap_entity_id_bound_status_divergence): a
//     terminal row with no real end signal leaves flameIntervalEnd's own
//     start-as-end branch in play, which is never After(start), so
//     newFrame's own root frame is nil -- the only frame each builder's
//     own validateFlameFrames call can see. A genuinely non-terminal
//     (running/open/in-progress-category) status instead reaches the
//     request-clock branch and answers 200 -- an intentional candidate/
//     baseline divergence for "pr" and "issue" (baseline has no such
//     gate at all), not a defect. Deployment's own running-status
//     divergence is the one case still unasserted: no known production
//     row carries a non-terminal deployment status with no other signal
//     to bind to, so this corpus asserts no live entry for that specific
//     arm.
//   - buildPRFlameResponse's own end selection (flame.go:374-382: MergedAt
//     / ClosedAt / flameIntervalEnd's shared status-gated request-clock
//     fallback, prStatusIsRunning's own "open" check) and its "Review
//     waiting" frame's presence/absence (flame.go:390-394, FirstReviewAt
//     after start or not): the one live "pr" case (pr_entity_id_bound_200)
//     exercises exactly one arm of each per run, determined by whichever
//     PR GET /api/v1/drilldown/prs' own default_window entry names that
//     run (that route's own query orders by newest creation time only,
//     with no status/end-time qualification -- pr_entity_id_bound_200's
//     own declared 200/200 assumes, not guarantees, that the newest PR is
//     not itself a gap row; if it ever is, this entry's own live run
//     reports the mismatch, the authoritative check, not a claim made
//     here) -- not independently selectable without a second,
//     differently-shaped producer. A terminal ("merged" or "closed") PR
//     with neither merged_at nor closed_at set (a sync/lookup gap) instead
//     reaches flameIntervalEnd's own start-as-end branch and answers 422 --
//     the baseline's identical fallback chain (services/flame.py:140) has
//     no such gate and still answers 200 for that shape, an intentional
//     divergence declared live by pr_gap_entity_id_bound_status_divergence
//     below, bound to a deliberately chosen PR rather than relying on
//     whichever row a fresh default_window happens to return.
//   - buildIssueFlameResponse's own end selection (flame.go:429-430:
//     CompletedAt / the same shared flameIntervalEnd, issueStatusIsRunning's
//     own five-category non-terminal set) and its "Backlog waiting" frame's
//     presence/absence (flame.go:438-442, StartedAt after start or not):
//     the same per-run, one-arm-only limitation as the "pr" case above,
//     though GET /api/v1/drilldown/issues' own query (fetchIssuesQuery,
//     drilldown/issues.go) orders by `wct.completed_at DESC` rather than
//     creation time -- ClickHouse sorts a NULL completed_at last in a
//     DESC order, so the pick prefers any row with a real completed_at
//     over a gap row whenever one exists in the window, a narrower
//     exposure than the "pr" case's own creation-time-only order, not a
//     guarantee. Same declared-live "done"/"canceled"-with-no-completed_at
//     divergence class either way, by
//     issue_gap_entity_id_bound_status_divergence below.
//   - reworkWindows' own "requested_changes"/"request_changes" state-alias
//     branches (flame.go:287, alongside the accepted "changes_requested"):
//     unreachable for any row this system has ever written. GitHub's own
//     review ingestion writes the provider's raw state string verbatim
//     (processors/github.py:833,838, `state=str(r.state or "")`), and that
//     provider's own Literal type (api/external_ingest/schemas.py:321) is
//     "APPROVED" | "CHANGES_REQUESTED" | "COMMENTED" | "DISMISSED" |
//     "PENDING" -- never "REQUESTED_CHANGES" or "REQUEST_CHANGES" in any
//     case. GitLab's own mapper (processors/gitlab.py:379-467) never emits
//     a changes-requested-equivalent state at all: GitLab has no native
//     request-changes action, and an unapproval note maps to DISMISSED
//     (gitlab.py:461-467's own comment), not to any changes-requested
//     spelling. reviewState/_review_state (flame.go:255-257, services/
//     flame.py:85-86) only lowercases whatever was written; it invents
//     nothing.

// heatmapDedupParity is shared by every admissible (2xx) heatmap request:
// repos (ReplacingMergeTree(last_synced)) is joined without FINAL by
// every one of api/queries/heatmap.py's seven readers, and
// git_pull_requests/git_commits (same engine/version column) are read
// the same way by review_wait_density/repo_touchpoints and
// active_hours respectively -- the identical mechanism already declared
// for drilldown/prs's own repos+git_pull_requests join (this file's own
// drilldownPRsParity, same ticket, same class of divergence on the same
// table pair). This port's internal/heatmap package reads every one of
// those tables FINAL, with
// org_id inside the same statement (heatmap.go's own package doc
// comment). An unmerged physical version of a repo/PR/commit row can
// only ever ADD an extra row to what a bucket sums or counts, never
// remove one -- a weekday/hour/day/week/repo/file bucket mixes rows from
// MANY different source repos/PRs/commits, only some of which may carry
// the affected table's own unmerged duplicate, so no clean multiplier of
// a bucket's own total is provable in general; the one invariant the
// mechanism DOES guarantee is direction -- the reference plane's cell
// value is always >= this port's own, where they differ. Paths names
// data.cells alone: data.axes carries only the bucket LABEL lists (the
// x/y axis arrays), never a numeric leaf any shape can reach, so citing
// it named coverage this citation could never actually check -- a
// difference surfacing there stays outside every citation, correctly.
// Go is correct.

var heatmapDedupParity = Options{
	OrderInsensitiveLists: []OrderInsensitiveList{
		{
			Path:      "data.cells",
			KeyFields: []string{"x", "y"},
			Reason:    "every one of heatmap.py's seven readers' own GROUP BY carries no secondary sort beyond its own bucket key, identically on both planes; the SAME repos/git_pull_requests/git_commits fan-out this Options' own BaselineDefect declares can move a cell's rank in whatever incidental order the query returns it, shifting every later cell's position under a purely positional comparison. (x, y) is the response's own bucket key, unique per element on each plane's own result set.",
			Ticket:    "CHAOS-5803",
		},
	},
	BaselineDefects: []BaselineDefect{
		{
			Ticket:             "CHAOS-5803",
			Reason:             "repos, and (depending on the requested metric) git_pull_requests or git_commits, are each ReplacingMergeTree(last_synced) (000_raw_tables.sql); api/queries/heatmap.py's readers join repos with no FINAL or other merge-time dedup at all, where this port (internal/heatmap) reads every one of them FINAL, org_id filtered inside the same JOIN's ON clause. An unmerged physical version can only add extra rows to a weekday/hour/day/week/repo/file bucket's sum or count, never remove one, so the reference plane's cell value is always >= this port's own where they differ -- never a clean multiplier, since a bucket mixes rows from many unrelated repos/PRs/commits and only some may carry the duplicate. Go is correct.",
			Paths:              []string{"data.cells"},
			Intermittent:       true,
			IntermittentReason: "present only while repos, git_pull_requests or git_commits (whichever this request's metric reads) holds an unmerged physical version inside the requested window; a comparison taken after the next background merge shows no divergence",
			KeyedDirectionShape: &KeyedDirectionShape{
				ListPath:   "data.cells",
				ValueField: "value",
				ValuePath:  "data.cells.value",
				KeyFields:  []string{"x", "y"},
			},
		},
	},
}

// data.cells.value is heatmap's ONE numeric leaf, but its
// declared type differs by REQUESTED METRIC -- the same dotted path is a
// genuine ClickHouse float aggregate for one metric and a bare row count
// for another. heatmapDedupParity's shared OrderInsensitiveLists/
// BaselineDefects (both keyed on the response SHAPE, not the metric) are
// inherited by value below unchanged; the numeric-leaf declaration is
// per metric and cannot live on heatmapDedupParity itself, because
// heatmapDedupParity is shared across metrics whose declared type
// differs at this one path.
//
// An integer leaf compared under a tolerance because the tolerance
// happens to be safe at this route's magnitudes is not a declared type
// -- repo_touchpoints/active_hours (heatmapRepoTouchpointsParity/
// heatmapActiveHoursParity below) are declared IntegerLeaves precisely
// so that fact is stated rather than left to the tolerance's own
// forgiveness. hotspot_risk/review_wait_density (heatmapHotspotRiskParity/
// heatmapReviewWaitDensityParity below) are declared FloatTierB because
// their own producing aggregate is genuinely float. Each of the four
// Options values below carries the NumericLeavesDeclared marker, so a
// numeric leaf this route's own comparison reaches that is named in
// neither map fails the run rather than defaulting silently.
//
// heatmapcellfloats_test.go's captured evidence (a real 15-cell ULP
// capture, a real ~2x repos-dedup capture) runs against
// heatmapHotspotRiskParity/heatmapReviewWaitDensityParity below -- the
// Options an admissible request actually carries -- so it proves
// something about the live path.
//
// heatmapReviewWaitDensityParity: fetchReviewWaitDensity (heatmap/
// queries.go) selects toFloat64(sum(dateDiff('minute', created_at,
// first_review_at)) / 60.0) AS value -- a fractional-hours duration.
// Declared float by DOMAIN: the leaf can and does land on a non-integer
// value in the ordinary case, whatever a single request's own sum
// happens to divide out to.
var heatmapReviewWaitDensityParity = Options{
	OrderInsensitiveLists: heatmapDedupParity.OrderInsensitiveLists,
	BaselineDefects:       heatmapDedupParity.BaselineDefects,
	NumericLeavesDeclared: true,
	FloatTierB: map[string]string{
		"data.cells.value": "review_wait_density's own toFloat64(sum(dateDiff('minute',created_at,first_review_at))/60.0) (heatmap/queries.go fetchReviewWaitDensity) -- a fractional-hours duration.",
	},
}

// heatmapRepoTouchpointsAxisTieGroupDefect narrows this route's own
// generic-branch axis sort (_axis_order's/axisOrder's default case) to
// the ONE consequence a genuine total-value TIE produces on data.axes.y:
// axisOrder's stable sort breaks a tie by name ascending
// (heatmap/response.go), where _axis_order's own stable sort (services/
// heatmap.py) keeps breaking it by row-encounter order -- unverifiable
// from the wire, and never fixed to match. See HeatmapAxisTieGroupShape's
// own doc comment (heatmapaxistiegroup.go) for the whole-axis invariant:
// it admits the axis only when each leg's axis is the ordering of its own
// cell totals over the same names; a cell value that differs between the
// legs is judged by heatmapDedupParity's and the team-scope declarations,
// never by this one.
var heatmapRepoTouchpointsAxisTieGroupDefect = BaselineDefect{
	Ticket:             "CHAOS-5965",
	Reason:             "_axis_order's (services/heatmap.py) and axisOrder's (heatmap/response.go) own generic branch are both a stable sort by per-repo total descending; two repositories whose own totals genuinely tie keep whatever order the reference plane's own row-encounter order gives them, where this port breaks the same tie by name ascending. Go is correct.",
	Paths:              []string{"data.axes.y"},
	Intermittent:       true,
	IntermittentReason: "present only while a request's repo_touchpoints result carries at least two repositories whose own total commit counts genuinely tie in this window; a window whose repository totals happen to be pairwise distinct shows no divergence",
	HeatmapAxisTieGroupShape: &HeatmapAxisTieGroupShape{
		CellsListPath: "data.cells",
		CellKeyFields: []string{"x", "y"},
		NameField:     "y",
		AxisListPath:  "data.axes.y",
	},
}

// heatmapRepoTouchpointsParity: fetchRepoTouchpoints selects
// toFloat64(count()) AS value -- a bare row count. Declared integer
// despite the toFloat64(...) wrapper: the cast exists only so the
// ClickHouse driver can scan the result into *float64 (see
// heatmapActiveHoursParity's own doc comment, the identical pattern),
// never because the quantity itself can take a fractional value.
var heatmapRepoTouchpointsParity = Options{
	OrderInsensitiveLists: heatmapDedupParity.OrderInsensitiveLists,
	BaselineDefects:       append(append([]BaselineDefect{}, heatmapDedupParity.BaselineDefects...), heatmapRepoTouchpointsAxisTieGroupDefect),
	NumericLeavesDeclared: true,
	IntegerLeaves: map[string]string{
		"data.cells.value": "repo_touchpoints' own toFloat64(count()) (heatmap/queries.go fetchRepoTouchpoints) -- a bare row count, cast to float64 only so the driver can scan it, never a ClickHouse floating-point aggregate.",
	},
}

// heatmapHotspotRiskParity: fetchHotspotRisk selects
// toFloat64(sum(hotspot_score)) AS value/total -- hotspot_score is a
// Float64 column (file_metrics_daily), so this is the SAME merged,
// order-nondeterministic aggregate class investmentQualityStatsFloats
// and every sibling FloatTierB table in this service already declares --
// not a cast-for-scanning artefact the way repo_touchpoints'/active_
// hours' count() is.
// heatmapHotspotRiskCellBoundaryDefect narrows heatmapDedupParity's own
// repos-join fan-out mechanism (inherited into heatmapHotspotRiskParity
// below via its own shared KeyedDirectionShape entry) to the FULL
// consequence set it produces specifically on hotspot_risk's own
// two-level structure: fetchHotspotRisk's own top_query (api/queries/
// heatmap.py:161-178, internal/queryapi/heatmap/queries.go:244-257)
// selects the top 20 files (services/heatmap.py:367, heatmap.go:322) by
// sum(hotspot_score) BEFORE the per-week detail query builds data.cells,
// and data.axes.y (services/heatmap.py's own `_axis_order` default
// branch; heatmap/response.go's own `axisOrder`) sorts by that SAME
// per-file total. A physically-duplicated repos row inflates every file
// that repository owns by the same integer factor (measured against a
// real production capture, GET /api/v1/heatmap hotspot_risk: one
// repository's own 24 differing shared cells each differed by EXACTLY
// 2.0x, and that same repository alone supplied every one of 30
// baseline-only and 30 candidate-only cells), which can cross the fixed
// top-20 boundary and reorder the file-name axis built from the same
// totals -- see HeatmapCellBoundaryShape's own doc comment
// (heatmapcellboundary.go) for the full derivation.
var heatmapHotspotRiskCellBoundaryDefect = BaselineDefect{
	Ticket:             "CHAOS-5955",
	Reason:             "fetchHotspotRisk's own top-20 file selection and per-week detail query (api/queries/heatmap.py:142-205, heatmap/queries.go:244-305) both derive from repos, ReplacingMergeTree(last_synced), joined without FINAL on the reference plane (api/queries/heatmap.py) and FINAL on this port (heatmap/queries.go). A physically-duplicated repos row inflates every file that repository owns by the same integer factor, which can cross the fixed top-20 boundary -- entering one plane's data.cells at the cost of whichever file currently ranks last on the other plane -- and reorder data.axes.y, whose own sort key is the same per-file total the fan-out inflates. Go is correct.",
	Paths:              []string{"data.cells", "data.axes.y"},
	Intermittent:       true,
	IntermittentReason: "present only while repos holds an unmerged physical version of a repository owning at least one file in this window's top-20 hotspot list; a comparison taken after the next background merge shows no divergence",
	HeatmapCellBoundaryShape: &HeatmapCellBoundaryShape{
		CellsListPath: "data.cells",
		CellKeyFields: []string{"x", "y"},
		FileField:     "y",
		CellValuePath: "data.cells.value",
		AxisListPath:  "data.axes.y",
		Limit:         20,
	},
}

// heatmapHotspotRiskAxisTieGroupDefect is heatmapRepoTouchpointsAxisTie
// GroupDefect's own mechanism over hotspot_risk's file axis instead of
// repo_touchpoints' repo axis: two files whose own totals genuinely tie.
// It composes with heatmapHotspotRiskCellBoundaryDefect above rather than
// replacing it: that defect owns a comparison whose two axes name
// different files (the top-20 boundary); this one owns a comparison whose
// axes name the same files and are each the ordering of their own leg's
// cell totals.
var heatmapHotspotRiskAxisTieGroupDefect = BaselineDefect{
	Ticket:             "CHAOS-5965",
	Reason:             "_axis_order's (services/heatmap.py) and axisOrder's (heatmap/response.go) own generic branch are both a stable sort by per-file total descending; two files whose own totals genuinely tie keep whatever order the reference plane's own row-encounter order gives them, where this port breaks the same tie by name ascending. Go is correct.",
	Paths:              []string{"data.axes.y"},
	Intermittent:       true,
	IntermittentReason: "present only while a request's hotspot_risk result carries at least two files whose own total hotspot scores genuinely tie in this window; a window whose file totals happen to be pairwise distinct shows no divergence",
	HeatmapAxisTieGroupShape: &HeatmapAxisTieGroupShape{
		CellsListPath: "data.cells",
		CellKeyFields: []string{"x", "y"},
		NameField:     "y",
		AxisListPath:  "data.axes.y",
	},
}

var heatmapHotspotRiskParity = Options{
	OrderInsensitiveLists: heatmapDedupParity.OrderInsensitiveLists,
	BaselineDefects:       append(append([]BaselineDefect{}, heatmapDedupParity.BaselineDefects...), heatmapHotspotRiskCellBoundaryDefect, heatmapHotspotRiskAxisTieGroupDefect),
	NumericLeavesDeclared: true,
	FloatTierB: map[string]string{
		"data.cells.value": "hotspot_risk's own toFloat64(sum(hotspot_score)) (heatmap/queries.go fetchHotspotRisk) over the Float64 hotspot_score column -- a genuine merged ClickHouse float aggregate.",
	},
}

// heatmapActiveHoursParity: fetchIndividualActiveHours selects
// toFloat64(count()) AS value, the same bare-count-cast-for-scanning
// shape as repo_touchpoints, over a different source table
// (git_commits).
var heatmapActiveHoursParity = Options{
	OrderInsensitiveLists: heatmapDedupParity.OrderInsensitiveLists,
	BaselineDefects:       heatmapDedupParity.BaselineDefects,
	NumericLeavesDeclared: true,
	IntegerLeaves: map[string]string{
		"data.cells.value": "active_hours' own toFloat64(count()) (heatmap/queries.go fetchIndividualActiveHours) -- a bare row count, cast to float64 only so the driver can scan it.",
	},
}

// heatmapRepoTouchpointsTeamScopeSubsetDefect/heatmapHotspotRiskTeamScope
// SubsetDefect/heatmapReviewWaitDensityTeamScopeSubsetDefect declare
// data.cells as a bounded subset for the three team-scoped heatmap
// metrics: internal/queryapi/heatmap/scopefilter.go's own
// scopeFilterForMetric splices teamscope.RepoCondition(orgID,"repo_id",
// scopeIDs,asOf) (scopefilter.go:144-174) for a team scope, resolving a
// team's repositories from team_repo_ownership through the one shared
// condition every repo-keyed route in this corpus uses. The reference
// plane's own scope_filter_for_metric (api/services/filtering.py:129-147,
// all three metrics call it with metric_scope="repo", services/heatmap.py:
// 299,335,353) resolves the identical team scope through
// resolve_repo_filter_ids -> resolve_repo_ids_for_teams
// (api/queries/scopes.py:72-89), reading DISTINCT repo_id off
// user_metrics_daily.team_id -- a per-author-attribution table, not a
// repository-ownership one (5916-prod-team-scope-facts.tsv: the real
// team's own 2224 distinct ids there match zero rows in repos, so
// resolve_repo_id discards every one of them and the reference's own
// verified repo-id list comes back empty, which build_scope_filter_multi
// reads as "no filter" -- the reference answers ORG-WIDE for a
// team-scoped request while the candidate answers over that team's own
// repositories, team_repo_ownership FINAL: 10 of the organization's 11
// repositories).
//
// repo_touchpoints/hotspot_risk's own GROUP BY already carries the
// bucket's repo/file identity (repos.repo/concat(repos.repo,':',path),
// heatmap/queries.go fetchRepoTouchpoints/fetchHotspotRisk) -- a matched
// (x, y) key's own rows are therefore ALREADY confined to one repository
// before any team scope narrows the read further, so team scope can only
// ever remove a whole key (a repository outside the team), never shrink
// one it keeps: EqualLeaves, not bounded.
var heatmapRepoTouchpointsTeamScopeSubsetDefect = BaselineDefect{
	Ticket:              "CHAOS-5940",
	Reason:              "GET /api/v1/heatmap's repo_touchpoints metric resolves team scope through team_repo_ownership (teamscope.RepoCondition) on the candidate, user_metrics_daily.team_id (empty for this org's real team) on the reference -- the reference answers organization-wide, the candidate over the team's own repositories. repo_touchpoints' own GROUP BY key already names the bucket's own repo (repos.repo), so a matched (x, y) key's own value is untouched by a narrower repo scope; only whole keys for a repository outside the team disappear. Go is correct.",
	Paths:               []string{"data.cells"},
	Intermittent:        true,
	IntermittentReason:  "present only while the requested team's own repositories are a PROPER subset of the organization's; a team owning every repository leaves the two lists identical",
	TeamRepoSubsetShape: &TeamRepoSubsetShape{ListPath: "data.cells", KeyFields: []string{"x", "y"}, EqualLeaves: []string{"value"}},
}

// heatmapRepoTouchpointsAxisOrderDefect narrows heatmapDedupParity's own
// repos-join fan-out mechanism (inherited into heatmapRepoTouchpointsParity
// via its own shared KeyedDirectionShape entry, which already admits the
// value-cell consequence on data.cells.value directionally) to the ONE
// leaf that KeyedDirectionShape's own magnitude-unbounded admission can
// never reach: data.axes.y, repo_touchpoints' own y-axis, which IS the
// repository identity directly (y_field="repo") sorted by per-repo total
// descending (_axis_order's/axisOrder's generic branch). A physically-
// duplicated repos row inflates every commit-count cell that repository
// owns by the SAME integer factor, which can shift that repository's own
// axis rank relative to a repository whose own cells are untouched --
// confirmed against a real production capture (team_scoped): one
// repository's own three shared cells each differed by EXACTLY 2.0x, and
// that inflation alone swapped its own axis position with its
// immediately-lower-ranked neighbour, whose own cells were byte-identical
// on both legs.
//
// This is narrower than HeatmapCellBoundaryShape's own axis rule
// (heatmapcellboundary.go), which HotspotListBoundaryShape's own
// LIMIT-boundary reasoning depends on: that shape's rule 3 requires both
// reconstructed totals maps to be EXACTLY the route's fixed Limit (20)
// long, which a team-scoped repo_touchpoints request never reaches when
// the team owns fewer repositories than the cap (confirmed live: this
// same capture sits at 6 repositories, not 20) -- there is no boundary to
// reason about, only a reorder among the SAME repository set both legs
// already agree on. See HeatmapAxisRepoOrderShape's own doc comment
// (heatmapaxisrepoorder.go) for the full derivation and its own
// baseline-side tie refusal -- a tie purely in the candidate's own
// totals is admitted whenever it matches axisOrder's own deterministic
// name-ascending tie-break exactly.
var heatmapRepoTouchpointsAxisOrderDefect = BaselineDefect{
	Ticket:             "CHAOS-5955",
	Reason:             "fetchRepoTouchpoints' own top-repository selection and per-day detail query (api/queries/heatmap.py:85-135, heatmap/queries.go:143-188) both derive from repos, ReplacingMergeTree(last_synced), joined without FINAL on the reference plane and FINAL on this port. A physically-duplicated repos row inflates every commit-count cell that repository owns by the same integer factor -- already admitted directionally on data.cells.value by heatmapDedupParity's own KeyedDirectionShape entry above -- which can also reorder data.axes.y, whose own sort key (_axis_order's/axisOrder's generic branch) is the SAME per-repository total the fan-out inflates. Go is correct.",
	Paths:              []string{"data.axes.y"},
	Intermittent:       true,
	IntermittentReason: "present only while repos holds an unmerged physical version of a repository whose own inflated total crosses a neighbouring repository's total in this window's axis ranking; a comparison taken after the next background merge shows no divergence",
	HeatmapAxisRepoOrderShape: &HeatmapAxisRepoOrderShape{
		CellsListPath: "data.cells",
		CellKeyFields: []string{"x", "y"},
		RepoField:     "y",
		AxisListPath:  "data.axes.y",
	},
}

var heatmapHotspotRiskTeamScopeSubsetDefect = BaselineDefect{
	Ticket:              "CHAOS-5940",
	Reason:              "the same team-repository-resolution mechanism as heatmapRepoTouchpointsTeamScopeSubsetDefect, over hotspot_risk. hotspot_risk's own GROUP BY key already names the bucket's own repo (concat(repos.repo,':',path) AS file_key, heatmap/queries.go fetchHotspotRisk), so a matched (x, y) key's own value is untouched by a narrower repo scope; only whole keys for a repository outside the team disappear. Go is correct.",
	Paths:               []string{"data.cells"},
	Intermittent:        true,
	IntermittentReason:  "present only while the requested team's own repositories are a PROPER subset of the organization's; a team owning every repository leaves the two lists identical",
	TeamRepoSubsetShape: &TeamRepoSubsetShape{ListPath: "data.cells", KeyFields: []string{"x", "y"}, EqualLeaves: []string{"value"}},
}

// heatmapReviewWaitDensityTeamScopeSubsetDefect differs from its two
// siblings above in exactly one way: review_wait_density's own GROUP BY
// (weekday, hour) (heatmap/queries.go fetchReviewWaitDensity) carries NO
// repo identity at all -- a matched (x, y) key's own
// sum(dateDiff('minute', created_at, first_review_at))/60.0 is a sum
// over EVERY pull request across every repo the request's own scope
// admits that shares that (weekday, hour) bucket, so a narrower team
// scope can genuinely shrink a matched key's own value, never merely
// remove a whole key. dateDiff('minute', created_at, first_review_at) is
// bound non-negative in practice by definition_wait_evidence's own
// caller (a PR's first review cannot precede its own creation on
// admissible data, the same domain assumption heatmapReviewWaitDensity
// Parity's own FloatTierB entry already carries for this leaf) --
// BoundedLeavesAllKeys, not BoundedLeafKeys naming individual (weekday,
// hour) keys one by one: every one of the 7*24 buckets this metric can
// ever produce is a repo-spanning sum by construction, with no
// repo-specific bucket among them the way repo_touchpoints/hotspot_risk
// carry.
var heatmapReviewWaitDensityTeamScopeSubsetDefect = BaselineDefect{
	Ticket:              "CHAOS-5940",
	Reason:              "GET /api/v1/heatmap's review_wait_density metric resolves team scope the same way as repo_touchpoints/hotspot_risk (heatmapRepoTouchpointsTeamScopeSubsetDefect's own Reason states the two tables in full), but its own GROUP BY (weekday, hour) carries no repo identity: a matched key's own value is a sum of non-negative per-repo contributions (dateDiff('minute', created_at, first_review_at)/60.0 summed over every admitted PR sharing that bucket), so a genuinely narrower team scope can only ever pull a matched key's own sum down, never up. Go is correct.",
	Paths:               []string{"data.cells"},
	Intermittent:        true,
	IntermittentReason:  "present only while the requested team's own repositories are a PROPER subset of the organization's AND at least one (weekday, hour) bucket mixes a team-owned repository's own pull requests with a non-team repository's; a team owning every repository, or a window whose every bucket's own PRs all belong to the team, leaves the two lists identical",
	TeamRepoSubsetShape: &TeamRepoSubsetShape{ListPath: "data.cells", KeyFields: []string{"x", "y"}, BoundedLeavesAllKeys: []string{"value"}},
}

var heatmapReviewWaitDensityTeamScopedParity = Options{
	OrderInsensitiveLists: heatmapReviewWaitDensityParity.OrderInsensitiveLists,
	NumericLeavesDeclared: true,
	FloatTierB:            heatmapReviewWaitDensityParity.FloatTierB,
	BaselineDefects:       append(append([]BaselineDefect{}, heatmapReviewWaitDensityParity.BaselineDefects...), heatmapReviewWaitDensityTeamScopeSubsetDefect),
}

var heatmapRepoTouchpointsTeamScopedParity = Options{
	OrderInsensitiveLists: heatmapRepoTouchpointsParity.OrderInsensitiveLists,
	NumericLeavesDeclared: true,
	IntegerLeaves:         heatmapRepoTouchpointsParity.IntegerLeaves,
	BaselineDefects:       append(append([]BaselineDefect{}, heatmapRepoTouchpointsParity.BaselineDefects...), heatmapRepoTouchpointsTeamScopeSubsetDefect, heatmapRepoTouchpointsAxisOrderDefect),
}

var heatmapHotspotRiskTeamScopedParity = Options{
	OrderInsensitiveLists: heatmapHotspotRiskParity.OrderInsensitiveLists,
	NumericLeavesDeclared: true,
	FloatTierB:            heatmapHotspotRiskParity.FloatTierB,
	BaselineDefects:       append(append([]BaselineDefect{}, heatmapHotspotRiskParity.BaselineDefects...), heatmapHotspotRiskTeamScopeSubsetDefect),
}

// sankeyRepoDedupParity is shared by every admissible (2xx) sankey
// request whose mode joins repos: investment (fetch_investment_flow_items'
// own LEFT JOIN repos) and hotspot (fetch_hotspot_rows' two quantile CTEs
// plus its main SELECT, all three joining repos). repos
// (ReplacingMergeTree(last_synced)) is joined by api/queries/sankey.py
// with no FINAL or org_id scoping at all -- the identical mechanism
// already declared for heatmap's own repos join (this file's own
// heatmapDedupParity, same ticket). This port's internal/sankey package
// reads repos FINAL, org_id inside the same JOIN's ON clause (queries.go's
// own doc comments). An unmerged physical version of a repo row can
// surface as an extra/relabeled target node (investment mode) or an
// extra/relabeled repo/directory/file node (hotspot mode). Unlike
// heatmap's own mechanism, HERE the join is keyed by one specific
// repo_id feeding every row that touches it, so its fan-out is a CLEAN,
// UNIFORM integer multiplier across the whole repo-rooted subtree that
// repo's own edges reach -- confirmed live in a production deployed-
// vs-deployed capture: repo full-chaos/script-manifest fanned out at
// EXACTLY 2.0x, uniformly, across both the two theme->repo edges
// targeting it in investment mode AND all three edges of its own
// repo->directory->file->change_type chain in hotspot mode. Paths names
// data.links alone: on GET/POST /api/v1/sankey SPECIFICALLY -- the only
// two routes this Options value covers -- Node.Value is always null,
// because THIS route's own node-building code (sankey/response.go's
// nodeAccumulator.touchNode) never sets it, so data.nodes carries no
// numeric leaf any shape could reach here, and citing it would be the
// appearance of coverage rather than coverage itself -- a node-level
// difference (an extra, missing or relabeled node name) is structural
// and stays outside every citation, correctly. Go is correct.
//
// THIS DOES NOT GENERALIZE to every route answering the sankey.Response
// struct: POST /api/v1/investment/flow and /investment/flow/repo-team
// reuse the SAME Node type but populate it through a DIFFERENT
// node-building code path (investmentflow/builders.go's own
// nodePresence/nodeRunningTotal), which DOES set a real running-total
// Value per node -- see investmentFlowRepoDedupParity's own
// SankeyRepoFanoutShape entry (NodeValuePath: "data.nodes.value") for
// where that route's own node-level coverage is declared. Reasoning from
// THIS struct's field default to THAT route's wire behaviour would
// produce exactly the wrong declaration.
//
// maxHotspotRowsCorpus mirrors maxHotspotRows (sankey.go:97) and
// MAX_HOTSPOT_ROWS (services/sankey.py:59) -- neither package is
// importable from here (sankey.go's constant is unexported inside
// internal/queryapi/sankey, and this package never imports a cmd
// tree), so HotspotListBoundaryShape's own Limit field is set from this
// mirrored value rather than the source constant directly. Hotspot's own
// query params carry no limit field on either plane (established from
// source: services/sankey.py's own _build_hotspot_flow never reads
// filters.limit), so this is the route's one fixed value, never a
// per-request override.
const maxHotspotRowsCorpus = 150

var sankeyRepoDedupParity = Options{
	OrderInsensitiveLists: []OrderInsensitiveList{
		{
			Path:      "data.nodes",
			KeyFields: []string{"name"},
			Reason:    "_touch_node/_add_edge (services/sankey.py) and this port's own nodeAccumulator/edgeAccumulator (response.go) both build nodes in first-touch order over a row set neither plane's own SQL fully orders (investment mode's GROUP BY source, target ORDER BY value DESC, hotspot mode's per-file ranking), so a near-tied value can break the stable tie the same fan-out that changes a link's value also disturbs. name is unique per node on each plane's own result set.",
			Ticket:    "CHAOS-5873",
		},
		{
			Path:      "data.links",
			KeyFields: []string{"source", "target"},
			Reason:    "_links_from_edges (services/sankey.py) and this port's own edgeAccumulator.links() (response.go) both sort descending by value with no secondary key beyond first-touch insertion order, identically on both planes; confirmed live in a production deployed-vs-deployed capture (GET /api/v1/sankey investment_default_org and hotspot_org): once paired by (source, target) the two planes' link sets are IDENTICAL, and every reported difference that looked like a source/target mismatch was pure position-shift noise from this same unordered tie-break, not a real divergence. (source, target) is unique per edge on each plane's own result set.",
			Ticket:    "CHAOS-5873",
		},
	},
	BaselineDefects: []BaselineDefect{
		{
			Ticket:             "CHAOS-5803",
			Reason:             "repos is ReplacingMergeTree(last_synced) (000_raw_tables.sql); api/queries/sankey.py's reader joins it with no FINAL or org_id scoping at all, where this port (internal/sankey) reads it FINAL, org_id filtered inside the JOIN's own ON clause. An unmerged physical version of a repo row can surface as an extra or relabeled node, and fans out EVERY row that joins through it -- since the join is keyed by one repo_id, the resulting inflation is a clean, uniform integer multiplier across the whole repo-rooted subtree that repo's own edges reach, confirmed live: repo full-chaos/script-manifest fanned out at exactly 2.0x, uniformly, across both its investment-mode theme->repo edges and all three edges of its hotspot-mode repo->directory->file->change_type chain, in a production deployed-vs-deployed capture. Go is correct.",
			Paths:              []string{"data.links"},
			Intermittent:       true,
			IntermittentReason: "present only while repos holds an unmerged physical version inside the requested window; a comparison taken after the next background merge shows no divergence",
			SankeyRepoFanoutShape: &SankeyRepoFanoutShape{
				NodesListPath:       "data.nodes",
				LinksListPath:       "data.links",
				LinkValuePath:       "data.links.value",
				RepoNodeGroups:      []string{"project", "repo"},
				FallbackAnchorNames: []string{"Other", "Unknown repo"},
			},
		},
	},
	// this Options value covers hotspot mode alone (both
	// hotspot_org and hotspot_repo_scoped). data.links.value there is
	// fetchHotspotRows' own CAST(sum(metrics.churn) AS Float64) (sankey/
	// queries.go) -- churn is UInt32 per file_metrics_daily's own DDL, and
	// the code comment at that call site says exactly why the Float64
	// cast exists: "the driver refuses to scan a UInt64 result into
	// *float64", never because the quantity itself can be fractional.
	// Declared integer: a sum of an integer column is exact regardless of
	// merge order, unlike hotspot_score's OWN Float64 sum on the
	// heatmap.hotspot_risk route (see heatmapHotspotRiskParity).
	NumericLeavesDeclared: true,
	IntegerLeaves: map[string]string{
		"data.links.value": "hotspot mode's own CAST(sum(metrics.churn) AS Float64) (sankey/queries.go fetchHotspotRows) over the UInt32 churn column -- a bare integer sum, cast to float64 only so the driver can scan it.",
	},
}

// sankeyHotspotParity extends sankeyRepoDedupParity with hotspot mode's
// own two-level consequence of the SAME repos-join fan-out:
// fetch_hotspot_rows/fetchHotspotRows (queries.go:401-478, api/queries/
// sankey.py:136-208) both GROUP BY repo, directory, file_path, ORDER BY
// churn DESC and LIMIT maxHotspotRows/MAX_HOTSPOT_ROWS (sankey.go:97,
// services/sankey.py:59) identically -- never a per-request override,
// hotspot's own query params carry no limit field. A value-multiplying
// row from the sibling SankeyRepoFanoutShape entry can (a) cross that
// file-list boundary, the same rank-displacement LimitDisplacementShape
// (limitdisplacement.go) admits for a flat list, and (b) shift a
// repo->directory parent link's own value, which _build_hotspot_flow/
// buildHotspotFlow (services/sankey.py:513-532, sankey/builders.go:
// 230-249) build as the SUM of every LISTED file's own churn under that
// directory -- a consequence a flat list has no parent to produce. This
// is the ONLY route in this file whose sankey response carries that
// second, hierarchical level, so this extension is kept OFF
// sankeyRepoDedupParity itself (and therefore off sankeyInvestmentParity
// below, which inherits from it): investment mode's own nodes never
// carry a "directory"/"file" group (services/sankey.py's own
// _build_investment_flow touches only "initiative"/"project"), so the
// shape would find nothing to reconstruct there and admit nothing --
// harmless, but a defect entry that can never fire on a route it is
// nominally attached to is exactly the kind of stale-looking citation
// this package's own liveUnexplainedDefects accounting exists to flag,
// so it is scoped to hotspot's own Parity instead. Go is correct.
var sankeyHotspotParity = Options{
	OrderInsensitiveLists: sankeyRepoDedupParity.OrderInsensitiveLists,
	BaselineDefects: append(append([]BaselineDefect{}, sankeyRepoDedupParity.BaselineDefects...), BaselineDefect{
		Ticket:             "CHAOS-5803",
		Reason:             "the SAME unmerged repos row the sibling SankeyRepoFanoutShape entry above declares also moves which files rank inside hotspot's own bounded, value-DESC list, and shifts a repo->directory parent link's own value by the leaving/entering files' contribution plus the fan-out's own inflation of the children still shared by both legs -- see HotspotListBoundaryShape's own doc comment (hotspotlistboundary.go) for the full derivation. Go is correct.",
		Paths:              []string{"data.links", "data.nodes"},
		Intermittent:       true,
		IntermittentReason: "present only while repos holds an unmerged physical version inside the requested window AND the affected repository has a file close enough to hotspot's own list boundary to cross it; a comparison taken after the next background merge, or with no such boundary crossing, shows no divergence",
		HotspotListBoundaryShape: &HotspotListBoundaryShape{
			NodesListPath:      "data.nodes",
			LinksListPath:      "data.links",
			LinkValuePath:      "data.links.value",
			RepoNodeGroup:      "repo",
			DirectoryNodeGroup: "directory",
			FileNodeGroup:      "file",
			Limit:              maxHotspotRowsCorpus,
		},
	}),
	NumericLeavesDeclared: sankeyRepoDedupParity.NumericLeavesDeclared,
	IntegerLeaves:         sankeyRepoDedupParity.IntegerLeaves,
}

// sankeyInvestmentParity extends sankeyRepoDedupParity with the
// investment mode's second, independent divergence source: the
// argMax-tuple fix analytics.LatestWorkUnitInvestmentsSource() carries
// for work_unit_investments.repo_id (a Nullable(UUID) column) -- the same
// mechanism-class defect already declared for the GraphQL investmentFull
// operation's own coverage paths (this repo's investmentFull corpus
// entry, same class ticket). A work unit whose newest generation cleared
// repo_id relative to an earlier generation reads the TRUE latest
// (possibly-null) value here, where api/queries/sankey.py's own
// LATEST_WORK_UNIT_INVESTMENTS_CTE import null-skips to a stale non-null
// repo_id -- changing which target node that work unit's effort lands on.
// This is a RELABELLING, not an addition: the work unit's effort moves
// from one target to another, it is not created or destroyed, so the
// population's link-value total is CONSERVED even though an individual
// edge's value is not -- unlike sankeyRepoDedupParity's own repos-join
// mechanism (which can only ever add rows, so a per-edge direction or
// multiplier holds), no single edge's sign is predictable here, only the
// whole list's total.
var sankeyInvestmentParity = Options{
	OrderInsensitiveLists: []OrderInsensitiveList{
		{
			Path:      "data.nodes",
			KeyFields: []string{"name"},
			Reason:    "_touch_node/_add_edge (services/sankey.py) and this port's own nodeAccumulator/edgeAccumulator (response.go) both build nodes in first-touch order over a row set neither plane's own SQL fully orders (investment mode's GROUP BY source, target ORDER BY value DESC), so a near-tied value can break the stable tie the same fan-out that changes a link's value also disturbs. name is unique per node on each plane's own result set.",
			Ticket:    "CHAOS-5873",
		},
		{
			Path:      "data.links",
			KeyFields: []string{"source", "target"},
			Reason:    "_links_from_edges (services/sankey.py) and this port's own edgeAccumulator.links() (response.go) both sort descending by value with no secondary key beyond first-touch insertion order, identically on both planes; confirmed live in a production deployed-vs-deployed capture (GET /api/v1/sankey investment_default_org): once paired by (source, target) the two planes' link sets are IDENTICAL, and every reported difference that looked like a source/target mismatch was pure position-shift noise from this same unordered tie-break, not a real divergence. (source, target) is unique per edge on each plane's own result set.",
			Ticket:    "CHAOS-5873",
		},
	},
	BaselineDefects: append([]BaselineDefect{
		{
			Ticket:             "CHAOS-4547",
			Reason:             "work_unit_investments.repo_id is Nullable(UUID) (017_investment_materialize_tables.sql); api/queries/investment.py's LATEST_WORK_UNIT_INVESTMENTS_CTE (imported unchanged by api/queries/sankey.py's fetch_investment_flow_items) dedups it via a bare argMax(repo_id, computed_at), which SKIPS a row whose repo_id is NULL when picking the newest version, returning a STALE non-null repo_id from an earlier generation instead of the true latest value. This port reuses analytics.LatestWorkUnitInvestmentsSource(), which tuple-wraps repo_id -- (argMax(tuple(repo_id), computed_at)).1 -- and therefore reads the true latest value. A work unit whose newest generation cleared repo_id relative to an earlier one changes which target node (a resolved repo, or the \"Other\" fallback) its effort lands on -- a redistribution of the SAME total effort, never a change in it. Paths names data.links alone, not data.nodes: on GET/POST /api/v1/sankey specifically, Node.Value is always null (sankey/response.go's own nodeAccumulator.touchNode never sets it, unlike investment/flow's own node builders -- see sankeyRepoDedupParity's own doc comment for that contrast), so no numeric leaf under data.nodes exists for any shape to reach on THIS route. Go is correct.",
			Paths:              []string{"data.links"},
			Intermittent:       true,
			IntermittentReason: "present only while at least one work unit in the requested window has a newer generation whose repo_id differs (including a NULL transition) from an earlier generation's; a request whose work units never re-categorize shows no divergence",
			ConservationShape: &ConservationShape{
				ListPath:   "data.links",
				ValueField: "value",
				ValuePath:  "data.links.value",
			},
		},
	}, sankeyRepoDedupParity.BaselineDefects...),
	// investment mode's data.links.value is
	// fetchInvestmentFlowItems' own sum(theme_kv.2 * work_unit_
	// investments.effort_value) (sankey/queries.go) -- effort_value is
	// Float64 (017_investment_materialize_tables.sql) and theme_kv.2 is
	// Float32 (the ARRAY JOIN's own CAST(...AS Array(Tuple(String,
	// Float32)))), so this is a genuine merged ClickHouse float
	// aggregate, the same class investmentQualityStatsFloats and every
	// sibling FloatTierB table in this service already declares.
	NumericLeavesDeclared: true,
	FloatTierB: map[string]string{
		"data.links.value": "investment mode's own sum(theme_kv.2 * effort_value) (sankey/queries.go fetchInvestmentFlowItems) over Float64 effort_value/Float32 theme weight -- a genuine merged ClickHouse float aggregate.",
	},
}

// sankeyCycleTimesDedupParity is the expense mode's own declared
// divergence: work_item_cycle_times is ReplacingMergeTree(computed_at)
// (001_metrics_v2.sql, ordered by (provider, work_item_id)) since its
// very first migration; api/queries/sankey.py's fetch_expense_abandoned
// reads it with no FINAL or other dedup at all -- the same class of
// divergence sankeyRepoDedupParity declares for repos, on a different
// table. This port reads it FINAL. An unmerged physical version of a
// work item's cycle-times row (a redrive/recompute) can double-count it
// into canceled_items -- buildExpenseFlow (builders.go) computes
// abandoned = max(0, min(rework, canceledItems)), and min() is
// monotonic, so however the clamp lands, a double-counted canceledItems
// can only push the Rework->Abandonment / rewrite edge's baseline value
// UP relative to this port's own, never down. canceled_items feeds
// EXACTLY that one edge in buildExpenseFlow -- no other edge derives
// from it -- so the mechanism has a fixed, single (source, target) key,
// not a family of them.
var sankeyCycleTimesDedupParity = Options{
	OrderInsensitiveLists: []OrderInsensitiveList{
		{
			Path:      "data.links",
			KeyFields: []string{"source", "target"},
			Reason:    "this port's own edgeAccumulator.links() (response.go) sorts every mode's links descending by value, expense mode's fixed four included, with no secondary key beyond first-touch insertion order; the SAME work_item_cycle_times dedup gap this Options' own BaselineDefect declares changes canceled_items' derived value, which can move the Rework->Abandonment / rewrite edge's own rank among the other three fixed edges and shift a later one's position under a purely positional comparison. (source, target) is unique per edge in this fixed four-edge set.",
			Ticket:    "CHAOS-5803",
		},
	},
	BaselineDefects: []BaselineDefect{
		{
			Ticket:             "CHAOS-5803",
			Reason:             "work_item_cycle_times is ReplacingMergeTree(computed_at) (001_metrics_v2.sql); api/queries/sankey.py's fetch_expense_abandoned reads it with no FINAL or other dedup at all, where this port (internal/sankey) reads it FINAL. An unmerged physical version of a work item's cycle-times row can double-count it into canceled_items, changing the Rework->Abandonment / rewrite edge's value -- buildExpenseFlow's own abandoned = max(0, min(rework, canceledItems)) is monotonic in canceledItems, so this always pushes the reference plane's edge value UP relative to this port's own, never down. Go is correct.",
			Paths:              []string{"data.links"},
			Intermittent:       true,
			IntermittentReason: "present only while work_item_cycle_times holds an unmerged physical version of some work item's row inside the requested window; a comparison taken after the next background merge shows no divergence",
			KeyedDirectionShape: &KeyedDirectionShape{
				ListPath:   "data.links",
				ValueField: "value",
				ValuePath:  "data.links.value",
				KeyFields:  []string{"source", "target"},
				Keys:       [][]string{{"Rework", "Abandonment / rewrite"}},
			},
		},
	},
	// expense mode's data.links.value is shared by THREE
	// edges (Planned->Unplanned, Unplanned->Rework, Rework->Abandonment),
	// and FloatTierB/IntegerLeaves declare by DOTTED PATH, index-free --
	// there is no per-element declaration, so the path's own declared type
	// is the JOIN of all three edges' provenances (the same "join of
	// provenances" rule that decides one edge's own type when it is itself
	// built from several sources):
	//   - Planned->Unplanned = max(0, newBugs); newBugs =
	//     CAST(sum(new_bugs_count) AS Float64) (sankey/queries.go
	//     fetchExpenseCounts) over UInt32 new_bugs_count -- integer
	//     provenance.
	//   - Unplanned->Rework = max(0, min(unplanned, bugCompleted));
	//     bugCompleted = sum(items_completed * bug_completed_ratio) --
	//     items_completed (count) times bug_completed_ratio (a Float64
	//     ratio), summed -- genuine float provenance. min(integer-
	//     provenance, float-provenance) has a domain that includes
	//     non-integers, so THIS edge alone is float by the join rule.
	//   - Rework->Abandonment = max(0, min(rework, canceledItems));
	//     canceledItems = CAST(countIf(status='canceled') AS Float64) --
	//     integer provenance, but rework (above) is already float, so this
	//     edge is float too.
	// Two of the three edges are float-domain, so the PATH's own declared
	// type is float. Declaring it Tier B is safe for the one integer-only
	// edge as well as the two mixed ones: at value V the tolerance is
	// max(1e-9, 1e-9*V), so a genuine integer difference of 1 on
	// Planned->Unplanned still clears the mismatch threshold for any V
	// below one billion -- nothing real hides behind this tolerance at any
	// count this route can plausibly carry.
	NumericLeavesDeclared: true,
	FloatTierB: map[string]string{
		"data.links.value": "expense mode's data.links.value is shared by three edges (see this Options' own doc comment for the full derivation); Unplanned->Rework and Rework->Abandonment are each min() of an integer- and a float-provenance operand, which makes the path float by the join-of-provenances rule, and Planned->Unplanned's own integer-only value still clears a real 1-count difference under Tier B's tolerance for any value below ~1e9.",
	},
}

// sankeyStateFlowParity is state mode's own declared numeric-leaf type
// (GET+POST /api/v1/sankey mode=state): buildStateFlow (sankey/
// builders.go) derives every one of its six edges (flowBacklog,
// flowTodo, blockedFlow, reviewFlow, canceledFlow, doneFlow) purely via
// min()/max()/subtraction over statusCounts, itself a sum of
// fetchStateStatusCounts' own CAST(sum(items_touched) AS Float64)
// (sankey/queries.go) -- items_touched is UInt32 per work_item_state_
// durations_daily's own DDL, cast to float64 only so the driver can scan
// it. Unlike expense mode, every operand every edge here composes shares
// the SAME integer provenance -- no min()/max() here ever mixes an
// integer- and a float-provenance operand -- so the join-of-provenances
// rule gives a single, unambiguous answer: integer, for the whole path,
// with no per-edge ambiguity to resolve.
//
// state mode carries no repos/git_pull_requests/git_commits join (it
// reads work_item_state_durations_daily alone), so it has no baseline
// defect or ordering declaration of its own -- this Options value exists
// solely to carry the numeric-leaf declaration.
var sankeyStateFlowParity = Options{
	NumericLeavesDeclared: true,
	IntegerLeaves: map[string]string{
		"data.links.value": "state mode's six edges are all min()/max()/subtraction over statusCounts, itself a sum of CAST(sum(items_touched) AS Float64) (sankey/queries.go fetchStateStatusCounts) over the UInt32 items_touched column -- every operand shares the same integer provenance, no mixing.",
	},
}

// sankeyNodesTeamScopeSubsetDefect/sankeyLinksTeamScopeSubsetDefect
// declare data.nodes/data.links as a bounded subset for a team-scoped
// GET/POST /api/v1/sankey request under investment or hotspot mode --
// the two modes whose team scope resolves a repo set at all
// (repoScopeFilter, sankey/scopefilter.go:148-171, splicing
// teamscope.RepoCondition the same way every other repo-keyed route in
// this corpus does). expense/state mode resolve team scope through a
// DIFFERENT, unaffected mechanism (see sankeyExpenseStateTeamScoped
// Parity's own doc comment below) and carry no citation here.
//
// The reference plane resolves the identical scope through
// resolve_repo_filter_ids -> resolve_repo_ids_for_teams
// (api/queries/scopes.py:72-89, the same user_metrics_daily.team_id read
// every sibling team-scope defect in this file cites) -- empty for this
// org's real team, so the reference answers organization-wide while the
// candidate answers over the team's own repositories.
//
// EqualLeaves, not BoundedLeavesAllKeys, for data.links.value: unlike
// heatmap's review_wait_density or investment/flow's category/team-level
// nodes, EVERY edge either mode produces is already GROUPED BY a column
// that names one specific repository -- investment mode's own GROUP BY
// source, target with target = r.repo (sankey/queries.go
// fetchInvestmentFlowItems:158), hotspot mode's own repo/directory/file
// chain rooted at one repo per row (fetchHotspotRows, same file) -- so a
// matched (source, target) key's own value is untouched by a narrower
// repo scope; only whole edges for a repository outside the team
// disappear. data.nodes carries no numeric leaf on THIS route at all
// (sankeyRepoDedupParity's own doc comment: Node.Value is always null
// here), so its own citation is membership plus group equality only.
var sankeyNodesTeamScopeSubsetDefect = BaselineDefect{
	Ticket:              "CHAOS-5940",
	Reason:              "GET/POST /api/v1/sankey's investment/hotspot modes resolve team scope through team_repo_ownership (teamscope.RepoCondition) on the candidate, user_metrics_daily.team_id (empty for this org's real team) on the reference -- the reference answers organization-wide, the candidate over the team's own repositories. Node.Value is always null on this route (sankeyRepoDedupParity's own doc comment), so this citation is membership plus group equality only. Go is correct.",
	Paths:               []string{"data.nodes"},
	Intermittent:        true,
	IntermittentReason:  "present only while the requested team's own repositories are a PROPER subset of the organization's; a team owning every repository leaves the two lists identical",
	TeamRepoSubsetShape: &TeamRepoSubsetShape{ListPath: "data.nodes", KeyFields: []string{"name"}, EqualLeaves: []string{"group"}},
}

var sankeyLinksTeamScopeSubsetDefect = BaselineDefect{
	Ticket:              "CHAOS-5940",
	Reason:              "the same team-repository-resolution mechanism as sankeyNodesTeamScopeSubsetDefect, over data.links. Both modes' own GROUP BY already names one specific repository per edge (investment mode's target = r.repo, hotspot mode's whole chain rooted at one repo per row), so a matched (source, target) key's own value is untouched by a narrower repo scope; only whole edges for a repository outside the team disappear. Go is correct.",
	Paths:               []string{"data.links"},
	Intermittent:        true,
	IntermittentReason:  "present only while the requested team's own repositories are a PROPER subset of the organization's; a team owning every repository leaves the two lists identical",
	TeamRepoSubsetShape: &TeamRepoSubsetShape{ListPath: "data.links", KeyFields: []string{"source", "target"}, EqualLeaves: []string{"value"}},
}

// sankeyInvestmentTeamScopedParity/sankeyHotspotTeamScopedParity: GET/POST
// /api/v1/sankey's own team_scoped entries for the two modes above.
var sankeyInvestmentTeamScopedParity = Options{
	OrderInsensitiveLists: sankeyInvestmentParity.OrderInsensitiveLists,
	NumericLeavesDeclared: sankeyInvestmentParity.NumericLeavesDeclared,
	FloatTierB:            sankeyInvestmentParity.FloatTierB,
	BaselineDefects: append(append([]BaselineDefect{}, sankeyInvestmentParity.BaselineDefects...),
		sankeyNodesTeamScopeSubsetDefect, sankeyLinksTeamScopeSubsetDefect),
}

var sankeyHotspotTeamScopedParity = Options{
	OrderInsensitiveLists: sankeyHotspotParity.OrderInsensitiveLists,
	NumericLeavesDeclared: sankeyHotspotParity.NumericLeavesDeclared,
	IntegerLeaves:         sankeyHotspotParity.IntegerLeaves,
	BaselineDefects: append(append([]BaselineDefect{}, sankeyHotspotParity.BaselineDefects...),
		sankeyNodesTeamScopeSubsetDefect, sankeyLinksTeamScopeSubsetDefect),
}

// expense/state mode's own team_scoped requests (below, in the GET/POST
// /api/v1/sankey endpoint specs) reuse sankeyCycleTimesDedupParity/
// sankeyStateFlowParity UNCHANGED -- FREE COVERAGE, not part of this
// ticket's own defect class: _team_scope_filter (services/sankey.py:
// 192-198) and this port's own teamScopeFilter (sankey/scopefilter.go:
// 175-180) both filter the SAME team_id column directly
// (ifNull(nullIf(team_id, ''), 'unassigned'), sankey/builders.go:
// 77-79,152-154 -- the identical column expression, confirmed by reading
// both call sites), never team_repo_ownership/user_metrics_daily at all
// -- these two modes never had the defect the other four routes in this
// ticket share, so a team-scoped request here is expected to be a PLAIN
// MATCH, and needs no new Shape or Parity variant.

// investmentFlowRepoDedupParity is shared by every admissible (2xx)
// investment/flow and investment/flow/repo-team request whose fetcher
// joins repos (every one of the five edge fetchers except fetch_
// investment_team_edges, per queries.go's own package doc comment) --
// the SAME defect class already declared for heatmap/sankey's own repos
// joins (this file's heatmapDedupParity/sankeyRepoDedupParity, same
// ticket): api/queries/investment.py's readers join `repos` with no
// FINAL or org_id scoping at all, where this port (internal/
// investmentflow) reads it FINAL, org_id inside the JOIN's own ON
// clause. An unmerged physical version of a repo row can surface as an
// extra/relabeled repo node anywhere in the response. Go is correct.
//
// STACKED with the SAME argMax-tuple-fix divergence sankeyInvestmentParity
// already declares for the identical shared CTE: api/queries/investment.py
// defines LATEST_WORK_UNIT_INVESTMENTS_CTE itself (api/queries/sankey.py
// imports that SAME definition by name for its own investment mode), and
// this port's every fetcher composes analytics.LatestWorkUnitInvestments
// Source() -- the Go-fixed, tuple-wrapped port of that exact CTE -- so a
// work unit whose newest generation cleared repo_id (or work_unit_type/
// work_unit_name/provider) relative to an earlier generation reads the
// TRUE latest (possibly-null) value here, where Python's own reader
// null-skips to a stale non-null value from an earlier generation.
//
// STACKED a third time with the work_unit_supersessions exclusion
// analytics.LatestWorkUnitInvestmentsSource() also carries unconditionally
// (see investmentsupersessions.go's own doc comment for the design
// citation): `work_unit_supersessions` names no table anywhere in
// api/queries/investment.py or api/services/
// investment_flow.py (confirmed against the whole src/ tree), so Python's
// own readers still include a work unit a later regrouping run retired,
// grouped under whatever repo_id/team/subcategory it carried before being
// superseded. This port excludes it, at every fetcher, because every one
// composes the same shared source. Go is correct -- the sidecar exists so
// a retired work unit's stale grouping stops surfacing.
//
// The same repos-join gap recurs in quadrant's REPO_METRICS readers
// (churn/throughput/review_load/review_latency), tracked under its own
// BaselineDefect (quadrantRepoDedupParity) rather than folded in here,
// since this ticket already covers four routes.
//
// ORDERING: unlike GET/POST /api/v1/sankey, this route's own nodes/links
// carried NO OrderInsensitiveList declaration until now, even though it
// reuses that SAME sankey.Response wire shape and the SAME first-touch
// insertion order every one of this package's own builders produces
// (investmentflow/builders.go's own doc comment: nodePresence/
// nodeRunningTotal both build in first-touch order over a row set
// neither plane's own SQL fully orders). Every shape below that reads a
// finding's own keyed pairing (SankeyRepoFanoutShape, KeyedDirectionShape)
// assumes this declaration already resolved that position-shift noise
// into per-key findings -- see each shape's own doc comment.
//
// THREE ROOT CAUSES ON data.nodes/data.links, THREE DIFFERENT SHAPES: the
// repos-join fan-out is a per-repo INTEGER MULTIPLIER, provable via each
// anchor's own direct edges (SankeyRepoFanoutShape); the argMax
// null-skip relabelling redistributes the SAME total across links with
// no per-edge sign, provable only as a whole-list CONSERVED total
// (ConservationShape); the supersession exclusion makes candidate's row
// population a STRICT SUBSET of baseline's, provable only as a per-key
// DIRECTION (baseline can only be pulled up, never down --
// KeyedDirectionShape). All three can coexist on the SAME response (a
// live org can hold unmerged repos rows, a re-categorizing work unit AND
// a supersession, all at once), so all three are declared as SIBLING
// entries under compare.go's own "a shaped defect's citation can
// legitimately share Paths with a sibling declaration that explains the
// same finding through a different mechanism" rule
// (classifyBaselineDefects' own doc comment) -- never folded into one.
//
// THE SUPERSESSION EXCLUSION'S OWN CITATION IS FURTHER SPLIT ACROSS
// FIVE ENTRIES, one per field, because each field's own aggregate shape
// differs: data.links/data.nodes are keyed lists (KeyedDirectionShape);
// distinct_team_targets/distinct_repo_targets are single scalars
// (ScalarDirectionShape); unassigned_reasons is a plain JSON object
// (DictKeyDirectionShape). data.chosen_mode/data.label/data.description
// AND data.team_coverage/data.repo_coverage are DELIBERATELY DROPPED
// from every citation below (see the two notes after the entries): the
// former because the mechanism's effect on them is a CATEGORICAL
// THRESHOLD FLIP, not a direction or a bound; the latter because no
// direction is derivable for this route's own ratio under this
// exclusion at all (see the note's own detail). Neither is a gap in
// authorship: no shape in this package can honestly cover either
// manifestation -- explainParity's own precedent for an uncoverable
// structural manifestation (explainScopeDropDefect's own "knowingly
// left uncovered" note) is the same discipline applied here to two
// further kinds of uncoverable manifestation.
//
// NEVER OBSERVED FIRING ON A REAL CAPTURE: none of the three available
// production deployed-vs-deployed prove runs (_records/deploy-71/r78/
// step73, deploy-70/r77/step72, deploy-69/r76/step71) shows a genuine
// instance of the repos-join fan-out, the argMax null-skip relabelling or
// the supersession exclusion on investment/flow or investment/flow/
// repo-team -- every admissible request in all three runs is a clean
// match. Every shape below is proven only by a deliberately injected
// fixture fault (sankeyrepofanout_nodevalue_test.go,
// dictkeydirection_test.go, scalardirection_test.go), the same standard
// sankeyInvestmentParity's own argMax-null-skip/ConservationShape entry
// already documents. Zero firings across these three runs does not
// establish the mechanisms cannot occur here -- it is equally consistent
// with these three captures simply not containing an unmerged repos row,
// a re-categorizing work unit or a supersession on these two routes; the
// shapes are unproven against real data either way.
var investmentFlowRepoDedupParity = Options{
	OrderInsensitiveLists: []OrderInsensitiveList{
		{
			Path:      "data.nodes",
			KeyFields: []string{"name"},
			Reason:    "investmentflow/builders.go's own nodePresence/nodeRunningTotal both build nodes in first-touch order over a row set neither plane's own SQL fully orders, the same tie-break class already declared for GET/POST /api/v1/sankey's own data.nodes (sankeyRepoDedupParity). name is unique per node on each plane's own result set.",
			Ticket:    "CHAOS-5873",
		},
		{
			Path:      "data.links",
			KeyFields: []string{"source", "target"},
			Reason:    "the SAME first-touch insertion order as data.nodes, over the SAME under-ordered row set, the same tie-break class already declared for GET/POST /api/v1/sankey's own data.links (sankeyRepoDedupParity). (source, target) is unique per edge on each plane's own result set.",
			Ticket:    "CHAOS-5873",
		},
	},
	BaselineDefects: []BaselineDefect{
		{
			Ticket:             "CHAOS-5803",
			Reason:             "repos is ReplacingMergeTree(last_synced) (000_raw_tables.sql); api/queries/investment.py's readers join it with no FINAL or org_id scoping at all, where this port (internal/investmentflow) reads it FINAL, org_id filtered inside the JOIN's own ON clause. An unmerged physical version of a repo row fans out a clean, uniform integer multiplier across the whole repo-rooted subtree that repo's own edges reach -- the same mechanism already declared for GET/POST /api/v1/sankey's own repos join (sankeyRepoDedupParity), extended here to also admit the anchor's own NODE value: unlike plain sankey, this route's own node builders (nodePresence/nodeRunningTotal) populate a real running total per node, so a genuine instance moves an anchor's node value by the SAME k as its incident edges, not only the edges themselves. Go is correct.",
			Paths:              []string{"data.nodes", "data.links"},
			Intermittent:       true,
			IntermittentReason: "present only while repos holds an unmerged physical version inside the requested window; a comparison taken after the next background merge shows no divergence",
			SankeyRepoFanoutShape: &SankeyRepoFanoutShape{
				NodesListPath:       "data.nodes",
				LinksListPath:       "data.links",
				LinkValuePath:       "data.links.value",
				NodeValuePath:       "data.nodes.value",
				RepoNodeGroups:      []string{"repo"},
				FallbackAnchorNames: []string{"Unassigned repo", "Other repos", "unassigned"},
			},
		},
		{
			Ticket:             "CHAOS-4547",
			Reason:             "work_unit_investments.repo_id/work_unit_type/work_unit_name/provider are Nullable (017_investment_materialize_tables.sql, 019_work_unit_investment_labels.sql); api/queries/investment.py's own LATEST_WORK_UNIT_INVESTMENTS_CTE (the same definition api/queries/sankey.py imports by name for its own investment mode, already declared under this ticket as sankeyInvestmentParity) dedups them via a bare argMax(col, computed_at), which SKIPS a row whose column is NULL when picking the newest version, returning a STALE non-null value from an earlier generation instead of the true latest one. This port reuses analytics.LatestWorkUnitInvestmentsSource(), which tuple-wraps each of those columns -- (argMax(tuple(col), computed_at)).1 -- and therefore reads the true latest value. A work unit whose newest generation cleared one of those columns relative to an earlier one moves its effort from one edge to another -- a redistribution, not an addition, so the response's own total link value is CONSERVED even though no single edge's sign is predictable, the same mechanism already declared for sankeyInvestmentParity's own data.links. Go is correct. Paths names data.links alone: a node's own value is a SUM/max over several edges (finishPresenceEdges/nodeRunningTotal), so this redistribution can move an individual node's total in either direction with no whole-node conservation to check -- ConservationShape's own whole-comparison rule only proves the LIST total is conserved, not any one node's share of it, and that gap is knowingly left uncovered (see the blind-spot note below) rather than guessed at. BLIND SPOT: a node-value-only instance of this exact mechanism -- one whose links happen to stay within FloatTierB tolerance while the node total it feeds visibly moves -- is never covered by this entry; it would surface as an ordinary, uncovered data.nodes.value finding, which is correct and intended, not a gap in this citation's own scope.",
			Paths:              []string{"data.links"},
			Intermittent:       true,
			IntermittentReason: "present only while at least one work unit in the requested window has a newer generation whose repo_id/work_unit_type/work_unit_name/provider differs (including a NULL transition) from an earlier generation's; a request whose work units never re-categorize shows no divergence",
			ConservationShape: &ConservationShape{
				ListPath:   "data.links",
				ValueField: "value",
				ValuePath:  "data.links.value",
			},
		},
		{
			Ticket:             "CHAOS-4441",
			Reason:             "every fetcher in this port composes analytics.LatestWorkUnitInvestmentsSource(), which appends `AND work_unit_id NOT IN (SELECT superseded_work_unit_id FROM work_unit_supersessions WHERE org_id = {org_id:String})` unconditionally -- a Go-only exclusion of work units a later regrouping run retired, making candidate's own row population a STRICT SUBSET of baseline's. A subtree/edge whose value sums that population can only be pulled UP by the rows Go excludes, never down, so baseline is admitted only when strictly greater than candidate at the same (source, target) key -- direction only, no magnitude bound. Go is correct.",
			Paths:              []string{"data.links"},
			Intermittent:       true,
			IntermittentReason: "present only while work_unit_supersessions holds at least one row for the org whose superseded_work_unit_id falls inside the requested window and scope; an org with no supersession rows in that window agrees on both planes",
			KeyedDirectionShape: &KeyedDirectionShape{
				ListPath:   "data.links",
				ValueField: "value",
				ValuePath:  "data.links.value",
				KeyFields:  []string{"source", "target"},
			},
		},
		{
			Ticket:             "CHAOS-4441",
			Reason:             "the same work_unit_supersessions exclusion as this ticket's data.links entry, over data.nodes: a node's own value sums the SAME strictly-subset row population, so it too can only be pulled up by an excluded row's effort, never down. Go is correct.",
			Paths:              []string{"data.nodes"},
			Intermittent:       true,
			IntermittentReason: "present only while work_unit_supersessions holds at least one row for the org whose superseded_work_unit_id falls inside the requested window and scope; an org with no supersession rows in that window agrees on both planes",
			KeyedDirectionShape: &KeyedDirectionShape{
				ListPath:   "data.nodes",
				ValueField: "value",
				ValuePath:  "data.nodes.value",
				KeyFields:  []string{"name"},
			},
		},
		{
			Ticket:             "CHAOS-4441",
			Reason:             "the same work_unit_supersessions exclusion, over distinct_team_targets: coverageStats/edgeStats count DISTINCT assigned team names among the rows (builders.go's own teamSeen/seen sets). Removing a row from a population can only remove or preserve a distinct label already seen, never add one Go's own smaller population lacks -- a plain structural fact, not an empirical skew -- so baseline (over the LARGER population) is admitted only when strictly greater than candidate. Go is correct.",
			Paths:              []string{"data.distinct_team_targets"},
			Intermittent:       true,
			IntermittentReason: "present only while work_unit_supersessions holds at least one row for the org whose superseded_work_unit_id falls inside the requested window and scope, AND that row's own team is not otherwise represented among the org's non-superseded rows; an org with no such row agrees on both planes",
			ScalarDirectionShape: &ScalarDirectionShape{
				Path:                  "data.distinct_team_targets",
				BaselineMustBeGreater: true,
			},
		},
		{
			Ticket:             "CHAOS-4441",
			Reason:             "the same work_unit_supersessions exclusion and the same distinct-count structural argument as this ticket's data.distinct_team_targets entry, over distinct_repo_targets. Go is correct.",
			Paths:              []string{"data.distinct_repo_targets"},
			Intermittent:       true,
			IntermittentReason: "present only while work_unit_supersessions holds at least one row for the org whose superseded_work_unit_id falls inside the requested window and scope, AND that row's own repo is not otherwise represented among the org's non-superseded rows; an org with no such row agrees on both planes",
			ScalarDirectionShape: &ScalarDirectionShape{
				Path:                  "data.distinct_repo_targets",
				BaselineMustBeGreater: true,
			},
		},
		{
			Ticket:             "CHAOS-4441",
			Reason:             "the same work_unit_supersessions exclusion, over unassigned_reasons: fetchInvestmentUnassignedCounts composes the SAME strictly-subset population to count missing_team/missing_repo rows. Counting rows that share a key over a strict SUBSET of a population can only read less than or equal to the same count over the full population -- a structural fact, not an empirical skew -- so baseline is admitted only when strictly greater than candidate at each key. Go is correct.",
			Paths:              []string{"data.unassigned_reasons"},
			Intermittent:       true,
			IntermittentReason: "present only while work_unit_supersessions holds at least one row for the org whose superseded_work_unit_id falls inside the requested window and scope, AND that row is itself missing a team or repo assignment; an org with no such row agrees on both planes",
			DictKeyDirectionShape: &DictKeyDirectionShape{
				DictPath:  "data.unassigned_reasons",
				ValuePath: "data.unassigned_reasons",
			},
		},
		{
			Ticket:             "CHAOS-5923",
			Reason:             "team_coverage (and its coverage.team_coverage duplicate) is assignedTeamValue/totalValue, summed straight over the fetched rows (coverageStats, investmentflow/builders.go; investment_flow.py's identical sum). A team-group sankey node's own value is finishPresenceEdges' max(incoming, outgoing) with incoming always zero for a team node (no edge ever targets a team label), so it equals that team's own row-value sum -- the SAME sum coverageStats accumulates per team. Summing every team-group node therefore reproduces totalValue on that SAME plane, and summing every one except the unassigned-team bucket (\"Unassigned team\") reproduces assignedTeamValue, so team_coverage is a pure function of that plane's OWN nodes. The sibling repos-join fan-out and supersession-exclusion entries above are what move one or more team-group node values between planes; when every team-group node difference in a comparison is already covered by one of those, the team_coverage difference has no source other than the covered nodes. Go is correct.",
			Paths:              []string{"data.team_coverage", "data.coverage.team_coverage"},
			Intermittent:       true,
			IntermittentReason: "present only under the same conditions the sibling fan-out/supersession-exclusion entries above state for their own node/link differences; a comparison with no covered team-group node difference agrees on both planes",
			TeamCoverageIdentityShape: &TeamCoverageIdentityShape{
				NodesListPath:          "data.nodes",
				TeamGroup:              "team",
				UnassignedTeamNodeName: "Unassigned team",
				CoveragePaths:          []string{"data.team_coverage", "data.coverage.team_coverage"},
			},
		},
	},
}

// POST /api/v1/investment/flow's own admissible scenarios
// reach THREE DIFFERENT numeric-leaf shapes depending on flow_mode --
// dynamic_default_org's own dynamic branch (buildDynamicModeResponse)
// leaves data.coverage/data.unassigned_reasons/data.top_n_repos nil
// (investmentflow.go's own comment: "Coverage/UnassignedReasons/
// FlowMode/DrillCategory/TopNRepos stay nil -- ... only the flow_mode
// branch does [set them]"), where the other three scenarios' shared
// flow_mode branch sets every one of them -- and POST /investment/flow/
// repo-team's own BuildRepoTeamFlowResponse sets NEITHER, only Nodes/
// Links/Mode/Unit/Label/Description/ChosenMode. Since UnusedTierB/
// IntegerLeaves enforcement is per COMPARISON (an entry unreached even
// once in a request using that Options reads as stale, no intermittent
// exemption the way BaselineDefects has one), one shared Options
// declaring all three routes' numeric leaves would refuse the two
// narrower routes' own runs the moment they don't reach a leaf the wider
// route's declaration names. Three Options values, one per reachable
// shape, all three still composing investmentFlowRepoDedupParity's own
// BaselineDefects/OrderInsensitiveLists BY VALUE (those tolerate the
// same variation already, via Intermittent).
//
// investmentFlowDynamicParity: dynamic_default_org. data.links.value/
// data.nodes.value are investmentflow/queries.go's own sum(subcategory_kv.2
// * effort_value) (all five fetchers share this shape) -- genuine merged
// ClickHouse float aggregate. team_coverage/repo_coverage are
// assignedValue/totalValue (investmentflow/builders.go edgeStats), a
// ratio of two sums of that SAME float aggregate -- float-valued for a
// less obvious reason than a bare aggregate: the numerator and
// denominator are each independently drift-prone, and dividing them does
// NOT cancel that drift (they are sums over different, data-dependent
// row subsets, not the same value divided by itself). distinct_team_
// targets/distinct_repo_targets are len(seen) set cardinalities -- integer.
var investmentFlowDynamicParity = Options{
	BaselineDefects:       investmentFlowRepoDedupParity.BaselineDefects,
	OrderInsensitiveLists: investmentFlowRepoDedupParity.OrderInsensitiveLists,
	NumericLeavesDeclared: true,
	FloatTierB: map[string]string{
		"data.links.value":   "sum(subcategory_kv.2 * effort_value) (investmentflow/queries.go, all five fetchers) -- genuine merged ClickHouse float aggregate.",
		"data.nodes.value":   "the same float Value as data.links.value, accumulated per node (investmentflow/builders.go nodePresence/nodeRunningTotal) -- unlike plain /api/v1/sankey, this route's own node builders DO populate a real value; see sankeyRepoDedupParity's own doc comment for that contrast.",
		"data.team_coverage": "assignedValue/totalValue (investmentflow/builders.go edgeStats) -- a ratio of two sums of the same float aggregate as data.links.value; the ratio does not cancel the sums' own drift, since numerator and denominator sum different, data-dependent row subsets.",
		"data.repo_coverage": "the same edgeStats ratio shape as data.team_coverage, over the repo-scoped row set.",
	},
	IntegerLeaves: map[string]string{
		"data.distinct_team_targets": "len(seen) (investmentflow/builders.go edgeStats) -- a set cardinality, not a ClickHouse aggregate.",
		"data.distinct_repo_targets": "the same set-cardinality shape as data.distinct_team_targets, over the repo-scoped row set.",
	},
}

// investmentFlowModeParity: team_category_repo_org, team_category_
// subcategory_repo_org, team_subcategory_repo_with_drill_org -- the
// three scenarios sharing BuildFlowResponse's own flow_mode branch.
// Extends investmentFlowDynamicParity's own leaves with the three this
// branch ALSO sets: data.coverage (map[string]float64{"team_coverage":
// teamCoverage, "repo_coverage": repoCoverage} -- investmentflow.go --
// the SAME two values as data.team_coverage/data.repo_coverage,
// duplicated; declared via the map's own parent path since coverage's
// keys, though fixed today, are string literals a corpus declaration
// would otherwise have to name twice for no benefit), data.
// unassigned_reasons (map[string]int{"missing_team":...,"missing_repo":
// ...} -- both countIf()-shaped, integer), and data.top_n_repos (*int,
// a pure echo of the request's own top_n_repos parameter/default,
// integer, never touches ClickHouse at all).
var investmentFlowModeParity = Options{
	BaselineDefects:       investmentFlowRepoDedupParity.BaselineDefects,
	OrderInsensitiveLists: investmentFlowRepoDedupParity.OrderInsensitiveLists,
	NumericLeavesDeclared: true,
	FloatTierB: map[string]string{
		"data.links.value":   investmentFlowDynamicParity.FloatTierB["data.links.value"],
		"data.nodes.value":   investmentFlowDynamicParity.FloatTierB["data.nodes.value"],
		"data.team_coverage": investmentFlowDynamicParity.FloatTierB["data.team_coverage"],
		"data.repo_coverage": investmentFlowDynamicParity.FloatTierB["data.repo_coverage"],
		"data.coverage":      "map[string]float64{team_coverage, repo_coverage} (investmentflow.go) -- the SAME two ratio values as data.team_coverage/data.repo_coverage, duplicated under fixed (if data-independent) keys.",
	},
	IntegerLeaves: map[string]string{
		"data.distinct_team_targets": investmentFlowDynamicParity.IntegerLeaves["data.distinct_team_targets"],
		"data.distinct_repo_targets": investmentFlowDynamicParity.IntegerLeaves["data.distinct_repo_targets"],
		"data.unassigned_reasons":    "map[string]int{missing_team, missing_repo} (investmentflow.go fetchInvestmentUnassignedCounts) -- both countIf()-shaped bare counts.",
		"data.top_n_repos":           "a pure echo of the request's own top_n_repos parameter/default (investmentflow.go) -- never touches ClickHouse.",
	},
}

// investmentFlowRepoTeamParity: POST /api/v1/investment/flow/repo-team's
// own default_org/theme_scoped_org. BuildRepoTeamFlowResponse
// (investmentflow.go) sets ONLY Nodes/Links/Mode/Unit/Label/Description/
// ChosenMode -- team_coverage/repo_coverage/coverage/distinct_*_targets/
// unassigned_reasons/top_n_repos all stay nil on both planes (never a
// numeric leaf either plane's comparison reaches), so declaring them
// here would make this Options value read as stale for every request
// that actually uses it -- see this section's own opening comment.
// data.links.value/data.nodes.value are fetchInvestmentRepoTeamEdges'
// own sum(subcategory_kv.2 * effort_value) (investmentflow/queries.go),
// the SAME shape every other investment-flow fetcher shares.
//
// data.links has its own declaration here, not investmentFlowRepoDedup
// Parity's: this route's rows are grouped by (subcategory, repo, team),
// one level finer than either link the builder draws, so the reference
// plane repeats a (source, target) pair once per row that reaches it
// (investmentFlowRepoTeamLinkCopiesDefect), while the candidate writes
// each pair once (buildRepoTeamSankey accumulates through orderedEdges).
// (source, target) is the identity of a link on the candidate plane and,
// after that defect's whole-key collapse, on the reference plane too.
var investmentFlowRepoTeamParity = Options{
	BaselineDefects: append(append([]BaselineDefect{}, investmentFlowRepoDedupParity.BaselineDefects...),
		investmentFlowRepoTeamLinkCopiesDefect),
	OrderInsensitiveLists: []OrderInsensitiveList{
		investmentFlowRepoDedupParity.OrderInsensitiveLists[0],
		{
			Path:      "data.links",
			KeyFields: []string{"source", "target"},
			Reason:    "the SAME first-touch insertion order as data.nodes, over the SAME under-ordered row set. (source, target) is unique per link on the candidate plane (orderedEdges, investmentflow/builders.go); the reference plane's repeated pairs are collapsed whole-key by investmentFlowRepoTeamLinkCopiesDefect before pairing.",
			Ticket:    "CHAOS-5873",
		},
	},
	NumericLeavesDeclared: true,
	FloatTierB: map[string]string{
		"data.links.value": investmentFlowDynamicParity.FloatTierB["data.links.value"],
		"data.nodes.value": investmentFlowDynamicParity.FloatTierB["data.nodes.value"],
	},
}

// investmentFlowRepoTeamLinkCopiesDefect: build_investment_repo_team_
// flow_response (api/services/investment_flow.py) appends one link per
// fetch_investment_repo_team_edges row, and those rows are grouped by
// (subcategory, repo, team) -- so a repo->team link repeats once per
// subcategory that reaches the repo, and a subcategory->repo link once per
// team the repo maps to, each copy carrying one row's share. The candidate
// sums every row reaching a pair into one link, the way the reference
// plane's own link_totals builders do for every other investment flow.
// The copies are compared whole-key: every baseline copy of a key is
// consumed once, the copies must agree outside value, and their sum must
// meet the candidate's value under data.links.value's own FloatTierB tier.
var investmentFlowRepoTeamLinkCopiesDefect = BaselineDefect{
	Ticket:             "CHAOS-6024",
	Reason:             "build_investment_repo_team_flow_response appends one SankeyLink per (subcategory, repo, team) row, so the reference body repeats a (source, target) link once per row that reaches it, each copy carrying that row's share; buildRepoTeamSankey writes each (source, target) once, valued at the sum of the same rows (orderedEdges, the accumulator the link_totals builders share). A link is the aggregated contribution between two nodes (docs/use/investment/investment-flows.md); node values are equal on both planes. Go is correct.",
	Paths:              []string{"data.links"},
	Intermittent:       true,
	IntermittentReason: "present only while some (source, target) pair is reached by two or more (subcategory, repo, team) rows in the requested window and scope; a body whose every repository maps to one team and is reached from one subcategory repeats no link",
	BaselineCopySumShape: &BaselineCopySumShape{
		ListPath:  "data.links",
		KeyFields: []string{"source", "target"},
		SumField:  "value",
	},
}

// investmentFlowNodesTeamScopeSubsetDefect/investmentFlowLinksTeamScope
// SubsetDefect declare data.nodes/data.links as a bounded subset for a
// team-scoped POST /api/v1/investment/flow (+/repo-team) request:
// repoScopeFilterClause (investmentflow.go:143-171) splices
// teamscope.RepoCondition(orgID,"repo_id",scopeIDs,asOf) for a team
// scope, the same shared condition and the same team_repo_ownership
// source every repo-keyed route in this corpus reads (heatmapRepo
// TouchpointsTeamScopeSubsetDefect's own Reason states the two tables in
// full). The reference plane resolves the identical scope through
// resolve_repo_filter_ids -> resolve_repo_ids_for_teams
// (api/queries/scopes.py:72-89, the same user_metrics_daily.team_id read
// every sibling team-scope defect in this file cites) -- empty for this
// org's real team, so the reference answers organization-wide while the
// candidate answers over the team's own repositories.
//
// buildDynamicFlowSankey/buildRepoTeamSankey (investmentflow/builders.go)
// both build a node's own value as a RUNNING SUM over every row that
// touches it (nodeRunningTotal.add) and a link's own value directly off
// one qualifying row -- every row is already scoped to a single admitted
// repository (the SQL's own repo_id filter), so a SOURCE (subcategory)
// node's own value sums across however many repositories the request's
// scope admits, a non-negative per-repo contribution the SAME class the
// class ruling this ticket's own card states covers: a genuinely
// narrower team scope can only pull that sum down, never up, at a
// (sub)category node or link this response still carries -- ADMITTED
// via BoundedLeavesAllKeys, not enumerated BoundedLeafKeys, since every
// node/link this response can ever produce is built the same running-sum
// way, with no separate leaf-name-only-sometimes-bounded case to carve
// out. group is EqualLeaves: a matched node's own group label
// (subcategory/repo/team) is fixed by which BRANCH produced it, never by
// how many repositories fed its value.
var investmentFlowNodesTeamScopeSubsetDefect = BaselineDefect{
	Ticket:             "CHAOS-5940",
	Reason:             "POST /api/v1/investment/flow (+/repo-team)'s team scope resolves a team's repositories from team_repo_ownership through teamscope.RepoCondition on the candidate, user_metrics_daily.team_id (empty for this org's real team) on the reference -- the reference answers organization-wide, the candidate over the team's own repositories. A node's own value is a running sum over every admitted row that touches it (nodeRunningTotal.add, investmentflow/builders.go), a non-negative per-repo contribution, so a genuinely narrower team scope can only pull a matched node's own sum down, never up. Go is correct.",
	Paths:              []string{"data.nodes"},
	Intermittent:       true,
	IntermittentReason: "present only while the requested team's own repositories are a PROPER subset of the organization's; a team owning every repository leaves the two lists identical",
	TeamRepoSubsetShape: &TeamRepoSubsetShape{
		ListPath:             "data.nodes",
		KeyFields:            []string{"name"},
		EqualLeaves:          []string{"group"},
		BoundedLeavesAllKeys: []string{"value"},
	},
}

var investmentFlowLinksTeamScopeSubsetDefect = BaselineDefect{
	Ticket:             "CHAOS-5940",
	Reason:             "the same team-repository-resolution mechanism as investmentFlowNodesTeamScopeSubsetDefect, over data.links: a link's own value is one qualifying row's own contribution (or, for the flow_mode branch's own accumulators, a running sum over the rows sharing that (source, target) pair), always confined to the request's own admitted repositories, so a genuinely narrower team scope can only pull a matched link's own value down, never up. Go is correct.",
	Paths:              []string{"data.links"},
	Intermittent:       true,
	IntermittentReason: "present only while the requested team's own repositories are a PROPER subset of the organization's; a team owning every repository leaves the two lists identical",
	TeamRepoSubsetShape: &TeamRepoSubsetShape{
		ListPath:             "data.links",
		KeyFields:            []string{"source", "target"},
		BoundedLeavesAllKeys: []string{"value"},
	},
}

// investmentFlowDynamicTeamScopedParity: POST /api/v1/investment/flow's
// own team_scoped entry (the dynamic, non-flow_mode branch --
// investmentFlowDynamicParity's own doc comment states this branch's
// numeric leaves in full).
var investmentFlowDynamicTeamScopedParity = Options{
	OrderInsensitiveLists: investmentFlowDynamicParity.OrderInsensitiveLists,
	NumericLeavesDeclared: true,
	FloatTierB:            investmentFlowDynamicParity.FloatTierB,
	IntegerLeaves:         investmentFlowDynamicParity.IntegerLeaves,
	BaselineDefects: append(append([]BaselineDefect{}, investmentFlowDynamicParity.BaselineDefects...),
		investmentFlowNodesTeamScopeSubsetDefect, investmentFlowLinksTeamScopeSubsetDefect),
}

// investmentFlowRepoTeamTeamScopedParity: POST /api/v1/investment/flow/
// repo-team's own team_scoped entry.
var investmentFlowRepoTeamTeamScopedParity = Options{
	OrderInsensitiveLists: investmentFlowRepoTeamParity.OrderInsensitiveLists,
	NumericLeavesDeclared: true,
	FloatTierB:            investmentFlowRepoTeamParity.FloatTierB,
	BaselineDefects: append(append([]BaselineDefect{}, investmentFlowRepoTeamParity.BaselineDefects...),
		investmentFlowNodesTeamScopeSubsetDefect, investmentFlowLinksTeamScopeSubsetDefect),
}

// investmentFlowRepoDedupParity DELIBERATELY carries no citation at all
// for data.chosen_mode/data.label/data.description, even though the
// SAME work_unit_supersessions exclusion above also feeds
// buildDynamicModeResponse's own coverage-threshold decision
// (distinctTeamTargets >= 2 && teamCoverage >= 0.70, else a repo_scope
// test, else fallback -- investmentflow.go). Those inputs move under
// this exact exclusion, so the CHOSEN MODE -- a categorical decision, not
// a number -- can flip between the two planes with no direction and no
// bound: a flip is not "a little wrong" or "a lot wrong" the way a ratio
// or a count is, so no shape in this package (direction, tolerance,
// multiplier or conservation) can honestly claim to explain it. label/
// description are fixed 1:1 off chosen_mode in the dynamic branch and
// fixed constants everywhere else, so they inherit the same problem.
// Leaving these three blanket-covered would hide a genuine Go bug in the
// >=0.70/>=2 threshold logic behind this citation -- exactly the failure
// mode this shaping programme exists to remove. A real chosen_mode/
// label/description divergence therefore surfaces as an ordinary,
// uncovered finding on the next prove run; that is correct and intended,
// not a regression.

// investmentFlowRepoDedupParity ALSO deliberately carries no citation
// for data.team_coverage/data.repo_coverage, even though the SAME
// work_unit_supersessions exclusion above still fires on this route's
// data.links/data.nodes/distinct_team_targets/distinct_repo_targets/
// unassigned_reasons whenever a supersession falls inside the
// requested window and scope.
//
// A prior version of this citation gave team_coverage/repo_coverage a
// ScalarDirectionShape whose Reason borrowed its direction from
// investmentFull's own SupersessionSkewShape (sankeycoverage.go /
// resolvers/analytics.py's dedicated coverage_sql) without deriving it
// from this route's own query. The two are not the same quantity:
// coverageStats/edgeStats (investmentflow/builders.go) compute
// team_coverage/repo_coverage as assignedValue/totalValue straight
// from the SAME rows fetchInvestmentTeamCategoryRepoEdges/
// fetchInvestmentTeamSubcategoryRepoEdges/fetchInvestmentRepoTeamEdges
// (queries.go) fetch for data.links/data.nodes -- grouped by (team,
// category|subcategory, repo), through an unconditional ARRAY JOIN
// over subcategory_distribution_json, summing
// subcategory_kv.2 * effort_value. investmentFull's own coverage query
// (per sankeycoverage.go's own doc comment) has neither that
// GROUP BY nor that ARRAY JOIN: it is a dedicated, un-grouped,
// repo-effort-weighted sum over the whole org population, built that
// way specifically because the ARRAY JOIN "re-weighted every
// effort-weighted column" it would otherwise touch. A direction proven
// for one aggregate shape does not carry to a structurally different
// one under the SAME exclusion: a direction claim is valid only for
// the query it was derived from.
//
// Nor does this route's own query supply a direction to derive
// instead. The work_unit_supersessions exclusion removes rows from
// BOTH the numerator (assignedValue) and the denominator (totalValue)
// of the SAME ratio together -- unlike distinct_team_targets/distinct_
// repo_targets/unassigned_reasons above (plain counts over a strict
// subset, which can only fall or hold under row removal) or
// data.links/data.nodes (a sum with no denominator at all), a ratio
// whose numerator and denominator both shrink under the same row
// removal has no direction fixed by that structure alone: which way it
// moves depends on whether the excluded rows' own assigned share
// differs from the surviving population's, a property this query's own
// text does not establish and this package does not assume. No shape
// here can honestly claim a direction, so team_coverage/repo_coverage
// are left uncovered. A real divergence on either surfaces as an
// ordinary, uncovered finding on the next prove run; that is this
// citation working as intended, not a regression.

// quadrantRepoDedupParity is shared by every REPO_METRICS-grain quadrant
// request whose fetcher joins repos -- churn_throughput's forced
// repo-grain (org/team scope for churn_throughput normalizes to repo
// scope before any metric read, quadrant.go's own doc comment) and any
// request that already asks for repo scope directly.
//
// api/services/quadrant.py's REPO_METRICS specs (churn, throughput,
// review_load, review_latency) all share
// `join_clause="INNER JOIN repos ON repos.id = m.repo_id"` -- no FINAL,
// no org_id in the ON clause. This is the SAME repos dedup gap already
// declared for investment flow's own repos join
// (investmentFlowRepoDedupParity): repos is ReplacingMergeTree
// (last_synced) (000_raw_tables.sql), so an unmerged physical version of
// a repo row fans the join out and doubles every affected repo's summed
// value. Filed under its own BaselineDefect rather than folded into
// investmentFlowRepoDedupParity's -- that one already carries four
// routes, and a fifth would make "is it fixed?" unanswerable;
// investmentFlowRepoDedupParity itself carries a one-line cross-reference
// noting the mechanism recurs here. Measured live: full-chaos/
// dev-health-deploy's churn/throughput trajectory exactly halved between
// planes on every window, reproduced across two separate prove runs.
// quadrant.go's own RepoMetrics join already reads `repos FINAL ... AND
// repos.org_id = {org_id:String}` -- Go is correct.
//
// The same requests also carry data.points' own order:
// fetch_quadrant_metric's GROUP BY (api/queries/quadrant.py, quadrant.go)
// ends `ORDER BY bucket` only, IDENTICALLY on both planes. ClickHouse
// gives no order guarantee for entities tied within the same bucket, so
// the identical SQL, run independently on each plane, can emit them in a
// different order -- a property of the engine's result for a query with
// no secondary sort, not a divergence between the planes (both planes'
// SQL is unordered the same way; this is not "Go orders differently from
// Python", and must never be closed by adding an ORDER BY to only one
// side). Matched by entity_id every field agrees. Same precedent as the
// sankey nodes/edges case (compare.go's own OrderInsensitiveList doc
// comment).
var quadrantRepoDedupParity = Options{
	NumericLeavesDeclared: true,
	FloatTierB:            quadrantPointFloats,
	BaselineDefects: []BaselineDefect{
		{
			Ticket:             "CHAOS-5867",
			Reason:             "REPO_METRICS' churn/throughput/review_load/review_latency specs (api/services/quadrant.py) join `repos` with `INNER JOIN repos ON repos.id = m.repo_id` -- no FINAL, no org_id in the ON clause, the same gap already declared for investment flow's own repos join (investmentFlowRepoDedupParity). An unmerged physical version of a repos row fans the join out, doubling the summed value for the affected repo/day. This port's RepoMetrics join (quadrant.go) already reads `repos FINAL` with org_id in the ON clause. Go is correct.",
			Paths:              []string{"data.points.x", "data.points.y", "data.points.trajectory.x", "data.points.trajectory.y"},
			Intermittent:       true,
			IntermittentReason: "present only while repos holds an unmerged physical version of the affected repo row -- a merge-state trigger, not a content one: the next background merge to run on the repos table collapses the duplicate and the comparison shows no divergence, regardless of what the underlying metric data itself does",
		},
	},
	OrderInsensitiveLists: []OrderInsensitiveList{
		{
			Path:      "data.points",
			KeyFields: []string{"entity_id"},
			Reason:    "fetch_quadrant_metric's GROUP BY (api/queries/quadrant.py, quadrant.go) ends `ORDER BY bucket` only, identically on both planes; ClickHouse gives no order guarantee for entities tied within the same bucket, so the identical SQL can emit them in a different order on separate executions. A property of the engine's result for a query with no secondary sort, not a divergence between the planes.",
			Ticket:    "CHAOS-5857",
		},
	},
}

// codeHotspotsInts declares code_hotspots' own two leaf shapes: the
// root's own churn sum (depth zero) and every deeper directory's own
// churn sum, reached at ANY nesting depth through the one repeat form
// this ticket adds (see the flame/aggregated spec's own doc comment
// below). Both are `sum(churn)` over `file_metrics_daily`'s `churn
// UInt32` (001_metrics_v2.sql) -- an INTEGER ClickHouse aggregate, not a
// merged float one. Both code_hotspots entries below share this one
// Options value, the same convention cycleBreakdownFloats/
// aggFlameThroughputInts already use for their own sibling modes.
var codeHotspotsInts = map[string]string{
	"data.root.value":           "sum(churn) over file_metrics_daily's churn UInt32 column, the org-wide total across every top-level entry -- integer.",
	"data.root.children+.value": "the same sum(churn) shape at any nesting depth beneath the root, one or more directory levels deep -- integer.",
}

var codeHotspotsParity = Options{
	NumericLeavesDeclared: true,
	IntegerLeaves:         codeHotspotsInts,
}

var restEndpointSpecs = map[string]RESTEndpointSpec{
	"REST:GET:/api/v1/quadrant": {
		Method: "GET",
		Path:   "/api/v1/quadrant",
		Requests: []RESTRequest{
			{
				// churn_throughput's org/team scope normalizes to repo grain
				// before any metric read (quadrant.go's own
				// forced-repo-grain override), so this request is
				// already REPO_METRICS-grain -- quadrantOrgRequest's
				// shared Options (FloatTierB only) is not enough here,
				// so this entry is written out instead of using that
				// helper.
				Name:                "churn_throughput_org",
				Query:               url.Values{"type": {"churn_throughput"}, "scope_type": {"org"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Parity:   quadrantRepoDedupParity,
			},
			quadrantOrgRequest("cycle_throughput_org", "cycle_throughput"),
			{
				// baseline's wip_throughput read answers 503 "Data
				// unavailable"; query-api answers 200 with real data (no
				// matching entry in query-api-503-causes.txt) -- a
				// baseline-only failure, encoded as the expected
				// baseline status rather than hidden behind a refusal.
				Name:                "wip_throughput_org",
				Query:               url.Values{"type": {"wip_throughput"}, "scope_type": {"org"}},
				WantCandidateStatus: 200, WantBaselineStatus: 503,
				StatusDivergenceReason: "baseline answers 503 Data unavailable for this route; query-api answers 200 with real data. Confirmed baseline-side: query-api's own 503 telemetry carries no wip_throughput entry for the run this was measured in.",
				BodyMode:               RESTBodyModeStatusOnly,
			},
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
				Parity:     Options{NumericLeavesDeclared: true, FloatTierB: quadrantPointFloats},
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
				Parity:     quadrantRepoDedupParity,
				IDBindings: []RESTIDBinding{{Producer: "repo_id", QueryParam: "scope_id"}},
			},
			{
				// person scope, scope_id bound to people's own live
				// person_id (GET /api/v1/people's query_string_search
				// entry).
				// Same baseline-only 503 as wip_throughput_org.
				Name:                "wip_throughput_person_scoped",
				Query:               url.Values{"type": {"wip_throughput"}, "scope_type": {"person"}},
				WantCandidateStatus: 200, WantBaselineStatus: 503,
				StatusDivergenceReason: "baseline answers 503 Data unavailable for this route; query-api answers 200 with real data. Confirmed baseline-side: query-api's own 503 telemetry carries no wip_throughput entry for the run this was measured in.",
				BodyMode:               RESTBodyModeStatusOnly,
				IDBindings:             []RESTIDBinding{{Producer: "person_id", QueryParam: "scope_id"}},
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
				Parity:   quadrantRepoDedupParity,
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
			{
				// type present but not one of QuadrantDefinitions'/
				// QUADRANT_DEFINITIONS' four keys -- distinct from
				// missing_type above (a MISSING type is a 422 at the
				// framework/aggregation layer; a present-but-unknown
				// type reaches build_quadrant_response/BuildResponse and
				// answers this 404 instead).
				Name:                "unknown_type",
				Query:               url.Values{"type": {"not_a_real_quadrant_type"}},
				WantCandidateStatus: 404, WantBaselineStatus: 404,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// scope_type outside {org, team, repo, service, person,
				// developer} -- Python's own ScopeFilter construction
				// raises inside the `try/except Exception` around
				// building MetricFilter/ScopeFilter (services/quadrant.py),
				// answering the SAME "Invalid scope filter" 400 this
				// port's normalizedScope switch (response.go) answers.
				Name:                "invalid_scope_type",
				Query:               url.Values{"type": {"wip_throughput"}, "scope_type": {"not_a_real_scope"}},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// range_days non-numeric -- this route's own instance of
				// the same FastAPI-signature int_parsing 422 people
				// summary's invalid_range_days entry already exercises
				// for a different route.
				Name:                "invalid_range_days",
				Query:               url.Values{"type": {"wip_throughput"}, "range_days": {"not-a-number"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// bucket outside {week, month} -- build_quadrant_response's
				// own explicit check (services/quadrant.py), answered
				// after every FastAPI-signature validation above already
				// passed.
				Name:                "invalid_bucket",
				Query:               url.Values{"type": {"wip_throughput"}, "bucket": {"day"}},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// person/developer scope with no scope_id -- checked
				// before any ClickHouse call on both planes (quadrant.py:
				// 500-503, response.go's own person-scope-id check).
				Name:                "person_missing_scope_id",
				Query:               url.Values{"type": {"wip_throughput"}, "scope_type": {"person"}},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// person scope, scope_id a neutral fixed string that can
				// never be a real identity's md5 digest (resolvePersonIdentity's
				// WHERE clause, identity.go) -- deterministically 404s on
				// both planes, the same literal-value technique the
				// people-summary spec's own person_not_found entry uses
				// for its path segment.
				Name:                "person_not_found",
				Query:               url.Values{"type": {"wip_throughput"}, "scope_type": {"person"}, "scope_id": {"not-a-real-person-id"}},
				WantCandidateStatus: 404, WantBaselineStatus: 404,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// _reject_comparative_params (main.py:893) -- quadrant_route.go's
				// own doc comment states this check's precedence: after every
				// FastAPI-signature validation, before build_quadrant_response.
				Name:                "forbidden_param",
				Query:               url.Values{"type": {"wip_throughput"}, "rank": {"1"}},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	// flame/aggregated: every query param this route takes is an OPTIONAL
	// filter (main.py:807-819), so a bare `?mode=X` request is a perfectly
	// valid, reachable 200 on real org data -- no id-binding producer is
	// needed here, unlike flame's own corpus (this route's entity has no
	// id at all). Both live entries below bound `range_days` to a small
	// window (7 days) to keep the compared row set small and the request
	// deterministic run to run.
	//
	// FLOAT TIER: checked leaf-by-leaf, not assumed. code_hotspots'
	// numeric leaves (root.value down through every nested
	// root.children...value) are `sum(churn)` over `file_metrics_daily`'s
	// `churn UInt32` (001_metrics_v2.sql) -- an INTEGER ClickHouse
	// aggregate, and compare.go's own compareJSON takes an exact-int64
	// fast path for any whole-number pair not declared Tier B (see its own
	// "DELIBERATE DIVERGENCE" doc comment).
	// cycle_breakdown's leaves ARE genuine merged Float64 aggregates
	// (`sum(duration_hours)` over an argMax-deduped subquery,
	// work_item_state_durations_daily.duration_hours Float64) -- the same
	// risk class quadrantPointFloats' own doc comment names by name
	// ("avg/sum/stddevPop over Float64"). cycle_breakdown's own tree shape
	// is FIXED at exactly two levels (buildCycleBreakdownTree: root ->
	// category -> status leaf, no category or leaf ever has further
	// children), so its three possible leaf depths are enumerable as
	// concrete dotted paths -- cycleBreakdownFloats below declares all
	// three, and its three live entries carry NumericLeavesDeclared.
	// throughput's tree (buildThroughputTree) is the SAME fixed
	// two-level shape (root -> work type -> team, the team level's own
	// Children always []Node{}), so its three leaf depths are equally
	// enumerable -- aggFlameThroughputParity above declares them integer
	// (uniqExact-derived counts, never a float aggregate) and both live
	// throughput entries below carry it.
	//
	// code_hotspots' own tree is DIFFERENT IN KIND from either of those:
	// buildCodeHotspotsTree splits a real repository file path on "/"
	// (aggflame.go), so its depth is UNBOUNDED by any fixed number of
	// directory segments a real repo can have -- no FIXED set of dotted
	// paths, however many, could enumerate every depth. Options'
	// FloatTierB/FloatExactLeaves/IntegerLeaves accept exactly one
	// escape from that: a declared segment ending in a single trailing
	// "+" (FloatTierB's own doc comment; matchRepeatDeclaration,
	// compare.go) matches one or more consecutive occurrences of its own
	// base segment name at that position -- "data.root.children+.value"
	// reaches every depth from one directory level to any number.
	// codeHotspotsInts below declares the root's own value (depth zero,
	// no repeated segment -- the repeat form never matches zero
	// occurrences, so this needs its own entry) plus that one repeat
	// entry for every deeper value, and both live entries carry
	// NumericLeavesDeclared.
	"REST:GET:/api/v1/flame/aggregated": {
		Method: "GET",
		Path:   "/api/v1/flame/aggregated",
		Requests: []RESTRequest{
			{
				// mode has no default (main.py:809) -- required, no value
				// on the wire, the same missingFieldError shape quadrant's
				// own missing_type entry already exercises for a single
				// required query param.
				Name:                "missing_mode",
				Query:               url.Values{},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// _reject_comparative_params (main.py:202-208, called at
				// main.py:836): checked AFTER FastAPI's own required-field
				// validation already passed -- confirmed live for
				// /api/v1/people and /api/v1/flame, the same ordering this
				// route's own port reuses (flame_aggregated_route.go's own
				// doc comment).
				Name: "comparative_param_rejected",
				Query: url.Values{
					"mode": {"throughput"}, "rank": {"1"},
				},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// The mode-enum 400 (main.py:838-842) -- deterministic, no
				// ClickHouse call either plane.
				Name: "unknown_mode",
				Query: url.Values{
					"mode": {"not-a-real-mode"},
				},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// Live happy path, cycle_breakdown mode, org scope, a
				// small bounded window. See this spec's own doc comment
				// for cycleBreakdownFloats' three fixed leaf-depth paths.
				Name:                "cycle_breakdown_org_week",
				Query:               url.Values{"mode": {"cycle_breakdown"}, "range_days": {"7"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Parity:   Options{NumericLeavesDeclared: true, FloatTierB: cycleBreakdownFloats},
			},
			{
				// Live happy path, code_hotspots mode, org scope, a small
				// bounded window and a small limit. codeHotspotsParity:
				// every leaf is an exact integer churn sum, declared at any
				// depth through the one repeat form (this table's own doc
				// comment above).
				Name:                "code_hotspots_org_week_limit5",
				Query:               url.Values{"mode": {"code_hotspots"}, "range_days": {"7"}, "limit": {"5"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: codeHotspotsParity,
			},
			{
				// cycle_breakdown's team_id branch (fetchCycleBreakdown's
				// own `AND team_id = {team_id:String}`, clickhouse.go) --
				// team_id bound to filters/options' own live team_id, the
				// same producer quadrant's team-scoped entries already
				// consume. Same fixed two-level tree shape as
				// cycle_breakdown_org_week, so the same three Tier B leaf
				// depths apply.
				Name:                "cycle_breakdown_team_scoped",
				Query:               url.Values{"mode": {"cycle_breakdown"}, "range_days": {"7"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     Options{NumericLeavesDeclared: true, FloatTierB: cycleBreakdownFloats},
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "team_id"}},
			},
			{
				// cycle_breakdown's provider branch (fetchCycleBreakdown's
				// own `AND provider = {provider:String}`) -- provider bound
				// to GET /api/v1/drilldown/issues' own "default_window"
				// entry, its issues_org_provider producer (IssueItem.Provider,
				// wct.provider -- the same provider-taxonomy column
				// work_item_state_durations_daily.provider carries). That
				// entry is org-wide, so this binding carries no dependency
				// on which person the people-search producer happens to
				// select -- unlike a person-scoped issues read, which can
				// bind to a person with no issues at all and refuse.
				Name:                "cycle_breakdown_provider_scoped",
				Query:               url.Values{"mode": {"cycle_breakdown"}, "range_days": {"7"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     Options{NumericLeavesDeclared: true, FloatTierB: cycleBreakdownFloats},
				IDBindings: []RESTIDBinding{{Producer: "issues_org_provider", QueryParam: "provider"}},
			},
			{
				// code_hotspots' repo_id branch (fetchCodeHotspots' own
				// `AND toString(repo_id) = {repo_id:String}`) -- repo_id
				// bound to filters/options' own live repo_id, the same
				// producer code_hotspots_org_week_limit5's own sibling
				// entries in this table already consume for other routes.
				// codeHotspotsParity, same reasoning as
				// code_hotspots_org_week_limit5 above.
				Name:                "code_hotspots_repo_scoped",
				Query:               url.Values{"mode": {"code_hotspots"}, "range_days": {"7"}, "limit": {"5"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     codeHotspotsParity,
				IDBindings: []RESTIDBinding{{Producer: "repo_id", QueryParam: "repo_id"}},
			},
			{
				// Live happy path, throughput mode, org scope -- exercises
				// fetchThroughputByType (buildThroughput's primary read).
				// aggFlameThroughputParity: ItemsCompleted is uniqExact(...),
				// an exact distinct count summed at each of this tree's own
				// two fixed levels -- integer, never a merged float
				// aggregate. See this spec's own doc comment for why
				// throughput's fixed shape can carry NumericLeavesDeclared
				// where code_hotspots' unbounded one cannot.
				Name:                "throughput_org_week",
				Query:               url.Values{"mode": {"throughput"}, "range_days": {"7"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: aggFlameThroughputParity,
			},
			{
				// throughput's team_id branch (fetchThroughputByType's own
				// `AND t.team_id = {team_id:String}`) -- team_id bound to
				// filters/options' own live team_id.
				Name:                "throughput_team_scoped",
				Query:               url.Values{"mode": {"throughput"}, "range_days": {"7"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     aggFlameThroughputParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "team_id"}},
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
				// FilterOptionsResponse (models/filters.py) is all list[str] --
				// teams/repos/services/developers/work_category/issue_type/
				// flow_stage carry zero numeric leaves. NumericLeavesDeclared
				// is set with both numeric tables absent on purpose, the
				// same "declared empty" convention opportunities/
				// work-unit-explain's own corpus use: it turns a future
				// numeric field added to this shape into a reported,
				// undeclared leaf rather than a silent exact comparison.
				Parity: Options{NumericLeavesDeclared: true, BaselineDefects: []BaselineDefect{{
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
	"REST:GET:/api/v1/heatmap": {
		Method: "GET",
		Path:   "/api/v1/heatmap",
		Requests: []RESTRequest{
			{
				// org scope (the default): no repo resolution query
				// runs at all (scopeFilterForMetric's own org-scope
				// no-op, heatmap package doc comment), so this exercises
				// only the metric read itself.
				Name:                "review_wait_density_org",
				Query:               url.Values{"type": {"temporal_load"}, "metric": {"review_wait_density"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: heatmapReviewWaitDensityParity,
			},
			{
				// repo scope, scope_id bound to filters/options' own
				// live repo_id -- exercises resolveRepoFilterIDs's own
				// repos-FINAL read.
				Name:                "repo_touchpoints_repo_scoped",
				Query:               url.Values{"type": {"context_switch"}, "metric": {"repo_touchpoints"}, "scope_type": {"repo"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     heatmapRepoTouchpointsParity,
				IDBindings: []RESTIDBinding{{Producer: "repo_id", QueryParam: "scope_id"}},
			},
			{
				Name:                "hotspot_risk_org",
				Query:               url.Values{"type": {"risk"}, "metric": {"hotspot_risk"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: heatmapHotspotRiskParity,
			},
			{
				// team scope, scope_id bound to filters/options' own live
				// team_id. Timeout raised: the baseline leg's own
				// team-to-repo resolution is an extra read the org-scope
				// default entry never makes (heatmapRepoTouchpointsTeamScope
				// SubsetDefect's own Reason states the two tables).
				Name:                "repo_touchpoints_team_scoped",
				Query:               url.Values{"type": {"context_switch"}, "metric": {"repo_touchpoints"}, "scope_type": {"team"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     heatmapRepoTouchpointsTeamScopedParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
				Timeout:    180 * time.Second,
			},
			{
				// team scope, same binding as repo_touchpoints_team_scoped
				// above.
				Name:                "hotspot_risk_team_scoped",
				Query:               url.Values{"type": {"risk"}, "metric": {"hotspot_risk"}, "scope_type": {"team"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     heatmapHotspotRiskTeamScopedParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
				Timeout:    180 * time.Second,
			},
			{
				// team scope, same binding as repo_touchpoints_team_scoped
				// above.
				Name:                "review_wait_density_team_scoped",
				Query:               url.Values{"type": {"temporal_load"}, "metric": {"review_wait_density"}, "scope_type": {"team"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     heatmapReviewWaitDensityTeamScopedParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
				Timeout:    180 * time.Second,
			},
			{
				// developer scope, scope_id bound to people's own live
				// person_id (GET /api/v1/people's query_string_search
				// entry) -- exercises identity resolution plus the
				// git_commits-side dedup read.
				Name:                "active_hours_person_scoped",
				Query:               url.Values{"type": {"individual"}, "metric": {"active_hours"}, "scope_type": {"developer"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     heatmapActiveHoursParity,
				IDBindings: []RESTIDBinding{{Producer: "person_id", QueryParam: "scope_id"}},
			},
			{
				// newHeatmapWorkHandler aggregates every invalid query
				// param into ONE 422, byte-shaped like Pydantic's own
				// aggregation -- missing type+metric answers the same
				// missingFieldError shape quadrant/drilldown/prs already
				// use.
				Name:                "missing_type_and_metric",
				Query:               url.Values{},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// _metric_for's own 404, surfaced through the HTTP layer
				// via heatmap.AsRequestError.
				Name:                "unknown_metric",
				Query:               url.Values{"type": {"temporal_load"}, "metric": {"not_a_real_metric"}},
				WantCandidateStatus: 404, WantBaselineStatus: 404,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// ScopeFilter.level's Literal-set validation, surfaced as
				// 400 "Invalid scope filter".
				Name:                "invalid_scope_type",
				Query:               url.Values{"type": {"temporal_load"}, "metric": {"review_wait_density"}, "scope_type": {"bogus"}},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// _reject_comparative_params (main.py:202-208): ANY of
				// these query-param keys present is a 400, regardless of
				// value -- the same shape people's own corpus entry
				// already exercises for the identical shared check.
				Name:                "comparative_param_rejected",
				Query:               url.Values{"type": {"temporal_load"}, "metric": {"review_wait_density"}, "rank": {"1"}},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:GET:/api/v1/sankey": {
		Method: "GET",
		Path:   "/api/v1/sankey",
		Requests: []RESTRequest{
			{
				// mode defaults to "investment" (main.py's own query-param
				// default), org scope -- no repo resolution query runs at
				// all.
				Name:                "investment_default_org",
				Query:               url.Values{},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: sankeyInvestmentParity,
			},
			{
				Name:                "expense_org",
				Query:               url.Values{"mode": {"expense"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: sankeyCycleTimesDedupParity,
			},
			{
				Name:                "state_org",
				Query:               url.Values{"mode": {"state"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: sankeyStateFlowParity,
			},
			{
				Name:                "hotspot_org",
				Query:               url.Values{"mode": {"hotspot"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: sankeyHotspotParity,
			},
			{
				// scope_type=repo, scope_id bound to filters/options' own
				// live repo_id -- exercises resolveRepoFilterIDs' own
				// repos-FINAL read on the hotspot mode's repo-column
				// variant ("metrics.repo_id").
				Name:                "hotspot_repo_scoped",
				Query:               url.Values{"mode": {"hotspot"}, "scope_type": {"repo"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     sankeyHotspotParity,
				IDBindings: []RESTIDBinding{{Producer: "repo_id", QueryParam: "scope_id"}},
			},
			{
				// team scope, scope_id bound to filters/options' own live
				// team_id. Timeout raised: the baseline leg's own
				// team-to-repo resolution is an extra read the org-scope
				// default entry never makes (sankeyNodesTeamScopeSubsetDefect's
				// own Reason states the two tables).
				Name:                "investment_team_scoped",
				Query:               url.Values{"scope_type": {"team"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     sankeyInvestmentTeamScopedParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
				Timeout:    180 * time.Second,
			},
			{
				// team scope, same binding as investment_team_scoped above.
				Name:                "hotspot_team_scoped",
				Query:               url.Values{"mode": {"hotspot"}, "scope_type": {"team"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     sankeyHotspotTeamScopedParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
				Timeout:    180 * time.Second,
			},
			{
				// team scope on expense mode -- FREE COVERAGE, expected
				// PLAIN MATCH (this Options value's own doc comment).
				Name:                "expense_team_scoped",
				Query:               url.Values{"mode": {"expense"}, "scope_type": {"team"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     sankeyCycleTimesDedupParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
				Timeout:    180 * time.Second,
			},
			{
				// team scope on state mode -- FREE COVERAGE, expected PLAIN
				// MATCH.
				Name:                "state_team_scoped",
				Query:               url.Values{"mode": {"state"}, "scope_type": {"team"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     sankeyStateFlowParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
				Timeout:    180 * time.Second,
			},
			{
				// build_sankey_response's own ValueError for an unknown
				// mode string reaches sankey_get's generic
				// `except Exception: 503`, never a 400/404 -- no dedicated
				// validation surface the way heatmap's `type`/`metric`
				// pair has one (route file's own package doc comment).
				Name:                "unknown_mode_is_503",
				Query:               url.Values{"mode": {"not-a-real-mode"}},
				WantCandidateStatus: 503, WantBaselineStatus: 503,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// _filters_from_query's own ScopeFilter(level=scope_type)
				// construction raises for any scope_type outside the
				// Literal set, INSIDE sankey_get's try block -- the same
				// generic 503, not heatmap's dedicated 400 "Invalid scope
				// filter" (that check lives in build_heatmap_response
				// itself, sankey has no equivalent).
				Name:                "invalid_scope_type_is_503",
				Query:               url.Values{"scope_type": {"bogus"}},
				WantCandidateStatus: 503, WantBaselineStatus: 503,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:POST:/api/v1/sankey": {
		Method: "POST",
		Path:   "/api/v1/sankey",
		Requests: []RESTRequest{
			{
				Name:                "investment_default_org",
				Body:                map[string]any{"mode": "investment", "filters": map[string]any{}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: sankeyInvestmentParity,
			},
			{
				Name:                "expense_org",
				Body:                map[string]any{"mode": "expense", "filters": map[string]any{}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: sankeyCycleTimesDedupParity,
			},
			{
				Name:                "state_org",
				Body:                map[string]any{"mode": "state", "filters": map[string]any{}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: sankeyStateFlowParity,
			},
			{
				Name:                "hotspot_org",
				Body:                map[string]any{"mode": "hotspot", "filters": map[string]any{}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: sankeyHotspotParity,
			},
			{
				// team scope, bound to filters/options' own live team_id --
				// the POST-body twin of GET's own investment_team_scoped
				// sibling above.
				Name: "investment_team_scoped",
				Body: map[string]any{
					"mode": "investment", "filters": map[string]any{"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}}},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     sankeyInvestmentTeamScopedParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
				Timeout:    180 * time.Second,
			},
			{
				// team scope, same binding as investment_team_scoped above.
				Name: "hotspot_team_scoped",
				Body: map[string]any{
					"mode": "hotspot", "filters": map[string]any{"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}}},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     sankeyHotspotTeamScopedParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
				Timeout:    180 * time.Second,
			},
			{
				// team scope on expense mode -- FREE COVERAGE, expected
				// PLAIN MATCH.
				Name: "expense_team_scoped",
				Body: map[string]any{
					"mode": "expense", "filters": map[string]any{"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}}},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     sankeyCycleTimesDedupParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
				Timeout:    180 * time.Second,
			},
			{
				// team scope on state mode -- FREE COVERAGE, expected PLAIN
				// MATCH.
				Name: "state_team_scoped",
				Body: map[string]any{
					"mode": "state", "filters": map[string]any{"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}}},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     sankeyStateFlowParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
				Timeout:    180 * time.Second,
			},
			{
				// Confirmed live shape (pydantic_validation_error.go): a
				// body with neither "mode" nor "filters" produces
				// byte-parity 422 `missing` envelopes on both planes, in
				// SankeyRequest's own field order.
				Name:                "missing_mode_and_filters",
				Body:                map[string]any{},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// mode outside the four-way Literal is a 422 literal_error,
				// never reaching build_sankey_response's own ValueError at
				// all -- unlike GET's unknown_mode_is_503 sibling above.
				Name:                "invalid_mode_literal",
				Body:                map[string]any{"mode": "not-a-real-mode", "filters": map[string]any{}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:POST:/api/v1/investment/flow": {
		Method: "POST",
		Path:   "/api/v1/investment/flow",
		Requests: []RESTRequest{
			{
				// flow_mode omitted -- build_investment_flow_response's own
				// dynamic (coverage-driven team/repo_scope/fallback)
				// branch, org scope.
				Name:                "dynamic_default_org",
				Body:                map[string]any{"filters": map[string]any{}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: investmentFlowDynamicParity,
			},
			{
				// team scope, bound to filters/options' own live team_id --
				// the POST-body twin of every other team_scoped entry's own
				// BodyPath binding in this corpus. Timeout raised: the
				// baseline leg's own team-to-repo resolution is an extra read
				// the org-scope default entry never makes.
				Name: "team_scoped",
				Body: map[string]any{
					"filters": map[string]any{"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}}},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     investmentFlowDynamicTeamScopedParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
				Timeout:    180 * time.Second,
			},
			{
				// repo scope: BOUNDED CANDIDATE ITERATION over filters/
				// options' own repo_id list (up to 10), exposed as
				// investment_repo_id once either leg of a candidate yields
				// investment_repo_flow_node below and its compare is
				// admitted. filters/options' first repo alone can
				// easily hold zero investment rows for the requested
				// window; this route then answers empty nodes/links with
				// non-null labels and zeroed coverage, which is not a
				// vacuous body, so the declared node list -- empty on
				// both legs -- is what makes that candidate lose
				// (RESTRefusalNoLegProducedTheDeclaredID) and the search
				// move on. investment_repo_id is the winning
				// candidate, consumed by every other repo_scoped entry on
				// the sibling investment-family routes below (restRunOrder
				// runs this operation first among them) instead of each
				// running its own independent search -- the same single-
				// resolution-then-reuse shape drilldown/prs' own
				// drilldown_prs_default person_id iteration already
				// establishes for the sibling person routes.
				Name: "repo_scoped",
				Body: map[string]any{
					"filters": map[string]any{"scope": map[string]any{"level": "repo", "ids": []string{"11111111-1111-1111-1111-111111111111"}}},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     investmentFlowDynamicParity,
				IDBindings: []RESTIDBinding{{Producer: "repo_id", BodyPath: "filters.scope.ids", Candidates: 10, ExposeAs: "investment_repo_id"}},
				// The win condition above: a node in the candidate
				// repository's own flow, on either plane. A repository
				// with investment rows in the window always answers at
				// least one node (a subcategory node on the single-
				// repository fallback branch); one without answers none.
				Produces: []RESTIDProducer{{Name: "investment_repo_flow_node", ListPath: "nodes", IDField: "name"}},
			},
			{
				Name:                "team_category_repo_org",
				Body:                map[string]any{"filters": map[string]any{}, "flow_mode": "team_category_repo"},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: investmentFlowModeParity,
			},
			{
				Name:                "team_category_subcategory_repo_org",
				Body:                map[string]any{"filters": map[string]any{}, "flow_mode": "team_category_subcategory_repo"},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: investmentFlowModeParity,
			},
			{
				// flow_mode="team_subcategory_repo" WITH drill_category --
				// the one flow_mode value that requires it (see the
				// missing_drill_category_is_400 entry below for the ValueError
				// branch when it is absent).
				Name:                "team_subcategory_repo_with_drill_org",
				Body:                map[string]any{"filters": map[string]any{}, "flow_mode": "team_subcategory_repo", "drill_category": "feature_delivery"},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: investmentFlowModeParity,
			},
			{
				// build_investment_flow_response's own ValueError -- the
				// ONE case this route's error mapping answers 400 (str(exc)
				// as the detail) rather than 503, confirmed byte-identical
				// on both planes (the message is a fixed literal, not
				// request-derived, so no Parity/BaselineDefect is needed).
				Name:                "missing_drill_category_is_400",
				Body:                map[string]any{"filters": map[string]any{}, "flow_mode": "team_subcategory_repo"},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// flow_mode outside the closed Literal set is a 422
				// literal_error, never reaching build_investment_flow_
				// response's own ValueError at all.
				Name:                "invalid_flow_mode_literal",
				Body:                map[string]any{"filters": map[string]any{}, "flow_mode": "not-a-real-mode"},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// No body at all -- InvestmentFlowRequest is a required
				// body parameter even though every one of its own fields
				// has a default; confirmed live (this route's own
				// TEST-EVIDENCE) that FastAPI still answers a top-level
				// "missing" 422 for a genuinely absent body.
				Name:                "missing_body_is_422",
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:POST:/api/v1/investment/flow/repo-team": {
		Method: "POST",
		Path:   "/api/v1/investment/flow/repo-team",
		Requests: []RESTRequest{
			{
				Name:                "default_org",
				Body:                map[string]any{"filters": map[string]any{}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: investmentFlowRepoTeamParity,
			},
			{
				Name:                "theme_scoped_org",
				Body:                map[string]any{"filters": map[string]any{}, "theme": "feature_delivery"},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: investmentFlowRepoTeamParity,
			},
			{
				// team scope, same binding as investment/flow's own
				// team_scoped entry above.
				Name: "team_scoped",
				Body: map[string]any{
					"filters": map[string]any{"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}}},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     investmentFlowRepoTeamTeamScopedParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
				Timeout:    180 * time.Second,
			},
			{
				// repo scope, bound to investment/flow's own winning
				// investment_repo_id (POST /api/v1/investment/flow runs
				// strictly before this operation in restRunOrder) -- the
				// same repo every repo_scoped entry on these two routes
				// reuses, never a second independent candidate search.
				Name: "repo_scoped",
				Body: map[string]any{
					"filters": map[string]any{"scope": map[string]any{"level": "repo", "ids": []string{"11111111-1111-1111-1111-111111111111"}}},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     investmentFlowRepoTeamParity,
				IDBindings: []RESTIDBinding{{Producer: "investment_repo_id", BodyPath: "filters.scope.ids"}},
			},
			{
				// investment_flow_repo_team has NO ValueError branch at all
				// (unlike its investment/flow sibling): every failure this
				// route's own body can raise is caught by the generic
				// `except Exception: 503`. flow_mode/drill_category/top_n_repos
				// are still validated (the wire model is shared) even though
				// this route's own handler ignores them once parsing
				// succeeds -- an invalid flow_mode here is still a 422, not
				// a request that silently proceeds.
				Name:                "invalid_flow_mode_literal_still_422",
				Body:                map[string]any{"filters": map[string]any{}, "flow_mode": "not-a-real-mode"},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				Name:                "missing_body_is_422",
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
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
				// Produces pr_id from this response's own FIRST item as
				// "<repo_id>:<number>" -- flame's own "pr" entity_id
				// shape (parseRepoEntity, internal/queryapi/flame)
				// -- via restidbind.go's JoinField, so both halves come
				// from the SAME PR rather than an independently-produced
				// repo_id that might name a different repo. Consumed by
				// flame's own pr_entity_id_bound_200 entry below.
				//
				// Also produces pr_repo_id: this SAME response's own FIRST
				// item's repo_id alone, with no JoinField -- a repo known,
				// from this response, to have at least one PR in-window,
				// unlike filters/options' own repo_id (an arbitrary repo
				// off the org's full repo list, with no PR-count signal at
				// all). Consumed by this operation's own "repo_scoped"
				// entry below, so that entry names a repo drilldown/prs can
				// actually return something for instead of a repo whose
				// only guarantee is existing.
				Produces: []RESTIDProducer{
					{Name: "pr_id", ListPath: "items", IDField: "repo_id", JoinField: "number"},
					{Name: "pr_repo_id", ListPath: "items", IDField: "repo_id"},
				},
			},
			{
				Name:                "range_days_90",
				Query:               url.Values{"range_days": {"90"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownPRsParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
			},
			{
				// scope_type=repo, scope_id bound at run time to this
				// operation's own "default_window" entry above -- pr_repo_id,
				// a repo that response's own PR list already showed to have
				// a PR in-window (see that entry's own doc comment), rather
				// than filters/options' own repo_id, which names an
				// arbitrary org repo with no PR-count signal and can
				// resolve to a repo with zero PRs.
				Name:                "repo_scoped",
				Query:               url.Values{"scope_type": {"repo"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownPRsParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
				IDBindings: []RESTIDBinding{{Producer: "pr_repo_id", QueryParam: "scope_id"}},
			},
			{
				// team scope, scope_id bound to filters/options' own live
				// team_id, bound to a FIXED window (start_date/end_date, both
				// GET query params drilldown_prs itself accepts, api/main.py:
				// 940-947) rather than range_days -- drilldownPRsTeamScope
				// SubsetDefect's own doc comment states why this window and
				// no other. Timeout raised: the baseline leg's own
				// team-to-repo resolution is an extra read the org-scope
				// default entry never makes.
				Name:                "team_scoped",
				Query:               url.Values{"scope_type": {"team"}, "start_date": {"2026-08-15"}, "end_date": {"2026-08-17"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownPRsTeamScopedParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
				Timeout:    180 * time.Second,
			},
			{
				// CHAOS-5988: the SAME team as this operation's own
				// team_scoped entry above, over a window ONE day wide
				// instead of two (start_date == end_date's own day) --
				// GET carries no limit parameter at all, so a short
				// enough window is the ONLY way to keep this route's own
				// fixed 50-row limit from ever risking a genuine cut.
				Name:                "team_scoped_short_window",
				Query:               url.Values{"scope_type": {"team"}, "start_date": {"2026-08-15"}, "end_date": {"2026-08-16"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownPRsTeamScopedParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
				Timeout:    180 * time.Second,
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
				BodyMode: RESTBodyModeJSON, Parity: drilldownPRsParityWithLimit(25),
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
			},
			{
				// team scope, bound to filters/options' own live team_id, over
				// the SAME fixed window as GET's own team_scoped sibling
				// above (filters.time.start_date/end_date, the POST-body
				// twin of GET's own start_date/end_date query params).
				Name: "team_scoped",
				Body: map[string]any{
					"filters": map[string]any{
						"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}},
						"time":  map[string]any{"start_date": "2026-08-15", "end_date": "2026-08-17"},
					},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownPRsTeamScopedParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
				IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
				Timeout:    180 * time.Second,
			},
			{
				// CHAOS-5988: the SAME team and window as this operation's
				// own team_scoped entry above, over a window ONE day wide
				// instead of two -- POST's own short-window twin of GET's
				// own team_scoped_short_window sibling.
				Name: "team_scoped_short_window",
				Body: map[string]any{
					"filters": map[string]any{
						"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}},
						"time":  map[string]any{"start_date": "2026-08-15", "end_date": "2026-08-16"},
					},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownPRsTeamScopedParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
				IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
				Timeout:    180 * time.Second,
			},
			{
				// CHAOS-5988: the SAME team and window as this operation's
				// own team_scoped entry above, with an explicit `limit`
				// high enough (500) that the true population -- doubled by
				// the fan-out or not -- has ample headroom under it. POST's
				// own limit has no ceiling on either plane (see
				// drilldownPRsTeamScopeUncutDefect's own Reason), unlike
				// GET, which carries no limit parameter at all.
				Name: "team_scoped_high_limit",
				Body: map[string]any{
					"filters": map[string]any{
						"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}},
						"time":  map[string]any{"start_date": "2026-08-15", "end_date": "2026-08-17"},
					},
					"limit": 500,
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON, Parity: drilldownPRsTeamScopedParityWithLimit(500),
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
				IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
				Timeout:    180 * time.Second,
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
	"REST:GET:/api/v1/investment": {
		Method: "GET",
		Path:   "/api/v1/investment",
		Requests: []RESTRequest{
			{
				Name:                "default_window",
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Parity:   investmentParity,
			},
			{
				Name:                "range_days_90",
				Query:               url.Values{"range_days": {"90"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Parity:   investmentParity,
			},
			{
				// team scope, scope_id bound to filters/options' own live
				// team_id. Timeout raised: the baseline leg's own
				// team-to-repo resolution is an extra read the org-scope
				// default entry never makes.
				Name:                "team_scoped",
				Query:               url.Values{"scope_type": {"team"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     investmentTeamScopedParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
				Timeout:    180 * time.Second,
			},
			{
				// repo scope, bound to investment/flow's own winning
				// investment_repo_id -- both planes resolve an explicit
				// repo ref through the identical FINAL-and-org-scoped
				// resolver (investmentexplain/repofilter.go's own doc
				// comment claims an exact Python port for this branch), so
				// this reuses plain investmentParity, never the team-only
				// investmentTeamScopedParity (its own dict-subset citations
				// name the team_repo_ownership-vs-user_metrics_daily.
				// team_id divergence, which an explicit repo ref never
				// reaches).
				Name:                "repo_scoped",
				Query:               url.Values{"scope_type": {"repo"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     investmentParity,
				IDBindings: []RESTIDBinding{{Producer: "investment_repo_id", QueryParam: "scope_id"}},
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
	"REST:POST:/api/v1/investment": {
		Method: "POST",
		Path:   "/api/v1/investment",
		Requests: []RESTRequest{
			{
				Name:                "default_filters",
				Body:                map[string]any{"filters": map[string]any{}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Parity:   investmentParity,
			},
			{
				// team scope, bound to filters/options' own live team_id.
				Name: "team_scoped",
				Body: map[string]any{
					"filters": map[string]any{"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}}},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     investmentTeamScopedParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
				Timeout:    180 * time.Second,
			},
			{
				// repo scope, bound to investment/flow's own winning
				// investment_repo_id -- see GET /api/v1/investment's own
				// repo_scoped entry for why plain investmentParity applies.
				Name: "repo_scoped",
				Body: map[string]any{
					"filters": map[string]any{"scope": map[string]any{"level": "repo", "ids": []string{"11111111-1111-1111-1111-111111111111"}}},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     investmentParity,
				IDBindings: []RESTIDBinding{{Producer: "investment_repo_id", BodyPath: "filters.scope.ids"}},
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
	"REST:GET:/api/v1/investment/sunburst": {
		Method: "GET",
		Path:   "/api/v1/investment/sunburst",
		Requests: []RESTRequest{
			{
				Name:                "default_window",
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Parity:   investmentSunburstParityWithLimit(investmentSunburstDefaultLimit),
			},
			{
				Name:                "explicit_limit",
				Query:               url.Values{"limit": {"50"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Parity:   investmentSunburstParityWithLimit(50),
			},
			{
				// team scope, scope_id bound to filters/options' own live
				// team_id.
				Name:                "team_scoped",
				Query:               url.Values{"scope_type": {"team"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     investmentSunburstTeamScopedParityWithLimit(investmentSunburstDefaultLimit),
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
				Timeout:    180 * time.Second,
			},
			{
				// repo scope, bound to investment/flow's own winning
				// investment_repo_id -- see GET /api/v1/investment's own
				// repo_scoped entry for why plain investmentSunburstParity
				// (no team-only subset defect) applies: GROUP BY theme,
				// subcategory, scope already names one repository per key,
				// and the explicit-ref resolver is the same exact port as
				// investment's own.
				Name:                "repo_scoped",
				Query:               url.Values{"scope_type": {"repo"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     investmentSunburstParityWithLimit(investmentSunburstDefaultLimit),
				IDBindings: []RESTIDBinding{{Producer: "investment_repo_id", QueryParam: "scope_id"}},
			},
			{
				Name:                "invalid_limit",
				Query:               url.Values{"limit": {"not-a-number"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
		},
	},
	"REST:POST:/api/v1/investment/explain": {
		Method:          "POST",
		Path:            "/api/v1/investment/explain",
		PythonForwarder: true,
		Requests: []RESTRequest{
			{
				Name:                "default_filters",
				Body:                map[string]any{"filters": map[string]any{}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode: RESTBodyModeJSON,
				Parity: Options{
					VolatileFields:        investmentExplainProseFields,
					NumericLeavesDeclared: true,
					FloatTierB:            investmentExplainDeterministicFloats,
					IntegerLeaves:         investmentExplainIntegerLeaves,
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
			{
				// team scope bound to filters/options' own live team_id
				// (restidbind.go's BodyPath binding, the POST-body twin of
				// the QueryParam binding every GET team-scoped entry in
				// this corpus uses) -- exercises scopeRepoFilter's team
				// branch (ResolveRepoFilterIDs + teamscope.RepoCondition)
				// against a team that genuinely resolves to repositories,
				// the same request shape a real team-scoped client sends.
				// The two planes resolve that team's repositories from
				// different tables, so this entry is one of the knowingly
				// uncovered findings this file's own TEAM SCOPE paragraph
				// names, not a declared defect.
				// llm_provider=mock is a query param BOTH planes honor
				// identically: Python's investment_explain route
				// (api/main.py) forwards it into is_llm_available/
				// resolve_provider_name unchanged; Go's investment_explain
				// _route.go forwards it into IsLLMAvailableForOrg/
				// ResolveProviderKindForOrg, the org-aware siblings of the
				// same resolution chain. "mock" short-circuits both chains
				// to the mock provider before either ever consults an
				// operator/org-configured real provider (auto-detected env
				// credentials, or an org's own BYO setting) -- confirmed
				// from source: Python's resolve_provider_name (llm/
				// providers/__init__.py) returns an explicit, non-"auto"
				// request UNCHANGED, and Go's ResolveProviderKindForOrg
				// (categorize/providerkind.go) does the identical thing at
				// its own first line. The mock provider is always
				// available and answers LOCALLY on both planes (Python's
				// llm/providers/mock.py: "without external API calls"; Go
				// providerHasRequiredConfig's ProviderKindMock case,
				// provider.go), so this request never reaches an external
				// LLM, unlike default_filters above (whose own "auto"
				// resolution, unset here, can select a real, org/env-
				// configured provider and a genuinely cached response).
				Name:  "team_scoped",
				Query: url.Values{"llm_provider": {"mock"}},
				Body: map[string]any{
					"filters": map[string]any{"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}}},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
				Parity: Options{
					VolatileFields:        investmentExplainProseFields,
					NumericLeavesDeclared: true,
					FloatTierB:            investmentExplainDeterministicFloats,
					IntegerLeaves:         investmentExplainIntegerLeaves,
				},
			},
			{
				// repo scope via filters.what.repos -- the POST-only field
				// this route's own body decode supports -- bound to
				// filters/options' own live repo_id, same BodyPath and
				// llm_provider=mock reasoning as team_scoped above.
				Name:  "repo_scoped",
				Query: url.Values{"llm_provider": {"mock"}},
				Body: map[string]any{
					"filters": map[string]any{"what": map[string]any{"repos": []string{"11111111-1111-1111-1111-111111111111"}}},
				},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				IDBindings: []RESTIDBinding{{Producer: "repo_id", BodyPath: "filters.what.repos"}},
				Parity: Options{
					VolatileFields:        investmentExplainProseFields,
					NumericLeavesDeclared: true,
					FloatTierB:            investmentExplainDeterministicFloats,
					IntegerLeaves:         investmentExplainIntegerLeaves,
				},
			},
		},
	},
	"REST:GET:/api/v1/drilldown/issues": {
		Method: "GET",
		Path:   "/api/v1/drilldown/issues",
		Requests: []RESTRequest{
			{
				// The baseline plane answers HTTP 503 for this request in
				// production; the candidate answers HTTP 200 with real
				// data (see StatusDivergenceReason below).
				Name:                "default_window",
				WantCandidateStatus: 200, WantBaselineStatus: 503,
				StatusDivergenceReason: "CHAOS-5868: the baseline plane answers HTTP 503 for this request in production; the candidate plane answers HTTP 200 with real data; bodies are not compared.",
				BodyMode:               RESTBodyModeStatusOnly,
				// Produces declares work_item_id for flame's own
				// issue_entity_id_bound_200 entry below. BodyMode:
				// status_only means this request's body is never
				// compared between planes, but it IS still decoded for
				// this: ValidateRESTCorpus's isBaselineOnlyFailure
				// exception lets a status-only, declared-failing-baseline
				// request Produce ids read from the CANDIDATE leg
				// (proveOneRESTRequest's own doc comment,
				// cmd/go-api-rest-prove/main.go). A body that does not
				// yield work_item_id refuses THIS request by name
				// (RESTRefusalCandidateProducerUnresolved for an empty
				// list, RESTRefusalDeclaredIDListUnrecognised for any
				// other shape), and the
				// consumer refuses separately, by name
				// (rest_request_id_binding_unresolved). This request's
				// own successful admission today is this entry's own
				// structural evidence: an empty or malformed candidate
				// body would have already refused it before this comment
				// is ever reached. Also Produces issues_org_provider, off
				// the same list -- an org-wide provider value GET
				// /api/v1/flame/aggregated's own cycle_breakdown_provider_scoped
				// entry consumes, chosen over the person-scoped issues
				// route precisely because this request is org-wide: it
				// carries no dependency on which person the people-search
				// producer happens to bind, unlike a person-scoped read
				// whose bound person may have no issues at all.
				Produces: []RESTIDProducer{
					{Name: "work_item_id", ListPath: "items", IDField: "work_item_id"},
					{Name: "issues_org_provider", ListPath: "items", IDField: "provider"},
				},
			},
			{
				// Same baseline-only 503 as default_window above. Produces
				// its own structural evidence (see default_window's own
				// doc comment for the mechanism) on a different IssueItem
				// field than default_window's own producer.
				Name:                "range_days_90",
				Query:               url.Values{"range_days": {"90"}},
				WantCandidateStatus: 200, WantBaselineStatus: 503,
				StatusDivergenceReason: "CHAOS-5868: the baseline plane answers HTTP 503 for this request in production; the candidate plane answers HTTP 200 with real data; bodies are not compared.",
				BodyMode:               RESTBodyModeStatusOnly,
				Produces:               []RESTIDProducer{{Name: "issues_range_days_90_provider", ListPath: "items", IDField: "provider"}},
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
			{
				// start_date/end_date query params (route.go's own
				// parseISODateQueryParam handling) -- the one branch of
				// this GET route's own date parsing no other entry
				// reaches. Same wide window GET /api/v1/quadrant's own
				// custom_window_org entry already carries live, reused here
				// rather than guessed fresh, to maximize the chance this
				// range actually holds at least one issue. Same
				// baseline-only 503 as default_window above.
				Name:                "explicit_window",
				Query:               url.Values{"start_date": {"2026-06-01"}, "end_date": {"2026-09-01"}},
				WantCandidateStatus: 200, WantBaselineStatus: 503,
				StatusDivergenceReason: "CHAOS-5868: the baseline plane answers HTTP 503 for this request in production; the candidate plane answers HTTP 200 with real data; bodies are not compared.",
				BodyMode:               RESTBodyModeStatusOnly,
				Produces:               []RESTIDProducer{{Name: "issues_explicit_window_status", ListPath: "items", IDField: "status"}},
			},
			{
				// A malformed start_date -- the same shared
				// parseISODateQueryParam/dateQueryParamError shape
				// drilldown/prs' own GET and quadrant's own
				// malformed_start_date entry already exercise
				// (pydantic_validation_error.go unchanged): a real 422 on
				// both planes, ahead of any ClickHouse call.
				Name:                "malformed_start_date",
				Query:               url.Values{"start_date": {"not-a-date"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// scope_type=team, scope_id bound to filters/options' own
				// live team_id (restidbind.go) -- exercises scopeClauseTeam's
				// own team branch (issues.go:98-105) live. This team_id
				// carries no issue-count guarantee, so this alone does not
				// prove the filter NARROWS the result -- only that the
				// branch executes and answers a non-empty body (this
				// entry's own Produces refuses it otherwise).
				// TestBuildIssuesResponseTeamScopeBindsScopeIDs/
				// TestBuildIssuesResponseBindsExpectedParams (issues_test.go)
				// pin the emitted predicate/binding shape itself at the unit
				// level. Unlike default_window/range_days_90/explicit_window
				// above, the team-scoped statement adds `AND t.team_id IN
				// (...)` ahead of the FINAL-dedup join, which does not raise
				// the org-wide exception those entries' own baseline-only 503
				// comes from -- the baseline answers 200 here, so this entry
				// compares bodies under drilldownIssuesParity like every
				// other admissible request on this route, not status-only.
				Name:                "team_scoped",
				Query:               url.Values{"scope_type": {"team"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     drilldownIssuesParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", QueryParam: "scope_id"}},
				Produces:   []RESTIDProducer{{Name: "issues_team_scoped_work_item_id", ListPath: "items", IDField: "work_item_id"}},
			},
		},
	},
	"REST:POST:/api/v1/drilldown/issues": {
		Method: "POST",
		Path:   "/api/v1/drilldown/issues",
		Requests: []RESTRequest{
			{
				// Same baseline-only 503 as GET /api/v1/drilldown/issues'
				// own default_window entry. Produces its own structural
				// evidence (see that entry's own doc comment for the
				// candidate-leg extraction mechanism).
				Name:                "default_filters",
				Body:                map[string]any{"filters": map[string]any{}},
				WantCandidateStatus: 200, WantBaselineStatus: 503,
				StatusDivergenceReason: "CHAOS-5868: the baseline plane answers HTTP 503 for this request in production; the candidate plane answers HTTP 200 with real data; bodies are not compared.",
				BodyMode:               RESTBodyModeStatusOnly,
				Produces:               []RESTIDProducer{{Name: "issues_post_default_filters_provider", ListPath: "items", IDField: "provider"}},
			},
			{
				// This route's OWN documented asymmetry from drilldown/prs
				// (internal/drilldown/issues.go's own IssueParams doc
				// comment): scope_filter_for_metric's team-only branch list
				// means a "repo" scope level applies NO filter at all here
				// -- a real behavioural difference, not a port defect, so
				// no declaration covers it; this entry exercises exactly
				// that no-op path (an org-level default scope). The team
				// scope branch itself is exercised by this operation's own
				// team_scoped entry below, live.
				// Also baseline-only 503, same as this route's sibling
				// entries. Produces its own structural evidence.
				Name: "explicit_scope_and_sort",
				Body: map[string]any{
					"filters": map[string]any{
						"scope": map[string]any{"level": "org"},
						"time":  map[string]any{"range_days": 30},
					},
					"sort":  "started_at",
					"limit": 25,
				},
				WantCandidateStatus: 200, WantBaselineStatus: 503,
				StatusDivergenceReason: "CHAOS-5868: the baseline plane answers HTTP 503 for this request in production; the candidate plane answers HTTP 200 with real data; bodies are not compared.",
				BodyMode:               RESTBodyModeStatusOnly,
				Produces:               []RESTIDProducer{{Name: "issues_post_explicit_scope_and_sort_status", ListPath: "items", IDField: "status"}},
			},
			{
				// filters.scope.level outside {org, team, repo, service,
				// developer} -- the same Literal-enum 422 validateScopeFilter
				// answers (pydantic_metric_filter.go), confirmed live by
				// investment/explain's own sibling invalid_scope_level entry
				// sharing the identical validator and error envelope.
				Name: "invalid_scope_level",
				Body: map[string]any{"filters": map[string]any{
					"scope": map[string]any{"level": "not-a-real-scope-level"},
				}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// filters.scope.level="team" with a real, live team_id
				// (filters/options' own producer) bound via BodyPath -- the
				// POST-body twin of GET's own team_scoped entry above,
				// exercising the SAME scopeClauseTeam branch. Same caveat as
				// that entry: proves the branch executes and answers
				// non-empty, not that the filter narrows. Same
				// baseline-answers-200 reasoning as GET's own team_scoped
				// entry above: the team filter avoids the org-wide
				// exception, so this compares bodies under
				// drilldownIssuesParity, not status-only.
				Name: "team_scoped",
				Body: map[string]any{"filters": map[string]any{
					"scope": map[string]any{"level": "team", "ids": []string{"11111111-1111-1111-1111-111111111111"}},
				}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     drilldownIssuesParity,
				IDBindings: []RESTIDBinding{{Producer: "team_id", BodyPath: "filters.scope.ids"}},
				Produces:   []RESTIDProducer{{Name: "issues_post_team_scoped_work_item_id", ListPath: "items", IDField: "work_item_id"}},
			},
			{
				// sort is present but not a string -- newDrilldownIssuesPostHandler's
				// stringBodyFieldError branch (drilldown_issues_route.go),
				// the same shared validator drilldown/prs' own POST already
				// uses (confirmed live for every non-string JSON type,
				// stringBodyFieldError's own doc comment).
				Name: "sort_wrong_type",
				Body: map[string]any{
					"filters": map[string]any{},
					"sort":    123,
				},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// limit is present but not int-coercible -- coerceIntBodyField's
				// own int_parsing branch (drilldown_issues_route.go),
				// confirmed live for every branch by that function's own
				// doc comment; the same validator drilldown/prs' own POST
				// already uses.
				Name: "limit_wrong_type",
				Body: map[string]any{
					"filters": map[string]any{},
					"limit":   "not-a-number",
				},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
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
			explainTeamScopedRequest("review_latency_team_scoped", "review_latency", "team_id"),
			explainScopedRequest("deploy_freq_repo_scoped", "deploy_freq", "repo", "repo_id"),
			explainTeamScopedRequest("deploy_freq_team_scoped", "deploy_freq", "team_id"),
			explainScopedRequest("churn_repo_scoped", "churn", "repo", "repo_id"),
			explainTeamScopedRequest("churn_team_scoped", "churn", "team_id"),
			explainScopedRequest("change_failure_rate_repo_scoped", "change_failure_rate", "repo", "repo_id"),
			explainTeamScopedRequest("change_failure_rate_team_scoped", "change_failure_rate", "team_id"),
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
			{
				// The "pr" entity_type's own 200 path, reached at all
				// only via id binding: entity_id is bound at run time to
				// GET /api/v1/drilldown/prs' own live pr_id
				// (restidbind.go's JoinField-composed "<repo_id>:<number>",
				// this table's own doc comment above), the same
				// pattern peopleDetailParity's own person_id binding
				// already establishes for a path-segment id -- here a
				// query-param id instead. Reaches fetchPullRequest/
				// fetchPullRequestReviews' own declared FINAL-dedup fix
				// (flamePRIDBoundParity's own doc comment), unreachable
				// before a pr_id producer existed.
				Name:                "pr_entity_id_bound_200",
				Query:               url.Values{"entity_type": {"pr"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     flamePRIDBoundParity,
				IDBindings: []RESTIDBinding{{Producer: "pr_id", QueryParam: "entity_id"}},
			},
			{
				// The gap-declared counterpart to pr_entity_id_bound_200
				// above, the "pr" entity_type's own status divergence:
				// buildPRFlameResponse's end selection (flame.go:374-382)
				// now answers 422 for a terminal ("merged" or "closed") PR
				// with neither merged_at nor closed_at set -- a sync/lookup
				// gap, the same shape already fixed for deployments --
				// while the baseline's identical
				// fallback chain (services/flame.py:140) has no status
				// gate and still answers 200 for that row, unconditionally.
				// This is a real, reachable divergence at a route already
				// live in production (unlike deployment's own gap case,
				// this shape needs no transient "still running" window to
				// exist -- a legacy sync gap on a closed PR persists
				// indefinitely), so it is declared with its own live
				// comparing entry rather than left undeclared: entity_id
				// is bound via -bind pr_gap_id=... (this file's own
				// restOperatorSuppliedProducers, below), a deliberately
				// chosen PR known to carry this exact shape, distinct from
				// pr_entity_id_bound_200's own dynamically-resolved
				// default_window pick. Bodies are never compared
				// (RESTBodyModeStatusOnly): the baseline's is a real flame
				// response and the candidate's is a 422 error payload, two
				// different shapes by construction.
				Name:                "pr_gap_entity_id_bound_status_divergence",
				Query:               url.Values{"entity_type": {"pr"}},
				WantCandidateStatus: 422, WantBaselineStatus: 200,
				StatusDivergenceReason: "a terminal PR (state=\"merged\" or \"closed\") with neither merged_at nor closed_at set has no measurable duration on the candidate plane -- buildPRFlameResponse's end selection only falls back to the request clock for a genuinely non-terminal (\"open\") state, so this row's root frame is nil and validateFlameFrames refuses it: HTTP 422. The baseline plane's identical fallback chain (services/flame.py:140, merged_at or closed_at or now()) has no status gate at all and still resolves a real, non-empty interval ending at the request clock for the same row: HTTP 200. An intentional divergence, not a defect -- a closed PR must never render as still under review just because its own real end timestamp was never recorded.",
				BodyMode:               RESTBodyModeStatusOnly,
				IDBindings:             []RESTIDBinding{{Producer: "pr_gap_id", QueryParam: "entity_id"}},
			},
			{
				// The "issue" entity_type's own 200 path: entity_id is
				// bound to GET /api/v1/drilldown/issues' own live
				// work_item_id -- unlike "pr"/"deployment", the "issue"
				// entity_id IS the work_item_id directly (no repo_id
				// prefix; BuildResponse's own "issue" case passes
				// params.EntityID straight to fetchIssue). No Parity:
				// flame.go's own doc comment states fetch_issue already
				// reads work_item_cycle_times FINAL on both planes for
				// this route (the RMT-dedup fix flamePRIDBoundParity
				// declares for "pr" has no counterpart here, the same
				// asymmetry drilldownIssuesParity's own doc comment
				// states for the drilldown/issues routes, person-scoped
				// included).
				//
				// The baseline plane answers HTTP 503 for this request in
				// production (StatusDivergenceReason below) -- the same
				// server exception the issue-drilldown routes' own 503
				// entries share -- so this entry is declared status-only,
				// the same shape those entries use. Its producer, GET
				// /api/v1/drilldown/issues' own default_window entry,
				// still Produces work_item_id: ValidateRESTCorpus's
				// isBaselineOnlyFailure exception lets a status-only,
				// declared-failing-baseline request read an id from the
				// CANDIDATE leg's own body (that entry's own doc comment).
				//
				// This entry's own Produces declares a second id
				// (issue_flame_frame_id, off `frames`) purely as a
				// structural check on the candidate's own 200 body:
				// BodyMode: status_only compares no body between planes,
				// and this is the only live case of the "issue" branch,
				// so nothing else confirms the shape flame.Response/
				// Frame's own json tags promise on this route. Nothing
				// downstream consumes issue_flame_frame_id; a candidate
				// body that does not yield it refuses this request by
				// name (RESTRefusalCandidateProducerUnresolved for an
				// empty list, RESTRefusalDeclaredIDListUnrecognised for
				// any other shape).
				Name:                "issue_entity_id_bound_200",
				Query:               url.Values{"entity_type": {"issue"}},
				WantCandidateStatus: 200, WantBaselineStatus: 503,
				StatusDivergenceReason: "CHAOS-5868: the baseline plane's issue reads -- fetch_issue here, fetch_issues for GET /api/v1/drilldown/issues, and the people-scoped issue drilldown -- fail with the same ClickHouse server exception (code 10, NOT_FOUND_COLUMN_IN_BLOCK) raised while planning the pushed-down conjunction over `work_item_cycle_times AS wct FINAL` LEFT JOINed to the primary team-attribution subquery; the handler catches it generically and answers HTTP 503 with no logging. The candidate plane answers HTTP 200 with real data; bodies are not compared.",
				BodyMode:               RESTBodyModeStatusOnly,
				IDBindings:             []RESTIDBinding{{Producer: "work_item_id", QueryParam: "entity_id"}},
				Produces:               []RESTIDProducer{{Name: "issue_flame_frame_id", ListPath: "frames", IDField: "id"}},
			},
			{
				// The gap-declared counterpart to issue_entity_id_bound_200
				// above, the "issue" entity_type's own status divergence:
				// buildIssueFlameResponse's end selection (flame.go:429-430)
				// now answers 422 for a terminal ("done" or "canceled")
				// work item with no completed_at set -- a sync/lookup gap,
				// the same class deployment's own ruling fixed. The
				// baseline's OWN divergence here is dominated by the
				// unrelated, already-declared route-level 503
				// (issue_entity_id_bound_200's own StatusDivergenceReason,
				// above): that failure happens at ClickHouse's query-
				// PLANNING stage, before any row's own content is ever
				// evaluated, so it fires identically for this row too --
				// baseline never reaches its own "invented duration"
				// branch (services/flame.py:221) to compare against here.
				// Declared anyway, not left undeclared, because this
				// entry's real job is proving the CANDIDATE side of the
				// new gate fires for a genuinely gap-shaped row at a route
				// already live in production (this shape persists
				// indefinitely, no transient window needed) -- entity_id
				// is bound via -bind issue_gap_id=... (this file's own
				// restOperatorSuppliedProducers, below), a deliberately
				// chosen work item known to carry this exact shape,
				// distinct from issue_entity_id_bound_200's own
				// dynamically-resolved default_window pick. Bodies are
				// never compared (RESTBodyModeStatusOnly): the baseline's
				// is a generic 503 error payload and the candidate's is a
				// 422 error payload, two different shapes by construction.
				Name:                "issue_gap_entity_id_bound_status_divergence",
				Query:               url.Values{"entity_type": {"issue"}},
				WantCandidateStatus: 422, WantBaselineStatus: 503,
				StatusDivergenceReason: "the baseline plane's issue reads fail unconditionally with the same route-level 503 issue_entity_id_bound_200 already declares (a ClickHouse query-planning exception raised before any row's own content is evaluated), so this entry cannot observe whether baseline would also invent a duration for a terminal work item with no completed_at the way its own fallback chain (services/flame.py:221) has no gate against -- only that baseline never reaches 200 for ANY issue read today. The candidate plane's own gate (buildIssueFlameResponse, flame.go:429-430) does reach and correctly refuse this row: no completed_at and a terminal status category (\"done\"/\"canceled\", not one of backlog/todo/in_progress/in_review/blocked) leaves the root frame nil, and validateFlameFrames answers 422. Declared so a genuinely gap-shaped production row is proven to reach the new gate at all, not left unverified.",
				BodyMode:               RESTBodyModeStatusOnly,
				IDBindings:             []RESTIDBinding{{Producer: "issue_gap_id", QueryParam: "entity_id"}},
			},
			{
				// entity_id present, entity_type absent -- FastAPI's own
				// required-query-param validation (main.py:785-787, no
				// default on either param) answers a ONE-error 422 body,
				// distinct from missing_entity_type_and_id's two-error
				// body above. newFlameWorkHandler's own two independent
				// Query.Has checks (flame_route.go:173-174) mirror this:
				// only the entity_type branch fires here.
				Name:                "entity_type_missing_only",
				Query:               url.Values{"entity_id": {"whatever"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// entity_type present, entity_id absent -- the mirror of
				// entity_type_missing_only above; flame_route.go:176-177's
				// own independent Query.Has check.
				Name:                "entity_id_missing_only",
				Query:               url.Values{"entity_type": {"issue"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// _parse_repo_entity's own empty-suffix branch
				// (services/flame.py:59-60, "Entity id missing suffix"),
				// reached only after the separator AND repo-uuid checks
				// above already passed -- a syntactically valid repo id
				// with nothing after the colon. No corpus entry has
				// reached this branch before (pr_entity_id_missing_repo_
				// prefix and pr_entity_id_invalid_repo_uuid both fail
				// earlier in the same function). parseRepoEntity's own Go
				// port: internal/queryapi/flame/flame.go:178-180.
				Name: "pr_entity_id_missing_suffix",
				Query: url.Values{
					"entity_type": {"pr"}, "entity_id": {"00000000-0000-0000-0000-000000000000:"},
				},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// The "deployment" entity_type's own call into
				// _parse_repo_entity (services/flame.py:410, same function
				// pr_entity_id_missing_repo_prefix already covers via the
				// "pr" call site at services/flame.py:372) -- deterministic,
				// no ClickHouse call, no producer needed. BuildResponse's
				// own "deployment" case: internal/queryapi/flame/
				// flame.go:512-516.
				Name: "deployment_entity_id_missing_repo_prefix",
				Query: url.Values{
					"entity_type": {"deployment"}, "entity_id": {"not-prefixed-42"},
				},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// The "deployment" call site's own invalid-repo-uuid branch
				// (services/flame.py:57-58 via line 410).
				Name: "deployment_entity_id_invalid_repo_uuid",
				Query: url.Values{
					"entity_type": {"deployment"}, "entity_id": {"not-a-uuid:42"},
				},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// The "deployment" call site's own empty-suffix branch
				// (services/flame.py:59-60 via line 410) -- the deployment
				// counterpart to pr_entity_id_missing_suffix above.
				Name: "deployment_entity_id_missing_suffix",
				Query: url.Values{
					"entity_type": {"deployment"}, "entity_id": {"00000000-0000-0000-0000-000000000000:"},
				},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// build_flame_response's own PR-not-found branch
				// (services/flame.py:386-387, "PR not found"), reached
				// after a syntactically valid repo id and numeric suffix
				// both already parsed -- fetchPullRequest (cmd/query-api/
				// internal/flame/clickhouse.go:96-120) returns nil, nil for
				// a query that matches no row. No producer is needed: the
				// all-zero UUID is already this corpus's own convention for
				// a syntactically-valid, guaranteed-absent repo id (see
				// pr_entity_id_non_numeric_suffix above), paired here with
				// a PR number no live repo could plausibly reach.
				Name: "pr_entity_id_bound_not_found",
				Query: url.Values{
					"entity_type": {"pr"}, "entity_id": {"00000000-0000-0000-0000-000000000000:999999999"},
				},
				WantCandidateStatus: 404, WantBaselineStatus: 404,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// build_flame_response's own issue-not-found branch
				// (services/flame.py:404-405, "Issue not found") --
				// fetchIssue (clickhouse.go:192-212) returns nil, nil for a
				// work_item_id matching no row. The issue entity_id carries
				// no shape constraint (it IS the work_item_id, no repo
				// prefix -- flame.go's own package doc comment), so a
				// neutral, guaranteed-absent literal needs no producer.
				//
				// The baseline plane never reaches that branch: it shares
				// fetch_issue with issue_entity_id_bound_200 above, so the
				// same organisation-wide statement raises the same
				// ClickHouse code 10 exception while planning the
				// pushed-down conjunction, before any row lookup (found or
				// not) happens. The handler answers HTTP 503 regardless of
				// whether ABC-0 would have matched a row, so this entry is
				// declared status-only, the same shape issue_entity_id_
				// bound_200 uses. The candidate plane reaches its own
				// not-found branch and answers 404.
				Name: "issue_entity_id_not_found",
				Query: url.Values{
					"entity_type": {"issue"}, "entity_id": {"ABC-0"},
				},
				WantCandidateStatus: 404, WantBaselineStatus: 503,
				StatusDivergenceReason: "CHAOS-5868: the baseline plane's issue read -- shared with issue_entity_id_bound_200 above -- fails with the same ClickHouse server exception (code 10, NOT_FOUND_COLUMN_IN_BLOCK) raised while planning the pushed-down conjunction over `work_item_cycle_times AS wct FINAL` LEFT JOINed to the primary team-attribution subquery, before it ever reaches the not-found check; the handler catches it generically and answers HTTP 503 with no logging. The candidate plane reaches its own not-found branch and answers HTTP 404; bodies are not compared.",
				BodyMode:               RESTBodyModeStatusOnly,
			},
			{
				// The "deployment" entity_type's own declared 200 path
				// (services/flame.py:409-424, buildDeploymentFlameResponse
				// in internal/queryapi/flame/flame.go:414-473):
				// entity_id is bound to deployment_entity_id, a
				// restOperatorSuppliedProducers entry supplied by the run
				// invocation's own -bind flag rather than by an earlier
				// request's Produces -- no route this corpus covers reads
				// the deployments table, so no corpus request could ever
				// Produce this id itself (this file's own doc comment on
				// the flame corpus entry, above). On production data, 0 of
				// the proof organisation's 1135 deployments satisfy end >
				// start: every one carries only deployed_at, so this entry's
				// own -bind names a value no live deployment satisfies --
				// deployment_gap_entity_id_bound_422 below is the deployment
				// branch's only live case; this entry stays declared for a
				// deployment that does satisfy end > start.
				// fetch_deployment's own
				// WHERE already fully specifies deployments' ReplacingMergeTree
				// sort key (org_id, repo_id, deployment_id), so its ORDER
				// BY last_synced DESC LIMIT 1 read is already a
				// deterministic pick -- no BaselineDefect is declared for
				// this table (flame.go's own DATA-LAYER NOTE), unlike the
				// "pr" entity_type's own flamePRIDBoundParity. The
				// "deployment" entity dict (repo_id, deployment_id,
				// status, environment) carries no numeric field either
				// (same doc comment), so no IntegerLeaves declaration
				// applies. No Parity is declared here, the same as
				// issue_entity_id_bound_200 above.
				Name:                "deployment_entity_id_bound_200",
				Query:               url.Values{"entity_type": {"deployment"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				IDBindings: []RESTIDBinding{{Producer: "deployment_entity_id", QueryParam: "entity_id"}},
			},
			{
				// The gap-declared counterpart to deployment_entity_id_
				// bound_200 above. buildDeploymentFlameResponse's own root
				// frame (flame.go:505, newFrame at flame.go:187-195;
				// _build_deployment_flame_response's own root, services/
				// flame.py:298-308, _frame at flame.py:26-48) is nil
				// whenever end <= start -- and queue/pipeline/deploy
				// (flame.go:510-526 / flame.py:310-348) all carry a
				// non-nil ParentID, so root is the ONLY frame
				// validateFlameFrames' own top_level filter (flame.go:
				// 228-233 / flame.py:67) can ever see for this
				// entity_type. A nil root leaves top_level empty, and
				// validateFlameFrames refuses an empty top_level
				// immediately (flame.go:234-236 / flame.py:68-69) --
				// before its own start/end coverage check ever runs. A
				// deployment row carrying only deployed_at (started_at,
				// merged_at, finished_at all NULL) with a terminal status
				// hits exactly this: start = end = deployed_at
				// (flameIntervalEnd, flame.go:322-331, called at
				// flame.go:501 / flame.py:283-294's own coalesce chains)
				// -- there is no real end (FinishedAt nil), and
				// deploymentStatusIsRunning (flame.go:475-485) is false
				// for a terminal status, so flameIntervalEnd's own
				// default branch returns start itself, never nowLike().
				// On production data, 0 of the proof
				// organisation's 1135 deployments satisfy end > start:
				// every one carries only deployed_at with a terminal
				// status, so every one hits this path (see this file's
				// own "Branches this corpus cannot provably reach" doc
				// comment above). This entry, not deployment_entity_id_
				// bound_200 above, is the deployment branch's live
				// comparing case. Same operator-supplied-producer shape as
				// deployment_entity_id_bound_200: no route this corpus
				// covers exposes a deployment_id on the wire, so this id
				// is bound via -bind deployment_gap_entity_id=... (this
				// file's own restOperatorSuppliedProducers, below) rather
				// than any request's Produces. Both planes answer the
				// identical literal body regardless of which specific
				// frame went missing -- validate_flame_frames/
				// validateFlameFrames both raise the same "Flame frames
				// have gaps" 422 -- so BodyMode is JSON, not status_only,
				// and no Parity is declared: there is no data-dependent
				// field left to diverge on. A row bound to this same
				// producer but carrying a genuinely non-terminal status
				// (deploymentStatusIsRunning true) would instead answer
				// candidate 200 / baseline 422, a real declared divergence
				// -- see this file's own "Branches this corpus cannot
				// provably reach" doc comment above for why no live entry
				// asserts that case today.
				Name:                "deployment_gap_entity_id_bound_422",
				Query:               url.Values{"entity_type": {"deployment"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode:   RESTBodyModeJSON,
				IDBindings: []RESTIDBinding{{Producer: "deployment_gap_entity_id", QueryParam: "entity_id"}},
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
				// Captured live: internal/queryapi/server/testdata/people_422/
				// get_non_numeric_limit.json.
				Name:                "invalid_limit",
				Query:               url.Values{"limit": {"not-a-number"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// Captured live: internal/queryapi/server/testdata/people_422/
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
				Parity:     peopleSummaryParity,
				IDBindings: []RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
			},
			{
				// compare_days non-numeric -- this route's own second
				// int-typed query param, the same int_parsing 422 shape
				// invalid_range_days above already pins for range_days.
				Name:                "invalid_compare_days",
				Query:               url.Values{"compare_days": {"not-a-number"}},
				WantCandidateStatus: 422, WantBaselineStatus: 422,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// _reject_comparative_params (main.py:1081) -- bound to a
				// real person_id so the 400 is proven on the same request
				// shape summary_default's own 200 uses, not just against
				// the literal "{person_id}" text every other entry here
				// resolves to.
				Name:                "forbidden_param",
				Query:               url.Values{"rank": {"1"}},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode:   RESTBodyModeJSON,
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
				// peopleMetricChurnParity: churn's own sum(loc_touched)
				// shape, integer.
				Name:                "metric_default",
				Query:               url.Values{"metric": {"churn"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     peopleMetricChurnParity,
				IDBindings: []RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
			},
			{
				// metric=cycle_time: personMetricConfig's own table for
				// this metric is work_item_user_metrics_daily (metric.go's
				// series read), with ByWorkType AND ByStage breakdowns both
				// on work_item_cycle_times (metricconfig.go) -- neither
				// breakdown statement runs under any other live entry in
				// this table. peopleMetricCycleTimeParity: avg() over
				// Nullable(Float64) columns throughout, float.
				Name:                "metric_cycle_time",
				Query:               url.Values{"metric": {"cycle_time"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     peopleMetricCycleTimeParity,
				IDBindings: []RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
			},
			{
				// metric=review_latency: table user_metrics_daily, ByRepo
				// breakdown with its own "INNER JOIN repos FINAL"
				// (metricconfig.go) -- a different join than churn's own
				// ByRepo (different table/column), so this is a distinct
				// statement from metric_default's. peopleMetricReviewLatencyParity:
				// avg() over a Nullable(Float64) column, float.
				Name:                "metric_review_latency",
				Query:               url.Values{"metric": {"review_latency"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     peopleMetricReviewLatencyParity,
				IDBindings: []RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
			},
			{
				// metric=throughput: table work_item_user_metrics_daily
				// (different column than cycle_time's own read of the same
				// table), ByWorkType breakdown on work_item_cycle_times.
				// peopleMetricThroughputParity: sum() over integer-domain
				// count columns throughout, integer.
				Name:                "metric_throughput",
				Query:               url.Values{"metric": {"throughput"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     peopleMetricThroughputParity,
				IDBindings: []RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
			},
			{
				// metric=wip_overlap: table work_item_user_metrics_daily,
				// NO breakdown config at all (metricconfig.go) -- the only
				// live entry in this table whose metric reaches
				// BuildMetricResponse with cfg.ByRepo/ByWorkType/ByStage
				// all nil, so breakdowns stays the static empty-lists value
				// (metric.go) rather than running a breakdown statement.
				// peopleMetricWipOverlapParity declares data.timeseries.value
				// only, float (avg()), for exactly that reason.
				Name:                "metric_wip_overlap",
				Query:               url.Values{"metric": {"wip_overlap"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     peopleMetricWipOverlapParity,
				IDBindings: []RESTIDBinding{{Producer: "person_id", PathParam: "person_id"}},
			},
			{
				// metric=blocked_work: table work_item_cycle_times (the
				// only metric whose SERIES read, not just its breakdown,
				// targets this table), ByWorkType breakdown on the same
				// table. peopleMetricBlockedWorkParity: sum() over a plain
				// 0/1 count expression throughout, integer.
				Name:                "metric_blocked_work",
				Query:               url.Values{"metric": {"blocked_work"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:   RESTBodyModeJSON,
				Parity:     peopleMetricBlockedWorkParity,
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
				// live person_id producer, via bounded candidate iteration
				// (restidbind.go's own RESTIDBinding.Candidates doc
				// comment): the run loop tries each search result, in
				// order, against this request until one candidate's own
				// compare against personDrilldownPRsParity is genuinely
				// admitted -- not vacuously refused -- or 10 candidates are
				// exhausted. A candidate whose pull requests are empty on
				// both planes is refused as vacuous_empty_legs
				// (structural.go's own vacuousEmptyLegs, armed by
				// personDrilldownPRsParity's own declared BaselineDefects)
				// and the loop tries the next candidate; a candidate with
				// real pull requests on either plane is admitted, whether
				// its own compare matches or finds a genuine divergence --
				// iteration never substitutes a later, fully-matching
				// candidate for an earlier one that already surfaced real
				// data. The winning candidate is exposed as prs_person_id,
				// consumed by every sibling entry below instead of the raw
				// person_id producer, so they all share the SAME selected
				// person.
				Name:                "drilldown_prs_default",
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:      RESTBodyModeJSON,
				Parity:        personDrilldownPRsParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
				IDBindings: []RESTIDBinding{
					{Producer: "person_id", PathParam: "person_id", Candidates: 10, ExposeAs: "prs_person_id"},
				},
				// Produces prs_person_created_at from this response's own
				// FIRST returned item's created_at (ExtractRESTID only
				// ever returns the first non-empty match, restidbind.go's
				// own doc comment) -- a real value the cursor query param
				// actually accepts, consumed by this operation's own
				// valid_cursor entry below.
				Produces: []RESTIDProducer{{Name: "prs_person_created_at", ListPath: "items", IDField: "created_at"}},
			},
			{
				// cursor bound to drilldown_prs_default's own WINNING
				// candidate's first item's created_at above -- reaches
				// fetchPersonPullRequestsQuery's own cursor filter branch
				// (drilldownprs.go's own fetchPersonPullRequests) live.
				// person_id is bound to prs_person_id, not the raw
				// person_id producer, so this request is scoped to the
				// SAME person drilldown_prs_default selected -- the one
				// with real pull requests, when one exists among the
				// bounded candidates.
				Name:                "valid_cursor",
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:      RESTBodyModeJSON,
				Parity:        personDrilldownPRsParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
				IDBindings: []RESTIDBinding{
					{Producer: "prs_person_id", PathParam: "person_id"},
					{Producer: "prs_person_created_at", QueryParam: "cursor"},
				},
			},
			{
				// limit above boundedDrilldownLimit's own 200 ceiling
				// (drilldownprs.go's own maxDrilldownLimit) -- clamps
				// rather than 422s. TestBoundedDrilldownLimitClamp
				// (drilldownprs_test.go) pins the clamp function itself;
				// this proves the route actually applies it live, against
				// a real person with pull requests. person_id is bound to
				// prs_person_id (the same selected person as
				// drilldown_prs_default), not the raw person_id producer.
				Name:                "limit_above_ceiling",
				Query:               url.Values{"limit": {"500"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:      RESTBodyModeJSON,
				Parity:        parityWithPageCutLimit(personDrilldownPRsParity, personDrilldownPRsCeilingLimit),
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
				IDBindings: []RESTIDBinding{{Producer: "prs_person_id", PathParam: "person_id"}},
			},
			{
				// limit=0 -- boundedDrilldownLimit's own <=0 branch falls
				// back to 50, the same "absent and zero share one
				// fallback" contract that function's own doc comment
				// states. person_id is bound to prs_person_id (the same
				// selected person as drilldown_prs_default), not the raw
				// person_id producer.
				Name:                "limit_zero_falls_back_to_default",
				Query:               url.Values{"limit": {"0"}},
				WantCandidateStatus: 200, WantBaselineStatus: 200,
				BodyMode:      RESTBodyModeJSON,
				Parity:        personDrilldownPRsParity,
				DedupListPath: drilldownPRsDedup.ListPath, DedupKeyFields: drilldownPRsDedup.KeyFields,
				IDBindings: []RESTIDBinding{{Producer: "prs_person_id", PathParam: "person_id"}},
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
				// Same shared int_parsing validator drilldown/prs's own
				// GET already exercises, for this route's OWN limit query
				// param (people_drilldown_issues_route.go's own
				// intQueryParamError call, distinct from range_days above).
				Name:                "invalid_limit",
				Query:               url.Values{"limit": {"not-a-number"}},
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
				// _reject_comparative_params (main.py) runs BEFORE the
				// try/except that produces this route's own 503 -- real and
				// comparable on both planes even though every success path
				// on this route is baseline-only 503. Same mechanism/body
				// shape GET /api/v1/people's own comparative_param_rejected
				// entry already exercises (people_route.go's own
				// writeRESTError doc comment; peopleForbiddenQueryParams is
				// this route's own copy of the identical set).
				Name:                "comparative_param_rejected",
				Query:               url.Values{"compare_to": {"1"}},
				WantCandidateStatus: 400, WantBaselineStatus: 400,
				BodyMode: RESTBodyModeJSON,
			},
			{
				// Same live person_id binding as drilldown/prs' own
				// "drilldown_prs_default" entry above, with one
				// difference: this binding is bounded-candidate (Candidates
				// > 0, restidbind.go's own RESTIDBinding.Candidates doc
				// comment). GET /api/v1/people's own person_id producer
				// yields every non-empty person_id in search-result order,
				// not only the first; the run loop tries each, in order,
				// against THIS request until one candidate's own Produces
				// below (provider, person_issue_completed_at) actually
				// resolve -- i.e. until it finds a person who has issues --
				// or 10 candidates are exhausted. The winning candidate is
				// exposed as issues_person_id (ExposeAs below), consumed by
				// every sibling entry in this operation instead of the raw
				// person_id producer, so they all share the SAME selected
				// person, not independently the first search result. The
				// baseline plane answers HTTP 503 for this request in
				// production; the candidate answers HTTP 200 with real data
				// (see StatusDivergenceReason below). A production run
				// against a search result set that contains at least one
				// person with issue activity within the bounded candidate
				// count now admits this request AND every sibling entry
				// below (valid_cursor, limit_above_ceiling,
				// limit_zero_falls_back_to_default) instead of refusing all
				// four by name; a search result set with no such person
				// among the bounded candidates still refuses this request,
				// now by RESTRefusalCandidateIterationExhausted, and every
				// sibling still cascades by rest_request_id_binding_unresolved.
				Name:                "drilldown_issues_default",
				WantCandidateStatus: 200, WantBaselineStatus: 503,
				StatusDivergenceReason: "CHAOS-5868: the baseline plane answers HTTP 503 for this request in production; the candidate plane answers HTTP 200 with real data; bodies are not compared.",
				BodyMode:               RESTBodyModeStatusOnly,
				IDBindings: []RESTIDBinding{
					{Producer: "person_id", PathParam: "person_id", Candidates: 10, ExposeAs: "issues_person_id"},
				},
				// Produces person_issue_completed_at, the WINNING
				// candidate's own FIRST returned issue's completed_at
				// (ExtractRESTID only ever returns the first non-empty
				// match, restidbind.go's own doc comment) -- the real
				// `next_cursor` shape a caller would actually send back
				// (parseISODateTimeQueryParam's own doc comment: "always
				// the next_cursor this same route's own previous response
				// emitted"), consumed by this operation's own valid_cursor
				// entry below. Since WantCandidateStatus is 200 with
				// WantBaselineStatus 503, this also doubles as this
				// entry's own structural evidence: a StatusOnly+Produces
				// request extracts from the CANDIDATE leg's decoded body
				// (proveOneRESTRequest, cmd/go-api-rest-prove/main.go). A
				// candidate whose body does not yield BOTH declared ids
				// loses (the run loop tries the next candidate); every
				// candidate failing exhausts the bound and refuses THIS
				// request by name (RESTRefusalCandidateIterationExhausted).
				// A downstream sibling bound to issues_person_id refuses
				// separately, by name (rest_request_id_binding_unresolved)
				// -- flame/aggregated's own provider producer lives on GET
				// /api/v1/drilldown/issues' org-wide default_window entry
				// instead (see that entry's own doc comment), so it does
				// not depend on this route's own person selection.
				Produces: []RESTIDProducer{
					{Name: "provider", ListPath: "items", IDField: "provider"},
					{Name: "person_issue_completed_at", ListPath: "items", IDField: "completed_at"},
				},
			},
			{
				// cursor bound to drilldown_issues_default's own WINNING
				// candidate's first item's completed_at above -- reaches
				// fetchPersonIssuesQuery's own cursor_filter branch
				// (drilldownissues.go:138-141) live. A cursor at the FIRST
				// item's own completed_at (the only item ExtractRESTID can
				// ever supply) still returns every older issue behind it,
				// so this is not vacuous. Same baseline-only 503 as
				// drilldown_issues_default above. person_id is bound to
				// issues_person_id, not the raw person_id producer, so
				// this request is scoped to the SAME person
				// drilldown_issues_default selected -- the one with issue
				// activity, when one exists among the bounded candidates.
				Name:                "valid_cursor",
				WantCandidateStatus: 200, WantBaselineStatus: 503,
				StatusDivergenceReason: "CHAOS-5868: the baseline plane answers HTTP 503 for this request in production; the candidate plane answers HTTP 200 with real data; bodies are not compared.",
				BodyMode:               RESTBodyModeStatusOnly,
				IDBindings: []RESTIDBinding{
					{Producer: "issues_person_id", PathParam: "person_id"},
					{Producer: "person_issue_completed_at", QueryParam: "cursor"},
				},
				Produces: []RESTIDProducer{{Name: "issues_valid_cursor_status", ListPath: "items", IDField: "status"}},
			},
			{
				// limit above boundedDrilldownLimit's own 200 ceiling
				// (drilldownprs.go:37-55, shared with this route) -- clamps
				// rather than 422s. TestBoundedDrilldownLimitClamp
				// (drilldownprs_test.go) pins the clamp function itself;
				// this proves the route actually applies it live, against a
				// real person_id. Same baseline-only 503 as
				// drilldown_issues_default above. person_id is bound to
				// issues_person_id (the same selected person as that
				// entry), not the raw person_id producer.
				Name:                "limit_above_ceiling",
				Query:               url.Values{"limit": {"500"}},
				WantCandidateStatus: 200, WantBaselineStatus: 503,
				StatusDivergenceReason: "CHAOS-5868: the baseline plane answers HTTP 503 for this request in production; the candidate plane answers HTTP 200 with real data; bodies are not compared.",
				BodyMode:               RESTBodyModeStatusOnly,
				IDBindings:             []RESTIDBinding{{Producer: "issues_person_id", PathParam: "person_id"}},
				Produces:               []RESTIDProducer{{Name: "issues_limit_above_ceiling_work_item_id", ListPath: "items", IDField: "work_item_id"}},
			},
			{
				// limit=0 -- boundedDrilldownLimit's own <=0 branch falls
				// back to 50, the same "absent and zero share one fallback"
				// contract that function's own doc comment states. Same
				// baseline-only 503 as drilldown_issues_default above.
				// person_id is bound to issues_person_id (the same
				// selected person as that entry), not the raw person_id
				// producer.
				Name:                "limit_zero_falls_back_to_default",
				Query:               url.Values{"limit": {"0"}},
				WantCandidateStatus: 200, WantBaselineStatus: 503,
				StatusDivergenceReason: "CHAOS-5868: the baseline plane answers HTTP 503 for this request in production; the candidate plane answers HTTP 200 with real data; bodies are not compared.",
				BodyMode:               RESTBodyModeStatusOnly,
				IDBindings:             []RESTIDBinding{{Producer: "issues_person_id", PathParam: "person_id"}},
				Produces:               []RESTIDProducer{{Name: "issues_limit_zero_provider", ListPath: "items", IDField: "provider"}},
			},
		},
	},
	// GET+POST /api/v1/home -- corpus content lives in home_corpus.go
	// (this package); these two lines are the shared-map registration
	// only.
	"REST:GET:/api/v1/home":  homeGetEndpointSpec,
	"REST:POST:/api/v1/home": homePostEndpointSpec,
	// GET+POST /api/v1/work-units -- corpus content lives in
	// workunits_corpus.go (this package); these two lines are the
	// shared-map registration only.
	"REST:GET:/api/v1/work-units":  workUnitsGetEndpointSpec,
	"REST:POST:/api/v1/work-units": workUnitsPostEndpointSpec,
	// POST /api/v1/work-units/{work_unit_id}/explain -- corpus content lives
	// in workunitexplain_corpus.go.
	"REST:POST:/api/v1/work-units/{work_unit_id}/explain": workUnitExplainPostEndpointSpec,
	"REST:GET:/api/v1/opportunities":                      opportunitiesGetEndpointSpec,
	"REST:POST:/api/v1/opportunities":                     opportunitiesPostEndpointSpec,
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
func KnownRESTPaths() []string { return KnownRESTPathsFor(RESTServiceQueryAPI) }

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
	if err := validateRESTServices(); err != nil {
		return err
	}
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
			if req.BodyMode != RESTBodyModeJSON && req.BodyMode != RESTBodyModeStatusOnly && req.BodyMode != RESTBodyModeCandidateShape {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q has an unrecognised BodyMode %q", operation, req.Name, req.BodyMode)
			}
			if req.CandidateShapeArray && req.BodyMode != RESTBodyModeCandidateShape {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q sets CandidateShapeArray but BodyMode is %q, not RESTBodyModeCandidateShape", operation, req.Name, req.BodyMode)
			}
			if err := validateBaselineDefects(req.Parity.BaselineDefects); err != nil {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q: %w", operation, req.Name, err)
			}
			if err := validateNumericLeaves(req.Parity); err != nil {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q: %w", operation, req.Name, err)
			}
			if req.Timeout < 0 {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q declares a negative Timeout", operation, req.Name)
			}
			if req.BaselineTimeoutDeclared != nil {
				if err := req.BaselineTimeoutDeclared.Validate(req); err != nil {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q: %w", operation, req.Name, err)
				}
			}
			if (req.DedupListPath == "") != (len(req.DedupKeyFields) == 0) {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q sets DedupListPath and DedupKeyFields inconsistently -- both or neither", operation, req.Name)
			}
			if len(req.Produces) > 0 && req.BodyMode != RESTBodyModeJSON {
				// The one exception: a request whose baseline is declared
				// failing (Wants differ, and the CANDIDATE's own want is
				// 200) may still Produce ids, extracted from the
				// candidate leg's decoded body instead of the baseline's
				// -- an id is a request parameter, not evidence compared
				// between planes, so it is honest to read it from
				// whichever leg actually answers with a body (see
				// proveOneRESTRequest's own doc comment in cmd/go-api-
				// rest-prove/main.go). Every other StatusOnly+Produces
				// combination stays refused: there is no body to read an
				// id from at all.
				isBaselineOnlyFailure := diverges && req.WantCandidateStatus == 200
				if !isBaselineOnlyFailure {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q declares Produces with BodyMode %q -- an id can only be extracted from a decoded JSON body", operation, req.Name, req.BodyMode)
				}
			}
			for _, prod := range req.Produces {
				if prod.Name == "" {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q declares a Produces entry with no Name", operation, req.Name)
				}
			}
			iteratingBindings := 0
			for _, binding := range req.IDBindings {
				if binding.Producer == "" {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q declares an IDBinding with no Producer", operation, req.Name)
				}
				setCount := 0
				for _, set := range []bool{binding.QueryParam != "", binding.PathParam != "", binding.BodyPath != ""} {
					if set {
						setCount++
					}
				}
				if setCount != 1 {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q binds id %q with QueryParam=%q PathParam=%q BodyPath=%q -- exactly one of QueryParam/PathParam/BodyPath must be set", operation, req.Name, binding.Producer, binding.QueryParam, binding.PathParam, binding.BodyPath)
				}
				if binding.BodyPath != "" && req.Body == nil {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q binds id %q to BodyPath %q, but this request carries no Body", operation, req.Name, binding.Producer, binding.BodyPath)
				}
				if binding.Candidates < 0 {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q binds id %q with a negative Candidates", operation, req.Name, binding.Producer)
				}
				if (binding.Candidates > 0) != (binding.ExposeAs != "") {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q binds id %q with Candidates=%d ExposeAs=%q -- both or neither must be set", operation, req.Name, binding.Producer, binding.Candidates, binding.ExposeAs)
				}
				if binding.Candidates > 0 {
					iteratingBindings++
				}
			}
			if iteratingBindings > 1 {
				return fmt.Errorf("goapiproof: REST corpus entry %q request %q declares %d bounded-candidate IDBindings -- at most one is supported per request", operation, req.Name, iteratingBindings)
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
	"REST:GET:/api/v1/flame/aggregated",
	"REST:POST:/api/v1/investment/explain",
	"REST:POST:/api/v1/investment/flow",
	"REST:POST:/api/v1/investment/flow/repo-team",
	"REST:GET:/api/v1/investment",
	"REST:POST:/api/v1/investment",
	"REST:GET:/api/v1/investment/sunburst",
	"REST:GET:/api/v1/meta",
	"REST:GET:/api/v1/quadrant",
	"REST:GET:/api/v1/heatmap",
	"REST:GET:/api/v1/sankey",
	"REST:POST:/api/v1/sankey",
	"REST:GET:/api/v1/home",
	"REST:POST:/api/v1/home",
	"REST:GET:/api/v1/work-units",
	"REST:POST:/api/v1/work-units",
	// After both work-units entries: its GET default_window is what
	// produces the live work_unit_id the bound entries here consume.
	"REST:POST:/api/v1/work-units/{work_unit_id}/explain",
	"REST:GET:/api/v1/opportunities",
	"REST:POST:/api/v1/opportunities",
}

// RESTRunOrder returns a fresh copy of restRunOrder -- cmd/go-api-rest-
// prove's own request-loop order (see restRunOrder's own doc comment for
// why this differs from KnownRESTOperations' alphabetical order).
func RESTRunOrder() []string {
	out := make([]string, len(restRunOrder))
	copy(out, restRunOrder)
	return out
}

// restOperatorSuppliedProducers names every id this corpus consumes via
// an operator-supplied -bind NAME=VALUE flag on cmd/go-api-rest-prove
// (that binary's own flags.binds, resolved into the SAME produced map an
// earlier request's own Produces declaration would populate) rather than
// an earlier request's Produces. flame's own "deployment" entity_type is
// the first case: no route this corpus covers exposes a deployment_id on
// the wire (this file's own doc comment on the flame corpus entry, above),
// so deployment_entity_id_bound_200's own IDBindings names a producer no
// request here Produces at all. deployment_gap_entity_id_bound_422 (same
// doc comment) is the second: a distinct deployment_id, chosen to hit
// buildDeploymentFlameResponse's own gap branch rather than its happy
// path, so it needs its own producer name -- binding both entries to the
// same name would make one request's -bind value the other's, which is
// never true for two entries declaring different WantCandidateStatus.
// validateIDBindingOrder accepts a binding.Producer that resolves in
// EITHER this set or an earlier request's own Produces, and separately
// REQUIRES every name declared here to be consumed by at least one
// request's own IDBindings -- an entry naming an id nothing binds is a
// dead allowlist entry, refused the same as any other corpus
// inconsistency. A run whose invocation omits the matching -bind refuses
// that one request by name (RESTRefusalIDBindingUnresolved, restidbind.go),
// the same as any other unproduced id -- never an invented value, and no
// production literal ever appears in this file. pr_gap_id and
// issue_gap_id are the third and fourth cases, the same shape as
// deployment_gap_entity_id: unlike pr_entity_id_bound_200/
// issue_entity_id_bound_200 (which consume the regular pr_id/work_item_id
// a normal GET /api/v1/drilldown/prs'/`/issues`' own default_window entry
// Produces), pr_gap_entity_id_bound_status_divergence and
// issue_gap_entity_id_bound_status_divergence (flame corpus entry, above)
// each need a DELIBERATELY chosen row known to carry a terminal status
// with no real end timestamp, not whatever a fresh window happens to
// return -- so each needs its own producer name.
var restOperatorSuppliedProducers = map[string]bool{
	"deployment_entity_id":     true,
	"deployment_gap_entity_id": true,
	"pr_gap_id":                true,
	"issue_gap_id":             true,
}

// IsOperatorSuppliedIDProducer reports whether name is declared in
// restOperatorSuppliedProducers -- cmd/go-api-rest-prove's own run loop
// uses this to name the flag (-bind NAME=...) in an unresolved-producer
// refusal's own Detail, rather than the generic "no earlier request"
// wording that describes only the OTHER kind of producer.
func IsOperatorSuppliedIDProducer(name string) bool {
	return restOperatorSuppliedProducers[name]
}

// ValidateRESTIDBindingOrder checks restRunOrder against restEndpointSpecs
// and every declared Produces/IDBindings pair against
// restOperatorSuppliedProducers -- see validateIDBindingOrder's own doc
// comment for the checks themselves. Run by TestRESTCorpusIsValid via
// ValidateRESTCorpus, which calls this directly.
func ValidateRESTIDBindingOrder() error {
	return validateIDBindingOrder(restRunOrder, restEndpointSpecs, restOperatorSuppliedProducers)
}

// validateIDBindingOrder is ValidateRESTIDBindingOrder's own logic,
// parameterised so a test can exercise it against a small fixture corpus
// and a small operator-supplied set instead of this package's full, real
// ones. runOrder must be exactly a permutation of specs' own keys, no id
// name may be Produced twice, and every IDBindings.Producer must either
// already have been Produced by a request strictly earlier in runOrder
// (or earlier in the same operation's own Requests slice) or be named in
// operatorSupplied -- a consumer placed ahead of its producer, or naming
// neither, is refused here, at startup, rather than discovered as an
// always-refused request in a live run. Every name operatorSupplied
// itself declares must be consumed by at least one request's own
// IDBindings, checked last -- the allowlist cannot carry a dead name no
// request ever binds.
func validateIDBindingOrder(runOrder []string, specs map[string]RESTEndpointSpec, operatorSupplied map[string]bool) error {
	if len(runOrder) != len(specs) {
		return fmt.Errorf("goapiproof: restRunOrder has %d entries, restEndpointSpecs has %d -- every corpus operation must appear in the run order exactly once", len(runOrder), len(specs))
	}
	seenOps := make(map[string]bool, len(runOrder))
	for _, op := range runOrder {
		if seenOps[op] {
			return fmt.Errorf("goapiproof: restRunOrder lists operation %q twice", op)
		}
		seenOps[op] = true
		if _, ok := specs[op]; !ok {
			return fmt.Errorf("goapiproof: restRunOrder names operation %q, which restEndpointSpecs does not declare", op)
		}
	}
	for op := range specs {
		if !seenOps[op] {
			return fmt.Errorf("goapiproof: restEndpointSpecs declares operation %q, which restRunOrder never lists", op)
		}
	}

	produced := map[string]string{} // producer Name -> "operation/request" that declares it
	operatorSuppliedUsed := make(map[string]bool, len(operatorSupplied))
	for _, op := range runOrder {
		spec := specs[op]
		for _, req := range spec.Requests {
			for _, binding := range req.IDBindings {
				if operatorSupplied[binding.Producer] {
					operatorSuppliedUsed[binding.Producer] = true
				} else if _, ok := produced[binding.Producer]; !ok {
					return fmt.Errorf("goapiproof: REST corpus entry %q request %q binds id %q, which no earlier request in restRunOrder Produces and restOperatorSuppliedProducers does not declare -- a consumer must run strictly after its producer, or the id must be operator-supplied", op, req.Name, binding.Producer)
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
			// A bounded-candidate binding's own ExposeAs makes the
			// WINNING candidate itself available to a later request's
			// own IDBindings, exactly like an ordinary Produces entry --
			// registered here, at THIS request's position, so a consumer
			// of it is held to the identical "strictly after" ordering
			// rule the check above already enforces for Producer.
			for _, binding := range req.IDBindings {
				if binding.ExposeAs == "" {
					continue
				}
				if existing, dup := produced[binding.ExposeAs]; dup {
					return fmt.Errorf("goapiproof: id %q is Produced by both %s and %s/%s -- give it one producer", binding.ExposeAs, existing, op, req.Name)
				}
				produced[binding.ExposeAs] = op + "/" + req.Name
			}
		}
	}
	for name := range operatorSupplied {
		if !operatorSuppliedUsed[name] {
			return fmt.Errorf("goapiproof: restOperatorSuppliedProducers declares %q, which no corpus request consumes via IDBindings -- remove it", name)
		}
	}
	return nil
}
