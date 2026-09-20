package goapiproof

import (
	"fmt"
	"sort"
)

// Window is the request window every windowed operation is asked for.
//
// It is a PARAMETER, not a constant, and it is folded into every
// receipt's request_identity, because a proof run is evidence for one
// request -- "the same operation over a different window" is a different
// request and must not silently reuse another run's verdict. The defaults
// below reproduce the window a live measurement used
// (lane-goapi-enable's go_api_request_payloads.py), so a re-run of that
// measurement is reproducible rather than "whatever today happens to be".
//
// Deliberately NOT derived from time.Now(): a rolling window makes two
// runs incomparable and makes a mismatch impossible to reproduce. The
// cost is that the defaults age; an operator moves them with the flags.
type Window struct {
	SinceUTC  string // RFC3339, e.g. "2026-06-01T00:00:00Z"
	UntilUTC  string // RFC3339
	SinceDate string // "YYYY-MM-DD"
	UntilDate string // "YYYY-MM-DD"
	WeekStart string // "YYYY-MM-DD"
}

// DefaultWindow reproduces that measurement's window.
func DefaultWindow() Window {
	return Window{
		SinceUTC:  "2026-06-01T00:00:00Z",
		UntilUTC:  "2026-09-01T00:00:00Z",
		SinceDate: "2026-06-01",
		UntilDate: "2026-09-01",
		WeekStart: "2026-09-01",
	}
}

// Validate refuses an incomplete window rather than sending an operation
// a zero-valued date and reading the resulting validation rejection as a
// parity finding.
func (w Window) Validate() error {
	for name, value := range map[string]string{
		"since-utc": w.SinceUTC, "until-utc": w.UntilUTC,
		"since-date": w.SinceDate, "until-date": w.UntilDate, "week-start": w.WeekStart,
	} {
		if value == "" {
			return fmt.Errorf("goapiproof: window field %s is empty", name)
		}
	}
	return nil
}

// OperationSpec is one registered operation's committed request shape and
// its declared parity configuration.
type OperationSpec struct {
	// Variables builds the GraphQL `variables` object for this operation.
	//
	// Ported from lane-goapi-enable's go_api_request_payloads.py, which
	// carries the lesson these shapes encode: an input object's required
	// fields are NOT interchangeable with the operation's top-level
	// variables. An earlier mechanical build put `orgId` inside
	// `CapacityForecastInput` (which has no such field); every one of
	// those requests failed validation, fell back to Python, and looked
	// exactly like a routing failure. Check the SDL, never infer.
	Variables func(orgID string, window Window) map[string]any

	// ResponseRoot is the GraphQL field the operation's REGISTERED
	// DOCUMENT selects at the top of `data` -- which is not always the
	// operation name. flowMatrix, investmentBreakdown and investmentFull
	// all select `analytics`, because query_route.go's operation keys are
	// its own Mux/PostgresSwitch keys, chosen to disambiguate several
	// registered documents that share a root field.
	//
	// It exists so every declared parity path can be checked against the
	// subtree it will actually be compared against. Assigning declarations
	// by operation NAME produced paths that matched nothing -- which fails
	// the run correctly, but for a reason nobody would have understood
	// from the failure.
	ResponseRoot string

	// RootNullable mirrors the SDL: whether ResponseRoot is declared
	// WITHOUT a trailing `!`. capacityForecast, throughputForecast and
	// pr are; TestResponseRootNullabilityMatchesTheSDL derives the truth
	// from contracts/graphql/v1/schema.graphql and fails if this drifts.
	RootNullable bool

	// InstanceVariable names a document variable that identifies ONE
	// STORED ROW -- `pr`'s `$id` is the only one today -- and that this
	// table therefore cannot supply.
	//
	// Empty for every operation whose request is fully determined by
	// the org and the window. When set, proveOne refuses the operation
	// by name rather than sending an invented value. The refusal, not
	// the omission, is the point: leaving `pr` OUT of this table would
	// refuse the whole RUN via AssertCoverage (which is what blocked
	// JOB 5), while inventing an id would satisfy the SDL, return null
	// on both planes and report a match having measured nothing.
	InstanceVariable string

	// Parity is this operation's declared comparator configuration.
	Parity Options

	// Variants are ADDITIONAL requests proven under this SAME operation
	// name, document and routing row -- for an operation whose committed
	// input selects between materially different server code paths, where
	// the single (Variables, Parity) pair above cannot cover more than one
	// of them.
	//
	// CHAOS-5426 is why this exists:
	// flowMatrix's own Variables sends dimension=WORK_TYPE, the one
	// dimension #2374 left untouched, so no proof row built from Variables
	// alone could ever measure the code path #2374 changed (TEAM/REPO +
	// useInvestment=true). A citation declared against the base request
	// only ever covers what the base request exercises -- adding a second
	// dimension to Variables would have meant picking ONE of WORK_TYPE or
	// TEAM/REPO and losing proof coverage (and the pinned CHAOS-5448
	// regression protection) for the other. Variants keeps both: each
	// entry gets its own Outcome, its own request_identity (RequestIdentity
	// digests the actual variables sent, not the operation name -- see
	// identity.go), and its own receipt, so a citation on one variant can
	// never leak into another's terminal state.
	//
	// The base (Variables, Parity) pair is never itself a Variant -- it has
	// no Name and is always proven first, unconditionally, exactly as
	// before this field existed. Every other operation today declares zero
	// Variants and its behavior is byte-for-byte unchanged.
	Variants []Variant
}

// Variant is one additional (Variables, Parity) pair an OperationSpec
// proves alongside its base request. See OperationSpec.Variants.
type Variant struct {
	// Name distinguishes this variant on the Outcome and in operator
	// output (e.g. "TEAM", "REPO"). Never empty -- SpecFor's
	// TestEveryVariantIsNamed pins this, because an empty Name is
	// indistinguishable from the base request in a report line.
	Name string

	// Variables builds this variant's GraphQL variables. Same contract as
	// OperationSpec.Variables.
	Variables func(orgID string, w Window) map[string]any

	// Parity is this variant's OWN declared comparator configuration --
	// deliberately separate from the base request's Parity, since a
	// variant proves a different code path and a citation that covers the
	// base request's divergence has no reason to cover the variant's (or
	// vice versa).
	Parity Options

	// Instance, when set, marks a variant whose request needs one REAL
	// identifier from the run (a repository that has alerts, a search term
	// that matches an alert, ...). The run supplies it as
	// `-instance-id <operation>.<variant>=<value>`; without one the variant
	// is REFUSED by name, never sent with a guessed value.
	Instance *VariantInstance
}

// VariantInstance describes the run-supplied identifier a Variant needs.
type VariantInstance struct {
	// Kind says in words what KIND of value the run must supply, so the
	// operator can pick it with a read-only query. It never carries a value.
	Kind string
	// Bind writes the supplied value into the variant's variables.
	Bind func(variables map[string]any, value string)
	// EchoFor declares where the answer must show the supplied value: it
	// returns the ScopeEcho requirements for one supplied value.
	EchoFor func(value string) []ScopeEcho
}

// Echo returns the scope-echo requirements for one supplied value.
func (i *VariantInstance) Echo(value string) []ScopeEcho {
	if i == nil || i.EchoFor == nil {
		return nil
	}
	return i.EchoFor(value)
}

// kindFor returns the words describing the identifier a missing key needs.
func (i *VariantInstance) kindFor(missingKey string) string {
	if i == nil || missingKey == "" {
		return ""
	}
	return i.Kind
}

// InstanceKey is the -instance-id key naming one variant's identifier.
func InstanceKey(operation, variant string) string { return operation + "." + variant }

// volatileReason documents one excluded field. Kept as a named constant
// so the reason travels with every operation that cites it rather than
// being retyped (and drifting) per entry.
const volatileForecastIdentity = "freshly generated per request: an identical request produced Go forecastId=33fb9f32... / Python 78296c67... with computedAt ~350ms apart (CHAOS-5425, 2026-09-07 live measurement). Excluded per CHAOS-4381; scoped to the forecast operations only, and NOT a licence to exclude any other field."

// operationSpecs is the committed per-operation table.
//
// It is a table, not a default plus overrides: an operation missing from
// here is a REFUSAL (see SpecFor), never a request built from a guessed
// shape. A guessed shape fails validation, falls back to Python, and
// produces a receipt that looks like a routing defect.
//
// Three declaration kinds live in each entry's Parity, and all three obey
// the same rule -- a declaration that matches nothing FAILS the run:
//
//   - VolatileFields: values regenerated per request (forecastId,
//     computedAt). Populated below from a live measurement.
//   - FloatTierB: leaves whose value derives from a ClickHouse FLOATING-POINT
//     aggregate, per CHAOS-5451. Populated below from lane-goapi-parity's
//     read of the actual SQL, with the source line for each.
//   - BaselineDefects: differences where PYTHON is wrong and Go is right
//     (CHAOS-5447/5448/5449), each citing its ticket, evidence path and the
//     field subtree it covers.
//
// Which operation a declaration belongs to is decided by the operation's
// REGISTERED DOCUMENT, not by its name: several operations select a
// differently-named root field (investmentBreakdown and investmentFull both
// select `analytics`; flowMatrix does too), and evidenceQualityStats is in
// investmentBreakdown's document but NOT investmentFull's. Assigning by
// name would have produced entries that match nothing -- which fails the
// run, correctly, but for a reason nobody would have understood.
var operationSpecs = map[string]OperationSpec{
	// The three AI rollup operations read the same ai_impact_metrics_daily
	// rows and share one request shape: the org, a day range and an optional
	// scope. Their variants cover every scope branch a document can send:
	// work type, attribution buckets, a repository that resolves to nothing
	// (as a uuid and as a name), a team that owns nothing, and the pair.
	// aiReviewLoad's document also selects aiComparison, so the comparison is
	// proven inside it as well as on its own document.
	"aiImpactSummary": {
		ResponseRoot: "aiImpactSummary",
		Variables:    aiRollupVariables(nil),
		Parity:       aiImpactSummaryParity(),
		Variants: append(aiRollupVariants(aiImpactSummaryParity()),
			aiInstanceVariant("REPO_VALID", "a repository id that has rollup rows", "repoId", aiImpactSummaryParity(), "data.aiImpactSummary.daily", "data.aiImpactSummary.repoBreakdown", "scopeId"),
			aiInstanceVariant("REPO_NAME_VALID", "the full name of a repository that has rollup rows", "repoId", aiImpactSummaryParity(), "data.aiImpactSummary.daily", "", ""),
			aiInstanceVariant("TEAM_VALID", "a team id stored on rollup rows", "teamId", aiImpactSummaryParity(), "data.aiImpactSummary.daily", "data.aiImpactSummary.teamBreakdown", "scopeId"),
		),
	},
	"aiComparison": {
		ResponseRoot: "aiComparison",
		Variables:    aiRollupVariables(nil),
		Variants: append(aiRollupVariants(Options{}),
			aiInstanceVariant("REPO_VALID", "a repository id that has rollup rows", "repoId", Options{}, "", "", ""),
			aiInstanceVariant("REPO_NAME_VALID", "the full name of a repository that has rollup rows", "repoId", Options{}, "", "", ""),
			aiInstanceVariant("TEAM_VALID", "a team id stored on rollup rows", "teamId", Options{}, "", "", ""),
		),
	},
	"aiReviewLoad": {
		ResponseRoot: "aiReviewLoad",
		Variables:    aiRollupVariables(nil),
		Parity:       aiReviewLoadParity(),
		Variants: append(aiRollupVariants(aiReviewLoadParity()),
			aiInstanceVariant("REPO_VALID", "a repository id that has rollup rows", "repoId", aiReviewLoadParity(), "data.aiReviewLoad.byBucket", "", ""),
			aiInstanceVariant("REPO_NAME_VALID", "the full name of a repository that has rollup rows", "repoId", aiReviewLoadParity(), "data.aiReviewLoad.byBucket", "", ""),
			aiInstanceVariant("TEAM_VALID", "a team id that has rollup rows and whose repo patterns select a repository", "teamId", aiReviewLoadParity(), "data.aiReviewLoad.byBucket", "", ""),
		),
	},
	"compoundingRisk": {
		ResponseRoot: "compoundingRisk",
		Variables: func(orgID string, _ Window) map[string]any {
			return compoundingRiskVariables(orgID, nil)
		},
		Parity: compoundingRiskParity,
		// A team breakout whose team has NO stored team rows is deliberately
		// not a variant: that branch derives team points from the repo rows
		// through team ownership on Go and through teams.repo_patterns on
		// Python, so the two answers differ by design and no computed check
		// admits it; it is a refused case, not a blanket citation.
		Variants: []Variant{
			compoundingRiskVariant("REPO_BREAKOUT", map[string]any{"breakout": "REPO", "trendDays": 30}),
			compoundingRiskVariant("TEAM_BREAKOUT", map[string]any{"breakout": "TEAM", "trendDays": 30}),
			compoundingRiskDayVariant("DAY", "REPO"),
			compoundingRiskDayVariant("TEAM_DAY", "TEAM"),
			compoundingRiskVariant("REPO_IDS_UNKNOWN", map[string]any{"breakout": "REPO", "repoIds": []any{"00000000-0000-0000-0000-000000000001"}, "trendDays": 30}),
			compoundingRiskVariant("REPO_IDS_EMPTY", map[string]any{"breakout": "REPO", "repoIds": []any{}, "trendDays": 30}),
			compoundingRiskVariant("TEAM_IDS_UNKNOWN", map[string]any{"breakout": "TEAM", "teamIds": []any{"team-abc-123"}, "trendDays": 30}),
			compoundingRiskVariant("TEAM_IDS_EMPTY", map[string]any{"breakout": "TEAM", "teamIds": []any{}, "trendDays": 30}),
			compoundingRiskInstanceVariant("REPO_VALID", "a repository id that has stored compounding-risk rows", "REPO", "repoIds"),
			compoundingRiskInstanceVariant("TEAM_STORED", "a team id that has stored team-scope compounding-risk rows", "TEAM", "teamIds"),
			compoundingRiskVariant("TEAM_AND_REPO_UNKNOWN", map[string]any{"breakout": "TEAM", "teamIds": []any{"team-abc-123"}, "repoIds": []any{"00000000-0000-0000-0000-000000000001"}, "trendDays": 30}),
			compoundingRiskVariant("TREND_ONE_DAY", map[string]any{"breakout": "REPO", "trendDays": 1}),
			compoundingRiskVariant("TREND_CLAMPED_HIGH", map[string]any{"breakout": "REPO", "trendDays": 1000}),
			compoundingRiskVariant("TREND_CLAMPED_LOW", map[string]any{"breakout": "REPO", "trendDays": 0}),
		},
	},
	"busFactor": {
		ResponseRoot: "busFactor",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "scope": nil}
		},
		// A team that OWNS repositories is deliberately not a variant: Go
		// selects the team's repositories by ownership (team_repo_ownership)
		// and Python by member authorship, so the two answers differ by
		// design and no computed check admits that difference; it is a
		// refused case, not a blanket citation.
		// Every other variant compares exactly. The team variants name a
		// team that owns nothing and that no member has authored for, so
		// both planes answer an empty window; a team that owns repositories
		// is not a corpus case because the two planes select its
		// repositories differently (ownership on Go, member authorship on
		// Python).
		Variants: []Variant{
			busFactorVariant("REPO_UNKNOWN", map[string]any{"repoId": "00000000-0000-0000-0000-000000000001"}),
			busFactorVariant("REPO_MALFORMED", map[string]any{"repoId": "not-a-uuid"}),
			busFactorVariant("TEAM_UNKNOWN", map[string]any{"teamId": "team-abc-123"}),
			busFactorVariant("TEAM_BLANK", map[string]any{"teamId": ""}),
			busFactorInstanceVariant("REPO_VALID", "a repository id that has commit stats", "repoId", "data.busFactor.repos"),
			busFactorVariant("REPO_AND_TEAM_UNKNOWN", map[string]any{"repoId": "00000000-0000-0000-0000-000000000001", "teamId": "team-abc-123"}),
		},
	},
	// experiments derives its items from the opportunity cards, which are
	// built from the home read; the scope level and ids are the only request
	// inputs that reach it (the window is fixed). The base request is the org
	// scope. Every other scope level is a variant naming an id that matches
	// nothing, so both planes answer the same single "steady flow" card and
	// the comparison is non-empty for each level.
	"experiments": {
		ResponseRoot: "experiments",
		Parity:       Options{RequireNonEmpty: []string{"data.experiments.items"}},
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "filters": nil}
		},
		Variants: []Variant{
			experimentsVariant("ORG_EXPLICIT", "ORG", nil),
			experimentsVariant("TEAM_UNKNOWN", "TEAM", []string{"team-abc-123"}),
			experimentsVariant("REPO_UNKNOWN", "REPO", []string{"00000000-0000-0000-0000-000000000001"}),
			experimentsVariant("SERVICE_UNKNOWN", "SERVICE", []string{"service-abc-123"}),
			experimentsVariant("DEVELOPER_UNKNOWN", "DEVELOPER", []string{"dev-abc-123"}),
		},
	},
	// The two product telemetry dashboards take a half-open day range only; the
	// org dashboard reads the caller's own org (its hash), the platform
	// dashboard every org and needs a superuser credential, without which both
	// planes deny and the prover refuses the errored leg. The base request is
	// the measurement window; the variant is the empty range (start equals
	// end), which both planes answer with empty lists and an all-null summary.
	"productTelemetryDashboard": {
		ResponseRoot: "productTelemetryDashboard",
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"orgId": orgID, "input": map[string]any{"startDate": w.SinceDate, "endDate": w.UntilDate}}
		},
		Variants: []Variant{{
			Name: "EMPTY_RANGE",
			Variables: func(orgID string, w Window) map[string]any {
				return map[string]any{"orgId": orgID, "input": map[string]any{"startDate": w.SinceDate, "endDate": w.SinceDate}}
			},
		}},
	},
	"productTelemetryPlatformDashboard": {
		ResponseRoot: "productTelemetryPlatformDashboard",
		Variables: func(_ string, w Window) map[string]any {
			return map[string]any{"input": map[string]any{"startDate": w.SinceDate, "endDate": w.UntilDate}}
		},
		Variants: []Variant{{
			Name: "EMPTY_RANGE",
			Variables: func(_ string, w Window) map[string]any {
				return map[string]any{"input": map[string]any{"startDate": w.SinceDate, "endDate": w.SinceDate}}
			},
		}},
	},
	"capacityForecast": {
		ResponseRoot: "capacityForecast",
		RootNullable: true,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "input": map[string]any{}}
		},
		Parity: Options{
			VolatileFields: map[string]string{
				"data.capacityForecast.forecastId": volatileForecastIdentity,
				"data.capacityForecast.computedAt": volatileForecastIdentity,
			},
			StochasticLeaves: capacityForecastStochasticLeaves,
		},
	},
	// capacityForecasts (the LIST) declares no Tier-B and no volatile
	// fields: its resolver reads stored columns back rather than
	// recomputing, so forecastId/computedAt are not per-request values and
	// throughputMean/throughputStddev are not engine aggregates
	// (lane-goapi-parity, CHAOS-5451 read of the resolver). An earlier
	// draft copied the SINGULAR operation's volatile pair onto it; those
	// entries would have matched nothing -- the list nests under
	// edges.node -- and failed every run as stale.
	"capacityForecasts": {
		ResponseRoot: "capacityForecasts",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "filters": map[string]any{}}
		},
		Parity: Options{BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-5450",
			Reason: "Python's LIST path stringifies the driver's NAIVE datetime (resolvers/capacity.py:24) and emits a space-separated timestamp with no offset, while its own SINGULAR path (capacity.py:50) stringifies a tz-aware one and does emit an offset -- Python is internally inconsistent with itself and stays frozen. Note the state this describes: as of this entry NEITHER plane emits the RFC 3339 form the schema documents (schema.graphql:704-705 says isoformat; :430/:2239 type the field String!, so nothing enforced it), and Go's own capacity resolvers are being fixed to emit it under R55 -- so this covers a real divergence today and continues to after that fix, but it must not be read as a claim that Go is already RFC 3339. Evidence: /var/lib/oci-cache/lane-scratch/lane-goapi-parity/5450/analysis.txt",
			Paths:  []string{"data.capacityForecasts.edges.node.computedAt"},
		}}},
	},
	// Both catalog documents select the `catalog` root field. The web
	// client's dimension picker sends any DimensionInput, so the base
	// request asks for TEAM (the roster read) and every other dimension is a
	// variant: each one compiles different SQL (repos slug read, investment
	// event sources). AUTHOR is a rejection on both planes -- an error body,
	// not data -- so it has no comparable response and is covered by the
	// resolver's own tests.
	// The document declares no `filters` variable, so a filtered catalog
	// cannot be sent through a registered document and is covered by the
	// resolver's own tests instead.
	"catalogValues": {
		ResponseRoot: "catalog",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "dimension": "TEAM"}
		},
		Variants: catalogDimensionVariants("REPO", "THEME", "WORK_TYPE", "SUBCATEGORY"),
	},
	// The repository-scope read fixes REPO in the document text, so its
	// only variable is the org.
	"acrRepositoryScopes": {
		ResponseRoot: "catalog",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID}
		},
	},
	// The four documents that select `dataHealth`. All send the team as a
	// document variable except the lineage document, which fixes team ALL and
	// sends the metric. The identity read is the only one the team changes
	// (it narrows which identities count as mapped), so its base request asks
	// for team ALL and a variant asks for the empty team, which takes the
	// unscoped branch. The lineage document reads one metric's source tables,
	// and the metrics of one table share every code path, so the base request
	// asks for one metric per table and a variant asks for a metric no table
	// holds.
	"connectorsDataHealth": {
		ResponseRoot: "dataHealth",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"teamId": "ALL"}
		},
		Parity: Options{BaselineDefects: []BaselineDefect{{
			Ticket:             "CHAOS-6090",
			Reason:             "sync_configurations.last_sync_at and job_runs.completed_at/started_at are Postgres timestamptz; Python's resolver hands the driver's tz-aware datetimes to strawberry's DateTime scalar, which isoformat()s them with a \"+00:00\" offset, while Go's gqlgen DateTime scalar formats the same instant as RFC 3339 with \"Z\" (resolvers/data_health.py resolve_connectors, models/data_health.py ConnectorStatus/ConnectorFailure). The instants are equal; only the wire text differs. Go's form is the canonical DateTime wire form (graphqldate.RFC3339UTC's doc comment); the Python form is the declared defect and stays frozen.",
			Paths:              []string{"data.dataHealth.connectors.lastSyncAt", "data.dataHealth.connectors.lastFailure.occurredAt"},
			Intermittent:       true,
			IntermittentReason: "the two leaves exist only for a connector that has synced or failed; an org without one has no timestamp to differ",
		}}},
	},
	"dataHealthIdentity": {
		ResponseRoot: "dataHealth",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"team": "ALL"}
		},
		Variants: []Variant{{
			Name: "EMPTY_TEAM",
			Variables: func(orgID string, _ Window) map[string]any {
				return map[string]any{"team": ""}
			},
		}},
	},
	"mappingCoverageHealth": {
		ResponseRoot: "dataHealth",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"teamId": "ALL"}
		},
	},
	"metricLineage": {
		ResponseRoot: "dataHealth",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"metricId": "throughput"}
		},
		Parity: metricLineageParity(),

		Variants: metricLineageVariants("review_load", "after_hours_ratio", "investment_mix", "no_such_metric"),
	},
	"cognitiveLoad": {
		ResponseRoot: "cognitiveLoad",
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"input": map[string]any{
				"orgId": orgID, "sinceDate": w.SinceDate, "untilDate": w.UntilDate,
				"teamId": nil, "repoId": nil,
			}}
		},
	},
	"complexityTimeseries": {
		ResponseRoot: "complexityTimeseries",
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"input": map[string]any{
				"orgId": orgID, "sinceUtc": w.SinceUTC, "untilUtc": w.UntilUTC,
				"granularity": "DAY", "scope": "REPO",
				"repoIds": nil, "teamIds": nil, "limit": 500,
			}}
		},
	},
	// CHAOS-5523 registered this operation; the table did not gain an
	// entry with it, and AssertCoverage correctly refused the whole run
	// (JOB 5). Its registered document declares
	// `$orgId: String!` and `$limit: Int!` as REQUIRED and `$flagKey` /
	// `$environment` as nullable, so all four are sent explicitly:
	// omitting a nullable variable and sending it as null are the same
	// thing to the resolver here (both arrive as a nil *string), and
	// stating them keeps the request shape readable beside the document.
	//
	// limit=1000 is the SDL's own default for the argument
	// (`limit: Int! = 1000`), which is what a client that omits it gets --
	// the document requires the variable, so a value must be chosen, and
	// the SDL's default is the only non-arbitrary one available.
	"featureFlagEvents": {
		ResponseRoot: "featureFlagEvents",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{
				"orgId": orgID, "flagKey": nil, "environment": nil,
				"limit": 1000,
			}
		},
	},
	"featureFlags": {
		ResponseRoot: "featureFlags",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{
				"orgId": orgID, "provider": nil, "project": nil,
				"includeArchived": false, "limit": 100,
			}
		},
	},
	"flowMatrix": {
		ResponseRoot: "analytics",
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"orgId": orgID, "batch": map[string]any{
				"flowMatrix": map[string]any{
					"dimension": "WORK_TYPE", "measure": "COUNT",
					"dateRange": map[string]any{"startDate": w.SinceDate, "endDate": w.UntilDate},
					"maxNodes":  50, "maxEdges": 200,
				},
			}}
		},
		Parity: Options{BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-5448",
			Reason: "Python omits FINAL on work_item_cycle_times (templates.py:304/:397) where Go has it (flowmatrix.go:732/:802), so Python counts superseded ReplacingMergeTree row versions and its answer converges onto Go's only after a background merge. Go is correct. Evidence: /var/lib/oci-cache/lane-scratch/lane-goapi-parity/5448/repro.txt",
			Paths:  []string{"data.analytics.flowMatrix.nodes.value", "data.analytics.flowMatrix.edges.value"},
		}}},
		// CHAOS-5426: the base request
		// above sends dimension=WORK_TYPE, the one dimension CHAOS-5426's
		// fix (#2374, flowmatrix.go:199-227) left untouched -- so it can
		// never prove that fix. TEAM and REPO with useInvestment=true are
		// the code path #2374 changed (matching web's useChordFlow.ts,
		// which always sends useInvestment:true) and previously had ZERO
		// executed proof coverage. Added as Variants, not a repointed base
		// request, so the CHAOS-5448 WORK_TYPE regression coverage above is
		// not lost.
		Variants: []Variant{
			flowMatrixInvestmentVariant("TEAM"),
			flowMatrixInvestmentVariant("REPO"),
		},
	},
	// hotspots.riskScore is deliberately NOT Tier B, and the reasoning is
	// worth keeping: it is a STORED Float64 column in file_hotspot_daily
	// (migration 007:48) that both planes read via argMax, so no float
	// arithmetic happens and CHAOS-5451's engine nondeterminism does not
	// apply. The measured divergence is ~1e-2 relative -- seven orders of
	// magnitude above a 1e-9 tolerance -- so Tier B would not have excused
	// it anyway; it is a real defect, not last-bit noise
	// (lane-goapi-parity, correcting the field list it was handed).
	"hotspots": {
		ResponseRoot: "hotspots",
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"input": map[string]any{
				"orgId": orgID, "sinceUtc": w.SinceUTC, "untilUtc": w.UntilUTC,
				"repoIds": nil, "teamIds": nil, "limit": 50,
			}}
		},
		Parity: Options{BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-5447",
			Reason: "Python argMaxes on computed_at ALONE where Go keys on (day, computed_at), so it can select a different physical row for the same file. Two distinct halves, only one of which is a tie: (a) INVERSION -- an older day recomputed later carries a newer computed_at and wins outright, no tie involved; (b) TIE -- on an identical computed_at Python falls back to ClickHouse's internal row order, measured at 31,072 of 100,000 seeded files resolving to the OLDER day against 0 of 100,000 for Go. Live shape: 17 of the 48 files present on both sides disagreed on churn in BOTH directions, and 22 of those 48 had identical churn/blame/cyclomatic but a different riskScore -- both-directions is the tell that this is row SELECTION, not arithmetic. Go is correct. Covers the whole rows subtree because a different row differs in every field, not only riskScore. Evidence: /var/lib/oci-cache/lane-scratch/lane-goapi-parity/5447/repro.txt",
			Paths:  []string{"data.hotspots.rows"},
		}}},
	},
	// Both select the `analytics` root field, but NOT the same subtree:
	// evidenceQualityStats is in investmentBreakdown's registered document
	// and absent from investmentFull's, so declaring it on both would leave
	// a permanently-stale entry on one of them.
	"investmentBreakdown": {
		ResponseRoot: "analytics",
		Variables:    investmentVariables,
		Parity: Options{FloatTierB: map[string]string{
			"data.analytics.evidenceQualityStats.mean":   "avgIf(evidence_quality) -- ClickHouse float aggregate, order-nondeterministic (investmentquality.go:250, CHAOS-5451)",
			"data.analytics.evidenceQualityStats.stddev": "stddevPopIf(evidence_quality) -- ClickHouse float aggregate; 20 identical runs over 5M rows gave 9 distinct values (investmentquality.go:251, CHAOS-5451)",
			"data.analytics.breakdowns.items.value":      "on the investment path MeasureCount compiles to SUM(subcategory_kv.2), a FLOAT sum (validate.go:245-246). Derived from CHAOS-5451's rule rather than an observed divergence, and this table's payload always sets useInvestment=true; on the non-investment path the same measure is an exact integer sum",
		}},
	},
	// sankey.coverage.teamCoverage/.repoCoverage: CORRECTED, not new --
	// this table used to say these were count()/countIf() integers on the
	// committed WORK_TYPE payload and would only become float sums if a
	// future document added a REPO dimension to the sankey path. That was
	// wrong about the gate: resolveSankeyCoverage's totalExpr/repoTotalExpr
	// and assignedTeamCountExpr/assignedRepoCountExpr switch to
	// sum()/sumIf(repoEffortCol, ...) whenever coverageUseInvestment is
	// true (sankeycoverage.go:210-213), unconditionally on whether REPO is
	// in the path -- useRepoAllocation is a different flag that gates
	// breakdowns/sankey VALUE compilation (validate.go's dbExpression),
	// not coverage. investmentFullVariables sets batch-level
	// useInvestment=true, and resolve.go:428 reads coverage's flag
	// straight from that (the raw three-state flag, not auto-routed), so
	// this document's coverage has always compiled to the float-sum form.
	// A deployed-executed run (JOB 5, build e4a9fabff) measured this
	// directly: repoCoverage came back 1 ULP off
	// (0.9931181145418517 vs 0.9931181145418516) with the vacuity guard
	// not firing. Rule-derived per CHAOS-5451, not just the one observed
	// field -- teamCoverage shares the same sum()/sumIf() expressions and
	// is declared alongside it rather than left to fail only on a future
	// run where the summation order happens to differ.
	//
	// Coverage under a WORK-CATEGORY FILTER is a separate question from
	// the declaration above, and is knowingly left undeclared. CHAOS-5498
	// found that `ARRAY JOIN subcategory_kv` re-weights each unit by its
	// surviving subcategory count, in BOTH the numerator and the
	// denominator, so a filter moves the headline on identical data
	// (measured 0.5 -> 0.333). Today it is not a parity finding at all:
	// the defect is pre-existing in Go (sankeycoverage.go) AND in the
	// Python original (resolvers/analytics.py:833), both wrong the same
	// way, so the planes AGREE and a baseline-defect declaration here
	// would match nothing and fail every run as stale. After 5498's Go-side
	// fix lands they will diverge -- but only under that filter, and no
	// registered document sends one (investmentVariables below sets no
	// filters at all), so the path stays unmeasured either way. Declare it
	// only if a registered document starts sending a work-category filter.
	"investmentFull": {
		ResponseRoot: "analytics",
		Variables:    investmentFullVariables,
		Parity: Options{
			FloatTierB: map[string]string{
				"data.analytics.breakdowns.items.value":       "on the investment path MeasureCount compiles to SUM(subcategory_kv.2), a FLOAT sum (validate.go:245-246). Rule-derived, not observed (CHAOS-5451)",
				"data.analytics.sankey.nodes.value":           "CompileSankey calls the same dbExpression as breakdowns, so on the investment path this is the same SUM(subcategory_kv.2) float sum; subcategory_kv ARRAY JOINs a Map(String, Float64) column (investment.go:514, migration 017:12). Rule-derived, not observed (CHAOS-5451)",
				"data.analytics.sankey.edges.value":           "same float sum as sankey.nodes.value -- one dbExpression, one compiled measure (validate.go:245-246, investment.go:514). Rule-derived, not observed (CHAOS-5451)",
				"data.analytics.sankey.coverage.teamCoverage": "ratio of float sums: assignedTeam/total, both sum()/sumIf(repoEffortCol, ...) once coverageUseInvestment is true, which this document's committed useInvestment=true payload always sets (sankeycoverage.go:210-212,490). Rule-derived, not observed (CHAOS-5451)",
				"data.analytics.sankey.coverage.repoCoverage": "ratio of float sums: assignedRepo/repoTotal, both sum()/sumIf(repoEffortCol, ...) once coverageUseInvestment is true, same gate as teamCoverage (sankeycoverage.go:210-211,213,493). Measured live at build e4a9fabff (JOB 5, run 5167b7de-18da-4747-addf-16c65a2858c8): 0.9931181145418517 vs 0.9931181145418516, 1 ULP, relative 1.1e-16 (CHAOS-5451)",
			},
			// Intermittent because the fan-out lasts only from a repos write
			// until the next background merge; a merged-state run shows no
			// difference here and must not refuse as stale.
			BaselineDefects: []BaselineDefect{{
				Ticket: "CHAOS-4773",
				Reason: "Python joins repos (ReplacingMergeTree(org_id, id)) without FINAL in the sankey compiler (graphql/sql/compiler.py) and in the coverage query (resolvers/analytics.py). Any repo row with an unmerged physical version multiplies every sankey node and edge value for units in that repo, and inflates repo-assigned coverage, by the version count. Measured on prod at query-api build 16bfb582: three repos read exactly 2x on Python, the TEAM, THEME and REPO totals each differ by exactly that duplicated effort, and breakdowns (no repos join) match. Go reads repos FINAL (investment.go, sankeycoverage.go). Go is correct.",
				Paths: []string{
					"data.analytics.sankey.nodes.value",
					"data.analytics.sankey.edges.value",
					"data.analytics.sankey.coverage.teamCoverage",
					"data.analytics.sankey.coverage.repoCoverage",
				},
				Intermittent:       true,
				IntermittentReason: "repos is a ReplacingMergeTree rewritten every sync cycle and merged in the background, so the fan-out is present only between a repos write and the next merge; the same operation matched on an earlier build while the rows were merged",
				// The blanket path citation above admits ANY difference
				// under these four paths, including a real Go regression
				// -- see RepoFanoutShape's doc comment (repofanout.go)
				// for the incident this narrows down to exactly the
				// transform the repos-join fan-out can produce. Paths is
				// unchanged; this only tightens what counts as covered
				// under it.
				RepoFanoutShape: &RepoFanoutShape{
					NodesListPath:    "data.analytics.sankey.nodes",
					EdgesListPath:    "data.analytics.sankey.edges",
					NodeValuePath:    "data.analytics.sankey.nodes.value",
					EdgeValuePath:    "data.analytics.sankey.edges.value",
					TeamCoveragePath: "data.analytics.sankey.coverage.teamCoverage",
					RepoCoveragePath: "data.analytics.sankey.coverage.repoCoverage",
				},
			}, {
				Ticket: "CHAOS-4547",
				Reason: "Python's coverage query (resolvers/analytics.py) reads investment rows through api/queries/investment.py's LATEST_WORK_UNIT_INVESTMENTS_CTE, which dedups ReplacingMergeTree versions with a plain argMax(col, computed_at) over four Nullable columns (repo_id among them); argMax skips a row whose col is NULL when picking the newest version, so once a work unit's newest generation clears repo_id or its resolved team vote, Python keeps a stale non-null value from an older generation instead of the true latest one. Go's LatestWorkUnitInvestmentsSource and buildUnitTeamSubquery (investment.go) tuple-wrap the same reads -- (argMax(tuple(col), computed_at)).1 -- and correctly report the true latest generation, including a NULL. CHAOS-4759 ruled Go's reads are the ones kept, with Python left unfixed under this repo's standing no-Python-graphql-work policy; investmentargmaxtransitionguard.go is the guard that detects, per org, the moment a work unit's generation history stops agreeing across the two planes, independent of any code deploy -- the two coverage ratios move only by the units whose newest generation transitioned since the guard's baseline snapshot. Measured on prod at query-api build 72ffdb5d: repoCoverage 0.8924689846789948 (Go) vs 0.8906755621478929 (Python), teamCoverage 0.8527558256115781 vs 0.8503000595320582, identical across two proves a minute apart with no attribution write between them. Go is correct.",
				Paths: []string{
					"data.analytics.sankey.coverage.teamCoverage",
					"data.analytics.sankey.coverage.repoCoverage",
				},
				Intermittent:       true,
				IntermittentReason: "present only while some attributed row's newest generation carries NULL in a column an older generation filled; a later generation that restores the value, or a retraction that removes the row, makes both planes agree again",
				// The blanket path citation above admits ANY coverage
				// difference, including one the repos-join fan-out
				// (CHAOS-4773, above) already explains through its own
				// citation of these same two leaves -- see
				// CoverageShiftShape's own doc comment (coverageshift.go)
				// for the shape that narrows admission down to exactly
				// what a null-transition on this query can produce. Paths
				// is unchanged; this only tightens what counts as covered
				// under it.
				CoverageShiftShape: &CoverageShiftShape{
					NodesListPath:    "data.analytics.sankey.nodes",
					EdgesListPath:    "data.analytics.sankey.edges",
					TeamCoveragePath: "data.analytics.sankey.coverage.teamCoverage",
					RepoCoveragePath: "data.analytics.sankey.coverage.repoCoverage",
				},
			}, {
				Ticket: "CHAOS-5865",
				Reason: "LatestWorkUnitInvestmentsSource (investment.go) unconditionally excludes a work unit retired by a later regrouping run, via work_unit_supersessions -- every reader in this package composes that source, including this operation's own coverage aggregate (sankeycoverage.go), which reads the whole org population with no row limit. The reference plane's own coverage query has no knowledge of the work_unit_supersessions table anywhere in its source, so it keeps counting a retired work unit under whatever repo or team it carried before being superseded. A work unit is retired because an earlier grouping run got it wrong, so the rows the reference plane keeps and Go excludes skew toward unassigned relative to the rest of the population -- pulling the reference plane's ratio strictly below Go's candidate ratio on both leaves, never above. sankey.nodes and sankey.edges compose the same shared source but are top-N truncated by value (sankey.go), so the same population difference that moves the untruncated coverage aggregate ordinarily lands on tail nodes the truncated, compared page never reaches, which is why this mechanism surfaces in coverage alone. Go is correct.",
				Paths: []string{
					"data.analytics.sankey.coverage.teamCoverage",
					"data.analytics.sankey.coverage.repoCoverage",
				},
				Intermittent:       true,
				IntermittentReason: "present only while work_unit_supersessions holds at least one row for this run's org whose superseded_work_unit_id falls within investmentFull's requested window; an org or window with no such row leaves both planes reading the identical population and agreeing",
				// The blanket path citation above admits ANY coverage
				// difference, including one the repos-join fan-out or the
				// null-transition entry declared above already explains
				// -- see SupersessionSkewShape's own doc comment
				// (supersessionskew.go) for the shape that narrows
				// admission to exactly the DIRECTION this mechanism can
				// produce: the reference plane strictly below the
				// candidate on both leaves, nothing else differing
				// anywhere in the response. It checks direction, not
				// magnitude -- a regrouping run can retire any number of
				// work units, so nothing bounds how far this mechanism
				// moves either ratio, and it does not attempt to. Paths
				// is unchanged; this only tightens what counts as covered
				// under it.
				SupersessionSkewShape: &SupersessionSkewShape{
					TeamCoveragePath: "data.analytics.sankey.coverage.teamCoverage",
					RepoCoveragePath: "data.analytics.sankey.coverage.repoCoverage",
				},
			}},
			// CHAOS-5546: sankey.go's nodes/edges queries are a plain
			// ClickHouse UNION ALL with no outer ORDER BY. Measured live:
			// the SAME compiled SQL for this exact request, run 4 times
			// in a row against the deployed ClickHouse, came back in 3
			// DIFFERENT branch orderings (REPO/THEME/TEAM,
			// THEME/REPO/TEAM x2, TEAM/THEME/REPO) -- this is genuine
			// engine nondeterminism (parallel UNION ALL execution order
			// is unspecified without an ORDER BY), present on BOTH
			// planes, not a Go-vs-Python defect. A positional comparison
			// of these two lists therefore reports a coin flip as a
			// finding on every run, forever, regardless of which side is
			// "correct" -- there is no stable baseline order to match.
			// The producer fix (sankey.go's `dim_order` + outer
			// `ORDER BY dim_order ASC, value DESC, node_id ASC`) makes
			// the GO side deterministic and grouped by the request's own
			// `path` order for every real client going forward; this
			// declaration is the parity-rule-5 escape for the PROOF
			// comparison itself, which still faces a Python baseline with
			// no such ordering fix (no Python fixes, ever).
			OrderInsensitiveLists: []OrderInsensitiveList{
				{
					Path:      "data.analytics.sankey.nodes",
					KeyFields: []string{"id"},
					Reason:    "CompileSankey's nodes query is an unordered ClickHouse UNION ALL; the identical compiled SQL returned 3 different branch orderings across 4 live runs (CHAOS-5546)",
					Ticket:    "CHAOS-5546",
				},
				{
					Path:      "data.analytics.sankey.edges",
					KeyFields: []string{"source", "target"},
					Reason:    "edges share the same unordered-UNION shape as nodes, one query per path hop with no cross-hop ordering guarantee either (CHAOS-5546)",
					Ticket:    "CHAOS-5546",
				},
			},
		},
	},
	"operatingReview": {
		ResponseRoot: "operatingReview",
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"orgId": orgID, "input": map[string]any{
				"weekStart": w.WeekStart, "teamId": nil,
			}}
		},
		Parity: Options{FloatTierB: map[string]string{
			"data.operatingReview.sections.metrics.value":            "avg()/sum() over Float64 (operatingreview.go:413-418,483-485,562-568,641,695,736,780,822,880) -- ClickHouse float aggregate, order-nondeterministic (CHAOS-5451). This is why two identical requests seconds apart on the SAME plane disagreed",
			"data.operatingReview.sections.metrics.delta.value":      "derived from the same float aggregates as sections.metrics.value (CHAOS-5451)",
			"data.operatingReview.sections.metrics.delta.priorValue": "derived from the same float aggregates as sections.metrics.value (CHAOS-5451)",
			"data.operatingReview.sections.metrics.delta.absolute":   "derived from the same float aggregates as sections.metrics.value (CHAOS-5451)",
			"data.operatingReview.sections.metrics.delta.percent":    "derived from the same float aggregates as sections.metrics.value (CHAOS-5451)",
		}},
	},
	// CHAOS-4991 registered `pr`; like featureFlagEvents it arrived
	// without an entry here and AssertCoverage refused the run.
	//
	// Unlike every other operation in this table, `pr` cannot be built
	// from the org and the window alone: its document requires `$id: ID!`,
	// an identifier for ONE stored pull request. InstanceVariable says so,
	// and proveOne refuses the operation by name rather than sending an
	// invented value -- see RefusalNeedsInstanceID for why an invented one
	// is worse than no entry at all rather than better.
	//
	// The Variables func is still written out, and correctly, because it
	// is what a caller with a real id would need; `id` is left empty so
	// nothing can read this entry as a usable request. If `pr` is ever to
	// be proven, the id has to come from the run (a flag, or a row read
	// from the org's own data), not from this table.
	"pr": {
		ResponseRoot:     "pr",
		RootNullable:     true,
		InstanceVariable: "id",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "id": ""}
		},
		Parity: Options{BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-5780",
			Reason: "git_pull_requests' created_at/merged_at/closed_at/first_review_at/first_comment_at columns, plus git_pull_request_reviews.submitted_at (reviews) and git_commits.author_when (commits, joined in via work_graph_pr_commit) are ALL ClickHouse DateTime64(3, 'UTC') (000_raw_tables.sql:69-78,92,27) -- the same column class as CHAOS-5450's capacityForecast fields and CHAOS-5492's featureFlags fields. Python's clickhouse_connect driver returns every one of them NAIVE (no tzinfo) even though the column declares UTC (src/dev_health_ops/api/graphql/resolvers/pr.py, models/pr.py: submitted_at/author_when are plain `datetime`), so strawberry's DateTime scalar isoformat()s them with no offset; Go's ClickHouse driver attaches UTC location, and every one of these fields is typed DateTime in the schema (schema.graphql:1654-1663 for pr itself, PullRequestReview.submittedAt, PullRequestCommit.authorWhen), which gqlgen's built-in graphql.Time scalar formats via RFC3339Nano -- always an explicit offset. The forecast fields already established this pattern: RFC3339's explicit offset is the canonical DateTime wire form, and a naive Python isoformat is the declared defect, not a bug to chase (graphqldate.RFC3339UTC's doc comment). featureFlags.createdAt/archivedAt went the other way only because a live-ClickHouse precedent test had already pinned the naive shape as that field's own contract; no such precedent exists for pr's fields. Go is correct. Paths below are index-free (a list element's position is stripped before matching), so one citation per field covers every element of reviews/commits, not just the one a live run happened to see non-null first.",
			Paths: []string{
				"data.pr.createdAt",
				"data.pr.mergedAt",
				"data.pr.closedAt",
				"data.pr.firstReviewAt",
				"data.pr.firstCommentAt",
				"data.pr.reviews.submittedAt",
				"data.pr.commits.authorWhen",
			},
		}}},
	},
	"reviewEdges": {
		ResponseRoot: "reviewEdges",
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"input": map[string]any{
				"orgId": orgID, "sinceDate": w.SinceDate, "untilDate": w.UntilDate,
				"repoIds": nil, "limit": 500,
			}}
		},
	},
	"securityAlerts": {
		ResponseRoot: "securityAlerts",
		Variables: func(orgID string, _ Window) map[string]any {
			return securityAlertsVariables(orgID, nil, nil)
		},
		Parity: securityAlertsParity,
		Variants: []Variant{
			securityAlertsVariant("OPEN_ONLY", map[string]any{"openOnly": true}, nil),
			securityAlertsVariant("STATES", map[string]any{"states": []any{"FIXED", "DISMISSED"}}, nil),
			securityAlertsVariant("OPEN_ONLY_OVER_STATES", map[string]any{"openOnly": true, "states": []any{"FIXED"}}, nil),
			securityAlertsVariant("SEVERITIES", map[string]any{"severities": []any{"CRITICAL", "HIGH", "UNKNOWN"}}, nil),
			securityAlertsVariant("SOURCES", map[string]any{"sources": []any{"DEPENDABOT", "GITLAB_DEPENDENCY"}}, nil),
			securityAlertsVariant("REPO_IDS", map[string]any{"repoIds": []any{"00000000-0000-0000-0000-000000000001"}}, nil),
			securityAlertsVariant("SINCE_UNTIL", map[string]any{"since": "2026-06-01", "until": "2026-08-31"}, nil),
			securityAlertsVariant("SEARCH", map[string]any{"search": "ABC-123"}, nil),
			securityAlertsInstanceVariant("REPO_VALID", "a repository id that has alerts", "repoIds", true),
			securityAlertsInstanceVariant("SEARCH_VALID", "a search term that matches an alert's title, package or CVE", "search", false),
			securityAlertsVariant("PAGE_FIRST", nil, map[string]any{"first": 5}),
			securityAlertsSecondPageVariant(),
			securityAlertsVariant("PAGE_ZERO", nil, map[string]any{"first": 0}),
		},
	},
	"securityOverview": {
		ResponseRoot: "securityOverview",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "filters": nil}
		},
		Variants: []Variant{
			securityOverviewVariant("OPEN_ONLY", map[string]any{"openOnly": true}),
			securityOverviewVariant("STATES", map[string]any{"states": []any{"FIXED", "DISMISSED"}}),
			securityOverviewVariant("SEVERITIES", map[string]any{"severities": []any{"CRITICAL", "HIGH", "UNKNOWN"}}),
			securityOverviewVariant("SOURCES", map[string]any{"sources": []any{"DEPENDABOT", "GITLAB_DEPENDENCY"}}),
			securityOverviewVariant("REPO_IDS", map[string]any{"repoIds": []any{"00000000-0000-0000-0000-000000000001"}}),
			securityOverviewVariant("SINCE_UNTIL", map[string]any{"since": "2026-06-01", "until": "2026-08-31"}),
			securityOverviewVariant("SEARCH", map[string]any{"search": "ABC-123"}),
			securityOverviewRepoVariant("REPO_VALID", "a repository id that has open alerts"),
		},
	},
	// The overlay `threshold` fields, estimateCoverage.ratio and
	// rollingWindows.meanWeeklyThroughput are deliberately absent: the
	// thresholds are hardcoded constants (kernel.go:70), and the other two
	// are integer sums divided once in Go. All three are exact, and a
	// tolerance on an exact field excuses a real defect
	// (lane-goapi-parity, CHAOS-5451).
	"throughputForecast": {
		ResponseRoot: "throughputForecast",
		RootNullable: true,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "input": map[string]any{}}
		},
		Parity: Options{
			VolatileFields: map[string]string{
				"data.throughputForecast.forecastId": volatileForecastIdentity,
				"data.throughputForecast.computedAt": volatileForecastIdentity,
			},
			FloatTierB: map[string]string{
				"data.throughputForecast.reviewBottleneck.value": "avg(pr_first_review_p50_hours) -- ClickHouse float aggregate (throughputforecast/clickhouse.go:393, CHAOS-5451)",
				"data.throughputForecast.reviewBottleneck.score": "derived from reviewBottleneck.value (CHAOS-5451)",
				"data.throughputForecast.wipCongestion.value":    "avg(wip_count_end_of_day) -- ClickHouse float aggregate (clickhouse.go:169, CHAOS-5451)",
				"data.throughputForecast.wipCongestion.score":    "derived from wipCongestion.value (CHAOS-5451)",
				"data.throughputForecast.incidentLoad.value":     "sum(incidents_count)/weeks -- ClickHouse float aggregate (clickhouse.go:490, CHAOS-5451)",
				"data.throughputForecast.incidentLoad.score":     "derived from incidentLoad.value (CHAOS-5451)",
				"data.throughputForecast.primaryRisk.value":      "a copy of whichever overlay is primary, so it inherits that overlay's float aggregate (CHAOS-5451)",
				"data.throughputForecast.primaryRisk.score":      "a copy of whichever overlay is primary (CHAOS-5451)",
				"data.throughputForecast.staleWip.p50AgeHours":   "avg(wip_age_p50_hours) -- ClickHouse float aggregate (clickhouse.go:237, CHAOS-5451)",
				"data.throughputForecast.staleWip.p90AgeHours":   "avg(wip_age_p90_hours) -- ClickHouse float aggregate (clickhouse.go:238, CHAOS-5451)",
			},
		},
	},
	"workGraphArtifacts": {Variables: workGraphVariables, ResponseRoot: "workGraphArtifacts"},
	// The baseline plane reads work_graph_edges (a ReplacingMergeTree keyed
	// on the edge identity) without collapsing unmerged physical versions
	// of one logical edge, so its page can hold more than one row for the
	// same edgeId while the merge is pending; the candidate plane
	// collapses them before applying the page limit. Whether that
	// divergence is present in any one comparison depends on which
	// physical parts happen to be merged at request time, so the
	// declaration is Intermittent: absent while the source table holds no
	// unmerged duplicate, and expected once it does.
	//
	// WorkGraphEdgeDedupShape narrows the blanket "any leaf difference
	// under Paths is covered" rule to exactly that mechanism -- a
	// duplicated baseline row whose copies agree with each other and with
	// the candidate's own row for the same id -- rather than admitting
	// any difference under the edges list, which would also excuse a
	// genuine per-field regression sharing the same path. Admission is
	// PER EDGE ID: one edge's duplicate copies disagreeing, or its
	// shared content disagreeing with the candidate, excludes only that
	// edge, leaving its own findings outside the citation, named by id --
	// it does not widen or narrow any other edge's own verdict. pageInfo's
	// endCursor (TrailingCursorPath below) names whichever edge lands in
	// the page's last slot, so the same slot-count shift that explains
	// every reordered edge also determines it deterministically, and its
	// own admission is resolved through THAT edge's own id.
	"workGraphEdges": {
		ResponseRoot: "workGraphEdges",
		Variables:    workGraphVariables,
		Parity:       workGraphEdgesParity,
	},
	// releaseImpact is the release impact document the feature flag pages
	// send: the `workGraphEdges` root field with the filters `nodeId`,
	// `sourceType` and `limit`. It reads the same resolver as the
	// workGraphEdges document, so its declared baseline difference is the
	// same. The base request is the page's request for every release
	// (`nodeId` empty, `sourceType` RELEASE, `limit` 200); the variants prove
	// the branches a page can reach: one release by node id (a real id the
	// run supplies), and a page cut at one edge.
	"releaseImpact": {
		ResponseRoot: "workGraphEdges",
		Variables:    releaseImpactVariables,
		Parity:       workGraphEdgesParity,
		Variants: []Variant{
			releaseImpactVariant("LIMIT_ONE", map[string]any{"nodeId": "", "sourceType": "RELEASE", "limit": 1}),
			releaseImpactNodeVariant("NODE_ID_VALID", "a node id that is the source of at least one release edge"),
		},
	},
	"workGraphFlow": {Variables: workGraphVariables, ResponseRoot: "workGraphFlow"},
}

// capacityForecastStochasticLeaves is the one StochasticLeafClass in this
// table. Every capacityForecast leaf it does not name -- backlogSize,
// targetItems, targetDate, throughputMean, historyDays and the rest -- is
// still compared exactly.
//
// The items group is listed p95 first: the kernel takes the 50th, 15th and
// 5th percentiles of simulated items and stores them as p50/p85/p95 (for
// items the conservative answer is the LOW one), so p95Items <= p85Items <=
// p50Items. Days and dates run the other way.
var capacityForecastStochasticLeaves = &StochasticLeafClass{
	Ticket: "CHAOS-5901",
	Reason: "Mechanism: capacityForecast runs its Monte Carlo on every request with a fresh seed on both planes (the reference plane never seeds its generator; the Go resolver draws a seed from crypto/rand), so its percentile leaves are drawn values that differ between any two responses, including two from the same plane. Those leaves are not compared across planes; on each plane they are checked for type (date or integer), for order (p50 <= p85 <= p95 for days and dates, p95 <= p85 <= p50 for items), and each date for being the UTC date of computedAt plus its days leaf, and a null on one plane against a value on the other stays a finding. Scope: exactly the nine percentile leaves listed here; every other leaf of the operation is compared exactly. Blind spot: a plane drawing from the wrong distribution while keeping that order and those offsets passes; the distribution is pinned by the seeded golden fixtures tests/fixtures/capacity_forecast_golden.json (read by internal/jobs/metrics/numerical) and tests/fixtures/cpython_random_golden.json (read by internal/jobs/metrics/numerical/cpyrandom).",
	Orderings: []StochasticOrdering{
		{Type: StochasticTypeInteger, NonDecreasing: []string{
			"data.capacityForecast.p50Days",
			"data.capacityForecast.p85Days",
			"data.capacityForecast.p95Days",
		}},
		{Type: StochasticTypeDate, NonDecreasing: []string{
			"data.capacityForecast.p50Date",
			"data.capacityForecast.p85Date",
			"data.capacityForecast.p95Date",
		}},
		{Type: StochasticTypeInteger, NonDecreasing: []string{
			"data.capacityForecast.p95Items",
			"data.capacityForecast.p85Items",
			"data.capacityForecast.p50Items",
		}},
	},
	DateOffsets: []StochasticDateOffset{
		{DatePath: "data.capacityForecast.p50Date", DaysPath: "data.capacityForecast.p50Days"},
		{DatePath: "data.capacityForecast.p85Date", DaysPath: "data.capacityForecast.p85Days"},
		{DatePath: "data.capacityForecast.p95Date", DaysPath: "data.capacityForecast.p95Days"},
	},
	BaseTimestampPath: "data.capacityForecast.computedAt",
}

// flowMatrixInvestmentVariant builds the TEAM or REPO flowMatrix Variant,
// useInvestment=true, matching the CHAOS-5426 fix (#2374) and the shape
// web's useChordFlow.ts always sends. Both dimensions declare the SAME
// CHAOS-5426 BaselineDefect: chris ruled 2026-09-09 (ticket CHAOS-5426
// comment, ticket accepted the compose canary flip) that Go's output on
// this path is correct, and this is not a fresh data-semantics call --
// it restates an already-accepted ruling as a citation.
func flowMatrixInvestmentVariant(dimension string) Variant {
	return Variant{
		Name: dimension,
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"orgId": orgID, "batch": map[string]any{
				"flowMatrix": map[string]any{
					"dimension": dimension, "measure": "COUNT", "useInvestment": true,
					"dateRange": map[string]any{"startDate": w.SinceDate, "endDate": w.UntilDate},
					"maxNodes":  50, "maxEdges": 200,
				},
			}}
		},
		Parity: Options{BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-5426",
			Reason: "Python's compile_flow_matrix TEAM/REPO branch reads work_item_cycle_times unconditionally and ignores useInvestment (compiler.py:450-533, templates.py:187-446); Go honors useInvestment and reads latest_work_unit_investments/latest_work_unit_repo_effort per #2374 (flowmatrix.go:199-227). Python is frozen under R60 (GO-ONLY-GraphQL ruling) and not to be patched. Chris accepted Go's output as correct 2026-09-09 (ticket comment, compose canary flip to ffd9e5d5d..., re-verified byte-identical through the product edge). Evidence: .remember/lanes/lane-chord-fix/handoff-2026-09-09.md.",
			Paths:  []string{"data.analytics.flowMatrix.nodes.value", "data.analytics.flowMatrix.edges.value"},
		}}},
	}
}

// investmentVariables builds investmentBreakdown's request only.
//
// It used to be shared with investmentFull, which was the bug: this
// function's batch carries a `breakdowns` sub-request and nothing else, so
// on investmentFull -- whose registered document
// (query_route.go's registeredInvestmentFullDocument) also selects
// `analytics.sankey.nodes.value` / `.edges.value` -- `batch.Sankey` stayed
// nil, `resolveSankey` never ran (resolve.go:194), and both planes
// answered null. The first deployed-executed run correctly REFUSED the
// two declared FloatTierB sankey paths as vacuous rather than reporting a
// match having measured nothing (JOB 5). See
// investmentFullVariables below for investmentFull's own request.
//
// `useInvestment` is a field of AnalyticsRequestInput, NOT of
// BreakdownRequestInput (whose fields are exactly dimension, measure,
// dateRange, topN). Nesting it one level deeper failed validation and fell
// back to Python -- the same wrong-object mistake as putting `orgId`
// inside an input that has no such field.
func investmentVariables(orgID string, w Window) map[string]any {
	return map[string]any{"orgId": orgID, "batch": map[string]any{
		"breakdowns": []any{map[string]any{
			"dimension": "WORK_TYPE", "measure": "COUNT",
			"dateRange": map[string]any{"startDate": w.SinceDate, "endDate": w.UntilDate},
			"topN":      10,
		}},
		"useInvestment": true,
	}}
}

// investmentFullVariables builds investmentFull's request: the same
// `breakdowns` sub-request as investmentVariables above, PLUS a `sankey`
// sub-request -- the registered document selects both subtrees
// (registeredInvestmentFullDocument), so both must actually be asked for,
// or the unrequested one resolves to null on both planes and any
// declaration under it is vacuous by construction.
//
// path/measure/maxNodes/maxEdges mirror
// web/src/lib/graphql/investmentFetchers.ts's own default sankey batch
// (getInvestmentFlowViaGraphQL's team_category_repo mode -- the real
// client's default flow_mode): ["TEAM", "THEME", "REPO"], COUNT, 50/200.
// Three dimensions rather than the schema's >= 2 minimum
// (validateSankeyPath, sankey.go) because that is the shape a real
// request actually sends, not an arbitrary minimal one -- go-api-prove
// exists to measure the request traffic makes. `useInvestment: true` only
// at the batch level: resolveSankey reads `batch.UseInvestment`
// (resolve.go:195), not SankeyRequestInput's own optional field, so
// setting the batch-level flag is what actually selects the investment
// path CompileSankey's Tier-B declarations describe.
func investmentFullVariables(orgID string, w Window) map[string]any {
	dateRange := map[string]any{"startDate": w.SinceDate, "endDate": w.UntilDate}
	return map[string]any{"orgId": orgID, "batch": map[string]any{
		"breakdowns": []any{map[string]any{
			"dimension": "WORK_TYPE", "measure": "COUNT",
			"dateRange": dateRange,
			"topN":      10,
		}},
		"sankey": map[string]any{
			"path":      []any{"TEAM", "THEME", "REPO"},
			"measure":   "COUNT",
			"dateRange": dateRange,
			"maxNodes":  50,
			"maxEdges":  200,
		},
		"useInvestment": true,
	}}
}

// workGraphEdgesParity is the declared comparator configuration shared by
// every request that reads the workGraphEdges resolver.
var workGraphEdgesParity = Options{BaselineDefects: []BaselineDefect{{
	Ticket:             "CHAOS-5791",
	Reason:             "work_graph_edges is a ReplacingMergeTree keyed on the edge identity; the baseline plane reads it with no merge-time collapse, so an unmerged duplicate physical version of one logical edge surfaces as two content-identical rows sharing one edgeId, spending one extra slot of the page limit and shifting every later element's position, including which edge's id pageInfo.endCursor names. The candidate plane collapses duplicate versions before applying the page limit, so it carries no repeated edgeId. Candidate is correct.",
	Paths:              []string{"data.workGraphEdges.edges", "data.workGraphEdges.pageInfo.endCursor"},
	Intermittent:       true,
	IntermittentReason: "present only while the source table holds an unmerged duplicate physical version of some edge; a comparison taken after the background merge collapses it shows no repeated edgeId on the baseline side either",
	WorkGraphEdgeDedupShape: &WorkGraphEdgeDedupShape{
		EdgesListPath:      "data.workGraphEdges.edges",
		IDField:            "edgeId",
		TrailingCursorPath: "data.workGraphEdges.pageInfo.endCursor",
	},
}}}

func workGraphVariables(orgID string, _ Window) map[string]any {
	return map[string]any{"orgId": orgID, "filters": map[string]any{}}
}

// SpecFor returns the committed spec for an operation, or an error naming
// the operation. A missing spec is a REFUSAL with a named reason, never a
// skip: an operation the running binary registers but this table does not
// cover is exactly the gap a silent skip would hide.
func SpecFor(operation string) (OperationSpec, error) {
	spec, ok := operationSpecs[operation]
	if !ok {
		return OperationSpec{}, fmt.Errorf("goapiproof: no committed request payload for operation %q -- add one to operations.go rather than skipping it", operation)
	}
	return spec, nil
}

// KnownOperations lists every operation this table covers, sorted.
func KnownOperations() []string {
	names := make([]string, 0, len(operationSpecs))
	for name := range operationSpecs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// AssertCoverage checks this table against the operation set the RUNNING
// query-api reports at GET /registry -- the only authority on what is
// actually registered. Both directions are errors:
//
//   - registered but uncovered: prove would silently measure fewer than
//     all operations, which is the "a measurement that did not happen"
//     failure D15/R4 exists to stop.
//   - covered but unregistered: a stale entry here, which reads as
//     coverage while proving nothing.
func AssertCoverage(registered []string) error {
	registeredSet := make(map[string]bool, len(registered))
	for _, name := range registered {
		registeredSet[name] = true
	}

	var uncovered, stale []string
	for _, name := range registered {
		if _, ok := operationSpecs[name]; !ok {
			uncovered = append(uncovered, name)
		}
	}
	for name := range operationSpecs {
		if !registeredSet[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(uncovered)
	sort.Strings(stale)

	switch {
	case len(uncovered) > 0 && len(stale) > 0:
		return fmt.Errorf("goapiproof: request-payload table disagrees with the running registry: uncovered=%v stale=%v", uncovered, stale)
	case len(uncovered) > 0:
		return fmt.Errorf("goapiproof: the running query-api registers operations this table cannot build a request for: %v", uncovered)
	case len(stale) > 0:
		return fmt.Errorf("goapiproof: this table covers operations the running query-api does not register: %v", stale)
	}
	return nil
}

// securityAlertsParity declares the one divergence on the alert list: the
// three timestamp columns are ClickHouse DateTime64(3, 'UTC'); Python's
// driver returns them naive and strawberry prints no offset, while the Go
// plane prints RFC3339 with an explicit offset -- the same DateTime class
// the pr operation declares.
var securityAlertsParity = Options{BaselineDefects: []BaselineDefect{{
	Ticket: "CHAOS-5780",
	Reason: "security_alerts' created_at/fixed_at/dismissed_at are ClickHouse DateTime64(3, 'UTC'); Python's clickhouse_connect driver returns them NAIVE, so strawberry's DateTime scalar isoformat()s them with no offset, while Go's driver attaches UTC and gqlgen's graphql.Time scalar prints RFC3339 with an explicit offset -- the same declared DateTime class as pr's fields. Go is correct.",
	Paths: []string{
		"data.securityAlerts.edges.node.createdAt",
		"data.securityAlerts.edges.node.fixedAt",
		"data.securityAlerts.edges.node.dismissedAt",
	},
}}}

func securityAlertsVariables(orgID string, filters, pagination map[string]any) map[string]any {
	vars := map[string]any{"orgId": orgID, "filters": nil, "pagination": nil}
	if filters != nil {
		vars["filters"] = filters
	}
	if pagination != nil {
		vars["pagination"] = pagination
	}
	return vars
}

func securityAlertsVariant(name string, filters, pagination map[string]any) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return securityAlertsVariables(orgID, filters, pagination)
		},
		Parity: securityAlertsParity,
	}
}

func securityOverviewVariant(name string, filters map[string]any) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "filters": filters}
		},
	}
}

// catalogDimensionVariants builds one catalogValues variant per dimension,
// named by the dimension.
func catalogDimensionVariants(dimensions ...string) []Variant {
	variants := make([]Variant, 0, len(dimensions))
	for _, dimension := range dimensions {
		variants = append(variants, Variant{
			Name: dimension,
			Variables: func(orgID string, _ Window) map[string]any {
				return map[string]any{"orgId": orgID, "dimension": dimension}
			},
		})
	}
	return variants
}

func busFactorVariant(name string, scope map[string]any) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "scope": scope}
		},
	}
}

// metricLineageVariants builds one metricLineage variant per metric, named by
// the metric.
func metricLineageVariants(metrics ...string) []Variant {
	variants := make([]Variant, 0, len(metrics))
	for _, metric := range metrics {
		variants = append(variants, Variant{
			Name: metric,
			Variables: func(orgID string, _ Window) map[string]any {
				return map[string]any{"metricId": metric}
			},
			Parity: metricLineageParity(),
		})
	}
	return variants
}

// metricLineageParity declares the one wire-form difference every lineage
// request with a known metric carries.
func metricLineageParity() Options {
	return Options{BaselineDefects: []BaselineDefect{{
		Ticket:             "CHAOS-6090",
		Reason:             "the lineage read takes argMax(computed_at, computed_at) from the metric tables, whose computed_at column is a ClickHouse DateTime('UTC'); Python's driver returns it without tzinfo, so strawberry's DateTime scalar isoformat()s it with no offset, while Go's gqlgen DateTime scalar formats the same instant as RFC 3339 with \"Z\" (models/data_health.py compute_metric_lineage). The instants are equal; only the wire text differs. Go's form is the canonical DateTime wire form; the Python form is the declared defect and stays frozen.",
		Paths:              []string{"data.dataHealth.metricLineage.computedAt"},
		Intermittent:       true,
		IntermittentReason: "metricLineage is null for a metric no table holds and for tables with no answer, so the leaf exists only for a known metric",
	}}}
}

// busFactorInstanceVariant is a busFactor scope variant whose scope field is
// the run-supplied identifier, measured only when the answer lists at least
// one repository on a leg.
func busFactorInstanceVariant(name, kind, scopeField, nonEmpty string) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "scope": map[string]any{}}
		},
		Parity: Options{RequireNonEmpty: []string{nonEmpty}},
		Instance: &VariantInstance{
			Kind: kind,
			Bind: func(vars map[string]any, value string) { vars["scope"].(map[string]any)[scopeField] = value },
			EchoFor: func(value string) []ScopeEcho {
				return []ScopeEcho{
					{List: nonEmpty, Fields: []string{"repoId"}, Value: value},
					{List: "data.busFactor.scope.repoId", Scalar: true, Value: value},
				}
			},
		},
	}
}

// securityAlertsInstanceVariant filters the alert list by a run-supplied
// repository id (asList) or search term, measured only when the list holds
// at least one alert on a leg.
func securityAlertsInstanceVariant(name, kind, field string, asList bool) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return securityAlertsVariables(orgID, map[string]any{}, nil)
		},
		Parity: Options{
			BaselineDefects: securityAlertsParity.BaselineDefects,
			RequireNonEmpty: []string{"data.securityAlerts.edges"},
		},
		Instance: &VariantInstance{
			Kind: kind,
			Bind: func(vars map[string]any, value string) {
				var v any = value
				if asList {
					v = []any{value}
				}
				vars["filters"].(map[string]any)[field] = v
			},
			EchoFor: func(value string) []ScopeEcho {
				if asList {
					return []ScopeEcho{{List: "data.securityAlerts.edges", Fields: []string{"node.repoId"}, Value: value}}
				}
				return []ScopeEcho{{List: "data.securityAlerts.edges", Fields: []string{"node.title", "node.packageName", "node.cveId"}, Contains: true, Value: value}}
			},
		},
	}
}

// securityOverviewRepoVariant filters the overview by a run-supplied
// repository id, measured only when the top-repository list holds that
// repository (the only overview list whose elements name a repository).
func securityOverviewRepoVariant(name, kind string) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "filters": map[string]any{}}
		},
		Parity: Options{RequireNonEmpty: []string{"data.securityOverview.topRepos"}},
		Instance: &VariantInstance{
			Kind: kind,
			Bind: func(vars map[string]any, value string) {
				vars["filters"].(map[string]any)["repoIds"] = []any{value}
			},
			EchoFor: func(value string) []ScopeEcho {
				return []ScopeEcho{{List: "data.securityOverview.topRepos", Fields: []string{"repoId"}, Value: value}}
			},
		},
	}
}

// securityAlertsSecondPageVariant reads the page after the first five alerts;
// it is measured only when that page holds an alert on a leg.
func securityAlertsSecondPageVariant() Variant {
	v := securityAlertsVariant("PAGE_AFTER", nil, map[string]any{"first": 5, "after": "5"})
	v.Parity = Options{
		BaselineDefects: securityAlertsParity.BaselineDefects,
		RequireNonEmpty: []string{"data.securityAlerts.edges"},
	}
	return v
}

// aiRollupVariables builds the shared request of the AI rollup operations: the
// org, the run's day window and the given scope (nil sends no scope).
func aiRollupVariables(scope map[string]any) func(orgID string, w Window) map[string]any {
	return func(orgID string, w Window) map[string]any {
		vars := map[string]any{
			"orgId":     orgID,
			"dateRange": map[string]any{"startDate": w.SinceDate, "endDate": w.UntilDate},
			"scope":     nil,
		}
		if scope != nil {
			vars["scope"] = scope
		}
		return vars
	}
}

// aiRollupVariants is one variant per scope branch the AI rollup resolvers
// have. The repository and team values name nothing in any org, so both planes
// answer the empty window for them; a scope that selects real rows needs a
// run-supplied identifier.
func aiRollupVariants(parity Options) []Variant {
	scopes := []struct {
		name  string
		scope map[string]any
	}{
		{"WORK_TYPE", map[string]any{"workType": "pull_request"}},
		{"BUCKETS", map[string]any{"buckets": []any{"AI_ASSISTED", "HUMAN"}}},
		{"REPO_UNKNOWN", map[string]any{"repoId": "00000000-0000-0000-0000-000000000001"}},
		{"REPO_NAME_UNKNOWN", map[string]any{"repoId": "no-such-org/no-such-repo"}},
		{"TEAM_UNKNOWN", map[string]any{"teamId": "team-abc-123"}},
		{"REPO_AND_TEAM_UNKNOWN", map[string]any{"repoId": "00000000-0000-0000-0000-000000000001", "teamId": "team-abc-123"}},
	}
	variants := make([]Variant, 0, len(scopes))
	for _, sc := range scopes {
		variants = append(variants, Variant{Name: sc.name, Variables: aiRollupVariables(sc.scope), Parity: parity})
	}
	return variants
}

const aiComputedAtReason = "the rollup rows carry computed_at as a ClickHouse DateTime64(3, 'UTC'); Python's driver returns it tz-aware, so strawberry's DateTime scalar isoformat()s it with a \"+00:00\" offset and microsecond digits, while Go's gqlgen DateTime scalar formats the same instant as RFC 3339 with \"Z\" (resolvers/ai.py resolve_ai_impact_summary, computed_at = max over rows). The instants are equal; only the wire text differs. Go's form is the canonical DateTime wire form; the Python form is the declared defect and stays frozen."

// aiImpactSummaryParity declares the one wire-form difference the impact
// summary carries.
func aiImpactSummaryParity() Options {
	return Options{BaselineDefects: []BaselineDefect{{
		Ticket:             "CHAOS-6081",
		Reason:             aiComputedAtReason,
		Paths:              []string{"data.aiImpactSummary.computedAt"},
		Intermittent:       true,
		IntermittentReason: "computedAt is null when the window has no rollup rows, so the leaf exists only for a window with data",
	}}}
}

// aiReviewLoadParity declares pickup latency a merged floating-point
// aggregate: it is an average over raw pull-request rows that ClickHouse
// merges in thread-completion order.
func aiReviewLoadParity() Options {
	return Options{FloatTierB: map[string]string{
		"data.aiReviewLoad.byBucket.pickupLatencyHours": "avgIf over raw pull-request rows: ClickHouse merges partial aggregate states in thread-completion order, so the last bits differ run to run on both planes (CHAOS-5451)",
		"data.aiReviewLoad.daily.pickupLatencyHours":    "avgIf over raw pull-request rows: ClickHouse merges partial aggregate states in thread-completion order, so the last bits differ run to run on both planes (CHAOS-5451)",
	}}
}

// aiInstanceVariant is an AI rollup scope variant whose scope field is a
// run-supplied identifier. It is measured only when nonEmpty (when named)
// holds an element on a leg; echoList (when named) must carry the supplied
// value in echoField on every element.
func aiInstanceVariant(name, kind, scopeField string, parity Options, nonEmpty, echoList, echoField string) Variant {
	if nonEmpty != "" {
		parity.RequireNonEmpty = []string{nonEmpty}
	}
	return Variant{
		Name:      name,
		Variables: aiRollupVariables(map[string]any{}),
		Parity:    parity,
		Instance: &VariantInstance{
			Kind: kind,
			Bind: func(vars map[string]any, value string) { vars["scope"].(map[string]any)[scopeField] = value },
			EchoFor: func(value string) []ScopeEcho {
				if echoList == "" {
					return nil
				}
				return []ScopeEcho{{List: echoList, Fields: []string{echoField}, Value: value}}
			},
		},
	}
}

// compoundingRiskParity declares the two timestamp leaves: generatedAt is the
// response instant and differs on every request by construction; computedAt is
// a ClickHouse DateTime column that Python prints without an offset and Go
// prints as RFC3339 with one, the same DateTime class the other operations
// declare.
var compoundingRiskParity = Options{
	VolatileFields: map[string]string{
		"data.compoundingRisk.generatedAt": "the response instant, regenerated on every request on both planes; two calls can never agree on it",
	},
	BaselineDefects: []BaselineDefect{{
		Ticket: "CHAOS-5780",
		Reason: "compounding_risk_daily.computed_at is a ClickHouse DateTime column; Python's clickhouse_connect driver returns it NAIVE, so strawberry's DateTime scalar isoformat()s it with no offset, while Go's driver attaches UTC and gqlgen's graphql.Time scalar prints RFC3339 with an explicit offset -- the same declared DateTime class as pr's fields. Go is correct.",
		Paths:  []string{"data.compoundingRisk.rows.computedAt"},
	}},
}

func compoundingRiskVariables(orgID string, filter map[string]any) map[string]any {
	vars := map[string]any{"orgId": orgID, "filter": nil}
	if filter != nil {
		vars["filter"] = filter
	}
	return vars
}

func compoundingRiskVariant(name string, filter map[string]any) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return compoundingRiskVariables(orgID, filter)
		},
		Parity: compoundingRiskParity,
	}
}

// compoundingRiskDayVariant pins the read to the last day of the run's window.
func compoundingRiskDayVariant(name, breakout string) Variant {
	return Variant{
		Name: name,
		Variables: func(orgID string, w Window) map[string]any {
			return compoundingRiskVariables(orgID, map[string]any{"breakout": breakout, "day": w.UntilDate, "trendDays": 30})
		},
		Parity: compoundingRiskParity,
	}
}

// compoundingRiskInstanceVariant filters by a run-supplied repository or team
// id, measured only when the answer lists rows and every row is that scope.
func compoundingRiskInstanceVariant(name, kind, breakout, field string) Variant {
	parity := compoundingRiskParity
	parity.RequireNonEmpty = []string{"data.compoundingRisk.rows"}
	return Variant{
		Name: name,
		Variables: func(orgID string, _ Window) map[string]any {
			return compoundingRiskVariables(orgID, map[string]any{"breakout": breakout, "trendDays": 30})
		},
		Parity: parity,
		Instance: &VariantInstance{
			Kind: kind,
			Bind: func(vars map[string]any, value string) {
				vars["filter"].(map[string]any)[field] = []any{value}
			},
			EchoFor: func(value string) []ScopeEcho {
				return []ScopeEcho{{List: "data.compoundingRisk.rows", Fields: []string{"scopeId", "scopeLabel"}, Value: value}}
			},
		},
	}
}

func experimentsVariant(name, level string, ids []string) Variant {
	return Variant{
		Name:   name,
		Parity: Options{RequireNonEmpty: []string{"data.experiments.items"}},
		Variables: func(orgID string, _ Window) map[string]any {
			scope := map[string]any{"level": level, "ids": []string{}}
			if ids != nil {
				scope["ids"] = ids
			}
			return map[string]any{"orgId": orgID, "filters": map[string]any{"scope": scope}}
		},
	}
}
