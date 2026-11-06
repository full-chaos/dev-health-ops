// Package pdp is query-api's policy enforcement point: it asks auth-service's PDP (POST /v1/authorize) whether the
// caller of a REST route may perform the route's catalog action, presenting query-api's own workload token.
//
// QUERY_API_PDP_MODE builds the client: off (no client, no call), shadow or enforce (a client). Whether a route is
// enforced is the 5.17 cell of its action namespace (authzenforce.QueryAPI), not the mode: with the cell on the
// decision replaces the legacy answer, and an allow is served only as the pepgate.Scope its obligations name; with
// the cell off, shadow mode has the 5.12 shadow core ask beside the legacy check (counts and compares, never changes
// the answer) and enforce mode refuses. A cell on with no client is a 503. A bad value, or shadow/enforce with a
// missing input, never fails the start: one ERROR names the variable, the config-invalid gauge is 1, and every
// request that would ask the PDP is refused as unavailable.
package pdp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

const (
	// EnvMode is the switch: off, shadow or enforce; unset or empty is off.
	EnvMode = "QUERY_API_PDP_MODE"
	// EnvAuthProxyURL is auth-service's base URL, go-api's setting of the same name.
	EnvAuthProxyURL = "AUTH_PROXY_URL"

	ModeOff     = "off"
	ModeShadow  = "shadow"
	ModeEnforce = "enforce"
	modeInvalid = "invalid"

	// ClientService names query-api to the PDP; it confers nothing.
	ClientService = "dev-health-query-api"

	// The credential classes that stay on today's authorization path, never asking the PDP, until HS256 retires
	// (ticket 10457) and the envelope retires (W11).
	ClassHS256    = "ops_access_token_hs256"
	ClassEnvelope = "effective_principal_envelope"

	callTimeout    = 2 * time.Second
	unknownAction  = "unknown"
	profileService = "dev-health-query-api"
	restPrefix     = "/api/v1/"
	meterName      = "github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"

	exemptMetric        = "dev_health_query_api_pdp_exempt_total"
	resultMetric        = "dev_health_query_api_pdp_results_total"
	enabledMetric       = "dev_health_query_api_pdp_enabled"
	configInvalidMetric = "dev_health_query_api_pdp_config_invalid"
)

// allowlistCutOver is the cut-over registry: the row profiles ("METHOD /route") whose handlers narrow every read to
// a scope's repository allowlist. An allowlist decision on a route not listed is not carried out (503), so a caller
// with a repository ceiling never reaches an unfiltered read. Empty until the per-handler cutover lists its routes.
var allowlistCutOver = map[string]bool{}

// ExemptClasses is the explicit exempt list.
var ExemptClasses = []string{ClassHS256, ClassEnvelope}

// Results are the closed values of the result label.
const (
	ResultAllow         = "allow"
	ResultDeny          = "deny"
	ResultUnavailable   = "unavailable"
	ResultNotConfigured = "not_configured"
	ResultNotCarriedOut = "not_carried_out"
	ResultNoPDP         = "no_decision_point"
)

// The closed local reasons of a 503 the gate makes itself on an enforced route.
const (
	ReasonNoDecisionPoint pep.Reason = "authz_enforce_no_decision_point"
	// ReasonNotCarriedOut is the PEP's unknown_obligation: an obligation the seam cannot apply yet.
	ReasonNotCarriedOut    pep.Reason = pep.ReasonUnknownObligation
	ReasonTenantMismatch   pep.Reason = "tenant_scope_mismatch"
	ReasonNoRepositoryRule pep.Reason = "repository_scope_absent"
	ReasonEnforceCellOff   pep.Reason = "authz_enforce_cell_off"
)

// DenyError is a typed refusal. Local is true when no PDP verdict exists (PDP down, a timeout, a bad answer, a
// misconfigured gate): the request is refused as unavailable. Otherwise the PDP denied.
type DenyError struct {
	Reason pep.Reason
	Local  bool
}

func (e *DenyError) Error() string { return "pdp: denied: " + string(e.Reason) }

// Config builds a Gate.
type Config struct {
	// Mode is ModeOff, ModeShadow or ModeEnforce.
	Mode string
	// Decider asks the PDP; required unless Mode is ModeOff.
	Decider pep.Decider
	// Rows is an endpoint-profile document; nil reads query-api's.
	Rows   []byte
	Logger *slog.Logger
	// Meter records the counters and gauges; nil is the global meter.
	Meter metric.Meter
	// Switch is query-api's 5.17 cell set: a route is enforced only while the cell of its action namespace is on.
	Switch authzenforce.Switch
	// Lookup reads the 5.12 shadow knobs (AUTH_SHADOW_PDP_SAMPLE, _MAX_IN_FLIGHT, _HIGH_RISK) in shadow mode; nil
	// reads none, so every knob has its default.
	Lookup func(string) (string, bool)
}

// Gate is the PEP of one process. A nil Gate is off.
type Gate struct {
	mode    string
	decider pep.Decider
	actions map[string]string
	cells   authzenforce.Switch
	// carriesAllowlist is this gate's copy of allowlistCutOver.
	carriesAllowlist map[string]bool
	logger           *slog.Logger
	shadow           *policy.Shadow

	exempt  metric.Int64Counter
	results metric.Int64Counter
}

// FromSettings builds the gate from lookup. Off reads nothing but EnvMode and the 5.17 cell list. It never fails: a
// bad value or a missing input is a gate that refuses every PDP-gated request, after one ERROR naming the variable.
func FromSettings(lookup func(string) (string, bool), logger *slog.Logger) *Gate {
	if logger == nil {
		logger = slog.Default()
	}
	raw, _ := lookup(EnvMode)
	cells := authzenforce.Load(authzenforce.QueryAPI, lookup, logger)
	mode := strings.TrimSpace(raw)
	switch mode {
	case "", ModeOff:
		return build(Config{Mode: ModeOff, Logger: logger, Switch: cells})
	case ModeShadow, ModeEnforce:
	default:
		return invalid(Config{Logger: logger, Switch: cells}, EnvMode)
	}
	decider, variable := newDecider(lookup)
	if decider == nil {
		return invalid(Config{Logger: logger, Switch: cells}, variable)
	}
	return New(Config{Mode: mode, Decider: decider, Logger: logger, Switch: cells, Lookup: lookup})
}

// newDecider builds the PEP client in go-api's form (#5605): the workload client over the origin of AUTH_PROXY_URL,
// audience auth-service, a 2s call. variable names the bad input when it fails.
func newDecider(lookup func(string) (string, bool)) (pep.Decider, string) {
	raw, _ := lookup(EnvAuthProxyURL)
	origin, ok := workloadinput.ParseOrigin(strings.TrimSpace(raw))
	if !ok {
		return nil, EnvAuthProxyURL
	}
	client, err := workloadinput.NewClient(origin, 0, lookup, []workloadinput.Ask{{Flag: EnvMode, Selector: "the PDP credential"}})
	if err != nil {
		var input *workloadinput.InputError
		if errors.As(err, &input) && input.Variable != "" {
			return nil, input.Variable
		}
		return nil, "workload"
	}
	httpClient, err := client.HTTPClient(workloadinput.Transport(), origin.String(), authprovision.WorkloadAudience)
	if err != nil {
		return nil, "workload"
	}
	decider, err := pep.New(pep.Config{BaseURL: origin.String(), AllowPlainHTTP: origin.Scheme == "http", HTTPClient: httpClient,
		ClientService: ClientService, Timeout: callTimeout})
	if err != nil {
		return nil, EnvAuthProxyURL
	}
	return decider, ""
}

// New builds a gate from cfg; a bad cfg is a gate that refuses every PDP-gated request.
func New(cfg Config) *Gate {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeOff
	}
	switch {
	case cfg.Mode == ModeOff:
	case cfg.Mode != ModeShadow && cfg.Mode != ModeEnforce:
		return invalid(cfg, EnvMode)
	case cfg.Decider == nil:
		return invalid(cfg, "decider")
	}
	return build(cfg)
}

func invalid(cfg Config, variable string) *Gate {
	cfg.Mode, cfg.Decider = modeInvalid, nil
	g := build(cfg)
	g.logger.Error("query-api pdp: a missing input or a bad value; every PDP-gated request is refused as unavailable",
		slog.String("variable", variable))
	return g
}

func build(cfg Config) *Gate {
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter(meterName)
	}
	g := &Gate{mode: cfg.Mode, decider: cfg.Decider, logger: cfg.Logger, cells: cfg.Switch, carriesAllowlist: maps.Clone(allowlistCutOver)}
	if g.logger == nil {
		g.logger = slog.Default()
	}
	rows := cfg.Rows
	if rows == nil {
		rows = authv1.QueryAPIEndpointProfiles
	}
	actions, err := parseActions(rows)
	if err != nil && (g.mode != ModeOff || g.anyCellOn()) {
		g.logger.Error("query-api pdp: the endpoint-profile rows do not parse; every PDP-gated request is refused as unavailable",
			slog.String("variable", "rows"))
		g.mode, g.decider = modeInvalid, nil
	}
	g.actions = actions
	if err := authzenforce.Register(meter, g.cells); err != nil {
		g.logger.Error("query-api pdp: the authz enforcement gauges are not registered", slog.String("service", string(authzenforce.QueryAPI)))
	}
	noClient := g.anyCellOn() && g.decider == nil
	if noClient {
		g.logger.Error("query-api pdp: a namespace is switched on but query-api has no decision point client; its requests are refused with a 503",
			slog.String("env", authzenforce.QueryAPI.Env()), slog.String("variable", EnvMode))
	}
	if g.mode == ModeShadow {
		g.shadow = buildShadow(cfg, actions, g.logger)
	}
	g.exempt, _ = meter.Int64Counter(exemptMetric,
		metric.WithDescription("Accepted callers of a PDP-gated route whose credential class is on the exempt list, by class and row action"))
	g.results, _ = meter.Int64Counter(resultMetric,
		metric.WithDescription("PDP asks of query-api by mode (enforce, invalid), result and row action; shadow counts on the 5.12 counters"))
	enabled, _ := meter.Int64Gauge(enabledMetric, metric.WithDescription("1 while the PDP gate is built in shadow or enforce mode, else 0"))
	invalidGauge, _ := meter.Int64Gauge(configInvalidMetric,
		metric.WithDescription("1 when QUERY_API_PDP_MODE is bad, a mode that is on misses an input, or an enforcement cell is on with no client: PDP-gated requests are refused"))
	ctx := context.Background()
	if enabled != nil {
		enabled.Record(ctx, boolGauge(g.mode == ModeShadow || g.mode == ModeEnforce))
	}
	if invalidGauge != nil {
		invalidGauge.Record(ctx, boolGauge(g.mode == modeInvalid || noClient))
	}
	return g
}

func boolGauge(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

// Mode is the gate's mode: off, shadow, enforce or invalid. A nil gate is off.
func (g *Gate) Mode() string {
	if g == nil {
		return ModeOff
	}
	return g.mode
}

// anyCellOn reports whether some namespace is enforced.
func (g *Gate) anyCellOn() bool {
	for _, namespace := range authzenforce.Namespaces() {
		if g.cells.On(namespace) {
			return true
		}
	}
	return false
}

// Shadow is the 5.12 shadow core a shadow-mode gate holds; nil in every other mode, and in shadow mode when a knob is
// bad. The REST authentication hooks call its Observe with the legacy answer.
func (g *Gate) Shadow() *policy.Shadow {
	if g == nil {
		return nil
	}
	return g.shadow
}

// buildShadow builds the shadow over the REST rows and the gate's own decider; a bad 5.12 knob is nil, after the
// core's one ERROR naming the variable and its config-invalid gauge. It never fails the start.
func buildShadow(cfg Config, actions map[string]string, logger *slog.Logger) *policy.Shadow {
	rows, err := shadowRowsOf(actions)
	if err != nil {
		logger.Error("query-api pdp: the shadow rows do not build; the shadow is off", slog.String("variable", "rows"))
		return nil
	}
	lookup := cfg.Lookup
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	return policy.ShadowFromEnv(lookup, policy.ShadowEnvDeps{Rows: rows, Logger: logger, On: true,
		NewDecider: func() (pep.Decider, error) { return cfg.Decider, nil }})
}

// RESTShadowRows are the shadow's rows of an endpoint-profile document: every protected row of the REST routes
// (path under /api/v1/) that names a catalog action, keyed by "METHOD /route". The GraphQL, /query and /buildinfo
// rows are not REST rows.
func RESTShadowRows(raw []byte) (map[string]policy.ShadowRow, error) {
	actions, err := parseActions(raw)
	if err != nil {
		return nil, err
	}
	return shadowRowsOf(actions)
}

func shadowRowsOf(actions map[string]string) (map[string]policy.ShadowRow, error) {
	rows := map[string]policy.ShadowRow{}
	for key, action := range actions {
		_, route, _ := strings.Cut(key, " ")
		if !strings.HasPrefix(route, restPrefix) {
			continue
		}
		row, err := policy.ShadowRowFor(action)
		if err != nil {
			return nil, fmt.Errorf("pdp rows: %s: %w", key, err)
		}
		rows[key] = row
	}
	if len(rows) == 0 {
		return nil, errors.New("pdp rows: no REST row names an action")
	}
	return rows, nil
}

// Authorize decides whether the user access token bearer may perform the action of the row profile ("METHOD
// /route") in the verified organization orgID, and returns the scope the request may serve.
//
// A route is enforced only while the 5.17 cell of its action's namespace is on. Not enforced: the scope is
// pepgate.Legacy(orgID) (today's path) and a gate built in shadow or enforce mode asks in the background, never
// changing the answer. Enforced: a PDP deny is a *DenyError (403); no verdict, no client, or an allow whose
// obligations query-api does not carry out is a local *DenyError (503); an allow is the scope scopeOf builds. A
// misconfigured gate refuses every row with a local *DenyError. Every refusal carries the zero Scope.
func (g *Gate) Authorize(ctx context.Context, profile, bearer, orgID string) (pepgate.Scope, error) {
	if g == nil {
		return pepgate.Legacy(orgID), nil
	}
	action, known := g.actions[profile]
	label := action
	if !known {
		label = unknownAction
	}
	namespace, _, _ := strings.Cut(action, ".")
	enforced := known && g.cells.On(namespace)
	switch {
	case !known && (g.anyCellOn() || g.mode == ModeEnforce):
		// A row the gate cannot place under a cell is refused while any cell is on, or in enforce mode.
		g.count(ctx, ModeEnforce, ResultUnavailable, label)
		return pepgate.Scope{}, &DenyError{Reason: pep.ReasonInvalidRequest, Local: true}
	case g.mode == modeInvalid:
		reason := pep.ReasonNotConfigured
		if enforced {
			reason = ReasonNoDecisionPoint
		}
		g.count(ctx, modeInvalid, ResultNotConfigured, label)
		return pepgate.Scope{}, &DenyError{Reason: reason, Local: true}
	case !enforced && g.mode == ModeEnforce:
		// A mode named enforce never acts as shadow: with the cell off it refuses.
		g.count(ctx, ModeEnforce, ResultNotConfigured, label)
		return pepgate.Scope{}, &DenyError{Reason: ReasonEnforceCellOff, Local: true}
	case !enforced:
		return pepgate.Legacy(orgID), nil
	case g.decider == nil:
		g.count(ctx, ModeEnforce, ResultNoPDP, label)
		return pepgate.Scope{}, &DenyError{Reason: ReasonNoDecisionPoint, Local: true}
	}
	decision := g.decider.Decide(ctx, g.request(action, bearer, orgID))
	switch {
	case decision.Allowed():
		scope, refused := g.scopeOf(decision, profile, action, orgID)
		if refused != "" {
			g.count(ctx, ModeEnforce, ResultNotCarriedOut, label)
			return pepgate.Scope{}, &DenyError{Reason: refused, Local: true}
		}
		g.count(ctx, ModeEnforce, ResultAllow, label)
		return scope, nil
	case decision.Local():
		g.count(ctx, ModeEnforce, ResultUnavailable, label)
		return pepgate.Scope{}, &DenyError{Reason: decision.Reason(), Local: true}
	}
	g.count(ctx, ModeEnforce, ResultDeny, label)
	return pepgate.Scope{}, &DenyError{Reason: decision.Reason()}
}

func (g *Gate) request(action, bearer, orgID string) pep.Request {
	org := strings.ToLower(orgID)
	return pep.Request{PrincipalReference: bearer, Action: action, ResourceType: policycatalog.ScopeOrganization, ResourceKey: org, OrganizationID: org}
}

// scopeOf is the scope of an allow, or the closed reason it is not carried out. Fail-closed order: the decision and
// its tenant_scope must both name the request's organization, compared as UUIDs (the scope carries the canonical
// form); redaction,
// minimum_assurance and step_up are not carried out; repository_scope organization, or an allowlist holding "*", is
// every repository; any other allowlist is a ceiling only on a route the cut-over registry lists; neither repository
// member is the tenant alone for an action the catalog marks repository_scope none, and no repository otherwise.
func (g *Gate) scopeOf(d pep.Decision, profile, action, orgID string) (pepgate.Scope, pep.Reason) {
	org, ok := canonicalUUID(orgID)
	if !ok {
		return pepgate.Scope{}, ReasonTenantMismatch
	}
	if decided, ok := d.OrganizationID(); !ok || !sameUUID(decided, org) {
		return pepgate.Scope{}, ReasonTenantMismatch
	}
	o := d.Obligations()
	if tenant, ok := o.TenantScope(); !ok || !sameUUID(tenant, org) {
		return pepgate.Scope{}, ReasonTenantMismatch
	}
	if _, ok := o.MinimumAssurance(); ok || len(o.Redaction()) != 0 || o.StepUpRequired() {
		return pepgate.Scope{}, ReasonNotCarriedOut
	}
	switch {
	case o.RepositoryScopeOrganization():
		return pepgate.Organization(org), ""
	case o.RepositoryRestricted():
		keys := o.RepositoryAllowlist()
		if slices.Contains(keys, "*") {
			return pepgate.Organization(org), ""
		}
		if !g.carriesAllowlist[profile] {
			return pepgate.Scope{}, ReasonNotCarriedOut
		}
		return pepgate.Allowlist(org, keys), ""
	}
	if catalogued, err := policycatalog.Default().Action(action); err == nil && catalogued.RepositoryScope == policycatalog.RepositoryScopeNone {
		return pepgate.Organization(org), ""
	}
	return pepgate.Scope{}, ReasonNoRepositoryRule
}

// Wait blocks until every shadow call in flight has ended.
func (g *Gate) Wait() {
	if g != nil {
		g.shadow.Wait()
	}
}

// CountExempt counts one accepted caller of the row profile whose class is on the exempt list, in every mode. A class
// off the list counts nothing.
func (g *Gate) CountExempt(ctx context.Context, class, profile string) {
	if g == nil || g.exempt == nil || !slices.Contains(ExemptClasses, class) {
		return
	}
	action, ok := g.actions[profile]
	if !ok {
		action = unknownAction
	}
	g.exempt.Add(ctx, 1, metric.WithAttributes(attribute.String("class", class), attribute.String("action", action)))
}

func (g *Gate) count(ctx context.Context, mode, result, action string) {
	if g.results != nil {
		g.results.Add(ctx, 1, metric.WithAttributes(attribute.String("mode", mode), attribute.String("result", result), attribute.String("action", action)))
	}
}

var rowListeners = regexp.MustCompile(`@([a-z,]+)\]$`)

// parseActions keys the catalog action of every protected query-api row served on the query or internal listener by
// "METHOD /route". Every action must be organization-scoped: the request names the caller's organization.
func parseActions(raw []byte) (map[string]string, error) {
	var document struct {
		Rows []struct {
			ID             string `json:"id"`
			Service        string `json:"service"`
			Method         string `json:"method"`
			Route          string `json:"route"`
			Action         string `json:"action"`
			Classification string `json:"classification"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("pdp rows: %w", err)
	}
	actions := map[string]string{}
	for _, row := range document.Rows {
		if row.Service != profileService || row.Classification != "protected" || row.Action == "" {
			continue
		}
		match := rowListeners.FindStringSubmatch(row.ID)
		if match == nil {
			return nil, fmt.Errorf("pdp rows: row id %q names no listener", row.ID)
		}
		listeners := strings.Split(match[1], ",")
		if !slices.Contains(listeners, "query") && !slices.Contains(listeners, "internal") {
			continue
		}
		key := row.Method + " " + row.Route
		if seen, ok := actions[key]; ok && seen != row.Action {
			return nil, fmt.Errorf("pdp rows: %s names two actions", key)
		}
		action, err := policycatalog.Default().Action(row.Action)
		if err != nil {
			return nil, fmt.Errorf("pdp rows: %s: %w", key, err)
		}
		if action.ResourceKind != policycatalog.ScopeOrganization {
			return nil, fmt.Errorf("pdp rows: %s: action %s is not organization-scoped", key, action.ID)
		}
		actions[key] = row.Action
	}
	if len(actions) == 0 {
		return nil, errors.New("pdp rows: no row names an action")
	}
	return actions, nil
}

// WithGate carries g in ctx for the REST handlers.
func WithGate(ctx context.Context, g *Gate) context.Context {
	return context.WithValue(ctx, gateKey{}, g)
}

// FromContext is the gate WithGate carried; nil (off) when none.
func FromContext(ctx context.Context) *Gate {
	g, _ := ctx.Value(gateKey{}).(*Gate)
	return g
}

type gateKey struct{}

// canonicalUUID is the canonical (lowercase) form of an RFC 9562 UUID in its 36-character text form, any case. Any
// other spelling (braces, urn:uuid:, no hyphens) or a value that does not parse is not an organization.
func canonicalUUID(s string) (string, bool) {
	if len(s) != 36 {
		return "", false
	}
	u, err := uuid.Parse(s)
	if err != nil {
		return "", false
	}
	return u.String(), true
}

func sameUUID(s, canonical string) bool {
	c, ok := canonicalUUID(s)
	return ok && c == canonical
}
