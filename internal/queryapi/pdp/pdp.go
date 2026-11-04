// Package pdp is query-api's policy enforcement point: it asks auth-service's PDP (POST /v1/authorize) whether the
// caller of a REST route may perform the route's catalog action, presenting query-api's own workload token.
//
// QUERY_API_PDP_MODE selects off (no call), shadow (call, count and compare, never deny) or enforce (a deny refuses
// the request). A bad value, or shadow/enforce with a missing input, never fails the start: one ERROR names the
// variable, the config-invalid gauge is 1, and every request that would ask the PDP is refused as unavailable.
package pdp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
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
	shadowInFlight = 32
	unknownAction  = "unknown"
	profileService = "dev-health-query-api"
	meterName      = "github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"

	exemptMetric        = "dev_health_query_api_pdp_exempt_total"
	resultMetric        = "dev_health_query_api_pdp_results_total"
	enabledMetric       = "dev_health_query_api_pdp_enabled"
	configInvalidMetric = "dev_health_query_api_pdp_config_invalid"
)

// ExemptClasses is the explicit exempt list.
var ExemptClasses = []string{ClassHS256, ClassEnvelope}

// Results are the closed values of the result label.
const (
	ResultAllow         = "allow"
	ResultDeny          = "deny"
	ResultUnavailable   = "unavailable"
	ResultMatch         = "match"
	ResultMismatch      = "mismatch"
	ResultError         = "error"
	ResultDropped       = "dropped"
	ResultNotConfigured = "not_configured"
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
}

// Gate is the PEP of one process. A nil Gate is off.
type Gate struct {
	mode    string
	decider pep.Decider
	actions map[string]string
	slots   chan struct{}
	logger  *slog.Logger
	wg      sync.WaitGroup
	logGate atomic.Int64

	exempt  metric.Int64Counter
	results metric.Int64Counter
}

// FromSettings builds the gate from lookup. Off reads nothing but EnvMode. It never fails: a bad value or a missing
// input is a gate that refuses every PDP-gated request, after one ERROR naming the variable.
func FromSettings(lookup func(string) (string, bool), logger *slog.Logger) *Gate {
	if logger == nil {
		logger = slog.Default()
	}
	raw, _ := lookup(EnvMode)
	mode := strings.TrimSpace(raw)
	switch mode {
	case "", ModeOff:
		return build(Config{Mode: ModeOff, Logger: logger})
	case ModeShadow, ModeEnforce:
	default:
		return invalid(Config{Logger: logger}, EnvMode)
	}
	decider, variable := newDecider(lookup)
	if decider == nil {
		return invalid(Config{Logger: logger}, variable)
	}
	return New(Config{Mode: mode, Decider: decider, Logger: logger})
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
	g := &Gate{mode: cfg.Mode, decider: cfg.Decider, logger: cfg.Logger, slots: make(chan struct{}, shadowInFlight)}
	if g.logger == nil {
		g.logger = slog.Default()
	}
	rows := cfg.Rows
	if rows == nil {
		rows = authv1.QueryAPIEndpointProfiles
	}
	actions, err := parseActions(rows)
	if err != nil && g.mode != ModeOff {
		g.logger.Error("query-api pdp: the endpoint-profile rows do not parse; every PDP-gated request is refused as unavailable",
			slog.String("variable", "rows"))
		g.mode, g.decider = modeInvalid, nil
	}
	g.actions = actions
	g.exempt, _ = meter.Int64Counter(exemptMetric,
		metric.WithDescription("Accepted callers of a PDP-gated route whose credential class is on the exempt list, by class and row action"))
	g.results, _ = meter.Int64Counter(resultMetric,
		metric.WithDescription("PDP asks of query-api by mode (shadow, enforce, invalid), result and row action"))
	enabled, _ := meter.Int64Gauge(enabledMetric, metric.WithDescription("1 while the PDP gate is built in shadow or enforce mode, else 0"))
	invalidGauge, _ := meter.Int64Gauge(configInvalidMetric,
		metric.WithDescription("1 when QUERY_API_PDP_MODE is bad or a mode that is on misses an input: PDP-gated requests are refused"))
	ctx := context.Background()
	if enabled != nil {
		enabled.Record(ctx, boolGauge(g.mode == ModeShadow || g.mode == ModeEnforce))
	}
	if invalidGauge != nil {
		invalidGauge.Record(ctx, boolGauge(g.mode == modeInvalid))
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

// Authorize asks the PDP whether the user access token bearer may perform the action of the row profile
// ("METHOD /route") in the verified organization orgID. It returns a *DenyError only in enforce mode, or when the gate
// is misconfigured; off and shadow return nil. Shadow asks in the background and never changes the answer.
func (g *Gate) Authorize(ctx context.Context, profile, bearer, orgID string) error {
	if g == nil || g.mode == ModeOff {
		return nil
	}
	action, known := g.actions[profile]
	label := action
	if !known {
		label = unknownAction
	}
	if g.mode == modeInvalid {
		g.count(ctx, modeInvalid, ResultNotConfigured, label)
		return &DenyError{Reason: pep.ReasonNotConfigured, Local: true}
	}
	org := strings.ToLower(orgID)
	request := pep.Request{PrincipalReference: bearer, Action: action, ResourceType: policycatalog.ScopeOrganization, ResourceKey: org, OrganizationID: org}
	if g.mode == ModeShadow {
		g.shadow(ctx, request, known, label)
		return nil
	}
	if !known {
		g.count(ctx, ModeEnforce, ResultUnavailable, label)
		return &DenyError{Reason: pep.ReasonInvalidRequest, Local: true}
	}
	decision := g.decider.Decide(ctx, request)
	switch {
	case decision.Allowed():
		g.count(ctx, ModeEnforce, ResultAllow, label)
		return nil
	case decision.Local():
		g.count(ctx, ModeEnforce, ResultUnavailable, label)
		return &DenyError{Reason: decision.Reason(), Local: true}
	}
	g.count(ctx, ModeEnforce, ResultDeny, label)
	return &DenyError{Reason: decision.Reason()}
}

// shadow compares the PDP's answer with the legacy one, which has already allowed this caller.
func (g *Gate) shadow(ctx context.Context, request pep.Request, known bool, label string) {
	if !known {
		g.count(ctx, ModeShadow, ResultError, label)
		return
	}
	select {
	case g.slots <- struct{}{}:
	default:
		g.count(ctx, ModeShadow, ResultDropped, label)
		return
	}
	detached := context.WithoutCancel(ctx)
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		defer func() { <-g.slots }()
		defer func() {
			if recover() != nil {
				g.count(detached, ModeShadow, ResultError, label)
			}
		}()
		call, cancel := context.WithTimeout(detached, callTimeout)
		defer cancel()
		decision := g.decider.Decide(call, request)
		switch {
		case decision.Allowed():
			g.count(detached, ModeShadow, ResultMatch, label)
		case decision.Local():
			g.count(detached, ModeShadow, ResultError, label)
		default:
			g.count(detached, ModeShadow, ResultMismatch, label)
			if g.logAllowed() {
				g.logger.WarnContext(detached, "query-api pdp shadow: the PDP denies a caller the legacy check allowed",
					slog.String("action", label), slog.String("reason_code", string(decision.Reason())))
			}
		}
	}()
}

// logAllowed lets one mismatch line per second through.
func (g *Gate) logAllowed() bool {
	now := time.Now().UnixNano()
	last := g.logGate.Load()
	return now-last >= int64(time.Second) && g.logGate.CompareAndSwap(last, now)
}

// Wait blocks until every shadow call in flight has ended.
func (g *Gate) Wait() {
	if g != nil {
		g.wg.Wait()
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
