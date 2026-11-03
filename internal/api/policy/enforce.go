package policy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

// Closed outcomes of the enforcement counter.
const (
	enforceOutcomeAllow          = "allow"
	enforceOutcomeDeny           = "deny"
	enforceOutcomeErrorDeny      = "error_deny"
	enforceOutcomeUnjudgedLegacy = "unjudged_legacy"

	enforceDefaultTimeout = 2 * time.Second
	enforceRetryAfter     = "5"
)

// EnforceConfig builds an Enforcer.
type EnforceConfig struct {
	// Decider asks the policy decision point; a *pep.Client in production.
	Decider pep.Decider
	// Rows is the action map, keyed by "METHOD /route".
	Rows map[string]ShadowRow
	// Switch is the service's enforcement switch; a namespace that is off is never asked.
	Switch authzenforce.Switch
	// Timeout bounds one call. Zero is 2s.
	Timeout time.Duration
	Logger  *slog.Logger
}

// Enforcer lets the policy decision point decide the routes of the namespaces that are switched on. For such a
// route its answer replaces the legacy guard's: an allow passes, a deny refuses, and a failure of any kind refuses,
// because a guard that cannot get a verdict must not fall back to the check the switch replaced. The one
// exception is a caller the decision point cannot judge (a legacy HS256 token, a delegated session, an
// organization route with no organization): that request keeps the legacy answer, which is the same answer the
// route gives with its cell off.
type Enforcer struct {
	decider pep.Decider
	rows    map[string]ShadowRow
	sw      authzenforce.Switch
	timeout time.Duration
	logger  *slog.Logger
}

// NewEnforcer checks cfg.
func NewEnforcer(cfg EnforceConfig) (*Enforcer, error) {
	if cfg.Decider == nil {
		return nil, errors.New("enforce: no decider")
	}
	if len(cfg.Rows) == 0 {
		return nil, errors.New("enforce: no rows")
	}
	for key, row := range cfg.Rows {
		if !row.valid() {
			return nil, fmt.Errorf("enforce: row %q is outside the closed namespaces or resource kinds", key)
		}
	}
	if service := cfg.Switch.Service(); service != "" && service != authzenforce.GoAPI {
		return nil, errors.New("enforce: the switch is not go-api's")
	}
	e := &Enforcer{decider: cfg.Decider, rows: cfg.Rows, sw: cfg.Switch, timeout: cfg.Timeout, logger: cfg.Logger}
	if e.timeout <= 0 {
		e.timeout = enforceDefaultTimeout
	}
	if e.logger == nil {
		e.logger = slog.Default()
	}
	return e, nil
}

type enforceVerdict int

const (
	// enforceNotCovered: no row names an action for the route, or its namespace is off. The legacy guard decides.
	enforceNotCovered enforceVerdict = iota
	// enforceLegacy: the cell is on but the decision point cannot judge this caller. The legacy guard decides.
	enforceLegacy
	enforceAllow
	enforceDeny
	// enforceUnavailable: no verdict could be had. The route is refused with a 503.
	enforceUnavailable
)

// enforceObservation is what the guard tells the Enforcer about one authenticated request. It holds values only.
type enforceObservation struct {
	pattern         string
	bearer          string
	orgID           string
	userAccessToken bool
	impersonating   bool
}

func (e *Enforcer) decide(ctx context.Context, o enforceObservation) enforceVerdict {
	if e == nil {
		return enforceNotCovered
	}
	row, ok := e.rows[o.pattern]
	if !ok || !e.sw.On(row.Namespace) {
		return enforceNotCovered
	}
	org := strings.ToLower(o.orgID)
	if o.bearer == "" || !o.userAccessToken || o.impersonating || (row.ResourceType == policycatalog.ScopeOrganization && org == "") {
		countEnforce(ctx, row.Namespace, enforceOutcomeUnjudgedLegacy)
		return enforceLegacy
	}
	request := pep.Request{PrincipalReference: o.bearer, Action: row.Action, ResourceType: row.ResourceType, ResourceKey: shadowPlatformKey}
	if row.ResourceType == policycatalog.ScopeOrganization {
		request.ResourceKey, request.OrganizationID = org, org
	}
	decision, failed := e.ask(ctx, request)
	if failed || decision.Local() {
		countEnforce(ctx, row.Namespace, enforceOutcomeErrorDeny)
		e.logger.WarnContext(ctx, "api enforcement: no verdict, the route is refused",
			slog.String("namespace", row.Namespace), slog.String("reason", string(decision.Reason())))
		return enforceUnavailable
	}
	if decision.Allowed() && !carriesObligation(decision.Obligations()) {
		countEnforce(ctx, row.Namespace, enforceOutcomeAllow)
		return enforceAllow
	}
	countEnforce(ctx, row.Namespace, enforceOutcomeDeny)
	return enforceDeny
}

// ask calls the decider and turns a panic into a failure.
func (e *Enforcer) ask(ctx context.Context, request pep.Request) (decision pep.Decision, failed bool) {
	defer func() {
		if recover() != nil {
			decision, failed = pep.Decision{}, true
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	return e.decider.Decide(ctx, request), false
}

// carriesObligation reports whether an allow carries an obligation. This guard enforces none, so an allow that
// carries one is a deny: an obligation it cannot carry out is not met.
func carriesObligation(o pep.Obligations) bool {
	if _, ok := o.TenantScope(); ok {
		return true
	}
	if _, ok := o.MinimumAssurance(); ok {
		return true
	}
	return o.RepositoryRestricted() || len(o.Redaction()) > 0 || o.StepUpRequired()
}

// pdpHooks are the two ways the guard asks the decision point: the shadow beside the legacy check, and the
// Enforcer that replaces it for the namespaces switched on. They share one field so the Guard type keeps its shape.
type pdpHooks struct {
	shadow   *Shadow
	enforcer *Enforcer
}

// SetEnforcer installs e beside the guard; nil is off. Call it once, before the guard serves.
func (g *Guard) SetEnforcer(e *Enforcer) { g.pdp.enforcer = e }

// enforceVerdict asks the Enforcer about one authenticated request.
func (g *Guard) enforceVerdict(r *http.Request, user *User, bearer string) enforceVerdict {
	if g.pdp.enforcer == nil {
		return enforceNotCovered
	}
	ctx := r.Context()
	return g.pdp.enforcer.decide(ctx, enforceObservation{pattern: r.Pattern, bearer: bearer, orgID: user.OrgID,
		userAccessToken: user.accessToken, impersonating: ImpersonationFrom(ctx) != nil})
}

// writeEnforceRefusal answers a request the decision point refused or could not judge.
func writeEnforceRefusal(w http.ResponseWriter, level Authz, verdict enforceVerdict) {
	if verdict == enforceUnavailable {
		WriteDetail(w, http.StatusServiceUnavailable, "Authorization service temporarily unavailable", http.Header{"Retry-After": []string{enforceRetryAfter}})
		return
	}
	WriteDetail(w, http.StatusForbidden, enforceDenyDetail(level), nil)
}

// enforceDenyDetail is the refusal a denied caller gets: the body the legacy guard gives at the same level, so a
// client sees no new shape; the Authenticated level has no legacy refusal, so it gets a plain one.
func enforceDenyDetail(level Authz) string {
	switch level {
	case Admin, AdminOrg:
		return "Admin access required"
	case Superuser:
		return "Superuser access required"
	}
	return "Access denied"
}
