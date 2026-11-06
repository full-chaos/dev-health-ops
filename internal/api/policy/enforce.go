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
	enforceOutcomeAllow     = "allow"
	enforceOutcomeDeny      = "deny"
	enforceOutcomeErrorDeny = "error_deny"
	// enforceOutcomeImpersonationRefused: a caller with a live go-api impersonation row, refused before any call.
	enforceOutcomeImpersonationRefused = "impersonation_refused"
	// enforceOutcomeOrgMismatch: a served organization that is not the token's, refused before any call.
	enforceOutcomeOrgMismatch = "org_mismatch"

	enforceDefaultTimeout = 2 * time.Second
	enforceRetryAfter     = "5"

	enforceUnavailableDetail     = "Authorization service temporarily unavailable"
	enforceNoDecisionPointReason = "authz_enforce_no_decision_point"
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
	// Unconfigured is an Enforcer with no decision point client: go-api could not build one. It asks nothing and
	// refuses every request of a namespace switched on with a 503 and a reason code; Decider is nil.
	Unconfigured bool
	Logger       *slog.Logger
	// NoRepositoryDimension names the routes ("METHOD /route") that have no repository dimension. Nil is
	// goAPIRoutesWithoutRepositoryDimension.
	NoRepositoryDimension map[string]bool
}

// goAPIRoutesWithoutRepositoryDimension is the explicit list of go-api routes that have no repository dimension. A
// route not listed has one.
var goAPIRoutesWithoutRepositoryDimension = map[string]bool{}

// enforceOutcomes is the closed outcome set of the decision counter.
var enforceOutcomes = []string{enforceOutcomeAllow, enforceOutcomeDeny, enforceOutcomeErrorDeny, enforceOutcomeImpersonationRefused,
	enforceOutcomeOrgMismatch}

// Enforcer lets the policy decision point decide the routes of the namespaces that are switched on. For such a
// route its answer replaces the legacy guard's: an allow passes, a deny refuses, and a failure of any kind refuses,
// because a guard that cannot get a verdict must not fall back to the check the switch replaced. An allow with an
// obligation go-api does not carry out (see obligationsCarriedOut) is such a failure. A caller with a live go-api
// impersonation row presents the admin's own bearer, which the decision point would judge as the admin, not as the
// delegation: its read of a route that is not a platform wrap keeps the legacy answer (served as the target
// organization, product rule 1) and any other request is refused, as the legacy platform wrap refuses it, both before
// any call, except on the stop route. A delegated session's own bearer is decided
// like any other. The one exception is a caller the decision point cannot judge (a legacy HS256 token, an
// organization route with no organization, an impersonation read): that request keeps the legacy answer, which is the
// same answer the route gives with its cell off.
// Operator runbook: docs/operate/runbooks/auth-pdp-outage-and-rollback.md
type Enforcer struct {
	decider pep.Decider
	rows    map[string]ShadowRow
	sw      authzenforce.Switch
	timeout time.Duration
	logger  *slog.Logger
	// unconfigured: no decision point client; every request of a namespace switched on is refused.
	unconfigured bool
	// noRepositoryDimension: the routes with no repository dimension.
	noRepositoryDimension map[string]bool
}

// NewEnforcer checks cfg.
func NewEnforcer(cfg EnforceConfig) (*Enforcer, error) {
	if cfg.Decider == nil && !cfg.Unconfigured {
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
	noRepositoryDimension := cfg.NoRepositoryDimension
	if noRepositoryDimension == nil {
		noRepositoryDimension = goAPIRoutesWithoutRepositoryDimension
	}
	for key := range noRepositoryDimension {
		if _, ok := cfg.Rows[key]; !ok {
			return nil, fmt.Errorf("enforce: %q is flagged with no repository dimension but has no row", key)
		}
	}
	e := &Enforcer{decider: cfg.Decider, rows: cfg.Rows, sw: cfg.Switch, timeout: cfg.Timeout, logger: cfg.Logger, unconfigured: cfg.Unconfigured,
		noRepositoryDimension: noRepositoryDimension}
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
	// enforceNoDecisionPoint: go-api has no decision point client. The route is refused with a 503 and a reason code.
	enforceNoDecisionPoint
	// enforceImpersonationRefused: the caller has a live go-api impersonation row. The route is refused with a 403.
	enforceImpersonationRefused
	// enforceSessionRowUnavailable: the caller's go-api impersonation row could not be read. The route is refused with
	// the dependency failure 503.
	enforceSessionRowUnavailable
)

// enforceObservation is what the guard tells the Enforcer about one authenticated request. It holds values only.
type enforceObservation struct {
	pattern         string
	bearer          string
	orgID           string
	userAccessToken bool
	// impersonating: a live go-api impersonation row is in force for the caller's own bearer.
	impersonating bool
	// read: the request method writes nothing (readMethod, the legacy RefuseImpersonation's rule).
	read bool
	// platform: the route is a platform wrap, whose legacy guard refuses a live row itself (Guard.wrap, Superuser).
	platform bool
	// served: the organization a path or query parameter names for the handler to serve (servedOrgOf).
	served servedOrg
}

// decides reports whether the decision point decides pattern: its row is enforced and a decision point is configured.
func (e *Enforcer) decides(pattern string) (namespace string, ok bool) {
	if e == nil || e.unconfigured {
		return "", false
	}
	row, ok := e.rows[pattern]
	if !ok || !e.sw.On(row.Namespace) {
		return "", false
	}
	return row.Namespace, true
}

func (e *Enforcer) decide(ctx context.Context, o enforceObservation) enforceVerdict {
	if e == nil {
		return enforceNotCovered
	}
	row, ok := e.rows[o.pattern]
	if !ok || !e.sw.On(row.Namespace) {
		return enforceNotCovered
	}
	if e.unconfigured {
		countEnforce(ctx, row.Namespace, enforceOutcomeErrorDeny)
		return enforceNoDecisionPoint
	}
	if o.impersonating && !liveRowExempt(o.pattern) {
		if o.read && !o.platform {
			countEnforceUnjudged(ctx, o.pattern, enforceUnjudgedImpersonation)
			e.logger.DebugContext(ctx, "api enforcement: an impersonation read keeps the legacy answer",
				slog.String("route", o.pattern), slog.String("namespace", row.Namespace))
			return enforceLegacy
		}
		countEnforce(ctx, row.Namespace, enforceOutcomeImpersonationRefused)
		e.logger.WarnContext(ctx, "api enforcement: impersonation caller refused on an enforced row",
			slog.String("route", o.pattern), slog.String("namespace", row.Namespace))
		return enforceImpersonationRefused
	}
	// The handler must serve the organization the decision point judges; this holds for an unjudged caller too.
	if o.served.against(o.orgID) == servedOrgOther {
		countEnforce(ctx, row.Namespace, enforceOutcomeOrgMismatch)
		e.logger.WarnContext(ctx, "api enforcement: the request names an organization that is not the token organization, the route is refused",
			slog.String("route", o.pattern), slog.String("namespace", row.Namespace))
		return enforceDeny
	}
	org := strings.ToLower(o.orgID)
	if reason := unjudgedReason(o, row, org); reason != "" {
		countEnforceUnjudged(ctx, o.pattern, reason)
		e.logger.DebugContext(ctx, "api enforcement: caller not judged, the legacy answer stands",
			slog.String("route", o.pattern), slog.String("namespace", row.Namespace), slog.String("reason", reason))
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
	if decision.Allowed() && !obligationsCarriedOut(decision.Obligations(), request, e.noRepositoryDimension[o.pattern]) {
		countEnforce(ctx, row.Namespace, enforceOutcomeErrorDeny)
		e.logger.WarnContext(ctx, "api enforcement: the allow carries an obligation go-api does not carry out, no verdict, the route is refused",
			slog.String("namespace", row.Namespace))
		return enforceUnavailable
	}
	if decision.Allowed() {
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

// unjudgedReason says why the decision point cannot judge a request, or "" when it can. A token that is not a user
// access token comes first: nothing else can be said of it. A served organization that parses as no UUID is no_org:
// its handler refuses it.
func unjudgedReason(o enforceObservation, row ShadowRow, org string) string {
	switch {
	case o.bearer == "" || !o.userAccessToken:
		return enforceUnjudgedHS256
	case row.ResourceType == policycatalog.ScopeOrganization && org == "", o.served.against(org) == servedOrgUnparsed:
		return enforceUnjudgedNoOrg
	}
	return ""
}

// obligationsCarriedOut reports whether go-api carries out every obligation of an allow for request. A tenant scope
// is carried out when it is the request's organization; a repository allowlist or the organization repository
// scope only on a route with no repository dimension; redaction, minimum assurance and step-up never. An allow with
// one obligation not carried out is no verdict: it is neither served nor a deny.
func obligationsCarriedOut(o pep.Obligations, request pep.Request, noRepositoryDimension bool) bool {
	if tenant, ok := o.TenantScope(); ok && tenant != request.OrganizationID {
		return false
	}
	if (o.RepositoryRestricted() || o.RepositoryScopeOrganization()) && !noRepositoryDimension {
		return false
	}
	if _, ok := o.MinimumAssurance(); ok {
		return false
	}
	return len(o.Redaction()) == 0 && !o.StepUpRequired()
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
func (g *Guard) enforceVerdict(r *http.Request, user *User, bearer string, platform bool) enforceVerdict {
	if g.pdp.enforcer == nil {
		return enforceNotCovered
	}
	ctx := r.Context()
	impersonating := ImpersonationFrom(ctx) != nil
	// The session context is set for a holder of admin.impersonate only. On an enforced write or platform row the live
	// row decides for any caller, as the guard's own refusals do (Guard.sessionRow): a caller whose admin.impersonate
	// was revoked while its row is live is refused too. A read of any other row reads no row (the read limit).
	if namespace, enforced := g.pdp.enforcer.decides(r.Pattern); enforced && !impersonating && !liveRowExempt(r.Pattern) &&
		(platform || !readMethod(r.Method)) {
		session, err := g.sessionRow(r, user.ID)
		if err != nil {
			countEnforce(ctx, namespace, enforceOutcomeErrorDeny)
			g.logger.ErrorContext(ctx, "api enforcement: impersonation row lookup failed; refusing the request",
				slog.String("route", r.Pattern), slog.String("namespace", namespace), slog.Bool("unavailable", isUnavailable(err)),
				slog.String("missing_grant", MissingGrant(err)), slog.Any("error", err))
			// The session state is unknown: the shadow is not asked either.
			return enforceSessionRowUnavailable
		}
		impersonating = session != nil
	}
	verdict := g.pdp.enforcer.decide(ctx, enforceObservation{pattern: r.Pattern, bearer: bearer, orgID: user.OrgID,
		userAccessToken: user.accessToken, impersonating: impersonating, read: readMethod(r.Method), platform: platform,
		served: servedOrgOf(r, g.pdp.enforcer.rows[r.Pattern])})
	// The refusal returns before the legacy checks that observe the shadow, so the shadow is told here: it counts
	// impersonation_refused without a decision point call.
	if verdict == enforceImpersonationRefused {
		g.observeShadowAs(r, user, bearer, false, true)
	}
	return verdict
}

// writeEnforceRefusal answers a request the decision point refused or could not judge, or that no decision point can.
func writeEnforceRefusal(w http.ResponseWriter, level Authz, verdict enforceVerdict) {
	switch verdict {
	case enforceUnavailable:
		WriteDetail(w, http.StatusServiceUnavailable, enforceUnavailableDetail, http.Header{"Retry-After": []string{enforceRetryAfter}})
		return
	case enforceNoDecisionPoint:
		detail := ErrorDetail(enforceUnavailableDetail)
		detail.Set("reason_code", enforceNoDecisionPointReason)
		WriteDetail(w, http.StatusServiceUnavailable, detail, nil)
		return
	case enforceImpersonationRefused:
		WriteDetail(w, http.StatusForbidden, msgImpersonationDenied, nil)
		return
	case enforceSessionRowUnavailable:
		WriteAuthFailure(w, "", ErrUnavailable)
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
