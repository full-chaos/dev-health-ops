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

	enforceDefaultTimeout = 2 * time.Second
	enforceRetryAfter     = "5"

	enforceUnavailableDetail     = "Authorization service temporarily unavailable"
	enforceNoDecisionPointReason = "authz_enforce_no_decision_point"
)

// msgOrgContextRequired is the guard's refusal of a caller with no organization on a route that needs one.
const msgOrgContextRequired = "Organization context required"

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
var enforceOutcomes = []string{enforceOutcomeAllow, enforceOutcomeDeny, enforceOutcomeErrorDeny, enforceOutcomeImpersonationRefused}

// Enforcer lets the policy decision point decide the routes of the namespaces that are switched on. For such a
// route its answer replaces the legacy guard's: an allow passes, a deny refuses, and a failure of any kind refuses,
// because a guard that cannot get a verdict must not fall back to the check the switch replaced. An allow with an
// obligation go-api does not carry out (see obligationsCarriedOut) is such a failure. A caller with a live go-api
// impersonation row presents the admin's own bearer, which the decision point would judge as the admin, not as the
// delegation: its read of a route that is not a platform wrap keeps the legacy answer (served as the target
// organization, product rule 1) and any other request is refused, as the legacy platform wrap refuses it, both before
// any call, except on the stop route. A delegated session's own bearer is decided
// like any other. A token with no organization on an organization row is asked the one platform action the legacy
// guard admits it by, admin.orgs; on an AdminOrg row it is refused as the legacy guard refuses it, with no question
// (decideOrgless). The one exception is a caller the decision point cannot judge (a legacy HS256 token, a served
// organization that is no UUID, a delegated session's bearer with no organization, an impersonation read): that request
// keeps the legacy answer, which is the same answer the route gives with its cell off.
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
	// enforceDenyNoOrg: the decision point refused a platform row to a token with no organization. The route is refused
	// with the legacy guard's 403 for a caller with no organization (product rule 1: the legacy answer).
	enforceDenyNoOrg
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
	// platformWrap: the route is a platform wrap, whose legacy guard refuses a live row itself (Guard.wrap, Superuser).
	platformWrap bool
	// served: the organization a path or query parameter names for the handler to serve (servedOrgOf).
	served servedOrg
	// adminOr: the route is mounted WrapAdminOr: an organization admin of the token's organization passes too.
	adminOr bool
	// orgRequired: the route level is AdminOrg, which refuses a caller with no organization whoever it is.
	orgRequired bool
	// delegated: the bearer is a delegated session's (the token names an actor).
	delegated bool
	// orglessAction is the platform action the route's legacy guard admits a caller with no organization by (the wrap's
	// gate action, else admin.orgs; gate.orglessAction).
	orglessAction string
	// platform, when set, keeps the request's platform answers for HoldsPlatform.
	platform *platformDecisions
}

// adminOrOrgAction is the organization action that stands for the legacy Admin level on an admin-or row: only the
// owner and admin roles hold it.
const adminOrOrgAction = "members.manage"

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

// decide is decideOnce plus one production-level (Info) log line for a request that serves another organization than the
// token's, after the verdict: the route, the namespace, the organization the decision point was asked about, the organization
// the request names and the outcome. The named organization is request input and is logged quoted, never used.
func (e *Enforcer) decide(ctx context.Context, o enforceObservation) enforceVerdict {
	verdict := e.decideOnce(ctx, o)
	if e == nil || o.orgID == "" || o.served.against(o.orgID) != servedOrgOther {
		return verdict
	}
	outcome := ""
	switch verdict {
	case enforceAllow:
		outcome = enforceOutcomeAllow
	case enforceDeny, enforceDenyNoOrg:
		outcome = enforceOutcomeDeny
	case enforceUnavailable, enforceNoDecisionPoint:
		outcome = enforceOutcomeErrorDeny
	default:
		return verdict
	}
	e.logger.InfoContext(ctx, "api enforcement: a request that serves another organization than the token's was decided on the token organization",
		slog.String("route", o.pattern), slog.String("namespace", e.rows[o.pattern].Namespace),
		slog.String("decided_org", strings.ToLower(o.orgID)), slog.String("served_org", logSafe(o.served.raw)), slog.String("outcome", outcome))
	return verdict
}

// logSafe drops the line breaks of a request-supplied value so it cannot start a second log line.
func logSafe(value string) string {
	value = strings.ReplaceAll(value, "\n", "")
	return strings.ReplaceAll(value, "\r", "")
}

func (e *Enforcer) decideOnce(ctx context.Context, o enforceObservation) enforceVerdict {
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
		if o.read && !o.platformWrap {
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
	// A request that serves another organization than the token's is judged as the legacy guard judges it: the decision
	// point answers for the TOKEN organization, the route level's check, and the org scope and the handler keep admitting
	// the served one. Neither verdict widens the other: an allow for the token organization never widens what the scope or the
	// handler admit (product rule 1, D6773; CHAOS-10872 S2-parity). A token with no organization has none to judge: on an
	// organization row it is asked the platform action instead, whatever it serves (S1).
	if row.ResourceType == policycatalog.ScopeOrganization && o.orgID == "" && o.bearer != "" && o.userAccessToken {
		return e.decideOrgless(ctx, o, row)
	}
	org := strings.ToLower(o.orgID)
	if reason := unjudgedReason(o, org); reason != "" {
		countEnforceUnjudged(ctx, o.pattern, reason)
		e.logger.DebugContext(ctx, "api enforcement: caller not judged, the legacy answer stands",
			slog.String("route", o.pattern), slog.String("namespace", row.Namespace), slog.String("reason", reason))
		return enforceLegacy
	}
	if o.adminOr && row.ResourceType == policycatalog.ScopePlatform {
		return e.decideAdminOr(ctx, o, row, org)
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
	if decision.Allowed() && !obligationsCarriedOut(decision.Obligations(), request, e.noRepositoryDimension[o.pattern], o.read) {
		countEnforce(ctx, row.Namespace, enforceOutcomeErrorDeny)
		e.logger.WarnContext(ctx, "api enforcement: the allow carries an obligation go-api does not carry out, no verdict, the route is refused",
			slog.String("namespace", row.Namespace))
		return enforceUnavailable
	}
	if row.ResourceType == policycatalog.ScopePlatform && o.platform != nil {
		o.platform.seed(row.Action, decision.Allowed())
	}
	if decision.Allowed() {
		countEnforce(ctx, row.Namespace, enforceOutcomeAllow)
		return enforceAllow
	}
	countEnforce(ctx, row.Namespace, enforceOutcomeDeny)
	if row.ResourceType == policycatalog.ScopePlatform && org == "" {
		return enforceDenyNoOrg
	}
	return enforceDeny
}

// decideOrgless decides an organization row for a token with no organization (CHAOS-10872 S1, product rule 1). The
// legacy guard admits such a caller only if it holds the wrap's org-less action (Guard.wrap's org-less check: the gate's
// action, else admin.orgs) and refuses it on an AdminOrg row whoever it is, so the decision point is asked that one
// platform action and never the row's organization action, which needs an organization. An AdminOrg row refuses before any question: no
// verdict could change its answer, so it is no deny either. A delegated session's bearer keeps the legacy answer: the
// decision point refuses a delegated session every platform action, so judging it would narrow impersonation (F1).
func (e *Enforcer) decideOrgless(ctx context.Context, o enforceObservation, row ShadowRow) enforceVerdict {
	switch {
	case o.delegated:
		countEnforceUnjudged(ctx, o.pattern, enforceUnjudgedDelegatedOrgless)
		e.logger.DebugContext(ctx, "api enforcement: a delegated session's bearer with no organization keeps the legacy answer",
			slog.String("route", o.pattern), slog.String("namespace", row.Namespace), slog.String("reason", enforceUnjudgedDelegatedOrgless))
		return enforceLegacy
	case o.orgRequired:
		countEnforceUnjudged(ctx, o.pattern, enforceUnjudgedOrgRequired)
		e.logger.DebugContext(ctx, "api enforcement: a token with no organization on an AdminOrg row is refused as the legacy guard refuses it",
			slog.String("route", o.pattern), slog.String("namespace", row.Namespace), slog.String("reason", enforceUnjudgedOrgRequired))
		return enforceDenyNoOrg
	}
	var held bool
	var err error
	if o.platform != nil {
		held, err = o.platform.holds(ctx, o.orglessAction)
	} else {
		held, err = e.askPlatform(ctx, o.bearer, o.orglessAction)
	}
	if err != nil {
		countEnforce(ctx, row.Namespace, enforceOutcomeErrorDeny)
		e.logger.WarnContext(ctx, "api enforcement: no verdict for a token with no organization, the route is refused",
			slog.String("route", o.pattern), slog.String("namespace", row.Namespace))
		return enforceUnavailable
	}
	if held {
		countEnforce(ctx, row.Namespace, enforceOutcomeAllow)
		return enforceAllow
	}
	countEnforce(ctx, row.Namespace, enforceOutcomeDeny)
	return enforceDenyNoOrg
}

// decideAdminOr decides an admin-or row with one batch (adminOrRequests, read by adminOrAnswer): either allow passes;
// no verdict is a 503. A refusal of a token with no organization is the legacy guard's own 403 for it. The platform
// answer is kept for the request: a handler that acts cross-org asks HoldsPlatform, never the organization answer.
func (e *Enforcer) decideAdminOr(ctx context.Context, o enforceObservation, row ShadowRow, org string) enforceVerdict {
	requests := adminOrRequests(o.bearer, row.Action, org)
	decisions, failed := e.askBatch(ctx, requests)
	allowed, decision, verdict := adminOrAnswer(requests, decisions, e.noRepositoryDimension[o.pattern], o.read)
	if failed || !verdict {
		countEnforce(ctx, row.Namespace, enforceOutcomeErrorDeny)
		e.logger.WarnContext(ctx, "api enforcement: no verdict for an admin-or row, the route is refused",
			slog.String("route", o.pattern), slog.String("reason", string(decision.Reason())))
		return enforceUnavailable
	}
	if o.platform != nil {
		o.platform.seed(row.Action, decisions[0].Allowed())
	}
	switch {
	case allowed:
		countEnforce(ctx, row.Namespace, enforceOutcomeAllow)
		return enforceAllow
	case org == "":
		countEnforce(ctx, row.Namespace, enforceOutcomeDeny)
		return enforceDenyNoOrg
	}
	countEnforce(ctx, row.Namespace, enforceOutcomeDeny)
	return enforceDeny
}

// adminOrRequests is the one batch an admin-or row asks: the row's platform action, and adminOrOrgAction in the
// token's organization when there is one. The Enforcer and the shadow both ask it.
func adminOrRequests(bearer, action, org string) []pep.Request {
	requests := []pep.Request{{PrincipalReference: bearer, Action: action, ResourceType: policycatalog.ScopePlatform, ResourceKey: shadowPlatformKey}}
	if org != "" {
		requests = append(requests, pep.Request{PrincipalReference: bearer, Action: adminOrOrgAction,
			ResourceType: policycatalog.ScopeOrganization, ResourceKey: org, OrganizationID: org})
	}
	return requests
}

// adminOrAnswer reads the answers to an admin-or batch: either allow passes. verdict is false when there is none: a
// missing answer, a local one, or an allow whose obligations go-api does not carry out. decision is the answer that
// decides (the first allow, else the platform answer), or the one that has no verdict.
func adminOrAnswer(requests []pep.Request, decisions []pep.Decision, noRepositoryDimension, read bool) (allowed bool, decision pep.Decision, verdict bool) {
	if len(requests) == 0 || len(decisions) != len(requests) {
		return false, pep.Decision{}, false
	}
	decision = decisions[0]
	for i, d := range decisions {
		if d.Local() || (d.Allowed() && !obligationsCarriedOut(d.Obligations(), requests[i], noRepositoryDimension, read)) {
			return false, d, false
		}
		if d.Allowed() && !allowed {
			allowed, decision = true, d
		}
	}
	return allowed, decision, true
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

// askBatch calls the decider's batch and turns a panic into a failure.
func (e *Enforcer) askBatch(ctx context.Context, requests []pep.Request) (decisions []pep.Decision, failed bool) {
	defer func() {
		if recover() != nil {
			decisions, failed = nil, true
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	return e.decider.DecideBatch(ctx, requests), false
}

// unjudgedReason says why the decision point cannot judge a request, or "" when it can. A token that is not a user
// access token comes first: nothing else can be said of it. A served organization that parses as no UUID is no_org:
// its handler refuses it.
func unjudgedReason(o enforceObservation, org string) string {
	switch {
	case o.bearer == "" || !o.userAccessToken:
		return enforceUnjudgedHS256
	case o.served.against(org) == servedOrgUnparsed:
		return enforceUnjudgedNoOrg
	}
	return ""
}

// obligationsCarriedOut reports whether go-api carries out every obligation of an allow for request. A tenant scope
// is carried out when it is the request's organization; a repository allowlist or the organization repository
// scope only on a route with no repository dimension; a minimum assurance of aal1 on a read only (every session
// go-api accepts is aal1, session.AssuranceOf); redaction, another assurance level and step-up never. An allow with
// one obligation not carried out is no verdict: it is neither served nor a deny.
func obligationsCarriedOut(o pep.Obligations, request pep.Request, noRepositoryDimension, read bool) bool {
	if tenant, ok := o.TenantScope(); ok && tenant != request.OrganizationID {
		return false
	}
	if (o.RepositoryRestricted() || o.RepositoryScopeOrganization()) && !noRepositoryDimension {
		return false
	}
	if level, ok := o.MinimumAssurance(); ok && (level != pep.AssuranceAAL1 || !read) {
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
func (g *Guard) enforceVerdict(r *http.Request, user *User, bearer string, level Authz, platformRoute bool, gt gate, platform *platformDecisions) enforceVerdict {
	if g.pdp.enforcer == nil {
		return enforceNotCovered
	}
	ctx := r.Context()
	impersonating := ImpersonationFrom(ctx) != nil
	// The session context is set for a holder of admin.impersonate only. On an enforced write or platform row the live
	// row decides for any caller, as the guard's own refusals do (Guard.sessionRow): a caller whose admin.impersonate
	// was revoked while its row is live is refused too. A read of any other row reads no row (the read limit).
	if namespace, enforced := g.pdp.enforcer.decides(r.Pattern); enforced && !impersonating && !liveRowExempt(r.Pattern) &&
		(platformRoute || !readMethod(r.Method)) {
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
		userAccessToken: user.accessToken, impersonating: impersonating, read: readMethod(r.Method), platformWrap: platformRoute,
		served: servedOrgOf(r, g.pdp.enforcer.rows[r.Pattern]), adminOr: gt.kind == gateAdminOr, orgRequired: level == AdminOrg,
		delegated: user.ImpersonatedBy != nil, orglessAction: gt.orglessAction(), platform: platform})
	// The refusal returns before the legacy checks that observe the shadow, so the shadow is told here: it counts
	// impersonation_refused without a decision point call.
	if verdict == enforceImpersonationRefused {
		g.observeShadowAs(r, user, bearer, false, true, gt)
	}
	return verdict
}

// writeEnforceRefusal answers a request the decision point refused or could not judge, or that no decision point can.
func writeEnforceRefusal(w http.ResponseWriter, r *http.Request, level Authz, verdict enforceVerdict, deny DenyText) {
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
		deny.refused(r)
		WriteDetail(w, http.StatusForbidden, deny.impersonated(), nil)
		return
	case enforceSessionRowUnavailable:
		WriteAuthFailure(w, "", ErrUnavailable)
		return
	case enforceDenyNoOrg:
		WriteDetail(w, http.StatusForbidden, msgOrgContextRequired, nil)
		return
	}
	if verdict == enforceDeny {
		deny.refused(r)
	}
	WriteDetail(w, http.StatusForbidden, deny.denied(enforceDenyDetail(level)), nil)
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
