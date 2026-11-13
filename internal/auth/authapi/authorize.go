package authapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
	"github.com/full-chaos/dev-health-ops/internal/auth/delegationinput"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// The policy decision point routes. Each mounts only when the endpoint-profile
// document holds its row.
const (
	PatternAuthorize      = "/v1/authorize"
	PatternAuthorizeBatch = "/v1/authorize:batch"
)

const (
	opAuthorize      = "authorize"
	opAuthorizeBatch = "authorize_batch"

	// reasonPrincipalReferenceInvalid refuses the user's token named by
	// principal_reference; invalid_credentials refuses the caller's own
	// workload token.
	reasonPrincipalReferenceInvalid = "principal_reference_invalid"
	reasonRequestTooLarge           = "request_too_large"

	maxAuthorizeBody      = 16 << 10
	maxAuthorizeBatchBody = 256 << 10
)

// DecisionSession is the session a principal reference names, as the decision
// transaction reads it.
type DecisionSession struct {
	Found       bool
	PrincipalID uuid.UUID
	// Live is true when the session is neither revoked nor expired at the
	// transaction's clock.
	Live bool
	// HasActor and HasDelegatedFrom are true when the row names a real actor
	// or a delegating session: an impersonation session.
	HasActor         bool
	HasDelegatedFrom bool
	// The delegation columns of the row, as the same read returned them.
	// ActorPrincipalID is uuid.Nil without an actor. OrganizationID is the
	// target organization of an impersonation session.
	ActorPrincipalID uuid.UUID
	OrganizationID   *uuid.UUID
	AMR              []string
	PermittedActions []string
	// BaseLive is true when the delegating session is neither revoked nor
	// expired at the transaction's clock; false when there is none.
	BaseLive bool
}

// ActorState is the real actor of a delegated session, read in the decision
// transaction.
type ActorState struct {
	// Active is true when the actor is an active user principal and its
	// public.users row is active.
	Active bool
	// CanImpersonate is true when the actor holds admin.impersonate now.
	CanImpersonate bool
	// Owner is true when the actor holds platform_owner now.
	Owner bool
}

// serviceAccountKeyClass is the credential class of a service-account key
// (serviceaccount.KeyClass).
const serviceAccountKeyClass = "service_account_key"

// DecisionKey is the service-account key row a reference names, as the
// decision transaction reads it.
type DecisionKey struct {
	Found       bool
	PrincipalID uuid.UUID
	// AccountOrgID is the organization of the key principal's service-account
	// row; "" when the principal has none.
	AccountOrgID string
	Class        string
	DomainOwner  string
	Revoked      bool
	// ExpiresAtUnixNano is 0 when the row has no expiry.
	ExpiresAtUnixNano int64
	// NowUnixNano is the transaction's clock.
	NowUnixNano int64
}

// liveFor reports a key row that may stand for the reference: a platform
// service-account key of the referenced principal, whose account is in the
// organization the token names, neither revoked nor expired at the
// transaction's clock. The account's own state is the decision's, not this
// check's.
func (k DecisionKey) liveFor(p ReferencedPrincipal) bool {
	return k.Found && k.PrincipalID == p.ID && k.Class == serviceAccountKeyClass && k.DomainOwner == "platform" &&
		k.AccountOrgID != "" && k.AccountOrgID == p.OrgID && !k.Revoked &&
		k.NowUnixNano > 0 && k.ExpiresAtUnixNano != 0 && k.NowUnixNano < k.ExpiresAtUnixNano
}

// delegated reports an impersonation session: it is decided by the core's
// delegation rules over the row's own columns, never as the plain target user.
func (s DecisionSession) delegated() bool { return s.HasActor || s.HasDelegatedFrom }

// delegatedSnapshot is the effective subject's snapshot with the actor chain of
// the delegated session. An empty chain denies every request with
// delegation_invalid.
type delegatedSnapshot struct {
	authorization.GrantSnapshot
	del authorization.Delegation
}

func (d delegatedSnapshot) RepositoryID(orgID, path string) (string, bool) {
	if ri, ok := d.GrantSnapshot.(authorization.RepositoryIdentities); ok {
		return ri.RepositoryID(orgID, path)
	}
	return "", false
}

func (d delegatedSnapshot) PathForID(orgID, id string) (string, bool) {
	if ri, ok := d.GrantSnapshot.(authorization.RepositoryIdentities); ok {
		return ri.PathForID(orgID, id)
	}
	return "", false
}

func (d delegatedSnapshot) Delegation() (authorization.Delegation, bool) { return d.del, true }

// DecisionTx reads decision state inside one read-only transaction. Every
// error means the state could not be read.
type DecisionTx interface {
	Session(ctx context.Context, sessionID uuid.UUID) (DecisionSession, error)
	// Actor is the live state of the real actor of a delegated session; the
	// zero ActorState for an unknown principal.
	Actor(ctx context.Context, actor uuid.UUID) (ActorState, error)
	// ServiceAccountKey reads the key row a service-account reference names,
	// with the transaction's clock.
	ServiceAccountKey(ctx context.Context, credentialID uuid.UUID) (DecisionKey, error)
	// Snapshot is the principal's state for one organization ("" for a
	// platform decision), with the transaction's clock.
	Snapshot(ctx context.Context, principal uuid.UUID, orgID string) (authorization.GrantSnapshot, error)
	// Unfed is what no feed event covers, with the transaction's clock: read
	// on every call while the decision cache is in use.
	Unfed(ctx context.Context, principal uuid.UUID, orgID string) (UnfedState, error)
	// StateVersion is a digest of the versions of the rows a decision of the
	// pair depends on, read on every call while the decision cache is in use:
	// a committed change is a new key before its feed event arrives.
	StateVersion(ctx context.Context, principal uuid.UUID, orgID string) (string, error)
}

// DecisionState runs fn in one READ ONLY REPEATABLE READ transaction, so every
// read of one call sees one snapshot. An error opening or holding the
// transaction is returned as is; fn's error is returned unchanged.
type DecisionState interface {
	Read(ctx context.Context, fn func(DecisionTx) error) error
}

// AuthorizeDeps are the authorize routes' collaborators.
type AuthorizeDeps struct {
	Callers    *AuthorizeCallers
	References *PrincipalReferences
	State      DecisionState
	Evaluator  *authorization.Evaluator
	// Audit writes the audit rows of one call's high-risk decisions in one
	// transaction, all or none.
	Audit func(ctx context.Context, events []audit.AuditEvent) error
	// ProfileRows is the endpoint-profile document a route's row is looked up
	// in; nil reads the embedded document.
	ProfileRows []byte
	Logger      *slog.Logger
	Meter       metric.Meter
	// Cache is the decision cache, nil without the revision feed. It is asked
	// only while Ready reports the feed confirmed and caught up.
	Cache DecisionCache
	Ready func() bool
	// CallerLimit is one caller workload's bucket on each PDP route, per
	// process; a batch costs its decision count. Zero sets none.
	CallerLimit PDPCallerLimit
}

type authorizeHandlers struct {
	AuthorizeDeps
	rec        recorder
	cache      cacheMetrics
	decisionID func() (string, error)
	limiters   map[string]*callerLimiter
	// resolve, when set, resolves a principal reference in place of
	// References.Resolve; only tests set it.
	resolve func(reference string) (ReferencedPrincipal, string, error)
}

func (h authorizeHandlers) resolveReference(reference, caller string) (ReferencedPrincipal, string, error) {
	if h.resolve != nil {
		return h.resolve(reference)
	}
	return h.References.ResolveFor(reference, caller)
}

// AuthorizeRoutes returns the routes whose endpoint-profile row admits the
// workload token; a route without its row is not mounted and answers 404.
// Each registration is on a line of its own.
func AuthorizeRoutes(deps AuthorizeDeps) ([]httpapi.Route, error) {
	if deps.State == nil || deps.Evaluator == nil {
		return nil, errors.New("authapi: the authorize routes need decision state and an evaluator")
	}
	if deps.Audit == nil {
		return nil, errors.New("authapi: the authorize routes need an audit sink for high-risk decisions")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	meter := deps.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/authapi")
	}
	rec, err := newRecorder(deps.Logger, meter)
	if err != nil {
		return nil, err
	}
	rows := deps.ProfileRows
	if rows == nil {
		rows = authv1.AuthServiceEndpointProfiles
	}
	cache, err := newCacheMetrics(meter, deps.Cache)
	if err != nil {
		return nil, err
	}
	subjects := deps.Callers.callerSubjects()
	h := authorizeHandlers{AuthorizeDeps: deps, rec: rec, cache: cache, decisionID: newDecisionID, limiters: map[string]*callerLimiter{
		opAuthorize:             newCallerLimiter(pdpAuthorizeCallerLimitID, subjects, deps.CallerLimit),
		opAuthorizeBatch:        newCallerLimiter(pdpAuthorizeBatchCallerLimitID, subjects, deps.CallerLimit),
		opAuthorizeRepositories: newCallerLimiter(pdpAuthorizeRepositoriesCallerLimitID, subjects, deps.CallerLimit),
	}}
	var routes []httpapi.Route
	for _, route := range []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternAuthorize, Handler: http.HandlerFunc(h.authorize), MaxBodyBytes: maxAuthorizeBody, Admit: h.admit(opAuthorize)},
		{Method: http.MethodPost, Pattern: PatternAuthorizeBatch, Handler: http.HandlerFunc(h.authorizeBatch), MaxBodyBytes: maxAuthorizeBatchBody, Admit: h.admit(opAuthorizeBatch)},
		{Method: http.MethodPost, Pattern: PatternAuthorizeRepositories, Handler: http.HandlerFunc(h.authorizeRepositories), MaxBodyBytes: maxAuthorizeBody, Admit: h.admit(opAuthorizeRepositories)},
	} {
		if rowsAcceptFor(rows, []string{route.Pattern}, []string{workloadTokenClass}) != nil {
			h.Logger.Info("auth authorize route not mounted: no endpoint-profile row", "route", route.Pattern)
			continue
		}
		routes = append(routes, route)
	}
	return routes, nil
}

func (h authorizeHandlers) authorize(w http.ResponseWriter, r *http.Request) {
	decisions, ok := h.decide(w, r, opAuthorize, maxAuthorizeBody, func(body []byte) ([]AuthorizationQuestion, error) {
		q, err := DecodeAuthorizationRequest(body)
		return []AuthorizationQuestion{q}, err
	})
	if !ok {
		return
	}
	h.rec.record(r.Context(), opAuthorize, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, decisions[0])
}

func (h authorizeHandlers) authorizeBatch(w http.ResponseWriter, r *http.Request) {
	decisions, ok := h.decide(w, r, opAuthorizeBatch, maxAuthorizeBatchBody, DecodeAuthorizationBatchRequest)
	if !ok {
		return
	}
	doc := batchDocument{SchemaVersion: authorizationBatchResponseSchema, Decisions: make([]batchDecision, len(decisions))}
	for i, d := range decisions {
		doc.Decisions[i] = batchDecision{Index: i, Decision: d}
	}
	h.rec.record(r.Context(), opAuthorizeBatch, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, doc)
}

// authorizeCallerKey carries the verified caller from the route's admission
// gate to the handler.
type authorizeCallerKey struct{}

const reasonCallerNotAdmitted = "caller_not_admitted"

// admit is the routes' admission gate: it authenticates the policy-enforcement
// point before any body byte is read and answers the refusal itself.
func (h authorizeHandlers) admit(op string) func(http.ResponseWriter, *http.Request) (*http.Request, bool) {
	return func(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
		ctx := r.Context()
		token, ok := bearerToken(r)
		if !ok {
			h.rec.refuse(w, r, op, unauthorized(reasonInvalidCredentials), nil)
			return nil, false
		}
		caller, why, err := h.Callers.Authenticate(token)
		if err != nil {
			h.rec.refuse(w, r, op, unavailable(reasonDependency), err)
			return nil, false
		}
		if why != "" {
			h.Logger.WarnContext(ctx, "authorize refused: caller", "refusal", why, "operation", op)
			h.rec.refuse(w, r, op, unauthorized(reasonInvalidCredentials), nil)
			return nil, false
		}
		if !h.chargeCaller(w, r, op, caller.Subject, 1) {
			return nil, false
		}
		return r.WithContext(context.WithValue(ctx, authorizeCallerKey{}, caller)), true
	}
}

var (
	errAuthorizeCallerNotAdmitted = errors.New("authapi: the authorize handler was reached without an admitted caller")
	errAuthorizeReferenceRefused  = errors.New("authapi: a principal reference names no live session of its principal")
	errAuthorizeStateFailure      = errors.New("authapi: the decision core could not read its state")
)

type authorizeItem struct {
	question  AuthorizationQuestion
	principal ReferencedPrincipal
	// listing is true on the repository listing route: the item is decided by
	// Evaluator.ReadableRepositories and never through the decision cache.
	listing   bool
	decision  authorization.Decision
	now       int64
	delegated bool
	// actor and session are the real actor (uuid.Nil when the row names none)
	// and the delegated session of a delegated item: its audit row's ids.
	actor   uuid.UUID
	session uuid.UUID
	// orgKnown is true when the question's organization is a canonical uuid
	// the decision transaction found.
	orgKnown bool
	// cache is the item's lookup result; key and read are set on a miss whose
	// snapshot said what it read.
	cache string
	key   decisioncache.Key
	read  *decisioncache.Read
}

// decide authenticates the caller, decodes the body, resolves every principal
// reference, then decides every item in one state transaction. It answers
// every refusal itself and returns false. The items are decided apart: one
// item's decision never changes another's.
func (h authorizeHandlers) decide(w http.ResponseWriter, r *http.Request, op string, limit int64,
	decode func([]byte) ([]AuthorizationQuestion, error)) ([]decisionDocument, bool) {
	ctx := r.Context()
	caller, admitted := ctx.Value(authorizeCallerKey{}).(AuthorizeCaller)
	if !admitted {
		// The route's admission gate did not run: fail closed, and never verify
		// the bearer here.
		h.Logger.ErrorContext(ctx, "authorize handler reached without an admitted caller", "operation", op)
		h.rec.record(ctx, op, "error", reasonCallerNotAdmitted, errAuthorizeCallerNotAdmitted)
		w.WriteHeader(http.StatusInternalServerError)
		return nil, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			// The transport's own 413 (transport-error.v1), as for a body whose
			// Content-Length is over the route's bound.
			h.rec.record(ctx, op, "denied", reasonRequestTooLarge, nil)
			httpapi.WriteError(w, r, httpapi.CodePayloadTooLarge)
			return nil, false
		}
		h.rec.refuse(w, r, op, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return nil, false
	}
	questions, err := decode(body)
	if err != nil {
		h.rec.refuse(w, r, op, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return nil, false
	}
	// The admission gate charged one token; a batch costs its decision count.
	if len(questions) > 1 && !h.chargeCaller(w, r, op, caller.Subject, len(questions)-1) {
		return nil, false
	}
	items := make([]authorizeItem, len(questions))
	resolved := map[string]ReferencedPrincipal{}
	for i, q := range questions {
		p, seen := resolved[q.PrincipalReference]
		if !seen {
			var why string
			p, why, err = h.resolveReference(q.PrincipalReference, caller.Subject)
			if err != nil {
				h.rec.refuse(w, r, op, unavailable(reasonDependency), err)
				return nil, false
			}
			if why != "" {
				h.Logger.WarnContext(ctx, "authorize refused: principal reference", "refusal", why, "operation", op)
				h.rec.refuse(w, r, op, unauthorized(reasonPrincipalReferenceInvalid), nil)
				return nil, false
			}
			resolved[q.PrincipalReference] = p
		}
		if p.Kind == ReferenceWebApprover && !webApproverBinds(op, q, p) {
			h.Logger.WarnContext(ctx, "authorize refused: principal reference", "refusal", "approver_binding", "operation", op)
			h.rec.refuse(w, r, op, unauthorized(reasonPrincipalReferenceInvalid), nil)
			return nil, false
		}
		items[i] = authorizeItem{question: q, principal: p, listing: op == opAuthorizeRepositories}
	}
	// The stamp is taken BEFORE the transaction's snapshot: an event the
	// snapshot misses is later than the stamp, so a Put of what it read misses.
	// A listing is never cached: its key would be one repository question's.
	cached := h.Cache != nil && h.Ready != nil && h.Ready() && op != opAuthorizeRepositories
	var stamp decisioncache.Stamp
	if cached {
		stamp = h.Cache.Begin()
	}
	err = h.State.Read(ctx, func(tx DecisionTx) error { return h.evaluate(ctx, tx, resolved, items, cached) })
	switch {
	case errors.Is(err, errAuthorizeReferenceRefused):
		h.rec.refuse(w, r, op, unauthorized(reasonPrincipalReferenceInvalid), nil)
		return nil, false
	case err != nil:
		h.rec.refuse(w, r, op, unavailable(reasonStoreUnavailable), err)
		return nil, false
	}
	for i := range items {
		h.cache.lookup(ctx, items[i].cache)
		if read := items[i].read; read != nil {
			read.Stamp = stamp
			h.cache.put(ctx, h.Cache.Put(items[i].key, items[i].decision, *read))
		}
	}
	out := make([]decisionDocument, len(items))
	for i, item := range items {
		doc, err := h.document(item)
		if err != nil {
			h.rec.refuse(w, r, op, unavailable(reasonDependency), err)
			return nil, false
		}
		h.Logger.InfoContext(ctx, "authorize decision", append([]any{decisionIDLogKey, doc.DecisionID, "allowed", doc.Allowed, "reason_code", doc.ReasonCode,
			"reason", string(item.decision.Reason), "cache", item.cache}, obligationLogAttrs(doc.Obligations)...)...)
		// The owner-exemption audit event is written by a follow-up change,
		// outside this read-only transaction; until then the exemption is only
		// this log line.
		if a := item.decision.Audit; a != nil {
			h.Logger.WarnContext(ctx, "authorize owner exemption", decisionIDLogKey, doc.DecisionID, "actor_principal_id", a.ActorPrincipalID,
				"organization_id", a.OrganizationID, "action", a.Action, "audit_reason", a.Reason)
		}
		out[i] = doc
	}
	if err := h.auditDecisions(ctx, items, out); err != nil {
		h.rec.refuse(w, r, op, unavailable(reasonStoreUnavailable), err)
		return nil, false
	}
	return out, true
}

// referencedSession is a live session a principal reference names, with its
// actor when it is a delegated session.
type referencedSession struct {
	id    uuid.UUID
	row   DecisionSession
	actor ActorState
}

// evaluate checks each referenced session once (and reads the actor of a
// delegated one once), or a service account's key row once, then decides each
// item over its (principal, organization, session) snapshot, read once per
// triple. A delegated session is live only while its delegating session is.
// With the cache in use, an item of a user session that is not delegated reads
// its unfed values and asks the cache first; a hit reads no snapshot. A
// delegated session is never cached. A service account's decision never uses
// the cache: its account row is in no cache key and no feed dependency.
func (h authorizeHandlers) evaluate(ctx context.Context, tx DecisionTx, resolved map[string]ReferencedPrincipal, items []authorizeItem,
	cached bool) error {
	sessions := map[string]referencedSession{}
	accountRefs := map[string]bool{}
	for ref, p := range resolved {
		if p.Kind == ReferenceServiceAccount {
			k, err := tx.ServiceAccountKey(ctx, p.CredentialID)
			if err != nil {
				return err
			}
			if !k.liveFor(p) {
				return errAuthorizeReferenceRefused
			}
			accountRefs[ref] = true
			continue
		}
		sid, err := session.ParseWireSessionID(p.SessionID)
		if err != nil {
			return errAuthorizeReferenceRefused
		}
		s, err := tx.Session(ctx, sid)
		if err != nil {
			return err
		}
		if !s.Found || !s.Live || s.PrincipalID != p.ID || (s.delegated() && !s.BaseLive) {
			return errAuthorizeReferenceRefused
		}
		rs := referencedSession{id: sid, row: s}
		if s.delegated() && s.ActorPrincipalID != uuid.Nil {
			if rs.actor, err = tx.Actor(ctx, s.ActorPrincipalID); err != nil {
				return err
			}
		}
		sessions[ref] = rs
	}
	type triple struct {
		principal uuid.UUID
		org       string
		session   uuid.UUID
		account   bool
	}
	snapshots := map[triple]authorization.GrantSnapshot{}
	unfed := map[triple]UnfedState{}
	versions := map[triple]string{}
	for i := range items {
		q := items[i].question
		rs := sessions[q.PrincipalReference]
		delegated := rs.row.delegated()
		account := accountRefs[q.PrincipalReference]
		key := triple{principal: items[i].principal.ID, org: q.OrganizationID, account: account}
		if delegated {
			key.session = rs.id
		}
		items[i].cache = cacheBypass
		// A web approver reference is decided on the live session every call, never from the cache.
		if cached && !delegated && !account && items[i].principal.Kind != ReferenceWebApprover {
			u, ok := unfed[key]
			if !ok {
				var err error
				if u, err = tx.Unfed(ctx, key.principal, key.org); err != nil {
					return err
				}
				unfed[key] = u
			}
			if u.NowUnixNano <= 0 {
				return errAuthorizeStateFailure
			}
			version, ok := versions[key]
			if !ok {
				var err error
				if version, err = tx.StateVersion(ctx, key.principal, key.org); err != nil {
					return err
				}
				versions[key] = version
			}
			items[i].key = cacheKey(items[i], u, version)
			if d, hit := h.Cache.Get(items[i].key); hit {
				items[i].decision, items[i].now, items[i].cache = d, u.NowUnixNano, cacheHit
				continue
			}
			items[i].cache = cacheMiss
		}
		snap, ok := snapshots[key]
		if !ok {
			var err error
			if snap, err = tx.Snapshot(ctx, key.principal, key.org); err != nil {
				return err
			}
			if delegated {
				snap = delegatedSnapshot{snap, delegationinput.Delegation(delegationinput.Facts{
					Actor:               rs.row.ActorPrincipalID,
					Organization:        rs.row.OrganizationID,
					AMR:                 rs.row.AMR,
					PermittedActions:    rs.row.PermittedActions,
					ActorMayImpersonate: rs.actor.Active && rs.actor.CanImpersonate,
					ActorOwner:          rs.actor.Owner,
				})}
			}
			snapshots[key] = snap
		}
		if snap.NowUnixNano() <= 0 {
			return errAuthorizeStateFailure
		}
		req := authorization.Request{Action: q.Action, ResourceKind: q.ResourceType, OrganizationID: q.OrganizationID}
		if q.ResourceType == "repository" {
			req.ResourceKey = q.ResourceKey
		}
		var d authorization.Decision
		if items[i].listing {
			d = h.Evaluator.ReadableRepositories(req, snap)
		} else {
			d = h.Evaluator.Evaluate(req, snap)
		}
		if _, onWire := wireReasonCode(d.Reason); !onWire || !obligationsFit(d, delegated, q.OrganizationID) {
			return errAuthorizeStateFailure
		}
		if items[i].cache == cacheMiss {
			// A decision_bound allow is good for TTL from its decision while
			// the cache (and so the feed) bounds it; otherwise it expires now.
			if d.Allowed && d.ExpiresAtUnixNano == 0 && decisionBound(q.Action, d) {
				d.ExpiresAtUnixNano = snap.NowUnixNano() + int64(decisioncache.TTL)
			}
			if ds, ok := snap.(DependencySnapshot); ok {
				items[i].read = &decisioncache.Read{Dependencies: ds.Dependencies(), SetTypes: ds.SetTypes()}
			}
		}
		items[i].decision, items[i].now, items[i].delegated = d, snap.NowUnixNano(), delegated
		if delegated {
			items[i].actor, items[i].session = rs.row.ActorPrincipalID, rs.id
		}
		items[i].orgKnown = knownOrganization(q.OrganizationID, snap)
	}
	return nil
}

// obligationsFit is the wire rule of obligations: every allow of an organization
// or repository question carries them with the question's tenant scope, a
// delegated one also its assurance level; a deny and a platform allow carry none.
func obligationsFit(d authorization.Decision, delegated bool, organizationID string) bool {
	if !d.Allowed || organizationID == "" {
		return d.Obligations == nil
	}
	o := d.Obligations
	return o != nil && o.TenantScope == organizationID && (!delegated || o.MinimumAssurance != "")
}

func (h authorizeHandlers) document(item authorizeItem) (decisionDocument, error) {
	id, err := logging.DrawKept(decisionIDLogKey, maxDecisionIDDraws, h.decisionID)
	if err != nil {
		return decisionDocument{}, err
	}
	code, _ := wireReasonCode(item.decision.Reason)
	doc := decisionDocument{
		SchemaVersion:  authorizationDecisionSchema,
		DecisionID:     id,
		Allowed:        item.decision.Allowed,
		ReasonCode:     code,
		PrincipalID:    session.WirePrincipalID(item.principal.ID),
		PolicyRevision: item.decision.PolicyRevision,
		ExpiresAt:      decisionExpiry(item.decision, item.now).Format(expiresAtLayout),
	}
	if item.delegated {
		// A delegated allow is used once: the impersonation can end at any time.
		doc.ExpiresAt = time.Unix(0, item.now).UTC().Format(expiresAtLayout)
	}
	if org := item.question.OrganizationID; org != "" {
		doc.OrganizationID = &org
	}
	if doc.Obligations, err = wireObligationsOf(item.decision, item.question.OrganizationID); err != nil {
		return decisionDocument{}, err
	}
	return doc, nil
}

// obligationLogAttrs is a bounded summary of the served obligations for the decision log line: the repository
// scope kind, the allowlist size, and the presence of redaction and of a minimum assurance. It names no key, class
// or level. A member the wire does not carry is left out, and a decision that serves no obligations gets none.
func obligationLogAttrs(o *wireObligations) []any {
	if o == nil {
		return nil
	}
	var attrs []any
	switch {
	case o.RepositoryScope != nil:
		attrs = append(attrs, "obligation_repository_scope", authorization.RepositoryScopeOrganization)
	case o.RepositoryAllowlist != nil:
		attrs = append(attrs, "obligation_repository_scope", "allowlist", "obligation_repository_allowlist_size", len(*o.RepositoryAllowlist))
	default:
		attrs = append(attrs, "obligation_repository_scope", "none")
	}
	if o.Redaction != nil {
		attrs = append(attrs, "obligation_redaction", true)
	}
	if o.MinimumAssurance != nil {
		attrs = append(attrs, "obligation_minimum_assurance", true)
	}
	return attrs
}
