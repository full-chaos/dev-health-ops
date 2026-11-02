package authapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
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

// liveFor reports a key row that may stand for the reference.
func (k DecisionKey) liveFor(ReferencedPrincipal) bool { return false }

// delegated reports an impersonation session. The decision core's delegation
// rules are not wired to this route yet, so such a reference is denied with
// delegation_invalid instead of being decided as the plain target user.
func (s DecisionSession) delegated() bool { return s.HasActor || s.HasDelegatedFrom }

// delegatedSnapshot makes the decision core deny every request with
// delegation_invalid: a delegation with an empty actor chain.
type delegatedSnapshot struct{ authorization.GrantSnapshot }

func (delegatedSnapshot) Delegation() (authorization.Delegation, bool) {
	return authorization.Delegation{}, true
}

// DecisionTx reads decision state inside one read-only transaction. Every
// error means the state could not be read.
type DecisionTx interface {
	Session(ctx context.Context, sessionID uuid.UUID) (DecisionSession, error)
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
}

type authorizeHandlers struct {
	AuthorizeDeps
	rec        recorder
	cache      cacheMetrics
	decisionID func() (string, error)
	// resolve, when set, resolves a principal reference in place of
	// References.Resolve; only tests set it.
	resolve func(reference string) (ReferencedPrincipal, string, error)
}

func (h authorizeHandlers) resolveReference(reference string) (ReferencedPrincipal, string, error) {
	if h.resolve != nil {
		return h.resolve(reference)
	}
	return h.References.Resolve(reference)
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
	h := authorizeHandlers{AuthorizeDeps: deps, rec: rec, cache: cache, decisionID: newDecisionID}
	var routes []httpapi.Route
	for _, route := range []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternAuthorize, Handler: http.HandlerFunc(h.authorize), MaxBodyBytes: maxAuthorizeBody, Admit: h.admit(opAuthorize)},
		{Method: http.MethodPost, Pattern: PatternAuthorizeBatch, Handler: http.HandlerFunc(h.authorizeBatch), MaxBodyBytes: maxAuthorizeBatchBody, Admit: h.admit(opAuthorizeBatch)},
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
	decision  authorization.Decision
	now       int64
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
	if _, admitted := ctx.Value(authorizeCallerKey{}).(AuthorizeCaller); !admitted {
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
	items := make([]authorizeItem, len(questions))
	resolved := map[string]ReferencedPrincipal{}
	for i, q := range questions {
		p, seen := resolved[q.PrincipalReference]
		if !seen {
			var why string
			p, why, err = h.resolveReference(q.PrincipalReference)
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
		items[i] = authorizeItem{question: q, principal: p}
	}
	// The stamp is taken BEFORE the transaction's snapshot: an event the
	// snapshot misses is later than the stamp, so a Put of what it read misses.
	cached := h.Cache != nil && h.Ready != nil && h.Ready()
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
		h.Logger.InfoContext(ctx, "authorize decision", "decision_id", doc.DecisionID, "allowed", doc.Allowed, "reason_code", doc.ReasonCode,
			"cache", item.cache)
		out[i] = doc
	}
	if err := h.auditHighRisk(ctx, items, out); err != nil {
		h.rec.refuse(w, r, op, unavailable(reasonStoreUnavailable), err)
		return nil, false
	}
	return out, true
}

// evaluate checks each referenced session once, then decides each item over
// its (principal, organization) snapshot, read once per pair. With the cache
// in use, an item of a session that is not delegated reads its unfed values
// and asks the cache first; a hit reads no snapshot.
func (h authorizeHandlers) evaluate(ctx context.Context, tx DecisionTx, resolved map[string]ReferencedPrincipal, items []authorizeItem,
	cached bool) error {
	delegatedRefs := map[string]bool{}
	for ref, p := range resolved {
		sid, err := session.ParseWireSessionID(p.SessionID)
		if err != nil {
			return errAuthorizeReferenceRefused
		}
		s, err := tx.Session(ctx, sid)
		if err != nil {
			return err
		}
		if !s.Found || !s.Live || s.PrincipalID != p.ID {
			return errAuthorizeReferenceRefused
		}
		delegatedRefs[ref] = s.delegated()
	}
	type pair struct {
		principal uuid.UUID
		org       string
		delegated bool
	}
	snapshots := map[pair]authorization.GrantSnapshot{}
	unfed := map[pair]UnfedState{}
	versions := map[pair]string{}
	for i := range items {
		q := items[i].question
		key := pair{items[i].principal.ID, q.OrganizationID, delegatedRefs[q.PrincipalReference]}
		items[i].cache = cacheBypass
		if cached && !key.delegated {
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
			if key.delegated {
				snap = delegatedSnapshot{snap}
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
		d := h.Evaluator.Evaluate(req, snap)
		if _, onWire := wireReasonCode(d.Reason); !onWire {
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
		items[i].decision, items[i].now = d, snap.NowUnixNano()
		items[i].orgKnown = knownOrganization(q.OrganizationID, snap)
	}
	return nil
}

func (h authorizeHandlers) document(item authorizeItem) (decisionDocument, error) {
	id, err := h.decisionID()
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
	if org := item.question.OrganizationID; org != "" {
		doc.OrganizationID = &org
	}
	return doc, nil
}
