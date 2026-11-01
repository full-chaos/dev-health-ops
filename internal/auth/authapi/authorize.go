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
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
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
}

// DecisionTx reads decision state inside one read-only transaction. Every
// error means the state could not be read.
type DecisionTx interface {
	Session(ctx context.Context, sessionID uuid.UUID) (DecisionSession, error)
	// Snapshot is the principal's state for one organization ("" for a
	// platform decision), with the transaction's clock.
	Snapshot(ctx context.Context, principal uuid.UUID, orgID string) (authorization.GrantSnapshot, error)
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
	// ProfileRows is the endpoint-profile document a route's row is looked up
	// in; nil reads the embedded document.
	ProfileRows []byte
	Logger      *slog.Logger
	Meter       metric.Meter
}

type authorizeHandlers struct {
	AuthorizeDeps
	rec        recorder
	decisionID func() (string, error)
}

// AuthorizeRoutes returns the routes whose endpoint-profile row admits the
// workload token; a route without its row is not mounted and answers 404.
// Each registration is on a line of its own.
func AuthorizeRoutes(deps AuthorizeDeps) ([]httpapi.Route, error) {
	if deps.State == nil || deps.Evaluator == nil {
		return nil, errors.New("authapi: the authorize routes need decision state and an evaluator")
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
	h := authorizeHandlers{AuthorizeDeps: deps, rec: rec, decisionID: newDecisionID}
	var routes []httpapi.Route
	for _, route := range []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternAuthorize, Handler: http.HandlerFunc(h.authorize), MaxBodyBytes: maxAuthorizeBody},
		{Method: http.MethodPost, Pattern: PatternAuthorizeBatch, Handler: http.HandlerFunc(h.authorizeBatch), MaxBodyBytes: maxAuthorizeBatchBody},
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

var (
	errAuthorizeReferenceRefused = errors.New("authapi: a principal reference names no live session of its principal")
	errAuthorizeStateFailure     = errors.New("authapi: the decision core could not read its state")
)

type authorizeItem struct {
	question  AuthorizationQuestion
	principal ReferencedPrincipal
	decision  authorization.Decision
	now       int64
}

// decide authenticates the caller, decodes the body, resolves every principal
// reference, then decides every item in one state transaction. It answers
// every refusal itself and returns false. The items are decided apart: one
// item's decision never changes another's.
func (h authorizeHandlers) decide(w http.ResponseWriter, r *http.Request, op string, limit int64,
	decode func([]byte) ([]AuthorizationQuestion, error)) ([]decisionDocument, bool) {
	ctx := r.Context()
	token, ok := bearerToken(r)
	if !ok {
		h.rec.refuse(w, r, op, unauthorized(reasonInvalidCredentials), nil)
		return nil, false
	}
	_, why, err := h.Callers.Authenticate(token)
	if err != nil {
		h.rec.refuse(w, r, op, unavailable(reasonDependency), err)
		return nil, false
	}
	if why != "" {
		h.Logger.WarnContext(ctx, "authorize refused: caller", "refusal", why, "operation", op)
		h.rec.refuse(w, r, op, unauthorized(reasonInvalidCredentials), nil)
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
			p, why, err = h.References.Resolve(q.PrincipalReference)
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
	err = h.State.Read(ctx, func(tx DecisionTx) error { return h.evaluate(ctx, tx, resolved, items) })
	switch {
	case errors.Is(err, errAuthorizeReferenceRefused):
		h.rec.refuse(w, r, op, unauthorized(reasonPrincipalReferenceInvalid), nil)
		return nil, false
	case err != nil:
		h.rec.refuse(w, r, op, unavailable(reasonStoreUnavailable), err)
		return nil, false
	}
	out := make([]decisionDocument, len(items))
	for i, item := range items {
		doc, err := h.document(item)
		if err != nil {
			h.rec.refuse(w, r, op, unavailable(reasonDependency), err)
			return nil, false
		}
		h.Logger.InfoContext(ctx, "authorize decision", "decision_id", doc.DecisionID, "allowed", doc.Allowed, "reason_code", doc.ReasonCode)
		out[i] = doc
	}
	return out, true
}

// evaluate checks each referenced session once, then decides each item over
// its (principal, organization) snapshot, read once per pair.
func (h authorizeHandlers) evaluate(ctx context.Context, tx DecisionTx, resolved map[string]ReferencedPrincipal, items []authorizeItem) error {
	for _, p := range resolved {
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
	}
	type pair struct {
		principal uuid.UUID
		org       string
	}
	snapshots := map[pair]authorization.GrantSnapshot{}
	for i := range items {
		q := items[i].question
		key := pair{items[i].principal.ID, q.OrganizationID}
		snap, ok := snapshots[key]
		if !ok {
			var err error
			if snap, err = tx.Snapshot(ctx, key.principal, key.org); err != nil {
				return err
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
		items[i].decision, items[i].now = d, snap.NowUnixNano()
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
