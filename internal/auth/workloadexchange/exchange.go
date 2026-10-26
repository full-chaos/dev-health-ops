// Package workloadexchange is auth-service's workload-token exchange: a
// workload presents its Kubernetes projected ServiceAccount token, the
// Kubernetes TokenReview API authenticates it, the workload registry maps the
// reviewed identity to a registered workload, and the route answers a
// workload-token.v1 token for one audience. The route is internal-only and is
// mounted only when AUTH_WORKLOAD_EXCHANGE is on.
package workloadexchange

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// Pattern is the exchange route. The go-api proxy never forwards it: it
// forwards only /api/v1/auth routes.
const Pattern = "/internal/v1/token/exchange"

// ReviewAudience is the one audience a presented ServiceAccount token must be
// minted for. It is never the API server's own audience, so a pod's default
// API token cannot be exchanged, and a token for it is useless at the API
// server.
const ReviewAudience = "dev-health-workload-exchange"

// RFC 8693 request and response values.
const (
	GrantTypeTokenExchange = "urn:ietf:params:oauth:grant-type:token-exchange"
	TokenTypeJWT           = "urn:ietf:params:oauth:token-type:jwt"
)

// Request bounds.
const (
	MaxBodyBytes        = 8 << 10
	maxSubjectTokenSize = 4 << 10
	maxScopeActions     = 64
	maxActionSize       = 128
	maxAudienceSize     = 64
	// MaxInFlightReviews bounds the TokenReview calls in flight; one more
	// answers 503.
	MaxInFlightReviews = 16
	// ReviewCallTimeout bounds one TokenReview call end to end.
	ReviewCallTimeout = 5 * time.Second
)

// Identity is the reviewed federated identity the registry resolves. Namespace,
// ServiceAccountName and ServiceAccountUID come from the TokenReview response;
// TrustDomain and Issuer are this deployment's configuration. Nothing in it
// comes from the request.
type Identity struct {
	TrustDomain        string
	Issuer             string
	Namespace          string
	ServiceAccountName string
	ServiceAccountUID  string
}

// Registration is a resolved workload. Narrow is the registry's own narrowing
// (it never widens); this package never decides an audience or an action
// itself.
type Registration struct {
	PrincipalID uuid.UUID
	BindingID   uuid.UUID
	Revision    int64
	Audiences   []string
	Narrow      func(audience string, scope []string) ([]string, error)
}

// Registry resolves a reviewed identity (the workload store).
type Registry interface {
	Resolve(ctx context.Context, id Identity) (Registration, error)
}

// Signer signs a compact JWS with the active key and reads the key set the
// self-check verifies against (*token.Signer).
type Signer interface {
	Sign(ctx context.Context, typ string, payload []byte) (string, string, error)
	KeySet(ctx context.Context) (token.KeySet, error)
}

// Deps are the route's collaborators.
type Deps struct {
	// Validator authenticates the subject token through TokenReview
	// (NewReviewValidator).
	Validator authverify.SubjectTokenValidator
	Registry  Registry
	// RefusalReason names why Registry.Resolve refused, for the log line and
	// the counter only.
	RefusalReason func(error) string
	Signer        Signer
	// Audit writes the issued-token audit row (audit.Append).
	Audit func(ctx context.Context, event audit.AuditEvent) error
	// IssuerURL is the iss of every issued token.
	IssuerURL string
	// ClusterIssuer is the ServiceAccount issuer of the one cluster whose API
	// server reviews the tokens.
	ClusterIssuer string
	Logger        *slog.Logger
	// Meter registers the outcome counter; nil uses the process meter.
	Meter metric.Meter
	Now   func() time.Time
}

// Routes returns the exchange route. Its registration is on a line of its
// own: the endpoint-profile row anchors to it.
func Routes(deps Deps) ([]httpapi.Route, error) {
	if deps.Validator == nil || deps.Registry == nil || deps.RefusalReason == nil || deps.Signer == nil || deps.Audit == nil {
		return nil, errors.New("workloadexchange: validator, registry, refusal reason, signer and audit are required")
	}
	if !issuerURLPattern.MatchString(deps.IssuerURL) {
		return nil, errors.New("workloadexchange: the token issuer must be an https URL")
	}
	if strings.TrimSpace(deps.ClusterIssuer) == "" {
		return nil, errors.New("workloadexchange: the cluster issuer is required")
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	rec, err := newRecorder(deps.Logger, deps.Meter)
	if err != nil {
		return nil, err
	}
	h := &handler{Deps: deps, rec: rec, reviews: make(chan struct{}, MaxInFlightReviews)}
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: Pattern, Handler: h, MaxBodyBytes: MaxBodyBytes, ErrorWriter: h.transportError},
	}, nil
}

// issuerURLPattern is the iss shape the workload verifier accepts.
var issuerURLPattern = regexp.MustCompile(`^https://[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?(:[0-9]{1,5})?(/[A-Za-z0-9_~-][A-Za-z0-9._~-]{0,63}){0,8}$`)

var (
	invalidRequestBody = []byte(`{"error":"invalid_request"}`)
	invalidGrantBody   = []byte(`{"error":"invalid_grant"}`)
	unavailableBody    = []byte(`{"error":"temporarily_unavailable"}`)
)

type handler struct {
	Deps
	rec     recorder
	reviews chan struct{}
}

type exchangeRequest struct {
	subjectToken string
	audience     string
	scope        []string
}

type issuedAnswer struct {
	AccessToken     string `json:"access_token"`
	IssuedTokenType string `json:"issued_token_type"`
	TokenType       string `json:"token_type"`
	ExpiresIn       int64  `json:"expires_in"`
}

// ServeHTTP is POST /internal/v1/token/exchange. After the request shape is
// valid, every refusal answers the same 400 invalid_grant and every
// dependency failure the same 503; only the log line and the counter name the
// reason. The store is read only after TokenReview authenticated the caller.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, ok := parseRequest(r)
	if !ok {
		h.answer(w, r, outcomeInvalid, reasonInvalidRequest, audienceNone)
		return
	}
	audience := audienceLabel(req.audience)
	subject, reason := h.review(ctx, req.subjectToken)
	if reason != "" {
		outcome := outcomeRefused
		if reason != reasonSubjectTokenInvalid {
			outcome = outcomeUnavailable
		}
		h.answer(w, r, outcome, reason, audience)
		return
	}
	reg, err := h.Registry.Resolve(ctx, Identity{
		TrustDomain: subject.TrustDomain, Issuer: h.ClusterIssuer, Namespace: subject.Namespace,
		ServiceAccountName: subject.ServiceAccountName, ServiceAccountUID: subject.ServiceAccountUID,
	})
	if err != nil {
		h.answer(w, r, outcomeRefused, resolveReason(h.RefusalReason(err)), audience)
		return
	}
	if reg.Narrow == nil {
		h.answer(w, r, outcomeRefused, reasonResolveRefused, audience)
		return
	}
	if !isWorkloadAudience(req.audience) {
		h.answer(w, r, outcomeRefused, reasonAudienceRefused, audience)
		return
	}
	actions, err := reg.Narrow(req.audience, req.scope)
	if err != nil {
		reason := reasonScopeRefused
		if !slices.Contains(reg.Audiences, req.audience) {
			reason = reasonAudienceRefused
		}
		h.answer(w, r, outcomeRefused, reason, audience)
		return
	}
	now := h.Now()
	lifetime := int64(subject.ExpiresAt.Sub(now) / time.Second)
	if ceiling := int64(contracts.WorkloadTokenLifetimeCeiling / time.Second); lifetime > ceiling {
		lifetime = ceiling
	}
	if lifetime < 1 {
		h.answer(w, r, outcomeRefused, reasonLifetimeTooShort, audience)
		return
	}
	issued, jti, reason := h.mint(ctx, reg, req.audience, now, lifetime)
	if reason != "" {
		h.answer(w, r, outcomeUnavailable, reason, audience)
		return
	}
	principal := reg.PrincipalID.String()
	if actions == nil {
		actions = []string{}
	}
	if err := h.Audit(ctx, audit.AuditEvent{
		EventType: audit.EventWorkloadTokenIssued, Outcome: audit.OutcomeAllowed,
		SubjectPrincipalID: &principal, RequestID: requestid.Audit(ctx),
		Attributes: map[string]any{
			"jti": jti, "principal_id": principal, "binding_id": reg.BindingID.String(),
			"revision": reg.Revision, "audience": req.audience, "actions": actions,
		},
	}); err != nil {
		h.answer(w, r, outcomeUnavailable, reasonAuditUnavailable, audience)
		return
	}
	body, err := json.Marshal(issuedAnswer{
		AccessToken: issued, IssuedTokenType: TokenTypeJWT, TokenType: "Bearer", ExpiresIn: lifetime,
	})
	if err != nil {
		h.answer(w, r, outcomeUnavailable, reasonSignerUnavailable, audience)
		return
	}
	h.rec.record(ctx, outcomeIssued, reasonIssued, audience)
	writeJSON(w, http.StatusOK, body)
}

// review authenticates the subject token through TokenReview, at most
// MaxInFlightReviews at once. It returns subject_token_invalid for a token
// the API server does not vouch for and an unavailable reason for anything
// else: an outage never reaches the store.
func (h *handler) review(ctx context.Context, subjectToken string) (authverify.SubjectIdentity, string) {
	select {
	case h.reviews <- struct{}{}:
	default:
		return authverify.SubjectIdentity{}, reasonSaturated
	}
	defer func() { <-h.reviews }()
	reviewCtx, cancel := context.WithTimeout(ctx, ReviewCallTimeout)
	defer cancel()
	subject, err := h.Validator.Validate(reviewCtx, subjectToken)
	switch {
	case errors.Is(err, authverify.ErrSubjectTokenInvalid):
		return authverify.SubjectIdentity{}, reasonSubjectTokenInvalid
	case err != nil:
		return authverify.SubjectIdentity{}, reasonTokenReviewUnavailable
	}
	return subject, ""
}

func (h *handler) answer(w http.ResponseWriter, r *http.Request, outcome, reason, audience string) {
	h.rec.record(r.Context(), outcome, reason, audience)
	switch outcome {
	case outcomeInvalid:
		writeJSON(w, http.StatusBadRequest, invalidRequestBody)
	case outcomeRefused:
		writeJSON(w, http.StatusBadRequest, invalidGrantBody)
	default:
		writeJSON(w, http.StatusServiceUnavailable, unavailableBody)
	}
}

// transportError renders the server's refusals of this route: a body over
// MaxBodyBytes is a request shape fault; anything else keeps the server's
// envelope.
func (h *handler) transportError(w http.ResponseWriter, r *http.Request, code httpapi.Code) {
	if code == httpapi.CodePayloadTooLarge {
		h.answer(w, r, outcomeInvalid, reasonInvalidRequest, audienceNone)
		return
	}
	httpapi.WriteError(w, r, code)
}

func writeJSON(w http.ResponseWriter, status int, body []byte) {
	header := w.Header()
	header.Set("Content-Type", "application/json")
	header.Set("Cache-Control", "no-store")
	header.Set("Pragma", "no-cache")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

var requestFields = map[string]bool{
	"grant_type": true, "subject_token": true, "subject_token_type": true, "audience": true, "scope": true,
}

// parseRequest reads the RFC 8693 form: a closed field set, each field once,
// no query string, at most MaxBodyBytes.
func parseRequest(r *http.Request) (exchangeRequest, bool) {
	if r.URL.RawQuery != "" || r.Body == nil {
		return exchangeRequest{}, false
	}
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/x-www-form-urlencoded" {
		return exchangeRequest{}, false
	}
	for name, value := range params {
		if name != "charset" || !strings.EqualFold(value, "utf-8") {
			return exchangeRequest{}, false
		}
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, MaxBodyBytes+1))
	if err != nil || len(raw) > MaxBodyBytes {
		return exchangeRequest{}, false
	}
	values, err := url.ParseQuery(string(raw))
	if err != nil {
		return exchangeRequest{}, false
	}
	for name, list := range values {
		if !requestFields[name] || len(list) != 1 {
			return exchangeRequest{}, false
		}
	}
	if values.Get("grant_type") != GrantTypeTokenExchange || values.Get("subject_token_type") != TokenTypeJWT {
		return exchangeRequest{}, false
	}
	req := exchangeRequest{subjectToken: values.Get("subject_token"), audience: values.Get("audience")}
	if req.subjectToken == "" || len(req.subjectToken) > maxSubjectTokenSize ||
		req.audience == "" || len(req.audience) > maxAudienceSize {
		return exchangeRequest{}, false
	}
	if list, present := values["scope"]; present {
		actions := strings.Split(list[0], " ")
		if len(actions) > maxScopeActions {
			return exchangeRequest{}, false
		}
		for _, action := range actions {
			if action == "" || len(action) > maxActionSize {
				return exchangeRequest{}, false
			}
		}
		req.scope = actions
	}
	return req, true
}
