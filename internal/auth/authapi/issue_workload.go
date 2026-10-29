package authapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const (
	// compatUsesMetric counts accepted svc_authsess_ uses of the issue route,
	// by credential class only.
	compatUsesMetric = "auth.session.issue_compat_uses"
	// compatClass is the one class the compat counter counts.
	compatClass = "internal_svc_authsess_token"

	// issueWorkloadAudience is the audience the issue route accepts: the one
	// workload audience the registry gives the go-api workload.
	issueWorkloadAudience = "auth-service"

	// maxIssueBearerBytes bounds the bearer before any header is decoded. A
	// workload token is under 600 bytes.
	maxIssueBearerBytes = 2048

	reasonAcceptedWorkload = "accepted_workload"
)

// issueRefusedPrefixes are the credential classes the issue route never takes:
// a value that starts with one is refused before anything is decoded or read.
var issueRefusedPrefixes = []string{"svc_", "fcpush_", "fcacr_", "fcwlb_", "whsec_", "dhr1_"}

const compatPrefix = "svc_authsess_"

type issueClass int

const (
	issueClassRefused issueClass = iota
	issueClassCompat
	issueClassWorkload
)

// classifyIssueBearer picks the issue route's credential class from the
// bearer's text alone: no store, no key set, no signature. A refused class
// carries the reason for the log line.
func classifyIssueBearer(bearer string) (issueClass, string) {
	if len(bearer) > maxIssueBearerBytes {
		return issueClassRefused, "oversized"
	}
	if strings.HasPrefix(bearer, compatPrefix) {
		return issueClassCompat, ""
	}
	for _, prefix := range issueRefusedPrefixes {
		if strings.HasPrefix(bearer, prefix) {
			return issueClassRefused, "other_credential_class"
		}
	}
	header, _, ok := compactSegments(bearer)
	if !ok {
		return issueClassRefused, "not_a_jws"
	}
	var parsed struct {
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(header, &parsed); err != nil || parsed.Typ != contracts.WorkloadTokenTyp {
		return issueClassRefused, "not_a_workload_token"
	}
	return issueClassWorkload, ""
}

// compactSegments decodes the header and payload of a three-segment compact
// token. It verifies nothing.
func compactSegments(bearer string) (header, payload []byte, ok bool) {
	parts := strings.Split(bearer, ".")
	if len(parts) != 3 {
		return nil, nil, false
	}
	enc := base64.RawURLEncoding.Strict()
	header, err1 := enc.DecodeString(parts[0])
	payload, err2 := enc.DecodeString(parts[1])
	return header, payload, err1 == nil && err2 == nil
}

// workloadVerifier is satisfied by *authverify.WorkloadVerifier.
type workloadVerifier interface {
	Verify(token string, now time.Time) (*authverify.WorkloadClaims, error)
}

// SessionIssueWorkload lets the session issue route accept the go-api workload
// token: audience auth-service, and the one configured workload principal.
// The token carries no action claim, so the subject equality is the
// sessions.issue binding. A nil SessionIssueWorkload refuses every workload
// token.
type SessionIssueWorkload struct {
	verifier workloadVerifier
	subject  string
	now      func() time.Time
}

// SessionIssueWorkloadConfig builds a SessionIssueWorkload.
type SessionIssueWorkloadConfig struct {
	// IssuerURL is the iss every token carries (AUTH_TOKEN_ISSUER_URL).
	IssuerURL string
	// Keys is the signing key set the JWKS publishes.
	Keys KeySetSource
	// Subject is the go-api workload principal, in its session.v1 wire form.
	Subject string
	Now     func() time.Time
}

// NewSessionIssueWorkload validates cfg. Its errors never carry the subject.
func NewSessionIssueWorkload(cfg SessionIssueWorkloadConfig) (*SessionIssueWorkload, error) {
	if cfg.Keys == nil {
		return nil, errors.New("authapi: the session issue workload path needs a key set")
	}
	id, ok := parseWirePrincipal(cfg.Subject)
	if !ok || session.WirePrincipalID(id) != cfg.Subject {
		return nil, errors.New("authapi: the session issue workload subject is not a principal id")
	}
	verifier, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{
		Issuer: cfg.IssuerURL, Audience: issueWorkloadAudience, Keys: publishedKeys{source: cfg.Keys},
	})
	if err != nil {
		return nil, fmt.Errorf("authapi: the session issue workload verifier: %w", err)
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &SessionIssueWorkload{verifier: verifier, subject: cfg.Subject, now: now}, nil
}

// issueCaller is who asked for the session: a service credential row, or a
// workload principal and its token's jti.
type issueCaller struct {
	credentialID string
	workload     *uuid.UUID
	jti          string
}

// authorize decides a workload-typed bearer. It refuses a wrong audience or
// subject from the unverified payload before the key set is read, then
// verifies the signature, issuer, audience and times, then checks the subject
// again on the verified claims. err is set only when the key set could not be
// read. The reason goes to the log, never to the caller.
func (s *SessionIssueWorkload) authorize(bearer string) (issueCaller, string, error) {
	if s == nil || s.verifier == nil || s.subject == "" {
		return issueCaller{}, "not_configured", nil
	}
	_, payload, ok := compactSegments(bearer)
	if !ok {
		return issueCaller{}, "contract", nil
	}
	aud, sub, ok := peekAudienceAndSubject(payload)
	if !ok {
		return issueCaller{}, "contract", nil
	}
	if aud != issueWorkloadAudience {
		return issueCaller{}, "audience", nil
	}
	if sub != s.subject {
		return issueCaller{}, "subject", nil
	}
	claims, err := s.verifier.Verify(bearer, s.now())
	if errors.Is(err, authverify.ErrKeySourceUnavailable) {
		return issueCaller{}, "unavailable", err
	}
	if err != nil {
		return issueCaller{}, string(authverify.ReasonOf(err)), nil
	}
	principal, ok := parseWirePrincipal(claims.Subject)
	if claims.Audience != issueWorkloadAudience || claims.Subject != s.subject || !ok {
		return issueCaller{}, "subject", nil
	}
	return issueCaller{workload: &principal, jti: claims.TokenID}, "", nil
}

// issueCaller authorizes the issue route's bearer. On a refusal it has already
// answered, and ok is false.
func (h compatHandlers) issueCaller(w http.ResponseWriter, r *http.Request, bearer string) (issueCaller, bool) {
	ctx := r.Context()
	class, reason := classifyIssueBearer(bearer)
	switch class {
	case issueClassCompat:
		if h.ServiceCredentials == nil {
			h.rec.logger.WarnContext(ctx, "session issue refused: service credentials are off")
			break
		}
		credential, refused, err := h.ServiceCredentials.Check(ctx, contracts.NewSecretString(bearer))
		if err != nil {
			h.rec.refuse(w, r, opSessionIssue, unavailable(reasonStoreUnavailable), err)
			return issueCaller{}, false
		}
		if refused != "" {
			h.rec.logger.WarnContext(ctx, "session issue refused: service credential", "refusal", string(refused))
			break
		}
		h.rec.compatUses.Add(ctx, 1, metric.WithAttributes(attribute.String("class", compatClass)))
		return issueCaller{credentialID: credential.ID}, true
	case issueClassWorkload:
		caller, why, err := h.IssueWorkload.authorize(bearer)
		if err != nil {
			h.rec.refuse(w, r, opSessionIssue, unavailable(reasonDependency), err)
			return issueCaller{}, false
		}
		if why != "" {
			h.rec.logger.WarnContext(ctx, "session issue refused: workload token", "refusal", why)
			break
		}
		return caller, true
	default:
		h.rec.logger.WarnContext(ctx, "session issue refused: bearer", "refusal", reason)
	}
	h.rec.refuse(w, r, opSessionIssue, unauthorized(reasonInvalidCredentials), nil)
	return issueCaller{}, false
}

// peekAudienceAndSubject reads aud and sub from an unverified payload, as
// strict JSON: one object, no duplicate member, aud and sub present as strings,
// and no member that is aud or sub in another case (encoding/json would match
// it). The payload is already inside the bearer's length bound. The result only
// refuses: the verifier and the checks on its claims decide.
func peekAudienceAndSubject(payload []byte) (aud, sub string, ok bool) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return "", "", false
	}
	seen := map[string]bool{}
	for dec.More() {
		keyTok, err := dec.Token()
		key, isString := keyTok.(string)
		if err != nil || !isString || seen[key] {
			return "", "", false
		}
		seen[key] = true
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return "", "", false
		}
		for _, member := range []struct {
			name string
			into *string
		}{{"aud", &aud}, {"sub", &sub}} {
			if !strings.EqualFold(key, member.name) {
				continue
			}
			if key != member.name || json.Unmarshal(raw, member.into) != nil {
				return "", "", false
			}
		}
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return "", "", false
	}
	if _, err := dec.Token(); err != io.EOF {
		return "", "", false
	}
	return aud, sub, seen["aud"] && seen["sub"]
}
