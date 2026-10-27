package workloadexchange

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

type exchangeAnswer struct {
	AccessToken     string `json:"access_token"`
	IssuedTokenType string `json:"issued_token_type"`
	TokenType       string `json:"token_type"`
	ExpiresIn       int    `json:"expires_in"`
}

func decodeAnswer(t *testing.T, rec *httptest.ResponseRecorder) exchangeAnswer {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200; body %s", rec.Code, rec.Body.String())
	}
	dec := json.NewDecoder(strings.NewReader(rec.Body.String()))
	dec.DisallowUnknownFields()
	var answer exchangeAnswer
	if err := dec.Decode(&answer); err != nil {
		t.Fatalf("decode the 200 body: %v", err)
	}
	return answer
}

// verifyIssued checks the token with the canonical workload verifier
// (authverify, the CHAOS-10648 corpus verifier) against the signer's
// published keys, for audience.
func verifyIssued(t *testing.T, h *harness, compact, audience string) *authverify.WorkloadClaims {
	t.Helper()
	set, err := h.signer.KeySet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	keys := authverify.StaticKeys{}
	for _, key := range set.Published() {
		keys[key.KeyID] = key.PublicKey
	}
	verifier, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{Issuer: testIssuerURL, Audience: audience, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := verifier.Verify(compact, h.now)
	if err != nil {
		t.Fatalf("the issued token does not verify: %v", err)
	}
	return claims
}

func TestValidBindingGetsAWorkloadTokenForTheNarrowedAudience(t *testing.T) {
	h := newHarness(t)
	rec := h.post(form(h.validSubject(), "auth-service", testAction).Encode())
	answer := decodeAnswer(t, rec)
	if answer.IssuedTokenType != TokenTypeJWT || answer.TokenType != "Bearer" {
		t.Fatalf("issued_token_type %q token_type %q", answer.IssuedTokenType, answer.TokenType)
	}
	if answer.ExpiresIn != 300 {
		t.Fatalf("expires_in %d, want 300", answer.ExpiresIn)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control %q, want no-store", got)
	}
	claims := verifyIssued(t, h, answer.AccessToken, "auth-service")
	if claims.Subject != session.WirePrincipalID(h.registry.reg.PrincipalID) {
		t.Fatalf("sub %q is not the workload principal", claims.Subject)
	}
	if claims.Issuer != testIssuerURL || claims.Audience != "auth-service" {
		t.Fatalf("iss %q aud %q", claims.Issuer, claims.Audience)
	}
	if claims.IssuedAt != h.now.Unix() || claims.NotBefore != h.now.Unix() || claims.ExpiresAt != h.now.Unix()+300 {
		t.Fatalf("iat %d nbf %d exp %d", claims.IssuedAt, claims.NotBefore, claims.ExpiresAt)
	}
	header, payload := tokenParts(t, answer.AccessToken)
	if want := []string{"alg", "kid", "typ"}; !sameKeys(header, want) || header["typ"] != "workload+jwt" || header["alg"] != "EdDSA" {
		t.Fatalf("header %v", header)
	}
	if want := []string{"aud", "exp", "iat", "iss", "jti", "nbf", "principal_type", "sub"}; !sameKeys(payload, want) {
		t.Fatalf("claims %v, want exactly %v (workload-token.v1: no scope, org or session)", keysOf(payload), want)
	}
	if payload["principal_type"] != "workload" {
		t.Fatalf("principal_type %v", payload["principal_type"])
	}
}

func TestTheIssuedTokenFailsAtTheOtherWorkloadAudience(t *testing.T) {
	h := newHarness(t)
	answer := decodeAnswer(t, h.post(form(h.validSubject(), "auth-service", "").Encode()))
	set, _ := h.signer.KeySet(context.Background())
	keys := authverify.StaticKeys{}
	for _, key := range set.Published() {
		keys[key.KeyID] = key.PublicKey
	}
	other, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{Issuer: testIssuerURL, Audience: "dev-health-api", Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Verify(answer.AccessToken, h.now); authverify.ReasonOf(err) != authverify.ReasonAudience {
		t.Fatalf("a token for auth-service verified at dev-health-api: %v", err)
	}
}

func TestResolveSeesOnlyTheReviewedIdentityAndTheConfiguredCluster(t *testing.T) {
	h := newHarness(t)
	body := form(h.validSubject(), "auth-service", "")
	decodeAnswer(t, h.post(body.Encode()))
	want := Identity{TrustDomain: testTrustDomain, Issuer: testClusterIssuer, Namespace: testNamespace,
		ServiceAccountName: testSAName, ServiceAccountUID: testSAUID}
	if len(h.registry.inputs) != 1 || h.registry.inputs[0] != want {
		t.Fatalf("Resolve got %+v, want exactly %+v", h.registry.inputs, want)
	}
}

func TestEveryLifetimeIsCappedAtTheSubjectTokenExpiry(t *testing.T) {
	h := newHarness(t)
	answer := decodeAnswer(t, h.post(form(subjectToken(h.now.Add(100*time.Second)), "auth-service", "").Encode()))
	claims := verifyIssued(t, h, answer.AccessToken, "auth-service")
	if claims.ExpiresAt-claims.IssuedAt != 100 || answer.ExpiresIn != 100 {
		t.Fatalf("lifetime %d expires_in %d, want 100 (the subject token's remaining life)", claims.ExpiresAt-claims.IssuedAt, answer.ExpiresIn)
	}
}

func TestTheAuditRowBindsTheTokenToItsBindingAndRevision(t *testing.T) {
	h := newHarness(t)
	answer := decodeAnswer(t, h.post(form(h.validSubject(), "auth-service", testAction+" "+testAction).Encode()))
	claims := verifyIssued(t, h, answer.AccessToken, "auth-service")
	rows := h.auditRows()
	if len(rows) != 1 {
		t.Fatalf("%d audit rows, want 1", len(rows))
	}
	row := rows[0]
	if row.EventType != audit.EventWorkloadTokenIssued || row.Outcome != audit.OutcomeAllowed {
		t.Fatalf("event %q outcome %q", row.EventType, row.Outcome)
	}
	principal := h.registry.reg.PrincipalID.String()
	if row.SubjectPrincipalID == nil || *row.SubjectPrincipalID != principal {
		t.Fatalf("subject principal %v", row.SubjectPrincipalID)
	}
	want := map[string]any{
		"jti": claims.TokenID, "principal_id": principal, "binding_id": h.registry.reg.BindingID.String(),
		"revision": int64(3), "audience": "auth-service", "actions": []string{testAction},
	}
	if !sameKeys(row.Attributes, keysOfAny(want)) {
		t.Fatalf("audit attribute keys %v, want exactly %v", keysOfAny(row.Attributes), keysOfAny(want))
	}
	for key, value := range want {
		got, _ := json.Marshal(row.Attributes[key])
		exp, _ := json.Marshal(value)
		if string(got) != string(exp) {
			t.Errorf("audit %s = %s, want %s", key, got, exp)
		}
	}
	if err := audit.ValidateAttributes(row.Attributes); err != nil {
		t.Fatalf("the audit allowlist refuses the row: %v", err)
	}
}

func TestAnEmptyScopeRecordsNoActions(t *testing.T) {
	h := newHarness(t)
	decodeAnswer(t, h.post(form(h.validSubject(), "auth-service", "").Encode()))
	rows := h.auditRows()
	if len(rows) != 1 {
		t.Fatalf("%d audit rows", len(rows))
	}
	if got, _ := json.Marshal(rows[0].Attributes["actions"]); string(got) != "[]" {
		t.Fatalf("actions %s, want []", got)
	}
}

// refusalCase drives one refusal after a valid request shape.
type refusalCase struct {
	name   string
	reason string
	setup  func(h *harness) string
}

func refusalCases() []refusalCase {
	review := func(status int, authenticated bool, audiences []string, username, uid string) func(h *harness) string {
		return func(h *harness) string {
			h.review.set(func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) {
				writeReview(w, status, authenticated, audiences, username, uid)
			})
			return form(h.validSubject(), "auth-service", "").Encode()
		}
	}
	sa := "system:serviceaccount:" + testNamespace + ":" + testSAName
	store := func(reason string) func(h *harness) string {
		return func(h *harness) string {
			h.registry.err = refusedBecause(reason)
			return form(h.validSubject(), "auth-service", "").Encode()
		}
	}
	cases := []refusalCase{
		{"not authenticated (a foreign cluster's token)", "subject_token_invalid", review(http.StatusCreated, false, nil, "", "")},
		{"review audience missing", "subject_token_invalid", review(http.StatusCreated, true, []string{"https://kubernetes.default.svc"}, sa, testSAUID)},
		{"not a ServiceAccount", "subject_token_invalid", review(http.StatusCreated, true, []string{ReviewAudience}, "system:node:n1", testSAUID)},
		{"empty uid", "subject_token_invalid", review(http.StatusCreated, true, []string{ReviewAudience}, sa, "")},
		{"subject already expired", "subject_token_invalid", func(h *harness) string {
			return form(subjectToken(h.now.Add(-time.Second)), "auth-service", "").Encode()
		}},
		{"audience outside the workload audiences", "audience_refused", func(h *harness) string {
			h.registry.reg.Audiences = []string{"auth-service", "query-api"}
			return form(h.validSubject(), "query-api", "").Encode()
		}},
		{"audience not registered", "audience_refused", func(h *harness) string {
			return form(h.validSubject(), "dev-health-api", "").Encode()
		}},
		{"action not allowed", "scope_refused", func(h *harness) string {
			return form(h.validSubject(), "auth-service", testAction+" workloads.delete").Encode()
		}},
		{"subject expires within a second", "lifetime_too_short", func(h *harness) string {
			h.now = h.now.Add(500 * time.Millisecond)
			return form(subjectToken(time.Unix(h.now.Unix()+1, 0)), "auth-service", "").Encode()
		}},
		{"a refusal the store did not name", "resolve_refused", store("")},
	}
	for _, reason := range []string{"unknown", "disabled", "uid_mismatch", "no_audience", "store_error"} {
		cases = append(cases, refusalCase{"store: " + reason, reason, store(reason)})
	}
	return cases
}

// Every refusal after a valid shape answers the same bytes: only the log line
// and the counter name the reason.
func TestEveryRefusalAnswersTheSameBytes(t *testing.T) {
	var first *httptest.ResponseRecorder
	for _, tc := range refusalCases() {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			rec := h.post(tc.setup(h))
			if rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_grant\"}\n" {
				t.Fatalf("status %d body %q, want 400 {\"error\":\"invalid_grant\"}", rec.Code, rec.Body.String())
			}
			if first == nil {
				first = rec
			} else if got, want := comparableHeaders(rec.Header()), comparableHeaders(first.Header()); got != want {
				t.Fatalf("headers differ from the first refusal:\n%s\nwant\n%s", got, want)
			}
			if len(h.auditRows()) != 0 {
				t.Fatal("a refusal wrote an audit row")
			}
			if got := h.metricPoints()["refused/"+tc.reason+"/"+audienceLabelOf(tc)]; got != 1 {
				t.Fatalf("counter refused/%s = %d, want 1; points %v", tc.reason, got, h.metricPoints())
			}
			if !strings.Contains(h.logs.String(), `"reason":"`+tc.reason+`"`) {
				t.Fatalf("no log line names reason %s: %s", tc.reason, h.logs.String())
			}
		})
	}
}

func audienceLabelOf(tc refusalCase) string {
	if tc.name == "audience outside the workload audiences" {
		return "none"
	}
	if tc.name == "audience not registered" {
		return "dev-health-api"
	}
	return "auth-service"
}

func comparableHeaders(header http.Header) string {
	var lines []string
	for key, values := range header {
		if key == "Date" || key == "X-Auth-Request-Id" || key == "X-Request-Id" {
			continue
		}
		lines = append(lines, key+": "+strings.Join(values, ","))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// A store or registry refusal is decided only after TokenReview authenticated
// the caller; an unauthenticated caller never reaches the store.
func TestTheStoreIsReadOnlyAfterASuccessfulReview(t *testing.T) {
	h := newHarness(t)
	h.review.set(func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) {
		writeReview(w, http.StatusCreated, false, nil, "", "")
	})
	h.post(form(h.validSubject(), "auth-service", "").Encode())
	if h.registry.resolved != 0 {
		t.Fatalf("Resolve ran %d times for an unauthenticated subject", h.registry.resolved)
	}
}

func TestShapeFaultsAnswerInvalidRequestWithoutAReview(t *testing.T) {
	valid := func(h *harness) map[string][]string { return form(h.validSubject(), "auth-service", "") }
	for name, mutate := range map[string]func(h *harness, v map[string][]string){
		"grant type": func(_ *harness, v map[string][]string) { v["grant_type"] = []string{"client_credentials"} },
		"subject token type": func(_ *harness, v map[string][]string) {
			v["subject_token_type"] = []string{"urn:ietf:params:oauth:token-type:access_token"}
		},
		"no subject token":     func(_ *harness, v map[string][]string) { delete(v, "subject_token") },
		"empty subject token":  func(_ *harness, v map[string][]string) { v["subject_token"] = []string{""} },
		"subject token 4 KiB+": func(_ *harness, v map[string][]string) { v["subject_token"] = []string{strings.Repeat("a", 4<<10+1)} },
		"no audience":          func(_ *harness, v map[string][]string) { delete(v, "audience") },
		"two audiences":        func(_ *harness, v map[string][]string) { v["audience"] = []string{"auth-service", "dev-health-api"} },
		"repeated field": func(_ *harness, v map[string][]string) {
			v["grant_type"] = []string{GrantTypeTokenExchange, GrantTypeTokenExchange}
		},
		"unknown field":      func(_ *harness, v map[string][]string) { v["resource"] = []string{"https://x"} },
		"empty scope":        func(_ *harness, v map[string][]string) { v["scope"] = []string{""} },
		"double space scope": func(_ *harness, v map[string][]string) { v["scope"] = []string{testAction + "  " + testOtherAction} },
		"actor token":        func(_ *harness, v map[string][]string) { v["actor_token"] = []string{"x"} },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			v := valid(h)
			mutate(h, v)
			rec := h.post(encodeValues(v))
			assertInvalidRequest(t, h, rec)
		})
	}
	t.Run("json body", func(t *testing.T) {
		h := newHarness(t)
		rec := h.postWith(`{"grant_type":"`+GrantTypeTokenExchange+`"}`, "application/json", Pattern)
		assertInvalidRequest(t, h, rec)
	})
	t.Run("query string", func(t *testing.T) {
		h := newHarness(t)
		rec := h.postWith(form(h.validSubject(), "auth-service", "").Encode(), "application/x-www-form-urlencoded", Pattern+"?subject_token=x")
		assertInvalidRequest(t, h, rec)
	})
	t.Run("bare question mark", func(t *testing.T) {
		h := newHarness(t)
		rec := h.postWith(form(h.validSubject(), "auth-service", "").Encode(), "application/x-www-form-urlencoded", Pattern+"?")
		assertInvalidRequest(t, h, rec)
	})
	t.Run("semicolon separator", func(t *testing.T) {
		h := newHarness(t)
		rec := h.post(strings.ReplaceAll(form(h.validSubject(), "auth-service", "").Encode(), "&", ";"))
		assertInvalidRequest(t, h, rec)
	})
	t.Run("body over 8 KiB", func(t *testing.T) {
		h := newHarness(t)
		v := form(h.validSubject(), "auth-service", "")
		body := v.Encode() + "&pad=" + strings.Repeat("a", MaxBodyBytes)
		rec := h.post(body)
		assertInvalidRequest(t, h, rec)
	})
	t.Run("body over 8 KiB without a declared length", func(t *testing.T) {
		h := newHarness(t)
		body := form(h.validSubject(), "auth-service", "").Encode() + "&pad=" + strings.Repeat("a", MaxBodyBytes)
		req := httptest.NewRequest(http.MethodPost, Pattern, strings.NewReader(body))
		req.ContentLength = -1
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		h.server.ServeHTTP(rec, req)
		assertInvalidRequest(t, h, rec)
	})
}

func encodeValues(v map[string][]string) string {
	var parts []string
	keys := make([]string, 0, len(v))
	for key := range v {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		for _, value := range v[key] {
			parts = append(parts, urlEscape(key)+"="+urlEscape(value))
		}
	}
	return strings.Join(parts, "&")
}

func assertInvalidRequest(t *testing.T, h *harness, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_request\"}\n" {
		t.Fatalf("status %d body %q, want 400 {\"error\":\"invalid_request\"}", rec.Code, rec.Body.String())
	}
	if n := h.review.count(); n != 0 {
		t.Fatalf("a shape fault reached TokenReview %d times", n)
	}
	if got := h.metricPoints()["invalid/invalid_request/none"]; got != 1 {
		t.Fatalf("counter invalid/invalid_request/none = %d; points %v", got, h.metricPoints())
	}
}

// A TokenReview outage fails closed with 503: no token, no store read.
func TestTokenReviewFailuresFailClosedWith503(t *testing.T) {
	for name, answer := range map[string]func(w http.ResponseWriter, r *http.Request, spec reviewSpec){
		"401": func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) { w.WriteHeader(http.StatusUnauthorized) },
		"403": func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) { w.WriteHeader(http.StatusForbidden) },
		"500": func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) {
			w.WriteHeader(http.StatusInternalServerError)
		},
		"503": func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) {
			w.WriteHeader(http.StatusServiceUnavailable)
		},
		"undecodable": func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte("{not json"))
		},
		"oversize": func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"status":{"authenticated":true,"error":"` + strings.Repeat("x", 70<<10) + `"}}`))
		},
		"redirect": func(w http.ResponseWriter, r *http.Request, _ reviewSpec) {
			http.Redirect(w, r, "https://elsewhere.example.test/apis/authentication.k8s.io/v1/tokenreviews", http.StatusTemporaryRedirect)
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.review.set(answer)
			rec := h.post(form(h.validSubject(), "auth-service", "").Encode())
			assertUnavailable(t, h, rec, "tokenreview_unavailable")
		})
	}
}

func assertUnavailable(t *testing.T, h *harness, rec *httptest.ResponseRecorder, reason string) {
	t.Helper()
	if rec.Code != http.StatusServiceUnavailable || rec.Body.String() != "{\"error\":\"temporarily_unavailable\"}\n" {
		t.Fatalf("status %d body %q, want 503 {\"error\":\"temporarily_unavailable\"}", rec.Code, rec.Body.String())
	}
	if h.registry.resolved != 0 && reason == "tokenreview_unavailable" {
		t.Fatal("the store was read after a review failure")
	}
	if len(h.auditRows()) != 0 {
		t.Fatal("an outage wrote an audit row")
	}
	if got := h.metricPoints()["unavailable/"+reason+"/auth-service"]; got != 1 {
		t.Fatalf("counter unavailable/%s = %d; points %v", reason, got, h.metricPoints())
	}
	if !strings.Contains(h.logs.String(), `"reason":"`+reason+`"`) {
		t.Fatalf("no log line names %s", reason)
	}
}

func TestASlowReviewTimesOutWith503(t *testing.T) {
	h := newHarness(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	h.review.set(func(w http.ResponseWriter, r *http.Request, _ reviewSpec) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	start := time.Now()
	rec := h.post(form(h.validSubject(), "auth-service", "").Encode())
	if elapsed := time.Since(start); elapsed > ReviewCallTimeout+2*time.Second {
		t.Fatalf("the review took %s, over the %s bound", elapsed, ReviewCallTimeout)
	}
	assertUnavailable(t, h, rec, "tokenreview_unavailable")
}

// An API server without the TokenReview path (it answers 404) fails closed.
// Every httptest TLS server shares one test certificate, so this server is
// trusted by the configured CA: it does not prove the CA-only rule (see
// TestAReviewToAServerOutsideTheClusterCAFailsClosed).
func TestAnAPIServerWithoutTheTokenReviewPathFailsClosed(t *testing.T) {
	h := newHarness(t, func(h *harness, d *Deps) {
		other := httptest.NewTLSServer(http.NotFoundHandler())
		t.Cleanup(other.Close)
		validator, err := NewReviewValidator(ReviewConfig{
			APIServerURL: other.URL, CAFile: h.review.caFile, ReviewerTokenFile: writeTemp(t, "reviewer"),
			TrustDomain: testTrustDomain, Now: h.clock,
		})
		if err != nil {
			t.Fatal(err)
		}
		d.Validator = validator
	})
	rec := h.post(form(h.validSubject(), "auth-service", "").Encode())
	assertUnavailable(t, h, rec, "tokenreview_unavailable")
}

func TestAMissingOrEmptyReviewerTokenFailsClosed(t *testing.T) {
	for name, path := range map[string]func(t *testing.T) string{
		"missing": func(t *testing.T) string { return t.TempDir() + "/absent" },
		"empty":   func(t *testing.T) string { return writeTemp(t, "  \n") },
		"oversize": func(t *testing.T) string {
			return writeTemp(t, strings.Repeat("a", 16<<10+1))
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, func(h *harness, d *Deps) {
				validator, err := NewReviewValidator(ReviewConfig{
					APIServerURL: h.review.server.URL, CAFile: h.review.caFile, ReviewerTokenFile: path(t),
					TrustDomain: testTrustDomain, Now: h.clock,
				})
				if err != nil {
					t.Fatal(err)
				}
				d.Validator = validator
			})
			rec := h.post(form(h.validSubject(), "auth-service", "").Encode())
			assertUnavailable(t, h, rec, "tokenreview_unavailable")
			if h.review.count() != 0 {
				t.Fatal("a review was sent without a reviewer token")
			}
		})
	}
}

func TestTheReviewerTokenIsReadOnEveryReview(t *testing.T) {
	path := writeTemp(t, "first")
	h := newHarness(t, func(h *harness, d *Deps) {
		validator, err := NewReviewValidator(ReviewConfig{
			APIServerURL: h.review.server.URL, CAFile: h.review.caFile, ReviewerTokenFile: path,
			TrustDomain: testTrustDomain, Now: h.clock,
		})
		if err != nil {
			t.Fatal(err)
		}
		d.Validator = validator
	})
	var seen []string
	var mu sync.Mutex
	h.review.anyBearer()
	h.review.set(func(w http.ResponseWriter, r *http.Request, _ reviewSpec) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Authorization"))
		mu.Unlock()
		writeReview(w, http.StatusCreated, false, nil, "", "")
	})
	h.post(form(h.validSubject(), "auth-service", "").Encode())
	rewrite(t, path, "second")
	h.post(form(h.validSubject(), "auth-service", "").Encode())
	if len(seen) != 2 || seen[0] != "Bearer first" || seen[1] != "Bearer second" {
		t.Fatalf("reviewer bearers %v, want the file's value at each review", seen)
	}
}

func TestReviewConfigRefusesWhatCannotReviewSafely(t *testing.T) {
	ca := newReviewServer(t, "x").caFile
	token := writeTemp(t, "reviewer")
	for name, cfg := range map[string]ReviewConfig{
		"plain http":        {APIServerURL: "http://kubernetes.example.test", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"no url":            {CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"no CA":             {APIServerURL: "https://kubernetes.example.test", ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"absent CA":         {APIServerURL: "https://kubernetes.example.test", CAFile: ca + ".absent", ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"CA not PEM":        {APIServerURL: "https://kubernetes.example.test", CAFile: token, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"no token":          {APIServerURL: "https://kubernetes.example.test", CAFile: ca, TrustDomain: testTrustDomain},
		"no trust dom":      {APIServerURL: "https://kubernetes.example.test", CAFile: ca, ReviewerTokenFile: token},
		"bare query":        {APIServerURL: "https://kubernetes.example.test?", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"query":             {APIServerURL: "https://kubernetes.example.test?a=b", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"bare fragment":     {APIServerURL: "https://kubernetes.example.test#", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"path":              {APIServerURL: "https://kubernetes.example.test/apis", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"userinfo":          {APIServerURL: "https://u:p@kubernetes.example.test", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"empty port":        {APIServerURL: "https://kubernetes.example.test:", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"empty port, slash": {APIServerURL: "https://kubernetes.example.test:/", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"non-numeric port":  {APIServerURL: "https://kubernetes.example.test:x", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"port zero":         {APIServerURL: "https://kubernetes.example.test:0", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"port 65536":        {APIServerURL: "https://kubernetes.example.test:65536", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"port 99999":        {APIServerURL: "https://kubernetes.example.test:99999", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"signed port":       {APIServerURL: "https://kubernetes.example.test:+443", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
		"ipv6 empty port":   {APIServerURL: "https://[fd00::1]:", CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain},
	} {
		if _, err := NewReviewValidator(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for _, origin := range []string{"https://kubernetes.example.test", "https://kubernetes.example.test/", "https://kubernetes.example.test:1", "https://kubernetes.example.test:443", "https://kubernetes.example.test:65535", "https://10.43.0.1:6443"} {
		if _, err := NewReviewValidator(ReviewConfig{APIServerURL: origin, CAFile: ca, ReviewerTokenFile: token, TrustDomain: testTrustDomain}); err != nil {
			t.Errorf("origin %s refused: %v", origin, err)
		}
	}
}

func TestOneReviewOverTheInFlightCapAnswers503(t *testing.T) {
	h := newHarness(t)
	entered := make(chan struct{}, MaxInFlightReviews)
	release := make(chan struct{})
	h.review.set(func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) {
		entered <- struct{}{}
		<-release
		writeReview(w, http.StatusCreated, false, nil, "", "")
	})
	var wg sync.WaitGroup
	for range MaxInFlightReviews {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.post(form(h.validSubject(), "auth-service", "").Encode())
		}()
	}
	for range MaxInFlightReviews {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			close(release)
			t.Fatal("the reviews did not all start")
		}
	}
	rec := h.post(form(h.validSubject(), "auth-service", "").Encode())
	close(release)
	wg.Wait()
	if rec.Code != http.StatusServiceUnavailable || rec.Body.String() != "{\"error\":\"temporarily_unavailable\"}\n" {
		t.Fatalf("status %d body %q, want 503", rec.Code, rec.Body.String())
	}
	if got := h.metricPoints()["unavailable/saturated/auth-service"]; got != 1 {
		t.Fatalf("counter unavailable/saturated = %d", got)
	}
	if n := h.review.count(); n != MaxInFlightReviews {
		t.Fatalf("%d reviews reached the API server, want %d", n, MaxInFlightReviews)
	}
}

// keyOnlySigner signs with the real key but reports a key set that does not
// hold the signing kid, so the self-check refuses the minted token.
type keyOnlySigner struct {
	Signer
	set token.KeySet
}

func (k keyOnlySigner) KeySet(context.Context) (token.KeySet, error) { return k.set, nil }

func TestTheSelfCheckRefusesATokenItsKeySetCannotVerify(t *testing.T) {
	public, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	set, err := token.NewKeySet([]token.Key{{KeyID: "another-kid", State: token.StateActive, Algorithm: token.Algorithm, PublicKey: public}})
	if err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, func(h *harness, d *Deps) { d.Signer = keyOnlySigner{Signer: d.Signer, set: set} })
	rec := h.post(form(h.validSubject(), "auth-service", "").Encode())
	assertUnavailable(t, h, rec, "selfcheck_unknown_kid")
	if strings.Contains(rec.Body.String(), ".") {
		t.Fatal("the refused token reached the body")
	}
}

type failingSigner struct{ Signer }

func (failingSigner) Sign(context.Context, string, []byte) (string, string, error) {
	return "", "", errors.New("custody down")
}

func TestASignerFailureAnswers503(t *testing.T) {
	h := newHarness(t, func(h *harness, d *Deps) { d.Signer = failingSigner{Signer: d.Signer} })
	assertUnavailable(t, h, h.post(form(h.validSubject(), "auth-service", "").Encode()), "signer_unavailable")
}

func TestAnAuditFailureAnswers503WithoutAToken(t *testing.T) {
	h := newHarness(t)
	h.auditErr = errors.New("audit down")
	rec := h.post(form(h.validSubject(), "auth-service", "").Encode())
	assertUnavailable(t, h, rec, "audit_unavailable")
}

func TestRoutesRefusesMissingCollaborators(t *testing.T) {
	h := newHarness(t)
	_ = h
	base := func() Deps {
		v, _ := NewReviewValidator(ReviewConfig{APIServerURL: h.review.server.URL, CAFile: h.review.caFile,
			ReviewerTokenFile: writeTemp(t, "x"), TrustDomain: testTrustDomain})
		return Deps{Validator: v, Registry: h.registry, RefusalReason: workload.RefusalReason, Signer: h.signer,
			Audit: h.writeAudit, IssuerURL: testIssuerURL, ClusterIssuer: testClusterIssuer}
	}
	if _, err := Routes(base()); err != nil {
		t.Fatalf("complete deps refused: %v", err)
	}
	for name, mutate := range map[string]func(*Deps){
		"validator":      func(d *Deps) { d.Validator = nil },
		"registry":       func(d *Deps) { d.Registry = nil },
		"refusal reason": func(d *Deps) { d.RefusalReason = nil },
		"signer":         func(d *Deps) { d.Signer = nil },
		"audit":          func(d *Deps) { d.Audit = nil },
		"issuer":         func(d *Deps) { d.IssuerURL = "" },
		"plain issuer":   func(d *Deps) { d.IssuerURL = "http://auth.example.test" },
		"cluster":        func(d *Deps) { d.ClusterIssuer = "" },
	} {
		d := base()
		mutate(&d)
		if _, err := Routes(d); err == nil {
			t.Errorf("missing %s: accepted", name)
		}
	}
}

func TestOnlyPostIsServed(t *testing.T) {
	h := newHarness(t)
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, Pattern, nil)
		rec := httptest.NewRecorder()
		h.server.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s answered %d, want 405", method, rec.Code)
		}
	}
}

func tokenParts(t *testing.T, compact string) (map[string]any, map[string]any) {
	t.Helper()
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		t.Fatalf("%d segments", len(parts))
	}
	decode := func(s string) map[string]any {
		raw, err := base64.RawURLEncoding.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]any{}
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	return decode(parts[0]), decode(parts[1])
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func keysOfAny(m map[string]any) []string { return keysOf(m) }

func sameKeys(m map[string]any, want []string) bool {
	got := keysOf(m)
	sort.Strings(want)
	return strings.Join(got, ",") == strings.Join(want, ",")
}

// The TokenReview client follows no redirect: after one review call through
// the client production builds, the second origin saw no request.
func TestTheReviewClientNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	client := redirectprobe.Reach(reviewHTTPClient(x509.NewCertPool()))
	resp, err := client.Post(probe.Base.URL+"/apis/authentication.k8s.io/v1/tokenreviews", "application/json", strings.NewReader("{}"))
	if err == nil {
		_ = resp.Body.Close()
	}
	probe.Assert(t)
}

// The TokenReview client trusts the configured cluster CA pool only (never the
// system roots), uses no proxy (never one from the environment, so the
// ServiceAccount tokens reach only the API server) and is bounded.
func TestTheReviewClientTrustsOnlyTheClusterCAAndUsesNoProxy(t *testing.T) {
	pool := x509.NewCertPool()
	client := reviewHTTPClient(pool)
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("the review client transport is %T, want its own *http.Transport", client.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("the review client has a proxy function: an environment proxy would see the ServiceAccount tokens")
	}
	if transport.TLSClientConfig == nil || transport.TLSClientConfig.RootCAs != pool {
		t.Fatal("the review client does not trust exactly the configured cluster CA pool")
	}
	if transport.TLSClientConfig.InsecureSkipVerify || transport.TLSClientConfig.MinVersion < tls.VersionTLS12 {
		t.Fatal("the review client weakens TLS")
	}
	if client.Timeout != ReviewCallTimeout || ReviewCallTimeout != 5*time.Second {
		t.Fatalf("client Timeout %s, want ReviewCallTimeout (5 s)", client.Timeout)
	}
	if transport.TLSHandshakeTimeout != 2*time.Second || transport.ResponseHeaderTimeout != 3*time.Second {
		t.Fatalf("TLS handshake %s, response header %s, want 2 s and 3 s", transport.TLSHandshakeTimeout, transport.ResponseHeaderTimeout)
	}
	if client.CheckRedirect == nil {
		t.Fatal("the review client has no redirect policy")
	}
}

// An environment proxy never sees a review: the review reaches the API server
// directly although HTTPS_PROXY and HTTP_PROXY name a proxy.
func TestAReviewNeverGoesThroughAnEnvironmentProxy(t *testing.T) {
	var proxied atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxied.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(proxy.Close)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("NO_PROXY", "")
	h := newHarness(t)
	decodeAnswer(t, h.post(form(h.validSubject(), "auth-service", "").Encode()))
	if n := proxied.Load(); n != 0 {
		t.Fatalf("the environment proxy saw %d request(s)", n)
	}
	if h.review.count() != 1 {
		t.Fatalf("the API server saw %d reviews, want 1", h.review.count())
	}
}

// selfSignedTLSServer is a TLS API server whose certificate is a freshly
// generated CA that is not the configured cluster CA; caPEM is that CA.
func selfSignedTLSServer(t *testing.T, handler http.Handler) (*httptest.Server, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "foreign-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// RootCAs is the configured cluster CA pool only: an API server whose
// certificate chains to another CA is refused at TLS, so the review never
// reaches it and the exchange answers 503. The control trusts that CA and
// reaches the same server.
func TestAReviewToAServerOutsideTheClusterCAFailsClosed(t *testing.T) {
	var reached atomic.Int64
	foreign, foreignCA := selfSignedTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Add(1)
		writeReview(w, http.StatusCreated, true, []string{ReviewAudience}, "system:serviceaccount:"+testNamespace+":"+testSAName, testSAUID)
	}))
	withAPIServer := func(caFile func(h *harness) string) harnessOption {
		return func(h *harness, d *Deps) {
			validator, err := NewReviewValidator(ReviewConfig{
				APIServerURL: foreign.URL, CAFile: caFile(h), ReviewerTokenFile: writeTemp(t, "reviewer"),
				TrustDomain: testTrustDomain, Now: h.clock,
			})
			if err != nil {
				t.Fatal(err)
			}
			d.Validator = validator
		}
	}
	h := newHarness(t, withAPIServer(func(h *harness) string { return h.review.caFile }))
	assertUnavailable(t, h, h.post(form(h.validSubject(), "auth-service", "").Encode()), "tokenreview_unavailable")
	if n := reached.Load(); n != 0 {
		t.Fatalf("the server outside the cluster CA saw %d request(s)", n)
	}
	control := newHarness(t, withAPIServer(func(*harness) string { return writeTemp(t, string(foreignCA)) }))
	decodeAnswer(t, control.post(form(control.validSubject(), "auth-service", "").Encode()))
	if reached.Load() != 1 {
		t.Fatal("the control did not reach the server it trusts")
	}
}

type deadlineRecorder struct {
	inner    http.RoundTripper
	deadline time.Time
	ok       bool
}

func (d *deadlineRecorder) RoundTrip(r *http.Request) (*http.Response, error) {
	d.deadline, d.ok = r.Context().Deadline()
	return d.inner.RoundTrip(r)
}

// Every outgoing review request carries a context deadline at most
// ReviewCallTimeout away, even when the caller's context has none.
func TestTheOutgoingReviewRequestCarriesADeadline(t *testing.T) {
	rs := newReviewServer(t, "x")
	pool := x509.NewCertPool()
	pool.AddCert(rs.server.Certificate())
	client := reviewHTTPClient(pool)
	recorder := &deadlineRecorder{inner: client.Transport}
	client.Transport = recorder
	start := time.Now()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, rs.server.URL+"/apis/authentication.k8s.io/v1/tokenreviews",
		strings.NewReader(`{"spec":{"token":"x","audiences":["`+ReviewAudience+`"]}}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer x")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if !recorder.ok {
		t.Fatal("the outgoing review request carries no context deadline")
	}
	if end := time.Now(); !recorder.deadline.After(start) || recorder.deadline.After(end.Add(ReviewCallTimeout)) {
		t.Fatalf("deadline %s after the start, want at most ReviewCallTimeout (%s) after the call", recorder.deadline.Sub(start), ReviewCallTimeout)
	}
}

type ctxValidator struct {
	deadline time.Time
	ok       bool
}

func (c *ctxValidator) Validate(ctx context.Context, _ string) (authverify.SubjectIdentity, error) {
	c.deadline, c.ok = ctx.Deadline()
	return authverify.SubjectIdentity{}, authverify.ErrSubjectTokenInvalid
}

// The exchange hands the validator a context bounded by ReviewCallTimeout.
func TestTheExchangeBoundsEveryReviewByReviewCallTimeout(t *testing.T) {
	fake := &ctxValidator{}
	h := newHarness(t, func(_ *harness, d *Deps) { d.Validator = fake })
	start := time.Now()
	h.post(form(h.validSubject(), "auth-service", "").Encode())
	if !fake.ok {
		t.Fatal("the review context has no deadline")
	}
	if end := time.Now(); !fake.deadline.After(start) || fake.deadline.After(end.Add(ReviewCallTimeout)) {
		t.Fatalf("review deadline %s after the start, want at most ReviewCallTimeout (%s) after the call", fake.deadline.Sub(start), ReviewCallTimeout)
	}
}

// A requested audience reaches a log line or a counter label only as one of the
// literals auth-service, dev-health-api or none: for known and unknown
// audiences alike, the set of audience values in every log line and every
// counter point is a subset of those literals, and no request byte appears.
func TestARequestAudienceNeverReachesALogOrALabel(t *testing.T) {
	literals := map[string]bool{"auth-service": true, "dev-health-api": true, "none": true}
	for _, tc := range []struct {
		audience, label string
		status          int
	}{
		{"auth-service", "auth-service", http.StatusOK},
		{"dev-health-api", "dev-health-api", http.StatusBadRequest},
		{"PLANTEDAUDIENCEq7", "none", http.StatusBadRequest},
		{"auth-service\nPLANTEDAUDIENCEq7", "none", http.StatusBadRequest},
		{"Auth-Service", "none", http.StatusBadRequest},
		{"auth-service ", "none", http.StatusBadRequest},
	} {
		t.Run(tc.audience, func(t *testing.T) {
			h := newHarness(t)
			rec := h.post(form(h.validSubject(), tc.audience, "").Encode())
			if rec.Code != tc.status {
				t.Fatalf("status %d, want %d", rec.Code, tc.status)
			}
			seen := map[string]bool{}
			for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
				var entry map[string]any
				if err := json.Unmarshal([]byte(line), &entry); err != nil {
					t.Fatalf("log line is not JSON: %q", line)
				}
				if v, ok := entry["audience"]; ok {
					seen[fmt.Sprint(v)] = true
				}
			}
			counted := false
			for key, value := range h.metricPoints() {
				parts := strings.Split(key, "/")
				seen[parts[len(parts)-1]] = true
				if value > 0 && parts[len(parts)-1] == tc.label {
					counted = true
				}
			}
			for value := range seen {
				if !literals[value] {
					t.Errorf("an audience value outside the literals reached a log or a label: %q", value)
				}
			}
			if !counted {
				t.Errorf("no counter point with audience %q", tc.label)
			}
			for surface, text := range map[string]string{"logs": h.logs.String(), "metrics": dumpMetrics(t, h.reader)} {
				if strings.Contains(text, "PLANTEDAUDIENCEq7") || strings.Contains(text, "Auth-Service") || strings.Contains(text, `"auth-service "`) {
					t.Errorf("%s carry the requested audience", surface)
				}
			}
		})
	}
}
