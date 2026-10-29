package workloadexchange

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

const (
	testIssuerURL     = "https://auth.example.test"
	testClusterIssuer = "https://kubernetes.default.svc.cluster.local"
	testTrustDomain   = "cluster.example.test"
	testNamespace     = "dev-health"
	testSAName        = "dev-health-go-api"
	testSAUID         = "0b9f3c1e-7a4d-4e2b-9c8f-1d2e3f4a5b6c"
	testAction        = "workloads.read"
	testOtherAction   = "workloads.operate"
)

// Planted markers: each credential the route handles carries one, so a test
// can prove none of them reaches a log line, a metric, a refusal or the audit
// row.
const (
	subjectMarker  = "PLANTEDSUBJECTb7Q2"
	reviewerMarker = "PLANTEDREVIEWERk9X4"
)

// reviewServer is a fake Kubernetes API server: TLS, the TokenReview path
// only. answer decides each review; nil answers the default authenticated
// review of the test ServiceAccount.
type reviewServer struct {
	server *httptest.Server
	caFile string

	mu      sync.Mutex
	answer  func(w http.ResponseWriter, r *http.Request, spec reviewSpec)
	calls   int
	lastErr string
	bearer  string
}

type reviewSpec struct {
	Token     string   `json:"token"`
	Audiences []string `json:"audiences"`
}

func newReviewServer(t *testing.T, reviewer string) *reviewServer {
	t.Helper()
	rs := &reviewServer{bearer: reviewer}
	rs.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rs.mu.Lock()
		rs.calls++
		answer := rs.answer
		bearer := rs.bearer
		rs.mu.Unlock()
		if r.Method != http.MethodPost || r.URL.Path != "/apis/authentication.k8s.io/v1/tokenreviews" {
			rs.fail("unexpected request " + r.Method + " " + r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if bearer != "" && r.Header.Get("Authorization") != "Bearer "+bearer {
			rs.fail("the reviewer token is not the file's")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var body struct {
			APIVersion string     `json:"apiVersion"`
			Kind       string     `json:"kind"`
			Spec       reviewSpec `json:"spec"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			rs.fail("undecodable review")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(body.Spec.Audiences) != 1 || body.Spec.Audiences[0] != ReviewAudience {
			rs.fail("the review does not ask for the review audience")
		}
		if answer != nil {
			answer(w, r, body.Spec)
			return
		}
		writeReview(w, http.StatusCreated, true, []string{ReviewAudience}, "system:serviceaccount:"+testNamespace+":"+testSAName, testSAUID)
	}))
	t.Cleanup(rs.server.Close)
	t.Cleanup(func() {
		rs.mu.Lock()
		defer rs.mu.Unlock()
		if rs.lastErr != "" {
			t.Errorf("the fake API server saw a bad review: %s", rs.lastErr)
		}
	})
	rs.caFile = filepath.Join(t.TempDir(), "ca.crt")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rs.server.Certificate().Raw})
	if err := os.WriteFile(rs.caFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return rs
}

func (rs *reviewServer) fail(msg string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.lastErr = msg
}

func (rs *reviewServer) set(answer func(w http.ResponseWriter, r *http.Request, spec reviewSpec)) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.answer = answer
}

// anyBearer accepts any reviewer token (a test that rotates the file).
func (rs *reviewServer) anyBearer() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.bearer = ""
}

func (rs *reviewServer) count() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.calls
}

func writeReview(w http.ResponseWriter, status int, authenticated bool, audiences []string, username, uid string) {
	var review struct {
		Status struct {
			Authenticated bool     `json:"authenticated"`
			Audiences     []string `json:"audiences"`
			User          struct {
				Username string `json:"username"`
				UID      string `json:"uid"`
			} `json:"user"`
		} `json:"status"`
	}
	review.Status.Authenticated = authenticated
	review.Status.Audiences = audiences
	review.Status.User.Username = username
	review.Status.User.UID = uid
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(review)
}

// subjectToken is a JWT-shaped projected token: TokenReview is the only
// authority, the validator reads exp alone after a successful review. Every
// segment carries the subject marker.
func subjectToken(exp time.Time) string {
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"alg":"RS256","kid":"` + subjectMarker + `"}`))
	payload := enc.EncodeToString([]byte(`{"exp":` + jsonInt(exp.Unix()) + `,"sub":"` + subjectMarker + `"}`))
	return header + "." + payload + "." + enc.EncodeToString([]byte(subjectMarker+"-signature"))
}

func jsonInt(v int64) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}

// fakeRegistry stands in for the workload store. It narrows as the store's
// Registration.Narrow does: the audience must be registered, every requested
// action allowed, and an empty request yields an empty scope.
type fakeRegistry struct {
	mu       sync.Mutex
	inputs   []Identity
	reg      Registration
	allowed  []string
	err      error
	resolved int
}

func (f *fakeRegistry) Resolve(_ context.Context, id Identity) (Registration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inputs = append(f.inputs, id)
	f.resolved++
	if f.err != nil {
		return Registration{}, f.err
	}
	reg := f.reg
	audiences := append([]string(nil), reg.Audiences...)
	allowed := append([]string(nil), f.allowed...)
	reg.Narrow = func(audience string, scope []string) ([]string, error) {
		found := false
		for _, a := range audiences {
			if a == audience {
				found = true
			}
		}
		if !found {
			return nil, errFakeRefused
		}
		out := []string{}
		seen := map[string]bool{}
		for _, s := range scope {
			ok := false
			for _, a := range allowed {
				if a == s {
					ok = true
				}
			}
			if !ok {
				return nil, errFakeRefused
			}
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
		return out, nil
	}
	return reg, nil
}

var errFakeRefused = workload.ErrRefused

// refusedBecause is the store's refusal for reason; "" is a refusal the store
// did not name.
func refusedBecause(reason string) error {
	if reason == "" {
		return errors.New("workload: refused")
	}
	return &workload.RefusalError{Reason: workload.Reason(reason)}
}

type harness struct {
	t        *testing.T
	now      time.Time
	review   *reviewServer
	registry *fakeRegistry
	signer   Signer
	keySeed  []byte

	auditMu  sync.Mutex
	audits   []audit.AuditEvent
	auditErr error

	logs   *lockedBuffer
	reader *sdkmetric.ManualReader
	server http.Handler
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type harnessOption func(*harness, *Deps)

func newHarness(t *testing.T, opts ...harnessOption) *harness {
	t.Helper()
	h := &harness{t: t, now: time.Unix(1_900_000_000, 0).UTC(), logs: &lockedBuffer{}}
	reviewer := "reviewer-" + reviewerMarker
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(reviewer+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	h.review = newReviewServer(t, reviewer)
	h.registry = &fakeRegistry{
		reg: Registration{
			PrincipalID: uuid.MustParse("6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e"),
			BindingID:   uuid.MustParse("7a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d"),
			Revision:    3,
			Audiences:   []string{"auth-service"},
		},
		allowed: []string{testAction, testOtherAction},
	}
	h.signer, h.keySeed = newTestSigner(t)
	validator, err := NewReviewValidator(ReviewConfig{
		APIServerURL: h.review.server.URL, CAFile: h.review.caFile, ReviewerTokenFile: tokenFile,
		TrustDomain: testTrustDomain, Now: h.clock,
	})
	if err != nil {
		t.Fatalf("NewReviewValidator: %v", err)
	}
	h.reader = sdkmetric.NewManualReader()
	deps := Deps{
		Validator: validator, Registry: h.registry, RefusalReason: workload.RefusalReason, Signer: h.signer,
		Audit: h.writeAudit, IssuerURL: testIssuerURL, ClusterIssuer: testClusterIssuer,
		Logger: slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Meter:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(h.reader)).Meter("test"),
		Now:    h.clock,
	}
	for _, opt := range opts {
		opt(h, &deps)
	}
	routes, err := Routes(deps)
	if err != nil {
		t.Fatalf("Routes: %v", err)
	}
	server, err := httpapi.NewServer(httpapi.ServerOptions{
		Address: "127.0.0.1:0", Routes: routes, RequestTimeout: 30 * time.Second, MaxBodyBytes: 1 << 20,
		RateLimit: 1e6, RateLimitBurst: 1e6, Logger: deps.Logger,
		Middleware: []func(http.Handler) http.Handler{requestid.Middleware},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	h.server = server.Handler()
	return h
}

func (h *harness) clock() time.Time { return h.now }

func (h *harness) writeAudit(_ context.Context, event audit.AuditEvent) error {
	h.auditMu.Lock()
	defer h.auditMu.Unlock()
	if h.auditErr != nil {
		return h.auditErr
	}
	if err := audit.ValidateAttributes(event.Attributes); err != nil {
		return err
	}
	h.audits = append(h.audits, event)
	return nil
}

func (h *harness) auditRows() []audit.AuditEvent {
	h.auditMu.Lock()
	defer h.auditMu.Unlock()
	return append([]audit.AuditEvent(nil), h.audits...)
}

func newTestSigner(t *testing.T) (*token.Signer, []byte) {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "signing.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return token.NewFileSigner(token.FileConfig{KeyPath: path, KeyID: "auth-signing-test"}), private.Seed()
}

// form is a valid exchange request body for audience and scope ("" omits
// scope).
func form(subject, audience, scope string) url.Values {
	v := url.Values{
		"grant_type":         {GrantTypeTokenExchange},
		"subject_token":      {subject},
		"subject_token_type": {TokenTypeJWT},
		"audience":           {audience},
	}
	if scope != "" {
		v.Set("scope", scope)
	}
	return v
}

func (h *harness) post(body string) *httptest.ResponseRecorder {
	return h.postWith(body, "application/x-www-form-urlencoded", Pattern)
}

func (h *harness) postWith(body, contentType, target string) *httptest.ResponseRecorder {
	h.t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	h.server.ServeHTTP(rec, req)
	return rec
}

func (h *harness) validSubject() string { return subjectToken(h.now.Add(time.Hour)) }

// metricPoints returns the outcome counter's Kubernetes-subject points and the
// request shape faults (subject none) as "outcome/reason/audience=value".
func (h *harness) metricPoints() map[string]int64 {
	h.t.Helper()
	out := map[string]int64{}
	for key, value := range collectCounter(h.t, h.reader, "auth.workload.exchanges") {
		subject, series, _ := strings.Cut(key, "|")
		switch {
		case subject == subjectKubernetes:
			out[series] = value
		case subject == subjectNone && strings.HasPrefix(series, outcomeInvalid+"/"):
			out[series] = value
		case subject == subjectNone:
			h.t.Fatalf("series %s is counted under subject none: only a shape fault is", series)
		}
	}
	return out
}

// bootstrapPoints returns the outcome counter's bootstrap-subject points as
// "outcome/reason/audience=value".
func (h *harness) bootstrapPoints() map[string]int64 {
	h.t.Helper()
	out := map[string]int64{}
	for key, value := range collectCounter(h.t, h.reader, "auth.workload.exchanges") {
		if subject, series, _ := strings.Cut(key, "|"); subject == subjectBootstrap {
			out[series] = value
		}
	}
	return out
}

func readAll(t *testing.T, r io.Reader) string {
	t.Helper()
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
