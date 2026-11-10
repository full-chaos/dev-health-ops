package livee2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
)

const (
	testReviewer = "reviewer-token-value"
	testAudience = "dev-health-workload-exchange"
	reviewPath   = "/apis/authentication.k8s.io/v1/tokenreviews"
)

func testConfig(subject string) TokenReviewConfig {
	return TokenReviewConfig{
		Subject: subject, ReviewerToken: testReviewer, Audience: testAudience,
		Namespace: "live-e2e", ServiceAccount: "web", UID: "0b6c1d7e-3f2a-4a56-9d0c-5f3a1c2b7e11",
	}
}

func post(t *testing.T, h http.Handler, bearer, token string, audiences []string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview",
		"spec": map[string]any{"token": token, "audiences": audiences},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, reviewPath, strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+bearer)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestSubjectJWSIsThreeSegmentsWithFutureExp(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	token, err := NewSubjectJWS(now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		t.Fatalf("want 3 non-empty segments, got %d", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatal(err)
	}
	if want := now.Add(time.Hour).Unix(); claims.Exp != want {
		t.Fatalf("exp = %d, want %d", claims.Exp, want)
	}
}

func TestTokenReviewFakeAuthenticatesOnlyTheWrittenSubject(t *testing.T) {
	issued := time.Unix(1_800_000_000, 0).UTC()
	subject, err := NewSubjectJWS(issued, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(subject)
	cfg.Now = func() time.Time { return issued.Add(time.Minute) }
	h := TokenReviewHandler(cfg)
	// A written subject without a readable exp claim is never authenticated: not a JWS, a payload without exp.
	enc := base64.RawURLEncoding
	for _, noExp := range []string{"a.b.c", enc.EncodeToString([]byte(`{"alg":"EdDSA"}`)) + "." + enc.EncodeToString([]byte(`{"iss":"x"}`)) + ".c2ln"} {
		if rec := post(t, TokenReviewHandler(testConfig(noExp)), testReviewer, noExp, []string{testAudience}); strings.Contains(rec.Body.String(), `"authenticated":true`) {
			t.Fatalf("FAIL: written subject %q without an exp claim was authenticated", noExp)
		}
	}
	for name, tc := range map[string]struct {
		bearer, token string
		audiences     []string
		status        int
		authenticated bool
	}{
		"the written subject":  {testReviewer, subject, []string{testAudience}, http.StatusCreated, true},
		"another subject":      {testReviewer, "x.y.z", []string{testAudience}, http.StatusCreated, false},
		"audience not asked":   {testReviewer, subject, []string{"other"}, http.StatusCreated, false},
		"wrong reviewer token": {"nope", subject, []string{testAudience}, http.StatusUnauthorized, false},
	} {
		rec := post(t, h, tc.bearer, tc.token, tc.audiences)
		if rec.Code != tc.status {
			t.Fatalf("%s: status %d, want %d", name, rec.Code, tc.status)
		}
		var out struct {
			Status struct {
				Authenticated bool `json:"authenticated"`
			} `json:"status"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		if out.Status.Authenticated != tc.authenticated {
			t.Fatalf("%s: authenticated = %t, want %t", name, out.Status.Authenticated, tc.authenticated)
		}
	}
	req := httptest.NewRequest(http.MethodGet, reviewPath, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET: status %d, want 405", rec.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "/other", strings.NewReader("{}"))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("other path: status %d, want 404", rec.Code)
	}
}

// The written subject is authenticated only before its exp, as a real TokenReview refuses an expired token. The clock is
// injected: no wall time is measured.
func TestTokenReviewFakeRefusesTheWrittenSubjectAfterItsExp(t *testing.T) {
	issued := time.Unix(1_800_000_000, 0).UTC()
	subject, err := NewSubjectJWS(issued, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var clock time.Time
	cfg := testConfig(subject)
	cfg.Now = func() time.Time { return clock }
	h := TokenReviewHandler(cfg)
	for _, tc := range []struct {
		name string
		at   time.Time
		want bool
	}{
		{"before exp", issued.Add(59 * time.Minute), true},
		{"at exp", issued.Add(time.Hour), false},
		{"after exp", issued.Add(2 * time.Hour), false},
	} {
		clock = tc.at
		rec := post(t, h, testReviewer, subject, []string{testAudience})
		var out struct {
			Status struct {
				Authenticated bool `json:"authenticated"`
			} `json:"status"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || rec.Code != http.StatusCreated {
			t.Fatalf("%s: status %d, body %q", tc.name, rec.Code, rec.Body.String())
		}
		if out.Status.Authenticated != tc.want {
			t.Errorf("FAIL: %s: authenticated = %t, want %t", tc.name, out.Status.Authenticated, tc.want)
		}
	}
}

// The real validator of auth-service (dev-health-go authverify) accepts the fake's answer for the written subject and refuses another token.
func TestTokenReviewFakeSatisfiesTheRealValidator(t *testing.T) {
	now := time.Now().UTC()
	subject, err := NewSubjectJWS(now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewTLSServer(TokenReviewHandler(testConfig(subject)))
	defer srv.Close()
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	validator, err := authverify.NewKubernetesTokenReviewValidator(authverify.KubernetesTokenReviewOptions{
		HTTPClient: client, APIServerURL: srv.URL,
		ReviewerToken: func() (string, error) { return testReviewer, nil },
		Audience:      testAudience, TrustDomain: "live-e2e.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	id, err := validator.Validate(context.Background(), subject)
	if err != nil {
		t.Fatalf("written subject refused: %v", err)
	}
	if id.Namespace != "live-e2e" || id.ServiceAccountName != "web" || id.ServiceAccountUID == "" {
		t.Fatalf("identity = %+v", id)
	}
	other, err := NewSubjectJWS(now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if other == subject {
		t.Fatal("two subjects are equal")
	}
	if _, err := validator.Validate(context.Background(), other); err == nil {
		t.Fatal("another subject was authenticated")
	}
}
