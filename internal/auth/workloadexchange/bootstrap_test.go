package workloadexchange

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

// bootstrapMarker is planted in every bootstrap value a test presents, so a
// test can prove the value reaches no answer, log line, metric or audit row.
const bootstrapMarker = "PLANTEDBOOTSTRAPm3V"

// bootstrapValue is a value of the bootstrap credential's shape, built at run
// time: the prefix, the marker and padding to 43 characters.
func bootstrapValue(fill string) string {
	return workload.BootstrapPrefix + bootstrapMarker + strings.Repeat(fill, 43-len(bootstrapMarker))
}

var testCredentialID = uuid.MustParse("8b3c4d5e-6f7a-4b8c-9d0e-1f2a3b4c5d6e")

// fakeBootstrap stands in for the workload store's bootstrap read. It records
// the digests it is asked for and narrows as Registration.Narrow does.
type fakeBootstrap struct {
	mu      sync.Mutex
	digests [][sha256.Size]byte
	nows    []time.Time
	reg     BootstrapRegistration
	allowed []string
	err     error
}

func (f *fakeBootstrap) ResolveBootstrap(_ context.Context, digest [sha256.Size]byte, now time.Time) (BootstrapRegistration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.digests = append(f.digests, digest)
	f.nows = append(f.nows, now)
	if f.err != nil {
		return BootstrapRegistration{}, f.err
	}
	reg := f.reg
	narrowing := fakeRegistry{reg: reg.Registration, allowed: f.allowed}
	inner, _ := narrowing.Resolve(context.Background(), Identity{})
	reg.Narrow = inner.Narrow
	return reg, nil
}

func (f *fakeBootstrap) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.digests)
}

// withBootstrap turns the bootstrap subject on with a fake registry whose
// credential is live for the test workload until expires (zero: no expiry).
func withBootstrap(f *fakeBootstrap) harnessOption {
	return func(h *harness, d *Deps) {
		f.reg = BootstrapRegistration{
			Registration: Registration{
				PrincipalID: uuid.MustParse("6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e"),
				Revision:    4,
				Audiences:   []string{"auth-service"},
			},
			CredentialID: testCredentialID,
		}
		f.allowed = []string{testAction, testOtherAction}
		d.Bootstrap = f
	}
}

// withoutKubernetes turns the Kubernetes subject off.
func withoutKubernetes() harnessOption {
	return func(_ *harness, d *Deps) { d.Validator, d.Registry, d.ClusterIssuer = nil, nil, "" }
}

func bootstrapForm(value, audience, scope string) url.Values {
	v := form(value, audience, scope)
	v.Set("subject_token_type", TokenTypeWorkloadBootstrap)
	return v
}

func TestABootstrapCredentialGetsAWorkloadTokenForItsAudience(t *testing.T) {
	boot := &fakeBootstrap{}
	h := newHarness(t, withBootstrap(boot), withoutKubernetes())
	value := bootstrapValue("A")
	answer := decodeAnswer(t, h.post(bootstrapForm(value, "auth-service", testAction).Encode()))
	if answer.IssuedTokenType != TokenTypeJWT || answer.TokenType != "Bearer" || answer.ExpiresIn != 300 {
		t.Fatalf("answer %+v", answer)
	}
	claims := verifyIssued(t, h, answer.AccessToken, "auth-service")
	if claims.Subject != session.WirePrincipalID(boot.reg.PrincipalID) || claims.Audience != "auth-service" {
		t.Fatalf("sub %q aud %q", claims.Subject, claims.Audience)
	}
	if claims.ExpiresAt != h.now.Unix()+300 {
		t.Fatalf("exp %d, want now+300", claims.ExpiresAt)
	}
	if len(boot.digests) != 1 || boot.digests[0] != sha256.Sum256([]byte(value)) || !boot.nows[0].Equal(h.now) {
		t.Fatalf("the registry was asked %d time(s), not once for the value's SHA-256 at now", len(boot.digests))
	}
	rows := h.auditRows()
	if len(rows) != 1 {
		t.Fatalf("%d audit rows, want 1", len(rows))
	}
	keys := make([]string, 0, len(rows[0].Attributes))
	for key := range rows[0].Attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if got := strings.Join(keys, ","); got != "actions,audience,credential_id,jti,principal_id,revision" {
		t.Fatalf("audit attribute keys %s", got)
	}
	attrs := rows[0].Attributes
	if attrs["credential_id"] != testCredentialID.String() || attrs["jti"] != claims.TokenID ||
		attrs["principal_id"] != boot.reg.PrincipalID.String() || attrs["revision"] != int64(4) || attrs["audience"] != "auth-service" {
		t.Fatalf("audit attributes %v", attrs)
	}
	if actions, _ := attrs["actions"].([]string); len(actions) != 1 || actions[0] != testAction {
		t.Fatalf("audit actions %v", attrs["actions"])
	}
	if got := h.bootstrapPoints()["issued/issued/auth-service"]; got != 1 {
		t.Fatalf("bootstrap issued counter %d; points %v", got, h.bootstrapPoints())
	}
	if got := h.metricPoints()["issued/issued/auth-service"]; got != 0 {
		t.Fatalf("a bootstrap exchange counted as a Kubernetes one (%d)", got)
	}
}

func TestTheBootstrapLifetimeStopsAtTheCredentialExpiry(t *testing.T) {
	for _, tc := range []struct {
		name string
		left time.Duration
		want int
	}{
		{"expires in 100 s", 100 * time.Second, 100},
		{"expires in 1 s", time.Second, 1},
		{"expires in 1.9 s", 1900 * time.Millisecond, 1},
		{"expires after the ceiling", time.Hour, 300},
	} {
		t.Run(tc.name, func(t *testing.T) {
			boot := &fakeBootstrap{}
			h := newHarness(t, withBootstrap(boot))
			boot.reg.ExpiresAt = h.now.Add(tc.left)
			answer := decodeAnswer(t, h.post(bootstrapForm(bootstrapValue("A"), "auth-service", "").Encode()))
			if answer.ExpiresIn != tc.want {
				t.Fatalf("expires_in %d, want %d", answer.ExpiresIn, tc.want)
			}
			claims := verifyIssued(t, h, answer.AccessToken, "auth-service")
			if claims.ExpiresAt > boot.reg.ExpiresAt.Unix() {
				t.Fatalf("the token outlives its bootstrap credential: exp %d > %d", claims.ExpiresAt, boot.reg.ExpiresAt.Unix())
			}
		})
	}
}

type bootstrapRefusal struct {
	name     string
	value    string
	audience string
	scope    string
	setup    func(f *fakeBootstrap, now time.Time)
	reason   string
	noLookup bool
}

func bootstrapRefusals() []bootstrapRefusal {
	refusedBy := func(reason string) func(*fakeBootstrap, time.Time) {
		return func(f *fakeBootstrap, _ time.Time) { f.err = refusedBecause(reason) }
	}
	return []bootstrapRefusal{
		{name: "no prefix", value: strings.TrimPrefix(bootstrapValue("A"), workload.BootstrapPrefix), reason: "malformed", noLookup: true},
		{name: "another prefix", value: "svc_authsess_" + strings.TrimPrefix(bootstrapValue("A"), workload.BootstrapPrefix), reason: "malformed", noLookup: true},
		{name: "one byte short", value: bootstrapValue("A")[:len(bootstrapValue("A"))-1], reason: "malformed", noLookup: true},
		{name: "one byte long", value: bootstrapValue("A") + "A", reason: "malformed", noLookup: true},
		{name: "outside base64url", value: bootstrapValue("+"), reason: "malformed", noLookup: true},
		{name: "upper-case prefix", value: "FCWLB_" + strings.TrimPrefix(bootstrapValue("A"), workload.BootstrapPrefix), reason: "malformed", noLookup: true},
		{name: "a JWT", value: subjectToken(time.Unix(1_900_003_600, 0)), reason: "malformed", noLookup: true},
		{name: "unknown", value: bootstrapValue("A"), setup: refusedBy("unknown"), reason: "unknown"},
		{name: "revoked", value: bootstrapValue("A"), setup: refusedBy("revoked"), reason: "revoked"},
		{name: "expired", value: bootstrapValue("A"), setup: refusedBy("expired"), reason: "expired"},
		{name: "disabled", value: bootstrapValue("A"), setup: refusedBy("disabled"), reason: "disabled"},
		{name: "no audience", value: bootstrapValue("A"), setup: refusedBy("no_audience"), reason: "no_audience"},
		{name: "store error", value: bootstrapValue("A"), setup: refusedBy("store_error"), reason: "store_error"},
		{name: "unnamed refusal", value: bootstrapValue("A"), setup: refusedBy(""), reason: "resolve_refused"},
		{name: "a Kubernetes-only reason", value: bootstrapValue("A"), setup: refusedBy("uid_mismatch"), reason: "resolve_refused"},
		{name: "audience not a workload audience", value: bootstrapValue("A"), audience: "acr-api", reason: "audience_refused"},
		{name: "audience outside the credential's", value: bootstrapValue("A"), audience: "dev-health-api", reason: "audience_refused"},
		{name: "action not allowed", value: bootstrapValue("A"), scope: "workloads.read admin.everything", reason: "scope_refused"},
		{name: "expires now", value: bootstrapValue("A"), setup: func(f *fakeBootstrap, now time.Time) { f.reg.ExpiresAt = now }, reason: "lifetime_too_short"},
		{name: "expires in under a second", value: bootstrapValue("A"),
			setup: func(f *fakeBootstrap, now time.Time) { f.reg.ExpiresAt = now.Add(999 * time.Millisecond) }, reason: "lifetime_too_short"},
	}
}

// Every refusal of the bootstrap subject is the Kubernetes subject's refusal,
// byte for byte; only the bootstrap counter series names the reason.
func TestEveryBootstrapRefusalAnswersTheSameBytes(t *testing.T) {
	reference := newHarness(t)
	reference.registry.err = refusedBecause("unknown")
	want := reference.post(form(reference.validSubject(), "auth-service", "").Encode())
	if want.Code != http.StatusBadRequest || want.Body.String() != "{\"error\":\"invalid_grant\"}\n" {
		t.Fatalf("reference refusal %d %q", want.Code, want.Body.String())
	}
	for _, tc := range bootstrapRefusals() {
		t.Run(tc.name, func(t *testing.T) {
			boot := &fakeBootstrap{}
			h := newHarness(t, withBootstrap(boot))
			if tc.setup != nil {
				tc.setup(boot, h.now)
			}
			audience := tc.audience
			if audience == "" {
				audience = "auth-service"
			}
			rec := h.post(bootstrapForm(tc.value, audience, tc.scope).Encode())
			if rec.Code != want.Code || rec.Body.String() != want.Body.String() || comparableHeaders(rec.Header()) != comparableHeaders(want.Header()) {
				t.Fatalf("%d %q %s, want the Kubernetes refusal %d %q %s", rec.Code, rec.Body.String(), comparableHeaders(rec.Header()),
					want.Code, want.Body.String(), comparableHeaders(want.Header()))
			}
			label := audienceLabel(audience)
			if got := h.bootstrapPoints()["refused/"+tc.reason+"/"+label]; got != 1 {
				t.Fatalf("counter refused/%s/%s = %d, want 1; points %v", tc.reason, label, got, nonZero(h.bootstrapPoints()))
			}
			if tc.noLookup && boot.calls() != 0 {
				t.Fatalf("a malformed value reached the registry %d time(s)", boot.calls())
			}
			if n := len(h.auditRows()); n != 0 {
				t.Fatalf("%d audit rows for a refusal", n)
			}
			if h.review.count() != 0 || h.registry.resolved != 0 {
				t.Fatalf("a bootstrap request reached TokenReview (%d) or the binding registry (%d)", h.review.count(), h.registry.resolved)
			}
		})
	}
}

func nonZero(points map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for k, v := range points {
		if v != 0 {
			out[k] = v
		}
	}
	return out
}

// The subject_token_type alone selects the path: a bootstrap value typed as a
// JWT goes to TokenReview only, and a JWT typed as a bootstrap value is
// refused by shape without a review or a store read.
func TestTheSubjectTypeAloneSelectsThePath(t *testing.T) {
	boot := &fakeBootstrap{}
	h := newHarness(t, withBootstrap(boot))
	h.review.set(func(w http.ResponseWriter, _ *http.Request, spec reviewSpec) {
		writeReview(w, http.StatusCreated, false, nil, "", "")
	})
	rec := h.post(form(bootstrapValue("A"), "auth-service", "").Encode())
	if rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_grant\"}\n" {
		t.Fatalf("a bootstrap value typed as a JWT: %d %q", rec.Code, rec.Body.String())
	}
	if h.review.count() != 1 || boot.calls() != 0 {
		t.Fatalf("reviews %d, bootstrap reads %d; want 1 and 0", h.review.count(), boot.calls())
	}
	if got := h.metricPoints()["refused/subject_token_invalid/auth-service"]; got != 1 {
		t.Fatalf("Kubernetes counter subject_token_invalid = %d", got)
	}
	rec = h.post(bootstrapForm(h.validSubject(), "auth-service", "").Encode())
	if rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_grant\"}\n" {
		t.Fatalf("a JWT typed as a bootstrap value: %d %q", rec.Code, rec.Body.String())
	}
	if h.review.count() != 1 || boot.calls() != 0 || h.registry.resolved != 0 {
		t.Fatalf("reviews %d, bootstrap reads %d, binding reads %d; want 1, 0, 0", h.review.count(), boot.calls(), h.registry.resolved)
	}
	if got := h.bootstrapPoints()["refused/malformed/auth-service"]; got != 1 {
		t.Fatalf("bootstrap counter malformed = %d", got)
	}
}

// A subject that is off is a request shape fault, answered before any review
// or store read, as an unknown subject_token_type is.
func TestASubjectThatIsOffIsAShapeFault(t *testing.T) {
	kubernetesOnly := newHarness(t)
	assertInvalidRequest(t, kubernetesOnly, kubernetesOnly.post(bootstrapForm(bootstrapValue("A"), "auth-service", "").Encode()))
	boot := &fakeBootstrap{}
	bootstrapOnly := newHarness(t, withBootstrap(boot), withoutKubernetes())
	assertInvalidRequest(t, bootstrapOnly, bootstrapOnly.post(form(bootstrapOnly.validSubject(), "auth-service", "").Encode()))
	unknownType := newHarness(t, withBootstrap(boot))
	unknown := bootstrapForm(bootstrapValue("A"), "auth-service", "")
	unknown.Set("subject_token_type", "urn:dev-health:params:oauth:token-type:workload-bootstrap-v2")
	assertInvalidRequest(t, unknownType, unknownType.post(unknown.Encode()))
	if boot.calls() != 0 {
		t.Fatalf("a shape fault reached the bootstrap registry (%d) or TokenReview", boot.calls())
	}
}

func TestBootstrapDependencyFailuresAnswer503(t *testing.T) {
	t.Run("signer", func(t *testing.T) {
		h := newHarness(t, withBootstrap(&fakeBootstrap{}), func(h *harness, d *Deps) { d.Signer = failingSigner{Signer: d.Signer} })
		assertBootstrapUnavailable(t, h, h.post(bootstrapForm(bootstrapValue("A"), "auth-service", "").Encode()), "signer_unavailable")
	})
	t.Run("audit", func(t *testing.T) {
		h := newHarness(t, withBootstrap(&fakeBootstrap{}))
		h.auditErr = errors.New("audit down")
		assertBootstrapUnavailable(t, h, h.post(bootstrapForm(bootstrapValue("A"), "auth-service", "").Encode()), "audit_unavailable")
	})
}

func assertBootstrapUnavailable(t *testing.T, h *harness, rec *httptest.ResponseRecorder, reason string) {
	t.Helper()
	res := rec.Result()
	body := readAll(t, res.Body)
	if res.StatusCode != http.StatusServiceUnavailable || body != "{\"error\":\"temporarily_unavailable\"}\n" {
		t.Fatalf("%d %q, want 503 temporarily_unavailable", res.StatusCode, body)
	}
	if got := h.bootstrapPoints()["unavailable/"+reason+"/auth-service"]; got != 1 {
		t.Fatalf("bootstrap counter unavailable/%s = %d; points %v", reason, got, nonZero(h.bootstrapPoints()))
	}
}

// The value never leaves the request: no answer, log line, metric or audit
// row carries it or its marker, on any outcome.
func TestTheBootstrapValueReachesNoAnswerLogMetricOrAuditRow(t *testing.T) {
	boot := &fakeBootstrap{}
	h := newHarness(t, withBootstrap(boot))
	var bodies []string
	bodies = append(bodies, h.post(bootstrapForm(bootstrapValue("A"), "auth-service", testAction).Encode()).Body.String())
	for _, tc := range bootstrapRefusals() {
		boot.err = nil
		if tc.setup != nil {
			tc.setup(boot, h.now)
		}
		audience := tc.audience
		if audience == "" {
			audience = "auth-service"
		}
		bodies = append(bodies, h.post(bootstrapForm(tc.value, audience, tc.scope).Encode()).Body.String())
	}
	var audit strings.Builder
	for _, row := range h.auditRows() {
		for k, v := range row.Attributes {
			audit.WriteString(k + "=" + strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(toString(v), "\n", " "), "\t", " ")) + "\n")
		}
	}
	for surface, text := range map[string]string{
		"answers": strings.Join(bodies, "\n"), "logs": h.logs.String(), "metrics": dumpMetrics(t, h.reader), "audit": audit.String(),
	} {
		if strings.Contains(text, bootstrapMarker) || strings.Contains(text, workload.BootstrapPrefix) {
			t.Errorf("the %s carry the bootstrap value", surface)
		}
	}
	if len(h.auditRows()) != 1 {
		t.Fatalf("%d audit rows, want the one issued row", len(h.auditRows()))
	}
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []string:
		return strings.Join(x, " ")
	}
	return ""
}

func TestEveryBootstrapCounterSeriesIsDeclaredAtZero(t *testing.T) {
	h := newHarness(t, withBootstrap(&fakeBootstrap{}))
	points := h.bootstrapPoints()
	want := map[string]bool{}
	for _, aud := range []string{"auth-service", "dev-health-api", "none"} {
		for _, reason := range []string{
			"malformed", "unknown", "revoked", "expired", "disabled", "no_audience", "store_error", "resolve_refused",
			"audience_refused", "scope_refused", "lifetime_too_short",
		} {
			want["refused/"+reason+"/"+aud] = true
		}
		for _, reason := range []string{
			"signer_unavailable", "audit_unavailable", "selfcheck_contract", "selfcheck_unknown_kid", "selfcheck_signature",
			"selfcheck_issuer", "selfcheck_audience", "selfcheck_not_yet_valid", "selfcheck_expired",
		} {
			want["unavailable/"+reason+"/"+aud] = true
		}
	}
	for _, aud := range []string{"auth-service", "dev-health-api"} {
		want["issued/issued/"+aud] = true
	}
	var missing, extra []string
	for key := range want {
		if v, ok := points[key]; !ok || v != 0 {
			missing = append(missing, key)
		}
	}
	for key := range points {
		if !want[key] {
			extra = append(extra, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("bootstrap series not declared at 0: %v; outside the closed set: %v", missing, extra)
	}
}

func TestRoutesNeedsOneSubject(t *testing.T) {
	h := newHarness(t)
	bootstrapOnly := Deps{Bootstrap: &fakeBootstrap{}, RefusalReason: workload.RefusalReason, Signer: h.signer,
		Audit: h.writeAudit, IssuerURL: testIssuerURL}
	if _, err := Routes(bootstrapOnly); err != nil {
		t.Fatalf("a bootstrap-only exchange was refused: %v", err)
	}
	for name, mutate := range map[string]func(*Deps){
		"no subject":                 func(d *Deps) { d.Bootstrap = nil },
		"refusal reason":             func(d *Deps) { d.RefusalReason = nil },
		"signer":                     func(d *Deps) { d.Signer = nil },
		"audit":                      func(d *Deps) { d.Audit = nil },
		"issuer":                     func(d *Deps) { d.IssuerURL = "" },
		"validator without registry": func(d *Deps) { d.Validator = hungValidator{} },
		"validator without cluster": func(d *Deps) {
			d.Validator, d.Registry = hungValidator{}, h.registry
		},
	} {
		d := bootstrapOnly
		mutate(&d)
		if _, err := Routes(d); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
