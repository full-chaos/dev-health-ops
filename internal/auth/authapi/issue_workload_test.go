package authapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

const (
	compatCounterName = "auth.session.issue_compat_uses"
	outcomeCounter    = "auth.api.session_route_outcomes"
	authsessClass     = "internal_svc_authsess_token"
)

// issueCountingKeys counts reads of the signer's key set and can fail them.
type issueCountingKeys struct {
	inner KeySetSource
	reads int
	err   error
}

func (k *issueCountingKeys) KeySet(ctx context.Context) (token.KeySet, error) {
	k.reads++
	if k.err != nil {
		return token.KeySet{}, k.err
	}
	return k.inner.KeySet(ctx)
}

// issueCountingUsers counts every read of the users store.
type issueCountingUsers struct {
	fakeUsers
	calls int
}

func (c *issueCountingUsers) Memberships(ctx context.Context, id uuid.UUID) ([]Membership, error) {
	c.calls++
	return c.fakeUsers.Memberships(ctx, id)
}

func (c *issueCountingUsers) State(ctx context.Context, id uuid.UUID) (UserState, bool, error) {
	c.calls++
	return c.fakeUsers.State(ctx, id)
}

func (c *issueCountingUsers) Role(ctx context.Context, user, org uuid.UUID) (*string, bool, error) {
	c.calls++
	return c.fakeUsers.Role(ctx, user, org)
}

type workloadIssueFixture struct {
	*compatFixture
	signer  *token.Signer
	keys    *issueCountingKeys
	users   *issueCountingUsers
	reader  *sdkmetric.ManualReader
	subject string
}

func newWorkloadIssueFixture(t *testing.T) *workloadIssueFixture {
	t.Helper()
	signer, _, _ := fileSigner(t)
	f := newCompatFixture(t)
	keys := &issueCountingKeys{inner: signer}
	subject := session.WirePrincipalID(uuid.New())
	workload, err := NewSessionIssueWorkload(SessionIssueWorkloadConfig{IssuerURL: goldenIssuer, Keys: keys, Subject: subject})
	if err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	users := &issueCountingUsers{fakeUsers: f.users}
	f.workload, f.usersWrap, f.meter = workload, users, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	f.buildCompat()
	return &workloadIssueFixture{compatFixture: f, signer: signer, keys: keys, users: users, reader: reader, subject: subject}
}

// mint signs a workload token with typ; mutate edits the claims first. It
// returns the Authorization header and the jti.
func (w *workloadIssueFixture) mint(typ string, mutate func(*contracts.WorkloadTokenClaims)) (map[string]string, string) {
	w.t.Helper()
	now := time.Now()
	claims := contracts.WorkloadTokenClaims{
		Iss: goldenIssuer, Aud: "auth-service", Sub: w.subject, PrincipalType: "workload", Jti: uuid.NewString(),
		Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300,
	}
	if mutate != nil {
		mutate(&claims)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		w.t.Fatal(err)
	}
	compact, _, err := w.signer.Sign(context.Background(), typ, payload)
	if err != nil {
		w.t.Fatal(err)
	}
	return map[string]string{"Authorization": "Bearer " + compact}, claims.Jti
}

func (w *workloadIssueFixture) counter(name string, want map[string]string) (total int64, series int) {
	w.t.Helper()
	var rm metricdata.ResourceMetrics
	if err := w.reader.Collect(context.Background(), &rm); err != nil {
		w.t.Fatal(err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != name || !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				match := true
				for k, v := range want {
					got, _ := point.Attributes.Value(attribute.Key(k))
					if got.AsString() != v {
						match = false
					}
				}
				if match {
					total += point.Value
					series++
				}
			}
		}
	}
	return total, series
}

// requireSuccessReasonsAreClosed asserts that every success the route recorded
// carries a reason of the closed auth-outcome vocabulary.
func (w *workloadIssueFixture) requireSuccessReasonsAreClosed() {
	w.t.Helper()
	var rm metricdata.ResourceMetrics
	if err := w.reader.Collect(context.Background(), &rm); err != nil {
		w.t.Fatal(err)
	}
	var seen int
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != outcomeCounter || !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				if result, _ := point.Attributes.Value("result"); result.AsString() != "success" || point.Value == 0 {
					continue
				}
				seen++
				reason, _ := point.Attributes.Value("reason_code")
				if !slices.Contains(contracts.AuthOutcomeSuccessReasons, reason.AsString()) {
					w.t.Fatalf("a success was recorded with reason_code %q, outside the closed vocabulary %v", reason.AsString(), contracts.AuthOutcomeSuccessReasons)
				}
			}
		}
	}
	if seen == 0 {
		w.t.Fatal("no success outcome was recorded")
	}
}

// nothingRead asserts a refusal touched no store: no credential check, no
// users read, no projection, no session.
func (w *workloadIssueFixture) nothingRead(name string) {
	w.t.Helper()
	if len(w.creds.seen) != 0 || w.users.calls != 0 || len(w.sessions.created) != 0 ||
		len(w.projector.userCalls) != 0 || len(w.projector.orgCalls) != 0 {
		w.t.Fatalf("%s: a refusal read stores: checks %d users %d sessions %d projections %d/%d", name,
			len(w.creds.seen), w.users.calls, len(w.sessions.created), len(w.projector.userCalls), len(w.projector.orgCalls))
	}
}

func (w *workloadIssueFixture) issue(header map[string]string) *httptest.ResponseRecorder {
	return w.do(http.MethodPost, PatternIssueSession, issueBody(w.principal, w.org.String(), "pwd"), header)
}

// refusalWithoutPerRequestFields checks that a refusal body carries the two
// per-request fields in their normal shape (a req_ request id and an RFC 3339
// time) and returns the rest of the body: every other field and the key set.
// Two refusals with the same return value have the same schema_version, status,
// reason_code and members; only request_id and occurred_at may differ.
func refusalWithoutPerRequestFields(t *testing.T, body []byte) string {
	t.Helper()
	doc := decodeObject(t, body)
	if id, _ := doc["request_id"].(string); !strings.HasPrefix(id, "req_") || len(id) <= len("req_") {
		t.Fatalf("request_id %v is not a req_ id: %s", doc["request_id"], body)
	}
	if at, _ := doc["occurred_at"].(string); at == "" {
		t.Fatalf("occurred_at missing: %s", body)
	} else if _, err := time.Parse(time.RFC3339Nano, at); err != nil {
		t.Fatalf("occurred_at %q is not RFC 3339: %v", at, err)
	}
	delete(doc, "request_id")
	delete(doc, "occurred_at")
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// T1: the go-api workload token (aud auth-service, the configured subject)
// issues a session, with no service credential check, and the audit row names
// the workload as the actor and its jti, never a credential id.
func TestIssueSessionAcceptsTheGoAPIWorkloadToken(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	header, jti := w.mint("workload+jwt", nil)
	response := w.issue(header)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if len(w.creds.seen) != 0 {
		t.Fatalf("the workload path checked a service credential: %v", w.creds.seen)
	}
	if len(w.sessions.created) != 1 {
		t.Fatalf("%d sessions created", len(w.sessions.created))
	}
	event := w.sessions.created[0].event
	wantActor := uuidOfWire(t, w.subject).String()
	if event.ActorPrincipalID == nil || *event.ActorPrincipalID != wantActor {
		t.Fatalf("actor %v, want the workload principal %s", event.ActorPrincipalID, wantActor)
	}
	if event.SubjectPrincipalID == nil || *event.SubjectPrincipalID != w.principal.String() {
		t.Fatalf("subject %v, want the user", event.SubjectPrincipalID)
	}
	if event.Attributes["workload_jti"] != jti {
		t.Fatalf("workload_jti %v, want %s", event.Attributes["workload_jti"], jti)
	}
	if _, has := event.Attributes["credential_id"]; has {
		t.Fatalf("a workload row names a credential_id: %v", event.Attributes)
	}
	if event.Attributes["source"] != "go-api-delegated" || event.Attributes["reason_code"] != "accepted" {
		t.Fatalf("attributes %v", event.Attributes)
	}
	body := decodeObject(t, response.Body.Bytes())
	if body["refresh_token"] == "" || body["token_type"] != "bearer" {
		t.Fatalf("body %s", response.Body)
	}
	if got, _ := w.counter(outcomeCounter, map[string]string{"operation": "session_issue", "result": "success", "reason_code": "accepted"}); got != 1 {
		t.Fatalf("session_issue success outcomes with reason accepted = %d, want 1", got)
	}
	w.requireSuccessReasonsAreClosed()
	if got, _ := w.counter(compatCounterName, nil); got != 0 {
		t.Fatalf("a workload issue counted %d compat uses", got)
	}
}

func uuidOfWire(t *testing.T, subject string) uuid.UUID {
	t.Helper()
	id, ok := parseWirePrincipal(subject)
	if !ok {
		t.Fatalf("subject %q is not a principal id", subject)
	}
	return id
}

// T2-T10: every wrong credential gets the same 401 error.v1 body (only the per-request
// request_id and occurred_at differ), and no store is read.
// The prefix, shape, typ, audience and subject refusals also read no key set.
func TestIssueSessionRefusesWrongCredentialsBeforeAnyRead(t *testing.T) {
	forty3 := strings.Repeat("A", 43)
	preKey := map[string]func(*workloadIssueFixture) map[string]string{
		"wrong audience": func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Aud = "dev-health-api" })
			return h
		},
		"wrong subject": func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Sub = session.WirePrincipalID(uuid.New()) })
			return h
		},
		"user access token typ": func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("at+jwt", nil)
			return h
		},
		"envelope typ": func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("jwt", nil)
			return h
		},
		"hs256 edge token": func(w *workloadIssueFixture) map[string]string { return w.bearer(w.grant()) },
		"fcacr_":           func(*workloadIssueFixture) map[string]string { return bearerOf("fcacr_" + forty3) },
		"fcpush_":          func(*workloadIssueFixture) map[string]string { return bearerOf("fcpush_" + forty3) },
		"fcwlb_":           func(*workloadIssueFixture) map[string]string { return bearerOf("fcwlb_" + forty3) },
		"svc_acr_":         func(*workloadIssueFixture) map[string]string { return bearerOf("svc_acr_" + forty3) },
		"svc_worker_":      func(*workloadIssueFixture) map[string]string { return bearerOf("svc_worker_" + forty3) },
		"whsec_":           func(*workloadIssueFixture) map[string]string { return bearerOf("whsec_" + forty3) },
		"refresh credential": func(*workloadIssueFixture) map[string]string {
			return bearerOf("dhr1_" + forty3)
		},
		"opaque value":  func(*workloadIssueFixture) map[string]string { return bearerOf("good") },
		"two segments":  func(*workloadIssueFixture) map[string]string { return bearerOf("a.b") },
		"four segments": func(*workloadIssueFixture) map[string]string { return bearerOf("a.b.c.d") },
		"not base64url": func(*workloadIssueFixture) map[string]string { return bearerOf("@@@.@@@.@@@") },
		"oversized":     func(*workloadIssueFixture) map[string]string { return bearerOf(strings.Repeat("a", 6000)) },
		"oversized jws": func(*workloadIssueFixture) map[string]string {
			return bearerOf(strings.Repeat("a", 3000) + "." + strings.Repeat("b", 3000) + ".c")
		},
		"oversized workload token": func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("workload+jwt", nil)
			return bearerOf(strings.TrimPrefix(h["Authorization"], "Bearer ") + strings.Repeat("A", 3000))
		},
		"empty bearer": func(*workloadIssueFixture) map[string]string { return map[string]string{"Authorization": "Bearer "} },
		"basic scheme": func(*workloadIssueFixture) map[string]string {
			return map[string]string{"Authorization": "Basic Z29vZA=="}
		},
		"longer typ": func(w *workloadIssueFixture) map[string]string { h, _ := w.mint("workload+jwt-x", nil); return h },
	}
	var golden string
	for name, build := range preKey {
		w := newWorkloadIssueFixture(t)
		response := w.issue(build(w))
		expectError(t, response, http.StatusUnauthorized, "invalid_credentials")
		w.nothingRead(name)
		if w.keys.reads != 0 {
			t.Fatalf("%s: a refusal read the key set %d times before the verdict", name, w.keys.reads)
		}
		got := refusalWithoutPerRequestFields(t, response.Body.Bytes())
		if golden == "" {
			golden = got
		}
		if got != golden {
			t.Fatalf("%s: refusal body %s differs from %s", name, got, golden)
		}
	}
	// The same body, but for the per-request fields, as today's refusal of a revoked svc_authsess_ credential.
	w := newWorkloadIssueFixture(t)
	w.creds.refusal = servicecred.RefusedRevoked
	if got := refusalWithoutPerRequestFields(t, w.issue(issuerBearer).Body.Bytes()); got != golden {
		t.Fatalf("a revoked service credential answers %s, want the workload refusal %s", got, golden)
	}
}

func bearerOf(value string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + value}
}

// T10: a token that passes the class, audience and subject peek but fails the
// verifier is refused: no store is read, and the body is the same.
func TestIssueSessionRefusesATokenTheVerifierRefuses(t *testing.T) {
	cases := map[string]func(*workloadIssueFixture) map[string]string{
		"expired": func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) {
				c.Iat, c.Nbf, c.Exp = c.Iat-400, c.Nbf-400, c.Exp-400
			})
			return h
		},
		"not yet valid": func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Iat, c.Nbf, c.Exp = c.Iat+120, c.Nbf+120, c.Exp+120 })
			return h
		},
		"wrong issuer": func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Iss = "https://other.example.test" })
			return h
		},
		"lifetime over the ceiling": func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Exp += 3600 })
			return h
		},
		"tampered signature": func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("workload+jwt", nil)
			token := strings.TrimPrefix(h["Authorization"], "Bearer ")
			last := token[len(token)-1]
			swap := byte('A')
			if last == 'A' {
				swap = 'B'
			}
			return bearerOf(token[:len(token)-1] + string(swap))
		},
		"org claim": func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("workload+jwt", nil)
			parts := strings.Split(strings.TrimPrefix(h["Authorization"], "Bearer "), ".")
			parts[1] += "AA"
			return bearerOf(strings.Join(parts, "."))
		},
	}
	var golden string
	for name, build := range cases {
		w := newWorkloadIssueFixture(t)
		response := w.issue(build(w))
		expectError(t, response, http.StatusUnauthorized, "invalid_credentials")
		w.nothingRead(name)
		got := refusalWithoutPerRequestFields(t, response.Body.Bytes())
		if golden == "" {
			golden = got
		}
		if got != golden {
			t.Fatalf("%s: refusal body %s differs from %s", name, got, golden)
		}
	}
}

// T11: a key set that cannot be read is a 503, never a pass.
func TestIssueSessionAnswers503WhenTheKeySetIsUnavailable(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	header, _ := w.mint("workload+jwt", nil)
	w.keys.err = errors.New("custody down")
	expectError(t, w.issue(header), http.StatusServiceUnavailable, "dependency_unavailable")
	w.nothingRead("key outage")
}

// T12: svc_authsess_ still issues, and every accepted use is counted by class.
func TestIssueSessionCountsAcceptedServiceCredentialUse(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	for range 2 {
		if response := w.issue(issuerBearer); response.Code != http.StatusOK {
			t.Fatalf("status %d: %s", response.Code, response.Body)
		}
	}
	if len(w.creds.seen) != 2 || w.creds.seen[0] != issuerToken {
		t.Fatalf("checks %v", w.creds.seen)
	}
	if got, _ := w.counter(compatCounterName, map[string]string{"class": authsessClass}); got != 2 {
		t.Fatalf("compat uses = %d, want 2", got)
	}
	w.requireSuccessReasonsAreClosed()
}

// T13: only an accepted svc_authsess_ use counts: not a refusal, not a
// workload accept, not a workload refusal.
func TestTheCompatCounterCountsOnlyAcceptedServiceCredentialUse(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	w.creds.refusal = servicecred.RefusedRevoked
	expectError(t, w.issue(issuerBearer), http.StatusUnauthorized, "invalid_credentials")
	w.creds.err = errors.New("db down")
	expectError(t, w.issue(issuerBearer), http.StatusServiceUnavailable, "store_unavailable")
	w.creds.refusal, w.creds.err = "", nil
	good, _ := w.mint("workload+jwt", nil)
	if response := w.issue(good); response.Code != http.StatusOK {
		t.Fatalf("workload issue: %d %s", response.Code, response.Body)
	}
	bad, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Aud = "dev-health-api" })
	expectError(t, w.issue(bad), http.StatusUnauthorized, "invalid_credentials")
	if got, _ := w.counter(compatCounterName, nil); got != 0 {
		t.Fatalf("compat uses = %d, want 0", got)
	}
}

// T14: the compat series exists at 0 before any use, with the class label
// alone, set to the credential class id.
func TestTheCompatCounterIsDeclaredAtZeroWithTheClassLabelOnly(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	var rm metricdata.ResourceMetrics
	if err := w.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != compatCounterName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok || len(sum.DataPoints) != 1 {
				t.Fatalf("compat counter data %+v, want one series", m.Data)
			}
			point := sum.DataPoints[0]
			attrs := point.Attributes.ToSlice()
			if point.Value != 0 || len(attrs) != 1 || string(attrs[0].Key) != "class" || attrs[0].Value.AsString() != authsessClass {
				t.Fatalf("series %+v, want class=%s at 0", point, authsessClass)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("the compat counter is not declared")
	}
}

// T15: no token text reaches a log line, an audit row, a metric label or an
// answer, on accept or refusal.
func TestIssueSessionLeaksNoTokenText(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	good, _ := w.mint("workload+jwt", nil)
	wrong, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Aud = "dev-health-api" })
	expired, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Exp, c.Nbf, c.Iat = c.Exp-900, c.Nbf-900, c.Iat-900 })
	var bodies []string
	for _, header := range []map[string]string{good, wrong, expired, issuerBearer, bearerOf("fcwlb_" + strings.Repeat("B", 43))} {
		bodies = append(bodies, w.issue(header).Body.String())
	}
	var rm metricdata.ResourceMetrics
	if err := w.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var labels strings.Builder
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, point := range sum.DataPoints {
					for _, kv := range point.Attributes.ToSlice() {
						labels.WriteString(kv.Value.Emit() + "\n")
					}
				}
			}
		}
	}
	var rows strings.Builder
	for _, created := range w.sessions.created {
		for _, v := range created.event.Attributes {
			rows.WriteString(fmt.Sprint(v) + "\n")
		}
	}
	haystack := strings.Join(bodies, "\n") + w.logs.String() + labels.String() + rows.String()
	for _, header := range []map[string]string{good, wrong, expired, issuerBearer} {
		value := strings.TrimPrefix(header["Authorization"], "Bearer ")
		for _, part := range strings.Split(value, ".") {
			if len(part) > 20 && strings.Contains(haystack, part) {
				t.Fatalf("token text %q... reached an answer, log, metric or audit row", part[:8])
			}
		}
	}
	if strings.Contains(haystack, strings.Repeat("B", 43)) {
		t.Fatal("a refused credential's text reached an answer, log, metric or audit row")
	}
}

// T17: without a configured workload subject, a workload token is refused as
// any wrong credential, and the key set is not read.
func TestIssueSessionRefusesWorkloadTokensWhenTheWorkloadPathIsOff(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	header, _ := w.mint("workload+jwt", nil)
	w.workload = nil
	w.buildCompat()
	expectError(t, w.issue(header), http.StatusUnauthorized, "invalid_credentials")
	w.nothingRead("workload path off")
	if w.keys.reads != 0 {
		t.Fatalf("the key set was read %d times with the path off", w.keys.reads)
	}
	if response := w.issue(issuerBearer); response.Code != http.StatusOK {
		t.Fatalf("svc_authsess_ with the workload path off: %d %s", response.Code, response.Body)
	}
}

// The workload path alone mounts the route; svc_authsess_ is then refused.
func TestIssueSessionIsMountedByTheWorkloadPathAlone(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	w.creds = nil
	w.buildCompat()
	header, _ := w.mint("workload+jwt", nil)
	if response := w.issue(header); response.Code != http.StatusOK {
		t.Fatalf("workload issue without service credentials: %d %s", response.Code, response.Body)
	}
	expectError(t, w.issue(issuerBearer), http.StatusUnauthorized, "invalid_credentials")
}

func TestNewSessionIssueWorkloadRefusesBadConfigurationNamingNoValue(t *testing.T) {
	signer, _, _ := fileSigner(t)
	good := session.WirePrincipalID(uuid.New())
	for name, cfg := range map[string]SessionIssueWorkloadConfig{
		"no keys":           {IssuerURL: goldenIssuer, Subject: good},
		"no issuer":         {Keys: signer, Subject: good},
		"http issuer":       {IssuerURL: "http://auth.example.test", Keys: signer, Subject: good},
		"empty subject":     {IssuerURL: goldenIssuer, Keys: signer},
		"not a principal":   {IssuerURL: goldenIssuer, Keys: signer, Subject: "prn_secretvalue"},
		"user id not wire":  {IssuerURL: goldenIssuer, Keys: signer, Subject: uuid.NewString()},
		"padded principal":  {IssuerURL: goldenIssuer, Keys: signer, Subject: good + "="},
		"surrounding space": {IssuerURL: goldenIssuer, Keys: signer, Subject: " " + good},
	} {
		if _, err := NewSessionIssueWorkload(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		} else if cfg.Subject != "" && strings.Contains(err.Error(), cfg.Subject) {
			t.Errorf("%s: the error names the subject value: %v", name, err)
		}
	}
	if _, err := NewSessionIssueWorkload(SessionIssueWorkloadConfig{IssuerURL: goldenIssuer, Keys: signer, Subject: good}); err != nil {
		t.Fatal(err)
	}
}

// The log line names why a bearer was refused, so an operator can tell a
// class refusal from a verifier refusal. The answer never does.
func TestIssueSessionLogsWhyABearerWasRefused(t *testing.T) {
	forty3 := strings.Repeat("A", 43)
	for name, tc := range map[string]struct {
		build func(*workloadIssueFixture) map[string]string
		want  string
	}{
		"user access token typ": {func(w *workloadIssueFixture) map[string]string { h, _ := w.mint("at+jwt", nil); return h }, "not_a_workload_token"},
		"hs256 edge token":      {func(w *workloadIssueFixture) map[string]string { return w.bearer(w.grant()) }, "not_a_workload_token"},
		"fcacr_":                {func(*workloadIssueFixture) map[string]string { return bearerOf("fcacr_" + forty3) }, "other_credential_class"},
		"opaque value":          {func(*workloadIssueFixture) map[string]string { return bearerOf("good") }, "not_a_jws"},
		"oversized":             {func(*workloadIssueFixture) map[string]string { return bearerOf(strings.Repeat("a", 6000)) }, "oversized"},
		"wrong audience": {func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Aud = "dev-health-api" })
			return h
		}, "audience"},
		"wrong subject": {func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Sub = session.WirePrincipalID(uuid.New()) })
			return h
		}, "subject"},
		"expired": {func(w *workloadIssueFixture) map[string]string {
			h, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Iat, c.Nbf, c.Exp = c.Iat-400, c.Nbf-400, c.Exp-400 })
			return h
		}, "expired"},
	} {
		w := newWorkloadIssueFixture(t)
		expectError(t, w.issue(tc.build(w)), http.StatusUnauthorized, "invalid_credentials")
		if !strings.Contains(w.logs.String(), `"refusal":"`+tc.want+`"`) {
			t.Errorf("%s: the log line does not carry refusal %q: %s", name, tc.want, w.logs.String())
		}
	}
}

// craftedToken is a compact token with a workload header and the given payload
// text and a placeholder signature: it is only ever read by the peek.
func craftedToken(payload string) string {
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"alg":"EdDSA","typ":"workload+jwt","kid":"k1"}`))
	return header + "." + enc.EncodeToString([]byte(payload)) + "." + enc.EncodeToString(make([]byte, 64))
}

// The peek reads strict JSON: a duplicate aud or sub, or a member spelled in
// another case, is refused before the key set is read.
func TestTheUnverifiedPeekIsStrictJSON(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	rest := `"iss":"` + goldenIssuer + `","principal_type":"workload","jti":"` + uuid.NewString() + `","iat":1,"nbf":1,"exp":2`
	for name, payload := range map[string]string{
		"duplicate aud, wrong then right": `{"aud":"dev-health-api","aud":"auth-service","sub":"` + w.subject + `",` + rest + `}`,
		"duplicate aud, right then wrong": `{"aud":"auth-service","aud":"dev-health-api","sub":"` + w.subject + `",` + rest + `}`,
		"duplicate sub":                   `{"aud":"auth-service","sub":"prn_AAAAAAAAAAAAAAAAAAAAAA","sub":"` + w.subject + `",` + rest + `}`,
		"upper case members":              `{"AUD":"auth-service","SUB":"` + w.subject + `",` + rest + `}`,
		"mixed case aud":                  `{"Aud":"auth-service","sub":"` + w.subject + `",` + rest + `}`,
		"aud not a string":                `{"aud":["auth-service"],"sub":"` + w.subject + `",` + rest + `}`,
		"not an object":                   `["auth-service"]`,
		"trailing document":               `{"aud":"auth-service","sub":"` + w.subject + `",` + rest + `}{}`,
		"exact and upper case aud":        `{"aud":"auth-service","AUD":"dev-health-api","sub":"` + w.subject + `",` + rest + `}`,
	} {
		w.logs.Reset()
		expectError(t, w.issue(bearerOf(craftedToken(payload))), http.StatusUnauthorized, "invalid_credentials")
		w.nothingRead(name)
		if w.keys.reads != 0 {
			t.Fatalf("%s: the peek let the token reach the key set (%d reads)", name, w.keys.reads)
		}
		if !strings.Contains(w.logs.String(), `"refusal":"payload_not_strict"`) {
			t.Fatalf("%s: the peek did not refuse it as payload_not_strict: %s", name, w.logs.String())
		}
	}
}

// (c) A forged token with the right aud and sub passes the peek and reaches the
// verifier, which refuses it: the same 401 body, and no credential check, users
// read, projection or session.
func TestAForgedTokenWithTheRightAudienceAndSubjectReachesTheVerifierAndIsRefused(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	other, _, _ := fileSigner(t)
	forger := &workloadIssueFixture{compatFixture: w.compatFixture, signer: other, subject: w.subject}
	forged, _ := forger.mint("workload+jwt", nil)
	response := w.issue(forged)
	expectError(t, response, http.StatusUnauthorized, "invalid_credentials")
	w.nothingRead("forged token")
	if w.keys.reads != 1 {
		t.Fatalf("key set reads = %d, want 1: the forged token must reach the verifier", w.keys.reads)
	}
	if !strings.Contains(w.logs.String(), `"refusal":"signature"`) {
		t.Fatalf("the verifier did not refuse on the signature: %s", w.logs.String())
	}
	w.creds.refusal = servicecred.RefusedRevoked
	if got, want := refusalWithoutPerRequestFields(t, response.Body.Bytes()), refusalWithoutPerRequestFields(t, w.issue(issuerBearer).Body.Bytes()); got != want {
		t.Fatalf("forged-token body %s differs from %s", got, want)
	}
}

// stubVerifier accepts every token and answers fixed claims.
type stubVerifier struct{ claims authverify.WorkloadClaims }

func (s stubVerifier) Verify(string, time.Time) (*authverify.WorkloadClaims, error) {
	claims := s.claims
	return &claims, nil
}

// (a) The peek only refuses. The route decides on the verified claims: a
// verifier that answers another subject or audience than the peek saw is a
// refusal, and the same claims that match the configuration issue a session.
func TestTheRouteRechecksTheVerifiedClaimsAfterVerify(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	payload := `{"aud":"auth-service","sub":"` + w.subject + `"}`
	verified := func(aud, sub string) *SessionIssueWorkload {
		return &SessionIssueWorkload{
			verifier: stubVerifier{authverify.WorkloadClaims{Audience: aud, Subject: sub, TokenID: uuid.NewString()}},
			subject:  w.subject, now: time.Now,
		}
	}
	for name, tc := range map[string]struct {
		aud, sub string
		ok       bool
	}{
		"matching claims":             {"auth-service", w.subject, true},
		"verified subject differs":    {"auth-service", session.WirePrincipalID(uuid.New()), false},
		"verified audience differs":   {"dev-health-api", w.subject, false},
		"verified subject not a prn_": {"auth-service", "prn_x", false},
		"verified subject empty":      {"auth-service", "", false},
	} {
		w.workload = verified(tc.aud, tc.sub)
		w.buildCompat()
		before, usersBefore := len(w.sessions.created), w.users.calls
		response := w.issue(bearerOf(craftedToken(payload)))
		if tc.ok {
			if response.Code != http.StatusOK {
				t.Fatalf("%s: status %d: %s", name, response.Code, response.Body)
			}
			continue
		}
		expectError(t, response, http.StatusUnauthorized, "invalid_credentials")
		if len(w.sessions.created) != before {
			t.Fatalf("%s: a session was created on claims the route must refuse", name)
		}
		if len(w.creds.seen) != 0 || w.users.calls != usersBefore {
			t.Fatalf("%s: a refusal read stores", name)
		}
	}
}

// A jti the workload-token schema allows (22 to 64 base64url characters), not
// a uuid, is valid in the audit row the session is issued with.
func TestReviewValidWorkloadJTIThroughProductionSessionAuditValidation(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	const jti = "AAAAAAAAAAAAAAAAAAAAAA"
	header, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Jti = jti })
	response := w.issue(header)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if len(w.sessions.created) != 1 {
		t.Fatalf("%d sessions created", len(w.sessions.created))
	}
	attributes := w.sessions.created[0].event.Attributes
	if attributes["workload_jti"] != jti {
		t.Fatalf("workload_jti %v, want %s", attributes["workload_jti"], jti)
	}
	if err := audit.ValidateAttributes(attributes); err != nil {
		t.Fatalf("the audit validation refuses the event the route builds: %v", err)
	}
}
