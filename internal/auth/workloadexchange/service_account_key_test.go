package workloadexchange

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// saKeyMarker is planted in every key value a test presents, so a test can
// prove the value reaches no answer, log line, metric or audit row.
const saKeyMarker = "PLANTEDSAKEYq4Z"

// saKeyValue is a value of the key's shape, built at run time: the prefix, the
// marker and padding to 43 characters.
func saKeyValue(fill string) string {
	return serviceaccount.KeyPrefix + saKeyMarker + strings.Repeat(fill, 43-len(saKeyMarker))
}

var (
	testKeyID     = uuid.MustParse("9c4d5e6f-7a8b-4c9d-8e0f-2a3b4c5d6e7f")
	testKeyOrgID  = uuid.MustParse("1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5e")
	testKeyPrinc  = uuid.MustParse("6f1c2d3e-4a5b-4c6d-8e7f-901a2b3c4d5e")
	testOtherKey  = uuid.MustParse("ad5e6f7a-8b9c-4d0e-9f10-3b4c5d6e7f80")
	saSubjectName = "service_account_key"
)

// fakeKeys stands in for the key registry. It records the digests it is asked
// for and answers reg unless err is set; byDigest overrides reg for one value.
type fakeKeys struct {
	mu       sync.Mutex
	digests  [][sha256.Size]byte
	nows     []time.Time
	reg      ServiceAccountKeyRegistration
	byDigest map[[sha256.Size]byte]ServiceAccountKeyRegistration
	err      error
}

func (f *fakeKeys) ResolveServiceAccountKey(_ context.Context, digest [sha256.Size]byte, now time.Time) (ServiceAccountKeyRegistration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.digests = append(f.digests, digest)
	f.nows = append(f.nows, now)
	if f.err != nil {
		return ServiceAccountKeyRegistration{}, f.err
	}
	if reg, ok := f.byDigest[digest]; ok {
		return reg, nil
	}
	return f.reg, nil
}

func (f *fakeKeys) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.digests)
}

// withServiceAccountKeys turns the key subject on with a registry whose key is
// live for the test account until the far future.
func withServiceAccountKeys(f *fakeKeys) harnessOption {
	return func(h *harness, d *Deps) {
		f.reg = ServiceAccountKeyRegistration{
			CredentialID: testKeyID, PrincipalID: testKeyPrinc, OrgID: testKeyOrgID,
			ExpiresAt: h.now.Add(90 * 24 * time.Hour), Revision: 5,
		}
		d.ServiceAccountKeys = f
	}
}

func saForm(value, audience, scope string) url.Values {
	v := form(value, audience, scope)
	v.Set("subject_token_type", TokenTypeServiceAccountKey)
	return v
}

// saPoints returns the counter's key-subject points as "outcome/reason/audience=value".
func (h *harness) saPoints() map[string]int64 {
	h.t.Helper()
	out := map[string]int64{}
	for key, value := range collectCounter(h.t, h.reader, "auth.workload.exchanges") {
		if subject, series, _ := strings.Cut(key, "|"); subject == saSubjectName {
			out[series] = value
		}
	}
	return out
}

func verifySA(t *testing.T, h *harness, compact string) *authverify.ServiceAccountClaims {
	t.Helper()
	set, err := h.signer.KeySet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	keys := authverify.StaticKeys{}
	for _, key := range set.Published() {
		keys[key.KeyID] = key.PublicKey
	}
	verifier, err := authverify.NewServiceAccountVerifier(authverify.ServiceAccountVerifierConfig{Issuer: testIssuerURL, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := verifier.Verify(compact, h.now)
	if err != nil {
		t.Fatalf("the issued token does not verify: %v", err)
	}
	return claims
}

func segment(t *testing.T, compact string, i int) map[string]any {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(strings.Split(compact, ".")[i])
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func sortedKeys(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func TestAServiceAccountKeyGetsAnSATokenForDevHealthAPI(t *testing.T) {
	keys := &fakeKeys{}
	h := newHarness(t, withServiceAccountKeys(keys), withoutKubernetes())
	value := saKeyValue("A")
	answer := decodeAnswer(t, h.post(saForm(value, "dev-health-api", "").Encode()))
	if answer.IssuedTokenType != TokenTypeJWT || answer.TokenType != "Bearer" || answer.ExpiresIn != 300 {
		t.Fatalf("answer %+v", answer)
	}
	claims := verifySA(t, h, answer.AccessToken)
	if claims.Subject != session.WirePrincipalID(testKeyPrinc) || claims.OrgID != testKeyOrgID.String() ||
		claims.CredentialID != "crd_"+testKeyID.String() || claims.Audience != "dev-health-api" {
		t.Fatalf("claims %+v", claims)
	}
	if claims.ExpiresAt != h.now.Unix()+300 || claims.IssuedAt != h.now.Unix() || claims.NotBefore != h.now.Unix() {
		t.Fatalf("iat %d nbf %d exp %d, now %d", claims.IssuedAt, claims.NotBefore, claims.ExpiresAt, h.now.Unix())
	}
	if got := segment(t, answer.AccessToken, 0); got["typ"] != "sa+jwt" || got["alg"] != "EdDSA" {
		t.Fatalf("header %v", got)
	}
	if got := sortedKeys(segment(t, answer.AccessToken, 1)); got != "aud,cid,exp,iat,iss,jti,nbf,principal_type,org_id,sub" && got != "aud,cid,exp,iat,iss,jti,nbf,org_id,principal_type,sub" {
		t.Fatalf("claim members %s: no sid, no scope, no audience list", got)
	}
	if segment(t, answer.AccessToken, 1)["principal_type"] != "service_account" {
		t.Fatal("principal_type is not service_account")
	}
	if len(keys.digests) != 1 || keys.digests[0] != sha256.Sum256([]byte(value)) || !keys.nows[0].Equal(h.now) {
		t.Fatalf("the registry was asked %d time(s), not once for the value's SHA-256 at now", len(keys.digests))
	}
	rows := h.auditRows()
	if len(rows) != 1 {
		t.Fatalf("%d audit rows, want 1", len(rows))
	}
	row := rows[0]
	if row.EventType != audit.EventServiceAccountTokenIssued || row.Outcome != audit.OutcomeAllowed {
		t.Fatalf("audit event %s %s", row.EventType, row.Outcome)
	}
	if row.OrganizationID == nil || *row.OrganizationID != testKeyOrgID.String() ||
		row.SubjectPrincipalID == nil || *row.SubjectPrincipalID != testKeyPrinc.String() {
		t.Fatalf("audit org %v subject %v", row.OrganizationID, row.SubjectPrincipalID)
	}
	if got := sortedKeys(row.Attributes); got != "audience,credential_class,credential_id,jti,principal_id,revision" {
		t.Fatalf("audit attribute keys %s", got)
	}
	attrs := row.Attributes
	if attrs["credential_id"] != testKeyID.String() || attrs["jti"] != claims.TokenID || attrs["principal_id"] != testKeyPrinc.String() ||
		attrs["revision"] != int64(5) || attrs["audience"] != "dev-health-api" || attrs["credential_class"] != "service_account_token" {
		t.Fatalf("audit attributes %v", attrs)
	}
	if got := h.saPoints()["issued/issued/dev-health-api"]; got != 1 {
		t.Fatalf("issued counter %d; points %v", got, nonZero(h.saPoints()))
	}
	if got := h.metricPoints()["issued/issued/dev-health-api"]; got != 0 {
		t.Fatalf("a key exchange counted as a Kubernetes one (%d)", got)
	}
}

func TestTheKeySubjectTypeURNIsPinned(t *testing.T) {
	if TokenTypeServiceAccountKey != "urn:dev-health:params:oauth:token-type:service-account-key" {
		t.Fatalf("TokenTypeServiceAccountKey = %q", TokenTypeServiceAccountKey)
	}
}

func TestTheParentKeyInOverlapAndItsSuccessorBothExchange(t *testing.T) {
	keys := &fakeKeys{}
	h := newHarness(t, withServiceAccountKeys(keys), withoutKubernetes())
	parent, successor := saKeyValue("A"), saKeyValue("B")
	keys.byDigest = map[[sha256.Size]byte]ServiceAccountKeyRegistration{
		sha256.Sum256([]byte(parent)):    {CredentialID: testKeyID, PrincipalID: testKeyPrinc, OrgID: testKeyOrgID, ExpiresAt: h.now.Add(time.Hour), Revision: 5},
		sha256.Sum256([]byte(successor)): {CredentialID: testOtherKey, PrincipalID: testKeyPrinc, OrgID: testKeyOrgID, ExpiresAt: h.now.Add(48 * time.Hour), Revision: 5},
	}
	got := map[string]int{}
	for _, value := range []string{parent, successor} {
		answer := decodeAnswer(t, h.post(saForm(value, "dev-health-api", "").Encode()))
		claims := verifySA(t, h, answer.AccessToken)
		got[claims.CredentialID] = answer.ExpiresIn
	}
	if len(got) != 2 || got["crd_"+testKeyID.String()] != 300 || got["crd_"+testOtherKey.String()] != 300 {
		t.Fatalf("exchanged key ids %v", got)
	}
}

func TestTheKeyTokenLifetimeStopsAtTheKeyExpiry(t *testing.T) {
	for _, tc := range []struct {
		name string
		left time.Duration
		want int
	}{
		{"expires in 100 s", 100 * time.Second, 100},
		{"expires in 1 s", time.Second, 1},
		{"expires in 1.9 s", 1900 * time.Millisecond, 1},
		{"expires at the ceiling", 300 * time.Second, 300},
		{"expires after the ceiling", time.Hour, 300},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys := &fakeKeys{}
			h := newHarness(t, withServiceAccountKeys(keys), withoutKubernetes())
			keys.reg.ExpiresAt = h.now.Add(tc.left)
			answer := decodeAnswer(t, h.post(saForm(saKeyValue("A"), "dev-health-api", "").Encode()))
			if answer.ExpiresIn != tc.want {
				t.Fatalf("expires_in %d, want %d", answer.ExpiresIn, tc.want)
			}
			claims := verifySA(t, h, answer.AccessToken)
			if claims.ExpiresAt > keys.reg.ExpiresAt.Unix() || claims.ExpiresAt-claims.IssuedAt > int64(contracts.ServiceAccountTokenLifetimeCeiling/time.Second) {
				t.Fatalf("the token outlives its key or the ceiling: exp %d, key %d", claims.ExpiresAt, keys.reg.ExpiresAt.Unix())
			}
		})
	}
}

type keyRefusalCase struct {
	name     string
	value    string
	audience string
	scope    string
	setup    func(f *fakeKeys, now time.Time)
	reason   string
	noLookup bool
}

func keyRefusals() []keyRefusalCase {
	refusedBy := func(reason string) func(*fakeKeys, time.Time) {
		return func(f *fakeKeys, _ time.Time) { f.err = &KeyRefusal{Reason: reason} }
	}
	valid := saKeyValue("A")
	body := strings.TrimPrefix(valid, serviceaccount.KeyPrefix)
	cases := []keyRefusalCase{
		{name: "no prefix", value: body, reason: "malformed", noLookup: true},
		{name: "another prefix", value: "fcwlb_" + body, reason: "malformed", noLookup: true},
		{name: "one byte short", value: valid[:len(valid)-1], reason: "malformed", noLookup: true},
		{name: "one byte long", value: valid + "A", reason: "malformed", noLookup: true},
		{name: "outside base64url", value: saKeyValue("+"), reason: "malformed", noLookup: true},
		{name: "upper-case prefix", value: "FCSAK_" + body, reason: "malformed", noLookup: true},
		{name: "a JWT", value: subjectToken(time.Unix(1_900_003_600, 0)), reason: "malformed", noLookup: true},
		{name: "audience auth-service", value: valid, audience: "auth-service", reason: "audience_refused", noLookup: true},
		{name: "audience acr-api", value: valid, audience: "acr-api", reason: "audience_refused", noLookup: true},
		{name: "audience not an audience", value: valid, audience: "everything", reason: "audience_refused", noLookup: true},
		{name: "a scope", value: valid, scope: testAction, reason: "scope_refused", noLookup: true},
		{name: "unnamed refusal", value: valid, setup: refusedBy(""), reason: "resolve_refused"},
		{name: "a reason outside the set", value: valid, setup: refusedBy("uid_mismatch"), reason: "resolve_refused"},
		{name: "expires now", value: valid, setup: func(f *fakeKeys, now time.Time) { f.reg.ExpiresAt = now }, reason: "lifetime_too_short"},
		{name: "expires in under a second", value: valid,
			setup: func(f *fakeKeys, now time.Time) { f.reg.ExpiresAt = now.Add(999 * time.Millisecond) }, reason: "lifetime_too_short"},
		{name: "no expiry known", value: valid, setup: func(f *fakeKeys, _ time.Time) { f.reg.ExpiresAt = time.Time{} }, reason: "lifetime_too_short"},
	}
	for _, reason := range []string{"unknown", "wrong_class", "not_platform", "wrong_kind", "revoked", "expired", "account_disabled", "principal_disabled", "org_suspended"} {
		cases = append(cases, keyRefusalCase{name: reason, value: valid, setup: refusedBy(reason), reason: reason})
	}
	return cases
}

// Every refusal of the key subject is the Kubernetes subject's refusal, byte
// for byte; only the key counter series names the reason.
func TestEveryKeyRefusalAnswersTheSameBytes(t *testing.T) {
	reference := newHarness(t)
	reference.registry.err = refusedBecause("unknown")
	want := reference.post(form(reference.validSubject(), "auth-service", "").Encode())
	if want.Code != http.StatusBadRequest || want.Body.String() != "{\"error\":\"invalid_grant\"}\n" {
		t.Fatalf("reference refusal %d %q", want.Code, want.Body.String())
	}
	for _, tc := range keyRefusals() {
		t.Run(tc.name, func(t *testing.T) {
			keys := &fakeKeys{}
			h := newHarness(t, withServiceAccountKeys(keys))
			if tc.setup != nil {
				tc.setup(keys, h.now)
			}
			audience := tc.audience
			if audience == "" {
				audience = "dev-health-api"
			}
			rec := h.post(saForm(tc.value, audience, tc.scope).Encode())
			if rec.Code != want.Code || rec.Body.String() != want.Body.String() || comparableHeaders(rec.Header()) != comparableHeaders(want.Header()) {
				t.Fatalf("%d %q %s, want the Kubernetes refusal %d %q %s", rec.Code, rec.Body.String(), comparableHeaders(rec.Header()),
					want.Code, want.Body.String(), comparableHeaders(want.Header()))
			}
			label := audienceLabel(audience)
			if got := h.saPoints()["refused/"+tc.reason+"/"+label]; got != 1 {
				t.Fatalf("counter refused/%s/%s = %d, want 1; points %v", tc.reason, label, got, nonZero(h.saPoints()))
			}
			if tc.noLookup && keys.calls() != 0 {
				t.Fatalf("a request refused before the store read reached the registry %d time(s)", keys.calls())
			}
			if n := len(h.auditRows()); n != 0 {
				t.Fatalf("%d audit rows for a refusal", n)
			}
			if h.review.count() != 0 || h.registry.resolved != 0 {
				t.Fatalf("a key request reached TokenReview (%d) or the binding registry (%d)", h.review.count(), h.registry.resolved)
			}
		})
	}
}

// The subject_token_type alone selects the path.
func TestTheKeySubjectTypeAloneSelectsThePath(t *testing.T) {
	keys := &fakeKeys{}
	boot := &fakeBootstrap{}
	h := newHarness(t, withServiceAccountKeys(keys), withBootstrap(boot))
	h.review.set(func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) {
		writeReview(w, http.StatusCreated, false, nil, "", "")
	})
	rec := h.post(form(saKeyValue("A"), "dev-health-api", "").Encode())
	if rec.Code != http.StatusBadRequest || h.review.count() != 1 || keys.calls() != 0 {
		t.Fatalf("a key typed as a JWT: %d, reviews %d, key reads %d", rec.Code, h.review.count(), keys.calls())
	}
	rec = h.post(saForm(h.validSubject(), "dev-health-api", "").Encode())
	if rec.Code != http.StatusBadRequest || h.review.count() != 1 || keys.calls() != 0 || h.registry.resolved != 0 {
		t.Fatalf("a JWT typed as a key: %d, reviews %d, key reads %d", rec.Code, h.review.count(), keys.calls())
	}
	if got := h.saPoints()["refused/malformed/dev-health-api"]; got != 1 {
		t.Fatalf("key counter malformed = %d", got)
	}
	rec = h.post(bootstrapForm(saKeyValue("A"), "auth-service", "").Encode())
	if rec.Code != http.StatusBadRequest || keys.calls() != 0 || boot.calls() != 0 {
		t.Fatalf("a key typed as a bootstrap value: %d, key reads %d, bootstrap reads %d", rec.Code, keys.calls(), boot.calls())
	}
}

// The other two subjects work beside the key subject.
func TestTheOtherSubjectsAreUnchangedBesideTheKeySubject(t *testing.T) {
	keys := &fakeKeys{}
	h := newHarness(t, withServiceAccountKeys(keys), withBootstrap(&fakeBootstrap{}))
	k := decodeAnswer(t, h.post(form(h.validSubject(), "auth-service", testAction).Encode()))
	verifyIssued(t, h, k.AccessToken, "auth-service")
	b := decodeAnswer(t, h.post(bootstrapForm(bootstrapValue("A"), "auth-service", testAction).Encode()))
	verifyIssued(t, h, b.AccessToken, "auth-service")
	if keys.calls() != 0 {
		t.Fatalf("the key registry was read %d time(s) by another subject", keys.calls())
	}
}

// A subject that is off is a request shape fault, answered before any store read.
func TestTheKeySubjectOffIsAShapeFault(t *testing.T) {
	h := newHarness(t)
	assertInvalidRequest(t, h, h.post(saForm(saKeyValue("A"), "dev-health-api", "").Encode()))
	keys := &fakeKeys{}
	on := newHarness(t, withServiceAccountKeys(keys), withoutKubernetes())
	unknown := saForm(saKeyValue("A"), "dev-health-api", "")
	unknown.Set("subject_token_type", "urn:dev-health:params:oauth:token-type:service-account-key-v2")
	rec := on.post(unknown.Encode())
	if rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_request\"}\n" || keys.calls() != 0 {
		t.Fatalf("an unknown subject type: %d %q reads %d", rec.Code, rec.Body.String(), keys.calls())
	}
}

func TestKeyDependencyFailuresAnswer503(t *testing.T) {
	assertKeyUnavailable := func(t *testing.T, h *harness, rec *httptest.ResponseRecorder, reason string) {
		t.Helper()
		body := rec.Body.String()
		if rec.Code != http.StatusServiceUnavailable || body != "{\"error\":\"temporarily_unavailable\"}\n" {
			t.Fatalf("%d %q, want 503 temporarily_unavailable", rec.Code, body)
		}
		if got := h.saPoints()["unavailable/"+reason+"/dev-health-api"]; got != 1 {
			t.Fatalf("counter unavailable/%s = %d; points %v", reason, got, nonZero(h.saPoints()))
		}
	}
	post := func(h *harness) *httptest.ResponseRecorder {
		return h.post(saForm(saKeyValue("A"), "dev-health-api", "").Encode())
	}
	t.Run("store", func(t *testing.T) {
		keys := &fakeKeys{}
		h := newHarness(t, withServiceAccountKeys(keys))
		keys.err = errors.New("serviceaccount: store unavailable")
		assertKeyUnavailable(t, h, post(h), "store_error")
		if len(h.auditRows()) != 0 {
			t.Fatal("an audit row for a store failure")
		}
	})
	t.Run("signer", func(t *testing.T) {
		h := newHarness(t, withServiceAccountKeys(&fakeKeys{}), func(h *harness, d *Deps) { d.Signer = failingSigner{Signer: d.Signer} })
		assertKeyUnavailable(t, h, post(h), "signer_unavailable")
	})
	t.Run("audit", func(t *testing.T) {
		h := newHarness(t, withServiceAccountKeys(&fakeKeys{}))
		h.auditErr = errors.New("audit down")
		rec := post(h)
		assertKeyUnavailable(t, h, rec, "audit_unavailable")
		if strings.Contains(rec.Body.String(), "access_token") || len(h.auditRows()) != 0 {
			t.Fatal("a token left with the audit write failed")
		}
	})
}

// The value and its digest never leave the request: no answer, log line,
// metric or audit row carries them, on any outcome.
func TestTheKeyValueAndItsDigestReachNoAnswerLogMetricOrAuditRow(t *testing.T) {
	keys := &fakeKeys{}
	h := newHarness(t, withServiceAccountKeys(keys))
	value := saKeyValue("A")
	digest := sha256.Sum256([]byte(value))
	forms := []string{
		hex.EncodeToString(digest[:]), strings.ToUpper(hex.EncodeToString(digest[:])),
		base64.StdEncoding.EncodeToString(digest[:]), base64.RawStdEncoding.EncodeToString(digest[:]),
		base64.URLEncoding.EncodeToString(digest[:]), base64.RawURLEncoding.EncodeToString(digest[:]),
	}
	bodies := []string{h.post(saForm(value, "dev-health-api", "").Encode()).Body.String()}
	for _, tc := range keyRefusals() {
		keys.err = nil
		keys.reg.ExpiresAt = h.now.Add(time.Hour)
		if tc.setup != nil {
			tc.setup(keys, h.now)
		}
		audience := tc.audience
		if audience == "" {
			audience = "dev-health-api"
		}
		bodies = append(bodies, h.post(saForm(tc.value, audience, tc.scope).Encode()).Body.String())
	}
	var rows strings.Builder
	for _, row := range h.auditRows() {
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		rows.Write(raw)
	}
	if rows.Len() == 0 {
		t.Fatal("no audit row to inspect")
	}
	for surface, text := range map[string]string{
		"answers": strings.Join(bodies, "\n"), "logs": h.logs.String(), "metrics": dumpMetrics(t, h.reader), "audit": rows.String(),
	} {
		if strings.Contains(text, saKeyMarker) || strings.Contains(text, serviceaccount.KeyPrefix) {
			t.Errorf("the %s carry the key value", surface)
		}
		for _, form := range forms {
			if strings.Contains(text, form) {
				t.Errorf("the %s carry the key digest (%d chars)", surface, len(form))
			}
		}
	}
}

func TestEveryKeyCounterSeriesIsDeclaredAtZero(t *testing.T) {
	h := newHarness(t, withServiceAccountKeys(&fakeKeys{}))
	points := h.saPoints()
	want := map[string]bool{}
	for _, aud := range []string{"auth-service", "dev-health-api", "none"} {
		for _, reason := range []string{
			"malformed", "unknown", "wrong_class", "not_platform", "wrong_kind", "revoked", "expired", "account_disabled",
			"principal_disabled", "org_suspended", "resolve_refused", "audience_refused", "scope_refused", "lifetime_too_short",
		} {
			want["refused/"+reason+"/"+aud] = true
		}
		for _, reason := range []string{
			"store_error", "signer_unavailable", "audit_unavailable", "selfcheck_contract", "selfcheck_unknown_kid", "selfcheck_signature",
			"selfcheck_issuer", "selfcheck_audience", "selfcheck_not_yet_valid", "selfcheck_expired",
		} {
			want["unavailable/"+reason+"/"+aud] = true
		}
	}
	want["issued/issued/dev-health-api"] = true
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
		t.Fatalf("key series not declared at 0: %v; outside the closed set: %v", missing, extra)
	}
}

func TestRoutesAcceptsTheKeySubjectAlone(t *testing.T) {
	h := newHarness(t)
	alone := Deps{ServiceAccountKeys: &fakeKeys{}, RefusalReason: func(error) string { return "" }, Signer: h.signer, Audit: h.writeAudit, IssuerURL: testIssuerURL}
	if _, err := Routes(alone); err != nil {
		t.Fatalf("a key-only exchange was refused: %v", err)
	}
	for name, mutate := range map[string]func(*Deps){
		"no subject": func(d *Deps) { d.ServiceAccountKeys = nil },
		"signer":     func(d *Deps) { d.Signer = nil },
		"audit":      func(d *Deps) { d.Audit = nil },
		"issuer":     func(d *Deps) { d.IssuerURL = "" },
	} {
		d := alone
		mutate(&d)
		if _, err := Routes(d); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Any scope field on a key subject, well formed or not, is the one scope
// refusal before the store read: a key holds no scope.
func TestEveryScopeFieldFormOnAKeyIsTheScopeRefusal(t *testing.T) {
	reference := newHarness(t)
	reference.registry.err = refusedBecause("unknown")
	want := reference.post(form(reference.validSubject(), "auth-service", "").Encode())
	tooMany := strings.TrimSpace(strings.Repeat(testAction+" ", maxScopeActions+1))
	cases := map[string][]string{
		"one action":       {testAction},
		"empty":            {""},
		"blank":            {" "},
		"double space":     {testAction + "  " + testOtherAction},
		"two scope fields": {testAction, testOtherAction},
		"too many actions": {tooMany},
		"action too long":  {strings.Repeat("a", maxActionSize+1)},
	}
	for name, scope := range cases {
		t.Run(name, func(t *testing.T) {
			keys := &fakeKeys{}
			h := newHarness(t, withServiceAccountKeys(keys))
			v := saForm(saKeyValue("A"), "dev-health-api", "")
			v["scope"] = scope
			rec := h.post(v.Encode())
			if rec.Code != want.Code || rec.Body.String() != want.Body.String() {
				t.Fatalf("%d %q, want the refusal %d %q", rec.Code, rec.Body.String(), want.Code, want.Body.String())
			}
			if got := h.saPoints()["refused/scope_refused/dev-health-api"]; got != 1 {
				t.Fatalf("counter refused/scope_refused = %d; points %v", got, nonZero(h.saPoints()))
			}
			if keys.calls() != 0 {
				t.Fatalf("a scope refusal reached the registry %d time(s)", keys.calls())
			}
		})
	}
}
