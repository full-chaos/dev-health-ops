package authapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

type accessFixture struct {
	*fixture
	signer *token.Signer
	dir    string
	reader *sdkmetric.ManualReader
	check  *AccessCheck
}

func accessMeter() (*sdkmetric.ManualReader, *sdkmetric.MeterProvider) {
	reader := sdkmetric.NewManualReader()
	return reader, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
}

func newAccessFixtureWith(t *testing.T, rows []accessverify.Row, keys func(*token.Signer) KeySetSource) *accessFixture {
	t.Helper()
	signer, dir, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, AccessTokenAudience)
	f := newFixtureWith(t, issuer)
	if rows == nil {
		var err error
		if rows, err = AuthServiceAccessRows(); err != nil {
			t.Fatal(err)
		}
	}
	var source KeySetSource = signer
	if keys != nil {
		source = keys(signer)
	}
	reader, provider := accessMeter()
	check, err := NewAccessCheck(AccessCheckConfig{IssuerURL: goldenIssuer, Keys: source, Legacy: issuer, Rows: rows, Meter: provider.Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	issuer.WithAccessCheck(check)
	return &accessFixture{fixture: f, signer: signer, dir: dir, reader: reader, check: check}
}

func newAccessFixture(t *testing.T) *accessFixture { return newAccessFixtureWith(t, nil, nil) }

func (a *accessFixture) counter(name string, want map[string]string) int64 {
	a.t.Helper()
	var rm metricdata.ResourceMetrics
	if err := a.reader.Collect(context.Background(), &rm); err != nil {
		a.t.Fatal(err)
	}
	var total int64
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
				}
			}
		}
	}
	return total
}

func (a *accessFixture) signClaims(c contracts.AccessTokenClaims) map[string]string {
	a.t.Helper()
	payload, err := json.Marshal(c)
	if err != nil {
		a.t.Fatal(err)
	}
	compact, _, err := a.signer.Sign(context.Background(), accessTokenTyp, payload)
	if err != nil {
		a.t.Fatal(err)
	}
	return map[string]string{"Authorization": "Bearer " + compact}
}

func (a *accessFixture) claims() contracts.AccessTokenClaims {
	now := time.Now().Unix()
	org := a.org.String()
	return contracts.AccessTokenClaims{
		Iss: goldenIssuer, Aud: AccessTokenAudience, Sub: session.WirePrincipalID(a.principal), PrincipalType: "human",
		Sid: session.WireSessionID(a.record.ID), Jti: uuid.NewString(), Iat: now, Nbf: now, Exp: now + 600,
		Scope: "auth:session", OrgID: &org,
	}
}

func TestSessionRoutesAcceptTheAuthServiceEdDSAToken(t *testing.T) {
	a := newAccessFixture(t)
	if r := a.do(http.MethodGet, "/v1/sessions/current", "", a.bearer(a.grant())); r.Code != http.StatusOK {
		t.Fatalf("current = %d, body %s", r.Code, r.Body)
	}
	if r := a.do(http.MethodPost, "/v1/sessions/revoke", "", a.bearer(a.grant())); r.Code != http.StatusNoContent {
		t.Fatalf("revoke = %d, body %s", r.Code, r.Body)
	}
	if r := a.do(http.MethodPost, "/v1/sessions/revoke-all", "", a.bearer(a.grant())); r.Code != http.StatusNoContent {
		t.Fatalf("revoke-all = %d, body %s", r.Code, r.Body)
	}
	for _, profile := range []string{"GET /v1/sessions/current", "POST /v1/sessions/revoke", "POST /v1/sessions/revoke-all"} {
		if got := a.counter(accessverify.AcceptedMetric, map[string]string{"service": AccessService, "profile": profile, "class": accessverify.ClassUser}); got != 1 {
			t.Errorf("accepted user %s = %d, want 1", profile, got)
		}
	}
}

func TestEveryAuthServiceBearerRowAcceptsTheUserTokenAndKeepsTheLegacyOptIn(t *testing.T) {
	rows, err := AuthServiceAccessRows()
	if err != nil {
		t.Fatal(err)
	}
	byProfile := map[string]accessverify.Row{}
	for _, row := range rows {
		if row.Service != AccessService {
			t.Errorf("row %s has service %q", row.Profile, row.Service)
		}
		byProfile[row.Profile] = row
	}
	want := []string{
		"GET " + PatternCurrent, "POST " + PatternRevoke, "POST " + PatternRevokeAll,
		"POST " + PatternSocialLink, "DELETE " + PatternSocialUnlink, "POST " + PatternChangePassword,
	}
	if len(byProfile) != len(want) {
		t.Fatalf("%d access rows, want %d: %v", len(byProfile), len(want), byProfile)
	}
	for _, profile := range want {
		row, ok := byProfile[profile]
		if !ok || !row.AcceptsUser || !row.LegacyHS256 {
			t.Errorf("row %q = %+v (found %v), want accepts_user and legacy_hs256", profile, row, ok)
		}
	}
}

func TestAnEdDSATokenOfAnotherAudienceIsRefusedAtTheAuthServiceRoutes(t *testing.T) {
	a := newAccessFixture(t)
	c := a.claims()
	c.Aud, c.Scope = "dev-health-api", "api:access"
	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", a.signClaims(c)), http.StatusUnauthorized, "audience")
	if got := a.counter(accessverify.RefusedMetric, map[string]string{"service": AccessService, "profile": "GET /v1/sessions/current", "reason": accessverify.ReasonClaims}); got != 1 {
		t.Errorf("refused claims = %d, want 1", got)
	}
}

func TestEdDSATokenWithoutAValidSidIsRefusedMalformed(t *testing.T) {
	for name, sid := range map[string]string{"empty": "", "not a session id": "ses_!!!", "a go-api shaped value": uuid.NewString()} {
		t.Run(name, func(t *testing.T) {
			a := newAccessFixture(t)
			c := a.claims()
			c.Sid = sid
			expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", a.signClaims(c)), http.StatusUnauthorized, "malformed")
			if len(a.sessions.revoked) != 0 {
				t.Fatal("a session was touched")
			}
		})
	}
}

func TestEdDSARefusalsCarryTheirAuthOutcomeReason(t *testing.T) {
	a := newAccessFixture(t)
	expired := a.claims()
	expired.Iat, expired.Nbf, expired.Exp = time.Now().Unix()-7200, time.Now().Unix()-7200, time.Now().Unix()-3600
	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", a.signClaims(expired)), http.StatusUnauthorized, "expired")

	foreignIssuer := a.claims()
	foreignIssuer.Iss = "https://other.example.test"
	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", a.signClaims(foreignIssuer)), http.StatusUnauthorized, "issuer")

	good := a.bearer(a.grant())["Authorization"]
	parts := strings.Split(good, ".")
	altered := parts[0] + "." + parts[1] + "." + flipFirst(parts[2])
	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", map[string]string{"Authorization": altered}), http.StatusUnauthorized, "bad_signature")

	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", map[string]string{"Authorization": "Bearer not.a.token"}), http.StatusUnauthorized, "malformed")
}

func flipFirst(s string) string {
	if s[0] == 'A' {
		return "B" + s[1:]
	}
	return "A" + s[1:]
}

func TestAKeySourceOutageIsARetryable503NotARefusal(t *testing.T) {
	a := newAccessFixtureWith(t, nil, func(*token.Signer) KeySetSource { return failingKeys{} })
	r := a.do(http.MethodGet, "/v1/sessions/current", "", a.bearer(a.grant()))
	expectError(t, r, http.StatusServiceUnavailable, "dependency_unavailable")
	if got := a.counter(accessverify.RefusedMetric, map[string]string{"service": AccessService, "profile": "GET /v1/sessions/current", "reason": accessverify.ReasonUnavailable}); got != 1 {
		t.Errorf("refused unavailable = %d, want 1", got)
	}
}

type failingKeys struct{}

func (failingKeys) KeySet(context.Context) (token.KeySet, error) {
	return token.KeySet{}, errors.New("custody unreadable")
}

func TestTheLegacyHS256TokenIsAcceptedOnlyOnARowThatOptsIn(t *testing.T) {
	hs, err := NewIssuer(testSecret, testIssuer, AccessTokenAudience)
	if err != nil {
		t.Fatal(err)
	}
	for _, optIn := range []bool{true, false} {
		rows := []accessverify.Row{{Service: AccessService, Profile: "GET /v1/sessions/current", AcceptsUser: true, LegacyHS256: optIn}}
		a := newAccessFixtureWith(t, rows, nil)
		compact, err := hs.Access(a.grant())
		if err != nil {
			t.Fatal(err)
		}
		r := a.do(http.MethodGet, "/v1/sessions/current", "", map[string]string{"Authorization": "Bearer " + compact})
		if optIn && r.Code != http.StatusOK {
			t.Errorf("legacy_hs256 true: status %d, body %s", r.Code, r.Body)
		}
		if !optIn {
			expectError(t, r, http.StatusUnauthorized, "malformed")
			if got := a.counter(accessverify.RefusedMetric, map[string]string{"profile": "GET /v1/sessions/current", "reason": accessverify.ReasonAlgNotAllowed}); got != 1 {
				t.Errorf("refused alg_not_allowed = %d, want 1", got)
			}
		}
	}
}

func TestALegacyTokenKeepsItsOwnRefusalReason(t *testing.T) {
	a := newAccessFixture(t)
	hs, err := NewIssuer(strings.Repeat("z", 40), testIssuer, AccessTokenAudience)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := hs.Access(a.grant())
	if err != nil {
		t.Fatal(err)
	}
	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", map[string]string{"Authorization": "Bearer " + compact}), http.StatusUnauthorized, "bad_signature")
}

func TestARevokedSessionRefusesItsEdDSAToken(t *testing.T) {
	a := newAccessFixture(t)
	now := time.Now()
	revoked := a.record
	revoked.RevokedAt = &now
	a.sessions.records[a.record.ID] = revoked
	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", a.bearer(a.grant())), http.StatusUnauthorized, "session_revoked")
}

func TestAfterATokenVersionBumpAnEdDSATokenOfTheOldSessionIsRefused(t *testing.T) {
	a := newAccessFixture(t)
	bearer := a.bearer(a.grant())
	if r := a.do(http.MethodGet, "/v1/sessions/current", "", bearer); r.Code != http.StatusOK {
		t.Fatalf("before the bump: %d %s", r.Code, r.Body)
	}
	user := a.users.users[a.principal]
	user.TokenVersion++
	a.users.users[a.principal] = user
	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", bearer), http.StatusUnauthorized, "revision_stale")
}

func TestAnEdDSATokenOfAnUnpinnedSessionIsRefused(t *testing.T) {
	a := newAccessFixture(t)
	unpinned := a.record
	unpinned.LegacyTokenVersion = nil
	a.sessions.records[a.record.ID] = unpinned
	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", a.bearer(a.grant())), http.StatusUnauthorized, "revision_stale")
}

func TestAnEdDSATokenOfASessionPinnedToAnOlderVersionIsRefused(t *testing.T) {
	a := newAccessFixture(t)
	old := int64(3)
	pinned := a.record
	pinned.LegacyTokenVersion = &old
	a.sessions.records[a.record.ID] = pinned
	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", a.bearer(a.grant())), http.StatusUnauthorized, "revision_stale")
}

func TestASingleSessionLogoutRefusesThatSidOnly(t *testing.T) {
	a := newAccessFixture(t)
	other := a.record
	other.ID = uuid.New()
	a.sessions.records[other.ID] = other
	g := a.grant()
	if r := a.do(http.MethodPost, "/v1/sessions/revoke", "", a.bearer(g)); r.Code != http.StatusNoContent {
		t.Fatalf("revoke = %d", r.Code)
	}
	now := time.Now()
	ended := a.record
	ended.RevokedAt = &now
	a.sessions.records[a.record.ID] = ended
	expectError(t, a.do(http.MethodGet, "/v1/sessions/current", "", a.bearer(g)), http.StatusUnauthorized, "session_revoked")
	g.SessionID = other.ID
	if r := a.do(http.MethodGet, "/v1/sessions/current", "", a.bearer(g)); r.Code != http.StatusOK {
		t.Fatalf("the other session = %d, want 200", r.Code)
	}
}

func TestAnIssuerWithoutAnAccessCheckRefusesTheEdDSATokenAsBefore(t *testing.T) {
	signer, _, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, AccessTokenAudience)
	f := newFixtureWith(t, issuer)
	r := f.do(http.MethodGet, "/v1/sessions/current", "", f.bearer(f.grant()))
	expectError(t, r, http.StatusUnauthorized, "bad_signature")
}

func TestTheValidatorSeesNoRouteOutsideItsRows(t *testing.T) {
	rows := []accessverify.Row{{Service: AccessService, Profile: "GET /v1/other", AcceptsUser: true}}
	a := newAccessFixtureWith(t, rows, nil)
	r := a.do(http.MethodGet, "/v1/sessions/current", "", a.bearer(a.grant()))
	if r.Code != http.StatusUnauthorized {
		t.Fatalf("a route with no row answered %d", r.Code)
	}
}

var _ = httptest.NewRecorder

func TestUserBearerRefusesAnEmptyOrInvalidSidMalformed(t *testing.T) {
	subject := session.WirePrincipalID(uuid.New())
	for name, sid := range map[string]string{"empty": "", "not a session id": "ses_!!!", "a bare uuid": uuid.NewString()} {
		bearer, reason, err := userBearer(&authverify.AccessClaims{Subject: subject, SessionID: sid})
		if err == nil || reason != "malformed" || bearer.SessionID != uuid.Nil {
			t.Errorf("%s: bearer %+v, reason %q, err %v; want a malformed refusal", name, bearer, reason, err)
		}
	}
	if _, reason, err := userBearer(&authverify.AccessClaims{Subject: "prn_x", SessionID: session.WireSessionID(uuid.New())}); err == nil || reason != "malformed" {
		t.Errorf("bad subject: reason %q, err %v; want a malformed refusal", reason, err)
	}
	if _, reason, err := userBearer(nil); err == nil || reason != "malformed" {
		t.Errorf("no claims: reason %q, err %v; want a malformed refusal", reason, err)
	}
	sid := uuid.New()
	bearer, reason, err := userBearer(&authverify.AccessClaims{Subject: subject, SessionID: session.WireSessionID(sid)})
	if err != nil || reason != "" || bearer.SessionID != sid || !bearer.sessionPinned {
		t.Errorf("valid claims: bearer %+v, reason %q, err %v", bearer, reason, err)
	}
}
