package accessverify

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-go/authverify"
)

const (
	testIssuer = "https://auth.example.test"
	svc        = "dev-health-ops-api"
	userRoute  = "GET /api/v1/things/{id}"
	legacyRte  = "DELETE /api/v1/admin/things/{id}"
	bothRoute  = "GET /api/v1/both/{id}"
)

var now = time.Unix(1_800_000_000, 0)

type fixture struct {
	t      *testing.T
	pub    ed25519.PublicKey
	priv   ed25519.PrivateKey
	reader *sdkmetric.ManualReader
	v      *Validator
	logs   *bytes.Buffer
}

func newFixture(t *testing.T, withEdDSA bool) *fixture {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, pub: pub, priv: priv, reader: sdkmetric.NewManualReader(), logs: &bytes.Buffer{}}
	cfg := Config{
		Rows: []Row{
			{Service: svc, Profile: userRoute, AcceptsUser: true},
			{Service: svc, Profile: legacyRte},
			{Service: svc, Profile: bothRoute, AcceptsUser: true},
		},
		Meter:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(f.reader)).Meter("test"),
		Logger: slog.New(slog.NewTextHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Now:    func() time.Time { return now },
	}
	if withEdDSA {
		verifier, err := authverify.NewVerifier(authverify.VerifierConfig{
			Issuer: testIssuer, Audience: "dev-health-api", Keys: authverify.StaticKeys{"kid-1": pub},
		})
		if err != nil {
			t.Fatal(err)
		}
		cfg.EdDSA = verifier
	}
	f.v, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func b64(raw []byte) string { return base64.RawURLEncoding.EncodeToString(raw) }

func (f *fixture) eddsa(header map[string]any, claims map[string]any) string {
	h, _ := json.Marshal(header)
	c, _ := json.Marshal(claims)
	signing := b64(h) + "." + b64(c)
	return signing + "." + b64(ed25519.Sign(f.priv, []byte(signing)))
}

func goodHeader() map[string]any {
	return map[string]any{"alg": "EdDSA", "typ": "at+jwt", "kid": "kid-1"}
}

func goodClaims() map[string]any {
	n := now.Unix()
	return map[string]any{
		"iss": testIssuer, "aud": "dev-health-api", "sub": "prn_abc", "principal_type": "human",
		"sid": "ses_" + strings.Repeat("a", 22), "jti": strings.Repeat("j", 22),
		"iat": n - 10, "nbf": n - 10, "exp": n + 600, "scope": "api:access",
	}
}

func hs256Token() string {
	h, _ := json.Marshal(map[string]any{"alg": "HS256", "typ": "JWT"})
	return b64(h) + "." + b64([]byte(`{"sub":"x"}`)) + "." + b64([]byte("sig"))
}

func tokenWithAlg(raw string) string {
	return b64([]byte(raw)) + "." + b64([]byte(`{}`)) + "." + b64([]byte("sig"))
}

func (f *fixture) counters() map[string]int64 {
	f.t.Helper()
	var rm metricdata.ResourceMetrics
	if err := f.reader.Collect(context.Background(), &rm); err != nil {
		f.t.Fatal(err)
	}
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				f.t.Fatalf("%s is not an int64 sum", m.Name)
			}
			for _, dp := range sum.DataPoints {
				attrs := dp.Attributes
				var parts []string
				for _, kv := range attrs.ToSlice() {
					parts = append(parts, string(kv.Key)+"="+kv.Value.Emit())
				}
				out[m.Name+"|"+strings.Join(parts, ",")] = dp.Value
			}
		}
	}
	return out
}

func accKey(profile, class string) string {
	return AcceptedMetric + "|class=" + class + ",profile=" + profile + ",service=" + svc
}

func refKey(profile, reason string) string {
	return RefusedMetric + "|profile=" + profile + ",reason=" + reason + ",service=" + svc
}

func TestValidate_EdDSAOnUserRow_Accepts(t *testing.T) {
	f := newFixture(t, true)
	res, err := f.v.Validate(context.Background(), svc, userRoute, f.eddsa(goodHeader(), goodClaims()))
	if err != nil || res.Class != ClassUser || res.Claims == nil || res.Claims.Subject != "prn_abc" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if got := f.counters()[accKey(userRoute, ClassUser)]; got != 1 {
		t.Fatalf("accepted counter = %d", got)
	}
}

func TestValidate_HS256_RefusedOnEveryRow(t *testing.T) {
	f := newFixture(t, true)
	for _, profile := range []string{userRoute, legacyRte, bothRoute} {
		res, err := f.v.Validate(context.Background(), svc, profile, hs256Token())
		if !errors.Is(err, ErrRefused) || res != nil {
			t.Fatalf("%s: res=%+v err=%v", profile, res, err)
		}
	}
	c := f.counters()
	for _, profile := range []string{userRoute, legacyRte, bothRoute} {
		if c[refKey(profile, ReasonAlgNotAllowed)] != 1 {
			t.Fatalf("%s: counters %v", profile, c)
		}
	}
	for k, v := range c {
		if strings.HasPrefix(k, AcceptedMetric+"|") && v != 0 {
			t.Fatalf("accepted series %s = %d", k, v)
		}
	}
}

func TestValidate_HS256HeaderIsRefusedWhateverItsOtherFields(t *testing.T) {
	f := newFixture(t, true)
	for _, header := range []string{`{"alg":"HS256","typ":"at+jwt","kid":"kid-1"}`, `{"alg":"HS256"}`} {
		if _, err := f.v.Validate(context.Background(), svc, userRoute, tokenWithAlg(header)); !errors.Is(err, ErrRefused) {
			t.Fatalf("%s: err=%v", header, err)
		}
	}
	if f.counters()[refKey(userRoute, ReasonAlgNotAllowed)] != 2 {
		t.Fatalf("counters %v", f.counters())
	}
}

func TestValidate_EdDSAOnRowWithoutUserClass_Refused(t *testing.T) {
	f := newFixture(t, true)
	_, err := f.v.Validate(context.Background(), svc, legacyRte, f.eddsa(goodHeader(), goodClaims()))
	if !errors.Is(err, ErrRefused) || f.counters()[refKey(legacyRte, ReasonAlgNotAllowed)] != 1 {
		t.Fatalf("err=%v counters=%v", err, f.counters())
	}
}

func TestValidate_AlgTable(t *testing.T) {
	f := newFixture(t, true)
	cases := map[string]struct {
		header string
		reason string
	}{
		"none":          {`{"alg":"none"}`, ReasonAlgNotAllowed},
		"hs384":         {`{"alg":"HS384"}`, ReasonAlgNotAllowed},
		"lowercase hs":  {`{"alg":"hs256"}`, ReasonAlgNotAllowed},
		"lowercase eds": {`{"alg":"eddsa"}`, ReasonAlgNotAllowed},
		"rs256":         {`{"alg":"RS256"}`, ReasonAlgNotAllowed},
		"missing":       {`{"typ":"at+jwt"}`, ReasonMalformed},
		"array":         {`[]`, ReasonMalformed},
		"number alg":    {`{"alg":5}`, ReasonMalformed},
		"duplicate alg": {`{"alg":"EdDSA","alg":"HS256"}`, ReasonMalformed},
		"trailing":      {`{"alg":"EdDSA"} {}`, ReasonMalformed},
		"not json":      {`nope`, ReasonMalformed},
	}
	for name, tc := range cases {
		before := f.counters()[refKey(bothRoute, tc.reason)]
		if _, err := f.v.Validate(context.Background(), svc, bothRoute, tokenWithAlg(tc.header)); !errors.Is(err, ErrRefused) {
			t.Fatalf("%s: err=%v", name, err)
		}
		if got := f.counters()[refKey(bothRoute, tc.reason)]; got != before+1 {
			t.Fatalf("%s: %s counter %d -> %d", name, tc.reason, before, got)
		}
	}
	for _, raw := range []string{"", "abc", "...", "a.b"} {
		if _, err := f.v.Validate(context.Background(), svc, bothRoute, raw); !errors.Is(err, ErrRefused) {
			t.Fatalf("%q: err=%v", raw, err)
		}
	}
}

func TestValidate_VerifierReasonsMapToClosedSet(t *testing.T) {
	f := newFixture(t, true)
	badKid := goodHeader()
	badKid["kid"] = "nope"
	expired := goodClaims()
	expired["exp"] = now.Unix() - 100
	expired["nbf"] = now.Unix() - 200
	expired["iat"] = now.Unix() - 200
	wrongAud := goodClaims()
	wrongAud["aud"] = "auth-service"
	wrongAud["scope"] = "auth:session"
	tampered := f.eddsa(goodHeader(), goodClaims())
	tampered = tampered[:len(tampered)-4] + "AAAA"
	cases := map[string]struct{ token, reason string }{
		"kid":       {f.eddsa(badKid, goodClaims()), ReasonKidUnknown},
		"expired":   {f.eddsa(goodHeader(), expired), ReasonExpired},
		"audience":  {f.eddsa(goodHeader(), wrongAud), ReasonClaims},
		"signature": {tampered, ReasonSignature},
		"contract":  {f.eddsa(map[string]any{"alg": "EdDSA", "typ": "JWT", "kid": "kid-1"}, goodClaims()), ReasonMalformed},
	}
	for name, tc := range cases {
		if _, err := f.v.Validate(context.Background(), svc, userRoute, tc.token); !errors.Is(err, ErrRefused) {
			t.Fatalf("%s: err=%v", name, err)
		}
		if f.counters()[refKey(userRoute, tc.reason)] != 1 {
			t.Fatalf("%s: want one %s, counters %v", name, tc.reason, f.counters())
		}
	}
}

func TestValidate_ZeroRowAndUnknownProfile_Refused(t *testing.T) {
	f := newFixture(t, true)
	for _, probe := range [][2]string{{"", ""}, {svc, ""}, {svc, "GET /api/v1/things/42"}, {"other-svc", userRoute}} {
		if _, err := f.v.Validate(context.Background(), probe[0], probe[1], f.eddsa(goodHeader(), goodClaims())); !errors.Is(err, ErrRefused) {
			t.Fatalf("%v: err=%v", probe, err)
		}
	}
	got := f.counters()[RefusedMetric+"|profile=unknown,reason="+ReasonNoProfile+",service=unknown"]
	if got != 4 {
		t.Fatalf("no_profile counter = %d", got)
	}
	for k := range f.counters() {
		if strings.Contains(k, "42") || strings.Contains(k, "other-svc") {
			t.Fatalf("raw input became a label: %s", k)
		}
	}
}

func TestValidate_NilVerifier_FailsClosed(t *testing.T) {
	f := newFixture(t, false)
	if _, err := f.v.Validate(context.Background(), svc, bothRoute, f.eddsa(goodHeader(), goodClaims())); !errors.Is(err, ErrRefused) {
		t.Fatalf("eddsa: %v", err)
	}
	if _, err := f.v.Validate(context.Background(), svc, bothRoute, hs256Token()); !errors.Is(err, ErrRefused) {
		t.Fatalf("hs256: %v", err)
	}
	if c := f.counters(); c[refKey(bothRoute, ReasonNotConfigured)] != 1 || c[refKey(bothRoute, ReasonAlgNotAllowed)] != 1 {
		t.Fatalf("counters %v", f.counters())
	}
}

func TestCounter_PreRegisteredAtZero(t *testing.T) {
	f := newFixture(t, true)
	c := f.counters()
	want := []string{accKey(userRoute, ClassUser), accKey(bothRoute, ClassUser)}
	for _, reason := range Reasons {
		for _, p := range []string{userRoute, legacyRte, bothRoute} {
			want = append(want, refKey(p, reason))
		}
	}
	for _, k := range want {
		if v, ok := c[k]; !ok || v != 0 {
			t.Fatalf("series %s missing or non-zero (%d, %v)", k, v, ok)
		}
	}
	for _, p := range []string{userRoute, legacyRte, bothRoute} {
		if _, ok := c[accKey(p, ClassLegacy)]; ok {
			t.Fatalf("%s: the retired HS256 class was pre-registered", p)
		}
	}
	if _, ok := c[accKey(legacyRte, ClassUser)]; ok {
		t.Fatal("a class the row does not accept was pre-registered")
	}
	want = append(want, RefusedMetric+"|profile=unknown,reason="+ReasonNoProfile+",service=unknown")
	if len(c) != len(want) {
		t.Fatalf("%d series, want %d", len(c), len(want))
	}
}

func TestCounter_OneRowPerOutcome(t *testing.T) {
	f := newFixture(t, true)
	ctx := context.Background()
	_, _ = f.v.Validate(ctx, svc, userRoute, f.eddsa(goodHeader(), goodClaims()))
	_, _ = f.v.Validate(ctx, svc, legacyRte, hs256Token())
	_, _ = f.v.Validate(ctx, svc, userRoute, hs256Token())
	before := len(f.counters())
	c := f.counters()
	if c[accKey(userRoute, ClassUser)] != 1 || c[refKey(legacyRte, ReasonAlgNotAllowed)] != 1 || c[refKey(userRoute, ReasonAlgNotAllowed)] != 1 {
		t.Fatalf("counters %v", c)
	}
	var sum int64
	for _, v := range c {
		sum += v
	}
	if sum != 3 || before != len(f.counters()) {
		t.Fatalf("outcome total %d, want 3", sum)
	}
}

func TestLogs_NeverContainToken(t *testing.T) {
	f := newFixture(t, true)
	token := f.eddsa(goodHeader(), goodClaims())
	token = token[:len(token)-4] + "AAAA"
	_, _ = f.v.Validate(context.Background(), svc, userRoute, token)
	_, _ = f.v.Validate(context.Background(), svc, userRoute, hs256Token())
	out := f.logs.String()
	for _, secret := range []string{token, "kid-1", "prn_abc", "ses_", strings.Split(token, ".")[0]} {
		if strings.Contains(out, secret) {
			t.Fatalf("log leaks %q: %s", secret, out)
		}
	}
	if !strings.Contains(out, "reason="+ReasonSignature) {
		t.Fatalf("log lacks the reason: %s", out)
	}
}

func TestCounter_NoProfileSeriesPreRegisteredAtZero(t *testing.T) {
	f := newFixture(t, true)
	key := RefusedMetric + "|profile=unknown,reason=" + ReasonNoProfile + ",service=unknown"
	if v, ok := f.counters()[key]; !ok || v != 0 {
		t.Fatalf("series missing or non-zero: %d %v", v, ok)
	}
}

func TestNew_RejectsBadInput(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("no meter accepted")
	}
	meter := sdkmetric.NewMeterProvider().Meter("t")
	if _, err := New(Config{Meter: meter, Rows: []Row{{Service: "s"}}}); err == nil {
		t.Fatal("row without profile accepted")
	}
}
