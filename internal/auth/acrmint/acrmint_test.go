package acrmint

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
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

const testIssuer = "https://auth.example.test"

var (
	testPrincipal = uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	testSession   = uuid.MustParse("00000000-0000-4000-8000-0000000000c1")
	testOrg       = uuid.MustParse("00000000-0000-4000-8000-0000000000b1")
	testNow       = time.Unix(1760000000, 0)
)

// newFileSigner is the production signer type over a fresh one-key set.
func newFileSigner(t *testing.T) *token.Signer {
	t.Helper()
	dir := t.TempDir()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "k1.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	set, err := json.Marshal(map[string]any{"keys": []map[string]string{{"kid": "k1", "state": "active", "file": "k1.pem"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "keyset.json"), set, 0o600); err != nil {
		t.Fatal(err)
	}
	return token.NewFileSigner(token.FileConfig{KeySetPath: filepath.Join(dir, "keyset.json")})
}

type impersonationStub struct {
	active bool
	err    error
	asked  []uuid.UUID
}

func (s *impersonationStub) ImpersonatingActor(_ context.Context, principal uuid.UUID) (bool, error) {
	s.asked = append(s.asked, principal)
	return s.active, s.err
}

type failingSigner struct{}

func (failingSigner) Sign(context.Context, string, []byte) (string, string, error) {
	return "", "", errors.New("custody down")
}

type fixture struct {
	minter *Minter
	signer *token.Signer
	reader *sdkmetric.ManualReader
	logs   *bytes.Buffer
	imp    *impersonationStub
}

func newFixture(t *testing.T, signer Signer) *fixture {
	t.Helper()
	f := &fixture{reader: sdkmetric.NewManualReader(), logs: &bytes.Buffer{}, imp: &impersonationStub{}}
	if signer == nil {
		f.signer = newFileSigner(t)
		signer = f.signer
	}
	m, err := New(Config{
		Signer: signer, IssuerURL: testIssuer, Impersonation: f.imp,
		Meter:  sdkmetric.NewMeterProvider(sdkmetric.WithReader(f.reader)).Meter("test"),
		Logger: slog.New(slog.NewJSONHandler(f.logs, nil)),
		Now:    func() time.Time { return testNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	f.minter = m
	return f
}

func (f *fixture) counts(t *testing.T) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := f.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != MintsMetric || !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				audience, _ := dp.Attributes.Value("audience")
				outcome, _ := dp.Attributes.Value("outcome")
				out[audience.AsString()+"/"+outcome.AsString()] += dp.Value
			}
		}
	}
	return out
}

func request(audience string) Request {
	return Request{Audience: audience, PrincipalID: testPrincipal, SessionID: testSession, OrganizationID: testOrg}
}

// decode splits a compact JWS into its header and claims documents.
func decode(t *testing.T, compact string) (map[string]any, []byte, []byte) {
	t.Helper()
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		t.Fatalf("%d parts", len(parts))
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	claims, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var h map[string]any
	if err := json.Unmarshal(header, &h); err != nil {
		t.Fatal(err)
	}
	return h, header, claims
}

// TestMintedTokenIsAnAccessTokenV1OfItsACRAudience: each ACR audience gets
// exactly the access-token.v1 claim set with its own scope, the 10 minute
// (600 s, pinned as a literal) default life, org_id, the wire sub and sid, and a signature that verifies
// under the signer's published active key.
func TestMintedTokenIsAnAccessTokenV1OfItsACRAudience(t *testing.T) {
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	for audience, scope := range map[string]string{contracts.ACRAPIAudience: "acr:api", contracts.ACRMCPAudience: "acr:mcp"} {
		t.Run(audience, func(t *testing.T) {
			f := newFixture(t, nil)
			compact, kid, err := f.minter.Mint(context.Background(), request(audience))
			if err != nil {
				t.Fatal(err)
			}
			header, headerJSON, claimsJSON := decode(t, compact)
			if header["alg"] != "EdDSA" || header["typ"] != "at+jwt" || header["kid"] != kid {
				t.Fatalf("header %v, kid %q", header, kid)
			}
			parsed, err := contracts.ParseAccessToken(root, []byte(`{"header":`+string(headerJSON)+`,"claims":`+string(claimsJSON)+`}`))
			if err != nil {
				t.Fatalf("not an access-token.v1 document: %v", err)
			}
			c := parsed.Claims
			if c.Iss != testIssuer || c.Aud != audience || c.Scope != scope || c.PrincipalType != "human" ||
				c.Sub != session.WirePrincipalID(testPrincipal) || c.Sid != session.WireSessionID(testSession) ||
				c.OrgID == nil || *c.OrgID != testOrg.String() || c.Iat != testNow.Unix() || c.Nbf != c.Iat ||
				c.Exp-c.Iat != 600 || c.Jti == "" {
				t.Fatalf("claims %+v", c)
			}
			set, err := f.signer.KeySet(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			parts := strings.Split(compact, ".")
			signature, err := base64.RawURLEncoding.DecodeString(parts[2])
			if err != nil {
				t.Fatal(err)
			}
			if set.Active().KeyID != kid || !ed25519.Verify(set.Active().PublicKey, []byte(parts[0]+"."+parts[1]), signature) {
				t.Fatal("the signature does not verify under the published active key")
			}
			if got := f.counts(t); got[audience+"/"+OutcomeMinted] != 1 || len(got) != 1 {
				t.Fatalf("counts %v", got)
			}
			if len(f.imp.asked) != 1 || f.imp.asked[0] != testPrincipal {
				t.Fatalf("impersonation asked for %v", f.imp.asked)
			}
		})
	}
}

// TestALifetimeInsideTheCeilingIsKeptAndOneOverItIsRefused: a shorter life is
// kept; one over 60 minutes, under one second or with a fraction of a second
// is refused, never clamped.
func TestALifetimeInsideTheCeilingIsKeptAndOneOverItIsRefused(t *testing.T) {
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, lifetime := range []time.Duration{time.Second, 5 * time.Minute, contracts.AccessTokenLifetimeCeiling} {
		f := newFixture(t, nil)
		r := request(contracts.ACRMCPAudience)
		r.Lifetime = lifetime
		compact, _, err := f.minter.Mint(context.Background(), r)
		if err != nil {
			t.Fatalf("%s: %v", lifetime, err)
		}
		_, h, c := decode(t, compact)
		parsed, err := contracts.ParseAccessToken(root, []byte(`{"header":`+string(h)+`,"claims":`+string(c)+`}`))
		if err != nil || parsed.Claims.Exp-parsed.Claims.Iat != int64(lifetime/time.Second) {
			t.Fatalf("%s: %v %+v", lifetime, err, parsed)
		}
	}
	for _, lifetime := range []time.Duration{contracts.AccessTokenLifetimeCeiling + time.Second, 24 * time.Hour, -time.Second, 500 * time.Millisecond, 90*time.Second + time.Millisecond} {
		f := newFixture(t, nil)
		r := request(contracts.ACRAPIAudience)
		r.Lifetime = lifetime
		if _, _, err := f.minter.Mint(context.Background(), r); !errors.Is(err, ErrLifetime) {
			t.Errorf("%s: err %v, want ErrLifetime", lifetime, err)
		}
		if got := f.counts(t); got[contracts.ACRAPIAudience+"/"+OutcomeRefusedLifetime] != 1 {
			t.Errorf("%s: counts %v", lifetime, got)
		}
	}
}

// TestEveryRefusalHasItsOutcomeAndNoToken: each refusal returns its error, no
// token, one count under its fixed outcome, and one Warn line whose fields are
// org_id and audience only.
func TestEveryRefusalHasItsOutcomeAndNoToken(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*fixture, *Request)
		signer  Signer
		want    error
		label   string
		outcome string
	}{
		{name: "audience dev-health-api", mutate: func(_ *fixture, r *Request) { r.Audience = contracts.PlatformAPIAudience }, want: ErrAudience, label: otherAudience, outcome: OutcomeRefusedAudience},
		{name: "audience auth-service", mutate: func(_ *fixture, r *Request) { r.Audience = contracts.SessionRouteAudience }, want: ErrAudience, label: otherAudience, outcome: OutcomeRefusedAudience},
		{name: "audience query-api", mutate: func(_ *fixture, r *Request) { r.Audience = "query-api" }, want: ErrAudience, label: otherAudience, outcome: OutcomeRefusedAudience},
		{name: "audience unknown", mutate: func(_ *fixture, r *Request) { r.Audience = "acr-web" }, want: ErrAudience, label: otherAudience, outcome: OutcomeRefusedAudience},
		{name: "audience empty", mutate: func(_ *fixture, r *Request) { r.Audience = "" }, want: ErrAudience, label: otherAudience, outcome: OutcomeRefusedAudience},
		{name: "audience case", mutate: func(_ *fixture, r *Request) { r.Audience = "ACR-API" }, want: ErrAudience, label: otherAudience, outcome: OutcomeRefusedAudience},
		{name: "no organization", mutate: func(_ *fixture, r *Request) { r.OrganizationID = uuid.Nil }, want: ErrOrganization, label: contracts.ACRAPIAudience, outcome: OutcomeRefusedOrganization},
		{name: "no principal", mutate: func(_ *fixture, r *Request) { r.PrincipalID = uuid.Nil }, want: ErrSubject, label: contracts.ACRAPIAudience, outcome: OutcomeRefusedSubject},
		{name: "no session", mutate: func(_ *fixture, r *Request) { r.SessionID = uuid.Nil }, want: ErrSubject, label: contracts.ACRAPIAudience, outcome: OutcomeRefusedSubject},
		{name: "impersonating actor", mutate: func(f *fixture, _ *Request) { f.imp.active = true }, want: ErrImpersonating, label: contracts.ACRAPIAudience, outcome: OutcomeRefusedImpersonating},
		{name: "impersonation read fails", mutate: func(f *fixture, _ *Request) { f.imp.err = errors.New("db down") }, want: ErrImpersonationUnavailable, label: contracts.ACRAPIAudience, outcome: OutcomeImpersonationUnavailable},
		{name: "impersonation read fails while true", mutate: func(f *fixture, _ *Request) { f.imp.active, f.imp.err = true, errors.New("db down") }, want: ErrImpersonationUnavailable, label: contracts.ACRAPIAudience, outcome: OutcomeImpersonationUnavailable},
		{name: "signer fails", signer: failingSigner{}, want: ErrSignerUnavailable, label: contracts.ACRAPIAudience, outcome: OutcomeSignerUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tc.signer)
			r := request(contracts.ACRAPIAudience)
			if tc.mutate != nil {
				tc.mutate(f, &r)
			}
			compact, kid, err := f.minter.Mint(context.Background(), r)
			if !errors.Is(err, tc.want) || compact != "" || kid != "" {
				t.Fatalf("token %q kid %q err %v; want no token and %v", compact, kid, err, tc.want)
			}
			if got := f.counts(t); got[tc.label+"/"+tc.outcome] != 1 || len(got) != 1 {
				t.Fatalf("counts %v; want one %s/%s", got, tc.label, tc.outcome)
			}
			lines := strings.Split(strings.TrimSpace(f.logs.String()), "\n")
			if len(lines) != 1 {
				t.Fatalf("log lines %q", f.logs.String())
			}
			var entry map[string]any
			if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
				t.Fatal(err)
			}
			for key := range entry {
				switch key {
				case "time", "level", "msg", "org_id", "audience":
				default:
					t.Errorf("the Warn line carries %q: %s", key, lines[0])
				}
			}
			if entry["level"] != "WARN" || entry["msg"] != tc.want.Error() || entry["audience"] != tc.label {
				t.Errorf("log line %s", lines[0])
			}
			for _, secret := range []string{testPrincipal.String(), session.WirePrincipalID(testPrincipal), testSession.String(), session.WireSessionID(testSession), "k1"} {
				if strings.Contains(lines[0], secret) {
					t.Errorf("the Warn line carries %q: %s", secret, lines[0])
				}
			}
		})
	}
}

// TestTheTargetOfAnImpersonationIsNotRefused: the check reads the actor side
// only. A principal that is the target of an active impersonation (and holds
// none itself) is minted for; the reader is asked about the principal.
func TestTheTargetOfAnImpersonationIsNotRefused(t *testing.T) {
	target := uuid.MustParse("00000000-0000-4000-8000-0000000000d1")
	admin := uuid.MustParse("00000000-0000-4000-8000-0000000000d2")
	actors := map[uuid.UUID]bool{admin: true}
	f := newFixture(t, nil)
	f.minter.impersonation = actorSet(actors)
	r := request(contracts.ACRMCPAudience)
	r.PrincipalID = target
	if _, _, err := f.minter.Mint(context.Background(), r); err != nil {
		t.Fatalf("the target was refused: %v", err)
	}
	r.PrincipalID = admin
	if _, _, err := f.minter.Mint(context.Background(), r); !errors.Is(err, ErrImpersonating) {
		t.Fatalf("the actor was not refused: %v", err)
	}
}

type actorSet map[uuid.UUID]bool

func (a actorSet) ImpersonatingActor(_ context.Context, principal uuid.UUID) (bool, error) {
	return a[principal], nil
}

// TestNewRefusesAMissingSignerIssuerOrImpersonationReader.
func TestNewRefusesAMissingSignerIssuerOrImpersonationReader(t *testing.T) {
	signer := newFileSigner(t)
	imp := &impersonationStub{}
	for name, cfg := range map[string]Config{
		"no signer":        {IssuerURL: testIssuer, Impersonation: imp},
		"no issuer":        {Signer: signer, Impersonation: imp},
		"no impersonation": {Signer: signer, IssuerURL: testIssuer},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: New built a minter", name)
		}
	}
	if _, err := New(Config{Signer: signer, IssuerURL: testIssuer, Impersonation: imp}); err != nil {
		t.Fatalf("control: %v", err)
	}
}
