package clientaddr

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"
)

// Test keys: 32 bytes each, base64url without padding. Test values only.
var (
	testSecretA = strings.Repeat("a", 32)
	testSecretB = strings.Repeat("b", 32)
	testEntryA  = "k1:" + base64.RawURLEncoding.EncodeToString([]byte(testSecretA))
	testEntryB  = "k2:" + base64.RawURLEncoding.EncodeToString([]byte(testSecretB))
	testNow     = time.Unix(1_800_000_000, 0)
)

const (
	trustedPeer   = "10.42.0.7"
	untrustedPeer = "192.0.2.50"
	provedClient  = "203.0.113.9"
	forgedClient  = "198.51.100.4"
	loginTarget   = "/api/v1/auth/login?next=%2Fhome"
	testRequestID = "0f6d6c1e-2f43-4d8e-9a43-7e7c8f0b1d2a"
)

func mustKeys(t *testing.T, value string) []ProofKey {
	t.Helper()
	keys, err := ParseProofKeys(EnvVerifyKey, value, MaxVerifyKeys)
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func testVerifier(t *testing.T, value string) *Verifier {
	t.Helper()
	v := NewVerifier(mustKeys(t, value))
	v.now = func() time.Time { return testNow }
	return v
}

func testTrust(t *testing.T) Trust {
	t.Helper()
	trust, err := ParseTrust("10.42.0.0/24,!10.42.0.0,!10.42.0.1")
	if err != nil {
		t.Fatal(err)
	}
	return trust
}

// request is one inbound request as auth-service's server sees it.
func inbound(method, target, peer string) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	r.RemoteAddr = peer + ":40000"
	return r
}

// proofCase builds the request of one proof outcome.
type proofCase struct {
	name    string
	outcome Outcome
	build   func(t *testing.T, peer string) *http.Request
}

func signed(t *testing.T, peer string, key ProofKey, at time.Time) *http.Request {
	t.Helper()
	r := inbound(http.MethodPost, loginTarget, peer)
	r.Header.Set("X-Request-Id", testRequestID)
	r.Header.Set("X-Forwarded-For", provedClient)
	r.Header.Set(ProofHeader, Prove(key, at, netip.MustParseAddr(provedClient), r.Method, r.RequestURI, testRequestID))
	return r
}

func proofCases(t *testing.T) []proofCase {
	keyA := mustKeys(t, testEntryA)[0]
	keyB := mustKeys(t, testEntryB)[0]
	return []proofCase{
		{"valid", OutcomeValid, func(t *testing.T, peer string) *http.Request { return signed(t, peer, keyA, testNow) }},
		{"bad mac (other key, same kid)", OutcomeBadMAC, func(t *testing.T, peer string) *http.Request {
			return signed(t, peer, ProofKey{id: "k1", secret: []byte(testSecretB)}, testNow)
		}},
		{"old ts", OutcomeStale, func(t *testing.T, peer string) *http.Request {
			return signed(t, peer, keyA, testNow.Add(-ProofMaxAge-time.Second))
		}},
		{"future ts", OutcomeFuture, func(t *testing.T, peer string) *http.Request {
			return signed(t, peer, keyA, testNow.Add(ProofMaxAhead+time.Second))
		}},
		{"unknown kid", OutcomeUnknownKid, func(t *testing.T, peer string) *http.Request { return signed(t, peer, keyB, testNow) }},
		{"missing header", OutcomeAbsent, func(t *testing.T, peer string) *http.Request {
			r := inbound(http.MethodPost, loginTarget, peer)
			r.Header.Set("X-Forwarded-For", forgedClient)
			return r
		}},
	}
}

func TestProofOutcomes(t *testing.T) {
	v := testVerifier(t, testEntryA)
	for _, c := range proofCases(t) {
		addr, outcome := v.Check(c.build(t, trustedPeer))
		if outcome != c.outcome {
			t.Errorf("%s: outcome %s, want %s", c.name, outcome, c.outcome)
		}
		if (outcome == OutcomeValid) != addr.IsValid() {
			t.Errorf("%s: address %v with outcome %s", c.name, addr, outcome)
		}
	}
}

func TestProofAgeEdges(t *testing.T) {
	v := testVerifier(t, testEntryA)
	key := mustKeys(t, testEntryA)[0]
	for _, c := range []struct {
		at   time.Time
		want Outcome
	}{
		{testNow.Add(-ProofMaxAge), OutcomeValid},
		{testNow.Add(-ProofMaxAge - time.Second), OutcomeStale},
		{testNow.Add(ProofMaxAhead), OutcomeValid},
		{testNow.Add(ProofMaxAhead + time.Second), OutcomeFuture},
	} {
		if _, got := v.Check(signed(t, trustedPeer, key, c.at)); got != c.want {
			t.Errorf("proof at now%+v: %s, want %s", c.at.Sub(testNow), got, c.want)
		}
	}
}

// TestModeMatrix: each mode x each proof outcome x a peer inside and outside the trusted-proxy list, on a route that
// is not well-known. "xff" = the X-Forwarded-For value is the client, "peer" = the socket peer is.
func TestModeMatrix(t *testing.T) {
	want := map[Mode]map[Outcome][2]string{ // [trusted peer, untrusted peer]
		ModeCIDR: {
			OutcomeValid: {"xff", "peer"}, OutcomeBadMAC: {"xff", "peer"}, OutcomeStale: {"xff", "peer"},
			OutcomeFuture: {"xff", "peer"}, OutcomeUnknownKid: {"xff", "peer"}, OutcomeAbsent: {"xff", "peer"},
		},
		ModeBoth: {
			OutcomeValid: {"xff", "xff"}, OutcomeBadMAC: {"xff", "peer"}, OutcomeStale: {"xff", "peer"},
			OutcomeFuture: {"xff", "peer"}, OutcomeUnknownKid: {"xff", "peer"}, OutcomeAbsent: {"xff", "peer"},
		},
		ModeSigned: {
			OutcomeValid: {"xff", "xff"}, OutcomeBadMAC: {"peer", "peer"}, OutcomeStale: {"peer", "peer"},
			OutcomeFuture: {"peer", "peer"}, OutcomeUnknownKid: {"peer", "peer"}, OutcomeAbsent: {"peer", "peer"},
		},
	}
	trust := testTrust(t)
	for mode, rows := range want {
		policy := Policy{Mode: mode, Trust: trust, Verifier: testVerifier(t, testEntryA)}
		for _, c := range proofCases(t) {
			for index, peer := range []string{trustedPeer, untrustedPeer} {
				r := c.build(t, peer)
				expect := peer
				if rows[c.outcome][index] == "xff" {
					expect = r.Header.Get("X-Forwarded-For")
				}
				var bound netip.Addr
				PolicyMiddleware(policy)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
					bound, _ = From(r.Context())
				})).ServeHTTP(httptest.NewRecorder(), r)
				if bound.String() != expect {
					t.Errorf("mode %s, %s, peer %s: bound %v, want %s", mode, c.name, peer, bound, expect)
				}
				if got := policy.Resolve(c.build(t, peer)); got.String() != expect {
					t.Errorf("mode %s, %s, peer %s: Resolve %v, want %s", mode, c.name, peer, got, expect)
				}
			}
		}
	}
}

func TestModeCIDRWithoutKeysIsTodaysRule(t *testing.T) {
	trust := testTrust(t)
	policy := Policy{Mode: ModeCIDR, Trust: trust}
	for _, c := range proofCases(t) {
		for _, peer := range []string{trustedPeer, untrustedPeer, "10.42.0.1"} {
			r := c.build(t, peer)
			if got, want := policy.Resolve(r), trust.Resolve(r); got != want {
				t.Errorf("%s, peer %s: %v, want Trust.Resolve %v", c.name, peer, got, want)
			}
		}
	}
}

// wellKnownRequests are the 4 requests of the well-known CIDR exception.
var wellKnownRequests = [][2]string{
	{http.MethodGet, "/.well-known/jwks.json"},
	{http.MethodHead, "/.well-known/jwks.json"},
	{http.MethodGet, "/.well-known/openid-configuration"},
	{http.MethodHead, "/.well-known/openid-configuration"},
}

func TestSignedModeWellKnownExceptionReadsTheTrustedProxyList(t *testing.T) {
	policy := Policy{Mode: ModeSigned, Trust: testTrust(t), Verifier: testVerifier(t, testEntryA)}
	for _, req := range wellKnownRequests {
		r := inbound(req[0], req[1], trustedPeer)
		r.Header.Set("X-Forwarded-For", forgedClient)
		if got := policy.Resolve(r); got.String() != forgedClient {
			t.Errorf("%s %s from a trusted proxy: %v, want the forwarded %s", req[0], req[1], got, forgedClient)
		}
		r = inbound(req[0], req[1], untrustedPeer)
		r.Header.Set("X-Forwarded-For", forgedClient)
		if got := policy.Resolve(r); got.String() != untrustedPeer {
			t.Errorf("%s %s from an untrusted peer: %v, want the peer", req[0], req[1], got)
		}
	}
}

// TestSignedModeNonWellKnownNeverTakesTheCIDRPath: in mode signed a request outside the 4 well-known requests, from a
// peer inside the trusted-proxy list and with no valid proof, is always named by its socket peer.
func TestSignedModeNonWellKnownNeverTakesTheCIDRPath(t *testing.T) {
	policy := Policy{Mode: ModeSigned, Trust: testTrust(t), Verifier: testVerifier(t, testEntryA)}
	for _, req := range [][2]string{
		{http.MethodPost, "/api/v1/auth/login"},
		{http.MethodPost, "/api/v1/auth/refresh"},
		{http.MethodGet, "/api/v1/auth/me"},
		{http.MethodPost, "/.well-known/jwks.json"},
		{http.MethodOptions, "/.well-known/openid-configuration"},
		{http.MethodGet, "/.well-known/jwks.json/"},
		{http.MethodGet, "//.well-known/jwks.json"},
		{http.MethodGet, "/.well-known/jwks%2Ejson"},
		{http.MethodGet, "/.well-known/openid-configuration/x"},
		{http.MethodGet, "/oauth/.well-known/jwks.json"},
		{http.MethodGet, "/oauth/.well-known/openid-configuration"},
		{http.MethodGet, "/.well-known/"},
	} {
		r := inbound(req[0], req[1], trustedPeer)
		r.Header.Set("X-Forwarded-For", forgedClient)
		r.Header.Set("X-Real-IP", forgedClient)
		if got := policy.Resolve(r); got.String() != trustedPeer {
			t.Errorf("%s %s: %v, want the peer %s (no CIDR path in mode signed)", req[0], req[1], got, trustedPeer)
		}
		if policy.Forwarded(r) {
			t.Errorf("%s %s: Forwarded = true without a proof in mode signed", req[0], req[1])
		}
	}
}

func TestForwardedFollowsTheMode(t *testing.T) {
	key := mustKeys(t, testEntryA)[0]
	for _, c := range []struct {
		mode        Mode
		withProof   bool
		peer        string
		wantForward bool
	}{
		{ModeCIDR, false, trustedPeer, true},
		{ModeCIDR, true, untrustedPeer, false},
		{ModeBoth, true, untrustedPeer, true},
		{ModeBoth, false, trustedPeer, true},
		{ModeSigned, true, untrustedPeer, true},
		{ModeSigned, false, trustedPeer, false},
	} {
		policy := Policy{Mode: c.mode, Trust: testTrust(t), Verifier: testVerifier(t, testEntryA)}
		r := inbound(http.MethodPost, loginTarget, c.peer)
		if c.withProof {
			r = signed(t, c.peer, key, testNow)
		}
		if got := policy.Forwarded(r); got != c.wantForward {
			t.Errorf("mode %s, proof %v, peer %s: Forwarded %v, want %v", c.mode, c.withProof, c.peer, got, c.wantForward)
		}
	}
}

// TestProofIsBoundToItsRequest: a valid proof moved to another method, target, request id or address is refused.
func TestProofIsBoundToItsRequest(t *testing.T) {
	v := testVerifier(t, testEntryA)
	key := mustKeys(t, testEntryA)[0]
	for name, change := range map[string]func(r *http.Request) *http.Request{
		"method": func(r *http.Request) *http.Request {
			out := inbound(http.MethodPut, loginTarget, trustedPeer)
			out.Header = r.Header.Clone()
			return out
		},
		"target": func(r *http.Request) *http.Request {
			out := inbound(http.MethodPost, "/api/v1/auth/refresh", trustedPeer)
			out.Header = r.Header.Clone()
			return out
		},
		"query": func(r *http.Request) *http.Request {
			out := inbound(http.MethodPost, "/api/v1/auth/login?next=%2Fadmin", trustedPeer)
			out.Header = r.Header.Clone()
			return out
		},
		"request id": func(r *http.Request) *http.Request {
			r.Header.Set("X-Request-Id", "11111111-2f43-4d8e-9a43-7e7c8f0b1d2a")
			return r
		},
		"request id removed": func(r *http.Request) *http.Request {
			r.Header.Del("X-Request-Id")
			return r
		},
		"address": func(r *http.Request) *http.Request {
			r.Header.Set("X-Forwarded-For", forgedClient)
			return r
		},
	} {
		if _, got := v.Check(change(signed(t, trustedPeer, key, testNow))); got != OutcomeBadMAC {
			t.Errorf("%s changed: %s, want %s", name, got, OutcomeBadMAC)
		}
	}
}

func TestProofMalformedForms(t *testing.T) {
	v := testVerifier(t, testEntryA)
	key := mustKeys(t, testEntryA)[0]
	valid := func() *http.Request { return signed(t, trustedPeer, key, testNow) }
	parts := strings.Split(valid().Header.Get(ProofHeader), ".")
	for name, change := range map[string]func(r *http.Request){
		"two proof lines":      func(r *http.Request) { r.Header.Add(ProofHeader, r.Header.Get(ProofHeader)) },
		"two XFF lines":        func(r *http.Request) { r.Header.Add("X-Forwarded-For", provedClient) },
		"XFF chain":            func(r *http.Request) { r.Header.Set("X-Forwarded-For", forgedClient+", "+provedClient) },
		"XFF with port":        func(r *http.Request) { r.Header.Set("X-Forwarded-For", provedClient+":443") },
		"XFF IPv4-mapped":      func(r *http.Request) { r.Header.Set("X-Forwarded-For", "::ffff:"+provedClient) },
		"XFF not canonical":    func(r *http.Request) { r.Header.Set("X-Forwarded-For", "2001:DB8::1") },
		"XFF absent":           func(r *http.Request) { r.Header.Del("X-Forwarded-For") },
		"two request id lines": func(r *http.Request) { r.Header.Add("X-Request-Id", testRequestID) },
		"version":              func(r *http.Request) { r.Header.Set(ProofHeader, "v2."+strings.Join(parts[1:], ".")) },
		"kid upper case": func(r *http.Request) {
			r.Header.Set(ProofHeader, strings.Join([]string{parts[0], "K1", parts[2], parts[3]}, "."))
		},
		"ts signed": func(r *http.Request) {
			r.Header.Set(ProofHeader, strings.Join([]string{parts[0], parts[1], "+" + parts[2], parts[3]}, "."))
		},
		"ts empty": func(r *http.Request) {
			r.Header.Set(ProofHeader, strings.Join([]string{parts[0], parts[1], "", parts[3]}, "."))
		},
		"mac padded": func(r *http.Request) {
			r.Header.Set(ProofHeader, strings.Join([]string{parts[0], parts[1], parts[2], parts[3] + "="}, "."))
		},
		"mac short": func(r *http.Request) {
			r.Header.Set(ProofHeader, strings.Join([]string{parts[0], parts[1], parts[2], parts[3][:20]}, "."))
		},
		"five fields": func(r *http.Request) { r.Header.Set(ProofHeader, r.Header.Get(ProofHeader)+".x") },
		"empty":       func(r *http.Request) { r.Header.Set(ProofHeader, "") },
	} {
		r := valid()
		change(r)
		if _, got := v.Check(r); got != OutcomeMalformed {
			t.Errorf("%s: %s, want %s", name, got, OutcomeMalformed)
		}
	}
}

func TestVerifierHoldsTwoKeysForRotation(t *testing.T) {
	v := testVerifier(t, testEntryA+","+testEntryB)
	for _, entry := range []string{testEntryA, testEntryB} {
		if _, got := v.Check(signed(t, trustedPeer, mustKeys(t, entry)[0], testNow)); got != OutcomeValid {
			t.Errorf("kid %s: %s, want valid", mustKeys(t, entry)[0].ID(), got)
		}
	}
}

func TestParseProofKeys(t *testing.T) {
	if keys, err := ParseProofKeys(EnvVerifyKey, " ", MaxVerifyKeys); err != nil || keys != nil {
		t.Fatalf("empty: %v %v, want no key and no error", keys, err)
	}
	if keys := mustKeys(t, testEntryA+", "+testEntryB); len(keys) != 2 || keys[0].ID() != "k1" || keys[1].ID() != "k2" {
		t.Fatalf("two entries: %v", keys)
	}
	short := base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("s", MinProofKeyBytes-1)))
	for name, value := range map[string]string{
		"three entries": testEntryA + "," + testEntryB + ",k3:" + strings.SplitN(testEntryA, ":", 2)[1],
		"short key":     "k1:" + short,
		"padded key":    "k1:" + base64.URLEncoding.EncodeToString([]byte(strings.Repeat("p", 32))),
		"std alphabet":  "k1:" + strings.Repeat("+", 43),
		"no kid":        ":" + strings.SplitN(testEntryA, ":", 2)[1],
		"bad kid":       "K_1:" + strings.SplitN(testEntryA, ":", 2)[1],
		"no colon":      strings.SplitN(testEntryA, ":", 2)[1],
		"repeated kid":  testEntryA + "," + testEntryA,
		"empty entry":   testEntryA + ",",
	} {
		_, err := ParseProofKeys(EnvVerifyKey, value, MaxVerifyKeys)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if secret := strings.SplitN(testEntryA, ":", 2)[1]; strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), short) {
			t.Errorf("%s: the error echoes a key: %v", name, err)
		}
	}
	if _, err := ParseSigningKey(testEntryA + "," + testEntryB); err == nil {
		t.Error("go-api signing key with two entries accepted")
	}
	if key, err := ParseSigningKey(""); err != nil || key != nil {
		t.Errorf("empty signing key: %v %v, want none", key, err)
	}
}

func TestParseMode(t *testing.T) {
	for value, want := range map[string]Mode{"": ModeCIDR, "cidr": ModeCIDR, " both ": ModeBoth, "signed": ModeSigned} {
		if got, err := ParseMode(value); err != nil || got != want {
			t.Errorf("ParseMode(%q) = %q %v, want %q", value, got, err, want)
		}
	}
	for _, value := range []string{"SIGNED", "on", "true", "signed,both"} {
		if _, err := ParseMode(value); err == nil {
			t.Errorf("ParseMode(%q) accepted", value)
		}
	}
}

// TestSetProofRoundTripsOverHTTP: a proof written by SetProof on a real client request verifies on the server side,
// so the client's URL.RequestURI and the server's RequestURI agree (escaped path and query included).
func TestSetProofRoundTripsOverHTTP(t *testing.T) {
	key := mustKeys(t, testEntryA)[0]
	policy := Policy{Mode: ModeSigned, Verifier: NewVerifier([]ProofKey{key})}
	var bound netip.Addr
	var outcome Outcome
	server := httptest.NewServer(PolicyMiddleware(policy)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bound, _ = From(r.Context())
		_, outcome = policy.Verifier.Check(r)
	})))
	defer server.Close()
	for _, target := range []string{"/api/v1/auth/login", "/api/v1/auth/oidc/a%2Fb/callback?code=x&state=y", "/api/v1/auth/verify?token=a+b"} {
		out, err := http.NewRequest(http.MethodPost, server.URL+target, nil)
		if err != nil {
			t.Fatal(err)
		}
		out.Header.Set("X-Request-Id", testRequestID)
		SetProof(out, &key, time.Now(), netip.MustParseAddr("2001:db8::7"))
		response, err := http.DefaultClient.Do(out)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if outcome != OutcomeValid || bound.String() != "2001:db8::7" {
			t.Errorf("%s: outcome %s, bound %v, want valid and 2001:db8::7", target, outcome, bound)
		}
	}
}

func TestSetProofWritesNothingWithoutAKey(t *testing.T) {
	out, err := http.NewRequest(http.MethodPost, "http://auth-service:8095/api/v1/auth/login", nil)
	if err != nil {
		t.Fatal(err)
	}
	SetProof(out, nil, time.Now(), netip.MustParseAddr(provedClient))
	if len(out.Header) != 0 {
		t.Fatalf("headers %v, want none", out.Header)
	}
}

func TestProofLogCarriesNoAddressKeyOrMAC(t *testing.T) {
	var lines []string
	policy := Policy{Mode: ModeSigned, Trust: testTrust(t), Verifier: testVerifier(t, testEntryA),
		Log: func(_ context.Context, outcome, route string) { lines = append(lines, outcome+" "+route) }}
	proofWarn.Lock()
	proofWarn.last = map[Outcome]time.Time{}
	proofWarn.Unlock()
	r := signed(t, trustedPeer, ProofKey{id: "k1", secret: []byte(testSecretB)}, testNow)
	for range 3 {
		PolicyMiddleware(policy)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(httptest.NewRecorder(), r)
	}
	if len(lines) != 1 || lines[0] != "bad_mac other" {
		t.Fatalf("lines %q, want one \"bad_mac other\"", lines)
	}
}
