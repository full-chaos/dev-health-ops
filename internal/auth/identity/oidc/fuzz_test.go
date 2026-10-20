package oidc

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

const fuzzProvider = "7f9a1c2e-0b4d-4e8f-9a6b-3c5d7e9f1a2b"

// FuzzOpenState: no input other than a sealed state for this provider opens,
// and a sealed state with any one byte changed never opens.
func FuzzOpenState(f *testing.F) {
	now := time.Unix(1_800_000_000, 0)
	token, _, err := sealState(testStateSecret, testFlowState(fuzzProvider), now)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(token, uint16(0))
	f.Add("", uint16(3))
	f.Add("AAAA", uint16(1))
	f.Add(strings.Repeat("A", 200), uint16(7))
	f.Fuzz(func(t *testing.T, candidate string, flip uint16) {
		// Seeds are re-sealed in every fuzz worker, so an opened state is
		// checked against the template it must have been sealed from.
		if state, err := openState(testStateSecret, candidate, fuzzProvider, now); err == nil {
			want := testFlowState(fuzzProvider)
			if state.ProviderID != want.ProviderID || state.CodeVerifier != want.CodeVerifier ||
				state.Nonce != want.Nonce || state.NonceHash != want.NonceHash || state.RedirectURI != want.RedirectURI {
				t.Fatalf("a state not sealed by this package opened: %+v", state)
			}
		}
		raw, _ := base64.RawURLEncoding.DecodeString(token)
		i := int(flip) % len(raw)
		tampered := append([]byte(nil), raw...)
		tampered[i] ^= byte(flip>>8) | 1
		if _, err := openState(testStateSecret, base64.RawURLEncoding.EncodeToString(tampered), fuzzProvider, now); err == nil {
			t.Fatalf("state with byte %d changed opened", i)
		}
	})
}

// FuzzParseJOSEHeader: an accepted header names an allowed asymmetric alg and
// carries no member that names a key or demands an extension.
func FuzzParseJOSEHeader(f *testing.F) {
	for _, h := range []string{`{"alg":"RS256","kid":"k1"}`, `{"alg":"none"}`, `{"alg":"HS256"}`,
		`{"alg":"RS256","alg":"HS256"}`, `{"alg":"ES512","jku":"x"}`, `{"alg":"PS256","crit":[]}`, `[]`, ``} {
		f.Add([]byte(h), "c2ln")
	}
	f.Fuzz(func(t *testing.T, header []byte, sig string) {
		token := base64.RawURLEncoding.EncodeToString(header) + ".e30." + sig
		got, err := parseJOSEHeader(token)
		if err != nil {
			return
		}
		if !algAllowed(got.Alg) || strings.HasPrefix(got.Alg, "HS") || got.Alg == "none" {
			t.Fatalf("accepted alg %q", got.Alg)
		}
		if checkJSONObject(header) != nil {
			t.Fatalf("accepted a header that is not one JSON object without duplicates: %q", header)
		}
		var m map[string]any
		if err := json.Unmarshal(header, &m); err != nil {
			t.Fatalf("accepted undecodable header: %v", err)
		}
		for _, k := range []string{"crit", "jwk", "jku", "x5u", "x5c"} {
			if _, ok := m[k]; ok {
				t.Fatalf("accepted a header with %q", k)
			}
		}
		if sig == "" {
			t.Fatal("accepted an empty signature part")
		}
	})
}

// FuzzValidateIDTokenClaims: an accepted payload has our issuer, our client in
// its audience, our nonce, a valid subject and a live expiry.
func FuzzValidateIDTokenClaims(f *testing.F) {
	rules := testRules()
	for _, s := range []string{
		`{"iss":"https://idp.example","aud":"client-1","sub":"subject-1","nonce":"nonce-1","iat":1800000000,"exp":1800000300}`,
		`{"iss":"https://idp.example","aud":["client-1","x"],"azp":"client-1","sub":"s","nonce":"nonce-1","iat":1800000000,"exp":1800000300}`,
		`{"iss":"https://idp.example","aud":"client-1","sub":"s","sub":"t","nonce":"nonce-1","iat":1800000000,"exp":1800000300}`,
		`{"iss":"https://idp.example","aud":"client-1","sub":"s","nonce":"nonce-1","iat":1e9,"exp":1.8000003e9}`,
		`{}`, `null`, `[]`,
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		got, err := checkClaims(payload, rules)
		if err != nil {
			return
		}
		var m map[string]any
		if err := json.Unmarshal(payload, &m); err != nil {
			t.Fatalf("accepted undecodable payload: %v", err)
		}
		if m["iss"] != rules.Issuer || got.Issuer != rules.Issuer {
			t.Fatalf("accepted issuer %v", m["iss"])
		}
		if m["nonce"] != rules.Nonce {
			t.Fatalf("accepted nonce %v", m["nonce"])
		}
		if !validSubject(got.Subject) || m["sub"] != got.Subject {
			t.Fatalf("accepted subject %q", got.Subject)
		}
		auds, ok := stringOrStrings(m["aud"])
		if !ok || !contains(auds, rules.ClientID) {
			t.Fatalf("accepted audience %v", m["aud"])
		}
		exp, ok := m["exp"].(float64)
		if !ok || !rules.Now.Before(time.Unix(int64(exp), 0).Add(clockSkew+time.Second)) {
			t.Fatalf("accepted exp %v", m["exp"])
		}
	})
}

// FuzzParseCallbackBody: a body without errors is one JSON object whose code
// and state are strings.
func FuzzParseCallbackBody(f *testing.F) {
	for _, s := range []string{`{"code":"c","state":"s"}`, `{"code":"c","state":"s","code":"x"}`,
		`{"code":null,"state":"s"}`, `{"code":"c","state":"s","code_verifier":1}`, `[]`, `"x"`, ``} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		body, errs := parseCallbackBody(data)
		if len(errs) > 0 {
			return
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("accepted a body that is not JSON: %q", data)
		}
		code, ok1 := m["code"].(string)
		state, ok2 := m["state"].(string)
		if !ok1 || !ok2 || code != body.Code || state != body.State {
			t.Fatalf("accepted body %q as %+v", data, body)
		}
	})
}
