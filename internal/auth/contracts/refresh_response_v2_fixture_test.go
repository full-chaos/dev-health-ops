package contracts

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// The refresh-response.v2 corpus stores no compact JWS string, as the signed access-token golden corpus does not:
// each token member is an object {header_json | header_segment, payload_json | payload_segment, signature
// [, trailing_bits]}. The wire document is the stored bytes with each such object replaced by the JSON string
//
//	base64url(header_json) "." base64url(payload_json) "." signature
//
// where a *_segment member is used as the segment itself (for a segment that is not base64url of any text), and
// trailing_bits names the one segment whose last character gets its lowest bit set (the non-zero trailing-bit
// spelling of the same bytes). Everything outside the token objects is kept byte for byte, so a repeated member
// stays repeated. ci/auth-contracts-ajv/run.mjs assembles the same way.

var v2TokenMember = regexp.MustCompile(`"(access_token|session_access_token)"\s*:\s*\{`)

const base64URLAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

type v2FixtureToken struct {
	HeaderJSON     *string `json:"header_json"`
	HeaderSegment  *string `json:"header_segment"`
	PayloadJSON    *string `json:"payload_json"`
	PayloadSegment *string `json:"payload_segment"`
	Signature      *string `json:"signature"`
	TrailingBits   string  `json:"trailing_bits"`
}

func v2Segment(text, segment *string, name string) (string, error) {
	switch {
	case text != nil && segment == nil:
		return base64.RawURLEncoding.EncodeToString([]byte(*text)), nil
	case segment != nil && text == nil:
		return *segment, nil
	}
	return "", fmt.Errorf("a token object needs exactly one of %s_json and %s_segment", name, name)
}

func assembleV2Token(object []byte) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(object))
	decoder.DisallowUnknownFields()
	var token v2FixtureToken
	if err := decoder.Decode(&token); err != nil {
		return "", err
	}
	header, err := v2Segment(token.HeaderJSON, token.HeaderSegment, "header")
	if err != nil {
		return "", err
	}
	payload, err := v2Segment(token.PayloadJSON, token.PayloadSegment, "payload")
	if err != nil {
		return "", err
	}
	if token.Signature == nil {
		return "", fmt.Errorf("a token object needs a signature")
	}
	parts := []string{header, payload, *token.Signature}
	if token.TrailingBits != "" {
		index := map[string]int{"header": 0, "payload": 1, "signature": 2}
		at, ok := index[token.TrailingBits]
		part := parts[at]
		if !ok || part == "" {
			return "", fmt.Errorf("trailing_bits %q names no non-empty segment", token.TrailingBits)
		}
		position := strings.IndexByte(base64URLAlphabet, part[len(part)-1])
		if position < 0 || position&1 == 1 {
			return "", fmt.Errorf("trailing_bits %q: the segment does not end in a zero-bit character", token.TrailingBits)
		}
		parts[at] = part[:len(part)-1] + string(base64URLAlphabet[position|1])
	}
	return strings.Join(parts, "."), nil
}

// assembleV2Fixture returns the wire bytes of a stored refresh-response.v2 fixture.
func assembleV2Fixture(raw []byte) ([]byte, error) {
	var out bytes.Buffer
	rest := raw
	for {
		loc := v2TokenMember.FindIndex(rest)
		if loc == nil {
			out.Write(rest)
			return out.Bytes(), nil
		}
		open := loc[1] - 1
		decoder := json.NewDecoder(bytes.NewReader(rest[open:]))
		var object json.RawMessage
		if err := decoder.Decode(&object); err != nil {
			return nil, err
		}
		compact, err := assembleV2Token(object)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(compact)
		if err != nil {
			return nil, err
		}
		out.Write(rest[:open])
		out.Write(bytes.ReplaceAll(encoded, []byte(`&`), []byte("&")))
		rest = rest[open+int(decoder.InputOffset()):]
	}
}

func mustAssembleV2Fixture(t testing.TB, raw []byte) []byte {
	t.Helper()
	wire, err := assembleV2Fixture(raw)
	if err != nil {
		t.Fatalf("assemble a refresh-response.v2 fixture: %v", err)
	}
	return wire
}

// compactJWS matches a stored compact JWS of JSON segments, the shape secret scanners flag.
var compactJWS = regexp.MustCompile(`eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]*\.`)

// No refresh-response.v2 fixture stores a compact JWS: the tokens are assembled at read time.
func TestRefreshResponseV2FixturesStoreNoCompactJWS(t *testing.T) {
	for _, name := range append(append(append([]string(nil), refreshResponseV2Inventory["accept"]...),
		refreshResponseV2Inventory["reject"]...), refreshResponseV2Inventory["reject_by_client"]...) {
		if compactJWS.Match(v2StoredFixture(t, name)) {
			t.Errorf("%s stores a compact JWS", name)
		}
	}
}

// The assembly is exact: a known stored token yields its compact form, and a malformed token object is refused, not
// silently assembled into something else.
func TestRefreshResponseV2FixtureAssembly(t *testing.T) {
	stored := []byte(`{"access_token": {"header_json": "{\"a\":1}", "payload_json": "", "signature": "EXAMPLE", "trailing_bits": "header"}, "x": 1}`)
	wire, err := assembleV2Fixture(stored)
	if err != nil {
		t.Fatal(err)
	}
	// base64url({"a":1}) is eyJhIjoxfQ; its last character Q has a zero low bit, and the trailing-bit spelling is R.
	if want := `{"access_token": "eyJhIjoxfR..EXAMPLE", "x": 1}`; string(wire) != want {
		t.Fatalf("assembled %s, want %s", wire, want)
	}
	for _, bad := range []string{
		`{"access_token": {"payload_json": "", "signature": "EXAMPLE"}}`,
		`{"access_token": {"header_json": "", "header_segment": "", "payload_json": "", "signature": "EXAMPLE"}}`,
		`{"access_token": {"header_json": "", "payload_json": ""}}`,
		`{"access_token": {"header_json": "", "payload_json": "", "signature": "EXAMPLE", "kid": "k"}}`,
		`{"access_token": {"header_json": "x", "payload_json": "", "signature": "EXAMPLE", "trailing_bits": "claims"}}`,
	} {
		if _, err := assembleV2Fixture([]byte(bad)); err == nil {
			t.Errorf("assembled %s", bad)
		}
	}
}
