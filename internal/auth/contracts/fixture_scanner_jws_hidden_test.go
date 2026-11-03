package contracts

import (
	"encoding/base64"
	"encoding/json"
	"math/rand"
	"strings"
	"testing"
)

// TestFixtureScannerFindsCredentialsHiddenInAJWS: the scanner fails closed. A
// token is erased as a synthetic JWS only when its header and payload are
// strict base64url JSON objects (or a payload placeholder) and its signature
// is one of the closed placeholders; anything else is scanned raw. Whatever
// the token is, a credential in a raw segment, in the signature tail, or
// hidden under base64 (any alignment, nested, inside a JSON string, escaped)
// is a finding.
func TestFixtureScannerFindsCredentialsHiddenInAJWS(t *testing.T) {
	b64 := func(text string) string { return base64.RawURLEncoding.EncodeToString([]byte(text)) }
	const header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
	const key = "AKIAIOSFODNN7EXAMPLE"
	const ghp = "ghp_abcdefghijklmnopqrstuvwxyz0123456789"
	const random = "aZ9qK2mX7pL4vR8tYw3NbC6dF1gH5jS0"
	const claims = `{"sub":"prn_AAAAAAAAAAAAAAAAAAAAAA"}`
	token := func(payload string) string { return header + "." + payload + ".EXAMPLESIGNATURE" }
	hidden := map[string]string{
		"A raw payload is a key":                     token(key),
		"B raw payload is a ghp_ token":              token(ghp),
		"C raw payload is a random secret":           token(random),
		"D payload is EXAMPLE plus a base64 key":     token("EXAMPLE" + b64(key)),
		"E JSON string is EXAMPLE plus a base64 key": token(b64(`{"k":"EXAMPLE` + b64(key) + `"}`)),
		"P JSON string is x. plus a base64 key":      token(b64(`{"k":"x.` + b64(key) + `"}`)),
		"F header is glued to a key":                 header + key + "." + b64(claims) + ".EXAMPLESIGNATURE",
		"G signature tail is a key":                  header + ".EXAMPLE.EXAMPLE" + key,
		"H signature tail is a random secret":        header + ".EXAMPLE.EXAMPLE" + random,
		"payload begins EXAMPLE and ends in a key":   token("EXAMPLEA" + b64(key)),
		"JSON string holds a base64 key":             token(b64(`{"k":"` + b64(key) + `"}`)),
		"JSON string holds an escaped key":           token(b64(`{"k":"\u0041KIA` + `ABCDEFGHIJKLMNOP"}`)),
		"base64 inside base64":                       token(b64(`{"k":"` + b64(b64(key)) + `"}`)),
		"JSON member name is the key":                token(b64(`{"` + key + `":1}`)),
		"JSON array holds the key":                   token(b64(`["` + key + `"]`)),
		"header carries the key":                     b64(`{"alg":"HS256","typ":"JWT","k":"`+key+`"}`) + "." + b64(claims) + ".EXAMPLESIGNATURE",
		"signature is not a placeholder":             header + "." + b64(claims) + ".EXAMPLESIGNATUREZ9q",
		"signature is real-looking":                  header + "." + b64(claims) + ".Zm9vYmFyYmF6cXV4MTIzNDU2Nzg5MGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6QUJDREVGR0g",
	}
	for name, content := range hidden {
		if findings := scanFixtureContent(name, `{"access_token":"`+content+`"}`); len(findings) == 0 {
			t.Errorf("%s: no finding", name)
		}
	}
	clean := map[string]string{
		"a synthetic claims payload": token(b64(`{"iss":"https://auth.example.test","sub":"prn_AAAAAAAAAAAAAAAAAAAAAA","sid":"ses_EXAMPLE000000000000000001"}`)),
		"an EXAMPLE payload":         token("EXAMPLE"),
		"an empty payload":           header + ".." + "EXAMPLESIGNATURE",
	}
	for name, content := range clean {
		if findings := scanFixtureContent(name, `{"access_token":"`+content+`"}`); len(findings) != 0 {
			t.Errorf("%s: refused: %v", name, findings)
		}
	}
}

// TestStrictDecodeWaiverIsClosedAndContentBound: no fixture stores a compact JWS (the refresh-response.v2 corpus keeps
// its tokens in split form and assembles them at read time), so the waiver list is empty and a stored token whose
// segment is not strict base64url is a finding. A waiver, were one listed, would still never admit a key.
func TestStrictDecodeWaiverIsClosedAndContentBound(t *testing.T) {
	if len(strictDecodeWaivers) != 0 {
		t.Fatalf("the waiver list holds %d fixtures; no fixture stores a compact JWS, so none may be waived", len(strictDecodeWaivers))
	}
	// base64url({"a":1}) is eyJhIjoxfQ; eyJhIjoxfR is its non-zero trailing-bit spelling.
	nonStrict := `{"access_token":"eyJhIjoxfR.eyJhIjoxfQ.EXAMPLESIGNATURE"}`
	path := "refresh-response.v2/client-rejected-header-trailing-bits.json"
	if hasStrictDecodeWaiver(path, []byte(nonStrict)) {
		t.Error("an unlisted fixture has a waiver")
	}
	if findings := scanFixtureContent(path, nonStrict); len(findings) == 0 {
		t.Error("a stored non-strict token was admitted")
	}
	hidden := `{"access_token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.AKIAIOSFODNN7EXAMPLE.EXAMPLESIGNATURE"}`
	if findings := scanFixtureContentWaiving(path, hidden, true); len(findings) == 0 {
		t.Error("the waiver admitted a key in a raw payload")
	}
}

// TestScannerDoesNotEraseATokenWhoseSegmentsAreNotPrintableJSON: a random secret
// can sit verbatim, 4-aligned, inside a payload that still strict-decodes to a
// JSON object, because its decoded bytes are JSON-safe noise. Such a token is
// not erased (its decoded header and payload must be printable ASCII), so its
// raw text gets every rule and the secret is found.
func TestScannerDoesNotEraseATokenWhoseSegmentsAreNotPrintableJSON(t *testing.T) {
	const header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
	probe := `{"access_token":"` + header + `.eyJhIjoi9LD6QOzb2r6d3z1YZ6w67DPDTVRWh1DmIn0.EXAMPLESIGNATURE"}`
	if findings := scanFixtureContent("probe", probe); len(findings) == 0 {
		t.Error("the reviewer's probe (a random secret inside a JSON-object payload) was erased")
	}

	const base62 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	random := rand.New(rand.NewSource(20261023))
	embedded := func(secret string) string {
		return `{"access_token":"` + header + "." + base64.RawURLEncoding.EncodeToString([]byte(`{"a":"`)) + secret +
			base64.RawURLEncoding.EncodeToString([]byte(`"}`)) + `.EXAMPLESIGNATURE"}`
	}
	cases := 0
	for attempts := 0; cases < 200 && attempts < 200000; attempts++ {
		secret := make([]byte, 32)
		for i := range secret {
			secret[i] = base62[random.Intn(len(base62))]
		}
		text := string(secret)
		if len(scanPlain("alone", text)) == 0 {
			continue // not a secret the entropy rule sees on its own
		}
		if _, ok := jsonObjectOnly(embedded(text)); !ok {
			continue // its decoded bytes are not JSON-safe: not the case under test
		}
		cases++
		if findings := scanFixtureContent("generated", embedded(text)); len(findings) == 0 {
			t.Fatalf("case %d: an embedded random secret was erased: %s", cases, text)
		}
	}
	if cases < 200 {
		t.Fatalf("only %d generated cases met the preconditions", cases)
	}
}

// jsonObjectOnly reports whether the payload segment of the access_token in
// content strict-decodes to a JSON object (the precondition of the attack).
func jsonObjectOnly(content string) (string, bool) {
	segments := strings.Split(strings.TrimSuffix(strings.SplitN(content, `":"`, 2)[1], `"}`), ".")
	raw, err := base64.RawURLEncoding.Strict().DecodeString(segments[1])
	if err != nil {
		return "", false
	}
	var object map[string]any
	if json.Unmarshal(raw, &object) != nil {
		return "", false
	}
	return string(raw), true
}
