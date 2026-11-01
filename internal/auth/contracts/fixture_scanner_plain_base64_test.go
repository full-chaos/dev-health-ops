package contracts

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

// Every credential-shaped input below is assembled at run time from fragments, so no contiguous literal in this
// file classifies as a secret for another scanner.
func plainCells() (key, ghp, random, punctuated string) {
	return "AKIA" + "IOSFODNN7EXAMPLE",
		"ghp" + "_abcdefghijklmnopqrstuvwxyz0123456789",
		"aZ9qK2mX7pL4vR8t" + "Yw3NbC6dF1gH5jS0",
		"svc:Xk9#mQ2$" + "vL7!pR4&wZ8^" + "nB3*kT6%~"
}

func stringDoc(t *testing.T, value string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]string{"value": value})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func wrapEvery(text string, width int, separator string) string {
	var parts []string
	for len(text) > width {
		parts = append(parts, text[:width])
		text = text[width:]
	}
	return strings.Join(append(parts, text), separator)
}

func TestScannerDecodesBase64InPlainJSONStringValues(t *testing.T) {
	key, ghp, random, punctuated := plainCells()
	std := func(text string) string { return base64.StdEncoding.EncodeToString([]byte(text)) }
	rawURL := func(text string) string { return base64.RawURLEncoding.EncodeToString([]byte(text)) }
	nest := func(text string, layers int) string {
		for i := 0; i < layers; i++ {
			text = std(text)
		}
		return text
	}
	urlOnly := rawURL(key + "~~~?>>")
	if !strings.ContainsAny(urlOnly, "-_") {
		t.Fatal("the URL-alphabet control no longer uses the URL alphabet")
	}
	hidden := map[string]string{
		"R1 standard alphabet, prefix rule":      stringDoc(t, std(key)),
		"R1b standard alphabet, ghp prefix":      stringDoc(t, std(ghp)),
		"R2 URL alphabet":                        stringDoc(t, urlOnly),
		"R3 entropy rule on the decoded text":    stringDoc(t, std(random)),
		"R4 pattern rule on the decoded text":    stringDoc(t, std("-----BEGIN "+"OPENSSH PRIVATE KEY"+"-----")),
		"R5 newline split":                       stringDoc(t, wrapEvery(std(key), 7, "\n")),
		"R6 CRLF split":                          stringDoc(t, wrapEvery(std(key), 7, "\r\n")),
		"R6b wrapped with indent":                stringDoc(t, wrapEvery(std(key), 7, "\n    ")),
		"R6c wrapped with tab indent":            stringDoc(t, wrapEvery(std(random), 7, "\r\n\t")),
		"R7 two layers":                          stringDoc(t, nest(key, 2)),
		"R7b four layers":                        stringDoc(t, nest(key, 4)),
		"R9 member name":                         `{"` + std(key) + `":1}`,
		"R10 one prefix character":               stringDoc(t, "x"+std(key)),
		"R10b two prefix characters":             stringDoc(t, "xy"+std(key)),
		"R10c three prefix characters":           stringDoc(t, "xyz"+std(key)),
		"R10d trailing text":                     stringDoc(t, std(key)+"!!"),
		"R11 escaped prefix":                     `{"value":"AKIA` + `IOSFODNN7EXAMPLE"}`,
		"R12 base64 inside prose":                stringDoc(t, "the value is "+std(key)+" and more"),
		"R13 array element":                      `["` + std(key) + `"]`,
		"R14 nested object value":                `{"a":{"b":["` + std(key) + `"]}}`,
		"Q1a punctuated secret, plain value":     stringDoc(t, punctuated),
		"Q1b punctuated secret, base64 std":      stringDoc(t, std(punctuated)),
		"Q1c punctuated secret, base64url":       stringDoc(t, rawURL(punctuated)),
		"Q1d punctuated secret, newline split":   stringDoc(t, wrapEvery(std(punctuated), 7, "\n")),
		"Q1e punctuated secret in a JSON string": stringDoc(t, std(`{"a":"`+punctuated+`"}`)),
	}
	for name, content := range hidden {
		if findings := scanFixtureContent(name, content); len(findings) == 0 {
			t.Errorf("%s: no finding for %s", name, content)
		}
	}
}

func TestScannerDepthBoundIsExact(t *testing.T) {
	key, _, _, _ := plainCells()
	layer := func(text string) string { return base64.StdEncoding.EncodeToString([]byte(text)) }
	text := key
	for layers := 1; layers <= 5; layers++ {
		text = layer(text)
		// Only the prefix rule counts: a long outer layer trips the entropy rule on its own text.
		found := strings.Contains(strings.Join(scanFixtureContent("depth", stringDoc(t, text)), "\n"), "forbidden credential prefix")
		if want := layers <= maxDecodeDepth; found != want {
			t.Errorf("%d base64 layers: found=%v, want %v (maxDecodeDepth %d)", layers, found, want, maxDecodeDepth)
		}
	}
}

func TestScannerPlainBase64GreenCells(t *testing.T) {
	_, _, random, _ := plainCells()
	std := func(text string) string { return base64.StdEncoding.EncodeToString([]byte(text)) }
	clean := map[string]string{
		"base64 of prose":                     stringDoc(t, std(strings.Repeat("note  ", 8))),
		"base64 of a synthetic id":            stringDoc(t, std("prn_AAAAAAAAAAAAAAAAAAAAAA")),
		"kebab identifier":                    stringDoc(t, "invalid-actor-chain-entry-missing-expiry"),
		"trailing newline identifier":         stringDoc(t, "auth-example-2026-10\n"),
		"prose with spaces":                   stringDoc(t, "Draft 2020-12 makes format annotation-only unless the validator opts in"),
		"two halves of a secret with a space": stringDoc(t, random[:16]+" "+random[16:]),
		"two halves of a secret with a tab":   stringDoc(t, random[:16]+"\t"+random[16:]),
		"wrapped prose":                       stringDoc(t, "first line of a note\n    second line of the same note"),
		"url":                                 stringDoc(t, "https://auth.fullchaos.dev/.well-known/jwks.json"),
	}
	for name, content := range clean {
		if findings := scanFixtureContent(name, content); len(findings) != 0 {
			t.Errorf("%s: refused: %v", name, findings)
		}
	}
}

func TestScannerJoinsHeaderAndPayloadText(t *testing.T) {
	key, _, random, _ := plainCells()
	b64 := func(text string) string { return base64.RawURLEncoding.EncodeToString([]byte(text)) }
	token := func(header, payload string) string {
		return `{"access_token":"` + b64(header) + "." + b64(payload) + `.EXAMPLESIGNATURE"}`
	}
	half := len(random) / 2
	hidden := map[string]string{
		"random key split across header and payload": token(
			`{"alg":"HS256","k":"`+random[:half]+`"}`, `{"v":"`+random[half:]+`"}`),
		"prefix split across header and payload": token(
			`{"alg":"HS256","k":"`+key[:3]+`"}`, `{"v":"`+key[3:]+`"}`),
		"punctuated secret in the payload": `{"access_token":"` +
			"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" + "." + b64(`{"a":"`+plainPunctuated()+`"}`) + `.EXAMPLESIGNATURE"}`,
		"punctuated secret as a whole decoded segment": `{"access_token":"` +
			"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" + "." + b64(plainPunctuated()) + `.EXAMPLESIGNATURE"}`,
	}
	for name, content := range hidden {
		if findings := scanFixtureContent(name, content); len(findings) == 0 {
			t.Errorf("%s: no finding", name)
		}
	}
	clean := token(`{"alg":"EdDSA","typ":"at+jwt","kid":"auth-example-2026-10"}`,
		`{"iss":"https://auth.example.test","sub":"prn_AAAAAAAAAAAAAAAAAAAAAA","sid":"ses_EXAMPLE000000000000000001"}`)
	if findings := scanFixtureContent("clean", clean); len(findings) != 0 {
		t.Errorf("a synthetic token was refused: %v", findings)
	}
}

func plainPunctuated() string {
	_, _, _, punctuated := plainCells()
	return punctuated
}
