package contracts

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
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
	urlOnly := rawURL(key + "~~~?>>" + key)
	// The URL-only characters sit in the middle: no start or trim offset removes them, so only a URL decoding reads it.
	if mid := urlOnly[4 : len(urlOnly)-4]; !strings.ContainsAny(mid, "-_") {
		t.Fatal("the URL-alphabet control no longer uses the URL alphabet")
	}
	// The URL-only run is also long and random-looking, so the entropy rule fires on its text alone; only the prefix
	// finding proves the decoded text was read.
	if findings := scanFixtureContent("R2", stringDoc(t, urlOnly)); !strings.Contains(strings.Join(findings, "\n"), "forbidden credential prefix") {
		t.Errorf("R2 URL alphabet: the decoded text was not read: %v", findings)
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
		"random key split across a later header value and a later payload value": token(
			`{"alg":"HS256","typ":"JWT","k":"`+random[:half]+`"}`, `{"iss":"https://auth.example.test","v":"`+random[half:]+`"}`),
		"random secret inside prose in the payload": token(
			`{"n":1}`, `{"note":"the secret is `+random+` ok"}`),
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

func TestScannerDecodeBudget(t *testing.T) {
	std := func(text string) string { return base64.StdEncoding.EncodeToString([]byte(text)) }
	benign := stringDoc(t, std(strings.Repeat("note  ", 2000)))
	findings, budget := scanFixtureBudgeted("benign", benign, false)
	if budget.exhausted || len(findings) != 0 {
		t.Errorf("a 16 KiB benign base64 value: exhausted=%v findings=%v", budget.exhausted, findings)
	}
	huge := stringDoc(t, std(strings.Repeat("note  ", 200000)))
	findings, budget = scanFixtureBudgeted("huge", huge, false)
	if !budget.exhausted || !strings.Contains(strings.Join(findings, "\n"), "exceeds the decode budget") {
		t.Errorf("a 1.6 MiB base64 value must fail closed: exhausted=%v findings=%d", budget.exhausted, len(findings))
	}
	if budget.bytes > maxScanBytes+decodeReadingsPerRun*len(huge) {
		t.Errorf("work after the budget ran out: %d bytes", budget.bytes)
	}
}

func TestScannerCorpusStaysFarUnderTheDecodeBudget(t *testing.T) {
	root := filepath.Join(ContractsDir(testRoot(t)), "examples")
	var maxCalls, maxBytes int
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		_, budget := scanFixtureBudgeted(path, string(raw), false)
		maxCalls, maxBytes = max(maxCalls, budget.calls), max(maxBytes, budget.bytes)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("corpus maximum per file: %d scans (budget %d), %d bytes (budget %d)", maxCalls, maxScanCalls, maxBytes, maxScanBytes)
	if maxCalls*4 > maxScanCalls || maxBytes*4 > maxScanBytes {
		t.Errorf("a corpus file uses over a quarter of the decode budget: %d scans, %d bytes", maxCalls, maxBytes)
	}
}

func TestScannerReadsStringsInsideEmbeddedJSON(t *testing.T) {
	key, _, _, punctuated := plainCells()
	std := func(text string) string { return base64.StdEncoding.EncodeToString([]byte(text)) }
	embedded := func(inner string) string { return stringDoc(t, `{"alg":"EdDSA","k":`+inner+`}`) }
	for name, content := range map[string]string{
		"punctuated secret in an embedded JSON string": embedded(`"` + punctuated + `"`),
		"base64 key in an embedded JSON string":        embedded(`"` + std(key) + `"`),
	} {
		if findings := scanFixtureContent(name, content); len(findings) == 0 {
			t.Errorf("%s: no finding", name)
		}
	}
	clean := stringDoc(t, `{"alg":"EdDSA","typ":"at+jwt","kid":"auth-example-2026-10"}`)
	if findings := scanFixtureContent("clean", clean); len(findings) != 0 {
		t.Errorf("an embedded header was refused: %v", findings)
	}
}

func TestScannerEmbeddedJSONDepthBoundIsExact(t *testing.T) {
	_, _, _, punctuated := plainCells()
	text := punctuated
	for layers := 1; layers <= 5; layers++ {
		inner, err := json.Marshal(map[string]string{"k": text})
		if err != nil {
			t.Fatal(err)
		}
		text = string(inner)
		found := len(scanFixtureContent("nested", stringDoc(t, text))) > 0
		if want := layers <= maxDecodeDepth; found != want {
			t.Errorf("%d embedded JSON layers: found=%v, want %v", layers, found, want)
		}
	}
}
