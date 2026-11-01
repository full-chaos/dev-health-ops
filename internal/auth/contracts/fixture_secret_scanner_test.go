package contracts

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

// G-73: "Fixtures use clearly synthetic, structurally safe values that cannot
// be mistaken for live tokens and are scanned for forbidden prefixes/entropy
// patterns where practical." G-16 names "test snapshots or golden fixtures"
// among the places a credential must never appear.
//
// It is very practical here, and the stakes are concrete rather than
// theoretical: dev-health-ops is a PUBLIC repository, so anything that lands
// in contracts/auth/v1/examples/ is world-readable the moment it is pushed.
// The principal surface now carries a `credential` object next to a class
// vocabulary that includes fcacr_* and fcpush_*, which is one careless
// copy-paste away from a fixture holding something that pattern-matches a
// live credential.
//
// This scans the whole examples/ tree, not just the principal directory, so
// every later surface inherits it without anyone remembering to opt in.

// forbiddenFixturePrefixes are credential prefixes observed in this platform's
// own vocabulary plus the common third-party shapes. A fixture containing one
// of these is either a real secret or a value close enough to one that a
// scanner elsewhere will flag it.
var forbiddenFixturePrefixes = []string{
	"fcacr_", "fcpush_", "svc_acr_", "svc_worker_",
	"sk_live_", "sk_test_", "whsec_", "ghp_", "ghs_", "github_pat_",
	"xoxb-", "AKIA", "AIza",
}

// forbiddenFixturePatterns catch shapes that carry no fixed prefix.
var forbiddenFixturePatterns = map[string]*regexp.Regexp{
	"PEM private key block": regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`),
	"PEM certificate block": regexp.MustCompile(`-----BEGIN CERTIFICATE-----`),
	"compact JWS (three base64url segments)": regexp.MustCompile(
		`\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\b`),
}

// entropyScanThreshold is the length at or above which a token-like run of
// characters is entropy-checked. Short identifiers are exempt because their
// entropy estimate is meaningless, not because they are safe.
const entropyScanThreshold = 24

// maxFixtureEntropyBits is the Shannon entropy ceiling, in bits per character,
// for a long token-like string in a fixture.
//
// MEASURED, not guessed. The first draft used 3.5 and the scanner immediately
// rejected this repository's own corpus: ordinary English filenames, JSON
// Pointers, module paths and URLs sit at 3.6-4.1 bits/char, above a 3.5
// ceiling. Measured on both populations:
//
//	prn_EXAMPLE0000000000000001              2.51   synthetic identifier
//	valid-human-minimal.json                 3.63   fixture filename
//	/actor_chain/0/permitted_actions/0       3.84   JSON Pointer
//	github.com/google/jsonschema-go          3.93   module path
//	infrastructure_deployment_credentials    3.90   credential class id
//	invalid-...-missing-expiry.json          4.09   longest real filename
//	----------------------------------------------  ceiling 4.30
//	<stripe live-key prefix + 24 mixed>      4.62   branded secret
//	<40-char AWS secret-key shape>           4.66   cloud provider secret
//	<32-char random base62>                  5.00   unbranded random secret
//
// 4.30 sits in the gap with margin on both sides. Entropy ALONE is still not
// enough, which is why hasAllCharacterClasses exists below.
//
// The three sampled secret shapes are DESCRIBED rather than written out, and
// the controls below assemble their inputs at run time instead of holding
// literals. That is not squeamishness: the first push of this file was
// REJECTED by GitHub push protection, which classified the Stripe sample in
// this very table as a live Stripe API key. A scanner's own examples are
// secrets to somebody else's scanner, and a guard that cannot be pushed is
// not a guard. Do not "tidy" the concatenations below back into literals.
const maxFixtureEntropyBits = 4.30

var tokenLikeRun = regexp.MustCompile(`[A-Za-z0-9_\-+/=.]{` +
	fmt.Sprint(entropyScanThreshold) + `,}`)

// shannonEntropyBitsPerChar estimates per-character entropy. Deliberately the
// simplest defensible measure: the point is to separate "EXAMPLE0000000000001"
// from a 32-byte random secret, not to grade cryptographic quality.
func shannonEntropyBitsPerChar(value string) float64 {
	if value == "" {
		return 0
	}
	counts := map[rune]int{}
	for _, r := range value {
		counts[r]++
	}
	total := float64(len([]rune(value)))
	var bits float64
	for _, count := range counts {
		p := float64(count) / total
		bits -= p * math.Log2(p)
	}
	return bits
}

// hasAllCharacterClasses reports whether a run mixes upper case, lower case
// and digits -- the shape of a generated secret.
//
// This is the SECOND half of the entropy test and it carries most of the
// weight. Prose, filenames, JSON Pointers, module paths and URLs are
// overwhelmingly lower case, so requiring all three classes excludes the
// entire population that produced this scanner's first-run false refusals,
// while every random-secret shape measured above satisfies it. Requiring both
// signals rather than either means a value must look random AND look
// generated before it is reported.
//
// Branded low-entropy secrets do not rely on this path at all: AKIA... and a
// compact JWS header both fall below the entropy ceiling and are caught by
// forbiddenFixturePrefixes and forbiddenFixturePatterns instead. The three
// checks are layered on purpose -- prefixes catch what is named, patterns
// catch what is structured, entropy catches what is merely random.
func hasAllCharacterClasses(value string) bool {
	var upper, lower, digit bool
	for _, r := range value {
		switch {
		case r >= 'A' && r <= 'Z':
			upper = true
		case r >= 'a' && r <= 'z':
			lower = true
		case r >= '0' && r <= '9':
			digit = true
		}
	}
	return upper && lower && digit
}

// jwsShape matches anything shaped like a compact JWS: a header segment that
// begins with the JWS marker eyJ, a payload segment (possibly empty) and a
// signature segment.
var jwsShape = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+`)

// syntheticSignatures is the closed list of signature placeholders a fixture
// may use; no real signature is any of them. A signature that is anything else
// (EXAMPLE followed by more text included) makes the token an ordinary
// string, scanned raw.
var syntheticSignatures = map[string]bool{
	"EXAMPLE": true, "EXAMPLEOTHER": true, "EXAMPLESIGNATURE": true,
	"EXAMPLESIGNATURE0": true, "EXAMPLESIGNATUREAB": true,
}

// syntheticPayloadPlaceholders are the payloads of the HS256-shaped fixtures,
// which carry no JSON: nothing is hidden in a placeholder.
var syntheticPayloadPlaceholders = map[string]bool{"": true, "EXAMPLE": true, "EXAMPLEOTHER": true}

// strictDecodeWaivers is the closed, reviewed list of fixtures whose token may
// have a header or payload that is not strict base64url JSON, keyed by exact
// path and the sha256 of the file content: they are client-reject vectors that
// the other language verifiers read from this corpus, and the not-strict
// segment is the very thing they pin. The waiver covers only the strict-decode
// requirement: the signature must still be a closed placeholder, the raw
// segments still get the prefix and pattern rules, every lenient reading of a
// segment is still decoded and scanned, and the fixture must still produce no
// finding. A different path, or a file with one byte changed, has no waiver.
var strictDecodeWaivers = map[string]string{}

func hasStrictDecodeWaiver(path string, content []byte) bool {
	sum := sha256.Sum256(content)
	want, listed := strictDecodeWaivers[path]
	return listed && want == hex.EncodeToString(sum[:])
}

// maxDecodeDepth bounds the base64 inside a decoded segment that the scanner
// follows (a payload that carries a base64 string that carries a base64 string).
const maxDecodeDepth = 4

var base64Decoders = []*base64.Encoding{base64.RawURLEncoding, base64.URLEncoding, base64.RawStdEncoding, base64.StdEncoding}

// base64Run is a candidate base64 or base64url run inside text.
var base64Run = regexp.MustCompile(`[A-Za-z0-9_+/=-]{8,}`)

// strictJSONObject decodes a JWS segment as strict base64url and returns the
// text when it is exactly one JSON object made only of printable ASCII.
func strictJSONObject(segment string) (string, bool) {
	raw, err := base64.RawURLEncoding.Strict().DecodeString(segment)
	if err != nil {
		return "", false
	}
	for _, b := range raw {
		if b < 0x20 || b > 0x7e {
			return "", false // noise, not text: a secret can hide in JSON-safe noise
		}
	}
	var object map[string]any
	if json.Unmarshal(raw, &object) != nil {
		return "", false
	}
	return string(raw), true
}

// decodeCandidates returns every base64 reading of run: all four alphabets,
// every start offset of 0 to 3 characters (a prefix such as EXAMPLE shifts the
// alignment) and every trailing trim of 0 to 3 characters.
func decodeCandidates(run string) []string {
	var out []string
	for start := 0; start < 4 && start < len(run); start++ {
		for trim := 0; trim < 4 && start+trim < len(run); trim++ {
			piece := run[start : len(run)-trim]
			for _, encoding := range base64Decoders {
				if raw, err := encoding.DecodeString(piece); err == nil && len(raw) >= 6 {
					out = append(out, string(raw))
				}
			}
		}
	}
	return out
}

// maxScanCalls and maxScanBytes bound the decoding work for one fixture file. Running out FAILS the fixture:
// a scan that stops quietly is a scan that passes while checking nothing.
const (
	maxScanCalls = 200000
	maxScanBytes = 64 << 20
	// decodeReadingsPerRun is the most decodes decodeCandidates makes for one run (4 starts x 4 trims x 4 alphabets).
	decodeReadingsPerRun = 64
)

type scanBudget struct {
	calls, bytes int
	exhausted    bool
}

func (b *scanBudget) charge(bytes int) bool {
	if b.exhausted {
		return false
	}
	b.calls++
	b.bytes += bytes
	if b.calls > maxScanCalls || b.bytes > maxScanBytes {
		b.exhausted = true
		return false
	}
	return true
}

// decodeCharged is decodeCandidates under the budget; it returns nothing once the budget is spent.
func decodeCharged(budget *scanBudget, run string) []string {
	if !budget.charge(decodeReadingsPerRun * len(run)) {
		return nil
	}
	return decodeCandidates(run)
}

// lineBreakJoin is a line break plus the indent after it: how a wrapped base64 block is laid out inside one string.
// A bare space or tab is not removed: that would join the words of prose.
var lineBreakJoin = regexp.MustCompile(`[\r\n]+[ \t]*`)

// scanWholeValue applies the entropy rule to one whole value with punctuation allowed. tokenLikeRun stops at the first
// punctuation mark, so a printable secret such as a service password is never one run. A value qualifies only when it
// is printable ASCII with no whitespace, which keeps prose and decoded binary out.
func scanWholeValue(name, value string) []string {
	if len(value) < entropyScanThreshold || embeddedJSON(value) {
		return nil
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x21 || value[i] > 0x7e {
			return nil
		}
	}
	bits := shannonEntropyBitsPerChar(value)
	if bits <= maxFixtureEntropyBits || !hasAllCharacterClasses(value) {
		return nil
	}
	return []string{fmt.Sprintf(
		"%s contains a high-entropy %d-character value (%.2f bits/char, ceiling %.2f, mixed case and digits, "+
			"punctuation allowed): %.16s... Fixture values must be unmistakably synthetic",
		name, len(value), bits, maxFixtureEntropyBits, value)}
}

// embeddedJSON reports a string value that is itself a JSON object or array (a token header stored as text); its own
// string values are read in turn, so the whole text is not judged as one secret.
func embeddedJSON(value string) bool {
	return len(value) > 1 && (value[0] == '{' || value[0] == '[') && json.Valid([]byte(value))
}

// scanFollowing applies the plain rules to a string value as it is and joined across line breaks, then decodes every
// base64 run of the joined text (under every alignment) and scans what that gives, to maxDecodeDepth.
func scanFollowing(budget *scanBudget, name, value string, depth int) []string {
	findings := append(scanPlain(name, value), scanWholeValue(name, value)...)
	joined := lineBreakJoin.ReplaceAllString(value, "")
	if joined != value {
		findings = append(findings, scanPlain(name+" (lines joined)", joined)...)
		findings = append(findings, scanWholeValue(name+" (lines joined)", joined)...)
	}
	if depth >= maxDecodeDepth {
		return findings
	}
	if embeddedJSON(value) {
		var document any
		if json.Unmarshal([]byte(value), &document) == nil {
			for _, inner := range jsonStrings(document, nil) {
				findings = append(findings, scanFollowing(budget, name+" (embedded JSON)", inner, depth+1)...)
			}
		}
	}
	for _, run := range base64Run.FindAllString(joined, -1) {
		for _, decoded := range decodeCharged(budget, run) {
			findings = append(findings, scanDecoded(budget, name+" (base64)", decoded, depth+1)...)
		}
	}
	return findings
}

// scanDecoded scans text decoded from a JWS segment or a JSON string with the plain rules, then follows what hides
// inside it: every string value and member name of a JSON document (unescaped, so \u0041KIA is AKIA) and every
// base64-looking run in the text and in those strings, under every alignment, to maxDecodeDepth. Decoded text that
// is not JSON is also read field by field for a punctuated secret.
func scanDecoded(budget *scanBudget, name, text string, depth int) []string {
	if !budget.charge(len(text)) {
		return nil
	}
	findings := scanPlain(name, text)
	sources := []string{text}
	var document any
	if json.Unmarshal([]byte(text), &document) == nil {
		for _, value := range jsonStrings(document, nil) {
			findings = append(findings, scanPlain(name+" (JSON string)", value)...)
			findings = append(findings, scanWholeValue(name+" (JSON string)", value)...)
			sources = append(sources, value)
			if depth < maxDecodeDepth && embeddedJSON(value) {
				findings = append(findings, scanDecoded(budget, name+" (embedded JSON)", value, depth+1)...)
			}
		}
	} else {
		for _, field := range strings.Fields(text) {
			findings = append(findings, scanWholeValue(name+" (field)", field)...)
		}
	}
	if depth >= maxDecodeDepth {
		return findings
	}
	for _, source := range sources {
		joined := lineBreakJoin.ReplaceAllString(source, "")
		if joined != source {
			findings = append(findings, scanPlain(name+" (lines joined)", joined)...)
			findings = append(findings, scanWholeValue(name+" (lines joined)", joined)...)
		}
		for _, run := range base64Run.FindAllString(joined, -1) {
			for _, decoded := range decodeCharged(budget, run) {
				findings = append(findings, scanDecoded(budget, name+" (base64)", decoded, depth+1)...)
			}
		}
	}
	return findings
}

// scanJoinedSegments reads every string value of the decoded header followed by every string value of the decoded
// payload (a key with half in each segment), under the budget. Pairs, not the concatenation of all values: merging
// unrelated values of one segment would invent runs that no secret has.
func scanJoinedSegments(budget *scanBudget, name, headerText, payloadText string) []string {
	var findings []string
	tails := orderedStringValues(payloadText)
	for _, head := range orderedStringValues(headerText) {
		for _, tail := range tails {
			if !budget.charge(len(head) + len(tail)) {
				return findings
			}
			joined := head + tail
			findings = append(findings, scanPlain(name+" (token header|payload joined)", joined)...)
			findings = append(findings, scanWholeValue(name+" (token header|payload joined)", joined)...)
		}
	}
	return findings
}

// orderedStringValues is every string value (not member name) of a JSON document in
// document order. Text that is not JSON gives nothing.
func orderedStringValues(text string) []string {
	type frame struct{ object, key bool }
	decoder := json.NewDecoder(strings.NewReader(text))
	var out []string
	var stack []frame
	valueDone := func() {
		if n := len(stack); n > 0 && stack[n-1].object {
			stack[n-1].key = true
		}
	}
	for {
		token, err := decoder.Token()
		if err != nil {
			return out
		}
		switch value := token.(type) {
		case json.Delim:
			switch value {
			case '{':
				stack = append(stack, frame{object: true, key: true})
			case '[':
				stack = append(stack, frame{})
			default:
				stack = stack[:len(stack)-1]
				valueDone()
			}
		case string:
			if n := len(stack); n > 0 && stack[n-1].object && stack[n-1].key {
				stack[n-1].key = false
				continue
			}
			out = append(out, value)
			valueDone()
		default:
			valueDone()
		}
	}
}

func jsonStrings(node any, out []string) []string {
	switch value := node.(type) {
	case string:
		out = append(out, value)
	case []any:
		for _, item := range value {
			out = jsonStrings(item, out)
		}
	case map[string]any:
		for key, item := range value {
			out = append(out, key)
			out = jsonStrings(item, out)
		}
	}
	return out
}

// scanFixtureContent fails closed. A token is erased as a synthetic JWS only
// when its header and payload strict-decode to JSON objects (or the payload is
// a fixed placeholder) AND its signature is one of syntheticSignatures; before
// it is erased, every raw segment and the signature get the prefix and pattern
// rules, and the decoded header and payload get every rule plus the base64 and
// JSON-string following of scanDecoded. Any other token-shaped text is scanned
// raw, with every rule, and its segments are also searched for hidden base64.
func scanFixtureContent(name, content string) []string {
	return scanFixtureContentWaiving(name, content, false)
}

// scanFixtureContentWaiving is scanFixtureContent with the strict-decode
// requirement waived for a listed fixture (hasStrictDecodeWaiver).
func scanFixtureContentWaiving(name, content string, waiveStrictDecode bool) []string {
	findings, _ := scanFixtureBudgeted(name, content, waiveStrictDecode)
	return findings
}

// scanFixtureBudgeted is scanFixtureContentWaiving that also returns the decode budget it spent.
func scanFixtureBudgeted(name, content string, waiveStrictDecode bool) ([]string, *scanBudget) {
	var findings []string
	budget := &scanBudget{}
	erased := jwsShape.ReplaceAllStringFunc(content, func(token string) string {
		segments := strings.Split(token, ".")
		header, payload, signature := segments[0], segments[1], segments[2]
		headerText, headerOK := strictJSONObject(header)
		payloadText, payloadOK := strictJSONObject(payload)
		payloadOK = payloadOK || syntheticPayloadPlaceholders[payload]
		strict := headerOK && payloadOK
		if !syntheticSignatures[signature] || (!strict && !waiveStrictDecode) {
			for _, segment := range segments {
				for _, decoded := range decodeCharged(budget, segment) {
					findings = append(findings, scanDecoded(budget, name+" (token segment)", decoded, 1)...)
				}
			}
			return token
		}
		for _, segment := range segments {
			findings = append(findings, scanPrefixesAndPatterns(name+" (token segment)", segment)...)
			if !strict {
				for _, decoded := range decodeCharged(budget, segment) {
					findings = append(findings, scanDecoded(budget, name+" (token segment)", decoded, 1)...)
				}
			}
		}
		if headerText != "" {
			findings = append(findings, scanDecoded(budget, name+" (token header)", headerText, 0)...)
		}
		if payloadText != "" {
			findings = append(findings, scanDecoded(budget, name+" (token payload)", payloadText, 0)...)
		}
		// A key split across the two decoded segments reads as one value only when a header value and a payload
		// value are joined.
		// Only a token that is erased as strict synthetic JWS reaches this join: a waived (non-strict) token gets no header|payload join.
		findings = append(findings, scanJoinedSegments(budget, name, headerText, payloadText)...)
		return "SYNTHETIC-JWS"
	})
	findings = append(findings, scanPlain(name, erased)...)
	findings = append(findings, scanStringValues(budget, name, erased)...)
	if budget.exhausted {
		findings = append(findings, fmt.Sprintf(
			"%s exceeds the decode budget (%d scans, %d bytes): a fixture string this large cannot be read in bounded "+
				"time and is refused rather than skipped", name, maxScanCalls, maxScanBytes))
	}
	sort.Strings(findings)
	return slices.Compact(findings), budget
}

// scanStringValues reads every string value and member name of a JSON fixture as the plain rules, unescaped, joined
// across line breaks and with its base64 decoded. A fixture that is not JSON is read as one string.
func scanStringValues(budget *scanBudget, name, content string) []string {
	values := []string{content}
	var document any
	if json.Unmarshal([]byte(content), &document) == nil {
		values = jsonStrings(document, nil)
	}
	var findings []string
	for _, value := range values {
		findings = append(findings, scanFollowing(budget, name+" (JSON string)", value, 0)...)
	}
	return findings
}

// scanPlain applies the prefix, pattern and entropy rules to text as it is.
func scanPlain(name, content string) []string {
	return append(scanPrefixesAndPatterns(name, content), scanEntropy(name, content)...)
}

// scanPrefixesAndPatterns applies the named-prefix and structured-pattern rules.
func scanPrefixesAndPatterns(name, content string) []string {
	var findings []string
	for _, prefix := range forbiddenFixturePrefixes {
		if strings.Contains(content, prefix) {
			findings = append(findings, fmt.Sprintf(
				"%s contains the forbidden credential prefix %q (G-73/G-16: no real-looking "+
					"secrets in fixtures, and this repository is public)", name, prefix))
		}
	}
	for label, pattern := range forbiddenFixturePatterns {
		if match := pattern.FindString(content); match != "" {
			findings = append(findings, fmt.Sprintf(
				"%s contains a %s (%.24s...)", name, label, match))
		}
	}
	return findings
}

// scanEntropy applies the high-entropy rule.
func scanEntropy(name, content string) []string {
	var findings []string
	for _, run := range tokenLikeRun.FindAllString(content, -1) {
		bits := shannonEntropyBitsPerChar(run)
		if bits <= maxFixtureEntropyBits || !hasAllCharacterClasses(run) {
			continue
		}
		findings = append(findings, fmt.Sprintf(
			"%s contains a high-entropy %d-character run (%.2f bits/char, ceiling %.2f, mixed "+
				"case and digits): %.16s... Fixture values must be unmistakably synthetic -- "+
				"repeated EXAMPLE bodies and zero-padded counters, not plausible randomness",
			name, len(run), bits, maxFixtureEntropyBits, run))
	}
	return findings
}

func TestNoFixtureCarriesARealLookingSecret(t *testing.T) {
	root := filepath.Join(ContractsDir(testRoot(t)), "examples")
	scanned := 0
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		scanned++
		relative, _ := filepath.Rel(root, path)
		for _, finding := range scanFixtureContentWaiving(relative, string(raw), hasStrictDecodeWaiver(relative, raw)) {
			t.Error(finding)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	if scanned == 0 {
		// Without this the scanner passes vacuously the moment the walk
		// breaks or the directory moves -- the exact failure mode a scanner
		// is least likely to notice about itself.
		t.Fatalf("scanned zero fixture files under %s; the walk has broken and this test "+
			"would pass while checking nothing", root)
	}
	t.Logf("scanned %d fixture files", scanned)
}

func TestTheFixtureScannerDetectsWhatItExistsToCatch(t *testing.T) {
	// Positive controls, one per detection class. A scanner with a broken
	// pattern passes every clean corpus, which is indistinguishable from a
	// scanner that works.
	// Every control input is ASSEMBLED here rather than written as a literal,
	// so this file contains no contiguous string that another scanner can
	// classify as a credential -- see the note on the measurement table above,
	// which this file learned the hard way. The assembly happens before
	// scanFixtureContent sees it, so the code path under test is identical to
	// the one a real fixture would take; only the source text differs.
	seg := func(parts ...string) string { return strings.Join(parts, "") }
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{"platform credential prefix",
			`{"credential_id": "` + seg("fcacr", "_", "abc") + `"}`, "forbidden credential prefix"},
		{"external push prefix",
			`{"credential_id": "` + seg("fcpush", "_", "abc") + `"}`, "forbidden credential prefix"},
		{"stripe live secret",
			`{"k": "` + seg("sk", "_live_", "51H8xQ2eZvKYlo2CkVdX9pQrS") + `"}`, "forbidden credential prefix"},
		{"webhook secret prefix",
			`{"k": "` + seg("whsec", "_", "abc") + `"}`, "forbidden credential prefix"},
		{"github token prefix",
			`{"k": "` + seg("ghp", "_", "abc") + `"}`, "forbidden credential prefix"},
		{"aws access key id",
			`{"k": "` + seg("AKIA", "IOSFODNN7EXAMPLE") + `"}`, "forbidden credential prefix"},
		{"PEM private key",
			`{"k": "` + seg("-----BEGIN ", "OPENSSH PRIVATE KEY", "-----") + `"}`, "PEM private key block"},
		{"compact JWS",
			`{"t": "` + seg("eyJhbGciOiJIUzI1NiJ9", ".", "eyJzdWIiOiIxIn0", ".", "dBjftJeZ4CVPmB92K27uhbUJU1p1r") + `"}`, "compact JWS"},
		{"compact JWS with the pinned HS256 header and a real-looking payload",
			`{"t": "` + seg("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", ".", "eyJzdWIiOiIxIn0", ".", "dBjftJeZ4CVPmB92K27uhbUJU1p1r") + `"}`, "compact JWS"},
		{"unbranded high-entropy run",
			`{"k": "` + seg("aZ9qK2mX7pL4v", "R8tN1wB6yH3cF5dG0sJ") + `"}`, "high-entropy"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			findings := scanFixtureContent("control.json", testCase.content)
			if len(findings) == 0 {
				t.Fatalf("scanner reported nothing for content it must reject: %s", testCase.content)
			}
			// Log EVERY finding, not only the one asserted on. A control that
			// reports selectively is a control you cannot read: this test
			// passes when the wanted finding is present, so if the guard ALSO
			// fires for an unintended reason the extra finding is invisible
			// and the control silently stops describing what it proves.
			// (Returned by lane-auth-wave1, who hit the same shape on #2143:
			// their six-way control logged only the regressions matching the
			// expectation and so hid whether two of the six were caught at
			// all.)
			for _, finding := range findings {
				t.Log("fired:", finding)
			}
			if !strings.Contains(strings.Join(findings, "\n"), testCase.want) {
				t.Fatalf("scanner fired but not for the expected reason; want %q, got:\n%s",
					testCase.want, strings.Join(findings, "\n"))
			}
		})
	}
}

func TestTheFixtureScannerDoesNotFireOnSyntheticValues(t *testing.T) {
	// Negative control. Without it, a scanner that flagged everything would
	// pass every positive control above and reject the whole corpus -- and
	// the false-refusal direction is the one a rejection-only suite cannot
	// see, which is the lesson this repo already paid for on the Docker guard.
	//
	// The first draft of this control held only synthetic identifiers, and
	// the scanner still rejected the real corpus on its first run -- every
	// false refusal was a FILENAME, a JSON Pointer or a module path in
	// manifest.json, none of which the control contained. A negative control
	// narrower than the real input cannot catch an over-strict guard, which
	// is the same trap as a suite written by the code's author inheriting the
	// author's assumptions. Every shape below is one the corpus actually
	// contains.
	clean := `{
      "principal_id": "prn_EXAMPLE0000000000000001",
      "subject_id": "usr_EXAMPLE0000000000000001",
      "organization_id": "org_EXAMPLE0000000000000001",
      "credential_id": "ses_EXAMPLE0000000000000001",
      "delegation_id": "dlg_EXAMPLE0000000000000001",
      "issuer": "https://auth.fullchaos.dev/",
      "audience": "dev-health-api",
      "issued_at": "2026-09-02T23:15:00Z",
      "file": "invalid-actor-chain-entry-missing-expiry.json",
      "pointer": "/actor_chain/0/permitted_actions/0",
      "module": "github.com/google/jsonschema-go",
      "schema_version": "contract-fixture-manifest.v1",
      "class": "infrastructure_deployment_credentials",
      "prose": "Draft 2020-12 makes format annotation-only unless the validator opts in"
    }`
	if findings := scanFixtureContent("clean.json", clean); len(findings) > 0 {
		t.Errorf("scanner fired on unmistakably synthetic values:\n%s", strings.Join(findings, "\n"))
	}
}

func TestTheFixtureScannerAllowsOnlyTheSyntheticHS256Token(t *testing.T) {
	seg := func(parts ...string) string { return strings.Join(parts, "") }
	synthetic := `{"t": "` + seg("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", ".", "EXAMPLE", ".", "EXAMPLESIGNATURE") + `"}`
	if findings := scanFixtureContent("synthetic.json", synthetic); len(findings) != 0 {
		t.Fatalf("a synthetic EXAMPLE token was flagged: %v", findings)
	}
	halfSynthetic := `{"t": "` + seg("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", ".", "EXAMPLE", ".", "dBjftJeZ4CVPmB92K27uhbUJU1p1r") + `"}`
	if findings := scanFixtureContent("half.json", halfSynthetic); len(findings) == 0 {
		t.Fatal("a token with a real-looking signature segment passed the scanner")
	}
}

// TestFixtureScannerReadsInsideASyntheticJWS: the EXAMPLE-signature allowance
// hides nothing, because the header and payload segments are decoded and
// scanned as text; a secret-shaped value in a payload is still a finding, and
// a token whose signature does not begin EXAMPLE is not admitted at all.
func TestFixtureScannerReadsInsideASyntheticJWS(t *testing.T) {
	segment := func(text string) string { return base64.RawURLEncoding.EncodeToString([]byte(text)) }
	header := segment(`{"alg":"EdDSA","typ":"at+jwt","kid":"auth-example-2026-10"}`)
	clean := header + "." + segment(`{"sub":"prn_AAAAAAAAAAAAAAAAAAAAAA"}`) + ".EXAMPLESIGNATURE"
	if findings := scanFixtureContent("clean", clean); len(findings) != 0 {
		t.Fatalf("a synthetic token was refused: %v", findings)
	}
	hidden := header + "." + segment(`{"key":"AKIA`+`ABCDEFGHIJKLMNOP"}`) + ".EXAMPLESIGNATURE"
	if findings := scanFixtureContent("hidden", hidden); len(findings) == 0 {
		t.Error("a secret-shaped value inside a synthetic token's payload was not reported")
	}
	real := header + "." + segment(`{"sub":"prn_AAAAAAAAAAAAAAAAAAAAAA"}`) + ".Zm9vYmFyYmF6cXV4MTIzNDU2Nzg5MGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6QUJDREVGR0g"
	if findings := scanFixtureContent("real", real); len(findings) == 0 {
		t.Error("a token with a non-EXAMPLE signature was admitted")
	}
}
