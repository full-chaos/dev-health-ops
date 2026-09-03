package contracts

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
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

func scanFixtureContent(name, content string) []string {
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
	sort.Strings(findings)
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
		for _, finding := range scanFixtureContent(relative, string(raw)) {
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
