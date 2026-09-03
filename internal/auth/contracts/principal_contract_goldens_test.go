package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// fixtureManifest mirrors contracts/auth/v1/examples/principal/manifest.json.
//
// The manifest is the SINGLE inventory of the corpus, read here, by the
// Python runner (tests/authclient) and by the TypeScript runner in
// dev-health-web. No runner may enumerate fixtures itself: three independent
// lists over one directory is the drift the cross-language goldens exist to
// catch, and a list that silently loses an entry looks exactly like a
// passing suite.
type fixtureManifest struct {
	RequiresFormatAssertion bool `json:"requires_format_assertion"`
	Accept                  []struct {
		File string `json:"file"`
	} `json:"accept"`
	Reject []struct {
		File                   string `json:"file"`
		ExpectInstanceLocation string `json:"expect_instance_location"`
		ExpectKeyword          string `json:"expect_keyword"`
		Why                    string `json:"why"`
	} `json:"reject"`
	// RejectByClient is the third category: documents the schema MUST accept
	// and every language client MUST refuse, for rules JSON Schema cannot
	// express at all. A duration bound is the clear case -- JSON Schema cannot
	// subtract two timestamps, so requiring an expires_at FIELD is not a
	// duration BOUND. Without this category the rule would have nowhere to
	// live but a comment.
	RejectByClient []struct {
		File string `json:"file"`
		Why  string `json:"why"`
	} `json:"reject_by_client"`
}

// goMessageForKeyword maps a JSON Schema keyword to the token
// github.com/google/jsonschema-go puts in its error message for it.
//
// This table exists because the library returns a flat error string with no
// structured keyword or instance location -- unlike the Python and
// TypeScript validators, which both report a keyword and a JSON Pointer into
// the INSTANCE. Two consequences the tests below handle explicitly rather
// than paper over:
//
//   - Most messages contain the keyword verbatim ("pattern:", "enum:"), but
//     additionalProperties does NOT -- it is phrased "unexpected additional
//     properties [...]". A naive contains-the-keyword check silently passes
//     on that one for the wrong reason.
//   - The library stops at the FIRST violation. That is only unambiguous
//     because TestEveryRejectedFixtureViolatesExactlyOneInstanceLocation
//     (Python side asserts the same) pins each reject fixture to a single
//     locus; without that, Go could legitimately report a different rule
//     than the manifest declares and the mismatch would look like a defect.
//
// The map is asserted TOTAL over the manifest in
// TestEveryManifestKeywordHasAGoMessageToken: an unrecognised keyword FAILS
// rather than being skipped. A skip here would mean a reject fixture whose
// rule is checked in Python and TypeScript but silently unchecked in Go,
// while all three suites stay green.
var goMessageForKeyword = map[string]string{
	"additionalProperties": "unexpected additional properties",
	"const":                "const:",
	"enum":                 "enum:",
	"minLength":            "minLength:",
	"minimum":              "minimum:",
	"oneOf":                "oneOf:",
	"pattern":              "pattern:",
	"required":             "required:",
	"uniqueItems":          "uniqueItems:",
}

func testRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root, err := RepoRoot(wd)
	if err != nil {
		t.Fatalf("locating repo root: %v", err)
	}
	return root
}

// principalFixtureDir and principalFixtureManifest are written as WHOLE
// repo-relative paths, not as a basename joined at runtime.
//
// tests/tooling/test_go_workflow_path_filters.py resolves the file literals in
// Go test sources to prove every input a Go test reads is covered by go.yml's
// path filters -- otherwise a PR touching only that input is classified
// non-Go, the workflow never runs, and go-quality passes vacuously. A bare
// basename is unresolvable to that oracle: it matches several files in the
// tree and the oracle refuses to guess between them (guessing is how a
// coverage oracle starts asserting coverage it does not have). Spelling the
// path in full makes the reference unambiguous and the coverage real rather
// than assumed. Do not collapse these back to a basename.
//
// The prose here deliberately avoids naming a file inside double quotes: that
// oracle scans the raw source with a string-literal regex, so it sees a quoted
// filename in a COMMENT exactly as it sees one in code. An explanatory comment
// about the rule was itself enough to trip the rule.
// manifestBasename is derived from the full path above rather than written
// again, so the two cannot drift apart.
var manifestBasename = filepath.Base(principalFixtureManifest)

const (
	principalFixtureDir      = "contracts/auth/v1/examples/principal"
	principalFixtureManifest = "contracts/auth/v1/examples/principal/manifest.json"
)

func fixtureDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(testRoot(t), principalFixtureDir)
}

func loadManifest(t *testing.T) fixtureManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), principalFixtureManifest))
	if err != nil {
		t.Fatalf("reading fixture manifest: %v", err)
	}
	var manifest fixtureManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parsing fixture manifest: %v", err)
	}
	return manifest
}

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir(t), name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return raw
}

func TestTheCorpusIsNotEmptyInEitherDirection(t *testing.T) {
	// A table-driven test over an empty slice reports zero failures and the
	// package still prints "ok" -- indistinguishable from a suite that ran.
	manifest := loadManifest(t)
	if len(manifest.Accept) == 0 {
		t.Error("manifest declares no accept fixtures")
	}
	if len(manifest.Reject) == 0 {
		t.Error("manifest declares no reject fixtures")
	}
	if len(manifest.RejectByClient) == 0 {
		t.Error("manifest declares no client-enforced fixtures")
	}
}

func TestEveryFixtureFileOnDiskIsClaimedByTheManifest(t *testing.T) {
	// A fixture nobody runs is worse than a missing one: it sits in the
	// directory reading as coverage while no runner in any language opens it.
	manifest := loadManifest(t)
	entries, err := os.ReadDir(fixtureDir(t))
	if err != nil {
		t.Fatalf("reading fixture dir: %v", err)
	}
	onDisk := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == manifestBasename || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		onDisk[entry.Name()] = true
	}
	claimed := map[string]bool{}
	for _, a := range manifest.Accept {
		claimed[a.File] = true
	}
	for _, r := range manifest.Reject {
		claimed[r.File] = true
	}
	for _, r := range manifest.RejectByClient {
		claimed[r.File] = true
	}
	var unclaimed, missing []string
	for name := range onDisk {
		if !claimed[name] {
			unclaimed = append(unclaimed, name)
		}
	}
	for name := range claimed {
		if !onDisk[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(unclaimed)
	sort.Strings(missing)
	if len(unclaimed) > 0 {
		t.Errorf("fixture files on disk that no manifest entry claims: %v", unclaimed)
	}
	if len(missing) > 0 {
		t.Errorf("manifest entries with no file on disk: %v", missing)
	}
}

func TestEveryManifestKeywordHasAGoMessageToken(t *testing.T) {
	// Totality check. An unrecognised keyword must FAIL here, not fall
	// through the reject test as an unchecked case -- the whole point of the
	// map is that Go's flat error string cannot be interrogated structurally,
	// so anything not in the map is a rule Go is not actually enforcing.
	manifest := loadManifest(t)
	for _, r := range manifest.Reject {
		if r.ExpectKeyword == "format" {
			t.Errorf(
				"%s expects keyword %q, which google/jsonschema-go IGNORES during "+
					"validation (its doc.go, \"Deviations from the specification\"). A "+
					"reject fixture must assert a keyword all three validators enforce -- "+
					"use \"pattern\" and keep \"format\" as the stricter check the other "+
					"two apply on top.",
				r.File, r.ExpectKeyword,
			)
			continue
		}
		if _, ok := goMessageForKeyword[r.ExpectKeyword]; !ok {
			t.Errorf(
				"%s expects keyword %q, which has no entry in goMessageForKeyword. Add "+
					"one (with the token this library actually emits, verified against a "+
					"real run) rather than letting the rule go unchecked in Go while "+
					"Python and TypeScript still check it.",
				r.File, r.ExpectKeyword,
			)
		}
	}
}

func TestAcceptedFixturesValidate(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadManifest(t).Accept {
		t.Run(entry.File, func(t *testing.T) {
			var document any
			if err := json.Unmarshal(loadFixture(t, entry.File), &document); err != nil {
				t.Fatalf("fixture is not valid JSON: %v", err)
			}
			if err := Validate(root, PrincipalSurface, document); err != nil {
				t.Fatalf("should validate but did not: %v", err)
			}
		})
	}
}

func TestAcceptedFixturesRoundTripThroughTheClient(t *testing.T) {
	// Validation alone does not prove the Go type can read the document: a
	// struct field with the wrong JSON tag validates fine and decodes to a
	// zero value, silently.
	root := testRoot(t)
	for _, entry := range loadManifest(t).Accept {
		t.Run(entry.File, func(t *testing.T) {
			principal, err := PrincipalFromWire(root, loadFixture(t, entry.File))
			if err != nil {
				t.Fatalf("PrincipalFromWire: %v", err)
			}
			if principal.SchemaVersion != PrincipalSchemaVersion {
				t.Errorf("schema_version = %q, want %q", principal.SchemaVersion, PrincipalSchemaVersion)
			}
			if principal.PrincipalID == "" {
				t.Error("principal_id decoded empty -- check the JSON tag")
			}
			if principal.PrincipalType == "" {
				t.Error("principal_type decoded empty -- check the JSON tag")
			}
			if principal.IssuedAt.IsZero() {
				t.Error("issued_at decoded to the zero time -- check the JSON tag")
			}
			if principal.ExpiresAt.IsZero() {
				t.Error("expires_at decoded to the zero time -- check the JSON tag")
			}
			// The nested objects are where a wrong JSON tag hides: the
			// document validates, the struct decodes, and the field is
			// silently a zero value nobody asserted on.
			if principal.Credential.Class == "" {
				t.Error("credential.class decoded empty -- check the JSON tag")
			}
			if principal.Credential.CredentialID == "" {
				t.Error("credential.credential_id decoded empty -- check the JSON tag")
			}
			if principal.Credential.Audience == "" {
				t.Error("credential.audience decoded empty -- check the JSON tag")
			}
			if principal.Authentication.Assurance == "" {
				t.Error("authentication.assurance decoded empty -- check the JSON tag")
			}
			if principal.Authentication.AuthenticatedAt.IsZero() {
				t.Error("authentication.authenticated_at decoded to the zero time")
			}
			if len(principal.Authentication.Methods) == 0 {
				t.Error("authentication.methods decoded empty -- the schema requires minItems 1")
			}
			// A delegated fixture must decode its chain: an actor_chain that
			// silently decodes to nil would make every delegation assertion
			// below vacuous while the document still validated.
			var rawChain struct {
				ActorChain []map[string]any `json:"actor_chain"`
			}
			if err := json.Unmarshal(loadFixture(t, entry.File), &rawChain); err != nil {
				t.Fatalf("re-reading actor_chain: %v", err)
			}
			if len(rawChain.ActorChain) != len(principal.ActorChain) {
				t.Fatalf("actor_chain decoded %d hops, document has %d -- check the JSON tags",
					len(principal.ActorChain), len(rawChain.ActorChain))
			}
			for i, hop := range principal.ActorChain {
				if hop.ActorPrincipalID == "" || hop.DelegationID == "" || hop.ExpiresAt.IsZero() {
					t.Errorf("actor_chain[%d] decoded with empty fields: %+v", i, hop)
				}
			}
		})
	}
}

func TestRejectedFixturesAreRejectedByTheDeclaredRule(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadManifest(t).Reject {
		t.Run(entry.File, func(t *testing.T) {
			var document any
			if err := json.Unmarshal(loadFixture(t, entry.File), &document); err != nil {
				t.Fatalf("fixture is not valid JSON: %v", err)
			}
			err := Validate(root, PrincipalSurface, document)
			if err == nil {
				t.Fatalf("validated cleanly; expected rejection by %q at %q (%s)",
					entry.ExpectKeyword, entry.ExpectInstanceLocation, entry.Why)
			}
			token, ok := goMessageForKeyword[entry.ExpectKeyword]
			if !ok {
				// Totality is asserted separately; fail loudly here too rather
				// than treating an unmapped keyword as "rejected, good enough".
				t.Fatalf("no Go message token registered for keyword %q", entry.ExpectKeyword)
			}
			if !strings.Contains(err.Error(), token) {
				t.Fatalf("rejected, but not by %q. token %q absent from: %v",
					entry.ExpectKeyword, token, err)
			}
		})
	}
}

func TestEntitlementCannotBeSmuggledIntoAPrincipal(t *testing.T) {
	// Duplicates one manifest row deliberately. The corpus row proves the
	// fixture is rejected; this test names the ADR, so deleting the fixture
	// cannot quietly delete the rule with it -- and the failure message tells
	// the next reader WHY the fields are forbidden, rather than leaving them
	// to read the absence as an oversight and "fix" it.
	root := testRoot(t)
	var document map[string]any
	if err := json.Unmarshal(loadFixture(t, "valid-human-minimal.json"), &document); err != nil {
		t.Fatalf("fixture is not valid JSON: %v", err)
	}
	// Control first: without the entitlement claims the document validates,
	// so a rejection below is caused by what this test adds and not by a
	// fixture that was already broken.
	if err := Validate(root, PrincipalSurface, document); err != nil {
		t.Fatalf("control failed: the base fixture should validate, got %v", err)
	}
	document["tier"] = "enterprise"
	document["licensed_features"] = []any{"agent_context_runtime"}
	if err := Validate(root, PrincipalSurface, document); err == nil {
		t.Fatal(
			"principal.v1 accepted entitlement claims (tier/licensed_features). " +
				"ACP-ADR-07 decision 2 makes entitlement an input to a decision and never " +
				"a claim in a credential; G-14 forbids it by name.",
		)
	}
}

// TestClientEnforcedFixturesValidateButAreRefused covers the third manifest
// category.
//
// BOTH halves are asserted, and the first is the one that matters. If such a
// fixture stopped validating, the rule would have quietly become a schema rule
// and a test asserting only the refusal would still pass -- hiding that the
// client check had become dead code.
//
// NON-VACUITY CONTROL, named here because it lives in another test and an
// unlabelled control is one refactor away from being deleted as redundant:
// the "must validate" assertion below is evidence rather than a tautology
// ONLY because Validate is proven able to reject. That proof is
// TestRejectedFixturesAreRejectedByTheDeclaredRule, which asserts Validate
// refuses all 22 `reject` fixtures, plus
// TestEntitlementCannotBeSmuggledIntoAPrincipal. The sibling category IS the
// control for this one. (Identified by lane-auth-wave1 reviewing this file;
// the same structure as their own posture suite, where the refusal
// assertions had NO capability control beside them and so proved nothing --
// the presence or absence of the sibling is the entire difference.)
func TestClientEnforcedFixturesValidateButAreRefused(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadManifest(t).RejectByClient {
		t.Run(entry.File, func(t *testing.T) {
			raw := loadFixture(t, entry.File)
			var document any
			if err := json.Unmarshal(raw, &document); err != nil {
				t.Fatalf("fixture is not valid JSON: %v", err)
			}
			if err := Validate(root, PrincipalSurface, document); err != nil {
				t.Fatalf("must VALIDATE against the schema but did not (%v). If the rule has "+
					"moved into the schema, this fixture belongs in `reject`, not "+
					"`reject_by_client`.", err)
			}
			if _, err := PrincipalFromWire(root, raw); err == nil {
				t.Fatalf("the client ACCEPTED it; %s", entry.Why)
			}
		})
	}
}

// TestTheDelegationBoundIsTheADRValueNotALiteral pins the constant to
// ACP-ADR-03's number, so a silent widening is visible in a diff.
func TestTheDelegationBoundIsTheADRValueNotALiteral(t *testing.T) {
	if MaxDelegationDuration != 15*time.Minute {
		t.Errorf("MaxDelegationDuration = %s, want 15m0s (ACP-ADR-03)", MaxDelegationDuration)
	}
}

// TestRevisionAcceptsAnIntegralDecimalAndRejectsAFractionalOne pins the
// cross-language agreement codex round 1 found missing.
//
// Draft 2020-12 defines "type": "integer" as any number with a zero fractional
// part, so 1.0 IS a valid integer and both languages must land on the same
// value. Before the fix Python produced a float and Go refused to decode.
func TestRevisionAcceptsAnIntegralDecimalAndRejectsAFractionalOne(t *testing.T) {
	for _, testCase := range []struct {
		raw     string
		want    Revision
		wantErr bool
	}{
		{raw: "1", want: 1},
		{raw: "1.0", want: 1},
		{raw: "0", want: 0},
		{raw: "42.000", want: 42},
		{raw: "1.5", wantErr: true},
		{raw: "-1.5", wantErr: true},
	} {
		t.Run(testCase.raw, func(t *testing.T) {
			var got Revision
			err := json.Unmarshal([]byte(testCase.raw), &got)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("%s decoded to %d; a fractional revision must be refused, never "+
						"truncated -- two wire documents collapsing to one revision breaks the "+
						"G-31 cache key", testCase.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", testCase.raw, err)
			}
			if got != testCase.want {
				t.Errorf("%s decoded to %d, want %d", testCase.raw, got, testCase.want)
			}
		})
	}
}

// TestPrincipalDecodesAnIntegralDecimalRevision tests the WIRING, not the type.
//
// TestRevisionAcceptsAnIntegralDecimalAndRejectsAFractionalOne unmarshals into
// a bare Revision and so proves only that the TYPE behaves. It says nothing
// about whether Principal's fields actually USE that type -- and they very
// nearly did not: the edit that introduced Revision changed the type
// declaration but silently failed to change the four struct fields, because
// gofmt had realigned them and an exact-match replacement no-opped. The build
// stayed green, the type test stayed green, and only decoding a whole document
// showed Go still rejecting 1.0.
//
// So this decodes a real fixture with a decimal revision through
// PrincipalFromWire, which is the path production takes. Reverting any of the
// four fields to int64 turns it red.
func TestPrincipalDecodesAnIntegralDecimalRevision(t *testing.T) {
	root := testRoot(t)
	for _, field := range []string{
		"membership_revision", "policy_revision", "grant_revision", "entitlement_revision",
	} {
		t.Run(field, func(t *testing.T) {
			var document map[string]any
			if err := json.Unmarshal(loadFixture(t, "valid-human-minimal.json"), &document); err != nil {
				t.Fatalf("fixture is not valid JSON: %v", err)
			}
			document[field] = json.RawMessage("7.0")
			raw, err := json.Marshal(document)
			if err != nil {
				t.Fatalf("re-marshalling: %v", err)
			}
			principal, err := PrincipalFromWire(root, raw)
			if err != nil {
				t.Fatalf("PrincipalFromWire refused an integral decimal, which draft 2020-12 "+
					"defines as a valid integer and the Python client accepts: %v", err)
			}
			got := map[string]Revision{
				"membership_revision":  principal.MembershipRevision,
				"policy_revision":      principal.PolicyRevision,
				"grant_revision":       principal.GrantRevision,
				"entitlement_revision": principal.EntitlementRevision,
			}[field]
			if got != 7 {
				t.Errorf("%s = %d, want 7", field, got)
			}
		})
	}
}

// TestRevisionRangeAgreesWithPython pins the int64 boundary in both
// directions, because the first version of this guard got it wrong in a way
// no fixture could see.
//
// The schema bounds revisions below at zero and not above -- correct for JSON
// Schema, wrong for a cross-language contract, since Python's integers are
// arbitrary-precision and Go's are not. Worse, the range check itself was
// lossy: float64(math.MaxInt64) rounds UP to 2^63, so 2^63 passed the guard
// and was SILENTLY CLAMPED to 2^63-1, making two different wire documents
// produce one revision. A revision is a G-31 cache-key input; two inputs
// collapsing to one value is the failure that key exists to prevent.
func TestRevisionRangeAgreesWithPython(t *testing.T) {
	for _, testCase := range []struct {
		raw     string
		want    Revision
		wantErr bool
	}{
		{raw: "9223372036854775807", want: 9223372036854775807}, // MaxInt64, accepted
		{raw: "9223372036854775808", wantErr: true},             // 2^63, silently clamped before the fix
		{raw: "1e19", wantErr: true},                            // integral but far past the range
		// 1e18 is inside int64 and OUTSIDE float64's exact-integer range, and
		// it arrives in decimal form, so it is refused by the 2^53 guard. That
		// expectation changed when the guard landed: this row previously
		// asserted acceptance, and updating it is the point rather than an
		// inconvenience -- an in-range value the parser has already rounded is
		// exactly what the guard exists to stop. The same number sent as an
		// integer token is accepted and exact; see the row below.
		{raw: "1e18", wantErr: true},
		{raw: "1000000000000000000", want: 1000000000000000000}, // integer token, exact
		{raw: "9007199254740991", want: 9007199254740991},       // 2^53-1, integer token
		{raw: "9007199254740991.0", want: 9007199254740991},     // decimal, just under the bound
		{raw: "9007199254740992.0", wantErr: true},              // decimal AT 2^53: cannot be
		//                                                          distinguished from 2^53+1
		{raw: "9007199254740993.0", wantErr: true}, // decimal that ROUNDS to 2^53
		{raw: "-1", want: -1},                      // the SCHEMA forbids this; the type need not
	} {
		t.Run(testCase.raw, func(t *testing.T) {
			var got Revision
			err := json.Unmarshal([]byte(testCase.raw), &got)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("%s decoded to %d; out-of-range must be REFUSED, never clamped -- "+
						"clamping makes two wire values one revision", testCase.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", testCase.raw, err)
			}
			if got != testCase.want {
				t.Errorf("%s decoded to %d, want %d", testCase.raw, got, testCase.want)
			}
		})
	}
}
