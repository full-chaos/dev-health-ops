package contractsv1

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
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

func fixtureDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(ContractsDir(testRoot(t)), "examples", "principal")
}

func loadManifest(t *testing.T) fixtureManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir(t), "manifest.json"))
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
		if entry.IsDir() || entry.Name() == "manifest.json" || !strings.HasSuffix(entry.Name(), ".json") {
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
			if principal.Revisions == (Revisions{}) {
				t.Error("revisions decoded to all zeroes -- check the JSON tags")
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
