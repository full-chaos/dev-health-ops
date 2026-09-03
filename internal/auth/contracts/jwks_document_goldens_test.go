package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/authverify"
)

// JWKSSurface names the jwks.v1 wire contract.
//
// A test-file constant rather than an exported one in the package proper:
// nothing in this repository PARSES a JWKS. The document is produced by
// Python and consumed by dev-health-go, and inventing a third reader here
// purely to give the constant a home would create the second implementation
// this whole programme exists to remove.
const JWKSSurface = "jwks.v1"

// jwksFixtureManifest mirrors contracts/auth/v1/examples/jwks/manifest.json.
//
// Its reject_by_client entries carry no http_status -- unlike the error
// corpus, a JWKS never arrives on a response line; it is a mounted file. The
// struct is separate rather than shared for that reason: a borrowed field that
// is always zero reads as "not set" to the next person, when the truth is "not
// applicable".
type jwksFixtureManifest struct {
	Accept []struct {
		File string `json:"file"`
		Why  string `json:"why"`
	} `json:"accept"`
	Reject []struct {
		File                   string `json:"file"`
		ExpectInstanceLocation string `json:"expect_instance_location"`
		ExpectKeyword          string `json:"expect_keyword"`
		Why                    string `json:"why"`
	} `json:"reject"`
	RejectByClient []struct {
		File string `json:"file"`
		Why  string `json:"why"`
	} `json:"reject_by_client"`
}

// goMessageForJWKSKeyword extends goMessageForKeyword with the one keyword
// this corpus asserts and the others do not.
//
// Read off the validator by a temporary probe, not guessed -- the same
// discipline the error corpus's table records. `minItems` reports "minItems:
// array length 0 is less than 1", so the token is the keyword plus its colon;
// that it matches the obvious guess is luck, and the probe is what turns luck
// into knowledge.
var goMessageForJWKSKeyword = map[string]string{
	"minItems": "minItems:",
}

func jwksTokenFor(keyword string) (string, bool) {
	if token, ok := goMessageForKeyword[keyword]; ok {
		return token, true
	}
	token, ok := goMessageForJWKSKeyword[keyword]
	return token, ok
}

const (
	jwksFixtureDir          = "contracts/auth/v1/examples/jwks"
	jwksFixtureManifestPath = "contracts/auth/v1/examples/jwks/manifest.json"
)

var jwksManifestBasename = filepath.Base(jwksFixtureManifestPath)

func jwksDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(testRoot(t), jwksFixtureDir)
}

func loadJWKSManifest(t *testing.T) jwksFixtureManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), jwksFixtureManifestPath))
	if err != nil {
		t.Fatalf("reading jwks manifest: %v", err)
	}
	var manifest jwksFixtureManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parsing jwks manifest: %v", err)
	}
	return manifest
}

func loadJWKSFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(jwksDir(t), name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return raw
}

func decodeJWKSFixture(t *testing.T, name string) any {
	t.Helper()
	var document any
	if err := json.Unmarshal(loadJWKSFixture(t, name), &document); err != nil {
		t.Fatalf("fixture %s is not valid JSON: %v", name, err)
	}
	return document
}

// consumerVerdict runs the REAL production consumer over a fixture.
//
// authverify.Ed25519JWKSVerifier reads a PATH, and the fixtures are already
// files on disk in exactly the on-the-wire form, so it is pointed straight at
// them. No copy, no temp file, no re-serialisation: a re-encode would silently
// normalise whatever the fixture was written to demonstrate, which for a
// BYTES-level contract is the one thing that must not happen.
func consumerVerdict(t *testing.T, name string) error {
	t.Helper()
	_, err := authverify.NewEd25519JWKSVerifier(filepath.Join(jwksDir(t), name)).Keys()
	return err
}

func TestTheJWKSManifestIsNotEmpty(t *testing.T) {
	// Every table-driven test below draws its cases from the manifest, so an
	// empty list runs zero cases and reports green while checking nothing.
	manifest := loadJWKSManifest(t)
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

func TestEveryJWKSFixtureOnDiskIsClaimedByTheManifest(t *testing.T) {
	// A fixture nobody reads is worse than no fixture: it sits in the
	// directory reading as a test that runs, and nothing fails when it stops
	// being true.
	manifest := loadJWKSManifest(t)
	entries, err := os.ReadDir(jwksDir(t))
	if err != nil {
		t.Fatalf("reading fixture dir: %v", err)
	}
	onDisk := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == jwksManifestBasename || !strings.HasSuffix(entry.Name(), ".json") {
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
	if len(unclaimed) > 0 || len(missing) > 0 {
		t.Fatalf("unclaimed on disk: %v; claimed but absent: %v", unclaimed, missing)
	}
}

func TestEveryJWKSManifestKeywordHasAGoMessageToken(t *testing.T) {
	// Totality. An unmapped keyword must FAIL rather than be skipped: a skip
	// means a reject fixture whose rule is checked in Python and silently
	// unchecked in Go, while both suites stay green.
	for _, entry := range loadJWKSManifest(t).Reject {
		if _, ok := jwksTokenFor(entry.ExpectKeyword); !ok {
			t.Errorf("no Go message token registered for keyword %q (fixture %s)",
				entry.ExpectKeyword, entry.File)
		}
	}
}

func TestAcceptedJWKSFixturesValidate(t *testing.T) {
	// The positive controls. A schema that is too strict passes every
	// rejection test while refusing every real document, and that failure is
	// invisible to a suite asserting only rejections.
	root := testRoot(t)
	for _, entry := range loadJWKSManifest(t).Accept {
		t.Run(entry.File, func(t *testing.T) {
			if err := Validate(root, JWKSSurface, decodeJWKSFixture(t, entry.File)); err != nil {
				t.Fatalf("accept fixture was rejected by the schema: %v", err)
			}
		})
	}
}

func TestAcceptedJWKSFixturesAreAcceptedByTheRealConsumer(t *testing.T) {
	// The half a schema-only suite cannot give. "Valid" is worth nothing if
	// the one program that actually reads this document refuses it, and the
	// consumer lives in another repository where nothing in this suite would
	// notice it tightening. Pointing the real Ed25519JWKSVerifier at the real
	// fixtures is what makes the contract a statement about production rather
	// than about the schema file.
	for _, entry := range loadJWKSManifest(t).Accept {
		t.Run(entry.File, func(t *testing.T) {
			if err := consumerVerdict(t, entry.File); err != nil {
				t.Fatalf("the schema accepts this document and the real consumer refuses it: %v", err)
			}
		})
	}
}

func TestRejectedJWKSFixturesFailByTheDeclaredKeyword(t *testing.T) {
	// Each rejection must fail for the declared REASON, not merely somewhere.
	// Asserting only "this does not validate" lets a fixture drift into
	// failing for an unrelated reason -- a typo in a neighbouring field --
	// while still passing, at which point it no longer tests what it was
	// written for.
	//
	// Go reports the FIRST violation only, and locates it in the SCHEMA rather
	// than the instance, so the instance location the manifest carries is
	// asserted by the Python runner and the keyword is asserted here. Neither
	// runner checks both; between them every field of every entry is checked.
	root := testRoot(t)
	for _, entry := range loadJWKSManifest(t).Reject {
		t.Run(entry.File, func(t *testing.T) {
			err := Validate(root, JWKSSurface, decodeJWKSFixture(t, entry.File))
			if err == nil {
				t.Fatalf("validated cleanly; expected rejection by %q at %q (%s)",
					entry.ExpectKeyword, entry.ExpectInstanceLocation, entry.Why)
			}
			token, ok := jwksTokenFor(entry.ExpectKeyword)
			if !ok {
				t.Fatalf("no Go message token registered for keyword %q", entry.ExpectKeyword)
			}
			if !strings.Contains(err.Error(), token) {
				t.Fatalf("rejected, but not by %q. token %q absent from: %v",
					entry.ExpectKeyword, token, err)
			}
		})
	}
}

func TestRejectedJWKSFixturesAreAlsoRefusedByTheRealConsumer(t *testing.T) {
	// The direction nobody thinks to test: is the schema STRICTER than the
	// consumer anywhere? A schema that refuses documents the consumer would
	// happily read is not a safety margin, it is a false alarm that will be
	// "fixed" by loosening the rule that was doing the work.
	//
	// The schema IS deliberately narrower than the consumer in three places,
	// documented in the schema's own description -- `use: ""`, a short
	// multi-byte `kid`, and nothing else. None of them appears in this corpus,
	// so every reject fixture here must be refused by both. If a future
	// fixture exercises one of those narrowings, this test is where it will
	// surface, and the fix is to move it to a category of its own rather than
	// to silence this.
	for _, entry := range loadJWKSManifest(t).Reject {
		t.Run(entry.File, func(t *testing.T) {
			if err := consumerVerdict(t, entry.File); err == nil {
				t.Fatalf("the schema refuses this document and the real consumer ACCEPTS it: "+
					"the schema is stricter than the code it describes (%s)", entry.Why)
			}
		})
	}
}

func TestClientEnforcedJWKSFixturesValidateButTheConsumerRefusesThem(t *testing.T) {
	// The category's whole point, in one assertion pair. The first half proves
	// the SCHEMA does not catch these -- which is what makes the consumer's
	// check load-bearing rather than belt-and-braces -- and the second proves
	// the consumer does. If the schema ever grew the ability to express one of
	// these, the first assertion fails, and that is the correct signal: the
	// rule should move out of this category and into the schema.
	root := testRoot(t)
	for _, entry := range loadJWKSManifest(t).RejectByClient {
		t.Run(entry.File, func(t *testing.T) {
			if err := Validate(root, JWKSSurface, decodeJWKSFixture(t, entry.File)); err != nil {
				t.Fatalf("this fixture is meant to be VALID against the schema, "+
					"so that the consumer's check is what refuses it; schema said: %v", err)
			}
			if err := consumerVerdict(t, entry.File); err == nil {
				t.Fatalf("the consumer accepted a document only it can refuse (%s)", entry.Why)
			}
		})
	}
}
