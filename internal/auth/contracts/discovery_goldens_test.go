package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// discoveryManifest mirrors contracts/auth/v1/examples/discovery/manifest.json.
type discoveryManifest struct {
	Accept []struct {
		File string `json:"file"`
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

const discoveryFixtureDir = "contracts/auth/v1/examples/discovery"

// discoveryTrustedIssuer is the issuer the accept fixtures describe, as a
// verifier's configuration would hold it.
var discoveryTrustedIssuers = map[string]string{
	"valid-minimal.json":                                "https://auth.example.test",
	"valid-port-and-path-prefix.json":                   "https://auth.example.test:8443/dev-health",
	"client-rejected-jwks-uri-other-origin.json":        "https://auth.example.test",
	"client-rejected-jwks-uri-other-port.json":          "https://auth.example.test",
	"client-rejected-jwks-uri-other-path-prefix.json":   "https://auth.example.test:8443/dev-health",
	"client-rejected-jwks-uri-nested-under-issuer.json": "https://auth.example.test",
	"client-rejected-issuer-not-pinned.json":            "https://auth.example.test",
}

func discoveryDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(testRoot(t), discoveryFixtureDir)
}

func loadDiscoveryManifest(t *testing.T) discoveryManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(discoveryDir(t), "manifest.json"))
	if err != nil {
		t.Fatalf("reading discovery manifest: %v", err)
	}
	var manifest discoveryManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parsing discovery manifest: %v", err)
	}
	return manifest
}

func loadDiscoveryFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(discoveryDir(t), name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return raw
}

func decodeDiscoveryFixture(t *testing.T, name string) any {
	t.Helper()
	var document any
	if err := json.Unmarshal(loadDiscoveryFixture(t, name), &document); err != nil {
		t.Fatalf("fixture %s is not valid JSON: %v", name, err)
	}
	return document
}

func TestTheDiscoveryManifestIsNotEmpty(t *testing.T) {
	manifest := loadDiscoveryManifest(t)
	if len(manifest.Accept) == 0 || len(manifest.Reject) == 0 || len(manifest.RejectByClient) == 0 {
		t.Fatal("a manifest category is empty; every table-driven test below would pass vacuously")
	}
}

func TestEveryDiscoveryFixtureOnDiskIsClaimedByTheManifest(t *testing.T) {
	manifest := loadDiscoveryManifest(t)
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
	entries, err := os.ReadDir(discoveryDir(t))
	if err != nil {
		t.Fatalf("reading fixture dir: %v", err)
	}
	var unclaimed, missing []string
	onDisk := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "manifest.json" || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		onDisk[entry.Name()] = true
		if !claimed[entry.Name()] {
			unclaimed = append(unclaimed, entry.Name())
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

func TestEveryDiscoveryFixtureThatNeedsAClientHasATrustedIssuer(t *testing.T) {
	// A fixture missing from the issuer table would be parsed against "" and
	// refused for the wrong reason, passing the client-rejection test vacuously.
	manifest := loadDiscoveryManifest(t)
	for _, a := range manifest.Accept {
		if discoveryTrustedIssuers[a.File] == "" {
			t.Errorf("accept fixture %s has no trusted issuer", a.File)
		}
	}
	for _, r := range manifest.RejectByClient {
		if discoveryTrustedIssuers[r.File] == "" {
			t.Errorf("client-rejected fixture %s has no trusted issuer", r.File)
		}
	}
}

func TestAcceptedDiscoveryFixturesValidateAndParse(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadDiscoveryManifest(t).Accept {
		t.Run(entry.File, func(t *testing.T) {
			if err := Validate(root, DiscoverySurface, decodeDiscoveryFixture(t, entry.File)); err != nil {
				t.Fatalf("accept fixture was rejected by the schema: %v", err)
			}
			doc, err := ParseDiscovery(root, loadDiscoveryFixture(t, entry.File), discoveryTrustedIssuers[entry.File])
			if err != nil {
				t.Fatalf("accept fixture was refused by the client: %v", err)
			}
			if doc.Issuer != discoveryTrustedIssuers[entry.File] {
				t.Fatalf("issuer = %q", doc.Issuer)
			}
		})
	}
}

func TestRejectedDiscoveryFixturesFailByTheDeclaredKeyword(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadDiscoveryManifest(t).Reject {
		t.Run(entry.File, func(t *testing.T) {
			err := Validate(root, DiscoverySurface, decodeDiscoveryFixture(t, entry.File))
			if err == nil {
				t.Fatalf("validated cleanly; expected rejection by %q (%s)", entry.ExpectKeyword, entry.Why)
			}
			token, ok := goMessageForKeyword[entry.ExpectKeyword]
			if !ok {
				t.Fatalf("no Go message token registered for keyword %q", entry.ExpectKeyword)
			}
			if !strings.Contains(err.Error(), token) {
				t.Fatalf("rejected, but not by %q. token %q absent from: %v", entry.ExpectKeyword, token, err)
			}
		})
	}
}

func TestClientEnforcedDiscoveryFixturesValidateButTheClientRefusesThem(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadDiscoveryManifest(t).RejectByClient {
		t.Run(entry.File, func(t *testing.T) {
			if err := Validate(root, DiscoverySurface, decodeDiscoveryFixture(t, entry.File)); err != nil {
				t.Fatalf("this fixture is meant to be VALID against the schema; schema said: %v", err)
			}
			_, err := ParseDiscovery(root, loadDiscoveryFixture(t, entry.File), discoveryTrustedIssuers[entry.File])
			if err == nil {
				t.Fatalf("the client accepted a document only it can refuse (%s)", entry.Why)
			}
			if !strings.Contains(err.Error(), "disagrees with its own endpoint URL") &&
				!strings.Contains(err.Error(), "not the trusted issuer") {
				t.Fatalf("refused, but not for an issuer rule: %v", err)
			}
		})
	}
}

func TestADiscoveryDocumentFromAnotherIssuerIsRefused(t *testing.T) {
	// The wrong-issuer case: a perfectly valid, self-consistent document that
	// is not the one the verifier is configured to trust.
	root := testRoot(t)
	raw := loadDiscoveryFixture(t, "valid-minimal.json")
	_, err := ParseDiscovery(root, raw, "https://other.example.test")
	if err == nil || !strings.Contains(err.Error(), "not the trusted issuer") {
		t.Fatalf("err = %v, want a trusted-issuer refusal", err)
	}
	if _, err := ParseDiscovery(root, raw, ""); err == nil {
		t.Fatal("an empty trusted issuer must never match")
	}
}

func TestADiscoveryDocumentWithADuplicateMemberIsRefused(t *testing.T) {
	// Raw text, because a decoded object cannot express a duplicate. The first
	// jwks_uri is the wrong issuer's; the survivor is the right one.
	raw := []byte(`{
  "issuer": "https://auth.example.test",
  "jwks_uri": "https://evil.example.test/.well-known/jwks.json",
  "jwks_uri": "https://auth.example.test/.well-known/jwks.json",
  "id_token_signing_alg_values_supported": ["EdDSA"]
}`)
	_, err := ParseDiscovery(testRoot(t), raw, "https://auth.example.test")
	if err == nil || !strings.Contains(err.Error(), "duplicate object member") {
		t.Fatalf("err = %v, want a duplicate-member refusal", err)
	}
}

func TestDiscoveryIssuerAndJWKSURIPatternsAreInTheCrossLanguageDialect(t *testing.T) {
	// schema_dialect_test covers every pattern generically; this pins that the
	// two URL patterns exist, so deleting one cannot silently loosen the shape.
	root := testRoot(t)
	raw, err := os.ReadFile(SchemaPath(root, DiscoverySurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties map[string]struct {
			Pattern string `json:"pattern"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"issuer", "jwks_uri"} {
		if schema.Properties[name].Pattern == "" {
			t.Errorf("%s has no pattern", name)
		}
	}
}
