package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// endpointAuthProfileManifest mirrors
// contracts/auth/v1/examples/endpoint-auth-profile/manifest.json. The Go
// runner, the Python runner and the ajv check read the SAME manifest over the
// SAME files; the inventory is never restated in a runner.
type endpointAuthProfileManifest struct {
	Accept []struct {
		File string `json:"file"`
	} `json:"accept"`
	Reject []struct {
		File                   string `json:"file"`
		ExpectInstanceLocation string `json:"expect_instance_location"`
		ExpectKeyword          string `json:"expect_keyword"`
		Why                    string `json:"why"`
	} `json:"reject"`
}

const endpointAuthProfileDir = "contracts/auth/v1/examples/endpoint-auth-profile"

// goProfileKeywordTokens extends goMessageForKeyword with the keywords this
// corpus asserts. Each token was read off google/jsonschema-go by validating
// one fixture per keyword and printing the real error, not guessed.
var goProfileKeywordTokens = map[string]string{
	"maxItems": "maxItems:",
	"minItems": "minItems:",
	"type":     "type:",
}

func profileTokenFor(keyword string) (string, bool) {
	if token, ok := goMessageForKeyword[keyword]; ok {
		return token, true
	}
	token, ok := goProfileKeywordTokens[keyword]
	return token, ok
}

func loadEndpointAuthProfileManifest(t *testing.T) endpointAuthProfileManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), endpointAuthProfileDir, "manifest.json"))
	if err != nil {
		t.Fatalf("reading manifest: %v", err)
	}
	var manifest endpointAuthProfileManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parsing manifest: %v", err)
	}
	return manifest
}

func loadEndpointAuthProfileFixture(t *testing.T, name string) any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), endpointAuthProfileDir, name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("fixture %s is not valid JSON: %v", name, err)
	}
	return document
}

func TestEndpointAuthProfileCorpusIsNotEmptyInEitherDirection(t *testing.T) {
	manifest := loadEndpointAuthProfileManifest(t)
	if len(manifest.Accept) == 0 {
		t.Error("manifest declares no accept fixtures")
	}
	if len(manifest.Reject) == 0 {
		t.Error("manifest declares no reject fixtures")
	}
}

func TestEveryEndpointAuthProfileFixtureOnDiskIsClaimedByTheManifest(t *testing.T) {
	manifest := loadEndpointAuthProfileManifest(t)
	entries, err := os.ReadDir(filepath.Join(testRoot(t), endpointAuthProfileDir))
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
		t.Errorf("fixture files no runner opens: %v", unclaimed)
	}
	if len(missing) > 0 {
		t.Errorf("manifest claims absent files: %v", missing)
	}
}

func TestEveryEndpointAuthProfileManifestKeywordHasAGoMessageToken(t *testing.T) {
	for _, entry := range loadEndpointAuthProfileManifest(t).Reject {
		if _, ok := profileTokenFor(entry.ExpectKeyword); !ok {
			t.Errorf("no Go message token registered for keyword %q (fixture %s)",
				entry.ExpectKeyword, entry.File)
		}
	}
}

func TestAcceptedEndpointAuthProfileFixturesValidate(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadEndpointAuthProfileManifest(t).Accept {
		t.Run(entry.File, func(t *testing.T) {
			document := loadEndpointAuthProfileFixture(t, entry.File)
			if err := Validate(root, EndpointAuthProfileSurface, document); err != nil {
				t.Fatalf("accept fixture was rejected: %v", err)
			}
		})
	}
}

func TestRejectedEndpointAuthProfileFixturesAreRejectedByTheDeclaredRule(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadEndpointAuthProfileManifest(t).Reject {
		t.Run(entry.File, func(t *testing.T) {
			document := loadEndpointAuthProfileFixture(t, entry.File)
			err := Validate(root, EndpointAuthProfileSurface, document)
			if err == nil {
				t.Fatalf("validated cleanly; expected rejection by %q at %q (%s)",
					entry.ExpectKeyword, entry.ExpectInstanceLocation, entry.Why)
			}
			token, ok := profileTokenFor(entry.ExpectKeyword)
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
