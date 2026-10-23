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
	RejectByClient []struct {
		File string `json:"file"`
		Why  string `json:"why"`
	} `json:"reject_by_client"`
}

const (
	endpointAuthProfileDir          = "contracts/auth/v1/examples/endpoint-auth-profile"
	endpointAuthProfileManifestPath = "contracts/auth/v1/examples/endpoint-auth-profile/manifest.json"
)

var endpointAuthProfileManifestBasename = filepath.Base(endpointAuthProfileManifestPath)

// goProfileKeywordTokens extends goMessageForKeyword with the keywords this
// corpus asserts. Each token was read off google/jsonschema-go by validating
// one fixture per keyword and printing the real error, not guessed.
var goProfileKeywordTokens = map[string]string{
	"maxItems": "maxItems:",
	"minItems": "minItems:",
	"not":      "not:",
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
	raw, err := os.ReadFile(filepath.Join(testRoot(t), endpointAuthProfileManifestPath))
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
	if len(manifest.RejectByClient) == 0 {
		t.Error("manifest declares no reject_by_client fixtures")
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
		if entry.IsDir() || entry.Name() == endpointAuthProfileManifestBasename || !strings.HasSuffix(entry.Name(), ".json") {
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
			if err := ValidateEndpointAuthProfile(root, document); err != nil {
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

// endpointAuthProfileGateRefusals is the refusal each reject_by_client fixture
// must draw from the profile gate. The schema accepts every one of them.
var endpointAuthProfileGateRefusals = map[string]string{
	"invalid-user-access-token-pins-the-envelope-typ.json":         `class "user_access_token" pins token_type "jwt"; its jwt_header_typ marker is "at+jwt"`,
	"invalid-envelope-pins-the-access-token-typ.json":              `class "effective_principal_envelope" pins token_type "at+jwt"; its jwt_header_typ marker is "jwt"`,
	"invalid-tokens-envelope-and-user-token-typs-swapped.json":     `class "effective_principal_envelope" pins token_type "at+jwt"; its jwt_header_typ marker is "jwt"`,
	"invalid-user-access-token-pins-hs256.json":                    `class "user_access_token" pins algorithm "HS256"; its jwt_header_alg marker is "EdDSA"`,
	"invalid-high-risk-action-cacheable.json":                      `action cache policy: action "members.invite" is high risk and its profile cache_class must be none`,
	"invalid-content-type-selector-no-accepted-class-owns-it.json": `selector content_type/application/x-www-form-urlencoded is declared by none of the accepted classes`,
}

func TestEndpointAuthProfileGateRefusalTableEqualsTheManifest(t *testing.T) {
	inManifest := map[string]bool{}
	for _, entry := range loadEndpointAuthProfileManifest(t).RejectByClient {
		inManifest[entry.File] = true
		if _, ok := endpointAuthProfileGateRefusals[entry.File]; !ok {
			t.Errorf("reject_by_client fixture %s has no expected refusal", entry.File)
		}
	}
	for file := range endpointAuthProfileGateRefusals {
		if !inManifest[file] {
			t.Errorf("expected refusal for %s, which the manifest does not file under reject_by_client", file)
		}
	}
}

func TestEndpointAuthProfileGateRefusesEachRejectByClientFixtureForItsOwnRule(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadEndpointAuthProfileManifest(t).RejectByClient {
		t.Run(entry.File, func(t *testing.T) {
			document := loadEndpointAuthProfileFixture(t, entry.File)
			if err := Validate(root, EndpointAuthProfileSurface, document); err != nil {
				t.Fatalf("the schema refused it, so the gate rule is not what refuses it: %v", err)
			}
			err := ValidateEndpointAuthProfile(root, document)
			if err == nil {
				t.Fatalf("the profile gate accepted it (%s)", entry.Why)
			}
			if want := endpointAuthProfileGateRefusals[entry.File]; !strings.Contains(err.Error(), want) {
				t.Fatalf("refused, but not by its rule: want %q in %v", want, err)
			}
		})
	}
}

func TestActionCachePolicyOverEveryCatalogActionAndCacheClass(t *testing.T) {
	catalog := readJSONFile(t, "contracts/auth/v1/vocabulary/actions.v1.json")
	for _, raw := range catalog["actions"].([]any) {
		a := raw.(map[string]any)
		id, risk := a["action"].(string), a["risk_class"].(string)
		for _, cache := range []string{"none", "decision_bound", "resource_revision_bound"} {
			err := CheckActionCachePolicy(map[string]any{"action": id, "cache_class": cache})
			wantRefused := risk == "high" && cache != "none"
			if wantRefused != (err != nil) {
				t.Errorf("%s (%s) cache %s: refused=%v want %v", id, risk, cache, err != nil, wantRefused)
			}
			if err != nil && strings.Count(err.Error(), id) != 1 {
				t.Errorf("refusal must name the action id once and nothing else of the profile: %v", err)
			}
		}
	}
}

func TestActionCachePolicyRefusesACachedProfileWithoutAnAction(t *testing.T) {
	for _, profile := range []map[string]any{
		{"cache_class": "decision_bound"},
		{"action": "", "cache_class": "resource_revision_bound"},
	} {
		if err := CheckActionCachePolicy(profile); err == nil {
			t.Errorf("accepted a cached profile with no action: %v", profile)
		}
	}
	if err := CheckActionCachePolicy(map[string]any{"classification": "public"}); err != nil {
		t.Errorf("a profile without cache_class was refused: %v", err)
	}
}

func TestActionCachePolicyRefusesACachedProfileForAnUncataloguedAction(t *testing.T) {
	for _, cache := range []string{"decision_bound", "resource_revision_bound"} {
		err := CheckActionCachePolicy(map[string]any{"action": "billing.refund", "cache_class": cache})
		if err == nil || strings.Count(err.Error(), "billing.refund") != 1 {
			t.Errorf("cache %s: err = %v, want a refusal naming the action once", cache, err)
		}
	}
	if err := CheckActionCachePolicy(map[string]any{"action": "billing.refund", "cache_class": "none"}); err != nil {
		t.Errorf("an unknown action with cache_class none was refused: %v", err)
	}
	if err := CheckActionCachePolicy(map[string]any{"action": "acr.context.read", "cache_class": "decision_bound"}); err != nil {
		t.Errorf("a pinned exemption was refused: %v", err)
	}
}

// The exemption list is pinned by hand: an added entry is red, and an entry
// with no committed valid cached example left is red.
func TestUncataloguedCachedActionExemptionsArePinnedExactly(t *testing.T) {
	want := []string{"acr.context.read"}
	var got []string
	for action, reason := range uncataloguedCachedActions {
		if reason == "" {
			t.Errorf("exemption %s has no reason", action)
		}
		got = append(got, action)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("exemptions %v, pinned %v", got, want)
	}
	catalog := map[string]bool{}
	for _, raw := range readJSONFile(t, "contracts/auth/v1/vocabulary/actions.v1.json")["actions"].([]any) {
		catalog[raw.(map[string]any)["action"].(string)] = true
	}
	users := map[string]bool{}
	for _, entry := range loadEndpointAuthProfileManifest(t).Accept {
		profile := loadEndpointAuthProfileFixture(t, entry.File).(map[string]any)
		action, _ := profile["action"].(string)
		if cache, _ := profile["cache_class"].(string); action != "" && cache != "" && cache != "none" && !catalog[action] {
			users[action] = true
			if _, ok := uncataloguedCachedActions[action]; !ok {
				t.Errorf("valid example %s caches uncatalogued action %s, which is not exempted", entry.File, action)
			}
		}
	}
	for _, action := range want {
		if catalog[action] {
			t.Errorf("exemption %s is now in the catalog: delete it", action)
		}
		if !users[action] {
			t.Errorf("exemption %s has no valid cached example left: delete it", action)
		}
	}
}
