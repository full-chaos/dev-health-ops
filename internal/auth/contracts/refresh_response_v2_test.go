package contracts

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func v2Fixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "examples", "refresh-response.v2", name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestRefreshResponseV2RefusalsNameTheirRule: each client-rejected v2 fixture
// is valid against the schema and refused at the client-rule stage with the
// rule it exists to pin; the header and claims rules are access-token.v1's.
func TestRefreshResponseV2RefusalsNameTheirRule(t *testing.T) {
	cases := map[string]struct{ stage, rule string }{
		"client-rejected-header-alg-hs256.json":                 {StageSchema, ""},
		"client-rejected-header-alg-none.json":                  {StageSchema, ""},
		"client-rejected-header-typ-jwt.json":                   {StageSchema, ""},
		"client-rejected-header-missing-kid.json":               {StageSchema, ""},
		"client-rejected-header-jku-member.json":                {StageSchema, ""},
		"client-rejected-header-duplicate-member.json":          {StageDuplicateMember, ""},
		"client-rejected-claims-lifetime-over-ceiling.json":     {StageClientRule, "lifetime_over_ceiling"},
		"client-rejected-claims-nbf-before-iat.json":            {StageClientRule, "nbf_before_iat"},
		"client-rejected-expires-in-not-token-lifetime.json":    {StageClientRule, "expires_in_not_token_lifetime"},
		"client-rejected-session-id-not-token-sid.json":         {StageClientRule, "session_id_not_token_sid"},
		"invalid-access-token-header-length-1-mod-4.json":       {StageSchema, ""},
		"invalid-access-token-signature-length-1-mod-4.json":    {StageSchema, ""},
		"invalid-access-token-padded-signature.json":            {StageSchema, ""},
		"invalid-access-token-standard-alphabet-signature.json": {StageSchema, ""},
		"invalid-access-token-newline-in-signature.json":        {StageSchema, ""},
		"client-rejected-header-trailing-bits.json":             {StageClientRule, "access_token_segment_encoding"},
		"client-rejected-payload-trailing-bits.json":            {StageClientRule, "access_token_segment_encoding"},
		"client-rejected-signature-trailing-bits.json":          {StageClientRule, "access_token_segment_encoding"},
		"client-rejected-segment-not-object.json":               {StageClientRule, "access_token_segment_not_object"},
		"client-rejected-expires-in-decimal.json":               {StageDecode, ""},
		"client-rejected-duplicate-access-token.json":           {StageDuplicateMember, ""},
	}
	for name, want := range cases {
		_, err := ParseRefreshResponse(testRoot(t), v2Fixture(t, name))
		var refusal *CredentialRefusal
		if !errors.As(err, &refusal) {
			t.Errorf("%s: err %v, want a CredentialRefusal", name, err)
			continue
		}
		if refusal.Surface != RefreshResponseV2Surface {
			t.Errorf("%s: surface %q, want %q", name, refusal.Surface, RefreshResponseV2Surface)
		}
		if refusal.Stage != want.stage || refusal.Rule != want.rule {
			t.Errorf("%s: refused at %s %q, want %s %q", name, refusal.Stage, refusal.Rule, want.stage, want.rule)
		}
	}
}

func TestRefreshResponseV2AcceptsTheEdDSATokenAndReturnsTheDocument(t *testing.T) {
	for _, name := range []string{"valid-full-lifetime.json", "valid-short-lifetime.json"} {
		response, err := ParseRefreshResponse(testRoot(t), v2Fixture(t, name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if response.SchemaVersion != RefreshResponseV2Surface || response.TokenType != "bearer" {
			t.Errorf("%s: parsed %q %q", name, response.SchemaVersion, response.TokenType)
		}
	}
}

// TestRefreshResponseVersionsDoNotDowngrade: v1 refuses an EdDSA token, v2
// refuses an HS256 token, and an unknown or missing version is refused.
func TestRefreshResponseVersionsDoNotDowngrade(t *testing.T) {
	root := testRoot(t)
	v1eddsa, err := os.ReadFile(filepath.Join(ContractsDir(root), "examples", "refresh-response", "invalid-access-token-eddsa-header.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseRefreshResponse(root, v1eddsa); err == nil {
		t.Error("a v1 document carrying an EdDSA token parsed")
	}
	v1hs256, err := os.ReadFile(filepath.Join(ContractsDir(root), "examples", "refresh-response", "valid-full-lifetimes.json"))
	if err != nil {
		t.Fatal(err)
	}
	asV2 := strings.Replace(string(v1hs256), "refresh-response.v1", "refresh-response.v2", 1)
	if _, err := ParseRefreshResponse(root, []byte(asV2)); err == nil {
		t.Error("a v2 document carrying an HS256 token parsed")
	}
	if _, err := ParseRefreshResponse(root, v1hs256); err != nil {
		t.Errorf("the v1 corpus document no longer parses: %v", err)
	}
	for name, mutate := range map[string]func(string) string{
		"unknown version": func(s string) string { return strings.Replace(s, "refresh-response.v2", "refresh-response.v3", 1) },
		"no version":      func(s string) string { return strings.Replace(s, `"schema_version": "refresh-response.v2",`, "", 1) },
		"empty version":   func(s string) string { return strings.Replace(s, "refresh-response.v2", "", 1) },
	} {
		raw := mutate(string(v2Fixture(t, "valid-full-lifetime.json")))
		if _, err := ParseRefreshResponse(root, []byte(raw)); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}

// TestRefreshResponseV2RefusalsCarryNoCredentialText: no refusal of any v2
// fixture, accepted-by-schema or not, repeats the token, the refresh
// credential or the session id.
func TestRefreshResponseV2RefusalsCarryNoCredentialText(t *testing.T) {
	root := testRoot(t)
	entries, err := os.ReadDir(filepath.Join(ContractsDir(root), "examples", "refresh-response.v2"))
	if err != nil {
		t.Fatal(err)
	}
	refused := 0
	for _, entry := range entries {
		if entry.Name() == "manifest.json" || strings.HasPrefix(entry.Name(), "valid-") {
			continue
		}
		raw := v2Fixture(t, entry.Name())
		_, err := ParseRefreshResponse(root, raw)
		if err == nil {
			t.Errorf("%s parsed", entry.Name())
			continue
		}
		refused++
		text := err.Error()
		for _, secret := range []string{"EXAMPLESIGNATURE", "eyJ", "EXAMPLEROTATED", "ses_EXAMPLE"} {
			if strings.Contains(text, secret) {
				t.Errorf("%s: the refusal repeats %q: %s", entry.Name(), secret, text)
			}
		}
	}
	if refused < 20 {
		t.Fatalf("only %d fixtures refused: the sentinel measured too little", refused)
	}
}

// TestRefreshResponseV2UsesTheAccessTokenRulesAndNoSecondCopy: the v2 check
// calls ParseAccessToken, and the file holds no algorithm or type literal of
// its own (a second copy of the header rules would drift).
func TestRefreshResponseV2UsesTheAccessTokenRulesAndNoSecondCopy(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "refresh_response_v2.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	calls := false
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.CallExpr:
			if ident, ok := n.Fun.(*ast.Ident); ok && ident.Name == "ParseAccessToken" {
				calls = true
			}
		case *ast.BasicLit:
			if n.Kind == token.STRING {
				for _, banned := range []string{"EdDSA", "at+jwt", "HS256", "alg"} {
					if strings.Contains(n.Value, banned) {
						t.Errorf("refresh_response_v2.go carries its own %q literal %s", banned, n.Value)
					}
				}
			}
		}
		return true
	})
	if !calls {
		t.Fatal("the v2 check does not call ParseAccessToken")
	}
}

// refreshResponseV2Inventory is the exact fixture inventory of the v2 corpus,
// by name. The manifest, the directory and this list must agree: removing a
// fixture needs an edit here (and in ci/auth-contracts-ajv/run.mjs), so a
// reject case cannot vanish with its manifest entry.
var refreshResponseV2Inventory = map[string][]string{
	"accept": {
		"valid-full-lifetime.json",
		"valid-short-lifetime.json",
	},
	"reject": {
		"invalid-missing-access-token.json",
		"invalid-schema-version-v1.json",
		"invalid-expires-in-zero.json",
		"invalid-expires-in-past-60-minutes.json",
		"invalid-expires-in-string.json",
		"invalid-access-token-not-jws.json",
		"invalid-access-token-empty-segment.json",
		"invalid-extra-field-user.json",
		"invalid-refresh-credential-jwt-shaped.json",
		"invalid-token-type-capitalised.json",
		"invalid-session-id-wrong-prefix.json",
		"invalid-access-token-header-length-1-mod-4.json",
		"invalid-access-token-signature-length-1-mod-4.json",
		"invalid-access-token-padded-signature.json",
		"invalid-access-token-standard-alphabet-signature.json",
		"invalid-access-token-newline-in-signature.json",
	},
	"reject_by_client": {
		"client-rejected-header-alg-hs256.json",
		"client-rejected-header-alg-none.json",
		"client-rejected-header-typ-jwt.json",
		"client-rejected-header-missing-kid.json",
		"client-rejected-header-jku-member.json",
		"client-rejected-header-duplicate-member.json",
		"client-rejected-claims-lifetime-over-ceiling.json",
		"client-rejected-claims-nbf-before-iat.json",
		"client-rejected-expires-in-not-token-lifetime.json",
		"client-rejected-session-id-not-token-sid.json",
		"client-rejected-segment-not-object.json",
		"client-rejected-expires-in-decimal.json",
		"client-rejected-duplicate-access-token.json",
		"client-rejected-header-trailing-bits.json",
		"client-rejected-payload-trailing-bits.json",
		"client-rejected-signature-trailing-bits.json",
	},
}

func TestRefreshResponseV2CorpusIsExactlyThePinnedInventory(t *testing.T) {
	dir := filepath.Join(ContractsDir(testRoot(t)), "examples", "refresh-response.v2")
	var manifest map[string]json.RawMessage
	if err := json.Unmarshal(v2Fixture(t, "manifest.json"), &manifest); err != nil {
		t.Fatal(err)
	}
	onDisk := map[string]bool{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != "manifest.json" {
			onDisk[entry.Name()] = true
		}
	}
	pinned := map[string]bool{}
	for group, names := range refreshResponseV2Inventory {
		var listed []struct {
			File string `json:"file"`
		}
		if err := json.Unmarshal(manifest[group], &listed); err != nil {
			t.Fatalf("group %s: %v", group, err)
		}
		got := make([]string, 0, len(listed))
		for _, entry := range listed {
			got = append(got, entry.File)
		}
		sort.Strings(got)
		want := append([]string(nil), names...)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("manifest group %s lists %v, the pinned inventory is %v", group, got, want)
		}
		for _, name := range names {
			pinned[name] = true
		}
	}
	for name := range onDisk {
		if !pinned[name] {
			t.Errorf("%s is on disk but not in the pinned inventory", name)
		}
	}
	for name := range pinned {
		if !onDisk[name] {
			t.Errorf("%s is pinned but absent from the directory", name)
		}
	}
}

// TestRefreshResponseV2TrailingBitFixturesDifferOnlyByTheirTrailingBits: the
// zero-bit spelling of each trailing-bit fixture parses, so the strict decode
// is the only thing that refuses the other.
func TestRefreshResponseV2TrailingBitFixturesDifferOnlyByTheirTrailingBits(t *testing.T) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	for name, index := range map[string]int{
		"client-rejected-header-trailing-bits.json":    0,
		"client-rejected-payload-trailing-bits.json":   1,
		"client-rejected-signature-trailing-bits.json": 2,
	} {
		var original map[string]any
		if err := json.Unmarshal(v2Fixture(t, name), &original); err != nil {
			t.Fatal(err)
		}
		segments := strings.Split(original["access_token"].(string), ".")
		last := segments[index][len(segments[index])-1]
		position := strings.IndexByte(alphabet, last)
		if position < 0 || position&1 == 0 {
			t.Fatalf("%s: the last character %q is not the non-zero-bit spelling", name, last)
		}
		segments[index] = segments[index][:len(segments[index])-1] + string(alphabet[position&^1])
		original["access_token"] = strings.Join(segments, ".")
		raw, err := json.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseRefreshResponse(testRoot(t), raw); err != nil {
			t.Errorf("%s: the zero-bit spelling is refused too: %v", name, err)
		}
	}
}

// TestRefreshResponseV2RefusesAnEmptySignatureSegment builds the document in
// the test: an unsigned token (the empty-signature shape) is not a fixture
// file, because the fixture scanner admits only a signature segment that
// begins EXAMPLE.
func TestRefreshResponseV2RefusesAnEmptySignatureSegment(t *testing.T) {
	var document map[string]any
	if err := json.Unmarshal(v2Fixture(t, "valid-full-lifetime.json"), &document); err != nil {
		t.Fatal(err)
	}
	segments := strings.Split(document["access_token"].(string), ".")
	document["access_token"] = segments[0] + "." + segments[1] + "."
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ParseRefreshResponse(testRoot(t), raw)
	var refusal *CredentialRefusal
	if !errors.As(err, &refusal) || refusal.Stage != StageSchema {
		t.Fatalf("an empty signature segment: err %v, want a schema refusal", err)
	}
}
