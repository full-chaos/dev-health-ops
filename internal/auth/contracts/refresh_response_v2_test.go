package contracts

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
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
		"client-rejected-header-alg-hs256.json":              {StageSchema, ""},
		"client-rejected-header-alg-none.json":               {StageSchema, ""},
		"client-rejected-header-typ-jwt.json":                {StageSchema, ""},
		"client-rejected-header-missing-kid.json":            {StageSchema, ""},
		"client-rejected-header-jku-member.json":             {StageSchema, ""},
		"client-rejected-header-duplicate-member.json":       {StageDuplicateMember, ""},
		"client-rejected-claims-lifetime-over-ceiling.json":  {StageClientRule, "lifetime_over_ceiling"},
		"client-rejected-claims-nbf-before-iat.json":         {StageClientRule, "nbf_before_iat"},
		"client-rejected-expires-in-not-token-lifetime.json": {StageClientRule, "expires_in_not_token_lifetime"},
		"client-rejected-session-id-not-token-sid.json":      {StageClientRule, "session_id_not_token_sid"},
		"client-rejected-segment-not-base64url.json":         {StageClientRule, "access_token_segment_encoding"},
		"client-rejected-segment-not-object.json":            {StageClientRule, "access_token_segment_not_object"},
		"client-rejected-expires-in-decimal.json":            {StageDecode, ""},
		"client-rejected-duplicate-access-token.json":        {StageDuplicateMember, ""},
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
