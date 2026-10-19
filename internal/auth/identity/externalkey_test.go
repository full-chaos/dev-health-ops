package identity

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func TestSocialProvidersAreAuditVocabulary(t *testing.T) {
	vocab := map[string]bool{}
	for _, v := range audit.AttributeVocabulary()["provider"] {
		vocab[v] = true
	}
	for _, p := range SocialProviders() {
		if !vocab[string(p)] {
			t.Errorf("provider %q is not in the audit provider vocabulary", p)
		}
		if issuer, ok := SocialIssuer(p); !ok || issuer == "" {
			t.Errorf("provider %q has no fixed issuer", p)
		}
	}
}

const ssoRow = "6f1c2b7e-3d4a-4b5c-8d9e-0a1b2c3d4e5f"

func TestExternalKeyValidate(t *testing.T) {
	for name, k := range map[string]ExternalKey{
		"google":       {Issuer: "https://accounts.google.com", Provider: ProviderGoogle, Subject: "10769150350006150715113082367"},
		"github":       {Issuer: "https://github.com", Provider: ProviderGitHub, Subject: "583231"},
		"gitlab":       {Issuer: "https://gitlab.com", Provider: ProviderGitLab, Subject: "1"},
		"sso oidc":     {Issuer: "https://idp.example.com/realms/acme", Provider: ssoRow, Subject: "00u1abcd"},
		"sso saml urn": {Issuer: "urn:example:idp", Provider: ssoRow, Subject: "user@example.com"},
	} {
		if err := k.Validate(); err != nil {
			t.Errorf("%s: valid key refused: %v", name, err)
		}
	}
	good := ExternalKey{Issuer: "https://accounts.google.com", Provider: ProviderGoogle, Subject: "1076915035"}
	for name, k := range map[string]ExternalKey{
		"local provider":       {Issuer: good.Issuer, Provider: "local", Subject: good.Subject},
		"protocol name":        {Issuer: good.Issuer, Provider: "oidc", Subject: good.Subject},
		"upper-case uuid":      {Issuer: good.Issuer, Provider: "6F1C2B7E-3D4A-4B5C-8D9E-0A1B2C3D4E5F", Subject: good.Subject},
		"braced uuid":          {Issuer: good.Issuer, Provider: "{" + ssoRow + "}", Subject: good.Subject},
		"empty provider":       {Issuer: good.Issuer, Subject: good.Subject},
		"social wrong issuer":  {Issuer: "https://evil.example", Provider: ProviderGoogle, Subject: good.Subject},
		"social issuer case":   {Issuer: "https://Accounts.Google.com", Provider: ProviderGoogle, Subject: good.Subject},
		"sso empty issuer":     {Provider: ssoRow, Subject: good.Subject},
		"empty subject":        {Issuer: good.Issuer, Provider: ProviderGoogle},
		"sso issuer edge":      {Issuer: " https://idp", Provider: ssoRow, Subject: good.Subject},
		"subject edge space":   {Issuer: good.Issuer, Provider: ProviderGoogle, Subject: good.Subject + "\u00a0"},
		"subject control":      {Issuer: good.Issuer, Provider: ProviderGoogle, Subject: "a\x00b"},
		"subject invalid utf8": {Issuer: good.Issuer, Provider: ProviderGoogle, Subject: "a\xffb"},
		"issuer too long":      {Issuer: strings.Repeat("i", maxIssuerBytes+1), Provider: ssoRow, Subject: good.Subject},
		"subject too long":     {Issuer: good.Issuer, Provider: ProviderGoogle, Subject: strings.Repeat("s", maxSubjectBytes+1)},
	} {
		if err := k.Validate(); !errors.Is(err, ErrInvalidExternalKey) {
			t.Errorf("%s: Validate() = %v, want ErrInvalidExternalKey", name, err)
		}
	}
}

func FuzzExternalKeyValidate(f *testing.F) {
	f.Add("https://accounts.google.com", "google", "1234567890")
	f.Add("https://github.com", "github", "583231")
	f.Add("https://gitlab.com", "gitlab", "42")
	f.Add("https://idp.example.com", ssoRow, "00u1")
	f.Add("urn:example:idp", ssoRow, "user@example.com")
	f.Add("", "oidc", "x")
	f.Add(" https://x", ssoRow, "x")
	f.Add("https://x", "local", "x")
	f.Add("https://x", ssoRow, "x\n")
	f.Add("https://x", "6F1C2B7E-3D4A-4B5C-8D9E-0A1B2C3D4E5F", "x")
	f.Add("https://github.com", "GitHub", "1")
	f.Fuzz(func(t *testing.T, issuer, provider, subject string) {
		key := ExternalKey{Issuer: issuer, Provider: Provider(provider), Subject: subject}
		err := key.Validate()
		want := validFuzzProvider(provider, issuer) && validFuzzPart(issuer, maxIssuerBytes) && validFuzzPart(subject, maxSubjectBytes)
		if (err == nil) != want {
			t.Fatalf("Validate(%q, %q, %q) = %v, want ok=%v", issuer, provider, subject, err, want)
		}
		if err != nil && !errors.Is(err, ErrInvalidExternalKey) {
			t.Fatalf("Validate error %v is not ErrInvalidExternalKey", err)
		}
		if err == nil && Provider(provider).IsSocial() == Provider(provider).IsSSO() {
			t.Fatalf("accepted provider %q is both or neither social and SSO", provider)
		}
	})
}

// validFuzzProvider and validFuzzPart restate the rule independently of the
// implementation: a social name with its fixed issuer, or a canonical
// lower-case UUID checked rune by rune.
func validFuzzProvider(p, issuer string) bool {
	switch p {
	case "github":
		return issuer == "https://github.com"
	case "gitlab":
		return issuer == "https://gitlab.com"
	case "google":
		return issuer == "https://accounts.google.com"
	}
	if len(p) != 36 {
		return false
	}
	for i, r := range p {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				return false
			}
		default:
			if !(r >= '0' && r <= '9') && !(r >= 'a' && r <= 'f') {
				return false
			}
		}
	}
	return true
}

func validFuzzPart(v string, maxBytes int) bool {
	if len(v) == 0 || len(v) > maxBytes || !utf8.ValidString(v) {
		return false
	}
	runes := []rune(v)
	for _, r := range runes {
		if r < 0x20 || (r >= 0x7f && r < 0xa0) || r == utf8.RuneError {
			return false
		}
	}
	return !unicode.IsSpace(runes[0]) && !unicode.IsSpace(runes[len(runes)-1])
}

// TestNoIdentityLookupOrLinkTakesAnEmail is the structural half of "never
// link by email": the functions that find, link, unlink or list external
// identities take only a context, a key (or its issuer, provider and subject),
// a principal id, an Actor and a LegacyMatch, and LegacyMatch has no email
// field. An email parameter cannot be added without failing here.
func TestNoIdentityLookupOrLinkTakesAnEmail(t *testing.T) {
	guarded := map[string]bool{"IdentityBySubject": true, "LinkIdentity": true, "UnlinkIdentity": true, "ListIdentities": true}
	allowedTypes := map[string]bool{"context.Context": true, "ExternalKey": true, "uuid.UUID": true, "Actor": true,
		"Provider": true, "*LegacyMatch": true}
	allowedStrings := map[string]bool{"issuer": true, "subject": true}
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		text := func(n ast.Node) string {
			return string(src[fset.Position(n.Pos()).Offset:fset.Position(n.End()).Offset])
		}
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if !guarded[d.Name.Name] {
					continue
				}
				seen[d.Name.Name] = true
				for _, field := range d.Type.Params.List {
					typ := text(field.Type)
					for _, n := range field.Names {
						if strings.Contains(strings.ToLower(n.Name), "email") {
							t.Errorf("%s has an email parameter %s", d.Name.Name, n.Name)
						}
						if typ == "string" && !allowedStrings[n.Name] {
							t.Errorf("%s takes a string parameter %s", d.Name.Name, n.Name)
						}
					}
					if typ != "string" && !allowedTypes[typ] {
						t.Errorf("%s takes a %s parameter", d.Name.Name, typ)
					}
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok || (ts.Name.Name != "LegacyMatch" && ts.Name.Name != "ExternalKey") {
						continue
					}
					st, _ := ts.Type.(*ast.StructType)
					for _, field := range st.Fields.List {
						for _, n := range field.Names {
							if strings.Contains(strings.ToLower(n.Name), "email") {
								t.Errorf("%s has an email field %s", ts.Name.Name, n.Name)
							}
						}
					}
				}
			}
		}
	}
	for name := range guarded {
		if !seen[name] {
			t.Errorf("%s not found; the guard checks nothing", name)
		}
	}
}
