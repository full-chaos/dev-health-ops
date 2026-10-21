package contracts

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var credentialRefusalStages = map[string]bool{
	StageDuplicateMember: true, StageSyntax: true, StageSchema: true, StageDecode: true, StageClientRule: true,
}

// credentialSurfaceCase builds a document of one credential-carrying surface
// with value in place of one member; cut, when set, ends the document right
// after value.
type credentialSurfaceCase struct {
	surface string
	base    string
	member  string
	parse   func(root string, raw []byte) error
}

func credentialSurfaceCases(t *testing.T) []credentialSurfaceCase {
	t.Helper()
	read := func(path string) string {
		raw, err := os.ReadFile(filepath.Join(testRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	return []credentialSurfaceCase{
		{AccessTokenSurface, read("contracts/auth/v1/examples/access-token/valid-api.json"), `"exp": 1760000600`,
			func(r string, b []byte) error { _, err := ParseAccessToken(r, b); return err }},
		{RefreshRequestSurface, read("contracts/auth/v1/examples/refresh-request/valid-minimal.json"), `"schema_version": "refresh-request.v1"`,
			func(r string, b []byte) error { _, err := ParseRefreshRequest(r, b); return err }},
		{RefreshResponseSurface, read("contracts/auth/v1/examples/refresh-response/valid-full-lifetimes.json"), `"expires_in": 3600`,
			func(r string, b []byte) error { _, err := ParseRefreshResponse(r, b); return err }},
	}
}

// TestCredentialSurfaceRefusalsCarryNothingFromTheInput: every stage of a
// credential-carrying parser refuses with a fixed text and a stage code. The
// inputs reach the duplicate scan, the JSON syntax check, the number decoder
// and the schema with values a generic message would repeat.
func TestCredentialSurfaceRefusalsCarryNothingFromTheInput(t *testing.T) {
	root := testRoot(t)
	for _, c := range credentialSurfaceCases(t) {
		if !strings.Contains(c.base, c.member) {
			t.Fatalf("%s: base document has no %s", c.surface, c.member)
		}
		name := c.member[:strings.Index(c.member, ":")]
		for label, value := range map[string]struct {
			text string
			cut  bool
		}{
			"number overflow":          {"12345678901234567890e999", false},
			"huge exponent":            {"7e999999999", false},
			"invalid UTF-8":            {"\"\xff\xfeINVALIDUTF8VALUE\"", false},
			"truncated string":         {"\"TRUNCATEDSTRINGVALUE", true},
			"duplicate with odd value": {"\"ODDDUPLICATEVALUE\", " + c.member, false},
		} {
			t.Run(c.surface+"/"+label, func(t *testing.T) {
				member := name + ": " + value.text
				raw := strings.Replace(c.base, c.member, member, 1)
				if value.cut {
					raw = raw[:strings.Index(raw, member)+len(member)]
				}
				err := c.parse(root, []byte(raw))
				if err == nil {
					t.Fatal("the document was accepted")
				}
				var refusal *CredentialRefusal
				if !errors.As(err, &refusal) {
					t.Fatalf("not a CredentialRefusal: %v", err)
				}
				if refusal.Surface != c.surface || !credentialRefusalStages[refusal.Stage] || err.Error() != refusal.Error() {
					t.Fatalf("refusal %+v, message %q", refusal, err.Error())
				}
				residue := err.Error()
				for _, fixed := range []string{c.surface + ":", "refused at", refusal.Stage, refusal.Rule, credentialRefusalPhrase} {
					if fixed != "" {
						residue = strings.Replace(residue, fixed, "", 1)
					}
				}
				for i := 0; i+6 <= len(raw); i++ {
					if window := raw[i : i+6]; strings.TrimSpace(window) != "" && strings.Contains(residue, window) {
						t.Fatalf("the refusal repeats input %q: %q", window, err.Error())
					}
				}
				if strings.Contains(err.Error(), strings.Trim(value.text, "\"")) {
					t.Fatalf("the refusal repeats the member value: %q", err.Error())
				}
			})
		}
	}
}
