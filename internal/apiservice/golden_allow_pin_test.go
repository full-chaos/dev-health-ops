package apiservice

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-10516: the frozen goldens of the admin 405s changed only in their Allow line (a 405 now lists every method its
// path serves). Each row is one edited entry: its Allow was Old and is New. maskedSHA256 pins every other byte of the
// file (the golden with all Allow values masked, as it was on main), so a status, header or body edit fails here.
type goldenAllowEdit struct{ file, entry, old, new string }

var goldenAllowEdits = []goldenAllowEdit{
	{"testdata/credentials/admin.json", "list: PUT is not a route", "GET", "GET, POST"},
	{"testdata/credentials/admin.json", "list: DELETE on the collection is not a route", "GET", "GET, POST"},
	{"testdata/credentials/admin.json", "get: PUT is not a route", "GET", "DELETE, GET, PATCH"},
	{"testdata/credentials/admin.json", "patch: PUT is not a route", "GET", "DELETE, GET, PATCH"},
	{"admin/testdata/governance/credentialdelete.json", "credential post is 405", "GET", "DELETE, GET, PATCH"},
	{"admin/testdata/governance/credentialdelete.json", "credential put is 405", "GET", "DELETE, GET, PATCH"},
	{"testdata/venue/venue-oracle-customer-push-reads.json", "PUT sources", "POST", "GET, POST"},
	{"testdata/venue/venue-oracle-customer-push-reads.json", "DELETE source", "GET", "GET, PATCH"},
	{"testdata/venue/venue-oracle-customer-push-writes.json", "PUT source", "GET", "GET, PATCH"},
	{"testdata/venue/venue-oracle-customer-push-writes.json", "DELETE tokens", "GET", "GET, POST"},
	{"testdata/venue/protected-routes.json", "teams import: POST to another team id with a malformed body is still a 405", "DELETE", "DELETE, GET, PATCH, POST"},
	{"testdata/venue/protected-routes.json", "teams import: POST to another team id is not a route", "DELETE", "DELETE, GET, PATCH, POST"},
	{"admin/testdata/governance/retention.json", "resource types post is 405", "GET", "DELETE, GET, PATCH"},
	{"admin/testdata/governance/retention.json", "resource types put is 405", "GET", "DELETE, GET, PATCH"},
	{"admin/testdata/governance/retention.json", "policy id put is 405", "GET", "DELETE, GET, PATCH"},
	{"admin/testdata/governance/llmsettings_nokey.json", "post is 405", "GET", "DELETE, GET, PUT"},
	{"admin/testdata/governance/llmsettings_nokey.json", "patch is 405", "GET", "DELETE, GET, PUT"},
	{"admin/testdata/governance/llmsettings_keyed.json", "post is 405", "GET", "DELETE, GET, PUT"},
	{"admin/testdata/governance/llmsettings_keyed.json", "patch is 405", "GET", "DELETE, GET, PUT"},
	{"admin/testdata/governance/settings_nokey.json", "get path post is 405", "GET", "DELETE, GET, PUT"},
	{"admin/testdata/governance/settings_keyed.json", "get path post is 405", "GET", "DELETE, GET, PUT"},
	{"admin/testdata/governance/governance.json", "ip post on entry path is 405", "GET", "DELETE, GET, PATCH, POST"},
	{"admin/testdata/governance/governance.json", "ip put on entry path is 405", "GET", "DELETE, GET, PATCH, POST"},
	{"testdata/venue/protected-routes.json", "teams: PUT is not a route (Allow header of the pattern's first route)", "DELETE", "DELETE, GET, PATCH, POST"},
}

var goldenMaskedSHA256 = map[string]string{
	"admin/testdata/governance/credentialdelete.json":       "b5ae6abecebe8cdcfaf0fa242a990692ead5f77ad4ba45f128fb880209e20f0d",
	"admin/testdata/governance/governance.json":             "d711ea823c738b25c8067e28066f3a1c39ff9d35a3d4abc79109efb08bac77fd",
	"admin/testdata/governance/llmsettings_keyed.json":      "230e350d7de6de765e69194829acdd8056a4328b1f05c9c442deb32154f0d38a",
	"admin/testdata/governance/llmsettings_nokey.json":      "1957f796bf493c7773724f1f3b55b7a76032f44fff80ebdee02b2a53c1109bac",
	"admin/testdata/governance/retention.json":              "0922d4d394d7e31a8f617db85e38cb5f5aa49e836b94914cb8c47777f07f0547",
	"admin/testdata/governance/settings_keyed.json":         "93a5462bc042d8ca5beaecf8c6396d35d675285aead4a84a15c95d61d7324280",
	"admin/testdata/governance/settings_nokey.json":         "4097308beb5b90d80dfe6244da17d5437b11833358f88e1825f50d9ebc93793b",
	"testdata/credentials/admin.json":                       "396d57c85fe59d9c3224e5150474a5726ae66c0db485c7782651fc0eb87badc5",
	"testdata/venue/protected-routes.json":                  "4f1f0fedc1c8cbf3239e5e1eb6c2510f8fc7febf66f7de6aaece360937e3043c",
	"testdata/venue/venue-oracle-customer-push-reads.json":  "48f95d676c05f2419a1f19a0caf33a702dd3b386f151b9f6fa1f07aa1def65d0",
	"testdata/venue/venue-oracle-customer-push-writes.json": "987b2d4ed635bb151f92efcaec93508a28563ff20e3272bb23768c5685c5c989",
}
var allowLine = regexp.MustCompile(`(?m)^(\s*"[Aa]llow": )"[^"]*"`)

func TestAdminGoldenAllowEditsAreAllowOnly(t *testing.T) {
	if len(goldenAllowEdits) != 24 {
		t.Fatalf("%d golden Allow edits, want 24", len(goldenAllowEdits))
	}
	text := map[string]string{}
	for _, edit := range goldenAllowEdits {
		body, ok := text[edit.file]
		if !ok {
			raw, err := os.ReadFile(edit.file)
			if err != nil {
				t.Fatal(err)
			}
			body = string(raw)
			text[edit.file] = body
		}
		_, after, found := strings.Cut(body, `"name": "`+edit.entry+`"`)
		if !found {
			t.Errorf("%s: entry %q not found", edit.file, edit.entry)
			continue
		}
		line := allowLine.FindString(after)
		if line == "" || !strings.HasSuffix(line, `"`+edit.new+`"`) {
			t.Errorf("%s: entry %q: Allow line %q, want %q", edit.file, edit.entry, line, edit.new)
		}
	}
	for file, want := range goldenMaskedSHA256 {
		sum := sha256.Sum256([]byte(allowLine.ReplaceAllString(text[file], `$1"MASKED"`)))
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Errorf("%s: golden differs from main outside its Allow lines (masked sha256 %s, want %s)", file, got, want)
		}
	}
}
