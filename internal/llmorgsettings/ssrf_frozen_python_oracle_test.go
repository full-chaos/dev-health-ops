package llmorgsettings

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

const pythonValidateBaseURLProgram = `
import json, sys
from dev_health_ops.llm.credentials import validate_llm_base_url
out = []
for url in json.loads(sys.stdin.read()):
    try:
        ok, error = validate_llm_base_url(url)
        out.append({"ok": ok, "error": error})
    except ValueError as exc:
        out.append({"raised": str(exc)})
print(json.dumps(out))
`

// validateBaseURLCorpus uses IP literals and names under the reserved
// .invalid TLD (which never resolve), so both planes decide without the
// network.
func validateBaseURLCorpus() []string {
	corpus := []string{
		"", "https://example.invalid/v1", "http://example.invalid/v1", "ftp://example.invalid/", "HTTPS://Example.Invalid/v1", "example.invalid",
		"https://u:p@example.invalid/", "https://@example.invalid/", "https://10.0.0.1/v1", "https://8.8.8.8/v1", "https://127.0.0.1/", "https://169.254.169.254/",
		"https://[::1]/v1", "https://[::ffff:10.0.0.1]/v1", "https://[2001:4860:4860::8888]/v1", "https://[fe80::1%25eth0]/v1", "https://[2001:db8::1]/",
		"https://100.64.0.1/", "https://192.0.2.1/", "https://0.0.0.0/", "https://255.255.255.255/", "https://224.0.0.1/",
		"https://example.invalid:65535/v1", "https://example.invalid:65536/v1", "https://example.invalid:abc/v1", "https://example.invalid:-1/v1",
		"https://example.invalid:/v1", "https://example.invalid:8443/v1", "https://example.invalid:٣/v1", "https://8.8.8.8:99999/",
		"https://[::1/v1", "https://::1]/v1", "https://[zz::1]/v1", "https://[::1]x/v1", "https://[1.2.3.4]/v1", "https://[v1.fe]/v1", "https://[vz.fe]/v1",
		"https://exa%mple.invalid/v1", "https://example%.invalid/v1", "https://exa%41mple.invalid", "https://例え.invalid/v1", "https://ＧＩＴ.invalid",
		"https://０.０.０.０/", "https://１２７.０.０.１/", "https://127。0。0。1/", "https://ｅxample.invalid/v1", "https://straße.invalid/v1", "https://ＬＯＣＡＬＨＯＳＴ/",
		"https://\u05d0\u05d1.invalid/", "https://a\u05d0.invalid/", "https://xn--é.invalid/", "https://é..invalid/", "https://" + strings.Repeat("ａ", 64) + ".invalid/",
		"https:", "https://", "https:///v1", "https://.", "https://...", "https://a..b.invalid/", "https://.a.invalid/", "https://a.invalid./",
		"https://" + strings.Repeat("a", 63) + ".invalid/", "https://" + strings.Repeat("a", 64) + ".invalid/", "https://a." + strings.Repeat("b", 63) + "/",
		"https://a." + strings.Repeat("b", 64) + "/", "https://" + strings.Repeat("é", 40) + ".invalid/", "https://" + strings.Repeat("é", 70) + ".invalid/",
		"https://exam\u3002ple.invalid/", "https://LOCALHOST/", "https://localhost./", "https://a b.invalid/", "https://a\x01b.invalid/", "not a url", "https://example.invalid/\x7f",
	}
	for index := range 200 {
		corpus = append(corpus, fmt.Sprintf("https://h%d.invalid:%d/v1", index, 65530+index))
	}
	return corpus
}

// TestValidateBaseURLMatchesFrozenPython compares ValidateBaseURLChecked with
// validate_llm_base_url, including the text of every refusal and the
// ValueError that escapes it.
func TestValidateBaseURLMatchesFrozenPython(t *testing.T) {
	corpus := validateBaseURLCorpus()
	input, _ := json.Marshal(corpus)
	output := frozenPython(t, "validate-base-url.golden.json",
		programoracle.Program{Name: "validate-base-url", Text: pythonValidateBaseURLProgram, Stdin: []byte(string(input))})[0]
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python returned %d results for %d cases", len(want), len(corpus))
	}
	mismatches := 0
	for index, url := range corpus {
		ok, reason, verr := ValidateBaseURLChecked(context.Background(), url)
		var got map[string]any
		if verr != nil {
			got = map[string]any{"raised": verr.Error()}
		} else if ok {
			got = map[string]any{"ok": true, "error": nil}
		} else {
			got = map[string]any{"ok": false, "error": reason}
		}
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want[index])
		if string(gotJSON) != string(wantJSON) {
			mismatches++
			if mismatches <= 20 {
				t.Errorf("%q:\n go     %s\n python %s", url, gotJSON, wantJSON)
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d cases differ", mismatches, len(corpus))
	}
	t.Logf("%d cases compared; 0 mismatches", len(corpus))
}
