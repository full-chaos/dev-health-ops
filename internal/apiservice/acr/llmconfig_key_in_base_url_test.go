package acr

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func sourceWithBaseURL(baseURL string) *fakeSource {
	source := configured()
	source.cfg.BaseURL = baseURL
	return source
}

// A stored base_url that carries the key it is served with is never served: the
// route answers the marked 404 with a named reason, and neither the key nor any
// part of the url appears in the body, a header or the log.
func TestLLMConfigRefusesABaseURLThatCarriesTheKey(t *testing.T) {
	cases := map[string]string{
		"key in the host":            "https://" + strings.ToLower(testKey) + ".example.invalid/v1",
		"key in the path":            "https://p.example.invalid/" + testKey,
		"key in the query":           "https://p.example.invalid/v1?k=" + testKey,
		"escaped letter in the path": "https://p.example.invalid/%73k-live-VERY-SECRET-key-material",
		"every byte escaped":         "https://p.example.invalid/" + percentEscapeEveryByte(testKey),
		"double escaped":             "https://p.example.invalid/%2573k-live-VERY-SECRET-key-material",
		"hex form":                   "https://p.example.invalid/" + hex.EncodeToString([]byte(testKey)),
		"base64 form":                "https://p.example.invalid/" + base64.RawURLEncoding.EncodeToString([]byte(testKey)),
	}
	for name, baseURL := range cases {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			response, body := call(t, &fakeVerifier{}, sourceWithBaseURL(baseURL), &logs, testOrg, "Bearer "+testToken)
			if response.StatusCode != http.StatusNotFound || response.Header.Get(NoConfigHeader) != NoConfigValue {
				t.Fatalf("status %d marker %q, want the marked 404 (body %s)", response.StatusCode, response.Header.Get(NoConfigHeader), body)
			}
			if got := response.Header.Get(NoConfigReasonHeader); got != "byo_base_url_carries_key" {
				t.Fatalf("%s = %q, want byo_base_url_carries_key", NoConfigReasonHeader, got)
			}
			if strings.TrimSpace(body) != `{"detail":"Not found"}` {
				t.Fatalf("body = %s", body)
			}
			everything := body + headerText(response.Header) + logs.String()
			for _, leak := range []string{testKey, "example.invalid", "VERY-SECRET", "%73k-live", hex.EncodeToString([]byte(testKey))} {
				if strings.Contains(everything, leak) {
					t.Fatalf("response or log carries %q: %s", leak, everything)
				}
			}
			if !strings.Contains(logs.String(), "byo_base_url_carries_key") || !strings.Contains(logs.String(), testOrg) {
				t.Fatalf("log lacks the reason or the org id: %s", logs.String())
			}
		})
	}
}

// The compare also holds against a stored key other than the served one (a
// purpose override's url that carries the default row's key).
func TestLLMConfigRefusesWhenTheStoreFlagsAKeyInTheBaseURL(t *testing.T) {
	source := sourceWithBaseURL("https://p.example.invalid/v1")
	source.cfg.KeyInBaseURL = true
	response, body := call(t, &fakeVerifier{}, source, io.Discard, testOrg, "Bearer "+testToken)
	if response.StatusCode != http.StatusNotFound || response.Header.Get(NoConfigReasonHeader) != "byo_base_url_carries_key" {
		t.Fatalf("status %d reason %q body %s", response.StatusCode, response.Header.Get(NoConfigReasonHeader), body)
	}
}

func TestLLMConfigServesACleanBaseURL(t *testing.T) {
	for name, baseURL := range map[string]string{
		"empty":                 "",
		"public host":           "https://api.openai.com/v1",
		"escapes, not the key":  "https://p.example.invalid/%73k-other-key",
		"half of the key as-is": "https://p.example.invalid/sk-live-VERY",
	} {
		t.Run(name, func(t *testing.T) {
			response, body := call(t, &fakeVerifier{}, sourceWithBaseURL(baseURL), io.Discard, testOrg, "Bearer "+testToken)
			if response.StatusCode != http.StatusOK || response.Header.Get(NoConfigReasonHeader) != "" {
				t.Fatalf("status %d reason %q body %s", response.StatusCode, response.Header.Get(NoConfigReasonHeader), body)
			}
			if !strings.Contains(body, testKey) {
				t.Fatalf("a clean config must still be served with its key: %s", body)
			}
		})
	}
}

// A key under eight bytes is checked in its hex form too (a four-byte key).
func TestLLMConfigRefusesAShortKeysHexFormInTheBaseURL(t *testing.T) {
	source := configured()
	source.cfg.APIKey = secrets.NewHidden("tiny")
	source.cfg.BaseURL = "https://p.example.invalid/" + hex.EncodeToString([]byte("tiny"))
	response, body := call(t, &fakeVerifier{}, source, io.Discard, testOrg, "Bearer "+testToken)
	if response.StatusCode != http.StatusNotFound || response.Header.Get(NoConfigReasonHeader) != "byo_base_url_carries_key" {
		t.Fatalf("status %d reason %q body %s", response.StatusCode, response.Header.Get(NoConfigReasonHeader), body)
	}
}

// A key that is empty cannot be "carried": a url-only config is served as before.
func TestLLMConfigServesAKeylessConfigUnchanged(t *testing.T) {
	source := configured()
	source.cfg.APIKey = secrets.NewHidden("")
	response, body := call(t, &fakeVerifier{}, source, io.Discard, testOrg, "Bearer "+testToken)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status %d body %s", response.StatusCode, body)
	}
}

func percentEscapeEveryByte(s string) string {
	var out strings.Builder
	for i := 0; i < len(s); i++ {
		out.WriteString("%" + strings.ToUpper(hex.EncodeToString([]byte{s[i]})))
	}
	return out.String()
}

func headerText(header http.Header) string {
	var out strings.Builder
	for name, values := range header {
		out.WriteString(name + ": " + strings.Join(values, ",") + "\n")
	}
	return out.String()
}
