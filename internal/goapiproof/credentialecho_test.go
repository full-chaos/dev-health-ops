package goapiproof

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// echoTestToken is a JWT-shaped value assembled at run time (no committed
// literal a secret scanner would read as a key): three distinct parts, each
// longer than the guard's part and window lengths.
func echoTestToken() string {
	return strings.Join([]string{
		strings.Repeat("Hd", 15),              // 30 bytes
		strings.Repeat("Pl", 9),               // 18 bytes: a part shorter than the window, longer than the part minimum
		strings.Repeat("?>", 30) + "Zz" + "y", // 63 bytes; its base64 has '+'/'/' forms and needs padding
	}, ".")
}

func secretsFor(value string) SentSecrets {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+value)
	header.Set("Content-Type", "application/json")
	return SecretsOnRequest(header)
}

func TestSentSecretsRecognisesTheReflectionForms(t *testing.T) {
	token := echoTestToken()
	secrets := secretsFor(token)
	if secrets.Empty() {
		t.Fatal("a bearer must be a guarded secret")
	}
	half := len(token) / 2
	for name, body := range map[string]string{
		"raw":                  `{"seen":"` + token + `"}`,
		"raw with scheme":      `{"seen":"Bearer ` + token + `"}`,
		"base64 std":           base64.StdEncoding.EncodeToString([]byte(token)),
		"base64 raw std":       base64.RawStdEncoding.EncodeToString([]byte(token)),
		"base64 url":           base64.URLEncoding.EncodeToString([]byte(token)),
		"base64 raw url":       base64.RawURLEncoding.EncodeToString([]byte(token)),
		"payload part only":    `{"claims":"` + strings.Repeat("Pl", 9) + `"}`,
		"split in two fields":  `{"a":"` + token[:half] + `","b":"` + token[half:] + `"}`,
		"piece of a long run":  `{"x":"` + token[10:10+echoWindowLen] + `"}`,
		"padded by other text": strings.Repeat("noise ", 500) + token + strings.Repeat(" tail", 500),
	} {
		if !secrets.ReflectedIn([]byte(body)) {
			t.Errorf("%s: a reflected credential went unseen", name)
		}
	}
}

func TestSentSecretsLeavesOrdinaryTextAndTheStatedLimitsAlone(t *testing.T) {
	token := echoTestToken()
	secrets := secretsFor(token)
	for name, body := range map[string]string{
		"empty":                "",
		"ordinary json":        `{"items":[{"id":"abc","name":"x"}],"total":1}`,
		"content type":         `{"media":"application/json"}`,
		"short piece of value": `{"x":"` + token[10:10+echoWindowLen-1] + `"}`,
	} {
		if secrets.ReflectedIn([]byte(body)) {
			t.Errorf("%s: flagged, but it carries no recognised form of the credential", name)
		}
	}
	if !secretsFor("short").Empty() {
		t.Error("a value shorter than the minimum must not be guarded (it would match ordinary text)")
	}
	if !secretsFor("").Empty() {
		t.Error("an empty value must not be guarded")
	}
}

func TestSentSecretsHeaderReflects(t *testing.T) {
	token := echoTestToken()
	secrets := secretsFor(token)
	header := http.Header{}
	header.Set("x-dev-health-build", "abc123def456")
	header.Set("Server", "uvicorn")
	if secrets.HeaderReflects(header) {
		t.Fatal("ordinary headers flagged")
	}
	header.Set("x-dev-health-build", token)
	if !secrets.HeaderReflects(header) {
		t.Fatal("a header value that carries the credential went unseen")
	}
	// Header NAMES are not searched.
	named := http.Header{}
	named.Set("X-"+strings.Repeat("Pl", 9), "v")
	if secrets.HeaderReflects(named) {
		t.Fatal("a header name is not a value")
	}
}

// The window search must stay linear in the body: a multi-megabyte body with
// no credential in it is scanned well under a second.
func TestSentSecretsScansALargeBodyQuickly(t *testing.T) {
	secrets := secretsFor(echoTestToken())
	body := []byte(strings.Repeat(`{"id":"0123456789abcdef","v":1},`, 200000)) // ~6 MB
	start := time.Now()
	if secrets.ReflectedIn(body) {
		t.Fatal("no credential in this body")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("scanning %d bytes took %s", len(body), elapsed)
	}
}

// TestFetchBuildIdentityRefusesAResponseThatReflectsTheCredential (CHAOS-6612):
// the /buildinfo commit is printed and written into every receipt, so a route
// that reflects the bearer it was sent as `commit` (or in a header) must be
// refused before it can become a build identity -- and the refusal must not
// carry the value.
func TestFetchBuildIdentityRefusesAResponseThatReflectsTheCredential(t *testing.T) {
	token := echoTestToken()
	credential := StaticCredential("Authorization", "candidate bearer", token)
	serve := func(t *testing.T, handler http.HandlerFunc) string {
		t.Helper()
		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)
		return server.URL + "/buildinfo"
	}
	for name, handler := range map[string]http.HandlerFunc{
		"commit reflects the bearer": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"commit":"` + strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") + `","modified":false}`))
		},
		"commit is the bearer base64": func(w http.ResponseWriter, r *http.Request) {
			seen := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			_, _ = w.Write([]byte(`{"commit":"` + base64.RawURLEncoding.EncodeToString([]byte(seen)) + `","modified":false}`))
		},
		"a header reflects the bearer": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Debug", strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
			_, _ = w.Write([]byte(`{"commit":"abc123def456","modified":false}`))
		},
	} {
		commit, err := FetchBuildIdentity(t.Context(), NewLegClient(0), serve(t, handler), credential)
		if err == nil {
			t.Errorf("%s: accepted build identity %q, want a refusal", name, commit)
			continue
		}
		if strings.Contains(err.Error(), token) || commit != "" {
			t.Errorf("%s: the refusal or result carries the credential: %q / %v", name, commit, err)
		}
	}
	// An honest build identity still passes.
	ok := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"commit":"abc123def456","modified":false}`))
	})
	if commit, err := FetchBuildIdentity(t.Context(), NewLegClient(0), ok, credential); err != nil || commit != "abc123def456" {
		t.Fatalf("an honest /buildinfo must still pass: %q, %v", commit, err)
	}
}
