package webhookintake

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

type intakeCase struct {
	name     string
	provider string
	headers  map[string]string
	body     string
}

func hexMAC(key, body string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(body))
	return hex.EncodeToString(mac.Sum(nil))
}

func intakeDeps(provider, secret string, logger *slog.Logger) (http.HandlerFunc, string) {
	deps := Deps{Logger: logger}
	switch provider {
	case "github":
		deps.Secrets.GitHub = secret
		return deps.handleGitHubWebhook(), "/api/v1/webhooks/github"
	case "gitlab":
		deps.Secrets.GitLab = secret
		return deps.handleGitLabWebhook(), "/api/v1/webhooks/gitlab"
	}
	deps.Secrets.Jira = secret
	return deps.handleJiraWebhook(), "/api/v1/webhooks/jira"
}

func serve(t *testing.T, tc intakeCase, secret string) *httptest.ResponseRecorder {
	t.Helper()
	// The other providers' env secrets stay empty, and so does this one when secret is empty.
	t.Setenv("GITHUB_WEBHOOK_SECRET", "")
	t.Setenv("GITLAB_WEBHOOK_TOKEN", "")
	t.Setenv("JIRA_WEBHOOK_SECRET", "")
	handler, path := intakeDeps(tc.provider, secret, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(tc.body))
	for k, v := range tc.headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

func intakeCases() []intakeCase {
	body := `{"zen":"x"}`
	var cases []intakeCase
	for _, provider := range []string{"github", "gitlab", "jira"} {
		event := map[string]string{"github": "X-GitHub-Event", "gitlab": "X-Gitlab-Event"}[provider]
		withEvent := func(h map[string]string) map[string]string {
			if event != "" {
				h[event] = "push"
				if provider == "github" {
					h["X-GitHub-Delivery"] = "d-1"
				}
			}
			return h
		}
		sigHeader := map[string]string{"github": "X-Hub-Signature-256", "gitlab": "X-Gitlab-Token", "jira": "X-Hub-Signature"}[provider]
		guess := "sha256=" + hexMAC("a guessed key", body)
		if provider == "gitlab" {
			guess = "a guessed token"
		}
		cases = append(cases,
			intakeCase{provider + " guessed credential", provider, withEvent(map[string]string{sigHeader: guess}), body},
			intakeCase{provider + " no credential header", provider, withEvent(map[string]string{}), body},
			intakeCase{provider + " empty credential header", provider, withEvent(map[string]string{sigHeader: ""}), body},
			intakeCase{provider + " garbage credential", provider, withEvent(map[string]string{sigHeader: "sha256=zz"}), body},
			intakeCase{provider + " json content type", provider, withEvent(map[string]string{sigHeader: guess, "Content-Type": "application/json"}), body},
			intakeCase{provider + " non-json content type", provider, withEvent(map[string]string{sigHeader: guess, "Content-Type": "text/plain"}), "not json at all"},
			intakeCase{provider + " empty body", provider, withEvent(map[string]string{sigHeader: guess}), ""},
		)
		if event != "" {
			cases = append(cases, intakeCase{provider + " no event header", provider, map[string]string{sigHeader: guess}, body})
		}
	}
	return cases
}

// The same delivery gets the same answer whether or not the provider has a credential set: status, body
// and every response header.
func TestUnverifiableProviderAnswersLikeAWrongCredential(t *testing.T) {
	for _, tc := range intakeCases() {
		t.Run(tc.name, func(t *testing.T) {
			configured := serve(t, tc, "a configured key")
			unverifiable := serve(t, tc, "")
			if configured.Code != unverifiable.Code {
				t.Fatalf("status %d vs %d", configured.Code, unverifiable.Code)
			}
			if configured.Body.String() != unverifiable.Body.String() {
				t.Fatalf("body %q vs %q", configured.Body.String(), unverifiable.Body.String())
			}
			if !reflect.DeepEqual(configured.Header(), unverifiable.Header()) {
				t.Fatalf("headers %v vs %v", configured.Header(), unverifiable.Header())
			}
			if unverifiable.Code == http.StatusInternalServerError || unverifiable.Code == http.StatusOK {
				t.Fatalf("status %d for a delivery nobody can have signed", unverifiable.Code)
			}
		})
	}
}

// A delivery signed with an empty key, or carrying an empty token, must not pass: the stand-in key is
// random, not empty.
func TestUnverifiableProviderRefusesDeliveriesSignedWithAnEmptyKey(t *testing.T) {
	body := `{"zen":"x"}`
	cases := []intakeCase{
		{"github", "github", map[string]string{"X-GitHub-Event": "push", "X-GitHub-Delivery": "d", "X-Hub-Signature-256": "sha256=" + hexMAC("", body)}, body},
		{"jira", "jira", map[string]string{"X-Hub-Signature": "sha256=" + hexMAC("", body)}, body},
		{"jira bare", "jira", map[string]string{"X-Hub-Signature": hexMAC("", body)}, body},
		{"gitlab", "gitlab", map[string]string{"X-Gitlab-Event": "Push Hook", "X-Gitlab-Token": ""}, body},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := serve(t, tc, "").Code; got != http.StatusUnauthorized {
				t.Fatalf("status %d, want 401", got)
			}
		})
	}
}

func TestStandInKeyIs32NonZeroRandomBytes(t *testing.T) {
	if len(unverifiableKey) != 32 {
		t.Fatalf("length %d, want 32", len(unverifiableKey))
	}
	zero := true
	for i := 0; i < len(unverifiableKey); i++ {
		if unverifiableKey[i] != 0 {
			zero = false
		}
	}
	if zero {
		t.Fatal("the stand-in key is all zero bytes")
	}
	if verifyingSecret("set") != "set" || verifyingSecret("") != unverifiableKey {
		t.Fatal("verifyingSecret must return the configured key, else the stand-in")
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("no entropy") }

func TestStandInKeyFailureStopsStartup(t *testing.T) {
	for name, source := range map[string]io.Reader{"error": failingReader{}, "short": bytes.NewReader(make([]byte, 5))} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("a failed random read must panic, never return a key")
				}
			}()
			mustRandomKey(source)
		})
	}
}

func TestLogUnconfiguredSecretsWritesOneLinePerMissingProvider(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", "marker-value-1")
	t.Setenv("GITLAB_WEBHOOK_TOKEN", "")
	t.Setenv("JIRA_WEBHOOK_SECRET", "")
	var out bytes.Buffer
	LogUnconfiguredSecrets(slog.New(slog.NewTextHandler(&out, nil)))
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || !strings.Contains(out.String(), "provider=gitlab") || !strings.Contains(out.String(), "provider=jira") || strings.Contains(out.String(), "provider=github") {
		t.Fatalf("log = %q, want one line each for gitlab and jira", out.String())
	}
	if strings.Contains(out.String(), "marker-value-1") {
		t.Fatalf("the log carries a credential value: %q", out.String())
	}
}

// A refused delivery to an unverifiable provider writes the same log records as a wrong credential and
// nothing at request time names the missing configuration.
func TestUnverifiableProviderLogsLikeAWrongCredential(t *testing.T) {
	tc := intakeCase{"github", "github", map[string]string{"X-GitHub-Event": "push", "X-GitHub-Delivery": "d", "X-Hub-Signature-256": "sha256=" + hexMAC("guess", "{}")}, "{}"}
	logOf := func(secret string) string {
		var out bytes.Buffer
		t.Setenv("GITHUB_WEBHOOK_SECRET", "")
		handler, path := intakeDeps("github", secret, slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		}})))
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(tc.body))
		for k, v := range tc.headers {
			req.Header.Set(k, v)
		}
		handler(httptest.NewRecorder(), req)
		return out.String()
	}
	if a, b := logOf("configured"), logOf(""); a != b || a == "" {
		t.Fatalf("log of a wrong credential %q vs of an unverifiable provider %q", a, b)
	}
}

func TestVerifyGitLabTokenComparesDigestsOfAnyLength(t *testing.T) {
	for _, tc := range []struct {
		header, secret string
		want           bool
	}{
		{"s", "s", true},
		{strings.Repeat("a", 5000), strings.Repeat("a", 5000), true},
		{"short", strings.Repeat("a", 5000), false},
		{strings.Repeat("a", 5000), "short", false},
		{"a", "b", false},
	} {
		if got := verifyGitLabToken(tc.header, tc.secret); got != tc.want {
			t.Fatalf("verifyGitLabToken(len %d, len %d) = %v, want %v", len(tc.header), len(tc.secret), got, tc.want)
		}
	}
}

// The work of a refused delivery does not depend on whether a credential is set or on its length:
// the allocations of one request are the same for an absent credential and for short and long ones.
func TestRefusedDeliveryWorkDoesNotDependOnTheCredential(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira"} {
		t.Run(provider, func(t *testing.T) {
			sigHeader := map[string]string{"github": "X-Hub-Signature-256", "gitlab": "X-Gitlab-Token", "jira": "X-Hub-Signature"}[provider]
			headers := map[string]string{sigHeader: "sha256=" + strings.Repeat("0", 64)}
			if provider == "github" {
				headers["X-GitHub-Event"], headers["X-GitHub-Delivery"] = "push", "d"
			}
			if provider == "gitlab" {
				headers["X-Gitlab-Event"] = "Push Hook"
			}
			measure := func(secret string) float64 {
				// The production path: the credential comes from the environment, not from Deps.Secrets.
				t.Setenv("GITHUB_WEBHOOK_SECRET", "")
				t.Setenv("GITLAB_WEBHOOK_TOKEN", "")
				t.Setenv("JIRA_WEBHOOK_SECRET", "")
				t.Setenv(map[string]string{"github": "GITHUB_WEBHOOK_SECRET", "gitlab": "GITLAB_WEBHOOK_TOKEN", "jira": "JIRA_WEBHOOK_SECRET"}[provider], secret)
				handler, path := intakeDeps(provider, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
				return testing.AllocsPerRun(100, func() {
					req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"zen":"x"}`))
					for k, v := range headers {
						req.Header.Set(k, v)
					}
					rec := httptest.NewRecorder()
					handler(rec, req)
					if rec.Code != http.StatusUnauthorized {
						t.Fatalf("status %d, want 401", rec.Code)
					}
				})
			}
			absent, short, long := measure(""), measure(strings.Repeat("k", 32)), measure(strings.Repeat("k", 4096))
			if absent != short || absent != long {
				t.Fatalf("allocations per refused delivery: absent %v, 32-byte %v, 4096-byte %v", absent, short, long)
			}
		})
	}
}

// The material derived once at construction accepts exactly what a direct HMAC with the credential
// accepted, for every key length around the SHA-256 block size; an absent credential accepts nothing
// signed with the empty key.
func TestDerivedVerifyingKeyMatchesDirectHMACForEveryKeyLength(t *testing.T) {
	body := []byte(`{"zen":"x"}`)
	direct := func(secret string) string {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(body)
		return hex.EncodeToString(mac.Sum(nil))
	}
	for _, length := range []int{1, 63, 64, 65, 128, 4096} {
		secret := strings.Repeat("k", length-1) + "z"
		key := newVerifyingKey(secret)
		good := direct(secret)
		if !verifyGitHubWithKey(body, "sha256="+good, key) || !verifyJiraWithKey(body, "sha256="+good, key) || !verifyJiraWithKey(body, good, key) {
			t.Fatalf("length %d: a signature from a direct HMAC was refused", length)
		}
		flipped := good[:len(good)-1] + map[bool]string{true: "1", false: "0"}[good[len(good)-1] == '0']
		if verifyGitHubWithKey(body, "sha256="+flipped, key) || verifyJiraWithKey(body, flipped, key) {
			t.Fatalf("length %d: a changed signature was accepted", length)
		}
		if other := direct(secret + "x"); verifyGitHubWithKey(body, "sha256="+other, key) {
			t.Fatalf("length %d: a signature of another key was accepted", length)
		}
	}
	empty := newVerifyingKey("")
	if verifyGitHubWithKey(body, "sha256="+direct(""), empty) || verifyJiraWithKey(body, direct(""), empty) || verifyGitLabWithKey("", empty) {
		t.Fatal("an absent credential accepted a delivery signed with the empty key")
	}
}

func TestDerivedGitLabDigestMatchesADirectCompare(t *testing.T) {
	for _, length := range []int{1, 63, 64, 65, 4096} {
		secret := strings.Repeat("t", length)
		key := newVerifyingKey(secret)
		for _, token := range []string{secret, secret + "x", secret[:length-1], "", strings.Repeat("t", 5000), "t"} {
			a, b := sha256.Sum256([]byte(token)), sha256.Sum256([]byte(secret))
			want := token != "" && a == b
			if got := verifyGitLabWithKey(token, key); got != want {
				t.Fatalf("secret length %d, token length %d: got %v, want %v", length, len(token), got, want)
			}
		}
	}
}

func realisticCredential() string {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	return hex.EncodeToString(raw)
}

// A refusal's log line, through the production logger, is the same for a configured realistic credential
// and for an absent one.
func TestRefusalLogLineIsTheSameThroughTheProductionLogger(t *testing.T) {
	cases := []struct{ provider, envName, word string }{
		{"github", "GITHUB_WEBHOOK_SECRET", realisticCredential()},
		{"gitlab", "GITLAB_WEBHOOK_TOKEN", realisticCredential()},
		{"jira", "JIRA_WEBHOOK_SECRET", realisticCredential()},
	}
	for _, tc := range cases {
		t.Run(tc.provider, func(t *testing.T) {
			sigHeader := map[string]string{"github": "X-Hub-Signature-256", "gitlab": "X-Gitlab-Token", "jira": "X-Hub-Signature"}[tc.provider]
			logOf := func(secret string) string {
				t.Setenv("GITHUB_WEBHOOK_SECRET", "")
				t.Setenv("GITLAB_WEBHOOK_TOKEN", "")
				t.Setenv("JIRA_WEBHOOK_SECRET", "")
				t.Setenv(tc.envName, secret)
				var out bytes.Buffer
				handler, path := intakeDeps(tc.provider, "", logging.NewJSON(&out, slog.LevelDebug))
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
				req.Header.Set(sigHeader, "sha256="+hexMAC("guess", "{}"))
				req.Header.Set("X-GitHub-Event", "push")
				req.Header.Set("X-GitHub-Delivery", "d")
				req.Header.Set("X-Gitlab-Event", "Push Hook")
				handler(httptest.NewRecorder(), req)
				var record map[string]any
				if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &record); err != nil {
					t.Fatalf("log %q: %v", out.String(), err)
				}
				return record["msg"].(string)
			}
			// The absent credential first: a registered credential stays registered for the process.
			absent := logOf("")
			if configured := logOf(tc.word); configured != absent {
				t.Fatalf("refusal log with a configured credential %q vs an absent one %q", configured, absent)
			}
		})
	}
}

// The credential stays in the log redaction registry: a log line that carries it by mistake comes out
// blanked through the production logger.
func TestConfiguredCredentialIsBlankedFromALogLine(t *testing.T) {
	token := realisticCredential()
	t.Setenv("GITLAB_WEBHOOK_TOKEN", token)
	var out bytes.Buffer
	logger := logging.NewJSON(&out, slog.LevelDebug)
	_ = Deps{}.handleGitLabWebhook() // reads and registers the credential
	logger.Warn("echoed " + token)
	if strings.Contains(out.String(), token) || !strings.Contains(out.String(), "[REDACTED]") {
		t.Fatalf("the credential was not blanked: %q", out.String())
	}
}

// Deriving the verification material is a startup step: no request does it.
func TestNoRequestDerivesVerificationMaterial(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "jira"} {
		handler, path := intakeDeps(provider, "configured", slog.New(slog.NewTextHandler(io.Discard, nil)))
		before := keyDerivations.Load()
		for i := 0; i < 5; i++ {
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
			req.Header.Set("X-GitHub-Event", "push")
			req.Header.Set("X-GitHub-Delivery", "d")
			req.Header.Set("X-Gitlab-Event", "Push Hook")
			handler(httptest.NewRecorder(), req)
		}
		if after := keyDerivations.Load(); after != before {
			t.Fatalf("%s: %d derivations during requests, want 0", provider, after-before)
		}
	}
}
