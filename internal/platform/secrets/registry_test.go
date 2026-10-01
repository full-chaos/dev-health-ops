package secrets

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func TestRegisteredPasswordIsRedactedByValueAnywhere(t *testing.T) {
	ResetRegistered()
	t.Cleanup(ResetRegistered)
	RegisterDSN("CLICKHOUSE_URI", "clickhouse://login_7409:Pw7409-registry@db:9000/x")
	for _, text := range []string{"Pw7409-registry", "x=Pw7409-registry;", `{"pw":"Pw7409-registry"}`} {
		if got := RedactRegistered(text); strings.Contains(got, "Pw7409-registry") {
			t.Errorf("%q kept the password: %q", text, got)
		}
	}
	if got := RedactRegistered("plain text"); got != "plain text" {
		t.Errorf("changed unrelated text: %q", got)
	}
}

func TestRegisteredLoginIsRedactedOnlyInEchoShapes(t *testing.T) {
	ResetRegistered()
	t.Cleanup(ResetRegistered)
	RegisterLogin("ch")
	RegisterLogin("planted_login_7409")
	for _, text := range []string{
		"code: 516, message: planted_login_7409: Authentication failed: password is incorrect",
		"connect clickhouse://planted_login_7409@db:9000",
		`for user "planted_login_7409"`,
		"user=planted_login_7409 refused",
		"dial //ch:secret@host",
	} {
		got := RedactRegistered(text)
		if strings.Contains(got, "planted_login_7409") || strings.Contains(got, "//ch:") {
			t.Errorf("%q kept the login: %q", text, got)
		}
	}
	for _, text := range []string{"chris-pending checks", "the ch table", "planted_login_7409 appears as a bare word"} {
		if got := RedactRegistered(text); got != text {
			t.Errorf("a bare word was changed: %q -> %q", text, got)
		}
	}
}

func TestShortSecretIsNotRegisteredAndWarnsOnceByName(t *testing.T) {
	ResetRegistered()
	t.Cleanup(ResetRegistered)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	previous := SetWarner(func(msg string, args ...any) { logger.Warn(msg, args...) })
	t.Cleanup(func() { SetWarner(previous) })
	var wait sync.WaitGroup
	for range 3 {
		wait.Add(1)
		go func() { defer wait.Done(); Register("CLICKHOUSE_PASSWORD", "short7") }()
	}
	wait.Wait()
	if got := RedactRegistered("short7"); got != "short7" {
		t.Errorf("a short value was redacted: %q", got)
	}
	out := logs.String()
	if strings.Count(out, "secret_shorter_than_minimum_not_redactable_by_value") != 1 {
		t.Fatalf("want one warning, got: %s", out)
	}
	if !strings.Contains(out, "CLICKHOUSE_PASSWORD") || strings.Contains(out, "short7") {
		t.Fatalf("the warning must name the setting and never the value: %s", out)
	}
}

func TestDecryptedCredentialFieldsAreRegistered(t *testing.T) {
	ResetRegistered()
	t.Cleanup(ResetRegistered)
	RegisterCredentialJSON([]byte(`{"token":"jira planted words","email":"sync7409@example.test","base_url":"https://acme.example.test/jira"}`))
	got := RedactRegistered("echo jira planted words for sync7409@example.test at https://acme.example.test/jira")
	if strings.Contains(got, "jira planted words") || strings.Contains(got, "sync7409@example.test") {
		t.Errorf("a credential field survived: %q", got)
	}
	if !strings.Contains(got, "https://acme.example.test/jira") {
		t.Errorf("a non-secret field was redacted: %q", got)
	}
}

// r1: a server may change the case of the login it echoes.
func TestLoginEchoIsRedactedWhateverTheCase(t *testing.T) {
	ResetRegistered()
	t.Cleanup(ResetRegistered)
	RegisterLogin("CaseMixedLogin7409")
	got := RedactRegistered("casemixedlogin7409: Authentication failed")
	if strings.Contains(strings.ToLower(got), "casemixedlogin7409") {
		t.Fatalf("a lower-cased login echo leaked: %q", got)
	}
}

// r1: the supported camelCase and nested forms of a credential are registered too.
func TestCredentialFieldsAreRegisteredInEverySupportedSpelling(t *testing.T) {
	ResetRegistered()
	t.Cleanup(ResetRegistered)
	RegisterCredentialJSON([]byte(`{"privateKey":"camel private words","apiToken":"camel api words","auth":{"client_secret":"nested client words"},"base_url":"https://x.example.test"}`))
	for _, secret := range []string{"camel private words", "camel api words", "nested client words"} {
		if got := RedactRegistered("echo " + secret); strings.Contains(got, secret) {
			t.Errorf("%q survived", secret)
		}
	}
}

// r1: the registry never drops a secret: a dropped secret is a leak.
func TestRegistryNeverDropsASecret(t *testing.T) {
	ResetRegistered()
	t.Cleanup(ResetRegistered)
	previous := SetWarner(func(string, ...any) {})
	t.Cleanup(func() { SetWarner(previous) })
	for i := 0; i <= warnRegisteredAt; i++ {
		Register("TOKEN", fmt.Sprintf("review-token-%06d", i))
	}
	for _, i := range []int{0, 1, warnRegisteredAt} {
		secret := fmt.Sprintf("review-token-%06d", i)
		if got := RedactRegistered("body " + secret); strings.Contains(got, secret) {
			t.Fatalf("%q was dropped from the registry", secret)
		}
	}
}

// A longer secret that contains a shorter one is replaced whole.
func TestLongestRegisteredSecretIsReplacedFirst(t *testing.T) {
	ResetRegistered()
	t.Cleanup(ResetRegistered)
	Register("A", "abcdefgh")
	Register("B", "abcdefgh-and-more")
	if got := RedactRegistered("x abcdefgh-and-more y"); strings.Contains(got, "and-more") {
		t.Fatalf("a suffix of the longer secret survived: %q", got)
	}
}

// r1: a secret read straight from the environment (SMTP, webhook, API keys) or
// through the process lookup is registered; a plain setting is not.
func TestDirectEnvironmentReadersRegister(t *testing.T) {
	ResetRegistered()
	t.Cleanup(ResetRegistered)
	t.Setenv("SMTP_PASSWORD", "smtp-pass-7409-planted")
	t.Setenv("RESEND_API_KEY", "resend planted words")
	t.Setenv("CLICKHOUSE_URI", "clickhouse://env_login_7409:env-dsn-pass-7409@h:9000/d")
	t.Setenv("APP_BASE_URL", "https://app.example.test/path")
	_ = GetenvSecret("SMTP_PASSWORD")
	_, _ = ProcessLookup("RESEND_API_KEY")
	_ = GetenvNamed("CLICKHOUSE_URI")
	_ = GetenvNamed("APP_BASE_URL")
	for _, secret := range []string{"smtp-pass-7409-planted", "resend planted words", "env-dsn-pass-7409"} {
		if got := RedactRegistered("535 " + secret); strings.Contains(got, secret) {
			t.Errorf("%q survived", secret)
		}
	}
	if got := RedactRegistered("https://app.example.test/path"); got != "https://app.example.test/path" {
		t.Errorf("a plain setting was redacted: %q", got)
	}
}

// D3697: a short value or a common word is redacted only in credential shapes,
// never as a bare word (both directions are tested).
func TestShortSecretAndCommonWordAreRedactedOnlyInCredentialShapes(t *testing.T) {
	ResetRegistered()
	t.Cleanup(ResetRegistered)
	previous := SetWarner(func(string, ...any) {})
	t.Cleanup(func() { SetWarner(previous) })
	Register("CLICKHOUSE_PASSWORD", "pw7")
	Register("APP_PASSWORD", "development")
	for _, text := range []string{
		"clickhouse password=pw7 refused", "pwd: pw7;", `{"password":"pw7"}`, `{"password": "pw7", "x":1}`,
		"dial postgres://app:pw7@db:5432/x failed", "passwd=pw7",
		"password=development refused", `{"password":"development"}`, "postgres://app:development@db/x",
	} {
		got := RedactRegistered(text)
		if strings.Contains(got, "pw7") || strings.Contains(got, "development") {
			t.Errorf("a credential shape kept the value: %q -> %q", text, got)
		}
		if !strings.Contains(got, RedactedMarker) {
			t.Errorf("no marker in %q -> %q", text, got)
		}
	}
	for _, text := range []string{
		"pw7 is a word here", "the pw7 table", "deployed to development rollout", "development database unavailable",
		"password reset for development team",
	} {
		if got := RedactRegistered(text); got != text {
			t.Errorf("a bare word was changed: %q -> %q", text, got)
		}
	}
}
