package secrets

import (
	"bytes"
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
	RegisterCredentialJSON([]byte(`{"token":"jira-token-7409-planted","email":"sync7409@example.test","base_url":"https://acme.example.test/jira"}`))
	got := RedactRegistered("echo jira-token-7409-planted for sync7409@example.test at https://acme.example.test/jira")
	if strings.Contains(got, "jira-token-7409-planted") || strings.Contains(got, "sync7409@example.test") {
		t.Errorf("a credential field survived: %q", got)
	}
	if !strings.Contains(got, "https://acme.example.test/jira") {
		t.Errorf("a non-secret field was redacted: %q", got)
	}
}
