package logging_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The executed leak repros of the first review round (CHAOS-7409 r1), kept as
// regression tests that use only the real resolution paths and the real process
// logger: a secret goes in through the path the reviewer used and is logged the
// way a refusal body is logged. Each is red on the first tip and green after.
func installBuffer(t *testing.T) *bytes.Buffer {
	t.Helper()
	secrets.ResetRegistered()
	t.Cleanup(secrets.ResetRegistered)
	var out bytes.Buffer
	t.Cleanup(logging.InstallDefault(logging.NewJSON(&out, slog.LevelInfo)))
	return &out
}

func TestR1LoginEchoInAnotherCaseIsRedacted(t *testing.T) {
	out := installBuffer(t)
	lookup := func(key string) (string, bool) {
		if key == "CLICKHOUSE_URI" {
			return "clickhouse://CaseMixedLogin7409:Pw7409-case-planted@ch:9000/db", true
		}
		return "", false
	}
	if _, _, err := config.ResolveDSN(lookup, "CLICKHOUSE_URI", config.ClickHouseSpec); err != nil {
		t.Fatal(err)
	}
	slog.Error("query failed", "error", "casemixedlogin7409: Authentication failed")
	if strings.Contains(strings.ToLower(out.String()), "casemixedlogin7409") || !strings.Contains(out.String(), "Authentication failed") {
		t.Fatalf("login echo: %s", out.String())
	}
}

func TestR1CamelCaseCredentialFieldIsRedacted(t *testing.T) {
	out := installBuffer(t)
	secrets.RegisterCredentialJSON([]byte(`{"privateKey":"review-private-key-7409-planted"}`))
	slog.Error("provider refused", "error", "echoed key=review-private-key-7409-planted")
	if strings.Contains(out.String(), "review-private-key-7409-planted") {
		t.Fatalf("camelCase field leaked: %s", out.String())
	}
}

func TestR1RegistryNeverDropsAnEarlierResolvedSecret(t *testing.T) {
	out := installBuffer(t)
	for i := 0; i <= 10000; i++ {
		lookup := func(key string) (string, bool) { return fmt.Sprintf("review-token-%06d", i), key == "REVIEW_TOKEN" }
		if _, _, err := secrets.ResolveSecret("REVIEW_TOKEN", lookup); err != nil {
			t.Fatal(err)
		}
	}
	slog.Error("provider refusal", "error", "upstream response review-token-000000")
	if strings.Contains(out.String(), "review-token-000000") {
		t.Fatalf("an earlier resolved secret leaked: %s", out.String())
	}
}

func TestR1SMTPPasswordFromTheEnvironmentIsRedacted(t *testing.T) {
	out := installBuffer(t)
	t.Setenv("EMAIL_PROVIDER", "smtp")
	t.Setenv("SMTP_HOST", "localhost")
	t.Setenv("SMTP_PORT", "2525")
	t.Setenv("SMTP_USERNAME", "review-smtp-user")
	t.Setenv("SMTP_PASSWORD", "review-smtp-pass-7409-planted")
	t.Setenv("EMAIL_FROM_ADDRESS", "a@example.test")
	_, _ = mail.NewSenderFromEnv(nil)
	slog.Error("smtp authentication failed", "error", `535 "review-smtp-pass-7409-planted"`)
	if strings.Contains(out.String(), "review-smtp-pass-7409-planted") {
		t.Fatalf("SMTP password leaked: %s", out.String())
	}
}
