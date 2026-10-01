package logging_test

import (
	"bytes"
	"log/slog"
	"net/url"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The executed repros of the second review round (CHAOS-7409 r2). The values are
// plain words, not key-shaped strings: none is a credential.

func TestR2BearerFromTheEnvironmentIsRedacted(t *testing.T) {
	out := installBuffer(t)
	t.Setenv("GO_API_ROUTING_BEARER", "routing planted words")
	_ = secrets.GetenvNamed("GO_API_ROUTING_BEARER")
	slog.Error("refusal response", "error", "echoed routing planted words")
	if strings.Contains(out.String(), "routing planted words") {
		t.Fatalf("bearer leaked: %s", out.String())
	}
}

func TestR2SMTPLoginEchoIsRedacted(t *testing.T) {
	out := installBuffer(t)
	t.Setenv("EMAIL_PROVIDER", "smtp")
	t.Setenv("SMTP_HOST", "localhost")
	t.Setenv("SMTP_PORT", "2525")
	t.Setenv("SMTP_USERNAME", "mail-login-planted")
	t.Setenv("EMAIL_FROM_ADDRESS", "a@example.test")
	_, _ = mail.NewSenderFromEnv(nil)
	slog.Error("mail delivery failed", "error", `smtp authentication failed: 535 "mail-login-planted: Authentication failed"`)
	if strings.Contains(out.String(), "mail-login-planted") || !strings.Contains(out.String(), "Authentication failed") {
		t.Fatalf("smtp login echo: %s", out.String())
	}
}

func TestR2PrivateTokenFieldIsRegistered(t *testing.T) {
	out := installBuffer(t)
	secrets.RegisterDecrypted([]byte(`{"private_token":"private token words"}`))
	slog.Error("provider response", "error", "body echoed private token words")
	if strings.Contains(out.String(), "private token words") {
		t.Fatalf("private_token: %s", out.String())
	}
}

func TestR2PercentEncodedEchoOfASecretIsRedacted(t *testing.T) {
	out := installBuffer(t)
	const secret = "plus+slash/equals=words"
	secrets.Register("REVIEW_SECRET", secret)
	slog.Error("upstream response", "error", "echoed "+url.QueryEscape(secret)+" then refused")
	if strings.Contains(out.String(), secret) || strings.Contains(out.String(), url.QueryEscape(secret)) {
		t.Fatalf("percent-encoded echo leaked: %s", out.String())
	}
}

func TestR2RegisteredCommonWordDoesNotEraseOrdinaryText(t *testing.T) {
	secrets.ResetRegistered()
	t.Cleanup(secrets.ResetRegistered)
	var out bytes.Buffer
	t.Cleanup(logging.InstallDefault(logging.NewJSON(&out, slog.LevelInfo)))
	var warnings bytes.Buffer
	warn := slog.New(slog.NewTextHandler(&warnings, nil))
	previous := secrets.SetWarner(func(msg string, args ...any) { warn.Warn(msg, args...) })
	t.Cleanup(func() { secrets.SetWarner(previous) })
	secrets.Register("APP_ENV_SECRET", "development")
	slog.Error("database unavailable during development rollout")
	if !strings.Contains(out.String(), "database unavailable during development rollout") {
		t.Fatalf("ordinary text was erased: %s", out.String())
	}
	if strings.Contains(warnings.String(), "development") {
		t.Fatalf("the warning must name the setting, never the value: %s", warnings.String())
	}
}
