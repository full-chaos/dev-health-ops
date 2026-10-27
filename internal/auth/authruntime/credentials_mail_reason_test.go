package authruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

const mailReason = "auth_mail_config_invalid"

func TestOwnedUsersMailRefusalsCarryTheReasonCode(t *testing.T) {
	resend := map[string]string{
		"EMAIL_PROVIDER": "resend", "EMAIL_FROM_ADDRESS": "noreply@example.com", "APP_BASE_URL": "https://app.example.com",
	}
	cases := map[string]map[string]string{
		"no from address":      without(smtpMail(), "EMAIL_FROM_ADDRESS"),
		"empty from address":   with(smtpMail(), "EMAIL_FROM_ADDRESS", " "),
		"no base url":          without(smtpMail(), "APP_BASE_URL"),
		"http base url":        with(smtpMail(), "APP_BASE_URL", "http://app.example.com"),
		"base url with path":   with(smtpMail(), "APP_BASE_URL", "https://app.example.com/app"),
		"unsupported provider": with(smtpMail(), "EMAIL_PROVIDER", "sentinel-provider"),
		"no provider":          without(smtpMail(), "EMAIL_PROVIDER", "SMTP_HOST"),
		"console provider":     with(smtpMail(), "EMAIL_PROVIDER", "console"),
		"smtp without host":    without(smtpMail(), "SMTP_HOST"),
		"bad smtp port":        with(smtpMail(), "SMTP_PORT", "sentinel-port"),
		"resend without key":   resend,
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			mailEnv(t, env)
			_, err := ownedUsersMailProvider(slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err == nil {
				t.Fatal("refusal expected")
			}
			var coded interface{ DependencyReason() string }
			if !errors.As(err, &coded) || coded.DependencyReason() != mailReason {
				t.Fatalf("error %q carries no DependencyReason %q", err, mailReason)
			}
			if strings.Contains(err.Error(), "sentinel") {
				t.Fatalf("refusal carries a value: %q", err)
			}
		})
	}
}

func TestOwnedUsersMailRefusalReasonReachesTheStartLog(t *testing.T) {
	mailEnv(t, without(smtpMail(), "EMAIL_FROM_ADDRESS"))
	env := compatEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:    "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile: "/nonexistent",
		authconfig.EnvSigningKeyID:   "credentials-test",
		authconfig.EnvUsersOwned:     "true",
	})
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if code := Execute(ctx, nil, lookupFrom(env), IO{Stdout: &logs, Stderr: &logs}); code != 1 {
		t.Fatalf("exit %d, want 1; log:\n%s", code, logs.String())
	}
	for _, line := range strings.Split(logs.String(), "\n") {
		if !strings.Contains(line, `"msg":"configure runtime dependencies"`) {
			continue
		}
		var entry struct {
			Category string `json:"error_category"`
			Reason   string `json:"reason"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		if entry.Category != "dependency_configuration_failed" || entry.Reason != mailReason {
			t.Fatalf("start log category=%q reason=%q, want dependency_configuration_failed and %s", entry.Category, entry.Reason, mailReason)
		}
		return
	}
	t.Fatalf("no 'configure runtime dependencies' line in the log:\n%s", logs.String())
}
