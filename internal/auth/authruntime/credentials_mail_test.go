package authruntime

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var mailEnvNames = []string{
	"EMAIL_PROVIDER", "EMAIL_FROM_ADDRESS", "EMAIL_API_KEY", "RESEND_API_KEY", "SMTP_HOST", "SMTP_PORT",
	"APP_BASE_URL", "AUTH_MAIL_ALLOW_CONSOLE",
}

// mailEnv sets exactly the given variables; every other mail variable is unset (not empty) for the test.
func mailEnv(t *testing.T, set map[string]string) {
	t.Helper()
	for _, name := range mailEnvNames {
		previous, had := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(name, previous)
			} else {
				_ = os.Unsetenv(name)
			}
		})
	}
	for name, value := range set {
		t.Setenv(name, value)
	}
}

func smtpMail() map[string]string {
	return map[string]string{
		"EMAIL_PROVIDER": "smtp", "EMAIL_FROM_ADDRESS": "noreply@example.com",
		"SMTP_HOST": "smtp.example.com", "APP_BASE_URL": "https://app.example.com",
	}
}

func without(base map[string]string, drop ...string) map[string]string {
	out := map[string]string{}
	for k, v := range base {
		out[k] = v
	}
	for _, name := range drop {
		delete(out, name)
	}
	return out
}

func with(base map[string]string, k, v string) map[string]string {
	out := without(base)
	out[k] = v
	return out
}

func ownedRoutes(t *testing.T, logs *bytes.Buffer) error {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://auth@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	_, components, err := credentialRoutes(credentialConfig(t, "true"), logger, pool, schema, mustLimits(t))
	for _, c := range components {
		_ = c.Shutdown(context.Background())
	}
	return err
}

func wantRefusal(t *testing.T, env map[string]string, name string, sentinels ...string) {
	t.Helper()
	mailEnv(t, env)
	var logs bytes.Buffer
	err := ownedRoutes(t, &logs)
	if err == nil {
		t.Fatalf("users owned started without a usable mail setting; want a refusal naming %s", name)
	}
	if !strings.Contains(err.Error(), name) {
		t.Fatalf("refusal %q does not name %s", err, name)
	}
	for _, sentinel := range sentinels {
		if strings.Contains(err.Error(), sentinel) || strings.Contains(logs.String(), sentinel) {
			t.Fatalf("refusal or log carries the value %q", sentinel)
		}
	}
}

func TestOwnedUsersRefuseTheConsoleProviderWhenNoneIsSet(t *testing.T) {
	wantRefusal(t, without(smtpMail(), "EMAIL_PROVIDER", "SMTP_HOST"), "EMAIL_PROVIDER")
}

func TestOwnedUsersRefuseTheExplicitConsoleProvider(t *testing.T) {
	wantRefusal(t, with(smtpMail(), "EMAIL_PROVIDER", "console"), "EMAIL_PROVIDER")
}

func TestOwnedUsersRefuseTheOverrideUnlessExactlyTrue(t *testing.T) {
	env := with(with(smtpMail(), "EMAIL_PROVIDER", "console"), "AUTH_MAIL_ALLOW_CONSOLE", "yes")
	wantRefusal(t, env, "EMAIL_PROVIDER")
}

func TestOwnedUsersRefuseResendWithoutAKey(t *testing.T) {
	wantRefusal(t, with(without(smtpMail(), "SMTP_HOST"), "EMAIL_PROVIDER", "resend"), "EMAIL_API_KEY")
}

func TestOwnedUsersRefuseSMTPWithoutAHost(t *testing.T) {
	wantRefusal(t, without(smtpMail(), "SMTP_HOST"), "SMTP_HOST")
}

func TestOwnedUsersRefuseAnAbsentFromAddress(t *testing.T) {
	wantRefusal(t, without(smtpMail(), "EMAIL_FROM_ADDRESS"), "EMAIL_FROM_ADDRESS")
}

func TestOwnedUsersRefuseAnEmptyFromAddress(t *testing.T) {
	wantRefusal(t, with(smtpMail(), "EMAIL_FROM_ADDRESS", ""), "EMAIL_FROM_ADDRESS")
}

func TestOwnedUsersRefuseAnAbsentAppBaseURL(t *testing.T) {
	wantRefusal(t, without(smtpMail(), "APP_BASE_URL"), "APP_BASE_URL")
}

func TestOwnedUsersRefuseAnEmptyAppBaseURL(t *testing.T) {
	wantRefusal(t, with(smtpMail(), "APP_BASE_URL", ""), "APP_BASE_URL")
}

func TestOwnedUsersRefuseAnAppBaseURLThatIsNotAnAbsoluteHTTPURL(t *testing.T) {
	for name, value := range map[string]string{
		"ftp scheme":   "ftp://sentinel-host.invalid",
		"relative":     "/sentinel/path",
		"no scheme":    "sentinel-host.invalid",
		"no host":      "https://",
		"scheme only":  "http:sentinel",
		"unparseable":  "http://sentinel host.invalid",
		"blank spaces": "   ",
	} {
		t.Run(name, func(t *testing.T) {
			wantRefusal(t, with(smtpMail(), "APP_BASE_URL", value), "APP_BASE_URL", "sentinel")
		})
	}
}

func TestOwnedUsersStartWithARealProviderAndNoWarning(t *testing.T) {
	mailEnv(t, smtpMail())
	var logs bytes.Buffer
	if err := ownedRoutes(t, &logs); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "AUTH_MAIL_ALLOW_CONSOLE") {
		t.Fatalf("a real provider logged the override warning: %s", logs.String())
	}
}

func TestOwnedUsersHonourTheConsoleOverrideWithOneWarning(t *testing.T) {
	mailEnv(t, with(with(smtpMail(), "EMAIL_PROVIDER", "console"), "AUTH_MAIL_ALLOW_CONSOLE", "true"))
	var logs bytes.Buffer
	if err := ownedRoutes(t, &logs); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(logs.String(), "AUTH_MAIL_ALLOW_CONSOLE"); got != 1 {
		t.Fatalf("override warnings = %d; want exactly 1: %s", got, logs.String())
	}
	if !strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("the override line is not a WARN: %s", logs.String())
	}
}

func TestOwnedUsersStillNeedAppBaseURLUnderTheConsoleOverride(t *testing.T) {
	env := with(with(without(smtpMail(), "APP_BASE_URL"), "EMAIL_PROVIDER", "console"), "AUTH_MAIL_ALLOW_CONSOLE", "true")
	wantRefusal(t, env, "APP_BASE_URL")
}

// TestMain gives every test in the package a real mail provider setting, so a users-owned start-up reaches the
// code under test; the mail tests above replace it with mailEnv.
func TestMain(m *testing.M) {
	for name, value := range smtpMail() {
		_ = os.Setenv(name, value)
	}
	os.Exit(m.Run())
}

func TestOwnedUsersRefuseAnHTTPAppBaseURLWithoutTheOverride(t *testing.T) {
	wantRefusal(t, with(smtpMail(), "APP_BASE_URL", "http://app.example.com"), "APP_BASE_URL")
}

func TestOwnedUsersAcceptAnHTTPAppBaseURLUnderTheOverride(t *testing.T) {
	mailEnv(t, with(with(with(smtpMail(), "EMAIL_PROVIDER", "console"), "AUTH_MAIL_ALLOW_CONSOLE", "true"), "APP_BASE_URL", "http://app.example.com"))
	var logs bytes.Buffer
	if err := ownedRoutes(t, &logs); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedUsersRefuseAnAppBaseURLWithUserinfo(t *testing.T) {
	wantRefusal(t, with(smtpMail(), "APP_BASE_URL", "https://sentinel-user:sentinel-pass@app.example.com"), "APP_BASE_URL", "sentinel")
}
