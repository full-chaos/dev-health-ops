package logging_test

import (
	"bytes"
	"errors"
	"log"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The process logger redacts every secret the process resolved, at log time,
// through every way a call site can log: no verb or call-site wrap is involved
// (CHAOS-7409). Every binary's logger is logging.NewJSON (the enumeration test
// above proves it), so this proves them all.
func TestRootLoggerRedactsResolvedSecretsThroughEveryCallShape(t *testing.T) {
	secrets.ResetRegistered()
	t.Cleanup(secrets.ResetRegistered)
	const chPassword, pgPassword, tokenValue = "Pw7409-ch-planted", "Pw7409-pg-planted", "tok-7409-planted-abc"
	env := map[string]string{
		"CLICKHOUSE_URI": "clickhouse://root_login_7409:" + chPassword + "@ch:9000/db",
		"POSTGRES_URI":   "postgres://pg_login_7409:" + pgPassword + "@pg:5432/db",
	}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	for _, key := range []string{"CLICKHOUSE_URI", "POSTGRES_URI"} {
		spec := config.ClickHouseSpec
		if key == "POSTGRES_URI" {
			spec = config.DomainDatabaseSpec
		}
		if _, configured, err := config.ResolveDSN(lookup, key, spec); err != nil || !configured {
			t.Fatalf("%s: configured=%v err=%v", key, configured, err)
		}
	}
	secrets.RegisterCredentialJSON([]byte(`{"token":"` + tokenValue + `","email":"a@b.example"}`))

	var out bytes.Buffer
	logger := logging.NewJSON(&out, slog.LevelDebug)
	restore := logging.InstallDefault(logger)
	defer restore()
	logAll := func(secret string) {
		slog.Warn("plain message "+secret, "attr", secret)
		slog.Default().With("bound", secret).Warn("bound")
		slog.Default().WithGroup("g").Warn("grouped", slog.Group("inner", "k", secret))
		slog.Error("failed", "error", errors.New("code: 516, message: dial: "+secret))
		logger.Warn("direct", "wrapped", errors.Join(errors.New("a"), errors.New(secret)))
		log.Print("stdlib " + secret)
	}
	for _, secret := range []string{chPassword, pgPassword, tokenValue} {
		logAll(secret)
	}
	slog.Warn("login echo", "error", "code: 516, message: root_login_7409: Authentication failed")
	got := out.String()
	for _, secret := range []string{chPassword, pgPassword, tokenValue, "root_login_7409"} {
		if strings.Contains(got, secret) {
			t.Errorf("%q reached the log:\n%s", secret, got)
		}
	}
	if strings.Count(got, "\n") < 19 {
		t.Fatalf("the planted lines did not all reach the log:\n%s", got)
	}
	if !strings.Contains(got, "Authentication failed") {
		t.Errorf("the server's refusal text must stay readable:\n%s", got)
	}
}

// The registry reports an unredactable (too short) secret by setting name through
// the installed process logger, and stops when the logger is restored.
func TestInstallDefaultRoutesRegistryWarningsAndRestores(t *testing.T) {
	secrets.ResetRegistered()
	t.Cleanup(secrets.ResetRegistered)
	var out bytes.Buffer
	restore := logging.InstallDefault(logging.NewJSON(&out, slog.LevelInfo))
	secrets.Register("CLICKHOUSE_PASSWORD", "short7")
	restore()
	if !strings.Contains(out.String(), "CLICKHOUSE_PASSWORD") || strings.Contains(out.String(), "short7") {
		t.Fatalf("warning not routed by name only: %s", out.String())
	}
	before := out.Len()
	secrets.Register("OTHER_PASSWORD", "short8")
	if out.Len() != before {
		t.Fatalf("the restored logger still receives registry warnings: %s", out.String())
	}
}
