//go:build integration

package postgres

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestAuthFailureNamesNeitherPasswordNorEffectiveLogin connects to a real
// PostgreSQL with a wrong password, with the login given in every form pgx reads
// it, and checks every surface that prints the failure. The server's failure
// text names the login pgx actually used: a URL query "user" overrides the
// userinfo, and the last duplicate keyword "user=" wins.
func TestAuthFailureNamesNeitherPasswordNorEffectiveLogin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	user := parsed.User.Username()
	if len(user) < 4 {
		t.Fatalf("the container login %q is too short to prove anything", user)
	}
	const wrongPassword = "not-the-password-8f31"
	host, database := parsed.Hostname(), strings.TrimPrefix(parsed.Path, "/")
	port := parsed.Port()

	forms := map[string]string{}
	userinfo := *parsed
	userinfo.User = url.UserPassword(user, wrongPassword)
	forms["userinfo"] = userinfo.String()
	query := *parsed
	query.User = url.UserPassword("decoy-login-unused", wrongPassword)
	query.RawQuery = url.Values{"user": {user}}.Encode()
	forms["query user overrides userinfo"] = query.String()
	forms["keyword, duplicate user"] = "host=" + host + " port=" + port + " dbname=" + database +
		" user=decoy-login-unused user=" + user + " password=" + wrongPassword + " sslmode=disable"

	// Credentials the DSN never carries: pgx reads PGUSER and PGPASSWORD, and a
	// service file, and the server's failure text names the login it resolved.
	t.Run("PGUSER and PGPASSWORD", func(t *testing.T) {
		t.Setenv("PGUSER", user)
		t.Setenv("PGPASSWORD", wrongPassword)
		dsn := "host=" + host + " port=" + port + " dbname=" + database + " sslmode=disable"
		checkOpenRedacts(t, ctx, dsn, user, wrongPassword, false)
	})
	t.Run("service file", func(t *testing.T) {
		serviceFile := filepath.Join(t.TempDir(), "pg_service.conf")
		if err := os.WriteFile(serviceFile, []byte("[review]\nuser="+user+"\npassword="+wrongPassword+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PGSERVICEFILE", serviceFile)
		dsn := "service=review host=" + host + " port=" + port + " dbname=" + database + " sslmode=disable"
		checkOpenRedacts(t, ctx, dsn, user, wrongPassword, false)
	})
	for name, dsn := range forms {
		t.Run(name, func(t *testing.T) { checkOpenRedacts(t, ctx, dsn, user, wrongPassword, true) })
	}
}

// checkOpenRedacts connects with the wrong password and checks every surface
// that prints the failure. The Boundary over the raw driver error is checked only
// when the DSN carries the credentials: a Boundary is built from the DSN alone.
func checkOpenRedacts(t *testing.T, ctx context.Context, dsn, user, wrongPassword string, driverSurface bool) {
	t.Helper()
	// The vector exists: pgx's own error carries the login it used.
	poolConfig, err := parseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := pgx.ConnectConfig(ctx, poolConfig.ConnConfig)
	if err == nil {
		_ = raw.Close(ctx)
		t.Fatal("the connection succeeded with a wrong password")
	}
	if !strings.Contains(err.Error(), user) {
		t.Fatalf("the server's authentication failure no longer names the login (%v): nothing to prove", err)
	}
	_, openErr := Open(ctx, DefaultConfig(dsn))
	if openErr == nil {
		t.Fatal("Open succeeded with a wrong password")
	}
	surfaces := map[string]string{
		"Open":           openErr.Error(),
		"Boundary(Open)": secrets.NewBoundary(dsn).Redact(openErr).Error(),
	}
	if driverSurface {
		surfaces["Boundary(driver)"] = secrets.NewBoundary(dsn).Redact(err).Error()
	}
	for surface, text := range surfaces {
		// The failure's own text stays: an operator must still tell an
		// authentication failure from a refused dial.
		if !strings.Contains(strings.ToLower(text), "authentication failed") {
			t.Errorf("%s lost the failure's own text:\n%s", surface, text)
		}
		for what, secret := range map[string]string{"password": wrongPassword, "login": user, "DSN": dsn} {
			if strings.Contains(text, secret) {
				t.Errorf("%s carries the %s %q:\n%s", surface, what, secret, text)
			}
		}
	}
}
