//go:build integration

package postgres

import (
	"context"
	"net/url"
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

	for name, dsn := range forms {
		t.Run(name, func(t *testing.T) {
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
				"Open":             openErr.Error(),
				"Boundary(Open)":   secrets.NewBoundary(dsn).Redact(openErr).Error(),
				"Boundary(driver)": secrets.NewBoundary(dsn).Redact(err).Error(),
			}
			for surface, text := range surfaces {
				for what, secret := range map[string]string{"password": wrongPassword, "login": user, "DSN": dsn} {
					if strings.Contains(text, secret) {
						t.Errorf("%s carries the %s %q:\n%s", surface, what, secret, text)
					}
				}
			}
		})
	}
}
