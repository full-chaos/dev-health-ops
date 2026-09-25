//go:build integration

package clickhouse

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestAuthFailureNamesNeitherPasswordNorLoginName connects to a real ClickHouse
// with a wrong password and checks every surface that prints the failure:
// Open's own error and the error a binary prints through its Boundary. The
// server's authentication-failure text carries the login name, so the login name
// is as much a credential component of the DSN as the password.
func TestAuthFailureNamesNeitherPasswordNorLoginName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
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
		t.Fatalf("the container login name %q is too short to prove anything: a short name matches unrelated text", user)
	}
	const wrongPassword = "not-the-password-8f31"
	// Every DSN form that puts the login where the driver reads it: the userinfo,
	// the query ("username=", which the driver prefers over the userinfo), and both
	// at once (the query one is the effective login; the userinfo one is decoy).
	base := *parsed
	forms := map[string]string{}
	{
		dsn := base
		dsn.User = url.UserPassword(user, wrongPassword)
		forms["userinfo"] = dsn.String()
		query := base
		query.User = nil
		query.RawQuery = url.Values{"username": {user}, "password": {wrongPassword}}.Encode()
		forms["query username"] = query.String()
		both := base
		both.User = url.UserPassword("decoy-login-unused", wrongPassword)
		both.RawQuery = url.Values{"username": {user}}.Encode()
		forms["query overrides userinfo"] = both.String()
	}
	for name, dsn := range forms {
		t.Run(name, func(t *testing.T) { checkAuthFailure(t, ctx, dsn, user, wrongPassword) })
	}
}

func checkAuthFailure(t *testing.T, ctx context.Context, dsn, user, wrongPassword string) {
	t.Helper()
	// The vector exists: the raw driver error carries the login name. Without
	// this the assertions below would pass on a server that never says it.
	options, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := clickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	rawErr := raw.Ping(ctx)
	_ = raw.Close()
	if rawErr == nil || !strings.Contains(rawErr.Error(), "Authentication failed") || !strings.Contains(rawErr.Error(), user) {
		t.Fatalf("the server's authentication failure no longer carries the login name (%v): this test has nothing to prove", rawErr)
	}

	_, openErr := Open(ctx, DefaultConfig(dsn))
	if openErr == nil {
		t.Fatal("Open succeeded with a wrong password")
	}
	surfaces := map[string]string{
		"Open":             openErr.Error(),
		"Boundary(Open)":   secrets.NewBoundary(dsn).Redact(openErr).Error(),
		"Boundary(driver)": secrets.NewBoundary(dsn).Redact(rawErr).Error(),
	}
	for name, text := range surfaces {
		for what, secret := range map[string]string{"password": wrongPassword, "login name": user, "DSN": dsn} {
			if strings.Contains(text, secret) {
				t.Errorf("%s carries the %s %q:\n%s", name, what, secret, text)
			}
		}
		if !strings.Contains(text, "Authentication failed") {
			t.Errorf("%s lost the failure's own text (an operator must still tell an authentication failure from a refused dial):\n%s", name, text)
		}
	}
}
