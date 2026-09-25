package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

const (
	envLogin    = "pg_env_login_x7"
	envPassword = "pg_env_password_9q"
)

// refusal is the error pgx returns when the server refuses the login.
func refusal(t *testing.T, uri string) error {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), uri)
	if err == nil {
		_ = conn.Close(context.Background())
		t.Fatal("the fake server accepted the login")
	}
	return err
}

// A login and password supplied only by PGUSER and PGPASSWORD are in the driver's
// failure text; a boundary built from the URI alone leaves them, and the boundary
// built from the resolved configuration removes them.
func TestBoundaryRedactsEnvironmentCredentials(t *testing.T) {
	host, port := fakepg.Start(t)
	t.Setenv("PGUSER", envLogin)
	t.Setenv("PGPASSWORD", envPassword)
	t.Setenv("PGSERVICEFILE", "")
	uri := fmt.Sprintf("postgres://%s:%d/appdb?sslmode=disable", host, port)
	err := refusal(t, uri)
	for _, secret := range []string{envLogin, envPassword} {
		if !strings.Contains(err.Error(), secret) {
			t.Fatalf("the driver error does not carry %q (the repro is void): %v", secret, err)
		}
		if !strings.Contains(secrets.NewBoundary(uri).Redact(err).Error(), secret) {
			t.Errorf("the URI-only boundary already redacts %q: the defect this pins is gone", secret)
		}
		if got := Boundary(uri).Redact(err).Error(); strings.Contains(got, secret) {
			t.Errorf("Boundary(uri) left %q in %q", secret, got)
		}
	}
}

// The same from a service file: the URI names only the service.
func TestBoundaryRedactsServiceFileCredentials(t *testing.T) {
	host, port := fakepg.Start(t)
	t.Setenv("PGUSER", "")
	t.Setenv("PGPASSWORD", "")
	service := filepath.Join(t.TempDir(), "pg_service.conf")
	content := fmt.Sprintf("[svc]\nhost=%s\nport=%d\nuser=svc_login_k3\npassword=svc_password_w8\ndbname=appdb\nsslmode=disable\n", host, port)
	if err := os.WriteFile(service, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PGSERVICEFILE", service)
	uri := "postgres:///appdb?service=svc"
	err := refusal(t, uri)
	for _, secret := range []string{"svc_login_k3", "svc_password_w8"} {
		if !strings.Contains(err.Error(), secret) {
			t.Fatalf("the driver error does not carry %q (the repro is void): %v", secret, err)
		}
		if !strings.Contains(secrets.NewBoundary(uri).Redact(err).Error(), secret) {
			t.Errorf("the URI-only boundary already redacts %q: the defect this pins is gone", secret)
		}
		if got := Boundary(uri).Redact(err).Error(); strings.Contains(got, secret) {
			t.Errorf("Boundary(uri) left %q in %q", secret, got)
		}
	}
}

// The URI's own credentials stay redacted, and a URI pgx cannot parse still gets a
// boundary (of the URI's own components).
func TestBoundaryKeepsTheURIComponents(t *testing.T) {
	t.Setenv("PGUSER", "")
	t.Setenv("PGPASSWORD", "")
	t.Setenv("PGSERVICEFILE", "")
	uri := "postgres://uri_login_a1:uri_password_b2@127.0.0.1:1/appdb"
	err := errors.New("failed for uri_login_a1 with uri_password_b2 at " + uri)
	if got := Boundary(uri).Redact(err).Error(); strings.Contains(got, "uri_login_a1") || strings.Contains(got, "uri_password_b2") {
		t.Errorf("redacted text %q still has a URI credential", got)
	}
	broken := "postgres://bad_login_c3:bad_password_d4@[::1"
	if got := Boundary(broken).Redact(errors.New("cannot parse " + broken)).Error(); strings.Contains(got, "bad_password_d4") {
		t.Errorf("an unparsable URI's password is in %q", got)
	}
}

// A URI the pool parser rejects (pool_max_conns=0) and pgx.Connect accepts still gets the
// resolved credentials in its boundary: the raw-connection verbs dial with it.
func TestBoundaryKeepsResolvedCredentialsWhenThePoolParserRefusesTheURI(t *testing.T) {
	host, port := fakepg.Start(t)
	t.Setenv("PGUSER", envLogin)
	t.Setenv("PGPASSWORD", envPassword)
	t.Setenv("PGSERVICEFILE", "")
	uri := fmt.Sprintf("postgres://%s:%d/appdb?sslmode=disable&pool_max_conns=0", host, port)
	if _, err := parseConfig(uri); err == nil {
		t.Fatal("the pool parser accepts pool_max_conns=0: the repro is void")
	}
	err := refusal(t, uri)
	for _, secret := range []string{envLogin, envPassword} {
		if !strings.Contains(err.Error(), secret) {
			t.Fatalf("the driver error does not carry %q (the repro is void): %v", secret, err)
		}
		if got := Boundary(uri).Redact(err).Error(); strings.Contains(got, secret) {
			t.Errorf("Boundary(uri) left %q in %q", secret, got)
		}
	}
}
