package edgetoken

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
)

const (
	testOrg = "11111111-2222-4333-8444-555555555555"
	testKey = "mint-edge-token-unit-test-signing-key-0123"
)

type fakeRow struct {
	values []any
}

func (r fakeRow) Scan(dest ...any) error {
	if len(dest) != len(r.values) {
		return errors.New("fakeRow: column count mismatch")
	}
	for i, d := range dest {
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(r.values[i]))
	}
	return nil
}

type fakeDB struct {
	active bool
	args   []any
}

func (d *fakeDB) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	d.args = args
	role := "viewer"
	return fakeRow{values: []any{
		edgetokenmint.ProvePrincipalID, "go-api-prove@service.dev-health.invalid",
		d.active, false, 6, edgetokenmint.ServiceAuthProvider, false, &role,
	}}
}

// opener returns an openFunc over db, recording whether it was used and
// whether the returned close func ran.
func opener(db *fakeDB, opened, closed *bool) openFunc {
	return func(context.Context) (edgetokenmint.RowQuerier, func(), error) {
		*opened = true
		return db, func() { *closed = true }, nil
	}
}

func TestRunRefusesBadFlagsWithoutOpeningTheDatabase(t *testing.T) {
	t.Setenv(edgetokenmint.SigningKeyEnvVar, testKey)
	for name, args := range map[string][]string{
		"no org":            {},
		"org not a UUID":    {"-org", "70d529e0"},
		"zero ttl":          {"-org", testOrg, "-ttl", "0s"},
		"negative ttl":      {"-org", testOrg, "-ttl", "-1m"},
		"ttl above the cap": {"-org", testOrg, "-ttl", "31m"},
	} {
		t.Run(name, func(t *testing.T) {
			var opened, closed bool
			var out bytes.Buffer
			if err := run(context.Background(), args, &out, io.Discard, opener(&fakeDB{active: true}, &opened, &closed)); err == nil {
				t.Fatal("expected a refusal")
			}
			if opened || out.Len() != 0 {
				t.Fatalf("opened=%v stdout=%d bytes; a refused invocation must do neither", opened, out.Len())
			}
		})
	}
}

func TestRunRequiresTheSigningKeyBeforeOpeningTheDatabase(t *testing.T) {
	t.Setenv(edgetokenmint.SigningKeyEnvVar, "")
	var opened, closed bool
	err := run(context.Background(), []string{"-org", testOrg}, &bytes.Buffer{}, io.Discard, opener(&fakeDB{active: true}, &opened, &closed))
	if err == nil || !strings.Contains(err.Error(), edgetokenmint.SigningKeyEnvVar) {
		t.Fatalf("err = %v, want a refusal naming %s", err, edgetokenmint.SigningKeyEnvVar)
	}
	if opened {
		t.Fatal("the database was opened without a signing key")
	}
}

func TestRunPrintsExactlyOneTokenForTheProvePrincipal(t *testing.T) {
	t.Setenv(edgetokenmint.SigningKeyEnvVar, testKey)
	t.Setenv(edgetokenmint.IssuerEnvVar, "")
	t.Setenv(edgetokenmint.AudienceEnvVar, "")
	db := &fakeDB{active: true}
	var opened, closed bool
	var out bytes.Buffer
	if err := run(context.Background(), []string{"-org", testOrg, "-ttl", "2m"}, &out, io.Discard, opener(db, &opened, &closed)); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !closed {
		t.Fatal("the database handle was not closed")
	}
	printed := out.String()
	if strings.Count(printed, "\n") != 1 || !strings.HasSuffix(printed, "\n") {
		t.Fatalf("stdout must be one line, the token: %d bytes, %d newlines", len(printed), strings.Count(printed, "\n"))
	}
	if !reflect.DeepEqual(db.args, []any{edgetokenmint.ProvePrincipalID, testOrg}) {
		t.Fatalf("looked up %v, want the proof principal in the requested org", db.args)
	}
	claims := &edgetokenmint.Claims{}
	if _, err := jwt.ParseWithClaims(strings.TrimSpace(printed), claims, func(*jwt.Token) (any, error) { return []byte(testKey), nil },
		jwt.WithValidMethods([]string{"HS256"}), jwt.WithIssuer(edgetokenmint.DefaultIssuer),
		jwt.WithAudience(edgetokenmint.DefaultAudience), jwt.WithExpirationRequired()); err != nil {
		t.Fatalf("the printed token does not verify: %v", err)
	}
	if claims.Subject != edgetokenmint.ProvePrincipalID || claims.OrgID != testOrg || claims.TokenVersion != 6 || claims.Role != "viewer" {
		t.Fatalf("claims = %+v", *claims)
	}
	if got := claims.ExpiresAt - claims.IssuedAt; got != 120 {
		t.Fatalf("exp-iat = %d, want the -ttl of 120s", got)
	}
}

func TestRunNeverPrintsTheKeyOrATokenWhenItRefuses(t *testing.T) {
	t.Setenv(edgetokenmint.SigningKeyEnvVar, testKey)
	var opened, closed bool
	var out bytes.Buffer
	err := run(context.Background(), []string{"-org", testOrg}, &out, io.Discard, opener(&fakeDB{active: false}, &opened, &closed))
	if !errors.Is(err, edgetokenmint.ErrPrincipalInactive) {
		t.Fatalf("err = %v, want the inactive-principal refusal", err)
	}
	if strings.Contains(err.Error(), testKey) || out.Len() != 0 {
		t.Fatalf("a refusal leaked: err=%v stdout=%d bytes", err, out.Len())
	}
	if !closed {
		t.Fatal("the database handle was not closed on refusal")
	}
}

// TestMint_MalformedFlagNeverReachesRealStderr pins the exact contract a
// codex round found broken: under the OLD subprocess model, a helper's
// stderr was simply never wired to the parent process, so a flag-parse
// diagnostic could never leak. Minting in process removes that free
// isolation -- Mint must uphold the SAME silence deliberately, by routing
// the flag set's own error output to io.Discard, not by relying on a
// process boundary that no longer exists. This test drives Mint (the
// exact in-process entry point internal/goapiproof.MintViaAllowlistedHelper
// calls) through a REAL os.Stderr swap, so a regression back to the
// zero-value flag.FlagSet (which defaults its output to os.Stderr) is
// caught by a real capture, not by reading the source. A malformed flag
// fails at fs.Parse, before any database connection is attempted.
func TestMint_MalformedFlagNeverReachesRealStderr(t *testing.T) {
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	defer func() { os.Stderr = old }()

	done := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(r)
		done <- data
	}()

	_, mintErr := Mint(context.Background(), []string{"-not-a-real-flag"})

	_ = w.Close()
	captured := <-done
	os.Stderr = old

	if mintErr == nil {
		t.Fatal("Mint([]string{\"-not-a-real-flag\"}) = nil error, want a refusal")
	}
	if len(captured) != 0 {
		t.Fatalf("Mint wrote %d byte(s) to the real stderr, want none: %q", len(captured), captured)
	}
}

func TestOpenPostgresNeverEchoesTheURI(t *testing.T) {
	t.Setenv(postgresURIEnvVar, "")
	if _, _, err := openPostgres(context.Background()); err == nil || !strings.Contains(err.Error(), postgresURIEnvVar) {
		t.Fatalf("unset URI: err = %v, want a refusal naming %s", err, postgresURIEnvVar)
	}

	const password = "s3cret-dsn-password"
	for _, uri := range []string{
		"postgresql://prove:" + password + "@127.0.0.1:1/db?connect_timeout=1",
		"not a dsn " + password,
	} {
		t.Setenv(postgresURIEnvVar, uri)
		_, _, err := openPostgres(context.Background())
		if err == nil {
			t.Fatal("an unreachable or malformed database was accepted")
		}
		if strings.Contains(err.Error(), password) {
			t.Fatalf("the error echoed the URI: %v", err)
		}
	}
}
