package edgetoken

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
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
	// sessionOwner is the principal the session row belongs to (default: the proof principal).
	sessionOwner string
	args         []any
	sql          string
}

func (d *fakeDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	if strings.Contains(sql, ".sessions") {
		pin := int64(6)
		owner := edgetokenmint.ProvePrincipalID
		if d.sessionOwner != "" {
			owner = d.sessionOwner
		}
		return fakeRow{values: []any{uuid.MustParse(owner), false, false, &pin}}
	}
	d.args, d.sql = args, sql
	// The row answers for the principal the lookup bound: the admin
	// principal holds admin, the proof principal viewer.
	id, email, role := edgetokenmint.ProvePrincipalID, "go-api-prove@service.dev-health.invalid", "viewer"
	if len(args) > 0 && args[0] == edgetokenmint.AdminProofPrincipalID {
		id, email, role = edgetokenmint.AdminProofPrincipalID, "go-api-admin-prove@service.dev-health.invalid", "admin"
	}
	return fakeRow{values: []any{id, email, d.active, false, 6, edgetokenmint.ServiceAuthProvider, false, &role}}
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
	useEdDSAEnv(t)
	for name, args := range map[string][]string{
		"no org":            {},
		"org not a UUID":    {"-org", "70d529e0"},
		"zero ttl":          {"-org", testOrg, "-ttl", "0s"},
		"negative ttl":      {"-org", testOrg, "-ttl", "-1m"},
		"ttl above the cap": {"-org", testOrg, "-ttl", "31m"},
		"unknown principal": {"-org", testOrg, "-principal", "owner"},
		"empty principal":   {"-org", testOrg, "-principal", ""},
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
	t.Setenv(edgetokenmint.SigningKeyFileEnvVar, "")
	var opened, closed bool
	err := run(context.Background(), []string{"-org", testOrg}, &bytes.Buffer{}, io.Discard, opener(&fakeDB{active: true}, &opened, &closed))
	if err == nil || !strings.Contains(err.Error(), "path") {
		t.Fatalf("err = %v, want a refusal of the empty key path", err)
	}
	if opened {
		t.Fatal("the database was opened without a signing key")
	}
}

func TestRunPrintsExactlyOneTokenForTheProvePrincipal(t *testing.T) {
	useEdDSAEnv(t)
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
	claims := payloadOf(t, strings.TrimSpace(printed))
	if claims["sub"] != wirePrincipal(edgetokenmint.ProvePrincipalID) || claims["org_id"] != testOrg || claims["scope"] != "api:access" {
		t.Fatalf("claims = %v", claims)
	}
	if got := int64(claims["exp"].(float64) - claims["iat"].(float64)); got != 120 {
		t.Fatalf("exp-iat = %d, want the -ttl of 120s", got)
	}
}

func TestRunNeverPrintsTheKeyOrATokenWhenItRefuses(t *testing.T) {
	useEdDSAEnv(t)
	var opened, closed bool
	var out bytes.Buffer
	err := run(context.Background(), []string{"-org", testOrg}, &out, io.Discard, opener(&fakeDB{active: false}, &opened, &closed))
	if !errors.Is(err, edgetokenmint.ErrPrincipalInactive) {
		t.Fatalf("err = %v, want the inactive-principal refusal", err)
	}
	if out.Len() != 0 {
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

// -principal admin-proof mints for the org-admin proof principal, and only
// then: the default is still the read-level proof principal.
func TestRunMintsForTheAdminProofPrincipalOnlyWhenAsked(t *testing.T) {
	useEdDSAEnv(t)
	t.Setenv(edgetokenmint.IssuerEnvVar, "")
	t.Setenv(edgetokenmint.AudienceEnvVar, "")
	for name, tc := range map[string]struct {
		args        []string
		wantSubject string
		wantRole    string
	}{
		"default":           {[]string{"-org", testOrg}, edgetokenmint.ProvePrincipalID, "viewer"},
		"explicit proof":    {[]string{"-org", testOrg, "-principal", "proof"}, edgetokenmint.ProvePrincipalID, "viewer"},
		"explicit admin":    {[]string{"-org", testOrg, "-principal", "admin-proof"}, edgetokenmint.AdminProofPrincipalID, "admin"},
		"admin, flag first": {[]string{"-principal", "admin-proof", "-org", testOrg, "-ttl", "1m"}, edgetokenmint.AdminProofPrincipalID, "admin"},
	} {
		t.Run(name, func(t *testing.T) {
			db := &fakeDB{active: true, sessionOwner: tc.wantSubject}
			var opened, closed bool
			var out bytes.Buffer
			if err := run(context.Background(), tc.args, &out, io.Discard, opener(db, &opened, &closed)); err != nil {
				t.Fatalf("run: %v", err)
			}
			if !reflect.DeepEqual(db.args, []any{tc.wantSubject, testOrg}) {
				t.Fatalf("looked up %v, want %s in the requested org", db.args, tc.wantSubject)
			}
			claims := payloadOf(t, strings.TrimSpace(out.String()))
			if claims["sub"] != wirePrincipal(tc.wantSubject) {
				t.Fatalf("claims subject %v, want %s", claims["sub"], wirePrincipal(tc.wantSubject))
			}
		})
	}
}

// The minter reads AUTH_PLATFORM_ROLES_OWNED through the one parse: while on
// the lookup also reads the auth platform roles; a malformed value refuses
// before the database is opened.
func TestRunReadsThePlatformRolesSwitch(t *testing.T) {
	useEdDSAEnv(t)
	for value, auth := range map[string]bool{"": false, "false": false, "true": true, " TRUE ": true} {
		t.Setenv(rule.OwnedEnv, value)
		db := &fakeDB{active: true}
		var opened, closed bool
		if err := run(context.Background(), []string{"-org", testOrg}, &bytes.Buffer{}, io.Discard, opener(db, &opened, &closed)); err != nil {
			t.Fatalf("%q: %v", value, err)
		}
		if got := strings.Contains(db.sql, "platform_role_assignments"); got != auth {
			t.Errorf("%q: the lookup reads the auth platform roles = %v, want %v", value, got, auth)
		}
	}
	t.Setenv(rule.OwnedEnv, "yes")
	var opened, closed bool
	err := run(context.Background(), []string{"-org", testOrg}, &bytes.Buffer{}, io.Discard, opener(&fakeDB{active: true}, &opened, &closed))
	if err == nil || !strings.Contains(err.Error(), rule.OwnedEnv) || opened {
		t.Fatalf("malformed switch: err %v opened %v, want a refusal naming the switch before the database", err, opened)
	}
}

// useEdDSAEnv gives the verb the Ed25519 key file, kid, issuer and session it mints with.
func useEdDSAEnv(t *testing.T) {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "edge-token.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(edgetokenmint.SigningKeyFileEnvVar, path)
	t.Setenv(edgetokenmint.KeyIDEnvVar, "k1")
	t.Setenv(edgetokenmint.IssuerURLEnvVar, "https://auth.example.test")
	t.Setenv(edgetokenmint.SessionIDEnvVar, "cccccccc-1111-4222-8333-444444444444")
}

func wirePrincipal(id string) string {
	parsed := uuid.MustParse(id)
	return "prn_" + base64.RawURLEncoding.EncodeToString(parsed[:])
}

func payloadOf(t *testing.T, compact string) map[string]any {
	t.Helper()
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		t.Fatalf("not a compact JWS: %q", compact)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatal(err)
	}
	return claims
}
