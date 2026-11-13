package livee2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	seedKey    = "ssoseed-verb-key-0123456789"
	seedSecret = "verb-client-secret-ABC123"
	seedOrg    = "00000000-0000-4000-8000-0000000000b1"
)

type seedFakeDB struct {
	execs   [][]any
	lookups int
}

func (d *seedFakeDB) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	d.execs = append(d.execs, args)
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (d *seedFakeDB) QueryRow(context.Context, string, ...any) pgx.Row {
	d.lookups++
	return seedRow{}
}

type seedRow struct{}

func (seedRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = seedOrg
	return nil
}

func seedEnv(extra map[string]string) func(string) string {
	env := map[string]string{"SETTINGS_ENCRYPTION_KEY": seedKey, "SSO_CLIENT_SECRET": seedSecret}
	for k, v := range extra {
		env[k] = v
	}
	return func(k string) string { return env[k] }
}

func fakeOpen(db *seedFakeDB, opened *int) func(context.Context) (ssoDB, func(), error) {
	return func(context.Context) (ssoDB, func(), error) {
		*opened++
		return db, func() {}, nil
	}
}

var oidcFlags = []string{"-issuer", "https://localhost:9443", "-client-id", "live-client", "-authorization-endpoint", "https://localhost:9443/authorize",
	"-token-endpoint", "https://localhost:9443/token", "-userinfo-endpoint", "https://localhost:9443/userinfo", "-jwks-uri", "https://localhost:9443/jwks"}

func TestSeedSSOProviderRefusesAnUnknownProtocolBeforeAnythingIsRead(t *testing.T) {
	for _, protocol := range []string{"ldap", "OIDC", "oauth_github", ""} {
		db, opened := &seedFakeDB{}, 0
		var out bytes.Buffer
		reads := 0
		getenv := func(string) string { reads++; return "x" }
		err := seedSSOProvider(context.Background(), append([]string{"admin@example.com", protocol}, oidcFlags...), getenv, &out, fakeOpen(db, &opened))
		if err == nil {
			t.Errorf("protocol %q accepted", protocol)
		}
		if opened != 0 || reads != 0 || len(db.execs) != 0 || out.Len() != 0 {
			t.Errorf("protocol %q: opened %d, env reads %d, writes %d, output %q; want nothing", protocol, opened, reads, len(db.execs), out.String())
		}
	}
}

// The verb prints the provider id only. The client secret and the sealed blob never reach its output, and the row holds the
// sealed value, not the secret.
func TestSeedSSOProviderOutputNeverHoldsTheSecretOrTheSealedBlob(t *testing.T) {
	db, opened := &seedFakeDB{}, 0
	var out bytes.Buffer
	if err := seedSSOProvider(context.Background(), append([]string{"admin@example.com", "oidc"}, oidcFlags...), seedEnv(nil), &out, fakeOpen(db, &opened)); err != nil {
		t.Fatal(err)
	}
	if len(db.execs) != 1 || db.lookups != 1 {
		t.Fatalf("%d inserts, %d lookups, want 1 and 1", len(db.execs), db.lookups)
	}
	var sealed string
	for _, arg := range db.execs[0] {
		if text, ok := arg.(*string); ok && text != nil && strings.Contains(*text, "client_secret") {
			sealed = *text
		}
	}
	if sealed == "" {
		t.Fatal("no encrypted_secrets argument in the insert")
	}
	if strings.Contains(sealed, seedSecret) {
		t.Errorf("the row holds the plain secret: %q", sealed)
	}
	var object map[string]string
	if err := json.Unmarshal([]byte(sealed), &object); err != nil || !strings.HasPrefix(object["client_secret"], "v1:") {
		t.Errorf("encrypted_secrets %q is not a sealed v1 object: %v", sealed, err)
	}
	if n := strings.Count(out.String(), seedSecret) + strings.Count(out.String(), object["client_secret"]) + strings.Count(out.String(), "v1:"); n != 0 {
		t.Errorf("the verb output holds the secret or the sealed blob (%d hits): %q", n, out.String())
	}
	if !strings.HasPrefix(out.String(), "seeded oidc provider ") || strings.Count(out.String(), "\n") != 1 {
		t.Errorf("output %q, want one 'seeded oidc provider <id>' line", out.String())
	}
}

func TestSeedSSOProviderSAMLReadsTheCertFileAndNeedsNoSecret(t *testing.T) {
	cert := filepath.Join(t.TempDir(), "saml-cert.pem")
	if err := os.WriteFile(cert, []byte("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	db, opened := &seedFakeDB{}, 0
	var out bytes.Buffer
	args := []string{"admin@example.com", "saml", "-entity-id", "https://localhost:9443/saml", "-sso-url", "https://localhost:9443/saml/sso",
		"-cert-file", cert, "-sp-entity-id", "urn:example:sp", "-acs-base", "http://127.0.0.1:3002"}
	if err := seedSSOProvider(context.Background(), args, seedEnv(map[string]string{"SSO_CLIENT_SECRET": ""}), &out, fakeOpen(db, &opened)); err != nil {
		t.Fatal(err)
	}
	if len(db.execs) != 1 {
		t.Fatalf("%d inserts, want 1", len(db.execs))
	}
	for _, arg := range db.execs[0] {
		if text, ok := arg.(*string); ok && text != nil && strings.Contains(*text, "client_secret") {
			t.Errorf("a SAML row holds a client_secret object: %q", *text)
		}
	}
}

func TestSeedSSOProviderNeedsTheKeyAndTheOIDCSecret(t *testing.T) {
	for _, env := range []map[string]string{{"SETTINGS_ENCRYPTION_KEY": ""}, {"SSO_CLIENT_SECRET": ""}} {
		db, opened := &seedFakeDB{}, 0
		var out bytes.Buffer
		if err := seedSSOProvider(context.Background(), append([]string{"admin@example.com", "oidc"}, oidcFlags...), seedEnv(env), &out, fakeOpen(db, &opened)); err == nil {
			t.Errorf("env %v accepted", env)
		}
		if opened != 0 || len(db.execs) != 0 {
			t.Errorf("env %v: opened %d, writes %d", env, opened, len(db.execs))
		}
	}
}

// CHAOS-13636 stand-in census: no production command links internal/cigates/livee2e/ssoseed; the cigate binary does. Every
// command of ./cmd/... is named in the cell (dho, auth-service, auth-migrate, the workers' commands and the rest).
func TestSSOSeedOnlyLinkedByCigate(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	const pkg = "github.com/full-chaos/dev-health-ops/internal/cigates/livee2e/ssoseed"
	env := append(os.Environ(), "GOFLAGS=-mod=readonly", "GOPROXY=off", "GOSUMDB=off")
	list := exec.Command("go", "list", "-f", "{{.ImportPath}}", "./cmd/...")
	list.Dir, list.Env = root, env
	raw, err := list.CombinedOutput()
	if err != nil {
		t.Fatalf("go list ./cmd/...: %v\n%s", err, raw)
	}
	const cigate = "github.com/full-chaos/dev-health-ops/cmd/cigate"
	var prod []string
	sawCigate, sawDho, sawAuth := false, false, false
	for _, line := range strings.Fields(string(raw)) {
		switch line {
		case cigate:
			sawCigate = true
			continue
		case "github.com/full-chaos/dev-health-ops/cmd/dho":
			sawDho = true
		case "github.com/full-chaos/dev-health-ops/cmd/auth-service":
			sawAuth = true
		}
		prod = append(prod, line)
	}
	if !sawCigate || !sawDho || !sawAuth || len(prod) < 5 {
		t.Fatalf("command list incomplete: cigate=%v dho=%v auth-service=%v others=%v", sawCigate, sawDho, sawAuth, prod)
	}
	for _, cell := range []struct {
		commands []string
		want     bool
	}{{prod, false}, {[]string{cigate}, true}} {
		cmd := exec.Command("go", append([]string{"list", "-deps"}, cell.commands...)...)
		cmd.Dir, cmd.Env = root, env
		raw, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("go list -deps: %v\n%s", err, raw)
		}
		found := false
		for _, line := range strings.Split(string(raw), "\n") {
			found = found || line == pkg
		}
		if found != cell.want {
			t.Fatalf("ssoseed dependency in %v: got %v, want %v", cell.commands, found, cell.want)
		}
	}
}

// -allowed-domains is stored as the JSON array the provider row holds; unset it stores [] (any domain).
func TestSeedSSOProviderStoresTheAllowedDomains(t *testing.T) {
	for _, tc := range []struct{ flag, want string }{{"", "[]"}, {"example.com, example.org ,", `["example.com","example.org"]`}} {
		db, opened := &seedFakeDB{}, 0
		var out bytes.Buffer
		args := append([]string{"admin@example.com", "oidc"}, oidcFlags...)
		if tc.flag != "" {
			args = append(args, "-allowed-domains", tc.flag)
		}
		if err := seedSSOProvider(context.Background(), args, seedEnv(nil), &out, fakeOpen(db, &opened)); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, arg := range db.execs[0] {
			if text, ok := arg.(*string); ok && text != nil && *text == tc.want {
				found = true
			}
		}
		if !found {
			t.Errorf("flag %q: no allowed_domains argument %s in %v", tc.flag, tc.want, db.execs[0])
		}
	}
}
