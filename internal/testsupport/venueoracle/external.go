package venueoracle

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// ExternalStores are the datastores of a target that already runs (go-api or auth-service): the venue uses
// them as its Go side instead of starting containers, and the target owns their schema and roles. Each URI
// names the database the target uses; the Valkey URI is the database its rate limits count in.
type ExternalStores struct {
	PostgresURI, ClickHouseURI, ValkeyURI string
}

// Environment names shared with the session corpus (sessionscenario.Env*).
const (
	EnvExternalPostgres   = "DEVHEALTH_AUTH_ORACLE_POSTGRES_URI"
	EnvExternalClickHouse = "DEVHEALTH_AUTH_ORACLE_CLICKHOUSE_URI"
	EnvExternalValkey     = "DEVHEALTH_AUTH_ORACLE_VALKEY_URI"
	// The token issuer the target trusts (its AUTH_ACCESS_TOKEN_ISSUER, auth-service's AUTH_TOKEN_ISSUER_URL; the same
	// name as authrows.EnvSessionIssuer), the path of its Ed25519 signing key (PKCS#8 PEM, a mode 0600 file) and its key id.
	EnvExternalIssuer         = "DEVHEALTH_AUTH_ORACLE_SESSION_ISSUER"
	EnvExternalSigningKeyFile = "DEVHEALTH_AUTH_ORACLE_SIGNING_KEY_FILE"
	EnvExternalSigningKeyID   = "DEVHEALTH_AUTH_ORACLE_SIGNING_KEY_ID"
)

// externalTokenIssuer is the issuer of the venue's tokens on an external target: the target's own token issuer, by
// the key file the environment names. The target verifies user access tokens (EdDSA) only, so the venue mints them
// under that issuer's key; the key never leaves the file and the process.
func externalTokenIssuer() (*edtokens.Issuer, error) {
	issuer, path, kid := os.Getenv(EnvExternalIssuer), os.Getenv(EnvExternalSigningKeyFile), os.Getenv(EnvExternalSigningKeyID)
	if issuer == "" || path == "" || kid == "" {
		return nil, fmt.Errorf("an external target needs its token issuer: set %s, %s and %s", EnvExternalIssuer, EnvExternalSigningKeyFile, EnvExternalSigningKeyID)
	}
	key, err := readSigningKey(path)
	if err != nil {
		return nil, err
	}
	return edtokens.FromKey(key, kid, issuer)
}

// readSigningKey reads an Ed25519 private key (PKCS#8 PEM) from path, which must be a regular file of mode 0600.
// No error carries the file's content.
func readSigningKey(path string) (ed25519.PrivateKey, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("signing key file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("signing key file %s is not a regular file", path)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		return nil, fmt.Errorf("signing key file %s has mode %04o, want 0600", path, perm)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("signing key file: %w", err)
	}
	block, rest := pem.Decode(raw)
	if block == nil || block.Type != "PRIVATE KEY" || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, fmt.Errorf("signing key file %s is not one PKCS#8 PEM block", path)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("signing key file %s is not PKCS#8", path)
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("signing key file %s is not an Ed25519 key", path)
	}
	return key, nil
}

// Validate is an error unless the stores can stand in for the venue's Go side. frozen is false for a venue
// that runs Python: the Python plane needs its own copy of the stores, which an external target cannot give.
func (e ExternalStores) Validate(frozen bool) error {
	if !frozen {
		return fmt.Errorf("external stores need a frozen golden or a Go-only venue: a live Python plane needs its own copy")
	}
	if e.PostgresURI == "" || e.ClickHouseURI == "" || e.ValkeyURI == "" {
		return fmt.Errorf("external stores need all of %s, %s and %s", EnvExternalPostgres, EnvExternalClickHouse, EnvExternalValkey)
	}
	if _, err := e.PostgresDatabase(); err != nil {
		return err
	}
	if _, err := e.ClickHouseDatabase(); err != nil {
		return err
	}
	if parsed, err := valkeygo.ParseURL(e.ValkeyURI); err != nil || parsed.InitAddress == nil {
		return fmt.Errorf("valkey URI is not a redis:// or valkey:// URL: %v", err)
	}
	return nil
}

// singleSegment is the database name of a URL path, which must hold exactly one segment.
func singleSegment(path string) (string, error) {
	database := strings.TrimPrefix(path, "/")
	if database == "" || strings.Contains(database, "/") {
		return "", fmt.Errorf("URI path %q is not exactly one database name", path)
	}
	return database, nil
}

// PostgresDatabase is the database the Postgres URI names.
func (e ExternalStores) PostgresDatabase() (string, error) {
	config, err := pgx.ParseConfig(e.PostgresURI)
	if err != nil {
		return "", fmt.Errorf("postgres URI: %w", err)
	}
	if strings.Contains(e.PostgresURI, "://") {
		parsed, err := url.Parse(e.PostgresURI)
		if err != nil {
			return "", fmt.Errorf("postgres URI: %w", err)
		}
		if _, err := singleSegment(parsed.Path); err != nil {
			return "", fmt.Errorf("postgres %w", err)
		}
	}
	if config.Database == "" {
		return "", fmt.Errorf("postgres URI names no database")
	}
	return config.Database, nil
}

// ClickHouseDatabase is the database the ClickHouse URI names.
func (e ExternalStores) ClickHouseDatabase() (string, error) {
	parsed, err := url.Parse(e.ClickHouseURI)
	if err != nil || parsed.Host == "" {
		return "", fmt.Errorf("clickhouse URI %q is not a URL with a host", e.ClickHouseURI)
	}
	if parsed.Scheme != "clickhouse" && parsed.Scheme != "tcp" {
		return "", fmt.Errorf("clickhouse URI scheme %q is not the native protocol (clickhouse:// or tcp://)", parsed.Scheme)
	}
	database, err := singleSegment(parsed.Path)
	if err != nil {
		return "", fmt.Errorf("clickhouse %w", err)
	}
	options, err := clickhousego.ParseDSN(e.ClickHouseURI)
	if err != nil {
		return "", fmt.Errorf("clickhouse URI: %w", err)
	}
	if options.Auth.Database != database {
		return "", fmt.Errorf("clickhouse URI selects database %q by query but names %q by path", options.Auth.Database, database)
	}
	return database, nil
}

// ExternalFromEnv reads the stores from the environment; ok is false when none is set.
func ExternalFromEnv() (ExternalStores, bool) {
	e := ExternalStores{PostgresURI: os.Getenv(EnvExternalPostgres), ClickHouseURI: os.Getenv(EnvExternalClickHouse), ValkeyURI: os.Getenv(EnvExternalValkey)}
	return e, e != (ExternalStores{})
}

// startExternal builds a frozen venue whose Go side is the external stores: no container, no Python, no
// template copy, no roles. The seed and the minted tokens are the same as in a built venue.
func startExternal(t *testing.T, ctx context.Context, options Options, v *Venue) *Venue {
	t.Helper()
	stores := *options.External
	if err := stores.Validate(v.frozen); err != nil {
		t.Fatalf("venue: %v", err)
	}
	postgresDB, _ := stores.PostgresDatabase()
	clickHouseDB, _ := stores.ClickHouseDatabase()
	v.postgresURI, v.clickHouseURI, v.ValkeyURI, v.PythonValkeyURI = stores.PostgresURI, stores.ClickHouseURI, stores.ValkeyURI, stores.ValkeyURI
	v.SourceDB, v.GoDB, v.PythonClickHouseDB, v.GoClickHouseDB = postgresDB, postgresDB, clickHouseDB, clickHouseDB
	admin := OpenPool(t, ctx, stores.PostgresURI)
	var specs map[string]map[string]any
	if options.Seed != nil {
		specs = options.Seed(t, ctx, admin, v)
	}
	if len(specs) > 0 {
		// The target verifies user access tokens of its own issuer only and checks each token's session live: mint
		// under that issuer's key and write the sessions into the target's auth schema.
		issuer, err := externalTokenIssuer()
		if err != nil {
			t.Fatalf("venue: %v", err)
		}
		v.TokenIssuer = issuer
		v.Tokens = v.mintGo(t, specs)
		writeSessions(t, ctx, admin, v.TokenIssuer)
	}
	return v
}

// targetEnvScript is what an external run calls, as `./ci/auth_corpus_set_env.sh NAME VALUE` (empty VALUE = unset)
// in targetEnvDir, to change the environment of the target it drives (it reconfigures or restarts the target).
const targetEnvScript = "./ci/auth_corpus_set_env.sh"

// targetEnvDir is the repository root the script runs in; the tests of this package point it at a temporary tree.
var targetEnvDir string

// limiterKeyPattern matches the keys of the go-api rate-limit store (ratelimitvalkey keyPrefix "dho:rl:").
const limiterKeyPattern = "dho:rl:*"

// ResetLimiterKeys deletes the rate-limit keys on the Valkey at uri and nothing else: an external target's
// Valkey may hold other data.
func ResetLimiterKeys(t *testing.T, ctx context.Context, uri string) {
	t.Helper()
	options, err := valkeygo.ParseURL(uri)
	if err != nil {
		t.Fatal(err)
	}
	client, err := valkeygo.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var cursor uint64
	for {
		entry, err := client.Do(ctx, client.B().Scan().Cursor(cursor).Match(limiterKeyPattern).Count(1000).Build()).AsScanEntry()
		if err != nil {
			t.Fatal(err)
		}
		if len(entry.Elements) > 0 {
			if err := client.Do(ctx, client.B().Del().Key(entry.Elements...).Build()).Error(); err != nil {
				t.Fatal(err)
			}
		}
		if cursor = entry.Cursor; cursor == 0 {
			return
		}
	}
}

// ExternalRequired is the external stores a run against baseURL needs: none (ok false) when baseURL is empty, an
// error when it is set and the stores in the environment are missing or unusable.
func ExternalRequired(baseURL string) (ExternalStores, bool, error) {
	if baseURL == "" {
		return ExternalStores{}, false, nil
	}
	stores, ok := ExternalFromEnv()
	if !ok {
		return ExternalStores{}, false, fmt.Errorf("a base URL is set but the target's stores are not: set %s, %s and %s", EnvExternalPostgres, EnvExternalClickHouse, EnvExternalValkey)
	}
	if err := stores.Validate(true); err != nil {
		return ExternalStores{}, false, err
	}
	if parsed, err := url.Parse(baseURL); err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return ExternalStores{}, false, fmt.Errorf("base URL %q is not an http(s) URL with a host", baseURL)
	}
	if err := checkTargetEnvScript(); err != nil {
		return ExternalStores{}, false, err
	}
	if _, err := externalTokenIssuer(); err != nil {
		return ExternalStores{}, false, err
	}
	return stores, true, nil
}

// SetTargetEnv sets (or, with an empty value, unsets) name for the target the corpus drives: in this process for an
// in-process go-api, and for an external target also through ./ci/auth_corpus_set_env.sh NAME VALUE (run in the
// repository root, no shell; an empty VALUE means unset), which reconfigures or restarts the target. The process
// environment follows in both cases because the frozen golden keys each call by it.
func SetTargetEnv(t *testing.T, external bool, name, value string) {
	t.Helper()
	if external {
		command := exec.Command("./ci/auth_corpus_set_env.sh", name, value)
		command.Dir = targetEnvRoot()
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("%s %s: %v\n%s", targetEnvScript, name, err, output)
		}
	}
	if value == "" {
		t.Setenv(name, "")
		_ = os.Unsetenv(name)
		return
	}
	t.Setenv(name, value)
}

// targetEnvRoot is the directory the script runs in: the repository root unless a test of this package set another.
func targetEnvRoot() string {
	if targetEnvDir != "" {
		return targetEnvDir
	}
	_, file, _, _ := moduleroot.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "..")
}

// checkTargetEnvScript is an error unless the script exists and is executable.
func checkTargetEnvScript() error {
	path := filepath.Join(targetEnvRoot(), targetEnvScript)
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("an external run needs %s: the corpus changes the target's environment mid-run: %w", targetEnvScript, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("%s is not an executable file", path)
	}
	return nil
}

// WithoutProvenanceHeaders is response without the headers a running dho api stamps on every answer to name its
// plane and build (x-dev-health-plane, x-dev-health-build). The frozen goldens were recorded against the route set
// served without that stamp, so a golden replay of an external target must not see them; the api's own tests hold
// the stamp (internal/apiservice, internal/api/buildinfo).
func WithoutProvenanceHeaders(response Response) Response {
	headers := make(map[string]string, len(response.Headers))
	for name, value := range response.Headers {
		if lower := strings.ToLower(name); lower == "x-dev-health-plane" || lower == "x-dev-health-build" {
			continue
		}
		headers[name] = value
	}
	response.Headers = headers
	return response
}
