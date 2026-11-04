package venueoracle

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

// A venue runs no Python: the Python plane's answers come from the golden, and
// the substrate the Python plane once built (the schema, the access tokens and
// the seed calls) comes from the Go production code that owns it on a
// deployed build:
//
//   - the PostgreSQL schema: pgmigrate (`dho migrate postgres upgrade`),
//     whose baseline is the executed Alembic chain;
//   - the ClickHouse schema: chmigrate (`dho migrate clickhouse upgrade`);
//   - the access tokens: user access tokens (EdDSA) of the venue's TokenIssuer,
//     each keyed to the caller of AuthService.create_access_token's token;
//   - CallPython: the Go ports in goCallPorts, each the one production code
//     uses for the same value.
//
// The goldens were recorded on the Go-built schema too, so the replay's Go
// plane reads the physical layout the recording's Python plane wrote.

// processEnvAtInit is the test process's environment when this package was
// initialised, before TestMain and before any test ran. A variable that
// differs from it later was set by test code.
var processEnvAtInit = envMap(os.Environ())

func envMap(env []string) map[string]string {
	out := make(map[string]string, len(env))
	for _, entry := range env {
		if name, value, found := strings.Cut(entry, "="); found && name != "" {
			out[name] = value
		}
	}
	return out
}

// testSetEnv is the variables test code set in the process since this package
// was initialised (t.Setenv, os.Setenv; TestMain included), as NAME=VALUE in
// name order.
func testSetEnv() []string {
	var out []string
	for name, value := range envMap(os.Environ()) {
		if earlier, was := processEnvAtInit[name]; was && earlier == value {
			continue
		}
		out = append(out, name+"="+value)
	}
	sort.Strings(out)
	return out
}

// planeEnv is the Python plane's settings the goldens were recorded under, as
// far as a Go port reads them: the harness's fixed entries, the test's JWT key
// (Options.JWTKey) and the test's PythonEnv last (a later entry wins).
func planeEnv(options Options) []string {
	return append([]string{"JWT_SECRET_KEY=" + options.JWTKey, "OTEL_SDK_DISABLED=true", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1",
		"PGBOUNCER_TRANSACTION_MODE=true", "ENVIRONMENT=test"}, options.PythonEnv...)
}

// pythonPlaneLookup is the Python plane's environment as the Go ports read it:
// what test code set in the process, then the venue's own entries, later
// entries winning.
func (v *Venue) pythonPlaneLookup(name string) (string, bool) {
	value, found := "", false
	for _, entry := range append(testSetEnv(), v.pythonEnv...) {
		key, rest, ok := strings.Cut(entry, "=")
		if ok && key == name {
			value, found = rest, true
		}
	}
	return value, found
}

// migratePostgresGo builds the source database's schema with pgmigrate under
// the settings the Python chain ran with (the venue's environment).
func (v *Venue) migratePostgresGo(t *testing.T, ctx context.Context) {
	t.Helper()
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatalf("frozen venue: %v", err)
	}
	if err := pgmigrate.CheckSettings(pgmigrate.ReadSettings(v.pythonPlaneLookup), baseline); err != nil {
		t.Fatalf("frozen venue: %v", err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatalf("frozen venue: %v", err)
	}
	conn, err := pgx.Connect(ctx, v.AdminURI(t, v.SourceDB))
	if err != nil {
		t.Fatalf("frozen venue: connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("frozen venue: migrate postgres: %v", err)
	}
}

// migrateClickHouseGo builds database's schema with chmigrate. The head
// baseline is production's ordering contract, so the contract check is the
// baseline's own.
func (v *Venue) migrateClickHouseGo(t *testing.T, ctx context.Context, database string) {
	t.Helper()
	migrateClickHouseAt(t, ctx, v.AdminClickHouseURI(t, database), database)
}

// MigrateClickHouseGo builds the schema of the ClickHouse database uri names
// with chmigrate (`dho migrate clickhouse upgrade`), with no Python: for a
// frozen or Go-only test that needs a migrated ClickHouse of its own. uri
// must carry an admin login and name its database.
func MigrateClickHouseGo(t *testing.T, ctx context.Context, uri string) {
	t.Helper()
	migrateClickHouseAt(t, ctx, uri, "")
}

// migrateClickHouseAt migrates the database uri names; a non-empty want is
// the database the connection must be on.
func migrateClickHouseAt(t *testing.T, ctx context.Context, uri, want string) {
	t.Helper()
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatalf("go clickhouse schema: %v", err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatalf("go clickhouse schema: %v", err)
	}
	config := chstorage.DefaultConfig(uri)
	config.MaxOpenConns, config.MaxIdleConns = 1, 1
	conn, err := chstorage.Open(ctx, config)
	if err != nil {
		t.Fatalf("go clickhouse schema: open: %v", err)
	}
	defer conn.Close()
	db, current, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatalf("go clickhouse schema: %v", err)
	}
	if current == "" || (want != "" && current != want) {
		t.Fatalf("go clickhouse schema: the connection is on %q, want %q", current, want)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("go clickhouse schema: migrate %s: %v", current, err)
	}
}

// mintKwargs are the create_access_token keyword arguments a seed may name.
// Any other name is refused: the Go minter would silently drop it, and a
// token without a claim the recording carried is a different caller.
var mintKwargs = map[string]bool{
	"user_id": true, "email": true, "org_id": true, "role": true, "is_superuser": true,
	"username": true, "full_name": true, "token_version": true, "impersonating_user_id": true,
}

// mintGo mints the seed's tokens as user access tokens (EdDSA) of the venue's
// TokenIssuer, and records for each the caller the recording's HS256 token
// named: create_access_token's payload for the same arguments under the Python
// plane's issuer and audience (JWT_ISSUER and JWT_AUDIENCE, AuthService's
// defaults when unset), so a replayed request keys the recorded one.
func (v *Venue) mintGo(t *testing.T, specs map[string]map[string]any) map[string]string {
	t.Helper()
	issuer, ok := v.pythonPlaneLookup("JWT_ISSUER")
	if !ok {
		issuer = "dev-health-ops"
	}
	audience, ok := v.pythonPlaneLookup("JWT_AUDIENCE")
	if !ok {
		audience = "dev-health-api"
	}
	if v.TokenIssuer == nil {
		v.TokenIssuer = edtokens.New(t)
	}
	tokens := make(map[string]string, len(specs))
	for name, spec := range specs {
		claims, err := accessClaims(spec)
		if err != nil {
			t.Fatalf("frozen venue: token %q: %v", name, err)
		}
		mint, err := claims.userToken()
		if err != nil {
			t.Fatalf("frozen venue: token %q: %v", name, err)
		}
		token, err := v.TokenIssuer.Mint(mint)
		if err != nil {
			t.Fatalf("frozen venue: token %q: %v", name, err)
		}
		recordedCallers.Store(token, claims.recordedIdentity(issuer, audience))
		tokens[name] = token
	}
	return tokens
}

// recordedCallers maps a minted user access token to the bearer identity of
// the recording's HS256 token for the same caller (bearerIdentity).
var recordedCallers sync.Map

// legacyClaims are create_access_token's keyword arguments, with its defaults.
type legacyClaims struct {
	UserID, Email, OrgID, Role              string
	Username, FullName, ImpersonatingUserID *string
	IsSuperuser                             bool
	TokenVersion                            int64
}

// recordedIdentity is the bearer identity of create_access_token's token for
// these arguments: its header and payload minus the volatile claims.
func (c legacyClaims) recordedIdentity(issuer, audience string) string {
	payload := map[string]any{
		"sub": c.UserID, "email": c.Email, "org_id": c.OrgID, "role": c.Role, "is_superuser": c.IsSuperuser,
		"tv": c.TokenVersion, "type": "access", "iss": issuer, "aud": audience,
	}
	for name, value := range map[string]*string{"username": c.Username, "full_name": c.FullName, "impersonating_user_id": c.ImpersonatingUserID} {
		if value != nil {
			payload[name] = *value
		}
	}
	canonical, _ := json.Marshal(payload)
	return `Bearer header:{"alg":"HS256","typ":"JWT"} claims:` + string(canonical)
}

// userToken is the user access token of the same caller: the user, the
// organization and the impersonating actor. The other arguments are the
// users row's, which the api reads live.
func (c legacyClaims) userToken() (edtokens.Claims, error) {
	user, err := uuid.Parse(c.UserID)
	if err != nil {
		return edtokens.Claims{}, fmt.Errorf("user_id %q is not a UUID: a user access token names a principal", c.UserID)
	}
	out := edtokens.Claims{UserID: user, OrgID: c.OrgID}
	if c.ImpersonatingUserID != nil {
		actor, err := uuid.Parse(*c.ImpersonatingUserID)
		if err != nil {
			return edtokens.Claims{}, fmt.Errorf("impersonating_user_id %q is not a UUID", *c.ImpersonatingUserID)
		}
		out.Actor = &actor
	}
	return out, nil
}

// accessClaims reads create_access_token's keyword arguments, with its
// defaults: org_id "", role "member", is_superuser False, token_version 0.
func accessClaims(spec map[string]any) (legacyClaims, error) {
	claims := legacyClaims{Role: "member"}
	for name := range spec {
		if !mintKwargs[name] {
			return claims, fmt.Errorf("create_access_token argument %q has no Go counterpart", name)
		}
	}
	text := func(name string, required bool) (*string, error) {
		raw, ok := spec[name]
		if !ok || raw == nil {
			if required {
				return nil, fmt.Errorf("%s is required", name)
			}
			return nil, nil
		}
		value, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("%s is %T, want a string", name, raw)
		}
		return &value, nil
	}
	var err error
	var value *string
	if value, err = text("user_id", true); err != nil {
		return claims, err
	}
	claims.UserID = *value
	if value, err = text("email", true); err != nil {
		return claims, err
	}
	claims.Email = *value
	if value, err = text("org_id", false); err != nil {
		return claims, err
	} else if value != nil {
		claims.OrgID = *value
	}
	if value, err = text("role", false); err != nil {
		return claims, err
	} else if value != nil {
		claims.Role = *value
	}
	if claims.Username, err = text("username", false); err != nil {
		return claims, err
	}
	if claims.FullName, err = text("full_name", false); err != nil {
		return claims, err
	}
	if claims.ImpersonatingUserID, err = text("impersonating_user_id", false); err != nil {
		return claims, err
	}
	if raw, ok := spec["is_superuser"]; ok {
		flag, ok := raw.(bool)
		if !ok {
			return claims, fmt.Errorf("is_superuser is %T, want a bool", raw)
		}
		claims.IsSuperuser = flag
	}
	if raw, ok := spec["token_version"]; ok {
		switch number := raw.(type) {
		case int:
			claims.TokenVersion = int64(number)
		case int64:
			claims.TokenVersion = number
		default:
			return claims, fmt.Errorf("token_version is %T, want an int", raw)
		}
	}
	return claims, nil
}

// goCallPort answers one CallPython target with the Go production code for
// the same value. Its result is the target's return value as Python's
// json.dumps writes it.
type goCallPort func(v *Venue, call PythonCall) (json.RawMessage, error)

// goCallPorts are the CallPython targets a frozen venue can answer. A target
// not listed fails the test naming it: a frozen venue never runs Python.
var goCallPorts = map[string]goCallPort{
	"dev_health_ops.core.encryption:encrypt_value": func(v *Venue, call PythonCall) (json.RawMessage, error) {
		plaintext, err := oneString(call)
		if err != nil {
			return nil, err
		}
		cipher, err := v.fernet()
		if err != nil {
			return nil, err
		}
		ciphertext, err := cipher.Encrypt([]byte(plaintext))
		if err != nil {
			return nil, err
		}
		return pythonString(ciphertext.Reveal()), nil
	},
	"dev_health_ops.core.encryption:decrypt_value": func(v *Venue, call PythonCall) (json.RawMessage, error) {
		ciphertext, err := oneString(call)
		if err != nil {
			return nil, err
		}
		cipher, err := v.fernet()
		if err != nil {
			return nil, err
		}
		plaintext, err := cipher.Decrypt(secrets.NewValue(ciphertext))
		if err != nil {
			return nil, fmt.Errorf("decrypt_value: %w", err)
		}
		return pythonString(string(plaintext)), nil
	},
	"dev_health_ops.api.services.users:_hash_password": func(_ *Venue, call PythonCall) (json.RawMessage, error) {
		password, err := oneString(call)
		if err != nil {
			return nil, err
		}
		hash, err := passwordhash.Hash(password)
		if err != nil {
			return nil, err
		}
		return pythonString(hash), nil
	},
}

// fernet is core/encryption.py's cipher under the Python plane's
// SETTINGS_ENCRYPTION_KEY and SETTINGS_ENCRYPTION_SALT. Without a key it is
// an error, as encrypt_value and decrypt_value raise.
func (v *Venue) fernet() (providerfoundation.FernetDecryptor, error) {
	key, _ := v.pythonPlaneLookup("SETTINGS_ENCRYPTION_KEY")
	if key == "" {
		return providerfoundation.FernetDecryptor{}, fmt.Errorf("SETTINGS_ENCRYPTION_KEY is not set on the Python plane")
	}
	salt, _ := v.pythonPlaneLookup("SETTINGS_ENCRYPTION_SALT")
	return providerfoundation.NewFernetDecryptor(secrets.NewValue(key), salt)
}

// oneString is the single positional string argument of call.
func oneString(call PythonCall) (string, error) {
	if len(call.Args) != 1 || len(call.Kwargs) != 0 {
		return "", fmt.Errorf("%s takes one positional argument here, got %d and %d keyword arguments", call.Target, len(call.Args), len(call.Kwargs))
	}
	value, ok := call.Args[0].(string)
	if !ok {
		return "", fmt.Errorf("%s: argument is %T, want a string", call.Target, call.Args[0])
	}
	return value, nil
}

// pythonString is json.dumps(value).
func pythonString(value string) json.RawMessage {
	return json.RawMessage(pythonparity.AppendPythonJSONString(nil, value))
}

// callGo answers calls with goCallPorts, in order.
func (v *Venue) callGo(t *testing.T, calls []PythonCall) []json.RawMessage {
	t.Helper()
	out, err := v.callGoErr(calls)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func (v *Venue) callGoErr(calls []PythonCall) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, len(calls))
	for index, call := range calls {
		port, ok := goCallPorts[call.Target]
		if !ok {
			return nil, fmt.Errorf("frozen venue: CallPython target %s has no Go port (goCallPorts): a frozen venue runs no Python", call.Target)
		}
		result, err := port(v, call)
		if err != nil {
			return nil, fmt.Errorf("frozen venue: %s: %w", call.Target, err)
		}
		out[index] = result
	}
	return out, nil
}
