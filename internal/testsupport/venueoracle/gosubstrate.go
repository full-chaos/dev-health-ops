package venueoracle

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// A frozen venue (Options.Golden set, not recording, or Options.GoOnly) runs
// no Python: the Python plane's answers come from the golden, so the only
// Python the venue ran before was its substrate -- the schema, the access
// tokens and the seed calls. Each is replaced here by the Go production code
// that already owns it on a deployed build:
//
//   - the PostgreSQL schema: pgmigrate (`dho migrate postgres upgrade`),
//     whose baseline is the executed Alembic chain;
//   - the ClickHouse schema: chmigrate (`dho migrate clickhouse upgrade`);
//   - the access tokens: edgetoken.Signer, the Go port of
//     AuthService.create_access_token (pinned byte for byte by
//     edgetoken's live oracle);
//   - CallPython: the Go ports in goCallPorts, each the one production code
//     uses for the same value.
//
// The schemas come from the Go migrators while recording too. pgmigrate
// builds the same schema as the Alembic chain (pgmigrate's
// TestBaselineIsTheFrozenPythonUpgrade compares them), but not
// the same physical tables: the chain's history leaves dropped columns in a
// table's tuples (sync_configurations keeps one), which moves where an
// updated row lands, so an unordered read after writes returns another
// order. Recording on the Go-built schema gives the Python plane the layout
// the replay's Go plane reads, as production does since the Python chain
// stopped running.
//
// A recording run keeps the pinned build's Python for the tokens and the seed
// calls, so a frozen replay checks the Go minter and the Go ports against
// it: a token whose claims differ changes the request key and fails naming
// the request, and a seed value that differs changes the answers or the
// frozen rows.

// pythonPlaneLookup is the Python plane's environment as its process sees it:
// what test code set in the process, then the venue's own entries, later
// entries winning (pythonChildEnv).
func (v *Venue) pythonPlaneLookup(name string) (string, bool) {
	value, found := "", false
	for _, entry := range v.pythonChildEnv() {
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

// mintGo mints the seed's tokens with the Go minter under the Python plane's
// issuer and audience (JWT_ISSUER and JWT_AUDIENCE, AuthService's defaults
// when unset).
func (v *Venue) mintGo(t *testing.T, key string, specs map[string]map[string]any) map[string]string {
	t.Helper()
	issuer, ok := v.pythonPlaneLookup("JWT_ISSUER")
	if !ok {
		issuer = "dev-health-ops"
	}
	audience, ok := v.pythonPlaneLookup("JWT_AUDIENCE")
	if !ok {
		audience = "dev-health-api"
	}
	signer, err := edgetoken.NewSigner(key, issuer, audience)
	if err != nil {
		t.Fatalf("frozen venue: %v", err)
	}
	now := time.Now()
	tokens := make(map[string]string, len(specs))
	for name, spec := range specs {
		claims, err := accessClaims(spec)
		if err != nil {
			t.Fatalf("frozen venue: token %q: %v", name, err)
		}
		token, err := signer.Access(claims, now, uuid.NewString())
		if err != nil {
			t.Fatalf("frozen venue: token %q: %v", name, err)
		}
		tokens[name] = token
	}
	return tokens
}

// accessClaims reads create_access_token's keyword arguments, with its
// defaults: org_id "", role "member", is_superuser False, token_version 0.
func accessClaims(spec map[string]any) (edgetoken.AccessClaims, error) {
	claims := edgetoken.AccessClaims{Role: "member"}
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
