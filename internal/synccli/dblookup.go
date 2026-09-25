package synccli

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The two Postgres reads `dev-hops sync <target>` makes before it touches a
// provider (CHAOS-6710), ported 1:1 from the Python verb:
//
//   - no --org and no ORG_ID: dev_health_ops/cli.py _resolve_first_org_id, the
//     oldest organization (`SELECT id FROM organizations ORDER BY created_at
//     ASC LIMIT 1`); any failure is "no organization" (None), never an error;
//   - no --auth / GITHUB_* credential: processors/sync.py
//     _resolve_github_sync_credentials' last step, resolve_credentials_sync
//     ("github", org, db_url, allow_env_fallback=False): the row named
//     "default" of that organization, decrypted with SETTINGS_ENCRYPTION_KEY,
//     built into GitHubCredentials. Every failure inside the lookup (no row,
//     empty payload, key missing or wrong, not JSON, not an object, an unknown
//     field, a key file that cannot be read, a shape GitHubCredentials
//     rejects, a connection that fails) is "no database credentials" and ends
//     in Python's "Missing GitHub credentials ..." message.
//
// Named divergences from Python, each outside the oracle corpus on purpose:
//   - is_active is NOT read (Python's get() does not filter on it either), but
//     a stored value that is not a JSON string (a number, a bool) for a field
//     the credential uses is "no database credentials" here; Python would keep
//     the value and fail later inside the provider call;
//   - SETTINGS_ENCRYPTION_SALT set to the EMPTY string: Python derives the v1
//     key with an empty salt, providerfoundation.FernetDecryptor treats an
//     empty salt as the default;
//   - dialects other than postgresql/mysql/mariadb/oracle/mssql (sqlite, ...):
//     the exception type is unverified and reported as NoSuchModuleError, and
//     the type of a database driver that IS installed in a Python environment
//     beyond pyproject.toml's (psycopg, pg8000, aiomysql) is not what Python
//     raises there;
//   - database URLs with driver-specific query parameters (asyncpg's ssl=...):
//     Python passes them to SQLAlchemy, this port passes them to pgx.

// errNoDBCredential is every "the lookup found nothing usable" outcome.
var errNoDBCredential = errors.New("no usable database credential")

// missingGitHubCredentials is Python's SystemExit text, verbatim, and the same
// refusal BuildPlan raises when neither a credential nor a database is given.
const missingGitHubCredentials = "Missing GitHub credentials (pass --auth, set GITHUB_TOKEN, configure GitHub App flags/env vars, or configure DB credentials)."

// dbLookups is the executor's view of the two Postgres reads.
type dbLookups struct {
	FirstOrg         func(ctx context.Context, dbURL string) (string, bool)
	GitHubCredential func(ctx context.Context, dbURL, orgID string, env cli.Env) (*GitHubCredentials, error)
}

func defaultDBLookups() dbLookups {
	defaults := defaultInlineDeps()
	return dbLookups{FirstOrg: defaults.FirstOrg, GitHubCredential: defaults.GitHubCredential}
}

// postgresDSN is the URL a Python SQLAlchemy sync engine would connect to, or
// false when create_engine would refuse it (an unknown dialect or a driver that
// is not psycopg2): _resolve_first_org_id turns that refusal into "no
// organization".
func syncPostgresDSN(dbURL string) (string, bool) {
	dbURL = strings.Replace(dbURL, "postgresql+asyncpg://", "postgresql://", 1)
	scheme, rest, found := strings.Cut(dbURL, "://")
	if !found {
		return "", false
	}
	switch strings.ToLower(scheme) {
	case "postgresql", "postgresql+psycopg2":
		return "postgresql://" + rest, true
	}
	return "", false
}

// firstOrganization is _resolve_first_org_id. It never returns a driver error:
// its text can carry the DSN.
func firstOrganization(ctx context.Context, dbURL string) (string, bool) {
	if dbURL == "" {
		return "", false
	}
	dsn, ok := syncPostgresDSN(dbURL)
	if !ok {
		return "", false
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "", false
	}
	defer func() { _ = conn.Close(ctx) }()
	var id string
	if err := conn.QueryRow(ctx, "SELECT id::text FROM organizations ORDER BY created_at ASC LIMIT 1").Scan(&id); err != nil {
		return "", false
	}
	return id, true
}

// asyncEngineRefusal names the exception create_async_engine(db_url) raises for
// a URL the credential lookup cannot use. It is raised OUTSIDE the lookup's own
// try/except, so Python ends in a traceback (exit 1) instead of the "Missing
// GitHub credentials" message. "" means the URL is accepted.
func asyncEngineRefusal(dbURL string) string {
	scheme, _, found := strings.Cut(dbURL, "://")
	if !found {
		return "ArgumentError"
	}
	dialect, driver, _ := strings.Cut(scheme, "+")
	switch strings.ToLower(dialect) {
	case "postgresql":
		switch strings.ToLower(driver) {
		case "asyncpg":
			return ""
		case "", "psycopg2":
			return "InvalidRequestError" // the driver is not asyncio
		}
		return "ModuleNotFoundError" // psycopg, pg8000, ...: not in pyproject.toml
	case "mysql", "mariadb", "oracle", "mssql":
		return "ModuleNotFoundError" // the DBAPI is not in pyproject.toml
	}
	return "NoSuchModuleError"
}

// githubDBFields are the keys GitHubCredentials(**dict) accepts.
var githubDBFields = map[string]bool{
	"provider": true, "source": true, "credential_name": true, "extra": true,
	"token": true, "app_id": true, "private_key": true, "private_key_path": true,
	"installation_id": true, "base_url": true,
}

// githubCredentialFromDB is resolve_credentials_sync("github", ...) followed by
// _build_credential and GitHubCredentials.__post_init__. It returns
// errNoDBCredential for every "found nothing usable" outcome, and a *Refusal
// for the one failure Python does not catch.
func githubCredentialFromDB(ctx context.Context, dbURL, orgID string, env cli.Env) (*GitHubCredentials, error) {
	if kind := asyncEngineRefusal(dbURL); kind != "" {
		return nil, &Refusal{Code: cli.ExitFailure, Stage: "error", Type: kind,
			Message: kind + ": the database URL cannot be used for the credential lookup (Python's create_async_engine refuses it)"}
	}
	dsn, ok := asyncpgToPgx(dbURL)
	if !ok {
		return nil, errNoDBCredential
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, errNoDBCredential
	}
	defer func() { _ = conn.Close(ctx) }()
	var ciphertext *string
	err = conn.QueryRow(ctx,
		"SELECT credentials_encrypted FROM integration_credentials WHERE org_id = $1 AND provider = 'github' AND name = 'default'",
		orgID).Scan(&ciphertext)
	if err != nil || ciphertext == nil || *ciphertext == "" {
		return nil, errNoDBCredential
	}
	decryptor, err := settingsDecryptor(env)
	if err != nil || !decryptor.Configured() {
		return nil, errNoDBCredential
	}
	plaintext, err := decryptor.Decrypt(secrets.NewValue(*ciphertext))
	if err != nil {
		return nil, errNoDBCredential
	}
	return githubCredentialFromJSON(plaintext)
}

// asyncpgToPgx is the pgx form of a URL asyncEngineRefusal accepted.
func asyncpgToPgx(dbURL string) (string, bool) {
	_, rest, found := strings.Cut(dbURL, "://")
	return "postgresql://" + rest, found
}

func settingsDecryptor(env cli.Env) (providerfoundation.FernetDecryptor, error) {
	key, _, err := secrets.Resolve("SETTINGS_ENCRYPTION_KEY", env.Lookup)
	if err != nil {
		return providerfoundation.FernetDecryptor{}, err
	}
	salt, _, err := secrets.Resolve("SETTINGS_ENCRYPTION_SALT", env.Lookup)
	if err != nil {
		return providerfoundation.FernetDecryptor{}, err
	}
	if !key.Configured() {
		return providerfoundation.FernetDecryptor{}, nil
	}
	return providerfoundation.NewFernetDecryptor(key, salt.Reveal())
}

// githubCredentialFromJSON is _build_credential + GitHubCredentials.
func githubCredentialFromJSON(plaintext []byte) (*GitHubCredentials, error) {
	var decoded any
	if err := json.Unmarshal(plaintext, &decoded); err != nil {
		return nil, errNoDBCredential
	}
	object, ok := decoded.(map[string]any)
	if !ok {
		return nil, errNoDBCredential
	}
	fields := map[string]string{}
	for key, value := range object {
		if value == nil { // _build_credential drops None values
			continue
		}
		if !githubDBFields[key] {
			return nil, errNoDBCredential // GitHubCredentials(**dict): unexpected keyword
		}
		text, isText := value.(string)
		switch key {
		case "extra", "source", "credential_name", "provider":
			continue
		}
		if !isText {
			return nil, errNoDBCredential
		}
		fields[key] = text
	}
	if object["private_key"] == nil { // "private_key" not in cred_dict (None was dropped)
		if path := fields["private_key_path"]; path != "" {
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, errNoDBCredential
			}
			fields["private_key"] = string(data)
		}
	}
	hasToken := fields["token"] != ""
	hasApp := fields["app_id"] != "" || fields["private_key"] != "" || fields["installation_id"] != ""
	hasCompleteApp := fields["app_id"] != "" && fields["private_key"] != "" && fields["installation_id"] != ""
	if (hasToken && hasApp) || (!hasToken && !hasCompleteApp) {
		return nil, errNoDBCredential
	}
	credential := &GitHubCredentials{Name: "default"}
	if value := fields["base_url"]; value != "" {
		credential.BaseURL = &value
	}
	if hasToken {
		credential.Mode, credential.Token = CredentialPAT, fields["token"]
		return credential, nil
	}
	credential.Mode = CredentialApp
	credential.AppID, credential.PrivateKey, credential.InstallationID = fields["app_id"], fields["private_key"], fields["installation_id"]
	return credential, nil
}

// resolveDBLookups fills what the plan left to the executor: the first
// organization and the database credential.
func resolveDBLookups(ctx context.Context, lookups dbLookups, plan Plan, env cli.Env) (Plan, *Refusal) {
	dbURL := ""
	if plan.DB != nil {
		dbURL = *plan.DB
	}
	wantsDBCredential := plan.GitHub != nil && plan.GitHub.Mode == CredentialDB
	if plan.OrgSource == OrgFromDBFirst {
		id, found := lookups.FirstOrg(ctx, dbURL)
		if !found {
			if wantsDBCredential {
				// ns.org stays None: `if db_url and org_id` is false.
				return plan, exitRefusal(missingGitHubCredentials)
			}
			return plan, notYet(plan, "a sync with no organization (none found in PostgreSQL; pass --org or set ORG_ID)", ticketDBLookups)
		}
		plan.Org, plan.OrgSource = &id, OrgFromDB
	}
	if wantsDBCredential {
		credential, err := lookups.GitHubCredential(ctx, dbURL, *plan.Org, env)
		var refusal *Refusal
		switch {
		case errors.As(err, &refusal):
			return plan, refusal
		case errors.Is(err, errNoDBCredential):
			return plan, exitRefusal(missingGitHubCredentials)
		case err != nil:
			return plan, &Refusal{Code: cli.ExitFailure, Stage: "error", Type: "Error", Message: "database credential lookup failed"}
		}
		plan.GitHub = credential
	}
	return plan, nil
}
