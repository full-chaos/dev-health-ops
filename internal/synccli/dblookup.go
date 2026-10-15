package synccli

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
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
// Named divergences from Python, each outside the oracle corpus on purpose
// (the ones of CHAOS-9678 are inside it as named entries, see
// dbLookupNamedDivergences):
//   - the credential is the one the org's ACTIVE github integration points at
//     (credential_id), read by the worker's own repository, not the row named
//     "default" (prod rows are not so named); no, no-credential or several
//     active integrations are typed refusals;
//   - the payload is decoded by the WORKER's decode and shape rule
//     (providerfoundation.DecodeStoredGitHubCredential, one shared function): a
//     payload key the credential does not use is IGNORED (its name is logged at
//     Warn), the camelCase spellings are aliased, and a value that is not a
//     string is read the way Python's str() reads it; Python's sync verb refused
//     the first two (_build_credential, GitHubCredentials(**dict)), which made
//     every credential the web saved (with an `org` key) unusable for the verb;
//   - is_active is NOT read (Python's get() does not filter on it either);
//   - SETTINGS_ENCRYPTION_SALT set to the EMPTY string: Python derives the v1
//     key with an empty salt, providerfoundation.FernetDecryptor treats an
//     empty salt as the default;
//   - dialects other than postgresql/mysql/mariadb/oracle/mssql (sqlite, ...):
//     the exception type is unverified and reported as NoSuchModuleError, and
//     the type of a database driver that IS installed in a Python environment
//     beyond pyproject.toml's (psycopg, pg8000, aiomysql) is not what Python
//     raises there;
//   - database URL query options: the first-organization read accepts libpq's
//     keywords (an unknown one is "none", as in Python) and the credential
//     read accepts asyncpg's `ssl` and `command_timeout` only (Python passes
//     every option to asyncpg.connect, whose other keyword arguments, such as
//     passfile or direct_tls, would work there and are refused here); an
//     option valid for both drivers but not modelled reaches neither;

// errNoDBCredential is every "the lookup found nothing usable" outcome.
var errNoDBCredential = errors.New("no usable database credential")

// The reasons a stored-credential lookup finds nothing usable (CHAOS-9678). The
// Python verb ends every one of them in the one "Missing GitHub credentials"
// text, and so does this one (the exit text is the ported contract); each reason
// is its own value, and githubCredentialFromDB logs it, so an operator can tell a
// wrong key from a missing row from a payload field the verb does not accept.
const (
	reasonDatabaseURLRefused     = "database_url_refused"
	reasonDatabaseURLNotUsable   = "database_url_not_usable"
	reasonDatabaseConnectFailed  = "database_connect_failed"
	reasonDatabaseQueryFailed    = "database_query_failed"
	reasonCredentialRowNotFound  = "credential_row_not_found"
	reasonCiphertextEmpty        = "credential_ciphertext_empty"
	reasonEncryptionKeyNotSet    = "encryption_key_not_configured"
	reasonEncryptionKeyUnusable  = "encryption_key_not_resolvable"
	reasonDecryptFailed          = "credential_decrypt_failed"
	reasonPayloadNotJSON         = "credential_payload_not_json"
	reasonPayloadNotObject       = "credential_payload_not_object"
	reasonPrivateKeyPathUnread   = "credential_private_key_path_unreadable"
	reasonCredentialShapeInvalid = "credential_shape_invalid"

	reasonGitHubIntegrationNotFound     = "github_integration_not_found"
	reasonGitHubIntegrationNoCredential = "github_integration_no_credential"
	reasonGitHubIntegrationsAmbiguous   = "github_integrations_ambiguous"
	reasonCredentialConfigInvalid       = "credential_config_invalid"
)

// credentialLookupFailure is one reason the lookup found nothing usable. It is
// an errNoDBCredential (errors.Is), so every caller that maps "nothing usable"
// keeps doing so. Class and Type are the error class and Go type of a driver or
// decryptor error, never its text; HasToken and HasApp say what a refused
// payload held.
type credentialLookupFailure struct {
	Reason, Class, Type string
	HasToken, HasApp    bool
	IntegrationIDs      []string
}

func (failure credentialLookupFailure) Error() string {
	return errNoDBCredential.Error() + ": " + failure.Reason
}

func (failure credentialLookupFailure) Is(target error) bool { return target == errNoDBCredential }

func lookupFailure(reason string) credentialLookupFailure {
	return credentialLookupFailure{Reason: reason}
}

func lookupFailureWith(reason string, cause error) credentialLookupFailure {
	return credentialLookupFailure{Reason: reason, Class: logging.ErrorClass(cause), Type: logging.ErrorType(cause)}
}

const (
	credentialLookupFailedEvent  = "synccli.github_credential_lookup_failed"
	credentialFieldsIgnoredEvent = "synccli.github_credential_fields_ignored"
)

// logCredentialLookupFailure writes the Warn line for a failure of the stored
// credential lookup: the reason and what names it, never a value.
func logCredentialLookupFailure(ctx context.Context, orgID string, failure credentialLookupFailure) {
	slog.WarnContext(ctx, credentialLookupFailedEvent,
		"org_id", orgID, "reason", failure.Reason,
		"error_class", failure.Class, "error_type", failure.Type,
		"payload_holds", failure.payloadHolds(),
		"integration_ids", strings.Join(failure.IntegrationIDs, ","))
}

// payloadHolds says what a refused payload held, as a word: the production logger
// redacts every attribute whose NAME says token, key or secret, so the
// flags are one value of a neutral attribute.
func (failure credentialLookupFailure) payloadHolds() string {
	switch {
	case failure.HasToken && failure.HasApp:
		return "token_and_app"
	case failure.HasToken:
		return "token"
	case failure.HasApp:
		return "app"
	}
	return "nothing"
}

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

// libpqKeywords are the connection options a libpq (psycopg2) URL accepts in
// its query; SQLAlchemy's psycopg2 dialect hands the rest to libpq, which
// rejects an unknown one ("invalid connection option") and so ends the
// first-organization lookup as "none".
var libpqKeywords = map[string]bool{
	"application_name": true, "channel_binding": true, "client_encoding": true, "connect_timeout": true,
	"fallback_application_name": true, "gssencmode": true, "gsslib": true, "hostaddr": true,
	"keepalives": true, "keepalives_count": true, "keepalives_idle": true, "keepalives_interval": true,
	"krbsrvname": true, "load_balance_hosts": true, "options": true, "passfile": true, "replication": true,
	"requirepeer": true, "service": true, "sslcert": true, "sslcompression": true, "sslcrl": true,
	"sslcrldir": true, "sslkey": true, "sslmode": true, "sslpassword": true, "sslrootcert": true,
	"sslsni": true, "ssl_max_protocol_version": true, "ssl_min_protocol_version": true,
	"target_session_attrs": true, "tcp_user_timeout": true,
}

// syncPostgresDSN is the URL a Python SQLAlchemy sync engine (psycopg2) would
// connect to, or false when create_engine or libpq would refuse it (an unknown
// dialect, a driver that is not psycopg2, a fragment, an option libpq does not
// know): _resolve_first_org_id turns every such failure into "no organization".
func syncPostgresDSN(dbURL string) (string, bool) {
	dbURL = strings.Replace(dbURL, "postgresql+asyncpg://", "postgresql://", 1)
	scheme, rest, found := strings.Cut(dbURL, "://")
	if !found {
		return "", false
	}
	switch strings.ToLower(scheme) {
	case "postgresql", "postgresql+psycopg2":
	default:
		return "", false
	}
	parsed, err := url.Parse("postgresql://" + rest)
	if err != nil || parsed.Fragment != "" || strings.Contains(rest, "#") {
		return "", false
	}
	for key := range parsed.Query() {
		if !libpqKeywords[key] {
			return "", false
		}
	}
	return parsed.String(), true
}

// firstOrganization is _resolve_first_org_id. It never returns a driver error:
// its text can carry the DSN, so a failure is logged by its type and class only.
// An empty organizations table is not a failure and is not logged.
func firstOrganization(ctx context.Context, dbURL string) (string, bool) {
	if dbURL == "" {
		return "", false
	}
	dsn, ok := syncPostgresDSN(dbURL)
	if !ok {
		logCredentialLookupFailure(ctx, "", lookupFailure(reasonDatabaseURLNotUsable))
		return "", false
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		logCredentialLookupFailure(ctx, "", lookupFailureWith(reasonDatabaseConnectFailed, err))
		return "", false
	}
	defer func() { _ = conn.Close(ctx) }()
	var id string
	if err := conn.QueryRow(ctx, "SELECT id::text FROM organizations ORDER BY created_at ASC LIMIT 1").Scan(&id); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			logCredentialLookupFailure(ctx, "", lookupFailureWith(reasonDatabaseQueryFailed, err))
		}
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

// githubCredentialFromDB is resolve_credentials_sync("github", ...) followed by
// _build_credential and GitHubCredentials.__post_init__. It returns
// errNoDBCredential for every "found nothing usable" outcome, and a *Refusal
// for the one failure Python does not catch.
func githubCredentialFromDB(ctx context.Context, dbURL, orgID string, env cli.Env) (*GitHubCredentials, error) {
	if kind := asyncEngineRefusal(dbURL); kind != "" {
		// The ported refusal (Python's create_async_engine raises outside the lookup's own
		// try/except) keeps its text and exit; it is logged like every other failure.
		logCredentialLookupFailure(ctx, orgID, credentialLookupFailure{Reason: reasonDatabaseURLRefused, Type: kind})
		return nil, &Refusal{Code: cli.ExitFailure, Stage: "error", Type: kind,
			Message: kind + ": the database URL cannot be used for the credential lookup (Python's create_async_engine refuses it)"}
	}
	credential, err := storedGitHubCredential(ctx, dbURL, orgID, env)
	var failure credentialLookupFailure
	if errors.As(err, &failure) {
		logCredentialLookupFailure(ctx, orgID, failure)
	}
	return credential, err
}

// storedGitHubCredential is the lookup itself: every branch that finds nothing
// usable returns its own credentialLookupFailure. The credential is the one the
// org's ACTIVE github integration points at (credential_id), read through the
// worker's own providerfoundation.PostgresCredentialRepository; no credential
// name is assumed (CHAOS-9678: prod rows are not named "default").
func storedGitHubCredential(ctx context.Context, dbURL, orgID string, env cli.Env) (*GitHubCredentials, error) {
	dsn, ok := asyncpgToPgx(dbURL)
	if !ok {
		return nil, lookupFailure(reasonDatabaseURLNotUsable)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, lookupFailureWith(reasonDatabaseConnectFailed, err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return nil, lookupFailureWith(reasonDatabaseConnectFailed, err)
	}
	integrationID, credentialID, err := activeGitHubIntegration(ctx, pool, orgID)
	if err != nil {
		return nil, err
	}
	record, err := providerfoundation.PostgresCredentialRepository{Pool: pool}.ResolveEncrypted(ctx, providerfoundation.TenantScope{
		OrgID: orgID, Provider: "github", IntegrationID: integrationID, CredentialID: credentialID,
	})
	switch {
	case errors.Is(err, providerfoundation.ErrCredentialNotFound):
		failure := lookupFailure(reasonCredentialRowNotFound)
		failure.IntegrationIDs = []string{integrationID}
		return nil, failure
	case err != nil:
		failure := lookupFailureWith(reasonCredentialConfigInvalid, err)
		failure.IntegrationIDs = []string{integrationID}
		return nil, failure
	case !record.Ciphertext.Configured():
		failure := lookupFailure(reasonCiphertextEmpty)
		failure.IntegrationIDs = []string{integrationID}
		return nil, failure
	}
	return githubCredentialFromCiphertext(ctx, record.Ciphertext.Reveal(), orgID, env)
}

// activeGitHubIntegration returns the id and credential id of the org's one
// active github integration. It refuses, with a typed reason that names the
// integration ids, when there is none, when it has no stored credential, and
// when there are several (the verb does not guess).
func activeGitHubIntegration(ctx context.Context, pool *pgxpool.Pool, orgID string) (integrationID, credentialID string, err error) {
	rows, err := pool.Query(ctx,
		`SELECT id::text, provider, COALESCE(credential_id::text, '') FROM integrations
		 WHERE org_id = $1 AND is_active = TRUE ORDER BY id`, orgID)
	if err != nil {
		return "", "", lookupFailureWith(reasonDatabaseQueryFailed, err)
	}
	defer rows.Close()
	var ids, credentialIDs []string
	for rows.Next() {
		var id, provider, credential string
		if err := rows.Scan(&id, &provider, &credential); err != nil {
			return "", "", lookupFailureWith(reasonDatabaseQueryFailed, err)
		}
		// The provider column is unconstrained text: the workers compare it in Go through the shared
		// normalizer, never with a SQL predicate, and so does this.
		if providerfoundation.NormalizeSourceKey(provider) != "github" {
			continue
		}
		ids, credentialIDs = append(ids, id), append(credentialIDs, credential)
	}
	if err := rows.Err(); err != nil {
		return "", "", lookupFailureWith(reasonDatabaseQueryFailed, err)
	}
	switch {
	case len(ids) == 0:
		return "", "", lookupFailure(reasonGitHubIntegrationNotFound)
	case len(ids) > 1:
		failure := lookupFailure(reasonGitHubIntegrationsAmbiguous)
		failure.IntegrationIDs = ids
		return "", "", failure
	case credentialIDs[0] == "":
		failure := lookupFailure(reasonGitHubIntegrationNoCredential)
		failure.IntegrationIDs = ids
		return "", "", failure
	}
	return ids[0], credentialIDs[0], nil
}

// githubCredentialFromCiphertext decrypts the stored payload and builds the
// credential from it: the part of the lookup after the database read.
func githubCredentialFromCiphertext(ctx context.Context, ciphertext, orgID string, env cli.Env) (*GitHubCredentials, error) {
	decryptor, err := settingsDecryptor(env)
	if err != nil {
		return nil, lookupFailureWith(reasonEncryptionKeyUnusable, err)
	}
	if !decryptor.Configured() {
		return nil, lookupFailure(reasonEncryptionKeyNotSet)
	}
	plaintext, err := decryptor.Decrypt(secrets.NewValue(ciphertext))
	if err != nil {
		return nil, lookupFailureWith(reasonDecryptFailed, err)
	}
	credential, ignored, err := githubCredentialFromJSON(plaintext)
	if len(ignored) > 0 {
		slog.WarnContext(ctx, credentialFieldsIgnoredEvent, "org_id", orgID, "fields", strings.Join(ignored, ","))
	}
	return credential, err
}

// asyncpgSSLModes are the string values asyncpg accepts for its ssl option,
// which SQLAlchemy's asyncpg dialect passes through as a keyword argument.
var asyncpgSSLModes = map[string]bool{"disable": true, "allow": true, "prefer": true, "require": true, "verify-ca": true, "verify-full": true}

// asyncpgToPgx is the pgx form of a URL asyncEngineRefusal accepted. The
// asyncpg dialect turns every query option into a connect() keyword: `ssl`
// takes asyncpg's string modes (here pgx's sslmode) and `command_timeout` a
// number (accepted, no effect on one query); any other option, `sslmode`
// included, is an unexpected keyword or an invalid value, which Python's
// lookup swallows as "no credentials" (false here).
func asyncpgToPgx(dbURL string) (string, bool) {
	_, rest, found := strings.Cut(dbURL, "://")
	if !found || strings.Contains(rest, "#") {
		return "", false
	}
	parsed, err := url.Parse("postgresql://" + rest)
	if err != nil {
		return "", false
	}
	query := url.Values{}
	for key, values := range parsed.Query() {
		if len(values) != 1 {
			return "", false
		}
		switch key {
		case "ssl":
			if !asyncpgSSLModes[values[0]] {
				return "", false
			}
			query.Set("sslmode", values[0])
		case "command_timeout":
			if _, err := strconv.ParseFloat(values[0], 64); err != nil {
				return "", false
			}
		default:
			return "", false
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), true
}

func settingsDecryptor(env cli.Env) (providerfoundation.FernetDecryptor, error) {
	key, _, err := secrets.ResolveSecret("SETTINGS_ENCRYPTION_KEY", env.Lookup)
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

// githubCredentialFromJSON builds the credential from the decrypted payload
// through the WORKER's own decode and shape rule
// (providerfoundation.DecodeStoredGitHubCredential: the camelCase aliases
// applied, a later key of the document wins, a null dropped, a key the
// credential does not use IGNORED with its name returned in ignored, never its
// value). Python's sync verb refused those payloads (_build_credential,
// GitHubCredentials(**dict), resolver.py:222-241), which made every credential
// the web saves (with an `org` key) unusable for the verb while the worker
// reads the same row (CHAOS-9678). A refusal is a typed credentialLookupFailure.
func githubCredentialFromJSON(plaintext []byte) (credential *GitHubCredentials, ignored []string, err error) {
	stored, decodeErr := providerfoundation.DecodeStoredGitHubCredential(plaintext)
	var refused *providerfoundation.StoredCredentialFailure
	if errors.As(decodeErr, &refused) {
		failure := lookupFailure(storedFailureReason(refused.Reason))
		failure.HasToken, failure.HasApp = refused.HasToken, refused.HasApp
		return nil, nil, failure
	}
	if decodeErr != nil {
		return nil, nil, lookupFailureWith(reasonPayloadNotObject, decodeErr)
	}
	built := &GitHubCredentials{Name: "default"}
	if baseURL := stored.BaseURL; baseURL.Configured() {
		value := baseURL.Reveal()
		built.BaseURL = &value
	}
	if stored.Token.Configured() {
		built.Mode, built.Token = CredentialPAT, stored.Token.Reveal()
		return built, stored.Ignored, nil
	}
	built.Mode = CredentialApp
	built.AppID, built.PrivateKey, built.InstallationID = stored.AppID.Reveal(), stored.PrivateKey.Reveal(), stored.InstallationID.Reveal()
	return built, stored.Ignored, nil
}

// storedFailureReason maps a refusal of the shared decode to this lookup's reason code.
func storedFailureReason(reason string) string {
	switch reason {
	case providerfoundation.StoredCredentialPayloadNotJSON:
		return reasonPayloadNotJSON
	case providerfoundation.StoredCredentialPayloadNotObject:
		return reasonPayloadNotObject
	case providerfoundation.StoredCredentialPrivateKeyPathUnread:
		return reasonPrivateKeyPathUnread
	}
	return reasonCredentialShapeInvalid
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
