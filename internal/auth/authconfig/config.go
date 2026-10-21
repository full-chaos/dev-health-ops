// Package authconfig resolves the Auth Control Plane service's
// operator-settable configuration.
//
// The split this package draws is load-bearing and is what makes CHAOS-4881's
// readiness contract testable:
//
//   - A SYNTACTIC configuration fault -- a value this process can decide is
//     wrong without touching the network or the filesystem (both a direct
//     secret and its _FILE companion set, an unparseable duration, an address
//     that is not host:port, a required setting absent, a bound violated) --
//     stops the process before it starts. Load returns an error and the binary
//     exits non-zero.
//   - A DEPENDENCY fault -- a syntactically valid DSN pointing at a database
//     that is unreachable, a signing-key path naming a file that is missing,
//     group-readable, a symlink, oversized or not an Ed25519 key -- does NOT
//     stop startup. The process starts and /readyz fails CLOSED, naming the
//     failing check.
//
// The second half is deliberate, for two reasons. It is what the ticket's
// executed proof requires (a readiness response has to exist to be pasted).
// And a dependency verified only once, at startup, goes stale: CHAOS-4512 is
// the same defect in cmd/query-api, where a start-time ClickHouse ping left
// /readyz answering 200 unconditionally for a dependency that had since gone
// away. Every dependency check in this service is therefore re-run live, per
// readiness request, under a bounded per-check deadline.
package authconfig

import (
	"encoding/base64"
	"fmt"
	envsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Service is this binary's process identity. It is a compile-time constant,
// never an operator setting: it labels logs, metrics and the operator surface.
const Service = "auth-service"

// Environment variable names. Declared as constants so the option registry,
// the resolution sites and the tests all name the same string.
const (
	EnvLogLevel               = "AUTH_SERVICE_LOG_LEVEL"
	EnvShutdownTimeout        = "AUTH_SERVICE_SHUTDOWN_TIMEOUT"
	EnvHealthCheckTimeout     = "AUTH_SERVICE_HEALTH_CHECK_TIMEOUT"
	EnvAPIAddress             = "AUTH_SERVICE_HTTP_ADDR"
	EnvOperatorAddress        = "AUTH_SERVICE_OPERATOR_HTTP_ADDR"
	EnvRequestTimeout         = "AUTH_SERVICE_REQUEST_TIMEOUT"
	EnvMaxBodyBytes           = "AUTH_SERVICE_MAX_BODY_BYTES"
	EnvRateLimitPerSecond     = "AUTH_SERVICE_RATE_LIMIT_PER_SECOND"
	EnvRateLimitBurst         = "AUTH_SERVICE_RATE_LIMIT_BURST"
	EnvDatabaseURI            = "AUTH_DATABASE_URI"
	EnvDatabaseSchema         = "AUTH_DATABASE_SCHEMA"
	EnvDatabaseMaxConns       = "AUTH_DATABASE_MAX_CONNS"
	EnvDatabaseConnectTimeout = "AUTH_DATABASE_CONNECT_TIMEOUT"
	EnvSigningKeyFile         = "AUTH_SIGNING_KEY_FILE"
	EnvSigningKeyID           = "AUTH_SIGNING_KEY_ID"
	// EnvSigningKeySetFile names a key-set manifest (several keys, each with a
	// kid and a rotation state) in place of the single key file and kid.
	EnvSigningKeySetFile = "AUTH_SIGNING_KEYSET_FILE"
	// EnvSigningKeyDirect is the PROHIBITED spelling. ACP-ADR-02 §4:
	// "Direct-value secret env vars are prohibited for signing material."
	// Load rejects it when present rather than ignoring it, because an
	// operator who set it believes the key is in effect.
	EnvSigningKeyDirect = "AUTH_SIGNING_KEY"
	// EnvAccessTokenSecret is the HS256 key of the compatibility access token:
	// the same value as go-api's JWT_SECRET_KEY, so the tokens auth-service
	// mints verify where go-api's do. Secret: environment or _FILE only.
	EnvAccessTokenSecret   = "AUTH_JWT_SECRET_KEY"
	EnvAccessTokenIssuer   = "AUTH_JWT_ISSUER"
	EnvAccessTokenAudience = "AUTH_JWT_AUDIENCE"
	// EnvSessionSuccessorKey is the 32-byte session successor key (base64url,
	// no padding) that derives a rotated refresh credential (CHAOS-10207
	// ruling Q-B1-1). Secret: environment or _FILE only.
	EnvSessionSuccessorKey = "AUTH_SESSION_SUCCESSOR_KEY"
	// EnvLoginLockoutKey is the 32-byte login lockout key (base64url, no
	// padding): the HMAC key of the lockout identifier (local.Config). Set,
	// with the session secrets, it mounts the login route. Secret.
	EnvLoginLockoutKey = "AUTH_LOGIN_LOCKOUT_KEY"
	// EnvCompatRoutes mounts go-api's /api/v1/auth/{refresh,logout,
	// social-login} (and login, with the lockout key) beside the session
	// routes: true or false, default false.
	EnvCompatRoutes = "AUTH_SERVICE_COMPAT_ROUTES"
	// EnvTrustedProxies lists the proxies whose X-Forwarded-For names the
	// client (internal/auth/clientaddr).
	EnvTrustedProxies = clientaddr.Env
)

// SocialProviders are the social sign-in providers, in go-api's
// SOCIAL_<PROVIDER>_CLIENT_ID / _CLIENT_SECRET naming.
var SocialProviders = []string{"github", "gitlab", "google"}

// SocialClientIDEnv and SocialClientSecretEnv are go-api's social client
// settings for provider.
func SocialClientIDEnv(provider string) string {
	return "SOCIAL_" + strings.ToUpper(provider) + "_CLIENT_ID"
}

func SocialClientSecretEnv(provider string) string {
	return "SOCIAL_" + strings.ToUpper(provider) + "_CLIENT_SECRET"
}

const (
	defaultAPIAddress             = ":8095"
	defaultOperatorAddress        = ":8096"
	defaultDatabaseSchema         = "auth"
	defaultShutdownTimeout        = 30 * time.Second
	defaultHealthCheckTimeout     = 2 * time.Second
	defaultRequestTimeout         = 10 * time.Second
	defaultMaxBodyBytes           = int64(1 << 20)
	defaultRateLimitPerSecond     = float64(20)
	defaultRateLimitBurst         = 40
	defaultDatabaseMaxConns       = int32(8)
	defaultDatabaseConnectTimeout = 5 * time.Second
	// go-api's JWT_ISSUER and JWT_AUDIENCE defaults (internal/platform/config).
	defaultAccessTokenIssuer   = "dev-health-ops"
	defaultAccessTokenAudience = "dev-health-api"
	// minAccessTokenSecretLength is edgetoken.MinSecretLength, the Python
	// signer's floor, counted in characters.
	minAccessTokenSecretLength = 32
	sessionSuccessorKeyBytes   = 32
	loginLockoutKeyBytes       = 32

	minShutdownTimeout        = 500 * time.Millisecond
	maxShutdownTimeout        = 10 * time.Minute
	minHealthCheckTimeout     = 50 * time.Millisecond
	maxHealthCheckTimeout     = 30 * time.Second
	minRequestTimeout         = 100 * time.Millisecond
	maxRequestTimeout         = 5 * time.Minute
	minDatabaseConnectTimeout = 100 * time.Millisecond
	maxDatabaseConnectTimeout = time.Minute
	minMaxBodyBytes           = int64(1024)
	maxMaxBodyBytes           = int64(32 << 20)
	minDatabaseMaxConns       = int32(1)
	maxDatabaseMaxConns       = int32(256)
	minRateLimitBurst         = 1
	maxRateLimitBurst         = 100_000
	minRateLimitPerSecond     = float64(0.01)
	maxRateLimitPerSecond     = float64(100_000)
)

// Config is the fully resolved, validated configuration of one auth-service
// process. Every field is safe to hold; the one sensitive value is wrapped in
// secrets.Value, which redacts itself in logs, %v formatting and JSON.
type Config struct {
	Service string

	LogLevel           slog.Level
	ShutdownTimeout    time.Duration
	HealthCheckTimeout time.Duration

	APIAddress      string
	OperatorAddress string
	RequestTimeout  time.Duration
	MaxBodyBytes    int64
	RateLimit       RateLimit

	DatabaseURI            secrets.Value
	DatabaseSchema         string
	DatabaseMaxConns       int32
	DatabaseConnectTimeout time.Duration

	SigningKeyPath string
	SigningKeyID   string
	// SigningKeySetPath is the key-set manifest; when set, SigningKeyPath and
	// SigningKeyID are empty.
	SigningKeySetPath string

	// Session holds the session routes' token settings. Session.Enabled is
	// false when neither secret is set: the service then mounts no session
	// route. Setting one secret without the other is a configuration error.
	Session SessionTokens

	// TrustedProxies are the peers whose X-Forwarded-For names the client.
	TrustedProxies clientaddr.Trust

	// Social is each social provider's OAuth client, keyed by provider. A
	// provider without both values answers go-api's 503.
	Social map[string]SocialClient

	// EnvOnlySettings names the non-secret settings that were resolved from
	// the environment although a canonical flag exists for them. A deployment
	// configured that way is invisible in `docker compose config`, so the
	// runtime warns once at startup rather than staying silent.
	EnvOnlySettings []string
}

// SessionTokens are the session routes' secrets and the access token's iss
// and aud.
type SessionTokens struct {
	Enabled        bool
	AccessSecret   secrets.Value
	AccessIssuer   string
	AccessAudience string
	SuccessorKey   secrets.Value
	// CompatRoutes mounts go-api's compatibility routes (EnvCompatRoutes).
	CompatRoutes bool
	// LoginEnabled is set with LockoutKey: the login route is mounted.
	LoginEnabled bool
	LockoutKey   secrets.Value
}

// SocialClient is this service's OAuth client at one social provider.
type SocialClient struct {
	ID     string
	Secret secrets.Value
}

// RateLimit is the token-bucket shape applied per route bucket.
type RateLimit struct {
	PerSecond float64
	Burst     int
}

// SafeAttrs renders the configuration for the startup log line. It contains no
// secret value and no filesystem path: the signing-key path is reported only
// as "configured", because a path leaks deployment layout and, on a developer
// machine, a username.
func (c Config) SafeAttrs() []slog.Attr {
	return []slog.Attr{
		slog.String("service", c.Service),
		slog.String("log_level", c.LogLevel.String()),
		slog.String("shutdown_timeout", c.ShutdownTimeout.String()),
		slog.String("health_check_timeout", c.HealthCheckTimeout.String()),
		slog.String("api_address", c.APIAddress),
		slog.String("operator_address", c.OperatorAddress),
		slog.String("request_timeout", c.RequestTimeout.String()),
		slog.Int64("max_body_bytes", c.MaxBodyBytes),
		slog.Float64("rate_limit_per_second", c.RateLimit.PerSecond),
		slog.Int("rate_limit_burst", c.RateLimit.Burst),
		slog.Bool("database_configured", c.DatabaseURI.Configured()),
		slog.String("database_schema", c.DatabaseSchema),
		slog.Int64("database_max_conns", int64(c.DatabaseMaxConns)),
		slog.String("database_connect_timeout", c.DatabaseConnectTimeout.String()),
		slog.Bool("signing_key_configured", c.SigningKeyPath != ""),
		slog.Bool("signing_keyset_configured", c.SigningKeySetPath != ""),
		slog.String("signing_key_id", c.SigningKeyID),
		slog.Bool("api_routes_enabled", c.Session.Enabled),
		slog.Bool("compat_routes_enabled", c.Session.CompatRoutes),
		slog.Bool("login_route_enabled", c.Session.LoginEnabled),
		slog.Int("trusted_proxy_count", c.TrustedProxies.Len()),
		slog.String("jwt_issuer", c.Session.AccessIssuer),
		slog.String("jwt_audience", c.Session.AccessAudience),
	}
}

// Spec is Load's input. Overrides carries the values an operator supplied on
// the command line, keyed by the environment variable each one shadows, so
// flag-over-environment precedence falls out of one layered lookup rather than
// a private branch per setting.
type Spec struct {
	Overrides map[string]string
	LookupEnv secrets.LookupEnv
}

// Load resolves and validates the configuration. See the package doc for what
// it does and does not reject.
func Load(spec Spec) (Config, error) {
	environment := spec.LookupEnv
	if environment == nil {
		environment = envsecrets.ProcessLookup
	}
	if err := validateOverrides(spec.Overrides); err != nil {
		return Config{}, err
	}
	lookup := layered(spec.Overrides, environment)

	cfg := Config{Service: Service}
	cfg.EnvOnlySettings = envOnlySettings(spec.Overrides, environment)

	var err error
	if cfg.LogLevel, err = logLevel(lookup); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = duration(
		lookup, EnvShutdownTimeout, defaultShutdownTimeout, minShutdownTimeout, maxShutdownTimeout,
	); err != nil {
		return Config{}, err
	}
	if cfg.HealthCheckTimeout, err = duration(
		lookup, EnvHealthCheckTimeout, defaultHealthCheckTimeout, minHealthCheckTimeout, maxHealthCheckTimeout,
	); err != nil {
		return Config{}, err
	}
	if cfg.APIAddress, err = address(lookup, EnvAPIAddress, defaultAPIAddress); err != nil {
		return Config{}, err
	}
	if cfg.OperatorAddress, err = address(lookup, EnvOperatorAddress, defaultOperatorAddress); err != nil {
		return Config{}, err
	}
	// Two listeners in one process must not be pointed at one address: the
	// second bind fails at Start with a errno the operator then has to
	// decode. Reject the identical spelling here, where the message can name
	// both settings. Different spellings of the same endpoint (":8095" vs
	// "0.0.0.0:8095") still collide at bind time -- this check is a cheap,
	// exact-match guard, not a claim to resolve every alias.
	if cfg.APIAddress == cfg.OperatorAddress {
		return Config{}, fmt.Errorf(
			"%s and %s must not be the same address", EnvAPIAddress, EnvOperatorAddress,
		)
	}
	if cfg.RequestTimeout, err = duration(
		lookup, EnvRequestTimeout, defaultRequestTimeout, minRequestTimeout, maxRequestTimeout,
	); err != nil {
		return Config{}, err
	}
	if cfg.MaxBodyBytes, err = integer(
		lookup, EnvMaxBodyBytes, defaultMaxBodyBytes, minMaxBodyBytes, maxMaxBodyBytes,
	); err != nil {
		return Config{}, err
	}
	if cfg.RateLimit.PerSecond, err = float(
		lookup, EnvRateLimitPerSecond, defaultRateLimitPerSecond, minRateLimitPerSecond, maxRateLimitPerSecond,
	); err != nil {
		return Config{}, err
	}
	burst, err := integer(
		lookup, EnvRateLimitBurst, int64(defaultRateLimitBurst), minRateLimitBurst, maxRateLimitBurst,
	)
	if err != nil {
		return Config{}, err
	}
	cfg.RateLimit.Burst = int(burst)

	// secrets.Resolve is the platform's direct-value/_FILE mutual exclusion
	// (TRD §6, ACP-ADR-02 §4). It is reused rather than reimplemented so this
	// service cannot drift from the rule the worker fleet already enforces.
	databaseURI, configured, err := secrets.ResolveDSNSetting(EnvDatabaseURI, lookup)
	if err != nil {
		return Config{}, err
	}
	if !configured {
		return Config{}, fmt.Errorf(
			"%s or %s_FILE is required", EnvDatabaseURI, EnvDatabaseURI,
		)
	}
	cfg.DatabaseURI = databaseURI

	cfg.DatabaseSchema = stringOr(lookup, EnvDatabaseSchema, defaultDatabaseSchema)
	if err := validateIdentifier(EnvDatabaseSchema, cfg.DatabaseSchema); err != nil {
		return Config{}, err
	}
	maxConns, err := integer(
		lookup, EnvDatabaseMaxConns, int64(defaultDatabaseMaxConns),
		int64(minDatabaseMaxConns), int64(maxDatabaseMaxConns),
	)
	if err != nil {
		return Config{}, err
	}
	cfg.DatabaseMaxConns = int32(maxConns)
	if cfg.DatabaseConnectTimeout, err = duration(
		lookup, EnvDatabaseConnectTimeout, defaultDatabaseConnectTimeout,
		minDatabaseConnectTimeout, maxDatabaseConnectTimeout,
	); err != nil {
		return Config{}, err
	}

	if cfg.SigningKeySetPath, cfg.SigningKeyPath, cfg.SigningKeyID, err = signingKey(lookup); err != nil {
		return Config{}, err
	}
	if cfg.Session, err = sessionTokens(lookup); err != nil {
		return Config{}, err
	}
	if cfg.TrustedProxies, err = clientaddr.ParseTrust(valueOf(lookup, EnvTrustedProxies)); err != nil {
		return Config{}, err
	}
	if cfg.Social, err = socialClients(lookup); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// socialClients resolves each provider's client id and secret.
func socialClients(lookup secrets.LookupEnv) (map[string]SocialClient, error) {
	out := make(map[string]SocialClient, len(SocialProviders))
	for _, provider := range SocialProviders {
		secret, _, err := secrets.ResolveSecret(SocialClientSecretEnv(provider), lookup)
		if err != nil {
			return nil, err
		}
		out[provider] = SocialClient{ID: strings.TrimSpace(valueOf(lookup, SocialClientIDEnv(provider))), Secret: secret}
	}
	return out, nil
}

// sessionTokens resolves the session routes' settings. The two secrets come
// together or not at all: a deployment with one of them meant to serve the
// session routes, and silently serving none would hide that.
func sessionTokens(lookup secrets.LookupEnv) (SessionTokens, error) {
	out := SessionTokens{
		AccessIssuer:   stringOr(lookup, EnvAccessTokenIssuer, defaultAccessTokenIssuer),
		AccessAudience: stringOr(lookup, EnvAccessTokenAudience, defaultAccessTokenAudience),
	}
	access, hasAccess, err := secrets.ResolveSecret(EnvAccessTokenSecret, lookup)
	if err != nil {
		return SessionTokens{}, err
	}
	successor, hasSuccessor, err := secrets.ResolveSecret(EnvSessionSuccessorKey, lookup)
	if err != nil {
		return SessionTokens{}, err
	}
	if hasAccess != hasSuccessor {
		return SessionTokens{}, fmt.Errorf(
			"%s and %s are set together or not at all (the session routes need both)",
			EnvAccessTokenSecret, EnvSessionSuccessorKey,
		)
	}
	compat := false
	if raw := strings.TrimSpace(valueOf(lookup, EnvCompatRoutes)); raw != "" {
		if compat, err = strconv.ParseBool(raw); err != nil {
			return SessionTokens{}, fmt.Errorf("%s must be true or false", EnvCompatRoutes)
		}
	}
	_, hasLockout, err := secrets.ResolveSecret(EnvLoginLockoutKey, lookup)
	if err != nil {
		return SessionTokens{}, err
	}
	switch {
	case compat && !hasAccess:
		return SessionTokens{}, fmt.Errorf("%s needs the session secrets (%s and %s)",
			EnvCompatRoutes, EnvAccessTokenSecret, EnvSessionSuccessorKey)
	case hasLockout && !compat:
		return SessionTokens{}, fmt.Errorf("%s mounts login, which is a compatibility route: it needs %s=true",
			EnvLoginLockoutKey, EnvCompatRoutes)
	case !hasAccess:
		return out, nil
	}
	if len([]rune(access.Reveal())) < minAccessTokenSecretLength {
		return SessionTokens{}, fmt.Errorf(
			"%s must be at least %d characters", EnvAccessTokenSecret, minAccessTokenSecretLength,
		)
	}
	if raw, decodeErr := base64.RawURLEncoding.DecodeString(successor.Reveal()); decodeErr != nil || len(raw) != sessionSuccessorKeyBytes {
		return SessionTokens{}, fmt.Errorf(
			"%s must be %d bytes, base64url without padding", EnvSessionSuccessorKey, sessionSuccessorKeyBytes,
		)
	}
	out.Enabled, out.AccessSecret, out.SuccessorKey, out.CompatRoutes = true, access, successor, compat
	lockout, _, err := secrets.ResolveSecret(EnvLoginLockoutKey, lookup)
	if err != nil {
		return SessionTokens{}, err
	}
	if !hasLockout {
		return out, nil
	}
	if raw, decodeErr := base64.RawURLEncoding.DecodeString(lockout.Reveal()); decodeErr != nil || len(raw) != loginLockoutKeyBytes {
		return SessionTokens{}, fmt.Errorf(
			"%s must be %d bytes, base64url without padding", EnvLoginLockoutKey, loginLockoutKeyBytes,
		)
	}
	out.LoginEnabled, out.LockoutKey = true, lockout
	return out, nil
}

// signingKey resolves the signing-key custody settings.
//
// The direct-value spelling is REJECTED, not ignored: ACP-ADR-02 §4 prohibits
// direct-value secret environment variables for signing material, and an
// operator who exported one believes the key is in effect. Silently preferring
// the file would leave a live deployment whose key is not the key its operator
// set.
//
// Only the syntax of the path is checked here. Whether the file exists, is a
// regular file, is not a symlink, is mode 0600-ish, is small enough, and
// parses as an Ed25519 private key is a DEPENDENCY question answered live by
// the keystore readiness check -- see this package's doc comment.
func signingKey(lookup secrets.LookupEnv) (keySetPath, path, keyID string, err error) {
	if _, present := lookup(EnvSigningKeyDirect); present {
		return "", "", "", fmt.Errorf(
			"%s must not carry signing material directly; mount it and set %s instead (ACP-ADR-02 §4)",
			EnvSigningKeyDirect, EnvSigningKeyFile,
		)
	}
	path = strings.TrimSpace(valueOf(lookup, EnvSigningKeyFile))
	keyID = strings.TrimSpace(valueOf(lookup, EnvSigningKeyID))
	if keySetPath = strings.TrimSpace(valueOf(lookup, EnvSigningKeySetFile)); keySetPath != "" {
		if path != "" || keyID != "" {
			return "", "", "", fmt.Errorf(
				"%s names the whole key set; unset %s and %s",
				EnvSigningKeySetFile, EnvSigningKeyFile, EnvSigningKeyID,
			)
		}
		return keySetPath, "", "", nil
	}
	if path == "" {
		return "", "", "", fmt.Errorf("%s is required and must name a file (or set %s)", EnvSigningKeyFile, EnvSigningKeySetFile)
	}
	if keyID == "" {
		return "", "", "", fmt.Errorf(
			"%s is required: every signing key carries a kid and a JWKS entry from day one (ACP-ADR-02 §5)",
			EnvSigningKeyID,
		)
	}
	if err := validateKeyID(keyID); err != nil {
		return "", "", "", err
	}
	return "", path, keyID, nil
}

// keyIDAllowed is the character set a JWKS `kid` may use here. It is
// deliberately narrow: the kid is echoed in JWKS documents and in token
// headers, and a bounded charset means no call site has to think about
// escaping it.
func validateKeyID(keyID string) error {
	if len(keyID) > 64 {
		return fmt.Errorf("%s must be at most 64 characters", EnvSigningKeyID)
	}
	for _, r := range keyID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return fmt.Errorf(
				"%s must contain only letters, digits, '-', '_' and '.'", EnvSigningKeyID,
			)
		}
	}
	return nil
}

// validateIdentifier restricts a schema name to [a-z][a-z0-9_]*.
//
// What this enforces: the string's SHAPE. It rejects whitespace, quotes,
// semicolons, mixed case and anything else outside that set, once, at load
// time, so a malformed value fails startup rather than surfacing later.
//
// What it does NOT enforce, stated because an earlier version of this comment
// implied otherwise: safety as a bare SQL identifier. Reserved keywords pass
// it -- `select`, `from`, `user`, `table` are all accepted. Today that costs
// nothing, because the only consumer (authstore.Postgres.Probe) binds the
// schema as a query PARAMETER and never emits it as an identifier. A future
// call site that needs an identifier must quote it via
// pgx.Identifier{...}.Sanitize() rather than treating this function as
// having already made it safe.
func validateIdentifier(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", name)
	}
	if len(value) > 63 {
		return fmt.Errorf("%s must be at most 63 characters", name)
	}
	for index, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9', r == '_':
			if index == 0 {
				return fmt.Errorf("%s must start with a lowercase letter", name)
			}
		default:
			return fmt.Errorf(
				"%s must be a lowercase unquoted PostgreSQL identifier ([a-z][a-z0-9_]*)", name,
			)
		}
	}
	return nil
}

// validateOverrides rejects a flag that shadows an environment variable this
// package does not declare. Without it a renamed option would silently stop
// resolving.
func validateOverrides(overrides map[string]string) error {
	if len(overrides) == 0 {
		return nil
	}
	declared := make(map[string]struct{}, len(optionRegistry))
	for _, option := range optionRegistry {
		declared[option.Env] = struct{}{}
	}
	unknown := make([]string, 0)
	for key := range overrides {
		if _, ok := declared[key]; !ok {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("undeclared configuration override(s): %s", strings.Join(unknown, ", "))
}

// envOnlySettings names the declared, non-secret settings that arrived through
// the environment with no flag shadowing them.
func envOnlySettings(overrides map[string]string, environment secrets.LookupEnv) []string {
	names := make([]string, 0)
	for _, option := range optionRegistry {
		if option.Secret || option.Env == "" {
			continue
		}
		if _, overridden := overrides[option.Env]; overridden {
			continue
		}
		if value, present := environment(option.Env); present && strings.TrimSpace(value) != "" {
			names = append(names, option.Env)
		}
	}
	sort.Strings(names)
	return names
}

// layered makes a flag override shadow the environment for the same setting. A
// blank override counts as NOT supplied so the environment still resolves
// beneath it: every resolution site below treats blank as unset, and an empty
// flag that shadowed a real environment value would be the one place in the
// surface where blank meant "override with nothing".
func layered(overrides map[string]string, environment secrets.LookupEnv) secrets.LookupEnv {
	return func(key string) (string, bool) {
		if value, ok := overrides[key]; ok && strings.TrimSpace(value) != "" {
			return value, true
		}
		return environment(key)
	}
}

func valueOf(lookup secrets.LookupEnv, key string) string {
	value, ok := lookup(key)
	if !ok {
		return ""
	}
	return value
}

func stringOr(lookup secrets.LookupEnv, key, fallback string) string {
	if value := strings.TrimSpace(valueOf(lookup, key)); value != "" {
		return value
	}
	return fallback
}

func duration(
	lookup secrets.LookupEnv, key string, fallback, low, high time.Duration,
) (time.Duration, error) {
	raw := strings.TrimSpace(valueOf(lookup, key))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration such as %q", key, fallback.String())
	}
	if value < low || value > high {
		return 0, fmt.Errorf("%s must be between %s and %s", key, low, high)
	}
	return value, nil
}

func integer(lookup secrets.LookupEnv, key string, fallback, low, high int64) (int64, error) {
	raw := strings.TrimSpace(valueOf(lookup, key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	if value < low || value > high {
		return 0, fmt.Errorf("%s must be between %d and %d", key, low, high)
	}
	return value, nil
}

func float(lookup secrets.LookupEnv, key string, fallback, low, high float64) (float64, error) {
	raw := strings.TrimSpace(valueOf(lookup, key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number", key)
	}
	// NaN fails both comparisons below, so it is rejected by the bound check
	// rather than slipping through as "not less than, not greater than".
	if !(value >= low && value <= high) {
		return 0, fmt.Errorf("%s must be between %g and %g", key, low, high)
	}
	return value, nil
}

func address(lookup secrets.LookupEnv, key, fallback string) (string, error) {
	value := stringOr(lookup, key, fallback)
	if _, _, err := net.SplitHostPort(value); err != nil {
		return "", fmt.Errorf("%s must be a host:port address", key)
	}
	return value, nil
}

func logLevel(lookup secrets.LookupEnv) (slog.Level, error) {
	raw := strings.ToLower(strings.TrimSpace(valueOf(lookup, EnvLogLevel)))
	switch raw {
	case "":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("%s must be one of debug, info, warn, error", EnvLogLevel)
	}
}
