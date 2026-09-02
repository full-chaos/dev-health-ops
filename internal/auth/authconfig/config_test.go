package authconfig

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// baseEnv is a minimally valid environment: the two settings Load requires and
// nothing else, so every case below changes exactly one thing.
func baseEnv() map[string]string {
	return map[string]string{
		EnvDatabaseURI:    "postgres://auth@localhost:5432/devhealth",
		EnvSigningKeyFile: "/run/secrets/auth-signing-key.pem",
		EnvSigningKeyID:   "auth-2026-09",
	}
}

func lookupFrom(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func TestLoadResolvesDocumentedDefaults(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(baseEnv())})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Service != Service {
		t.Errorf("Service = %q, want %q", cfg.Service, Service)
	}
	if cfg.APIAddress != defaultAPIAddress || cfg.OperatorAddress != defaultOperatorAddress {
		t.Errorf(
			"addresses = %q/%q, want %q/%q",
			cfg.APIAddress, cfg.OperatorAddress, defaultAPIAddress, defaultOperatorAddress,
		)
	}
	if cfg.DatabaseSchema != defaultDatabaseSchema {
		t.Errorf("DatabaseSchema = %q, want %q", cfg.DatabaseSchema, defaultDatabaseSchema)
	}
	if cfg.ShutdownTimeout != defaultShutdownTimeout {
		t.Errorf("ShutdownTimeout = %s, want %s", cfg.ShutdownTimeout, defaultShutdownTimeout)
	}
	if cfg.MaxBodyBytes != defaultMaxBodyBytes {
		t.Errorf("MaxBodyBytes = %d, want %d", cfg.MaxBodyBytes, defaultMaxBodyBytes)
	}
	if cfg.RateLimit.PerSecond != defaultRateLimitPerSecond || cfg.RateLimit.Burst != defaultRateLimitBurst {
		t.Errorf("RateLimit = %+v, want %g/%d", cfg.RateLimit, defaultRateLimitPerSecond, defaultRateLimitBurst)
	}
	if !cfg.DatabaseURI.Configured() {
		t.Error("DatabaseURI is not configured")
	}
}

// TestConfigNeverRendersTheDSN is the leak control. SafeAttrs feeds the
// startup log line, and secrets.Value's String/LogValue/MarshalJSON are what
// keep the DSN out of it. Asserting on the SUBSTRING of the real value catches
// a future field added to SafeAttrs that reveals it.
func TestConfigNeverRendersTheDSN(t *testing.T) {
	const dsn = "postgres://auth:hunter2@db.internal:5432/devhealth"
	env := baseEnv()
	env[EnvDatabaseURI] = dsn

	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, attr := range cfg.SafeAttrs() {
		if strings.Contains(attr.Value.String(), "hunter2") || strings.Contains(attr.Value.String(), "db.internal") {
			t.Fatalf("SafeAttrs leaked the DSN through %q = %q", attr.Key, attr.Value.String())
		}
	}
	if got := cfg.DatabaseURI.Reveal(); got != dsn {
		t.Fatalf("Reveal() = %q, want the configured DSN", got)
	}
	// The signing-key PATH is also withheld: it leaks deployment layout and,
	// on a developer machine, a username.
	for _, attr := range cfg.SafeAttrs() {
		if strings.Contains(attr.Value.String(), "/run/secrets") {
			t.Fatalf("SafeAttrs leaked the signing-key path through %q", attr.Key)
		}
	}
}

func TestLoadRejectsSyntacticFaults(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(map[string]string)
		wantSub string
	}{
		{
			name:    "database URI absent",
			mutate:  func(env map[string]string) { delete(env, EnvDatabaseURI) },
			wantSub: EnvDatabaseURI + " or " + EnvDatabaseURI + "_FILE is required",
		},
		{
			name: "database URI given both directly and by file",
			mutate: func(env map[string]string) {
				env[EnvDatabaseURI+"_FILE"] = "/run/secrets/dsn"
			},
			wantSub: "mutually exclusive",
		},
		{
			name: "signing key supplied as a direct value",
			mutate: func(env map[string]string) {
				env[EnvSigningKeyDirect] = "-----BEGIN PRIVATE KEY-----"
			},
			wantSub: "must not carry signing material directly",
		},
		{
			name: "signing key supplied as an EMPTY direct value",
			mutate: func(env map[string]string) {
				env[EnvSigningKeyDirect] = ""
			},
			wantSub: "must not carry signing material directly",
		},
		{
			name:    "signing key file absent",
			mutate:  func(env map[string]string) { delete(env, EnvSigningKeyFile) },
			wantSub: EnvSigningKeyFile + " is required",
		},
		{
			name:    "signing key id absent",
			mutate:  func(env map[string]string) { delete(env, EnvSigningKeyID) },
			wantSub: EnvSigningKeyID + " is required",
		},
		{
			name:    "signing key id carries a quote",
			mutate:  func(env map[string]string) { env[EnvSigningKeyID] = `a"b` },
			wantSub: "must contain only letters",
		},
		{
			name:    "address is not host:port",
			mutate:  func(env map[string]string) { env[EnvAPIAddress] = "8095" },
			wantSub: EnvAPIAddress + " must be a host:port address",
		},
		{
			name: "api and operator addresses collide",
			mutate: func(env map[string]string) {
				env[EnvAPIAddress] = ":9000"
				env[EnvOperatorAddress] = ":9000"
			},
			wantSub: "must not be the same address",
		},
		{
			name:    "duration is unparseable",
			mutate:  func(env map[string]string) { env[EnvShutdownTimeout] = "soon" },
			wantSub: EnvShutdownTimeout + " must be a duration",
		},
		{
			name:    "duration is below the floor",
			mutate:  func(env map[string]string) { env[EnvShutdownTimeout] = "1ms" },
			wantSub: EnvShutdownTimeout + " must be between",
		},
		{
			name:    "duration is above the ceiling",
			mutate:  func(env map[string]string) { env[EnvShutdownTimeout] = "24h" },
			wantSub: EnvShutdownTimeout + " must be between",
		},
		{
			name:    "body bound is below the floor",
			mutate:  func(env map[string]string) { env[EnvMaxBodyBytes] = "10" },
			wantSub: EnvMaxBodyBytes + " must be between",
		},
		{
			name:    "body bound is not an integer",
			mutate:  func(env map[string]string) { env[EnvMaxBodyBytes] = "1MiB" },
			wantSub: EnvMaxBodyBytes + " must be an integer",
		},
		{
			name:    "rate limit is NaN",
			mutate:  func(env map[string]string) { env[EnvRateLimitPerSecond] = "NaN" },
			wantSub: EnvRateLimitPerSecond + " must be between",
		},
		{
			name:    "rate limit is +Inf",
			mutate:  func(env map[string]string) { env[EnvRateLimitPerSecond] = "+Inf" },
			wantSub: EnvRateLimitPerSecond + " must be between",
		},
		{
			name:    "rate limit is zero",
			mutate:  func(env map[string]string) { env[EnvRateLimitPerSecond] = "0" },
			wantSub: EnvRateLimitPerSecond + " must be between",
		},
		{
			name:    "log level is unknown",
			mutate:  func(env map[string]string) { env[EnvLogLevel] = "verbose" },
			wantSub: EnvLogLevel + " must be one of",
		},
		{
			name:    "schema is not a bare identifier",
			mutate:  func(env map[string]string) { env[EnvDatabaseSchema] = `auth"; DROP SCHEMA public; --` },
			wantSub: EnvDatabaseSchema + " must be a lowercase unquoted PostgreSQL identifier",
		},
		{
			name:    "schema starts with a digit",
			mutate:  func(env map[string]string) { env[EnvDatabaseSchema] = "1auth" },
			wantSub: EnvDatabaseSchema + " must start with a lowercase letter",
		},
		{
			name:    "connection ceiling is out of range",
			mutate:  func(env map[string]string) { env[EnvDatabaseMaxConns] = "0" },
			wantSub: EnvDatabaseMaxConns + " must be between",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			env := baseEnv()
			testCase.mutate(env)
			_, err := Load(Spec{LookupEnv: lookupFrom(env)})
			if err == nil {
				t.Fatalf("Load accepted %s", testCase.name)
			}
			if !strings.Contains(err.Error(), testCase.wantSub) {
				t.Fatalf("error = %q, want it to contain %q", err, testCase.wantSub)
			}
		})
	}
}

// TestSigningKeyFileIsNotValidatedAtLoad pins the CONTRACT the executed proof
// depends on: a signing-key path naming a file that does not exist is a
// dependency fault, not a configuration fault, so the process starts and
// /readyz reports it. If someone later "hardens" Load by stat-ing the path,
// this test fails and says why.
func TestSigningKeyFileIsNotValidatedAtLoad(t *testing.T) {
	env := baseEnv()
	env[EnvSigningKeyFile] = filepath.Join(t.TempDir(), "absent.pem")

	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("Load rejected an absent key file at config time: %v", err)
	}
	if _, statErr := os.Stat(cfg.SigningKeyPath); statErr == nil {
		t.Fatal("test setup is wrong: the key file exists")
	}
}

// TestUnreachableDSNIsNotValidatedAtLoad is the same contract for the database
// half of the executed proof.
func TestUnreachableDSNIsNotValidatedAtLoad(t *testing.T) {
	env := baseEnv()
	env[EnvDatabaseURI] = "postgres://auth@127.0.0.1:1/devhealth"

	if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err != nil {
		t.Fatalf("Load rejected an unreachable DSN at config time: %v", err)
	}
}

func TestOverridesShadowTheEnvironment(t *testing.T) {
	env := baseEnv()
	env[EnvShutdownTimeout] = "45s"

	cfg, err := Load(Spec{
		Overrides: map[string]string{EnvShutdownTimeout: "90s"},
		LookupEnv: lookupFrom(env),
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ShutdownTimeout != 90*time.Second {
		t.Fatalf("ShutdownTimeout = %s, want the flag's 90s", cfg.ShutdownTimeout)
	}
	// A setting the operator passed as a flag is NOT reported as env-only.
	for _, name := range cfg.EnvOnlySettings {
		if name == EnvShutdownTimeout {
			t.Fatal("a flag-supplied setting was reported as environment-only")
		}
	}
}

// TestBlankOverrideFallsThroughToTheEnvironment pins the one asymmetry that
// would otherwise silently reset a deployment's budget to the package default:
// every resolution site treats a blank value as unset, so a blank FLAG must
// too, or "--shutdown-timeout=" would be the single place in the surface where
// blank meant "override with nothing".
func TestBlankOverrideFallsThroughToTheEnvironment(t *testing.T) {
	env := baseEnv()
	env[EnvShutdownTimeout] = "45s"

	cfg, err := Load(Spec{
		Overrides: map[string]string{EnvShutdownTimeout: ""},
		LookupEnv: lookupFrom(env),
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ShutdownTimeout != 45*time.Second {
		t.Fatalf("ShutdownTimeout = %s, want the environment's 45s", cfg.ShutdownTimeout)
	}
}

func TestLoadRejectsAnUndeclaredOverride(t *testing.T) {
	_, err := Load(Spec{
		Overrides: map[string]string{"AUTH_SERVICE_NOT_A_SETTING": "1"},
		LookupEnv: lookupFrom(baseEnv()),
	})
	if err == nil || !strings.Contains(err.Error(), "undeclared configuration override") {
		t.Fatalf("error = %v, want an undeclared-override rejection", err)
	}
}

// TestEveryDeclaredOptionResolves is the registry's rot guard. It is the check
// that catches an option declared for --help but never wired into Load: it
// sets each non-secret option to a value distinguishable from its default and
// asserts Load's result actually moved. A registry entry that no resolution
// site reads is exactly the four-surface drift CHAOS-3942 was.
func TestEveryDeclaredOptionResolves(t *testing.T) {
	distinct := map[string]string{
		EnvLogLevel:               "debug",
		EnvShutdownTimeout:        "42s",
		EnvHealthCheckTimeout:     "1500ms",
		EnvAPIAddress:             "127.0.0.1:19095",
		EnvOperatorAddress:        "127.0.0.1:19096",
		EnvRequestTimeout:         "7s",
		EnvMaxBodyBytes:           "4096",
		EnvRateLimitPerSecond:     "3.5",
		EnvRateLimitBurst:         "9",
		EnvDatabaseSchema:         "authtest",
		EnvDatabaseMaxConns:       "11",
		EnvDatabaseConnectTimeout: "3s",
		EnvSigningKeyFile:         "/tmp/other-key.pem",
		EnvSigningKeyID:           "other-kid",
	}
	read := map[string]func(Config) string{
		EnvLogLevel:               func(c Config) string { return c.LogLevel.String() },
		EnvShutdownTimeout:        func(c Config) string { return c.ShutdownTimeout.String() },
		EnvHealthCheckTimeout:     func(c Config) string { return c.HealthCheckTimeout.String() },
		EnvAPIAddress:             func(c Config) string { return c.APIAddress },
		EnvOperatorAddress:        func(c Config) string { return c.OperatorAddress },
		EnvRequestTimeout:         func(c Config) string { return c.RequestTimeout.String() },
		EnvMaxBodyBytes:           func(c Config) string { return itoa(c.MaxBodyBytes) },
		EnvRateLimitPerSecond:     func(c Config) string { return ftoa(c.RateLimit.PerSecond) },
		EnvRateLimitBurst:         func(c Config) string { return itoa(int64(c.RateLimit.Burst)) },
		EnvDatabaseSchema:         func(c Config) string { return c.DatabaseSchema },
		EnvDatabaseMaxConns:       func(c Config) string { return itoa(int64(c.DatabaseMaxConns)) },
		EnvDatabaseConnectTimeout: func(c Config) string { return c.DatabaseConnectTimeout.String() },
		EnvSigningKeyFile:         func(c Config) string { return c.SigningKeyPath },
		EnvSigningKeyID:           func(c Config) string { return c.SigningKeyID },
	}

	baseline, err := Load(Spec{LookupEnv: lookupFrom(baseEnv())})
	if err != nil {
		t.Fatalf("Load baseline: %v", err)
	}

	for _, option := range Options() {
		if option.Secret {
			continue
		}
		t.Run(option.Env, func(t *testing.T) {
			value, declared := distinct[option.Env]
			reader, readable := read[option.Env]
			if !declared || !readable {
				t.Fatalf(
					"option %s is declared in the registry but this test has no probe for it; "+
						"add one so a registry entry cannot exist without a resolution site",
					option.Env,
				)
			}
			env := baseEnv()
			env[option.Env] = value
			cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
			if err != nil {
				t.Fatalf("Load with %s=%s: %v", option.Env, value, err)
			}
			if reader(cfg) == reader(baseline) {
				t.Fatalf(
					"setting %s=%s did not change the resolved configuration (still %q)",
					option.Env, value, reader(cfg),
				)
			}
		})
	}
}

// TestHelpTextNamesEveryOption keeps --help, the binary's single discovery
// surface, from drifting behind the registry.
func TestHelpTextNamesEveryOption(t *testing.T) {
	help := HelpText()
	for _, option := range Options() {
		if !strings.Contains(help, option.Env) {
			t.Errorf("--help does not mention %s", option.Env)
		}
		if option.Secret {
			if strings.Contains(help, "--"+option.Flag) && option.Flag != "" {
				t.Errorf("--help advertises a flag for the secret %s", option.Env)
			}
			continue
		}
		if !strings.Contains(help, "--"+option.Flag) {
			t.Errorf("--help does not mention --%s", option.Flag)
		}
	}
}

func itoa(value int64) string {
	return strconv.FormatInt(value, 10)
}

func ftoa(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}
