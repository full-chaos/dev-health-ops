package authconfig

import (
	"fmt"
	"sort"
	"strings"
)

// Kind is the parse type of an option value. It tells the flag layer which
// registration to make and tells --help what to print after the flag name.
type Kind uint8

const (
	// KindString is an opaque string value.
	KindString Kind = iota
	// KindDuration is a Go duration ("30s", "5m").
	KindDuration
	// KindInt is a bounded integer.
	KindInt
	// KindFloat is a bounded floating-point value.
	KindFloat
)

// Placeholder is the value name --help prints after the flag.
func (k Kind) Placeholder() string {
	switch k {
	case KindDuration:
		return "duration"
	case KindInt:
		return "n"
	case KindFloat:
		return "rate"
	default:
		return "value"
	}
}

// Option declares one operator-settable setting of auth-service.
//
// This registry is the binary's single discovery surface: --help renders from
// it, the flag layer registers from it, and Load resolves through it. A
// setting that is not declared here has no flag, so a knob cannot exist on one
// surface and be missing from another -- the four-surface wiring problem the
// worker fleet hit (CHAOS-3942) and solved the same way in
// internal/platform/config.
//
// Deliberately NOT reusing internal/platform/config's registry: that registry
// is the worker fleet's, and config.Load unconditionally resolves POSTGRES_URI,
// CLICKHOUSE_URI, WORKER_DATABASE_URI, River retention windows and PagerDuty
// OAuth secrets. Wiring auth-service through it would give the auth control
// plane the ops application schema's DSN by default, which is exactly the
// ownership boundary ACP-ADR-04 exists to draw (the auth schema and role are
// separate, and the runtime owns no DDL anywhere). The leaf packages that are
// genuinely generic -- platform/secrets, platform/health, platform/lifecycle,
// platform/logging, platform/version -- ARE reused, unchanged.
type Option struct {
	// Flag is the canonical long flag, without leading dashes. Empty for a
	// secret, which is deliberately environment-only.
	Flag string
	// Env is the 12-factor fallback consulted when the flag is absent.
	Env  string
	Kind Kind
	// Default is the documented default, printed by --help. It is
	// descriptive: the authoritative default lives at the resolution site in
	// Load.
	Default string
	Usage   string
	// Secret marks a value that must never reach a command line. Process
	// arguments are readable by anyone who can run `ps` or `docker inspect`
	// and `docker compose config` renders them verbatim, so DSNs stay in the
	// environment on purpose and --help says so.
	Secret bool
	// Group is the --help section heading.
	Group string
}

// Help section headings, in display order.
const (
	GroupRuntime  = "Process runtime"
	GroupHTTP     = "HTTP surface"
	GroupDatabase = "Database"
	GroupKeys     = "Signing keys"
)

var groupOrder = []string{GroupRuntime, GroupHTTP, GroupDatabase, GroupKeys}

var optionRegistry = []Option{
	{
		Flag: "log-level", Env: EnvLogLevel, Kind: KindString,
		Default: "info", Group: GroupRuntime,
		Usage: "minimum log level (debug, info, warn, error)",
	},
	{
		Flag: "shutdown-timeout", Env: EnvShutdownTimeout, Kind: KindDuration,
		Default: defaultShutdownTimeout.String(), Group: GroupRuntime,
		Usage: "total budget for ordered graceful shutdown of every component",
	},
	{
		Flag: "health-check-timeout", Env: EnvHealthCheckTimeout, Kind: KindDuration,
		Default: defaultHealthCheckTimeout.String(), Group: GroupRuntime,
		Usage: "per-check bound applied to every required readiness check",
	},
	{
		Flag: "http-addr", Env: EnvAPIAddress, Kind: KindString,
		Default: defaultAPIAddress, Group: GroupHTTP,
		Usage: "host:port for the auth API surface (no routes are mounted in this wave)",
	},
	{
		Flag: "operator-http-addr", Env: EnvOperatorAddress, Kind: KindString,
		Default: defaultOperatorAddress, Group: GroupHTTP,
		Usage: "host:port for the operator surface (/healthz, /readyz, /metrics)",
	},
	{
		Flag: "request-timeout", Env: EnvRequestTimeout, Kind: KindDuration,
		Default: defaultRequestTimeout.String(), Group: GroupHTTP,
		Usage: "per-request handler deadline applied to every API route",
	},
	{
		Flag: "max-body-bytes", Env: EnvMaxBodyBytes, Kind: KindInt,
		Default: fmt.Sprintf("%d", defaultMaxBodyBytes), Group: GroupHTTP,
		Usage: "maximum accepted request body size in bytes",
	},
	{
		Flag: "rate-limit", Env: EnvRateLimitPerSecond, Kind: KindFloat,
		Default: fmt.Sprintf("%g", defaultRateLimitPerSecond), Group: GroupHTTP,
		Usage: "sustained requests per second allowed per route bucket",
	},
	{
		Flag: "rate-limit-burst", Env: EnvRateLimitBurst, Kind: KindInt,
		Default: fmt.Sprintf("%d", defaultRateLimitBurst), Group: GroupHTTP,
		Usage: "instantaneous burst allowance per route bucket",
	},
	{
		Env: EnvDatabaseURI, Kind: KindString, Secret: true, Group: GroupDatabase,
		Usage: "PostgreSQL DSN for the auth-owned schema; " + EnvDatabaseURI +
			"_FILE reads it from a mounted file instead (the two are mutually exclusive)",
	},
	{
		Flag: "database-schema", Env: EnvDatabaseSchema, Kind: KindString,
		Default: defaultDatabaseSchema, Group: GroupDatabase,
		Usage: "PostgreSQL schema owned by the auth control plane (ACP-ADR-04)",
	},
	{
		Flag: "database-max-conns", Env: EnvDatabaseMaxConns, Kind: KindInt,
		Default: fmt.Sprintf("%d", defaultDatabaseMaxConns), Group: GroupDatabase,
		Usage: "maximum PostgreSQL connections held by this replica",
	},
	{
		Flag: "database-connect-timeout", Env: EnvDatabaseConnectTimeout, Kind: KindDuration,
		Default: defaultDatabaseConnectTimeout.String(), Group: GroupDatabase,
		Usage: "dial timeout for a single PostgreSQL connection attempt",
	},
	{
		Flag: "signing-key-file", Env: EnvSigningKeyFile, Kind: KindString,
		Group: GroupKeys,
		Usage: "path to the Ed25519 signing key (PKCS#8 PEM); a direct " +
			EnvSigningKeyDirect + " value is rejected (ACP-ADR-02 §4)",
	},
	{
		Flag: "signing-key-id", Env: EnvSigningKeyID, Kind: KindString,
		Group: GroupKeys,
		Usage: "JWKS `kid` for the configured signing key (ACP-ADR-02 §5)",
	},
}

// Options returns the declared option set in registry order.
func Options() []Option {
	return append([]Option(nil), optionRegistry...)
}

// HelpText renders the option registry as the binary's --help output.
func HelpText() string {
	var out strings.Builder
	fmt.Fprintf(&out, "%s -- Auth Control Plane service (dormant: no route is mounted in this wave)\n\n", Service)
	fmt.Fprintf(&out, "Usage:\n  %s [options]\n\n", Service)
	fmt.Fprint(&out, "Options:\n  --version\n        print build metadata as JSON and exit\n  --help\n        print this message and exit\n")

	byGroup := make(map[string][]Option, len(groupOrder))
	for _, option := range optionRegistry {
		byGroup[option.Group] = append(byGroup[option.Group], option)
	}
	for _, group := range groupOrder {
		options := byGroup[group]
		if len(options) == 0 {
			continue
		}
		sort.SliceStable(options, func(i, j int) bool { return options[i].Env < options[j].Env })
		fmt.Fprintf(&out, "\n%s:\n", group)
		for _, option := range options {
			if option.Secret {
				fmt.Fprintf(&out, "  %s (environment only -- never a flag)\n", option.Env)
			} else {
				fmt.Fprintf(&out, "  --%s %s (env %s)\n", option.Flag, option.Kind.Placeholder(), option.Env)
			}
			fmt.Fprintf(&out, "        %s\n", option.Usage)
			if option.Default != "" {
				fmt.Fprintf(&out, "        default: %s\n", option.Default)
			}
		}
	}
	return out.String()
}
