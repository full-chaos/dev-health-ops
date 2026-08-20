package config

import (
	"fmt"
	"slices"
	"strings"
)

// Kind is the parse type of an option value. It tells the flag layer which
// flag.FlagSet registration to make and tells --help what to print after the
// flag name.
type Kind uint8

const (
	// KindString is an opaque string value.
	KindString Kind = iota
	// KindBool is a true/false value. Bool options are registered so that both
	// --flag and --flag=false work.
	KindBool
	// KindDuration is a Go duration ("30s", "16m").
	KindDuration
	// KindInt is a bounded integer.
	KindInt
)

// Placeholder is the value name --help prints after the flag.
func (k Kind) Placeholder() string {
	switch k {
	case KindBool:
		return "true|false"
	case KindDuration:
		return "duration"
	case KindInt:
		return "n"
	default:
		return "value"
	}
}

// Option declares one operator-settable setting.
//
// This registry is the single discovery surface CHAOS-4020 asks for: --help is
// rendered from it, the flag layer is registered from it, and Load resolves
// through it. A setting that is not declared here has no flag, so adding one
// cannot be forgotten halfway -- the previous four-surface wiring problem
// (CHAOS-3942) was exactly a setting that existed in some surfaces but not
// others.
type Option struct {
	// Flag is the canonical long flag, without leading dashes. Empty for a
	// secret, which is deliberately environment-only.
	Flag string
	// Short is an optional single-character alias registered alongside Flag.
	Short string
	// Aliases are additional accepted spellings of this option, kept so a
	// rename does not break manifests that are not in this repository. They are
	// noted in --help rather than listed as options of their own.
	Aliases []string
	// Env is the 12-factor fallback consulted when the flag is absent. Empty
	// means the setting is flag-only.
	Env  string
	Kind Kind
	// Default is the documented default, printed by --help. It is descriptive:
	// the authoritative default lives at the resolution site in Load.
	Default string
	Usage   string
	// Secret marks a value that must never reach a command line. Process
	// arguments are readable by anyone who can run `ps` or `docker inspect`,
	// and `docker compose config` renders them verbatim, so DSNs and tokens
	// stay in the environment on purpose and --help says so.
	Secret bool
	// QueueOnly restricts the option to binaries that consume queues. Load
	// already rejects queue settings on the other services; registering the
	// flag only where it applies keeps their --help honest.
	QueueOnly bool
	// Services, when non-empty, restricts the option to the named services.
	// This is the extension point for a single-binary knob: declare it here
	// with Services set and the flag, --help entry, env fallback, and
	// precedence all follow without touching the flag layer.
	Services []string
	// Group is the --help section heading.
	Group string
}

// Help section headings, in display order.
const (
	GroupWorker      = "Worker"
	GroupRuntime     = "Process runtime"
	GroupDatabase    = "Database and River"
	GroupRoutes      = "Provider routes"
	GroupBridge      = "Operational bridge"
	GroupCredentials = "Credentials"
)

var groupOrder = []string{
	GroupWorker,
	GroupRuntime,
	GroupDatabase,
	GroupRoutes,
	GroupBridge,
	GroupCredentials,
}

// optionRegistry declares every operator-settable setting of the Go worker
// binaries. The 40 provider route switches are deliberately absent: they have
// no flag at all. A worker executes the queues it subscribes to, and those
// switches survive only as the Python producer / Go executor agreement
// (see routes.go).
var optionRegistry = []Option{
	// Worker: queue topology and identity.
	{
		// Celery spells this -Q; -q is kept because the Go worker has always
		// accepted it and every checked-in manifest uses --queues.
		Flag: "queues", Short: "Q", Aliases: []string{"q"}, Kind: KindString,
		QueueOnly: true, Group: GroupWorker,
		Usage: "queues to consume (comma-separated or repeatable), mirroring celery worker -Q",
	},
	{
		Flag: "concurrency", Short: "c", Aliases: []string{"queue-concurrency"},
		Env:  "DEV_HEALTH_QUEUE_CONCURRENCY",
		Kind: KindString, QueueOnly: true, Group: GroupWorker,
		Usage: "queue worker budgets as queue=workers entries (comma-separated or repeatable)",
	},
	{
		Flag: "worker-group", Env: "DEV_HEALTH_WORKER_GROUP", Kind: KindString,
		Default: "worker", QueueOnly: true, Group: GroupWorker,
		Usage: "stable worker group label for logs and metrics; never selects queues",
	},

	// Process runtime.
	{
		Flag: "profile", Env: "DEV_HEALTH_PROFILE", Kind: KindString,
		Services: []string{"dev-health-stream-runner"}, Group: GroupRuntime,
		Usage: "runtime profile",
	},
	{
		Flag: "http-addr", Env: "DEV_HEALTH_HTTP_ADDR", Kind: KindString,
		Default: defaultHTTPAddress, Group: GroupRuntime,
		Usage: "host:port for the operator HTTP server (/healthz, /readyz, /metrics)",
	},
	{
		Flag: "shutdown-timeout", Env: "DEV_HEALTH_SHUTDOWN_TIMEOUT", Kind: KindDuration,
		Default: defaultShutdownTimeout.String(), Group: GroupRuntime,
		Usage: "graceful shutdown budget; must cover the longest selected job timeout",
	},
	{
		Flag: "health-check-timeout", Env: "DEV_HEALTH_HEALTH_CHECK_TIMEOUT", Kind: KindDuration,
		Default: defaultHealthCheckTimout.String(), Group: GroupRuntime,
		Usage: "per-dependency readiness probe timeout",
	},
	{
		// celery worker spells this --loglevel; both are accepted.
		Flag: "log-level", Aliases: []string{"loglevel"},
		Env: "DEV_HEALTH_LOG_LEVEL", Kind: KindString,
		Default: "info", Group: GroupRuntime,
		Usage: "debug, info, warn, or error",
	},
	{
		Flag: "environment", Env: devHealthEnv, Kind: KindString, Group: GroupRuntime,
		Usage: "deployment environment name; GO_PROVIDER_ROUTES=all requires local",
	},
	{
		Flag: "stream-replicas", Env: "DEV_HEALTH_STREAM_REPLICAS", Kind: KindInt,
		Default: "1", Group: GroupRuntime,
		Usage: "configured replica count of this stream family (1-8)",
	},

	// The CHAOS-4005 unreclaimable-dispatching sweep. Reconciler-only: this is
	// the per-binary scope the registry exists to express, so the flag never
	// appears in another binary's --help. Flag name, enum, environment
	// fallback, and the active-is-a-declaration warning are preserved exactly
	// as CHAOS-4005 shipped them; only the resolution path changed, from a
	// private flag>env branch to the shared layered lookup.
	{
		Flag: "unreclaimable-sweep", Env: "SYNC_UNRECLAIMABLE_SWEEP",
		Kind: KindString, Default: "shadow",
		Services: []string{"dev-health-reconciler"}, Group: GroupRuntime,
		Usage: "unreclaimable dispatch sweep: off|shadow|active (default shadow). " +
			"Setting active asserts that no Celery consumer serves provider " +
			"units for this deployment",
	},

	// Database and River.
	{
		Flag: "queue-database-mode", Env: "WORKER_DATABASE_MODE", Kind: KindString,
		Default: string(QueueControlDirect), Group: GroupDatabase,
		Usage: "queue-control endpoint semantics: direct, session, or transaction",
	},
	{
		Flag: "coordinator-database-mode", Env: "COORDINATOR_DATABASE_MODE", Kind: KindString,
		Default: string(QueueControlDirect), Group: GroupDatabase,
		Usage: "coordinator endpoint semantics: direct, session, or transaction",
	},
	{
		Flag: "domain-transaction-pooler", Env: "PGBOUNCER_TRANSACTION_MODE", Kind: KindBool,
		Default: "false", Group: GroupDatabase,
		Usage: "the domain DSN points at a transaction-pooling PgBouncer",
	},
	{
		Flag: "river-schema", Env: "RIVER_DATABASE_SCHEMA", Kind: KindString,
		Default: defaultRiverDatabaseSchema, Group: GroupDatabase,
		Usage: "PostgreSQL schema holding the River job tables",
	},
	{
		Flag: "domain-database-role", Env: "RIVER_DOMAIN_DATABASE_ROLE", Kind: KindString,
		Default: defaultDomainDatabaseRole, Group: GroupDatabase,
		Usage: "PostgreSQL role for the domain pool",
	},
	{
		Flag: "queue-database-role", Env: "RIVER_QUEUE_DATABASE_ROLE", Kind: KindString,
		Default: defaultQueueDatabaseRole, Group: GroupDatabase,
		Usage: "PostgreSQL role for the queue-control pool",
	},
	{
		Flag: "coordinator-database-role", Env: "RIVER_COORDINATOR_DATABASE_ROLE", Kind: KindString,
		Default: defaultCoordinatorDatabaseRole, Group: GroupDatabase,
		Usage: "PostgreSQL role for the coordinator pool",
	},
	{
		Flag: "domain-max-conns", Env: "WORKER_DOMAIN_DATABASE_MAX_CONNS", Kind: KindInt,
		Default: "4", Group: GroupDatabase,
		Usage: "domain pool connection ceiling (1-16)",
	},
	{
		Flag: "queue-max-conns", Env: "WORKER_DATABASE_MAX_CONNS", Kind: KindInt,
		Default: "2", Group: GroupDatabase,
		Usage: "queue-control pool connection ceiling (1-4)",
	},
	{
		Flag: "coordinator-max-conns", Env: "WORKER_COORDINATOR_DATABASE_MAX_CONNS", Kind: KindInt,
		Default: "2", Group: GroupDatabase,
		Usage: "coordinator pool connection ceiling (1-4)",
	},
	{
		Flag: "completed-job-retention", Env: "RIVER_COMPLETED_JOB_RETENTION", Kind: KindDuration,
		Default: defaultCompletedRetention.String(), Group: GroupDatabase,
		Usage: "how long completed River jobs are kept (24h-8760h)",
	},
	{
		Flag: "cancelled-job-retention", Env: "RIVER_CANCELLED_JOB_RETENTION", Kind: KindDuration,
		Default: defaultCancelledRetention.String(), Group: GroupDatabase,
		Usage: "how long cancelled River jobs are kept (24h-8760h)",
	},
	{
		Flag: "discarded-job-retention", Env: "RIVER_DISCARDED_JOB_RETENTION", Kind: KindDuration,
		Default: defaultDiscardedRetention.String(), Group: GroupDatabase,
		Usage: "how long discarded River jobs are kept (24h-8760h)",
	},
	{
		Flag: "job-cleaner-timeout", Env: "RIVER_JOB_CLEANER_TIMEOUT", Kind: KindDuration,
		Default: defaultJobCleanerTimeout.String(), Group: GroupDatabase,
		Usage: "per-run timeout of the River job cleaner (5s-5m)",
	},

	// Provider routes.
	{
		Flag: "github-work-items-status-mapping",
		Env:  "WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH",
		Kind: KindString, Group: GroupRoutes,
		Usage: "path to status_mapping.yaml; required when github/work-items is enabled",
	},
	{
		Flag: "github-work-items-investment-config",
		Env:  "WORKER_GITHUB_WORK_ITEMS_INVESTMENT_CONFIG_PATH",
		Kind: KindString, Group: GroupRoutes,
		Usage: "path to investment_areas.yaml; required when github/work-items is enabled",
	},
	{
		Flag: "pagerduty-webhook-transport", Env: "PAGERDUTY_WEBHOOK_TRANSPORT", Kind: KindString,
		Default: PagerDutyTransportCelery, Group: GroupRoutes,
		Usage: "owner of the PagerDuty webhook stream: celery or river",
	},

	// Operational bridge.
	{
		Flag: "operational-bridge-url", Env: "WORKER_OPERATIONAL_BRIDGE_URL", Kind: KindString,
		Group: GroupBridge,
		Usage: "base URL of the Python operational bridge",
	},
	{
		Flag: "operational-bridge-timeout", Env: "WORKER_OPERATIONAL_BRIDGE_TIMEOUT", Kind: KindDuration,
		Default: "10s", Group: GroupBridge,
		Usage: "operational bridge request timeout (100ms-30s)",
	},
	{
		Flag: "operational-bridge-allow-insecure", Env: "WORKER_OPERATIONAL_BRIDGE_ALLOW_INSECURE",
		Kind: KindBool, Default: "false", Group: GroupBridge,
		Usage: "permit a plaintext operational bridge origin",
	},

	// Credentials: environment only, on purpose.
	{Env: "POSTGRES_URI", Secret: true, Group: GroupCredentials, Usage: "domain PostgreSQL DSN"},
	{Env: "WORKER_DATABASE_URI", Secret: true, Group: GroupCredentials, Usage: "queue-control PostgreSQL DSN"},
	{Env: "COORDINATOR_DATABASE_URI", Secret: true, Group: GroupCredentials, Usage: "coordinator PostgreSQL DSN; required by coordinator binaries"},
	{Env: "CLICKHOUSE_URI", Secret: true, Group: GroupCredentials, Usage: "ClickHouse DSN; native protocol, port 9000"},
	{Env: "VALKEY_URI", Secret: true, Group: GroupCredentials, Usage: "Valkey/Redis DSN"},
	{Env: "SETTINGS_ENCRYPTION_KEY", Secret: true, Group: GroupCredentials, Usage: "provider credential encryption key"},
	{Env: "SETTINGS_ENCRYPTION_SALT", Secret: true, Group: GroupCredentials, Usage: "provider credential encryption salt"},
	{Env: "PAGER_DUTY_CLIENT_ID", Secret: true, Group: GroupCredentials, Usage: "PagerDuty OAuth client id"},
	{Env: "PAGER_DUTY_SECRET", Secret: true, Group: GroupCredentials, Usage: "PagerDuty OAuth client secret"},
	{Env: "WORKER_OPERATIONAL_BRIDGE_TOKEN", Secret: true, Group: GroupCredentials, Usage: "operational bridge bearer token"},
}

// Options returns every declared option in registry order.
func Options() []Option {
	return slices.Clone(optionRegistry)
}

// AppliesTo reports whether this option is offered by the named service.
func (o Option) AppliesTo(service string, requireQueues bool) bool {
	if o.QueueOnly && !requireQueues {
		return false
	}
	if len(o.Services) > 0 && !slices.Contains(o.Services, service) {
		return false
	}
	return true
}

// OptionsFor returns the options a service exposes, in --help display order.
func OptionsFor(service string, requireQueues bool) []Option {
	selected := make([]Option, 0, len(optionRegistry))
	for _, option := range optionRegistry {
		if option.AppliesTo(service, requireQueues) {
			selected = append(selected, option)
		}
	}
	slices.SortStableFunc(selected, func(a, b Option) int {
		return slices.Index(groupOrder, a.Group) - slices.Index(groupOrder, b.Group)
	})
	return selected
}

// optionByEnv indexes the registry by environment-variable name.
var optionByEnv = func() map[string]Option {
	index := make(map[string]Option, len(optionRegistry))
	for _, option := range optionRegistry {
		if option.Env != "" {
			index[option.Env] = option
		}
	}
	return index
}()

// settingLabel renders the operator-facing name of a setting for error
// messages. Naming both the canonical flag and the environment fallback means
// one message serves an operator who configured either surface, which matters
// most during the migration when a deployment mixes the two.
func settingLabel(env string) string {
	option, known := optionByEnv[env]
	if !known || option.Flag == "" {
		return env
	}
	return fmt.Sprintf("--%s (%s)", option.Flag, env)
}

// EnvNames returns every environment variable the registry reads, sorted. It
// backs the deployment-surface contract tests.
func EnvNames() []string {
	names := make([]string, 0, len(optionRegistry)+len(routeRegistry))
	for _, option := range optionRegistry {
		if option.Env != "" {
			names = append(names, option.Env)
		}
	}
	for _, route := range routeRegistry {
		names = append(names, route.Env)
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// FlagNames returns every canonical long flag the registry offers, sorted.
func FlagNames() []string {
	names := make([]string, 0, len(optionRegistry))
	for _, option := range optionRegistry {
		if option.Flag != "" {
			names = append(names, option.Flag)
		}
	}
	slices.Sort(names)
	return names
}

// RequiredEnvironment lists the settings that have no flag and must therefore
// still be supplied through the environment. This is the "documented handful"
// a standard deployment configures.
func RequiredEnvironment() []string {
	names := make([]string, 0, 16)
	for _, option := range optionRegistry {
		if option.Secret {
			names = append(names, option.Env)
		}
	}
	slices.Sort(names)
	return names
}

// HelpText renders the full --help body for one service.
func HelpText(service string, requireQueues bool) string {
	var out strings.Builder
	fmt.Fprintf(&out, "Usage: %s [options]\n\n", service)
	out.WriteString(
		"Configuration is flag-first: every option below may also be supplied through\n" +
			"its environment variable, and a flag always wins over the environment.\n" +
			"An unknown flag is rejected at startup rather than ignored.\n",
	)

	options := OptionsFor(service, requireQueues)
	currentGroup := ""
	for _, option := range options {
		if option.Group != currentGroup {
			currentGroup = option.Group
			fmt.Fprintf(&out, "\n%s:\n", strings.ToUpper(currentGroup))
		}
		out.WriteString(optionHelpLine(option))
	}

	// The forty WORKER_*_ENABLED provider route switches are deliberately not
	// listed as options: they have no flag. What a worker executes follows
	// from the queues it consumes, and the switches survive only so the Python
	// planner and the Go executor agree about what to plan.
	out.WriteString(
		"\nPROVIDER ROUTE SWITCHES:\n" +
			"  Environment only, and not selected here. A worker executes the queues it\n" +
			"  is told to consume with --queues; the WORKER_<PROVIDER>_<DATASET>_ENABLED\n" +
			"  variables remain the producer/executor agreement and are documented in\n" +
			"  docs/operate/run/workers-and-jobs.md.\n",
	)
	return out.String()
}

// optionHelpLine renders one option, its value placeholder, its environment
// fallback, and its default.
func optionHelpLine(option Option) string {
	var out strings.Builder
	if option.Secret {
		fmt.Fprintf(&out, "  %-42s %s\n", option.Env, option.Usage)
		fmt.Fprintf(&out, "  %-42s %s\n", "", "(environment only: never accepted as a flag)")
		return out.String()
	}
	name := "--" + option.Flag
	if option.Short != "" {
		name = "-" + option.Short + ", " + name
	}
	name += " " + option.Kind.Placeholder()
	usage := option.Usage
	if len(option.Aliases) > 0 {
		aliases := make([]string, 0, len(option.Aliases))
		for _, alias := range option.Aliases {
			dash := "--"
			if len(alias) == 1 {
				dash = "-"
			}
			aliases = append(aliases, dash+alias)
		}
		usage += fmt.Sprintf(" (alias: %s)", strings.Join(aliases, ", "))
	}
	fmt.Fprintf(&out, "  %-42s %s\n", name, usage)
	detail := ""
	switch {
	case option.Env != "" && option.Default != "":
		detail = fmt.Sprintf("env %s, default %s", option.Env, option.Default)
	case option.Env != "":
		detail = fmt.Sprintf("env %s", option.Env)
	case option.Default != "":
		detail = fmt.Sprintf("default %s", option.Default)
	}
	if detail != "" {
		fmt.Fprintf(&out, "  %-42s [%s]\n", "", detail)
	}
	return out.String()
}
