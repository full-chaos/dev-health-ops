package config

import (
	"slices"
	"strings"
	"testing"
)

// TestEveryNonSecretSettingIsReachableFromHelp is the acceptance test for
// "a new operator can configure a worker from --help alone". Every environment
// variable the configuration layer reads must appear in --help, either as a
// flag or as a documented environment-only credential. A setting that reaches
// Load without being declared here is exactly the unfindable variable this
// ticket removes.
func TestEveryNonSecretSettingIsReachableFromHelp(t *testing.T) {
	t.Parallel()

	help := HelpText("dev-health-worker", true)
	for _, option := range optionRegistry {
		if !option.AppliesTo("dev-health-worker", true) {
			continue
		}
		if option.Secret {
			if !strings.Contains(help, option.Env) {
				t.Errorf("credential %s is not documented in --help", option.Env)
			}
			continue
		}
		if !strings.Contains(help, "--"+option.Flag) {
			t.Errorf("option %s has no --help entry", option.Flag)
		}
		if option.Env != "" && !strings.Contains(help, option.Env) {
			t.Errorf("option %s does not document its %s fallback", option.Flag, option.Env)
		}
	}
	// There is no route enablement surface to advertise (CHAOS-4054). --help
	// says so explicitly and points at the two planes that do decide, rather
	// than leaving an operator to look for a switch that no longer exists.
	if !strings.Contains(help, "PROVIDER ROUTES:") {
		t.Error("--help must state that provider routes are not switch-selected")
	}
	if strings.Contains(help, "_ENABLED") {
		t.Error("--help still advertises a route enablement variable")
	}
}

// TestHelpDeclaresTheDocumentedEnvironmentHandful pins the acceptance
// criterion that a standard deployment configures a handful of variables rather
// than the eighty-five the binaries read today. The handful is the credential
// set, which stays in the environment on purpose.
func TestHelpDeclaresTheDocumentedEnvironmentHandful(t *testing.T) {
	t.Parallel()

	required := RequiredEnvironment()
	if len(required) > 12 {
		t.Fatalf("required environment grew to %d settings: %v", len(required), required)
	}
	for _, name := range required {
		option, declared := optionByEnv[name]
		if !declared || !option.Secret {
			t.Errorf("%s is reported as required environment but is not a declared credential", name)
		}
	}
	// Anything the registry reads that is not a credential must have a flag.
	for _, name := range EnvNames() {
		if slices.Contains(required, name) {
			continue
		}
		option, declared := optionByEnv[name]
		if declared && option.Flag == "" {
			t.Errorf("%s has neither a flag nor credential status", name)
		}
	}
}

// TestCredentialsAreNeverOfferedAsFlags keeps DSNs and tokens off the command
// line. Process arguments are visible through `ps` and `docker inspect`, and
// `docker compose config` renders a command: verbatim -- the very output this
// ticket asks operators to read.
func TestCredentialsAreNeverOfferedAsFlags(t *testing.T) {
	t.Parallel()

	for _, option := range optionRegistry {
		if option.Secret && (option.Flag != "" || option.Short != "") {
			t.Errorf("credential %s must not be offered as a flag", option.Env)
		}
	}
}

// TestOptionRegistryIsInternallyConsistent guards the registry itself: a
// duplicated flag name would silently make one declaration unreachable, and a
// duplicated environment name would make the override map ambiguous.
func TestOptionRegistryIsInternallyConsistent(t *testing.T) {
	t.Parallel()

	flags := map[string]bool{}
	envs := map[string]bool{}
	for _, option := range optionRegistry {
		if option.Flag != "" {
			if flags[option.Flag] {
				t.Errorf("flag --%s is declared more than once", option.Flag)
			}
			flags[option.Flag] = true
		}
		for _, alias := range option.Aliases {
			if flags[alias] {
				t.Errorf("flag --%s is declared more than once", alias)
			}
			flags[alias] = true
		}
		if option.Short != "" {
			if flags[option.Short] {
				t.Errorf("short flag -%s is declared more than once", option.Short)
			}
			flags[option.Short] = true
		}
		if option.Env != "" {
			if envs[option.Env] {
				t.Errorf("environment name %s is declared more than once", option.Env)
			}
			envs[option.Env] = true
		}
		if option.Usage == "" {
			t.Errorf("option %s%s has no usage text", option.Flag, option.Env)
		}
		if !slices.Contains(groupOrder, option.Group) {
			t.Errorf("option %s%s has unknown help group %q", option.Flag, option.Env, option.Group)
		}
	}
	if !flags["queue-concurrency"] {
		t.Error("--queue-concurrency must survive as an alias: manifests outside this repository still pass it")
	}
}

// TestSettingLabelNamesBothSurfaces keeps configuration errors useful during
// the migration, when one deployment may be configured by flag and another by
// environment.
func TestSettingLabelNamesBothSurfaces(t *testing.T) {
	t.Parallel()

	if got := settingLabel("WORKER_DATABASE_MODE"); got != "--queue-database-mode (WORKER_DATABASE_MODE)" {
		t.Fatalf("settingLabel = %q", got)
	}
	// A credential has no flag, so its own name is the whole label.
	if got := settingLabel("POSTGRES_URI"); got != "POSTGRES_URI" {
		t.Fatalf("credential label = %q, want the bare variable name", got)
	}
	if got := settingLabel("NOT_DECLARED"); got != "NOT_DECLARED" {
		t.Fatalf("undeclared label = %q", got)
	}
}

// TestOptionsForRespectsServiceScope proves the registry can carry a
// single-binary option without that flag leaking into every other binary's
// --help. This is the extension point a reconciler-only or scheduler-only knob
// uses.
func TestOptionsForRespectsServiceScope(t *testing.T) {
	t.Parallel()

	worker := HelpText("dev-health-worker", true)
	if strings.Contains(worker, "--profile") {
		t.Error("dev-health-worker must not advertise --profile")
	}
	if !strings.Contains(worker, "-Q, --queues") {
		t.Error("a queue-consuming binary must advertise -Q/--queues")
	}

	runner := HelpText("dev-health-stream-runner", false)
	if !strings.Contains(runner, "--profile") {
		t.Error("dev-health-stream-runner must advertise --profile")
	}
	if strings.Contains(runner, "-Q, --queues") {
		t.Error("a binary that consumes no queues must not advertise -Q/--queues")
	}
}

func TestHelpTextIsStableAndSelfDescribing(t *testing.T) {
	t.Parallel()

	help := HelpText("dev-health-worker", true)
	for _, fragment := range []string{
		"Usage: dev-health-worker [options]",
		"a flag always wins over the environment",
		"An unknown flag is rejected at startup",
		strings.ToUpper(GroupWorker) + ":",
		strings.ToUpper(GroupCredentials) + ":",
		"environment only: never accepted as a flag",
	} {
		if !strings.Contains(help, fragment) {
			t.Errorf("--help is missing %q", fragment)
		}
	}
	if !strings.Contains(help, "-Q, --queues") || !strings.Contains(help, "-c, --concurrency") {
		t.Error("--help must advertise the Celery short forms")
	}
}

func TestFlagNamesAreShellSafe(t *testing.T) {
	t.Parallel()

	for _, name := range FlagNames() {
		if name != strings.ToLower(name) || strings.ContainsAny(name, " _=") {
			t.Errorf("flag %q is not a lowercase dash-separated name", name)
		}
	}
	if len(FlagNames()) == 0 {
		t.Fatal("registry declares no flags")
	}
}

// TestUnreclaimableSweepIsReconcilerScoped proves the registry's per-binary
// scope does real work: CHAOS-4005's flag must appear in the reconciler's
// --help and in no other binary's, and it must keep the exact enum, fallback,
// and active-is-a-declaration warning that ticket shipped.
func TestUnreclaimableSweepIsReconcilerScoped(t *testing.T) {
	t.Parallel()

	reconciler := HelpText("dev-health-reconciler", false)
	if !strings.Contains(reconciler, "--unreclaimable-sweep") {
		t.Fatal("the reconciler must advertise --unreclaimable-sweep")
	}
	for _, fragment := range []string{
		"off|shadow|active",
		"default shadow",
		"Setting active asserts that no Celery consumer serves provider units for this deployment",
		"SYNC_UNRECLAIMABLE_SWEEP",
	} {
		if !strings.Contains(reconciler, fragment) {
			t.Errorf("--unreclaimable-sweep help lost %q", fragment)
		}
	}
	for _, service := range []string{
		"dev-health-worker", "dev-health-scheduler", "dev-health-stream-runner",
	} {
		if strings.Contains(HelpText(service, service == "dev-health-worker"), "--unreclaimable-sweep") {
			t.Errorf("%s must not advertise the reconciler's sweep flag", service)
		}
	}
}
