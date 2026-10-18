package workerservice

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

func githubWorkItemsConfigRepoRoot(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("moduleroot.Caller did not report this test file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
}

func validGitHubWorkItemsRuntimeConfig(t *testing.T) config.Config {
	t.Helper()
	root := githubWorkItemsConfigRepoRoot(t)
	return config.Config{
		WorkerGithubWorkItemsStatusMappingPath: filepath.Join(
			root, "src", "dev_health_ops", "config", "status_mapping.yaml",
		),
	}
}

func TestGitHubWorkItemsRuntimeConfigRequiresAValidatedExplicitStatusMappingPath(t *testing.T) {
	t.Setenv("STATUS_MAPPING_PATH", "")
	valid := validGitHubWorkItemsRuntimeConfig(t)
	runtimeConfig, err := githubWorkItemsRuntimeConfigFrom(valid)
	if err != nil {
		t.Fatal(err)
	}
	if !runtimeConfig.configured() ||
		runtimeConfig.statusMappingPath != valid.WorkerGithubWorkItemsStatusMappingPath {
		t.Fatalf("runtime config = %+v, config = %+v", runtimeConfig, valid)
	}

	root := githubWorkItemsConfigRepoRoot(t)
	for name, cfg := range map[string]config.Config{
		"missing status path": {},
		"blank status path":   {WorkerGithubWorkItemsStatusMappingPath: " "},
		"missing status file": {
			WorkerGithubWorkItemsStatusMappingPath: filepath.Join(root, "missing-status.yaml"),
		},
		"malformed status file": {
			WorkerGithubWorkItemsStatusMappingPath: filepath.Join(
				root, "internal", "providersync", "testdata", "status_mapping_configs", "structural_root_list.yaml",
			),
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := githubWorkItemsRuntimeConfigFrom(cfg)
			if !errors.Is(err, providersync.ErrInvalidConfiguration) {
				t.Fatalf("error = %v, want ErrInvalidConfiguration", err)
			}
		})
	}

	// CHAOS-4054: there is no "disabled" case left. A route switch used to say
	// "this deployment does not serve work-items", so unset artifacts were
	// simply not needed. Capability is always on now, so unset artifacts are a
	// configuration error that must surface at readiness, not a quiet opt-out
	// that turns into a per-claim executor failure later.
	if _, err := githubWorkItemsRuntimeConfigFrom(config.Config{}); !errors.Is(
		err, providersync.ErrInvalidConfiguration,
	) {
		t.Fatalf("unset artifact paths: error = %v, want ErrInvalidConfiguration", err)
	}
}

func TestGitHubWorkItemsRuntimeConfigRejectsEveryAmbientStatusMappingOverride(t *testing.T) {
	valid := validGitHubWorkItemsRuntimeConfig(t)
	for _, test := range []struct {
		name     string
		override string
	}{
		{name: "whitespace", override: " "},
		{name: "tab", override: "\t"},
		{name: "missing file", override: "/tmp/override-status.yaml"},
		// This is intentionally a valid artifact, not a malformed one. If
		// the production boundary ever stops rejecting ambient values, the
		// generic LoadStatusMapping fallback would accept this path and hide
		// the configuration split from the former error-only test.
		{name: "valid configured artifact", override: valid.WorkerGithubWorkItemsStatusMappingPath},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("STATUS_MAPPING_PATH", test.override)
			_, err := githubWorkItemsRuntimeConfigFrom(valid)
			if !errors.Is(err, providersync.ErrInvalidConfiguration) {
				t.Fatalf("override %q error = %v, want ErrInvalidConfiguration", test.override, err)
			}
		})
	}
}

func TestGitHubProjectsV2EnvironmentWarningCensusIsStartupOnly(t *testing.T) {
	t.Setenv("GITHUB_PROJECTS_V2", "")
	called := 0
	orphaned, err := githubProjectsV2EnvironmentNeedsStartupWarning(
		context.Background(),
		func(context.Context) (bool, error) {
			called++
			return false, nil
		},
	)
	if err != nil || orphaned || called != 0 {
		t.Fatalf("empty environment orphaned=%t calls=%d error=%v", orphaned, called, err)
	}

	t.Setenv("GITHUB_PROJECTS_V2", "acme:3")
	orphaned, err = githubProjectsV2EnvironmentNeedsStartupWarning(
		context.Background(), func(context.Context) (bool, error) { return false, nil },
	)
	if err != nil || !orphaned {
		t.Fatalf("env-only configuration orphaned=%t error=%v", orphaned, err)
	}
	orphaned, err = githubProjectsV2EnvironmentNeedsStartupWarning(
		context.Background(), func(context.Context) (bool, error) { return true, nil },
	)
	if err != nil || orphaned {
		t.Fatalf("durable configuration orphaned=%t error=%v", orphaned, err)
	}
	_, err = githubProjectsV2EnvironmentNeedsStartupWarning(
		context.Background(), func(context.Context) (bool, error) { return false, errors.New("probe fault") },
	)
	if !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("probe error = %v, want worker dependency unavailable", err)
	}
}

// The deprecated investment config path is accepted, never read, and logged once (CHAOS-9200): a deployment that
// still sets it keeps starting, and the setting is not a silent no-op.
func TestDeprecatedInvestmentConfigPathIsIgnoredAndWarnedOnce(t *testing.T) {
	t.Setenv("STATUS_MAPPING_PATH", "")
	// The default logger is process-wide: another goroutine (the role posture check) can log into it while this
	// test holds it, so the buffer is locked and the assertions look at the warning line only.
	logged := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logged, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	investmentConfigPathIgnoredWarning = sync.Once{}
	const warning = "WORKER_GITHUB_WORK_ITEMS_INVESTMENT_CONFIG_PATH is set and ignored"

	cfg := validGitHubWorkItemsRuntimeConfig(t)
	cfg.WorkerGithubWorkItemsInvestmentConfigPath = filepath.Join(t.TempDir(), "no-such-file.yaml")
	for range 3 {
		if _, err := githubWorkItemsRuntimeConfigFrom(cfg); err != nil {
			t.Fatalf("a set (and missing) investment path must not refuse the runtime config: %v", err)
		}
	}
	if got := strings.Count(logged.String(), warning); got != 1 {
		t.Fatalf("warning lines = %d, want exactly 1\n%s", got, logged.String())
	}
	investmentConfigPathIgnoredWarning = sync.Once{}
	logged.Reset()
	cfg.WorkerGithubWorkItemsInvestmentConfigPath = ""
	slog.Info("role posture check", "role", "queued_contract_versions") // the stray line another goroutine can log
	if _, err := githubWorkItemsRuntimeConfigFrom(cfg); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logged.String(), warning) {
		t.Fatalf("an unset path logged the warning: %s", logged.String())
	}
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func (b *lockedBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buffer.Reset()
}
