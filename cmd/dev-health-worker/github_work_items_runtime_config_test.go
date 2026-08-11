package main

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

func githubWorkItemsConfigRepoRoot(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller did not report this test file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
}

func validGitHubWorkItemsRuntimeConfig(t *testing.T) config.Config {
	t.Helper()
	root := githubWorkItemsConfigRepoRoot(t)
	return config.Config{
		WorkerGithubWorkItemsEnabled: true,
		WorkerGithubWorkItemsStatusMappingPath: filepath.Join(
			root, "src", "dev_health_ops", "config", "status_mapping.yaml",
		),
		WorkerGithubWorkItemsInvestmentConfigPath: filepath.Join(
			root, "src", "dev_health_ops", "config", "investment_areas.yaml",
		),
	}
}

func TestGitHubWorkItemsRuntimeConfigRequiresValidatedExplicitPaths(t *testing.T) {
	t.Setenv("STATUS_MAPPING_PATH", "")
	valid := validGitHubWorkItemsRuntimeConfig(t)
	runtimeConfig, err := githubWorkItemsRuntimeConfigFrom(valid)
	if err != nil {
		t.Fatal(err)
	}
	if !runtimeConfig.configured() ||
		runtimeConfig.statusMappingPath != valid.WorkerGithubWorkItemsStatusMappingPath ||
		runtimeConfig.investmentConfigPath != valid.WorkerGithubWorkItemsInvestmentConfigPath {
		t.Fatalf("runtime config = %+v, config = %+v", runtimeConfig, valid)
	}

	root := githubWorkItemsConfigRepoRoot(t)
	for name, cfg := range map[string]config.Config{
		"missing status path": {
			WorkerGithubWorkItemsEnabled:              true,
			WorkerGithubWorkItemsInvestmentConfigPath: valid.WorkerGithubWorkItemsInvestmentConfigPath,
		},
		"blank investment path": {
			WorkerGithubWorkItemsEnabled:              true,
			WorkerGithubWorkItemsStatusMappingPath:    valid.WorkerGithubWorkItemsStatusMappingPath,
			WorkerGithubWorkItemsInvestmentConfigPath: " ",
		},
		"missing status file": {
			WorkerGithubWorkItemsEnabled:              true,
			WorkerGithubWorkItemsStatusMappingPath:    filepath.Join(root, "missing-status.yaml"),
			WorkerGithubWorkItemsInvestmentConfigPath: valid.WorkerGithubWorkItemsInvestmentConfigPath,
		},
		"malformed status file": {
			WorkerGithubWorkItemsEnabled: true,
			WorkerGithubWorkItemsStatusMappingPath: filepath.Join(
				root, "internal", "providersync", "testdata", "status_mapping_configs", "structural_root_list.yaml",
			),
			WorkerGithubWorkItemsInvestmentConfigPath: valid.WorkerGithubWorkItemsInvestmentConfigPath,
		},
		"missing investment file": {
			WorkerGithubWorkItemsEnabled:              true,
			WorkerGithubWorkItemsStatusMappingPath:    valid.WorkerGithubWorkItemsStatusMappingPath,
			WorkerGithubWorkItemsInvestmentConfigPath: filepath.Join(root, "missing-investment.yaml"),
		},
		"malformed investment file": {
			WorkerGithubWorkItemsEnabled:           true,
			WorkerGithubWorkItemsStatusMappingPath: valid.WorkerGithubWorkItemsStatusMappingPath,
			WorkerGithubWorkItemsInvestmentConfigPath: filepath.Join(
				root, "internal", "providersync", "testdata", "investment_configs", "raises_empty.yaml",
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

	disabled, err := githubWorkItemsRuntimeConfigFrom(config.Config{})
	if err != nil || disabled.configured() {
		t.Fatalf("disabled config = %+v, error = %v", disabled, err)
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
