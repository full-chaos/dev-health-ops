//go:build integration

package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestBuildProviderSyncWorkerConstructsRealDependenciesForEveryRouteReadySwitch(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	clickhouse, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickhouse.Close(context.Background()) })
	valkey, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = valkey.Close(context.Background()) })
	registry, err := jobruntime.Load(filepath.Join("contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{Profiles: []string{"sync"}})
	if err != nil {
		t.Fatal(err)
	}
	for name, enable := range map[string]func(*config.Config){
		"launchdarkly_feature_flags": func(cfg *config.Config) { cfg.WorkerLaunchDarklyFeatureFlagsEnabled = true },
		"github_repo_metadata":       func(cfg *config.Config) { cfg.WorkerGithubRepoMetadataEnabled = true },
		"gitlab_repo_metadata":       func(cfg *config.Config) { cfg.WorkerGitlabRepoMetadataEnabled = true },
		"gitlab_commits":             func(cfg *config.Config) { cfg.WorkerGitlabCommitsEnabled = true },
		"gitlab_commit_stats":        func(cfg *config.Config) { cfg.WorkerGitlabCommitStatsEnabled = true },
		"gitlab_cicd":                func(cfg *config.Config) { cfg.WorkerGitlabCICDEnabled = true },
		"gitlab_tests":               func(cfg *config.Config) { cfg.WorkerGitlabTestsEnabled = true },
		"gitlab_incidents":           func(cfg *config.Config) { cfg.WorkerGitlabIncidentsEnabled = true },
		"github_prs":                 func(cfg *config.Config) { cfg.WorkerGithubPRsEnabled = true },
		"github_pr_reviews":          func(cfg *config.Config) { cfg.WorkerGithubPRReviewsEnabled = true },
		"github_pr_comments":         func(cfg *config.Config) { cfg.WorkerGithubPRCommentsEnabled = true },
		"github_cicd":                func(cfg *config.Config) { cfg.WorkerGithubCICDEnabled = true },
		"github_commits":             func(cfg *config.Config) { cfg.WorkerGithubCommitsEnabled = true },
		"github_deployments":         func(cfg *config.Config) { cfg.WorkerGithubDeploymentsEnabled = true },
		"github_security":            func(cfg *config.Config) { cfg.WorkerGithubSecurityEnabled = true },
		"github_files":               func(cfg *config.Config) { cfg.WorkerGithubFilesEnabled = true },
		"github_commit_stats":        func(cfg *config.Config) { cfg.WorkerGithubCommitStatsEnabled = true },
		"github_blame":               func(cfg *config.Config) { cfg.WorkerGithubBlameEnabled = true },
		"github_tests":               func(cfg *config.Config) { cfg.WorkerGithubTestsEnabled = true },
		"github_work_items": func(cfg *config.Config) {
			cfg.WorkerGithubWorkItemsEnabled = true
			cfg.WorkerGithubWorkItemsStatusMappingPath = filepath.Join(
				"src", "dev_health_ops", "config", "status_mapping.yaml",
			)
			cfg.WorkerGithubWorkItemsInvestmentConfigPath = filepath.Join(
				"src", "dev_health_ops", "config", "investment_areas.yaml",
			)
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := config.Config{
				Profile:               "sync",
				RiverDatabaseSchema:   "river",
				SettingsEncryptionKey: secrets.NewValue("test-encryption-key"),
				ClickHouseURI:         secrets.NewValue(clickhouse.URI),
				ValkeyURI:             secrets.NewValue(valkey.URI),
			}
			enable(&cfg)
			family, err := buildProviderSyncWorker(
				ctx, cfg, reportBuilderDatabase(t), registry, collector, slog.Default(),
			)
			if err != nil {
				t.Fatal(err)
			}
			if family.component == nil || len(family.handlers) != 1 || len(family.queues) != 1 {
				t.Fatalf("provider sync family=%#v", family)
			}
			if err := family.component.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
