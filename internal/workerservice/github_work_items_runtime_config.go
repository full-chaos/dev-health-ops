package workerservice

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// workItemsRuntimeConfig is the exact, already-validated configuration the
// production provider-unit factory closes over for every native work-item
// family. The existing WORKER_GITHUB_WORK_ITEMS_* path names remain the frozen
// deployment contract, but the artifacts themselves are provider-neutral.
//
// The status-mapping port intentionally mirrors Python's STATUS_MAPPING_PATH
// override for its generic oracle surface. That override is not admissible in
// the native worker: allowing it here would make readiness validate one file
// while execution reads another. Test-environment isolation scrubs it; this
// production boundary rejects a surviving ambient value instead of relying on
// that external hygiene alone (D19).
type workItemsRuntimeConfig struct {
	statusMappingPath string
	statusMapping     *providersync.StatusMapping
}

func (runtimeConfig workItemsRuntimeConfig) configured() bool {
	return runtimeConfig.statusMappingPath != "" && runtimeConfig.statusMapping != nil
}

// investmentConfigPathIgnoredWarning logs, once per process, that the deprecated investment config path is set
// and ignored: the rule classifier it configured is gone (CHAOS-9200), and a silent no-op would leave an operator
// believing the file still matters.
var investmentConfigPathIgnoredWarning sync.Once

func workItemsRuntimeConfigFrom(
	cfg config.Config,
) (workItemsRuntimeConfig, error) {
	if strings.TrimSpace(cfg.WorkerGithubWorkItemsInvestmentConfigPath) != "" {
		investmentConfigPathIgnoredWarning.Do(func() {
			slog.Warn("WORKER_GITHUB_WORK_ITEMS_INVESTMENT_CONFIG_PATH is set and ignored: the investment rule classifier was removed",
				"setting", "WORKER_GITHUB_WORK_ITEMS_INVESTMENT_CONFIG_PATH")
		})
	}
	// LoadStatusMapping tests the raw environment value, not its trimmed form.
	// A whitespace-only override would still replace the explicit path at
	// execution time, so reject every nonempty ambient value here.
	if os.Getenv("STATUS_MAPPING_PATH") != "" {
		return workItemsRuntimeConfig{}, fmt.Errorf(
			"%w: native work-items rejects ambient status mapping overrides",
			providersync.ErrInvalidConfiguration,
		)
	}
	runtimeConfig := workItemsRuntimeConfig{
		statusMappingPath: strings.TrimSpace(
			cfg.WorkerGithubWorkItemsStatusMappingPath,
		),
	}
	if runtimeConfig.statusMappingPath == "" {
		return workItemsRuntimeConfig{}, fmt.Errorf(
			"%w: native work-items requires an explicit status mapping path",
			providersync.ErrInvalidConfiguration,
		)
	}
	// The status mapping must exist as a regular file before either readiness
	// or executor construction can admit the family. (The investment rule file
	// is no longer read: the rule classifier is gone, CHAOS-9200; its path
	// setting is accepted and ignored so a deployment that still sets it starts.)
	for _, path := range []string{
		runtimeConfig.statusMappingPath,
	} {
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			return workItemsRuntimeConfig{}, fmt.Errorf(
				"%w: native work-items configuration artifact is unavailable",
				providersync.ErrInvalidConfiguration,
			)
		}
	}
	// Validate the configuration artifact before a readiness check can
	// report the route healthy. The per-claim deriver reloads the same explicit
	// paths to retain ported parsing behavior; this eager validation closes the
	// startup/configuration hole before any provider connection is opened.
	statusMapping, err := providersync.LoadStatusMapping(runtimeConfig.statusMappingPath)
	if err != nil {
		return workItemsRuntimeConfig{}, fmt.Errorf(
			"%w: native work-items status mapping is unavailable",
			providersync.ErrInvalidConfiguration,
		)
	}
	runtimeConfig.statusMapping = statusMapping
	return runtimeConfig, nil
}

// Compatibility aliases keep older focused tests and helper call sites small
// while production construction uses the provider-neutral names above.
type githubWorkItemsRuntimeConfig = workItemsRuntimeConfig

func githubWorkItemsRuntimeConfigFrom(cfg config.Config) (workItemsRuntimeConfig, error) {
	return workItemsRuntimeConfigFrom(cfg)
}

// githubProjectsV2EnvironmentNeedsStartupWarning keeps D18's legacy process
// setting outside the collector hot path. It only asks startup/readiness for a
// durable integration-config census when the old environment variable is
// nonempty. A durable (including explicitly empty) config wins; an env-only
// setting is ignored by the Go route and must be visible to operators.
func githubProjectsV2EnvironmentNeedsStartupWarning(
	ctx context.Context,
	durableConfigured func(context.Context) (bool, error),
) (bool, error) {
	if os.Getenv("GITHUB_PROJECTS_V2") == "" {
		return false, nil
	}
	if ctx == nil || durableConfigured == nil {
		return false, errWorkerDependencyUnavailable
	}
	configured, err := durableConfigured(ctx)
	if err != nil {
		return false, errWorkerDependencyUnavailable
	}
	return !configured, nil
}
