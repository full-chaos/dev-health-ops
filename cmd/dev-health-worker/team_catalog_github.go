package main

import "github.com/full-chaos/dev-health-ops/internal/jobruntime"

// githubTeamCatalogTelemetryBridge adapts jobruntime.GitHubTeamCatalogObserver
// to the narrow, package-local Observer shape providersync.
// GitHubTeamCatalogCollector expects (CHAOS-4434). providersync cannot
// depend on jobruntime directly (jobruntime already imports providersync,
// e.g. providersync.Capability -- the reverse import would cycle), so this
// process-wiring layer is the one place that bridges the two.
type githubTeamCatalogTelemetryBridge struct {
	observer jobruntime.GitHubTeamCatalogObserver
}

func (bridge githubTeamCatalogTelemetryBridge) ObserveGitHubTeamCatalogOutcome(
	outcome string, teamsWritten, membershipsWritten int,
) {
	if bridge.observer == nil {
		return
	}
	_ = bridge.observer.ObserveGitHubTeamCatalog(
		jobruntime.GitHubTeamCatalogOutcome(outcome), teamsWritten, membershipsWritten,
	)
}
