package providersync

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitLabPullRequestClickHouseEffects is the provider-bound direct sink for
// gitlab/prs. It reuses the exact whole-row FINAL readback implementation
// used by GitHub while fixing the provider binding at construction time.
type GitLabPullRequestClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitLabPullRequestClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	return (GitHubPullRequestClickHouseEffects{
		Conn: sink.Conn, Lease: sink.Lease, Provider: "gitlab",
	}).writePullRequestEffect(ctx, claim, effect, "prs")
}

func (sink GitLabPullRequestClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	return (GitHubPullRequestClickHouseEffects{
		Conn: sink.Conn, Lease: sink.Lease, Provider: "gitlab",
	}).inspectPullRequestEffect(ctx, claim, effect, "prs")
}

// GitLabPullRequestSocialClickHouseEffects is the complete sink for all
// three GitLab PR-social aliases. The outer claim remains the alias selected
// by the unit, while both durable destinations are written under that same
// lease and generation.
type GitLabPullRequestSocialClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitLabPullRequestSocialClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	return (GitHubPullRequestSocialClickHouseEffects{
		Conn: sink.Conn, Lease: sink.Lease, Provider: "gitlab",
	}).WriteEffect(ctx, claim, effect)
}

func (sink GitLabPullRequestSocialClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	return (GitHubPullRequestSocialClickHouseEffects{
		Conn: sink.Conn, Lease: sink.Lease, Provider: "gitlab",
	}).InspectEffect(ctx, claim, effect)
}

var _ EffectSink = GitLabPullRequestClickHouseEffects{}
var _ EffectReadback = GitLabPullRequestClickHouseEffects{}
var _ EffectSink = GitLabPullRequestSocialClickHouseEffects{}
var _ EffectReadback = GitLabPullRequestSocialClickHouseEffects{}
