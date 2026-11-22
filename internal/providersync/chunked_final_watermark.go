package providersync

import (
	"log/slog"
	"time"
)

// publishedFinalWatermark is the watermark the streaming executor publishes for a durable final chunk: the stored one,
// clamped by the route's rule. The rule can only withhold or lower it: a stored nil stays nil, a rule nil withholds,
// otherwise the earlier of the two is published. A final chunk an older binary prepared but did not publish is
// published from durable state without calling the route, so without this clamp it carried the older rule's watermark
// past a newer rule that withholds it (CHAOS-14422). A rule error withholds; the unit still completes and the next
// window reads the range again.
func publishedFinalWatermark(claim Claim, rule ChunkFinalWatermarkRule, final PreparedProviderChunk) *time.Time {
	stored := final.Watermark
	if rule == nil || stored == nil {
		return stored
	}
	derived, err := rule.FinalWatermark(claim, final)
	if err != nil {
		slog.Warn("final chunk watermark withheld",
			"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
			"reason", "route_rule_failed")
		return nil
	}
	if derived != nil && !derived.Before(*stored) {
		return stored
	}
	published := "withheld"
	if derived != nil {
		published = derived.UTC().Format(time.RFC3339Nano)
	}
	slog.Warn("final chunk watermark lowered by the route rule",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"reason", "route_rule_lowered",
		"stored_watermark", stored.UTC().Format(time.RFC3339Nano), "published_watermark", published)
	if derived == nil {
		return nil
	}
	lowered := derived.UTC()
	return &lowered
}

// FinalWatermark is the GitLab tests/cicd rule (gitLabTestsWatermark) applied to the final chunk's done cursor.
func (GitLabTestsRouteHandler) FinalWatermark(claim Claim, final PreparedProviderChunk) (*time.Time, error) {
	cursor, err := decodeGitLabTestsChunkCursor(final.CursorAfter)
	if err != nil {
		return nil, err
	}
	return gitLabTestsWatermark(cursor, claim), nil
}

// FinalWatermark is the GitHub tests/cicd rule (githubTestsBlocksWatermark) applied to the evidence the final chunk's
// Result carries, decoded as the production comparator decodes it (validateGitHubTestsCompletion).
func (GitHubTestsRouteHandler) FinalWatermark(claim Claim, final PreparedProviderChunk) (*time.Time, error) {
	var incomplete []GitHubTestsIncomplete
	if err := decodeCompletionValue(final.Result, GitHubTestsResultKeyIncomplete, &incomplete); err != nil {
		return nil, err
	}
	if githubTestsBlocksWatermark(
		incomplete,
		decodeOptionalGitHubTestsSkippedArtifacts(final.Result),
		decodeOptionalGitHubTestsSkippedArtifactsOverflow(final.Result),
		decodeOptionalGitHubTestsSkippedArtifactCauseOverflow(final.Result),
		decodeOptionalGitHubTestsSkippedArtifactCauseCount(final.Result),
	) {
		return nil, nil
	}
	return claim.BeforeAt, nil
}

// FinalWatermark is the GitHub files rule: a whole snapshot claims BeforeAt (gitHubFilesFinalBatch).
func (GitHubFilesRouteHandler) FinalWatermark(claim Claim, _ PreparedProviderChunk) (*time.Time, error) {
	return claim.BeforeAt, nil
}

var (
	_ ChunkFinalWatermarkRule = GitLabTestsRouteHandler{}
	_ ChunkFinalWatermarkRule = GitHubTestsRouteHandler{}
	_ ChunkFinalWatermarkRule = GitHubFilesRouteHandler{}
)
