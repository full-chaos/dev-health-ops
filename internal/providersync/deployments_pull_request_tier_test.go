package providersync

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func tierPulls(t *testing.T, raw string) []gitHubPullPayload {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var pulls []gitHubPullPayload
	if err := decoder.Decode(&pulls); err != nil {
		t.Fatal(err)
	}
	return pulls
}

// Each rule of the GitHub choice stores its own tier, and the chosen number
// and merged_at are the ones the choice returned before the tier existed.
func TestChooseDeploymentPullRequestStoresTheRuleThatMatched(t *testing.T) {
	t.Parallel()
	const sha = "abc"
	cases := []struct {
		name       string
		pulls      string
		wantNumber *int
		wantMerged *time.Time
		wantTier   string
	}{
		{
			name:       "exact merge sha wins over an earlier merged pull request",
			pulls:      `[{"number":10,"merged_at":"2026-07-20T10:00:00Z","merge_commit_sha":"other"},{"number":11,"merged_at":"2026-07-21T10:00:00Z","merge_commit_sha":"abc"}]`,
			wantNumber: intPointer(11), wantMerged: deploymentTimePointer(time.Date(2026, 7, 21, 10, 0, 0, 0, time.UTC)), wantTier: pullRequestTierExactMergeSHA,
		},
		{
			name:       "first merged pull request when no merge sha matches",
			pulls:      `[{"number":9,"merged_at":null,"merge_commit_sha":"abc"},{"number":10,"merged_at":"2026-07-20T10:00:00Z","merge_commit_sha":"other"},{"number":12,"merged_at":"2026-07-22T10:00:00Z","merge_commit_sha":"x"}]`,
			wantNumber: intPointer(10), wantMerged: deploymentTimePointer(time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)), wantTier: pullRequestTierMergedPR,
		},
		{
			name:       "first listed pull request when none is merged",
			pulls:      `[{"number":7,"merged_at":null},{"number":8,"merged_at":null}]`,
			wantNumber: intPointer(7), wantMerged: nil, wantTier: pullRequestTierFirstListed,
		},
		{name: "no pull request stores no tier", pulls: `[]`},
		{name: "a number that does not parse stores no tier", pulls: `[{"number":"x","merged_at":null}]`},
	}
	for _, tc := range cases {
		number, merged, tier := chooseDeploymentPullRequest(tierPulls(t, tc.pulls), sha)
		if !intPointersEqual(number, tc.wantNumber) || !timePointersEqual(merged, tc.wantMerged) || tier != tc.wantTier {
			t.Errorf("%s: number=%v merged=%v tier=%q, want %v %v %q", tc.name, derefInt(number), merged, tier, derefInt(tc.wantNumber), tc.wantMerged, tc.wantTier)
		}
	}
}

// The per-run DEBUG line carries counts per tier and no id or text.
func TestDeploymentPullRequestTiersLogCountsOnly(t *testing.T) {
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buffer, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	claim := nativeTestClaim("github", "deployments")
	logDeploymentPullRequestTiers(claim, []deploymentRow{
		{DeploymentID: "secret-1", PullRequestTier: pullRequestTierExactMergeSHA},
		{DeploymentID: "secret-2", PullRequestTier: pullRequestTierExactMergeSHA},
		{DeploymentID: "secret-3", PullRequestTier: pullRequestTierFirstListed},
		{DeploymentID: "secret-4", PullRequestLookupFailed: true},
	})
	line := buffer.String()
	for _, want := range []string{deploymentPullRequestTiersEvent, "exact_merge_sha=2", "merged_pr=0", "first_listed=1", "no_link=1", "lookup_failed=1", "rows=4"} {
		if !strings.Contains(line, want) {
			t.Errorf("log %q lacks %q", line, want)
		}
	}
	if strings.Contains(line, "secret") {
		t.Errorf("log carries a deployment id: %q", line)
	}
}
