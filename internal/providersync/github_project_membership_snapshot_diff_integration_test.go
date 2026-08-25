//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestGitHubProjectV2SnapshotDiffAddsIssueAndRetiresARemovedSubjectFromPresence
// is CHAOS-4193(d)'s reachability proof, in the same shape
// TestGitHubProjectsV2PullRequestReachesClickHouseThroughTheEffectPath used
// for CHAOS-4194: nothing between the provider payload and the stored row (or
// its disappearance) is stubbed.
//
// It seeds TWO board snapshots against the SAME live ClickHouse: the first
// sync's board carries an issue and a pull request; the second sync's board
// has dropped the issue. Ruling A case (2) is what makes the second sync's
// row exist at all -- github's Projects v2 API has no "removed from board"
// event, so the only way to know the issue left is to read what the table
// already believes (prior sync's committed row) and diff it against the
// current, fully paginated fetch.
//
// Both halves of the ticket's scope are proven end to end:
//   - issue membership: no row exists for the issue after sync 1 unless this
//     producer positively adds one (githubProjectV2MembershipRow never emits
//     Issue rows at all).
//   - removal: project_membership_presence must ANSWER FOR the issue after
//     sync 1 and STOP ANSWERING after sync 2, which is the presence view's own
//     "the last thing that happened to this membership decides" contract
//     (migration 077), asked of the graph rather than of any struct along the
//     way.
func TestGitHubProjectV2SnapshotDiffAddsIssueAndRetiresARemovedSubjectFromPresence(t *testing.T) {
	ctx, conn := newProjectMembershipEffectsConn(t)
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
		map[string]any{"org_login": "acme", "project_number": 3},
	}}
	credential := providerfoundation.Credential{Provider: "github", ID: claim.CredentialID}
	reader := GitHubProjectV2SnapshotDiffClickHouseReader{Conn: conn}
	sink, err := NewGitHubWorkItemClickHouseEffects(
		conn, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	derived := map[string][]json.RawMessage{}
	for _, destination := range githubWorkItemDerivedDestinations {
		derived[destination] = []json.RawMessage{}
	}

	// Sync 1: the board carries an issue (#7) and a pull request (#42).
	firstSyncAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	firstDoer := &gitHubProjectV2Doer{t: t, replies: []string{
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[` +
			`{"id":"PVTI_ISSUE","content":{"__typename":"Issue","number":7,"title":"Ship it","state":"OPEN","repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[]},"assignees":{"nodes":[]}},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}},` +
			`{"id":"PVTI_PR","createdAt":"2026-08-01T08:00:00Z","content":{"__typename":"PullRequest","number":42,"title":"A PR","repository":{"nameWithOwner":"acme/api"}},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}` +
			`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
	}}
	firstFetch, err := (GitHubProjectV2Fetcher{}).Fetch(
		ctx, claim, credential, githubProjectV2TestClient(t, firstDoer), firstSyncAt, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstFetch.Snapshots) != 1 || len(firstFetch.Snapshots[0].Subjects) != 2 {
		t.Fatalf("premise failed, sync 1 snapshot=%+v", firstFetch.Snapshots)
	}
	firstDiff, err := resolveGitHubProjectV2SnapshotDiff(ctx, reader, claim, firstFetch.Snapshots, firstSyncAt)
	if err != nil {
		t.Fatal(err)
	}
	// The issue has no prior state (this is the first sync ever), so the diff
	// must add it; the PR is untouched here -- its addition is ruling A case
	// (1), already in firstFetch.Rows.ProjectMemberships.
	if len(firstDiff) != 1 || firstDiff[0].SubjectKind != "work_item" || firstDiff[0].ToProjectID != "ghprojv2:acme#3" {
		t.Fatalf("sync 1 diff=%+v, want exactly one work_item addition", firstDiff)
	}
	firstFetch.Rows.ProjectMemberships = append(firstFetch.Rows.ProjectMemberships, firstDiff...)
	if len(firstFetch.Rows.ProjectMemberships) != 2 {
		t.Fatalf("sync 1 memberships=%+v, want the PR add plus the issue add", firstFetch.Rows.ProjectMemberships)
	}
	firstEffects, err := buildGitHubWorkItemsRouteEffects(firstFetch.Rows, derived)
	if err != nil {
		t.Fatal(err)
	}
	for _, effect := range firstEffects {
		if effect.Destination != "project_membership_transitions" && effect.Destination != "projects" {
			continue
		}
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatalf("write %s: %v", effect.Destination, err)
		}
		if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
			t.Fatalf("readback of %s = %v (err=%v), want EffectExact", effect.Destination, inspection, err)
		}
	}

	issueSubjectID := "gh:acme/api#7"
	var presenceCount uint64
	if err := conn.QueryRow(ctx, `
SELECT count() FROM project_membership_presence
WHERE org_id = ? AND subject_kind = 'work_item' AND subject_id = ?`,
		claim.OrgID, issueSubjectID,
	).Scan(&presenceCount); err != nil {
		t.Fatal(err)
	}
	if presenceCount != 1 {
		t.Fatalf("the issue has no presence edge after sync 1: count=%d", presenceCount)
	}

	// Sync 2: the board no longer carries the issue -- only the pull request
	// remains. Nothing in the GitHub API told us the issue left; the only
	// signal is its absence from this sync's complete, fully paginated list.
	secondSyncAt := firstSyncAt.Add(24 * time.Hour)
	secondDoer := &gitHubProjectV2Doer{t: t, replies: []string{
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[` +
			`{"id":"PVTI_PR","createdAt":"2026-08-01T08:00:00Z","content":{"__typename":"PullRequest","number":42,"title":"A PR","repository":{"nameWithOwner":"acme/api"}},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}` +
			`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
	}}
	secondFetch, err := (GitHubProjectV2Fetcher{}).Fetch(
		ctx, claim, credential, githubProjectV2TestClient(t, secondDoer), secondSyncAt, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondDiff, err := resolveGitHubProjectV2SnapshotDiff(ctx, reader, claim, secondFetch.Snapshots, secondSyncAt)
	if err != nil {
		t.Fatal(err)
	}
	// The table now believes both subjects are active (sync 1 landed both);
	// the current board only shows the PR. Exactly one removal, naming the
	// issue -- the PR is unaffected because it is still present.
	if len(secondDiff) != 1 || secondDiff[0].SubjectKind != "work_item" ||
		secondDiff[0].SubjectID != issueSubjectID || secondDiff[0].FromProjectID != "ghprojv2:acme#3" ||
		secondDiff[0].ToProjectID != "" {
		t.Fatalf("sync 2 diff=%+v, want exactly one work_item removal naming the issue", secondDiff)
	}
	secondFetch.Rows.ProjectMemberships = append(secondFetch.Rows.ProjectMemberships, secondDiff...)
	secondEffects, err := buildGitHubWorkItemsRouteEffects(secondFetch.Rows, derived)
	if err != nil {
		t.Fatal(err)
	}
	for _, effect := range secondEffects {
		if effect.Destination != "project_membership_transitions" && effect.Destination != "projects" {
			continue
		}
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatalf("write %s: %v", effect.Destination, err)
		}
		if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
			t.Fatalf("readback of %s = %v (err=%v), want EffectExact", effect.Destination, inspection, err)
		}
	}

	// The acceptance sentence: the removed subject's presence row DISAPPEARS.
	if err := conn.QueryRow(ctx, `
SELECT count() FROM project_membership_presence
WHERE org_id = ? AND subject_kind = 'work_item' AND subject_id = ?`,
		claim.OrgID, issueSubjectID,
	).Scan(&presenceCount); err != nil {
		t.Fatal(err)
	}
	if presenceCount != 0 {
		t.Fatalf("the issue still has a presence edge after its removal: count=%d", presenceCount)
	}

	// The PR, never removed, must still answer.
	var prPresenceCount uint64
	if err := conn.QueryRow(ctx, `
SELECT count() FROM project_membership_presence
WHERE org_id = ? AND subject_kind = 'pull_request' AND subject_id = '42'`,
		claim.OrgID,
	).Scan(&prPresenceCount); err != nil {
		t.Fatal(err)
	}
	if prPresenceCount != 1 {
		t.Fatalf("the untouched PR lost its presence edge: count=%d", prPresenceCount)
	}

	// Re-running the diff against the now-converged state must emit NOTHING:
	// "no change -> emit nothing" (ruling A case (2)) is what keeps an
	// unchanged board from accumulating a row every sync.
	thirdDiff, err := resolveGitHubProjectV2SnapshotDiff(ctx, reader, claim, secondFetch.Snapshots, secondSyncAt.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(thirdDiff) != 0 {
		t.Fatalf("an unchanged board produced a diff: %+v", thirdDiff)
	}
}
