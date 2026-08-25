package providersync

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/google/uuid"
)

// CHAOS-4193(d): the github Projects v2 board-membership producer #1896
// shipped emits ADDITIONS ONLY, and only for pull requests -- an issue is
// counted and dropped (githubProjectV2MembershipSkipReason's
// "issue_deferred_to_snapshot_diff"), and nothing ever retires a membership
// once the item leaves the board. This file is the deferred half: it reads
// the durably committed membership state back from ClickHouse and diffs it
// against the board's current, fully-paginated item list, ONE READ per
// project per sync (Context Fabric ruling A case (2), 2026-08-24: "github
// snapshot-diff reads current membership from the transitions FINAL argMax
// before emitting -- no change -> emit nothing; change -> exactly one row,
// occurred_at = that diff's observation time; the table IS the last-seen
// state").
//
// Pull-request ADDITIONS are deliberately excluded from this file's output.
// Ruling A case (1) already ships that half through
// githubProjectV2MembershipRow, keyed on the board item's own createdAt --
// re-sync-stable and test-pinned. Reproducing it here would mint a SECOND,
// diff-time row for the same real event (a different occurred_at, so a
// different event_id, so ReplacingMergeTree would never collapse the two).
// Everything else this table needs -- issue additions (no prior mechanism
// existed at all) and removals of either subject kind (no prior mechanism
// existed for either) -- has exactly one source, this diff.

// githubProjectV2SnapshotSubject is one board item's durable identity, kept
// provider- and destination-neutral so it can name either a pull_request or a
// work_item subject with the same shape the `project_membership_transitions`
// sorting key uses.
type githubProjectV2SnapshotSubject struct {
	SubjectKind string
	SubjectID   string
	RepoID      uuid.UUID
}

func (subject githubProjectV2SnapshotSubject) key() string {
	return subject.SubjectKind + "\x00" + subject.RepoID.String() + "\x00" + subject.SubjectID
}

// githubProjectV2BoardSnapshot is one project's complete, identifiable
// current membership -- every PullRequest and Issue item this sync's fully
// paginated fetch returned for that board, minus whatever this sync could not
// positively identify (see githubProjectV2ItemSubject).
type githubProjectV2BoardSnapshot struct {
	ProjectScopeID string
	Subjects       []githubProjectV2SnapshotSubject
}

// githubProjectV2ItemSubject identifies the durable subject a board item
// names, for both content types this table tracks. It returns ok=false for
// DraftIssue (no subject to name at all) and for any item this sync cannot
// positively identify -- the same repository/number/identity checks
// githubProjectV2MembershipRow already applies to PullRequest, generalized to
// cover Issue's canonical work_item_id derivation too (normalizeGitHubProjectV2Item's
// own "gh:"+repo+"#"+number shape, so the subject this file names is the same
// row the column-arm path already writes to work_items.project_id).
//
// Deliberately NOT gated on the item's createdAt: unlike the PullRequest
// ADDITION path, nothing this file emits uses the item's own timestamp (ruling
// A case (2) uses the diff's observation time), so an item missing createdAt
// is still a real, present board member for snapshot purposes -- excluding it
// here would manufacture a false removal for a subject that never left.
func githubProjectV2ItemSubject(item gitHubProjectV2ItemPayload) (githubProjectV2SnapshotSubject, bool) {
	content := item.Content
	var subjectKind, subjectID string
	switch content.Typename {
	case "PullRequest":
		subjectKind, subjectID = projectmembership.SubjectPullRequest, strconv.Itoa(content.Number)
	case "Issue":
		subjectKind = projectmembership.SubjectWorkItem
	default:
		return githubProjectV2SnapshotSubject{}, false
	}
	repository := strings.TrimSpace(content.Repository.NameWithOwner)
	if repository == "" || content.Number <= 0 {
		return githubProjectV2SnapshotSubject{}, false
	}
	if content.Typename == "Issue" {
		subjectID = "gh:" + repository + "#" + strconv.Itoa(content.Number)
	}
	identity, err := repositoryIdentity(repository)
	if err != nil {
		return githubProjectV2SnapshotSubject{}, false
	}
	repoID, err := uuid.Parse(identity)
	if err != nil {
		return githubProjectV2SnapshotSubject{}, false
	}
	return githubProjectV2SnapshotSubject{SubjectKind: subjectKind, SubjectID: subjectID, RepoID: repoID}, true
}

// githubProjectV2SnapshotDiffReader is the read-side seam: the FINAL argMax
// read ruling A case (2) requires before this producer may emit anything.
// GitHubProjectV2SnapshotDiffClickHouseReader is its production
// implementation; tests substitute a fixed prior-state double so the pure
// diff below is provable without a live ClickHouse.
type githubProjectV2SnapshotDiffReader interface {
	PriorActiveSubjects(ctx context.Context, orgID, projectScopeID string) ([]githubProjectV2SnapshotSubject, error)
}

// GitHubProjectV2SnapshotDiffClickHouseReader answers "what does the table
// itself currently say is active on this board", per subject.
type GitHubProjectV2SnapshotDiffClickHouseReader struct{ Conn driver.Conn }

// gitHubProjectV2PriorActiveSubjectsQuery reproduces, for ONE project, exactly
// the `project_membership_presence` view's own `touched`/`latest_membership`
// computation (migration 077): among every row that TOUCHES this project from
// either side (from_project_id = ? OR to_project_id = ?), the latest one by
// (occurred_at, event_id) decides the subject's membership, and it is active
// here iff that latest row's own to_project_id is this project. FINAL is
// load-bearing for the identical reason the view's own comment gives: argMax
// over (occurred_at, event_id) is deterministic only because that pair is
// unique within a group, which FINAL plus the sorting key provide.
const gitHubProjectV2PriorActiveSubjectsQuery = `
SELECT subject_kind, repo_id, subject_id FROM (
    SELECT subject_kind, repo_id, subject_id,
           argMax(to_project_id, (occurred_at, event_id)) AS latest_to_project_id
    FROM project_membership_transitions FINAL
    WHERE org_id = ? AND provider = 'github' AND (from_project_id = ? OR to_project_id = ?)
    GROUP BY subject_kind, repo_id, subject_id
) WHERE latest_to_project_id = ?`

func (reader GitHubProjectV2SnapshotDiffClickHouseReader) PriorActiveSubjects(
	ctx context.Context, orgID, projectScopeID string,
) ([]githubProjectV2SnapshotSubject, error) {
	if reader.Conn == nil || ctx == nil || strings.TrimSpace(orgID) == "" ||
		strings.TrimSpace(projectScopeID) == "" {
		return nil, ErrInvalidConfiguration
	}
	result, err := reader.Conn.Query(
		ctx, gitHubProjectV2PriorActiveSubjectsQuery,
		orgID, projectScopeID, projectScopeID, projectScopeID,
	)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	subjects := []githubProjectV2SnapshotSubject{}
	for result.Next() {
		var subject githubProjectV2SnapshotSubject
		if err := result.Scan(&subject.SubjectKind, &subject.RepoID, &subject.SubjectID); err != nil {
			return nil, err
		}
		subjects = append(subjects, subject)
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	return subjects, nil
}

// githubProjectV2SnapshotDiffCounts is the observable half of the diff --
// see reportGitHubProjectV2SnapshotDiff.
type githubProjectV2SnapshotDiffCounts struct {
	IssueAdditions int
	Removals       int
}

// diffGitHubProjectV2Snapshot is the pure half: the read is already done, and
// this decides, per subject, whether anything changed.
//
// Additions are restricted to work_item subjects -- see the file doc comment
// for why pull_request additions are excluded. Removals apply to EITHER
// subject kind: nothing else in this producer retires a membership for
// either, so this is their only source. A subject present in both current and
// prior contributes NOTHING, which is the ruled "no change -> emit nothing":
// re-running this diff every sync on an unchanged board must not accumulate a
// row per sync.
func diffGitHubProjectV2Snapshot(
	claim Claim,
	projectScopeID string,
	current, prior []githubProjectV2SnapshotSubject,
	normalizedAt time.Time,
) ([]projectmembership.Row, githubProjectV2SnapshotDiffCounts) {
	currentSet := make(map[string]struct{}, len(current))
	for _, subject := range current {
		currentSet[subject.key()] = struct{}{}
	}
	priorSet := make(map[string]struct{}, len(prior))
	for _, subject := range prior {
		priorSet[subject.key()] = struct{}{}
	}
	rows := []projectmembership.Row{}
	counts := githubProjectV2SnapshotDiffCounts{}
	for _, subject := range current {
		if subject.SubjectKind != projectmembership.SubjectWorkItem {
			continue
		}
		if _, active := priorSet[subject.key()]; active {
			continue
		}
		row := projectmembership.Row{
			OrgID: claim.OrgID, SubjectKind: subject.SubjectKind, SubjectID: subject.SubjectID,
			RepoID: subject.RepoID, Provider: "github",
			ToProjectID: projectScopeID, OccurredAt: normalizedAt.UTC(), LastSynced: normalizedAt.UTC(),
		}
		row.EventID = projectmembership.EventID(row)
		rows = append(rows, row)
		counts.IssueAdditions++
	}
	for _, subject := range prior {
		if _, present := currentSet[subject.key()]; present {
			continue
		}
		row := projectmembership.Row{
			OrgID: claim.OrgID, SubjectKind: subject.SubjectKind, SubjectID: subject.SubjectID,
			RepoID: subject.RepoID, Provider: "github",
			FromProjectID: projectScopeID, OccurredAt: normalizedAt.UTC(), LastSynced: normalizedAt.UTC(),
		}
		row.EventID = projectmembership.EventID(row)
		rows = append(rows, row)
		counts.Removals++
	}
	return rows, counts
}

// resolveGitHubProjectV2SnapshotDiff runs the ruled read-then-diff for every
// board this sync fetched and reports the aggregate. It is the route's only
// call site into this file.
func resolveGitHubProjectV2SnapshotDiff(
	ctx context.Context,
	reader githubProjectV2SnapshotDiffReader,
	claim Claim,
	snapshots []githubProjectV2BoardSnapshot,
	normalizedAt time.Time,
) ([]projectmembership.Row, error) {
	rows := []projectmembership.Row{}
	totals := githubProjectV2SnapshotDiffCounts{}
	for _, snapshot := range snapshots {
		prior, err := reader.PriorActiveSubjects(ctx, claim.OrgID, snapshot.ProjectScopeID)
		if err != nil {
			return nil, err
		}
		diffRows, counts := diffGitHubProjectV2Snapshot(
			claim, snapshot.ProjectScopeID, snapshot.Subjects, prior, normalizedAt,
		)
		rows = append(rows, diffRows...)
		totals.IssueAdditions += counts.IssueAdditions
		totals.Removals += counts.Removals
	}
	reportGitHubProjectV2SnapshotDiff(claim, totals)
	return rows, nil
}

// reportGitHubProjectV2SnapshotDiff is this file's structured-log counterpart
// to reportGitHubProjectV2MembershipSkips: this collector owns no Prometheus
// registration (D18), so a count nothing reads is a change nobody can see. As
// there, silence on an unchanged board is the intended common case and is not
// logged, and a reason with no occurrences this sync is omitted rather than
// written as zero.
func reportGitHubProjectV2SnapshotDiff(claim Claim, counts githubProjectV2SnapshotDiffCounts) {
	if counts.IssueAdditions == 0 && counts.Removals == 0 {
		return
	}
	attrs := []any{
		"provider", claim.Provider, "dataset", claim.Dataset,
		"org_id", claim.OrgID, "unit", claim.ID, "integration", claim.IntegrationID,
	}
	if counts.IssueAdditions > 0 {
		attrs = append(attrs, "issue_additions_emitted", int64(counts.IssueAdditions))
	}
	if counts.Removals > 0 {
		attrs = append(attrs, "removals_emitted", int64(counts.Removals))
	}
	slog.Log(context.Background(), slog.LevelInfo, "github_projects_v2.snapshot_diff", attrs...)
}
