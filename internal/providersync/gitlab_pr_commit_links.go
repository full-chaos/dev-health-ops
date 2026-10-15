package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	prCommitLinkDestination = "work_graph_pr_commit"
	prCommitLinkProvenance  = "native"
	prCommitLinkEvidence    = "api_pr_commits"

	prCommitLinkInsert = `
INSERT INTO work_graph_pr_commit (
  repo_id, pr_number, commit_hash, confidence, provenance, evidence, last_synced, org_id
)`
)

// prCommitLinkRow is one native merge request -> commit link, the provider's
// own statement of which commits a merge request holds.
type prCommitLinkRow struct {
	RepoID     string    `json:"repo_id"`
	PRNumber   int       `json:"pr_number"`
	CommitHash string    `json:"commit_hash"`
	Confidence float32   `json:"confidence"`
	Provenance string    `json:"provenance"`
	Evidence   string    `json:"evidence"`
	LastSynced time.Time `json:"last_synced"`
	OrgID      string    `json:"org_id"`
}

func (row prCommitLinkRow) validate(claim Claim) error {
	if claim.Provider != "gitlab" || !isPRSocialDataset(claim.Dataset) ||
		row.OrgID == "" || row.OrgID != claim.OrgID || len(row.RepoID) != 36 ||
		row.PRNumber < 1 || row.CommitHash == "" || row.Confidence != 1 ||
		row.Provenance != prCommitLinkProvenance || row.Evidence != prCommitLinkEvidence ||
		row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

type gitLabMergeRequestCommitPayload struct {
	ID any `json:"id"`
}

// fetchGitLabMergeRequestCommitLinks lists the commits of one merge request
// (one request per page of perPage commits) and returns one native link per
// commit. An ordinary failure of the listing leaves the merge request without
// links for this pass, as does a malformed listing item (the commit-stats
// route treats a malformed detail payload the same way); control-plane failures (lease, budget, cancellation)
// still fail the unit.
func fetchGitLabMergeRequestCommitLinks(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	claim Claim,
	projectID string,
	iid int,
	repoID string,
	normalizedAt time.Time,
	perPage, maxPages int,
) ([]prCommitLinkRow, int, error) {
	path := providerRelativePath(client, "api", "v4", "projects", projectID, "merge_requests", strconv.Itoa(iid), "commits")
	items, pages, capReached, err := collectGitLabRawPages(ctx, client, gitlabMergeRequestsRoute+"/{iid}/commits", path, url.Values{}, perPage, maxPages, nil, nil)
	if err != nil {
		if fatalErr := gitLabCommitStatsFatalDetailError(ctx, err); fatalErr != nil {
			return nil, pages, fatalErr
		}
		if controlErr := gitLabPullRequestControlError(ctx, err); controlErr != nil {
			return nil, pages, controlErr
		}
		return nil, pages, nil
	}
	if capReached {
		return nil, pages, ErrPaginationCapExceeded
	}
	seen := make(map[string]struct{}, len(items))
	links := make([]prCommitLinkRow, 0, len(items))
	for _, raw := range items {
		var payload gitLabMergeRequestCommitPayload
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if decoder.Decode(&payload) != nil || decoder.Decode(&struct{}{}) != io.EOF {
			return nil, pages, nil
		}
		hash := strings.TrimSpace(stringValue(payload.ID))
		if hash == "" {
			return nil, pages, nil
		}
		if _, dup := seen[hash]; dup {
			continue
		}
		seen[hash] = struct{}{}
		row := prCommitLinkRow{
			RepoID: repoID, PRNumber: iid, CommitHash: hash, Confidence: 1,
			Provenance: prCommitLinkProvenance, Evidence: prCommitLinkEvidence,
			LastSynced: normalizedAt, OrgID: claim.OrgID,
		}
		if err := row.validate(claim); err != nil {
			return nil, pages, err
		}
		links = append(links, row)
	}
	return links, pages, nil
}

func (sink GitHubPullRequestSocialClickHouseEffects) writePRCommitLinks(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if sink.provider() != "gitlab" {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	rows, err := decodeEffectRows[prCommitLinkRow](effect)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := row.validate(claim); err != nil {
			return err
		}
	}
	if len(rows) == 0 {
		return nil
	}
	if sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	batch, err := sink.Conn.PrepareBatch(ctx, prCommitLinkInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.RepoID, uint32(row.PRNumber), row.CommitHash, row.Confidence,
			row.Provenance, row.Evidence, row.LastSynced, row.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubPullRequestSocialClickHouseEffects) inspectPRCommitLinks(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if sink.provider() != "gitlab" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[prCommitLinkRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	exact, absent := 0, 0
	for _, row := range expected {
		if err := row.validate(claim); err != nil {
			return EffectConflict, err
		}
		inspection, err := inspectPRCommitLink(ctx, sink.Conn, row)
		if err != nil {
			return EffectConflict, err
		}
		switch inspection {
		case EffectExact:
			exact++
		case EffectAbsent:
			absent++
		default:
			return EffectConflict, nil
		}
	}
	switch len(expected) {
	case exact:
		return EffectExact, nil
	case absent:
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

// inspectPRCommitLink reads the link's current version. A version written at
// or after this effect's own instant (a later sync, or the text derivation of
// the same pair) supersedes it, so the effect counts as applied.
func inspectPRCommitLink(ctx context.Context, conn driver.Conn, expected prCommitLinkRow) (EffectInspection, error) {
	rows, err := conn.Query(ctx, `
SELECT last_synced
FROM work_graph_pr_commit FINAL
WHERE org_id = ? AND repo_id = ? AND pr_number = ? AND commit_hash = ?`,
		expected.OrgID, expected.RepoID, uint32(expected.PRNumber), expected.CommitHash,
	)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var lastSynced time.Time
	found := false
	for rows.Next() {
		if found {
			return EffectConflict, nil
		}
		if err := rows.Scan(&lastSynced); err != nil {
			return EffectConflict, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	if !found || lastSynced.UTC().Before(expected.LastSynced.UTC()) {
		return EffectAbsent, nil
	}
	return EffectExact, nil
}
