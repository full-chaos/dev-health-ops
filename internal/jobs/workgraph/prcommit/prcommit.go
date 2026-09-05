// Package prcommit is the Go port of the two PR<->commit sub-builders in
// work_graph/builder.py (CHAOS-5259, the remaining slice of CHAOS-4924 after
// issue_pr_links (CHAOS-4757) was carved out):
//
//   - _derive_pr_commit_links: parses commit messages for PR/MR references and
//     writes the fast-path table work_graph_pr_commit.
//   - _build_pr_commit_edges_from_fast_path: reads that table joined against
//     git_commits and writes work_graph_edges CONTAINS rows.
//
// The message-parsing itself is NOT re-implemented here: textrefs.ExtractPRRefs
// and textrefs.ExtractSquashPRRefs (CHAOS-4441) are already a byte-exact,
// corpus-tested port of extract_pr_refs/extract_squash_pr_refs, including their
// Unicode-digit and revert-message edge cases. This package is the corroboration
// and write logic Python wraps around those two functions, plus the fast-path
// edge join.
package prcommit

import (
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/textrefs"
)

// PullRequestRow is one row of the "known PR numbers" read
// (builder.py:1772-1799): org_id, repo_id, number only. The derivation uses the
// row's EXISTENCE, never any other column.
type PullRequestRow struct {
	OrgID  string
	RepoID uuid.UUID
	Number int
}

// CommitRow is one row of the commit-message read (builder.py:1801-1827).
//
// author_when is part of that query's SELECT (it bounds the WHERE clause) but,
// like Python's own loop body, is never read once the rows arrive -- so it has
// no field here. It reappears in FastPathCommitRow below, where the fast-path
// edge builder DOES consume it (as the edge's event_ts).
type CommitRow struct {
	OrgID   string
	RepoID  uuid.UUID
	Hash    string
	Message string
}

// Link is one derived PR<->commit fast-path row.
type Link struct {
	OrgID      string
	RepoID     uuid.UUID
	PRNumber   int
	CommitHash string
	// Confidence is float32 to match the live Float32 column, mirroring
	// edges.Row's own choice -- see edges.Row's doc comment on why this
	// quantisation is a field type decision, not a call-site one.
	Confidence float32
	Provenance string
	Evidence   string
	LastSynced time.Time
}

// Inputs is everything Derive reads. A pure-function shape, same reasoning as
// issueprlinks.Inputs: testable without ClickHouse.
type Inputs struct {
	OrgID        string
	PullRequests []PullRequestRow
	Commits      []CommitRow
}

// Result is one derivation's output.
type Result struct {
	Links []Link
	// CommitsScanned is the read count, for parity with Python's own log line
	// ("Found %d commits to scan for PR refs", builder.py:1828).
	CommitsScanned int
}

// prKey identifies a PR by its tenant and repo, mirroring Python's
// (org_key, repo_id_str) dict key (builder.py:1798-1799).
type prKey struct {
	orgID  string
	repoID string
}

// linkTier is one of the two extraction passes, in Python's fixed order
// (builder.py:1838-1848): explicit merge-keyword evidence first, then the
// ambiguous squash-subject form. Order is load-bearing -- for a commit whose
// message satisfies BOTH shapes (impossible today given the patterns, but not
// structurally excluded), the explicit tier's higher-confidence link would win
// the dedup below only because it runs first.
type linkTier struct {
	extract    func(string) []int
	confidence float32
	provenance string
	evidence   string
}

var linkTiers = []linkTier{
	{textrefs.ExtractPRRefs, 0.9, edges.ProvenanceExplicitText, "commit_message_pr_ref"},
	{textrefs.ExtractSquashPRRefs, 0.6, edges.ProvenanceHeuristic, "commit_message_squash_pr_ref"},
}

// Derive ports _derive_pr_commit_links (builder.py:1731-1891) exactly.
//
// Tenant isolation: a commit is only ever matched against PR numbers known in
// its OWN (org_id, repo_id) -- see prKey. A squash "(#N)" in org A can never
// link to org B's PR #N even when repo_id collides across tenants, the same
// property CHAOS-2189 established for the issue-PR mapping.
//
// Dedup: the FIRST tier to admit a given (org, repo, pr_number, commit_hash)
// wins. Re-deriving is otherwise idempotent regardless of call order, since the
// underlying table is a ReplacingMergeTree keyed on the same tuple
// (plus org_id).
func Derive(inputs Inputs) Result {
	knownPRs := make(map[prKey]map[int]struct{}, len(inputs.PullRequests))
	for _, pr := range inputs.PullRequests {
		key := prKey{orgID: pr.OrgID, repoID: pr.RepoID.String()}
		set, ok := knownPRs[key]
		if !ok {
			set = make(map[int]struct{})
			knownPRs[key] = set
		}
		set[pr.Number] = struct{}{}
	}

	result := Result{CommitsScanned: len(inputs.Commits)}
	seen := make(map[[4]string]struct{})

	for _, commit := range inputs.Commits {
		if commit.Hash == "" {
			continue
		}
		key := prKey{orgID: commit.OrgID, repoID: commit.RepoID.String()}
		repoPRs, ok := knownPRs[key]
		if !ok || len(repoPRs) == 0 {
			continue
		}

		for _, tier := range linkTiers {
			for _, prNumber := range tier.extract(commit.Message) {
				if _, known := repoPRs[prNumber]; !known {
					continue
				}
				dedupKey := [4]string{commit.OrgID, commit.RepoID.String(), strconv.Itoa(prNumber), commit.Hash}
				if _, already := seen[dedupKey]; already {
					continue
				}
				seen[dedupKey] = struct{}{}
				// LastSynced is left zero: Derive is a pure function of Inputs
				// with no clock of its own, same reasoning as
				// issueprlinks.Derive. The caller (Service.Produce) stamps the
				// real timestamp on every link afterward.
				result.Links = append(result.Links, Link{
					OrgID:      commit.OrgID,
					RepoID:     commit.RepoID,
					PRNumber:   prNumber,
					CommitHash: commit.Hash,
					Confidence: tier.confidence,
					Provenance: tier.provenance,
					Evidence:   tier.evidence,
				})
			}
		}
	}
	return result
}
