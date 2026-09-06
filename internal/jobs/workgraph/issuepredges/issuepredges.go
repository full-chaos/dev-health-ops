// Package issuepredges is the Go port of the two issue<->PR EDGE sub-builders
// in work_graph/builder.py (CHAOS-4924, the next slice after CHAOS-5264's
// PR<->commit port):
//
//   - _build_issue_pr_edges_from_fast_path: reads work_graph_issue_pr (the
//     mapping table issueprlinks/CHAOS-5249 and this package's own
//     ProduceTextParseEdges both populate) joined against git_pull_requests,
//     and writes work_graph_edges IMPLEMENTS rows.
//   - _build_issue_pr_edges: parses PR title/body/head-branch text for
//     Jira/GitHub/GitLab issue references, and writes BOTH work_graph_edges
//     rows AND new work_graph_issue_pr rows (provenance=explicit_text) --
//     the "fills fast path" half of Python's own comment at builder.py:434.
//
// The text extraction itself is NOT re-implemented here: textrefs.ExtractJiraKeys,
// ExtractGitHubIssueRefs and ExtractGitLabIssueRefs are already a byte-exact,
// corpus-tested port of extractors/text_parser.py. This package is the
// provider-lookup, edge/link construction and write logic Python wraps around
// those three functions, plus the fast-path join -- the same split prcommit
// uses for textrefs.ExtractPRRefs/ExtractSquashPRRefs.
//
// # Ordering
//
// Python calls the fast-path builder BEFORE the text-parse builder
// (builder.py:434-443) and unions their (work_item_id, pr_number) result sets
// into the input _build_heuristic_issue_pr_edges receives. This port does not
// carry that set forward in memory: the eventual heuristic pre-step reads
// work_graph_issue_pr FRESH from ClickHouse instead, which by the time it runs
// already contains BOTH provenances (issueprlinks' native rows plus this
// package's own explicit_text rows, committed by the two ProduceX pre-steps
// that ran earlier in the same build) -- a fresh read is equivalent to the
// union Python holds in memory, and decouples the heuristic port from this
// package's internal result shape. ProduceFastPathEdges must still register
// BEFORE ProduceTextParseEdges in buildPreStepOrder to match Python's edge
// creation order for a PR that satisfies both derivations in one build.
package issuepredges

import (
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/textrefs"
)

// FastPathRow is one row of the work_graph_issue_pr x git_pull_requests join
// (builder.py:1488-1499).
type FastPathRow struct {
	RepoID      uuid.UUID
	WorkItemID  string
	PRNumber    int
	Confidence  float32
	Provenance  string
	Evidence    string
	PRCreatedAt time.Time
}

// PullRequestRow is one row of the PR text-parsing read (builder.py:730-738).
type PullRequestRow struct {
	RepoID     uuid.UUID
	Number     int
	Title      string
	Body       string
	HeadBranch string
	CreatedAt  time.Time
}

// WorkItemRow is one row of the work-item lookup read (builder.py:764-772).
// project_key/project_id are in Python's own SELECT but never read in the
// loop body that follows (builder.py:788-812) -- omitted here for the same
// reason operationaledges omits unused SELECT columns from its row structs.
type WorkItemRow struct {
	RepoID     uuid.UUID
	WorkItemID string
	Provider   string
}

// parseProvenance mirrors WorkGraphBuilder._parse_provenance (builder.py:242-253)
// exactly, including its unconditional NATIVE fallback -- same helper as
// prcommit.parseProvenance, duplicated rather than exported, since neither
// package needs the other's public surface for this.
func parseProvenance(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case edges.ProvenanceNative:
		return edges.ProvenanceNative
	case edges.ProvenanceExplicitText:
		return edges.ProvenanceExplicitText
	case edges.ProvenanceHeuristic:
		return edges.ProvenanceHeuristic
	default:
		return edges.ProvenanceNative
	}
}

// DeriveFastPathEdges ports _build_issue_pr_edges_from_fast_path's row-to-edge
// step (builder.py:1512-1553) exactly.
//
// # The confidence-zero and empty-evidence quirks (builder.py:1518, :1523;
// preserved deliberately, same shape as prcommit.BuildFastPathEdges)
//
// Python reads `confidence = float(row.get("confidence") or 1.0)` and
// `evidence = str(evidence or "issue_pr_fast_path")` -- `or` treats a stored
// 0.0 / "" as FALSY, silently promoting them to the fallback at READ time.
// Replicated here rather than "fixed", since the port's contract is row-for-row
// parity with the producer it replaces, not an improvement on it.
func DeriveFastPathEdges(rows []FastPathRow, buildTime time.Time) []edges.Row {
	out := make([]edges.Row, 0, len(rows))
	for _, row := range rows {
		repoID := row.RepoID
		prID := edges.GeneratePRID(repoID, row.PRNumber)
		edgeID := edges.EdgeID(edges.NodeTypePR, prID, edges.EdgeTypeImplements, edges.NodeTypeIssue, row.WorkItemID)

		confidence := row.Confidence
		if confidence == 0 {
			confidence = 1.0
		}
		evidence := row.Evidence
		if evidence == "" {
			evidence = "issue_pr_fast_path"
		}
		eventTs := row.PRCreatedAt
		if eventTs.IsZero() {
			eventTs = buildTime
		}

		out = append(out, edges.Row{
			EdgeID:       edgeID,
			SourceType:   edges.NodeTypePR,
			SourceID:     prID,
			TargetType:   edges.NodeTypeIssue,
			TargetID:     row.WorkItemID,
			EdgeType:     edges.EdgeTypeImplements,
			Provenance:   parseProvenance(row.Provenance),
			Confidence:   confidence,
			Evidence:     evidence,
			DiscoveredAt: buildTime,
			LastSynced:   buildTime,
			EventTs:      eventTs,
			Day:          edges.DayFor(eventTs),
			RepoID:       &repoID,
		})
	}
	return out
}

// TextParseResult is DeriveTextParseEdges' output: the two writes Python
// performs (builder.py:1024-1025) plus the ref-count logging fields
// (builder.py:1027-1032), kept for parity with Python's own log line.
type TextParseResult struct {
	Edges           []edges.Row
	Links           []issueprlinks.Link
	JiraRefsFound   int
	GitHubRefsFound int
	GitLabRefsFound int
}

// providerLookup maps a provider's own issue-identifier shape to a
// work_item_id, mirroring the three dicts builder.py:783-812 builds from
// work_items rows before scanning any PR text.
type providerLookup struct {
	jiraKey map[string]string    // uppercased Jira key -> work_item_id
	github  map[[2]string]string // (repo_id string, issue number string) -> work_item_id
	gitlab  map[[2]string]string // (repo_id string, issue number string) -> work_item_id
}

// buildProviderLookup ports builder.py:783-812. Providers outside the three
// text-parse-covered ones (notably Linear) are not tracked here at all --
// their issue<->PR links arrive as native attachments and become edges via
// the work_item_dependencies pass, not this one, matching Python's own
// comment at builder.py:786-789.
func buildProviderLookup(workItems []WorkItemRow) providerLookup {
	lookup := providerLookup{
		jiraKey: make(map[string]string),
		github:  make(map[[2]string]string),
		gitlab:  make(map[[2]string]string),
	}
	for _, wi := range workItems {
		switch wi.Provider {
		case "jira":
			if strings.HasPrefix(wi.WorkItemID, "jira:") {
				key := strings.ToUpper(strings.TrimPrefix(wi.WorkItemID, "jira:"))
				lookup.jiraKey[key] = wi.WorkItemID
			}
		case "github":
			if idx := strings.LastIndex(wi.WorkItemID, "#"); idx >= 0 {
				num := wi.WorkItemID[idx+1:]
				lookup.github[[2]string{wi.RepoID.String(), num}] = wi.WorkItemID
			}
		case "gitlab":
			if idx := strings.LastIndex(wi.WorkItemID, "#"); idx >= 0 {
				num := wi.WorkItemID[idx+1:]
				lookup.gitlab[[2]string{wi.RepoID.String(), num}] = wi.WorkItemID
			}
		}
	}
	return lookup
}

// edgeTypeFor picks IMPLEMENTS for a closing reference, REFERENCES otherwise
// -- builder.py's repeated `EdgeType.IMPLEMENTS if ref.ref_type ==
// RefType.CLOSES else EdgeType.REFERENCES` (e.g. builder.py:864-867), one
// helper shared by all three provider branches below since the three copies
// in Python are textually identical.
func edgeTypeFor(refType textrefs.RefType) string {
	if refType == textrefs.RefCloses {
		return edges.EdgeTypeImplements
	}
	return edges.EdgeTypeReferences
}

// DeriveTextParseEdges ports _build_issue_pr_edges (builder.py:721-1024)
// exactly: build provider lookups, scan every PR's title+body+head_branch
// text for Jira/GitHub/GitLab references, and for every ref that resolves
// against a known work item, emit one edge AND one fast-path link.
//
// A PR with no title, body or head_branch is skipped entirely
// (builder.py:846-847); a PR with no number is skipped too (builder.py:848-849).
func DeriveTextParseEdges(prs []PullRequestRow, workItems []WorkItemRow, buildTime time.Time) TextParseResult {
	lookup := buildProviderLookup(workItems)
	var result TextParseResult

	for _, pr := range prs {
		if pr.Title == "" && pr.Body == "" && pr.HeadBranch == "" {
			continue
		}

		repoID := pr.RepoID
		repoIDStr := repoID.String()
		text := pr.Title + "\n" + pr.Body + "\n" + pr.HeadBranch

		eventTs := pr.CreatedAt
		if eventTs.IsZero() {
			eventTs = buildTime
		}

		emit := func(workItemID, provider, rawMatch string, refType textrefs.RefType) {
			edgeType := edgeTypeFor(refType)
			prID := edges.GeneratePRID(repoID, pr.Number)
			edgeID := edges.EdgeID(edges.NodeTypePR, prID, edgeType, edges.NodeTypeIssue, workItemID)
			providerCopy := provider

			result.Edges = append(result.Edges, edges.Row{
				EdgeID:       edgeID,
				SourceType:   edges.NodeTypePR,
				SourceID:     prID,
				TargetType:   edges.NodeTypeIssue,
				TargetID:     workItemID,
				EdgeType:     edgeType,
				Provenance:   edges.ProvenanceExplicitText,
				Confidence:   0.9,
				Evidence:     rawMatch,
				RepoID:       &repoID,
				Provider:     &providerCopy,
				DiscoveredAt: buildTime,
				LastSynced:   buildTime,
				EventTs:      eventTs,
				Day:          edges.DayFor(eventTs),
			})
			result.Links = append(result.Links, issueprlinks.Link{
				RepoID:     repoID,
				WorkItemID: workItemID,
				PRNumber:   uint32(pr.Number),
				Confidence: 0.9,
				Provenance: edges.ProvenanceExplicitText,
				Evidence:   rawMatch,
			})
		}

		jiraRefs := textrefs.ExtractJiraKeys(text)
		result.JiraRefsFound += len(jiraRefs)
		for _, ref := range jiraRefs {
			workItemID, ok := lookup.jiraKey[strings.ToUpper(ref.IssueKey)]
			if !ok {
				continue
			}
			emit(workItemID, "jira", ref.RawMatch, ref.RefType)
		}

		ghRefs := textrefs.ExtractGitHubIssueRefs(text)
		result.GitHubRefsFound += len(ghRefs)
		for _, ref := range ghRefs {
			workItemID, ok := lookup.github[[2]string{repoIDStr, ref.IssueKey}]
			if !ok {
				continue
			}
			emit(workItemID, "github", ref.RawMatch, ref.RefType)
		}

		glRefs := textrefs.ExtractGitLabIssueRefs(text)
		result.GitLabRefsFound += len(glRefs)
		for _, ref := range glRefs {
			workItemID, ok := lookup.gitlab[[2]string{repoIDStr, ref.IssueKey}]
			if !ok {
				continue
			}
			emit(workItemID, "gitlab", ref.RawMatch, ref.RefType)
		}
	}

	return result
}
