// Package issuecommitedges is the Go port of
// _build_issue_commit_edges_from_text_parsing (CHAOS-5304, the commit-message
// half of CHAOS-4924 the issue<->PR sub-builders (issueprlinks) and
// PR<->commit sub-builders (prcommit) already carved out): reads git_commits
// and work_items FINAL, regex-extracts jira/github/gitlab issue references
// from each commit message via internal/jobs/workgraph/textrefs
// (ExtractJiraKeys/ExtractGitHubIssueRefs/ExtractGitLabIssueRefs -- already a
// byte-exact, corpus-tested port of
// work_graph/extractors/text_parser.py's extract_jira_keys/
// extract_github_issue_refs/extract_gitlab_issue_refs, per that package's own
// doc comment; not re-verified here), and writes COMMIT->ISSUE edges.
//
// The message-parsing itself is not re-implemented here, same reasoning as
// prcommit's own doc comment: this package is the provider-lookup,
// dedup, and edge-assembly logic Python wraps around the three extractors.
package issuecommitedges

import (
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/textrefs"
)

// CommitRow is one row of the commit-message read (builder.py:1038-1063).
// AuthorWhen IS consumed here (unlike prcommit.CommitRow, where it is
// join-only) -- it becomes each derived edge's EventTs, matching Python's
// `event_ts = author_when` (builder.py:1131).
type CommitRow struct {
	RepoID     uuid.UUID
	Hash       string
	Message    string
	AuthorWhen time.Time
}

// WorkItemRow is one row of the work_items FINAL read (builder.py:1064-1077).
// Only the three fields the provider lookups actually key on are carried --
// project_key/project_id are read by Python but never used by this
// sub-builder (builder.py:1084-1106 never references them).
type WorkItemRow struct {
	RepoID     uuid.UUID
	WorkItemID string
	Provider   string
}

// Inputs is everything Derive reads. A pure-function shape, same reasoning as
// issueprlinks.Inputs and prcommit.Inputs: testable without ClickHouse.
type Inputs struct {
	Commits   []CommitRow
	WorkItems []WorkItemRow
}

// Result is one derivation's output.
type Result struct {
	Edges []edges.Row
	// JiraRefsFound, GitHubRefsFound, GitLabRefsFound are the raw
	// extractor-match counts (before any work-item lookup), for parity with
	// Python's own log line ("Commit message refs: jira=%d, github=%d,
	// gitlab=%d", builder.py:1265-1270) -- a ref can be found and still
	// produce no edge, when it names no work item this org tracks.
	JiraRefsFound   int
	GitHubRefsFound int
	GitLabRefsFound int
}

// providerLookup mirrors builder.py:1084-1106's three dict comprehensions,
// built once per Derive call from Inputs.WorkItems.
type providerLookup struct {
	// jiraKey maps an UPPERCASED "PROJECT-123" key to the work_item_id
	// ("jira:PROJECT-123") that produced it -- builder.py:1088-1090 strips
	// the "jira:" prefix (5 characters) and upper-cases before storing.
	jiraKey map[string]string
	// github/gitlab map (repo_id string, trailing-"#"-split issue number) to
	// the work_item_id that produced it (builder.py:1092-1099). The key is
	// the RAW captured issue number text, never converted to an integer --
	// same reasoning textrefs.ParsedIssueRef's own doc comment gives for
	// ExtractGitHubIssueRefs's IssueKey field.
	github map[[2]string]string
	gitlab map[[2]string]string
}

func buildProviderLookup(workItems []WorkItemRow) providerLookup {
	lookup := providerLookup{
		jiraKey: make(map[string]string),
		github:  make(map[[2]string]string),
		gitlab:  make(map[[2]string]string),
	}
	for _, item := range workItems {
		switch item.Provider {
		case "jira":
			// builder.py:1088-1090: `if str(work_item_id).startswith("jira:")`.
			const prefix = "jira:"
			if len(item.WorkItemID) > len(prefix) && item.WorkItemID[:len(prefix)] == prefix {
				key := item.WorkItemID[len(prefix):]
				lookup.jiraKey[toUpperASCII(key)] = item.WorkItemID
			}
		case "github":
			if number, ok := splitOnLastHash(item.WorkItemID); ok {
				lookup.github[[2]string{item.RepoID.String(), number}] = item.WorkItemID
			}
		case "gitlab":
			if number, ok := splitOnLastHash(item.WorkItemID); ok {
				lookup.gitlab[[2]string{item.RepoID.String(), number}] = item.WorkItemID
			}
		}
	}
	return lookup
}

// splitOnLastHash mirrors `str(work_item_id).split("#")[-1]` (builder.py:1096,
// :1100): the suffix after the LAST "#", or the whole string when there is
// none at all (Python's split on a string with no separator returns the
// original string as the sole element, so [-1] is that string). ok is false
// only when work_item_id is empty, matching Python's own truthiness guard
// (`if provider == "github" and repo_id and work_item_id:`).
func splitOnLastHash(workItemID string) (suffix string, ok bool) {
	if workItemID == "" {
		return "", false
	}
	for i := len(workItemID) - 1; i >= 0; i-- {
		if workItemID[i] == '#' {
			return workItemID[i+1:], true
		}
	}
	return workItemID, true
}

// toUpperASCII upper-cases ASCII letters only, matching Python str.upper()'s
// effect on a Jira project key -- which the live schema and every known
// provider constrain to ASCII letters/digits, so no Unicode case-folding
// divergence is reachable here in practice. Kept as an explicit ASCII-only
// helper rather than strings.ToUpper (which is Unicode-aware) so the
// contract is documented rather than incidental.
func toUpperASCII(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		out[i] = c
	}
	return string(out)
}

// edgeTypeFor mirrors the CLOSES->IMPLEMENTS / else->REFERENCES mapping every
// one of the three provider loops applies identically (builder.py:1143-1147,
// :1183-1187, :1223-1227). Jira refs are never CLOSES (extract_jira_keys
// hard-codes RefType.REFERENCES, text_parser.py:270), so this branch is
// unreachable for the jira loop specifically -- included anyway because
// Derive applies it uniformly across all three, matching Python's structure.
func edgeTypeFor(refType textrefs.RefType) string {
	if refType == textrefs.RefCloses {
		return edges.EdgeTypeImplements
	}
	return edges.EdgeTypeReferences
}

// issueCommitConfidence is the flat confidence every edge this sub-builder
// produces carries (builder.py:1160, :1200, :1240) -- unlike the sibling
// issue<->PR text-parse builder's 0.9, this one is 0.85 for all three
// providers, no per-provider variation.
const issueCommitConfidence = float32(0.85)

// Derive ports _build_issue_commit_edges_from_text_parsing's edge-derivation
// body (builder.py:1114-1272) exactly. buildTime is the single "now" Python
// stamps on every edge's discovered_at/last_synced in one build (`self._now`,
// builder.py:1276-1277) and the fallback for a commit with no author_when --
// threaded through as a parameter (not read from a package-level clock) so
// Derive stays a pure function of its inputs, same reasoning as
// prcommit.BuildFastPathEdges taking buildTime as a parameter.
//
// Dedup: seen_edges (builder.py:1118) is declared ONCE before the commit
// loop, not reset per commit -- reproduced here the same way (a single
// seen map spanning the whole call), though in practice it can only ever
// suppress a duplicate WITHIN one commit's three provider passes: edge_id
// embeds commit_id, so two DIFFERENT commits can never collide, and each of
// the three textrefs extractors already dedupes its own matches by issue
// number/key before Derive ever sees them. Kept for structural fidelity with
// the Python this ports, not because a live corpus is known to need it.
func Derive(inputs Inputs, buildTime time.Time) Result {
	lookup := buildProviderLookup(inputs.WorkItems)

	result := Result{}
	seen := make(map[string]struct{})

	for _, commit := range inputs.Commits {
		// builder.py:1125: `if not message or not commit_hash: continue`.
		if commit.Message == "" || commit.Hash == "" {
			continue
		}
		commitID := edges.GenerateCommitID(commit.RepoID, commit.Hash)

		// builder.py:1132-1138: a string event_ts is parsed, falling back to
		// self._now on a parse failure or an absent value. The ClickHouse
		// driver already hands back a time.Time (never a string) on this
		// read path, so only the "absent" fallback is reachable here -- same
		// reduction prcommit/issueprlinks apply to their own author_when
		// handling. buildTime is threaded through Derive (not read from a
		// package-level clock) so the whole call stays a pure function of
		// its Inputs, same reasoning as BuildFastPathEdges taking buildTime
		// as a parameter rather than reading time.Now() itself.
		eventTs := commit.AuthorWhen
		if eventTs.IsZero() {
			eventTs = buildTime
		}

		jiraRefs := textrefs.ExtractJiraKeys(commit.Message)
		result.JiraRefsFound += len(jiraRefs)
		for _, ref := range jiraRefs {
			workItemID, ok := lookup.jiraKey[toUpperASCII(ref.IssueKey)]
			if !ok {
				continue
			}
			appendEdge(&result, seen, commit.RepoID, commitID, workItemID, "jira", ref.RefType, ref.RawMatch, eventTs, buildTime)
		}

		githubRefs := textrefs.ExtractGitHubIssueRefs(commit.Message)
		result.GitHubRefsFound += len(githubRefs)
		for _, ref := range githubRefs {
			workItemID, ok := lookup.github[[2]string{commit.RepoID.String(), ref.IssueKey}]
			if !ok {
				continue
			}
			appendEdge(&result, seen, commit.RepoID, commitID, workItemID, "github", ref.RefType, ref.RawMatch, eventTs, buildTime)
		}

		gitlabRefs := textrefs.ExtractGitLabIssueRefs(commit.Message)
		result.GitLabRefsFound += len(gitlabRefs)
		for _, ref := range gitlabRefs {
			workItemID, ok := lookup.gitlab[[2]string{commit.RepoID.String(), ref.IssueKey}]
			if !ok {
				continue
			}
			appendEdge(&result, seen, commit.RepoID, commitID, workItemID, "gitlab", ref.RefType, ref.RawMatch, eventTs, buildTime)
		}
	}
	return result
}

func appendEdge(
	result *Result, seen map[string]struct{},
	repoID uuid.UUID, commitID, workItemID, provider string,
	refType textrefs.RefType, evidence string, eventTs, buildTime time.Time,
) {
	edgeType := edgeTypeFor(refType)
	edgeID := edges.EdgeID(edges.NodeTypeCommit, commitID, edgeType, edges.NodeTypeIssue, workItemID)
	if _, already := seen[edgeID]; already {
		return
	}
	seen[edgeID] = struct{}{}

	providerValue := provider
	result.Edges = append(result.Edges, edges.Row{
		EdgeID:       edgeID,
		SourceType:   edges.NodeTypeCommit,
		SourceID:     commitID,
		TargetType:   edges.NodeTypeIssue,
		TargetID:     workItemID,
		EdgeType:     edgeType,
		Provenance:   edges.ProvenanceExplicitText,
		Confidence:   issueCommitConfidence,
		Evidence:     evidence,
		DiscoveredAt: buildTime,
		LastSynced:   buildTime,
		EventTs:      eventTs,
		Day:          edges.DayFor(eventTs),
		RepoID:       &repoID,
		Provider:     &providerValue,
	})
}
