package prcommit

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

// BuildFastPathEdges ports _build_pr_commit_edges_from_fast_path's row-to-edge
// step (builder.py:1944-1990) exactly. buildTime is the single "now" Python
// stamps on every edge's discovered_at/last_synced in one build
// (`self._now`, builder.py:1986-1987); event_ts is per-row from the join and
// falls back to buildTime only when the row carries none.
//
// # The confidence-zero quirk (builder.py:1949, preserved deliberately)
//
// Python reads `confidence = float(row.get("confidence") or 1.0)` -- `or`
// treats a stored 0.0 as FALSY, so a genuinely zero-confidence row is silently
// promoted to 1.0 here, at read time, not written time. Nothing in this
// derivation's own writer (Write, above) ever persists 0.0 -- both link tiers
// use 0.9/0.6 -- but a fixture or a future writer could, and until this whole
// function is retired, replicating the exact Python read-time behaviour is the
// contract, not improving on it silently.
func BuildFastPathEdges(orgID string, rows []FastPathRow, buildTime time.Time) []edges.Row {
	out := make([]edges.Row, 0, len(rows))
	for _, row := range rows {
		repoID := row.RepoID
		prID := edges.GeneratePRID(repoID, row.PRNumber)
		commitID := edges.GenerateCommitID(repoID, row.CommitHash)
		edgeID := edges.EdgeID(edges.NodeTypePR, prID, edges.EdgeTypeContains, edges.NodeTypeCommit, commitID)

		confidence := row.Confidence
		if confidence == 0 {
			confidence = 1.0
		}

		evidence := row.Evidence
		if evidence == "" {
			evidence = "pr_commit_fast_path"
		}

		eventTs := row.AuthorWhen
		if eventTs.IsZero() {
			eventTs = buildTime
		}

		out = append(out, edges.Row{
			EdgeID:       edgeID,
			SourceType:   edges.NodeTypePR,
			SourceID:     prID,
			TargetType:   edges.NodeTypeCommit,
			TargetID:     commitID,
			EdgeType:     edges.EdgeTypeContains,
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
