package issueprlinks

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const storedLinksQuery = `
SELECT repo_id, work_item_id, pr_number, provenance
FROM work_graph_issue_pr FINAL
WHERE org_id = {org_id:String}`

// Querier is the one capability ReadStoredLinks needs.
type Querier interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// ReadStoredLinks reads every stored link of one organization, every tier,
// after the table's own deduplication (FINAL ranks native above the fallback
// tiers on a shared key, migration 084). It is org-wide on purpose: a native
// link outranks a fallback whatever window the build covers.
func ReadStoredLinks(ctx context.Context, querier Querier, orgID string) ([]Link, error) {
	if querier == nil {
		return nil, ErrUnavailable
	}
	if orgID == "" {
		return nil, fmt.Errorf("issueprlinks: org id is required")
	}
	rows, err := querier.Query(ctx, storedLinksQuery, clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, fmt.Errorf("load work_graph_issue_pr: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Link
	for rows.Next() {
		link := Link{OrgID: orgID}
		if err := rows.Scan(&link.RepoID, &link.WorkItemID, &link.PRNumber, &link.Provenance); err != nil {
			return nil, err
		}
		out = append(out, link)
	}
	return out, rows.Err()
}

// ExecConn is the one capability a retraction needs.
type ExecConn interface {
	Exec(ctx context.Context, query string, args ...any) error
}

// retractStatement removes exactly the planned rows. The table has no
// tombstone column, so a mutation is its only retraction; mutations_sync=2
// makes the next build step read the table without them. The provenance
// guard keeps a native row on the same key out of reach.
const retractStatement = `ALTER TABLE work_graph_issue_pr DELETE
WHERE org_id = {org_id:String}
  AND provenance IN ('explicit_text', 'heuristic')
  AND concat(toString(repo_id), '|', work_item_id, '|', toString(pr_number), '|', provenance) IN {keys:Array(String)}
SETTINGS mutations_sync = 2`

// RetractOutranked removes the planned lower-tier rows of one organization.
// No rows means no mutation.
func RetractOutranked(ctx context.Context, connection ExecConn, orgID string, outranked []Link) error {
	if connection == nil {
		return ErrUnavailable
	}
	if orgID == "" {
		return fmt.Errorf("issueprlinks: org id is required")
	}
	if len(outranked) == 0 {
		return nil
	}
	keys := make([]string, 0, len(outranked))
	for _, link := range outranked {
		if link.Provenance != ProvenanceExplicitText && link.Provenance != ProvenanceHeuristic {
			return fmt.Errorf("issueprlinks: refusing to retract a %q row", link.Provenance)
		}
		keys = append(keys, retractionKey(link))
	}
	if err := connection.Exec(ctx, retractStatement,
		clickhouse.Named("org_id", orgID), clickhouse.Named("keys", keys)); err != nil {
		return fmt.Errorf("retract outranked work_graph_issue_pr rows: %w", err)
	}
	return nil
}
