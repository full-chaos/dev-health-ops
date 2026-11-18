package teamattribution

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const linkedPRRepoChunk = 5000

// AttachLinkedPRRepos fills LinkedPRRepos of every issue subject (not a pull / merge request) from
// work_graph_issue_pr, the link of record: the repositories of the pull requests the issue is linked
// to, with each repository's provider from repos. An issue with no link keeps none.
func AttachLinkedPRRepos(
	ctx context.Context, conn driver.Conn, subjects map[string]GithubWorkItemDerivationSubject,
) error {
	idsByOrg := map[string][]string{}
	for id, subject := range subjects {
		if subject.OrgID == "" || GithubWorkItemDerivationIsPullOrMergeRequestType(subject.Provider, subject.Type) {
			continue
		}
		idsByOrg[subject.OrgID] = append(idsByOrg[subject.OrgID], id)
	}
	for orgID, ids := range idsByOrg {
		linked := map[string][]LinkedPRRepo{}
		for start := 0; start < len(ids); start += linkedPRRepoChunk {
			end := min(start+linkedPRRepoChunk, len(ids))
			rows, err := conn.Query(ctx, `
SELECT DISTINCT l.work_item_id, toString(l.repo_id), r.provider
FROM work_graph_issue_pr AS l FINAL
INNER JOIN (SELECT id, provider FROM repos FINAL WHERE org_id = ?) AS r ON r.id = l.repo_id
WHERE l.org_id = ? AND l.work_item_id IN (?)
ORDER BY l.work_item_id, r.provider, toString(l.repo_id)`,
				orgID, orgID, ids[start:end])
			if err != nil {
				return err
			}
			for rows.Next() {
				var workItemID, repoID, provider string
				if err := rows.Scan(&workItemID, &repoID, &provider); err != nil {
					_ = rows.Close()
					return err
				}
				linked[workItemID] = append(linked[workItemID], LinkedPRRepo{Provider: provider, RepoID: repoID})
			}
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				return err
			}
			_ = rows.Close()
		}
		for workItemID, repos := range linked {
			subject := subjects[workItemID]
			subject.LinkedPRRepos = repos
			subjects[workItemID] = subject
		}
	}
	return nil
}
