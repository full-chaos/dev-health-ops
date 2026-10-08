package investmentexplain

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/scopelabel"
)

// quoteSource identifies the source an evidence quote came from.
type quoteSource struct {
	SourceType string
	SourceID   string
}

// quoteSourceTitles resolves the stored title of each quote's source, inside
// the caller's organisation. Issue sources read work_items; pull request
// sources ("<repo uuid>#pr<number>") read git_pull_requests. A commit source
// has no title read and stays absent. Best effort: a failed lookup is logged
// and the quotes keep their rows without a title. A source with no
// displayable title is absent from the result.
func (reader *Reader) quoteSourceTitles(ctx context.Context, orgID string, quotes []WorkUnitInvestmentQuoteRow) map[quoteSource]string {
	titles := map[quoteSource]string{}
	var issueIDs []string
	var prRepoIDs, prNumbers []string
	prKeys := map[string][]quoteSource{}
	for _, quote := range quotes {
		switch quote.SourceType {
		case "issue":
			issueIDs = append(issueIDs, quote.SourceID)
		case "pr":
			match := prEvidenceRefRE.FindStringSubmatch(quote.SourceID)
			if match == nil {
				continue
			}
			repoID := strings.ToLower(match[1])
			prRepoIDs = append(prRepoIDs, repoID)
			prNumbers = append(prNumbers, match[2])
			key := repoID + "#" + match[2]
			prKeys[key] = append(prKeys[key], quoteSource{SourceType: "pr", SourceID: quote.SourceID})
		}
	}

	if reader == nil || reader.client == nil {
		return titles
	}
	for id, title := range scopelabel.ResolveWorkItemTitles(ctx, reader.client, orgID, issueIDs, scopelabel.Options{
		Suffix: settingsMaxExecutionTime(),
		Log:    "query-api: work units quote titles",
	}) {
		titles[quoteSource{SourceType: "issue", SourceID: id}] = title
	}

	prTitles, err := reader.fetchPullRequestTitles(ctx, orgID, prRepoIDs, prNumbers)
	if err != nil {
		log.Printf("query-api: work units quote titles: could not resolve pull request titles for sources=%d: %v", len(prKeys), err)
		return titles
	}
	for key, title := range prTitles {
		for _, source := range prKeys[key] {
			if clean, ok := scopelabel.CleanNameFor(title, source.SourceID); ok {
				titles[source] = clean
			}
		}
	}
	return titles
}

// fetchPullRequestTitles returns {"<repo uuid>#<number>": title}. Both the
// pull request row and its repository sit inside the caller's organisation:
// two organisations that sync one repository share its repo id, so the repo
// filter alone would admit another organisation's title.
func (reader *Reader) fetchPullRequestTitles(ctx context.Context, orgID string, repoIDs, numbers []string) (map[string]string, error) {
	repoIDs = uniqueNonEmpty(repoIDs)
	numbers = uniqueNonEmpty(numbers)
	if len(repoIDs) == 0 || len(numbers) == 0 {
		return map[string]string{}, nil
	}
	query := fmt.Sprintf(`
SELECT toString(pr.repo_id) AS repo_id, toString(pr.number) AS number, pr.title AS title
FROM git_pull_requests AS pr FINAL
WHERE pr.org_id = {org_id:String}
  AND pr.repo_id IN (SELECT id FROM repos FINAL WHERE org_id = {org_id:String})
  AND toString(pr.repo_id) IN {repo_ids:Array(String)}
  AND toString(pr.number) IN {numbers:Array(String)}
%s
`, settingsMaxExecutionTime())
	titles := map[string]string{}
	for _, chunk := range chunkStrings(repoIDs, lookupChunkSize) {
		rows, err := reader.client.Query(ctx, query, []dhclickhouse.Binding{
			{Name: "org_id", Value: orgID},
			{Name: "repo_ids", Value: chunk},
			{Name: "numbers", Value: numbers},
		})
		if err != nil {
			return nil, fmt.Errorf("query pull request titles: %w", err)
		}
		for rows.Next() {
			var repoID, number string
			var title *string
			if err := rows.Scan(&repoID, &number, &title); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan pull request title row: %w", err)
			}
			if title == nil {
				continue
			}
			if clean, ok := scopelabel.CleanName(*title); ok {
				if _, err := strconv.ParseUint(number, 10, 64); err == nil {
					titles[repoID+"#"+number] = clean
				}
			}
		}
		rowsErr := rows.Err()
		rows.Close()
		if rowsErr != nil {
			return nil, fmt.Errorf("iterate pull request title rows: %w", rowsErr)
		}
	}
	return titles, nil
}
