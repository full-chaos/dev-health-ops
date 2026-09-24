package aianalytics

import (
	"context"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// overlapRow is one per-bucket overlap aggregate of AI-attributed pull
// requests with a set of risky files.
type overlapRow struct {
	Bucket      string
	PrsTotal    int64
	PrsTouching int64
	AvgRisk     *float64
}

// aiPRFiles maps AI-attributed pull requests (event-time window, both
// attribution linkage paths, commits linked through the work graph) to their
// changed file paths. Every table with an org column carries the org filter.
func aiPRFiles(bindings *[]clickhouse.Binding, repoID string, repoIDs []string, useIDs bool) string {
	return `SELECT DISTINCT
        ai.repo_id AS repo_id,
        ai.number AS number,
        ai.kind AS bucket,
        cs.file_path AS file_path
    FROM (
        SELECT pr.repo_id AS repo_id, pr.number AS number, attr_map.kind AS kind
        FROM (
            ` + dedupedPRs(bindings, repoID, repoIDs, useIDs) + `
        ) AS pr
        INNER JOIN (
            ` + prAttributionSubquery + `
        ) AS attr_map
            ON attr_map.repo_id = pr.repo_id AND attr_map.number = pr.number
        WHERE attr_map.kind IN ('ai_assisted', 'agent_created', 'ai_review')
          AND ` + eventWindowFilter() + `
    ) AS ai
    INNER JOIN work_graph_pr_commit AS pc
        ON pc.repo_id = ai.repo_id AND pc.pr_number = ai.number
        AND pc.org_id = {org_id:String}
    INNER JOIN git_commit_stats AS cs
        ON cs.repo_id = pc.repo_id AND cs.commit_hash = pc.commit_hash
        AND cs.org_id = {org_id:String}`
}

const hotspotOverlapTail = `hotspots AS (
    SELECT repo_id, file_path, risk_score
    FROM (
        SELECT
            repo_id,
            file_path,
            risk_score,
            row_number() OVER (
                PARTITION BY repo_id
                ORDER BY risk_score DESC, file_path
            ) AS risk_rank,
            count() OVER (PARTITION BY repo_id) AS repo_file_count
        FROM (
            SELECT
                hs.repo_id AS repo_id,
                hs.file_path AS file_path,
                argMax(hs.risk_score, hs.computed_at) AS risk_score
            FROM file_hotspot_daily AS hs
            WHERE hs.day >= {start_day:Date}
              AND hs.day <= {end_day:Date}
              AND hs.org_id = {org_id:String}
            GROUP BY hs.repo_id, hs.file_path
            HAVING risk_score > 0
        )
    )
    WHERE risk_rank <= greatest(1, toUInt64(ceil(repo_file_count * 0.1)))
)
SELECT
    bucket,
    uniqExact((pf.repo_id, pf.number)) AS prs_total,
    uniqExactIf(
        (pf.repo_id, pf.number), h.file_path != ''
    ) AS prs_touching_hotspots,
    if(
        isNaN(avgIf(h.risk_score, h.file_path != '')),
        NULL,
        avgIf(h.risk_score, h.file_path != '')
    ) AS avg_hotspot_risk_score
FROM pr_files AS pf
LEFT JOIN hotspots AS h
    ON h.repo_id = pf.repo_id AND h.file_path = pf.file_path
GROUP BY bucket
ORDER BY bucket`

const complexityOverlapTail = `complex_files AS (
    SELECT
        fc.repo_id AS repo_id,
        fc.file_path AS file_path
    FROM file_complexity_snapshots AS fc
    WHERE fc.as_of_day <= {end_day:Date}
      AND fc.org_id = {org_id:String}
    GROUP BY fc.repo_id, fc.file_path
    HAVING argMax(
        fc.high_complexity_functions + fc.very_high_complexity_functions,
        fc.computed_at
    ) > 0
)
SELECT
    bucket,
    uniqExact((pf.repo_id, pf.number)) AS prs_total,
    uniqExactIf(
        (pf.repo_id, pf.number), cf.file_path != ''
    ) AS prs_touching_high_complexity
FROM pr_files AS pf
LEFT JOIN complex_files AS cf
    ON cf.repo_id = pf.repo_id AND cf.file_path = pf.file_path
GROUP BY bucket
ORDER BY bucket`

func loadOverlap(ctx context.Context, client QueryClient, orgID string, startDay, endDay time.Time, repoID string, repoIDs []string, useIDs, hotspot bool) ([]overlapRow, error) {
	start, end := dayBounds(startDay, endDay)
	bindings := []clickhouse.Binding{
		{Name: "start", Value: start},
		{Name: "end", Value: end},
		{Name: "start_day", Value: dateValue(startDay)},
		{Name: "end_day", Value: dateValue(endDay)},
		{Name: "org_id", Value: orgID},
	}
	files := aiPRFiles(&bindings, repoID, repoIDs, useIDs)
	tail := complexityOverlapTail
	if hotspot {
		tail = hotspotOverlapTail
	}
	statement := "SELECT * FROM (\nWITH pr_files AS (\n" + files + "\n),\n" + tail + "\n)"
	rs, err := client.Query(ctx, statement, bindings)
	if err != nil {
		return nil, fmt.Errorf("aianalytics: overlap query: %w", err)
	}
	defer rs.Close()
	var out []overlapRow
	for rs.Next() {
		var (
			r              overlapRow
			total, touched uint64
			risk           *float64
		)
		if hotspot {
			if err := rs.Scan(&r.Bucket, &total, &touched, &risk); err != nil {
				return nil, fmt.Errorf("aianalytics: overlap scan: %w", err)
			}
		} else if err := rs.Scan(&r.Bucket, &total, &touched); err != nil {
			return nil, fmt.Errorf("aianalytics: overlap scan: %w", err)
		}
		r.PrsTotal, r.PrsTouching, r.AvgRisk = int64(total), int64(touched), risk
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: overlap rows: %w", err)
	}
	return out, nil
}

// attributedPR is one AI-attributed pull request row.
type attributedPR struct {
	RepoID   string
	Number   int64
	Kind     *string
	WorkType *string
	TeamID   *string
	Title    *string
	MergedAt *time.Time
}

// The linked-issue path joins attribution to a pull request through the work
// item id alone, exactly as the reference does: an attribution record pinned
// to one repository also attributes the pull requests of another repository
// that the same work item links to.
const attributedPRsStatement = `SELECT
    repo_id_str, number, kind, work_type, title, merged_at
FROM (
    SELECT
        toString(pr.repo_id) AS repo_id_str,
        pr.repo_id AS repo_id,
        pr.number AS number,
        attr.kind AS kind,
        coalesce(nullIf(wi.type, ''), 'pull_request') AS work_type,
        pr.title AS title,
        pr.merged_at AS merged_at
    FROM git_pull_requests AS pr
    INNER JOIN work_graph_issue_pr AS link
        ON link.repo_id = pr.repo_id AND link.pr_number = pr.number
        AND link.org_id = {org_id:String}
    INNER JOIN ai_attribution_resolved AS attr
        ON attr.subject_type = 'pull_request'
        AND attr.subject_id = link.work_item_id
        AND attr.kind IN ('ai_assisted', 'agent_created', 'ai_review')
        AND toString(attr.org_id) = {org_id:String}
    LEFT JOIN work_items AS wi FINAL
        ON wi.repo_id = link.repo_id AND wi.work_item_id = link.work_item_id
        AND wi.org_id = {org_id:String}
    WHERE ((pr.created_at >= {start:DateTime64(3, 'UTC')} AND pr.created_at < {end:DateTime64(3, 'UTC')})
        OR (pr.merged_at IS NOT NULL AND pr.merged_at >= {start:DateTime64(3, 'UTC')} AND pr.merged_at < {end:DateTime64(3, 'UTC')}))
      %[1]s
      AND pr.org_id = {org_id:String}
    UNION ALL
    SELECT
        toString(pr.repo_id) AS repo_id_str,
        pr.repo_id AS repo_id,
        pr.number AS number,
        attr.kind AS kind,
        'pull_request' AS work_type,
        pr.title AS title,
        pr.merged_at AS merged_at
    FROM git_pull_requests AS pr
    INNER JOIN ai_attribution_resolved AS attr
        ON attr.subject_type = 'pull_request'
        AND attr.repo_id = pr.repo_id
        AND (attr.subject_id = toString(pr.number) OR attr.subject_id = concat(toString(pr.repo_id), '#', toString(pr.number)))
        AND attr.kind IN ('ai_assisted', 'agent_created', 'ai_review')
        AND toString(attr.org_id) = {org_id:String}
    WHERE ((pr.created_at >= {start:DateTime64(3, 'UTC')} AND pr.created_at < {end:DateTime64(3, 'UTC')})
        OR (pr.merged_at IS NOT NULL AND pr.merged_at >= {start:DateTime64(3, 'UTC')} AND pr.merged_at < {end:DateTime64(3, 'UTC')}))
      %[1]s
      AND pr.org_id = {org_id:String}
)
ORDER BY merged_at DESC NULLS LAST, repo_id, number DESC
LIMIT {limit:UInt32} OFFSET {offset:UInt32}`

// loadAttributedPRs reads one page of AI-attributed pull requests, first
// occurrence of each (repository, number) kept.
func loadAttributedPRs(ctx context.Context, client QueryClient, orgID string, startDay, endDay time.Time, repoID string, repoIDs []string, useIDs bool, limit, offset int) ([]attributedPR, error) {
	start, end := dayBounds(startDay, endDay)
	bindings := []clickhouse.Binding{
		{Name: "start", Value: start},
		{Name: "end", Value: end},
		{Name: "org_id", Value: orgID},
	}
	sf := scopeFilter(&bindings, repoID, repoIDs, useIDs)
	bindings = append(bindings,
		clickhouse.Binding{Name: "limit", Value: uint32(limit)},
		clickhouse.Binding{Name: "offset", Value: uint32(offset)})
	rs, err := client.Query(ctx, fmt.Sprintf(attributedPRsStatement, sf), bindings)
	if err != nil {
		return nil, fmt.Errorf("aianalytics: attributed pull requests query: %w", err)
	}
	defer rs.Close()
	seen := map[string]bool{}
	var out []attributedPR
	for rs.Next() {
		var r attributedPR
		var (
			kind, workType string
			number         uint32
		)
		if err := rs.Scan(&r.RepoID, &number, &kind, &workType, &r.Title, &r.MergedAt); err != nil {
			return nil, fmt.Errorf("aianalytics: attributed pull requests scan: %w", err)
		}
		r.Number = int64(number)
		r.Kind, r.WorkType = &kind, &workType
		key := fmt.Sprintf("%s#%d", r.RepoID, r.Number)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: attributed pull requests rows: %w", err)
	}
	return out, nil
}

// attributionMixRow is one resolved-kind count.
type attributionMixRow struct {
	Kind  string
	Count int64
}

// attributionEvidence is one resolved attribution record.
type attributionEvidence struct {
	SubjectType string
	SubjectID   string
	RepoID      *string
	Provider    string
	Kind        string
	Source      string
	Confidence  float32
	Actor       *string
	Evidence    string
	ObservedAt  time.Time
	TeamID      *string
}

func attributionFilters(bindings *[]clickhouse.Binding, repoID string, repoIDs []string, useIDs bool, kinds []string) string {
	out := ""
	if repoID != "" {
		*bindings = append(*bindings, clickhouse.Binding{Name: "repo_id", Value: repoID})
		out += "\n  AND repo_id = {repo_id:UUID}"
	}
	if useIDs {
		*bindings = append(*bindings, clickhouse.Binding{Name: "repo_ids", Value: repoIDs})
		out += "\n  AND toString(repo_id) IN {repo_ids:Array(String)}"
	}
	if len(kinds) > 0 {
		*bindings = append(*bindings, clickhouse.Binding{Name: "kinds", Value: kinds})
		out += "\n  AND kind IN {kinds:Array(String)}"
	}
	return out
}

func loadAttributionMix(ctx context.Context, client QueryClient, orgID string, startDay, endDay time.Time, repoID string, repoIDs []string, useIDs bool, kinds []string) ([]attributionMixRow, error) {
	start, end := dayBounds(startDay, endDay)
	bindings := []clickhouse.Binding{
		{Name: "start", Value: start}, {Name: "end", Value: end}, {Name: "org_id", Value: orgID},
	}
	filters := attributionFilters(&bindings, repoID, repoIDs, useIDs, kinds)
	rs, err := client.Query(ctx, `SELECT
    kind,
    count() AS count
FROM ai_attribution_resolved
WHERE observed_at >= {start:DateTime64(3, 'UTC')}
  AND observed_at < {end:DateTime64(3, 'UTC')}`+filters+`
  AND toString(org_id) = {org_id:String}
GROUP BY kind
ORDER BY kind`, bindings)
	if err != nil {
		return nil, fmt.Errorf("aianalytics: attribution mix query: %w", err)
	}
	defer rs.Close()
	var out []attributionMixRow
	for rs.Next() {
		var r attributionMixRow
		var n uint64
		if err := rs.Scan(&r.Kind, &n); err != nil {
			return nil, fmt.Errorf("aianalytics: attribution mix scan: %w", err)
		}
		r.Count = int64(n)
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: attribution mix rows: %w", err)
	}
	return out, nil
}

func loadAttributionEvidence(ctx context.Context, client QueryClient, orgID string, startDay, endDay time.Time, repoID string, repoIDs []string, useIDs bool, kinds []string, limit, offset int) ([]attributionEvidence, error) {
	start, end := dayBounds(startDay, endDay)
	bindings := []clickhouse.Binding{
		{Name: "start", Value: start}, {Name: "end", Value: end}, {Name: "org_id", Value: orgID},
	}
	filters := attributionFilters(&bindings, repoID, repoIDs, useIDs, kinds)
	bindings = append(bindings,
		clickhouse.Binding{Name: "limit", Value: uint32(limit)},
		clickhouse.Binding{Name: "offset", Value: uint32(offset)})
	rs, err := client.Query(ctx, `SELECT
    subject_type,
    subject_id,
    toString(repo_id) AS repo_id_str,
    provider,
    kind,
    source,
    confidence,
    actor,
    evidence,
    observed_at
FROM ai_attribution_resolved
WHERE observed_at >= {start:DateTime64(3, 'UTC')}
  AND observed_at < {end:DateTime64(3, 'UTC')}`+filters+`
  AND toString(org_id) = {org_id:String}
ORDER BY observed_at DESC, subject_id
LIMIT {limit:UInt32} OFFSET {offset:UInt32}`, bindings)
	if err != nil {
		return nil, fmt.Errorf("aianalytics: attribution evidence query: %w", err)
	}
	defer rs.Close()
	var out []attributionEvidence
	for rs.Next() {
		var r attributionEvidence
		if err := rs.Scan(&r.SubjectType, &r.SubjectID, &r.RepoID, &r.Provider, &r.Kind, &r.Source, &r.Confidence, &r.Actor, &r.Evidence, &r.ObservedAt); err != nil {
			return nil, fmt.Errorf("aianalytics: attribution evidence scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: attribution evidence rows: %w", err)
	}
	return out, nil
}

// repoTeamMap resolves each repository id to the team its full name
// matches; nil means the catalogues could not be read.
func repoTeamMap(ctx context.Context, client QueryClient, orgID string, repoIDs []string, operation string) map[string]*string {
	if len(repoIDs) == 0 || orgID == "" {
		return map[string]*string{}
	}
	teams, err := loadTeams(ctx, client, orgID)
	if err != nil {
		warnCatalogue(ctx, operation, err)
		return map[string]*string{}
	}
	names, err := loadRepoNamesFor(ctx, client, orgID, repoIDs)
	if err != nil {
		warnCatalogue(ctx, operation, err)
		return map[string]*string{}
	}
	return resolveTeams(teams, repoIDs, names)
}

func loadRepoNamesFor(ctx context.Context, client QueryClient, orgID string, ids []string) (map[string]string, error) {
	rs, err := client.Query(ctx, `SELECT toString(id) AS repo_id, coalesce(repo, '') AS full_name
FROM repos
WHERE org_id = {org_id:String}
  AND toString(id) IN {repo_ids:Array(String)}`, []clickhouse.Binding{
		{Name: "org_id", Value: orgID}, {Name: "repo_ids", Value: ids},
	})
	if err != nil {
		return nil, fmt.Errorf("repos query: %w", err)
	}
	defer rs.Close()
	out := map[string]string{}
	for rs.Next() {
		var id, name string
		if err := rs.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("repos scan: %w", err)
		}
		if name == "" {
			name = id
		}
		out[id] = name
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("repos rows: %w", err)
	}
	return out, nil
}
