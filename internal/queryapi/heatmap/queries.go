// ClickHouse readers -- ports api/queries/heatmap.py's seven functions
// verbatim, with the class-ruling FINAL-dedup fixes this package's doc
// comment declares (repos/git_pull_requests/git_commits read FINAL, org_id
// inside the same statement as every FINAL source, never a separate
// unfiltered subquery).
package heatmap

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// dateBindingValue formats t as a bare "YYYY-MM-DD" string for binding
// into a {name:Date}-typed native ClickHouse parameter -- REQUIRED, not
// cosmetic: dev-health-go's clickHouseParameter formats every time.Time
// value with a full DateTime literal regardless of the placeholder's
// declared type, so a {start_day:Date}/{end_day:Date}/{week_start:Date}/
// {week_end:Date} placeholder bound to a raw time.Time always fails live
// with "Value ... cannot be parsed as Date ... only 10 of 23 bytes was
// parsed", confirmed live against hotspot_risk's own two reads. Same fix
// shape as analytics.dateBindingValue and every other package carrying
// this class of bug; duplicated here per this binary's own "repeat,
// don't couple" convention for a helper this narrow.
func dateBindingValue(t time.Time) string {
	year, month, day := t.Date()
	return fmt.Sprintf("%04d-%02d-%02d", year, int(month), day)
}

// reviewWaitDensityRow is fetch_review_wait_density's row shape
// (queries/heatmap.py:13-38).
type reviewWaitDensityRow struct {
	Weekday int32
	Hour    int32
	Value   float64
}

// fetchReviewWaitDensity ports fetch_review_wait_density verbatim.
func fetchReviewWaitDensity(ctx context.Context, client QueryClient, startTS, endTS time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string) ([]reviewWaitDensityRow, error) {
	query := fmt.Sprintf(`
        SELECT
            toInt32(toDayOfWeek(created_at)) AS weekday,
            toInt32(toHour(created_at)) AS hour,
            toFloat64(sum(dateDiff('minute', created_at, first_review_at)) / 60.0) AS value
        FROM git_pull_requests FINAL
        INNER JOIN repos FINAL ON repos.id = git_pull_requests.repo_id AND repos.org_id = {org_id:String}
        WHERE created_at >= {start_ts:DateTime64(3, 'UTC')}
          AND created_at < {end_ts:DateTime64(3, 'UTC')}
          AND first_review_at IS NOT NULL
        %s
        GROUP BY weekday, hour
        %s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_ts", Value: startTS},
		{Name: "end_ts", Value: endTS},
		{Name: "org_id", Value: orgID},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("heatmap: fetch review wait density: %w", err)
	}
	defer rows.Close()

	out := make([]reviewWaitDensityRow, 0)
	for rows.Next() {
		var row reviewWaitDensityRow
		if err := rows.Scan(&row.Weekday, &row.Hour, &row.Value); err != nil {
			return nil, fmt.Errorf("heatmap: scan review wait density row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("heatmap: iterate review wait density rows: %w", err)
	}
	return out, nil
}

// fetchReviewWaitEvidence ports fetch_review_wait_evidence verbatim.
func fetchReviewWaitEvidence(ctx context.Context, client QueryClient, startTS, endTS time.Time, weekday, hour int, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, limit int, orgID string) ([]ReviewWaitEvidenceItem, error) {
	query := fmt.Sprintf(`
        SELECT
            toString(git_pull_requests.repo_id) AS repo_id,
            git_pull_requests.number AS number,
            git_pull_requests.title AS title,
            git_pull_requests.created_at AS created_at,
            git_pull_requests.first_review_at AS first_review_at
        FROM git_pull_requests FINAL
        INNER JOIN repos FINAL ON repos.id = git_pull_requests.repo_id AND repos.org_id = {org_id:String}
        WHERE created_at >= {start_ts:DateTime64(3, 'UTC')}
          AND created_at < {end_ts:DateTime64(3, 'UTC')}
          AND first_review_at IS NOT NULL
          AND toDayOfWeek(created_at) = {weekday:UInt8}
          AND toHour(created_at) = {hour:UInt8}
        %s
        ORDER BY created_at DESC
        LIMIT {limit:UInt64}
        %s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_ts", Value: startTS},
		{Name: "end_ts", Value: endTS},
		{Name: "weekday", Value: weekday},
		{Name: "hour", Value: hour},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: limit},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("heatmap: fetch review wait evidence: %w", err)
	}
	defer rows.Close()

	out := make([]ReviewWaitEvidenceItem, 0)
	for rows.Next() {
		var item ReviewWaitEvidenceItem
		if err := rows.Scan(&item.RepoID, &item.Number, &item.Title, &item.CreatedAt, &item.FirstReviewAt); err != nil {
			return nil, fmt.Errorf("heatmap: scan review wait evidence row: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("heatmap: iterate review wait evidence rows: %w", err)
	}
	return out, nil
}

// repoTouchpointsRow is fetch_repo_touchpoints's per-day-per-repo row
// shape (queries/heatmap.py:96-136).
type repoTouchpointsRow struct {
	Day   time.Time
	Repo  string
	Value float64
}

// fetchRepoTouchpoints ports fetch_repo_touchpoints: a top-N query picks
// the busiest `limit` repos, then a second query returns every day/repo
// pair for exactly those repos. The top-N query's own ORDER BY carries a
// secondary `repo ASC` key the reference query does not: two repos tying
// on total commits resolve to the SAME set and order on every run, where
// the reference's own `ORDER BY total DESC` alone leaves ClickHouse free
// to settle a tie at the LIMIT boundary, or between two returned repos,
// differently run to run.
func fetchRepoTouchpoints(ctx context.Context, client QueryClient, startTS, endTS time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, limit int, orgID string) ([]repoTouchpointsRow, error) {
	topQuery := fmt.Sprintf(`
        SELECT
            repos.repo AS repo,
            count() AS total
        FROM git_commits FINAL
        INNER JOIN repos FINAL ON repos.id = git_commits.repo_id AND repos.org_id = {org_id:String}
        WHERE author_when >= {start_ts:DateTime64(3, 'UTC')}
          AND author_when < {end_ts:DateTime64(3, 'UTC')}
        %s
        GROUP BY repos.repo
        ORDER BY total DESC, repo ASC
        LIMIT {limit:UInt64}
        %s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	topBindings := append([]dhclickhouse.Binding{
		{Name: "start_ts", Value: startTS},
		{Name: "end_ts", Value: endTS},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: limit},
	}, scopeBindings...)

	topRows, err := client.Query(ctx, topQuery, topBindings)
	if err != nil {
		return nil, fmt.Errorf("heatmap: fetch repo touchpoints (top): %w", err)
	}
	var repos []string
	for topRows.Next() {
		var repo string
		var total uint64
		if err := topRows.Scan(&repo, &total); err != nil {
			topRows.Close()
			return nil, fmt.Errorf("heatmap: scan repo touchpoints top row: %w", err)
		}
		if repo != "" {
			repos = append(repos, repo)
		}
	}
	if err := topRows.Err(); err != nil {
		topRows.Close()
		return nil, fmt.Errorf("heatmap: iterate repo touchpoints top rows: %w", err)
	}
	topRows.Close()
	if len(repos) == 0 {
		return nil, nil
	}

	query := fmt.Sprintf(`
        SELECT
            toDate(author_when) AS day,
            repos.repo AS repo,
            toFloat64(count()) AS value
        FROM git_commits FINAL
        INNER JOIN repos FINAL ON repos.id = git_commits.repo_id AND repos.org_id = {org_id:String}
        WHERE author_when >= {start_ts:DateTime64(3, 'UTC')}
          AND author_when < {end_ts:DateTime64(3, 'UTC')}
          AND repos.repo IN {repos:Array(String)}
        %s
        GROUP BY day, repo
        ORDER BY day
        %s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_ts", Value: startTS},
		{Name: "end_ts", Value: endTS},
		{Name: "org_id", Value: orgID},
		{Name: "repos", Value: repos},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("heatmap: fetch repo touchpoints: %w", err)
	}
	defer rows.Close()

	out := make([]repoTouchpointsRow, 0)
	for rows.Next() {
		var row repoTouchpointsRow
		if err := rows.Scan(&row.Day, &row.Repo, &row.Value); err != nil {
			return nil, fmt.Errorf("heatmap: scan repo touchpoints row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("heatmap: iterate repo touchpoints rows: %w", err)
	}
	return out, nil
}

// hotspotRiskRow is fetch_hotspot_risk's per-week-per-file row shape
// (queries/heatmap.py:139-186).
type hotspotRiskRow struct {
	Week    time.Time
	FileKey string
	Value   float64
}

// fetchHotspotRisk ports fetch_hotspot_risk (top-N files by summed
// hotspot_score, then the same set's per-week series), with the same
// secondary `file_key ASC` ORDER BY key fetchRepoTouchpoints' own top-N
// query carries and the reference query does not -- see
// fetchRepoTouchpoints' own doc comment for why.
// file_metrics_daily is ReplacingMergeTree(computed_at) since migration
// 096: read FINAL, org_id filtered in the same statement.
func fetchHotspotRisk(ctx context.Context, client QueryClient, startDay, endDay time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, limit int, orgID string) ([]hotspotRiskRow, error) {
	topQuery := fmt.Sprintf(`
        SELECT
            concat(repos.repo, ':', path) AS file_key,
            toFloat64(sum(hotspot_score)) AS total
        FROM file_metrics_daily FINAL
        INNER JOIN repos FINAL ON repos.id = file_metrics_daily.repo_id AND repos.org_id = {org_id:String}
        WHERE day >= {start_day:Date}
          AND day < {end_day:Date}
          AND file_metrics_daily.org_id = {org_id:String}
        %s
        GROUP BY file_key
        ORDER BY total DESC, file_key ASC
        LIMIT {limit:UInt64}
        %s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	topBindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: dateBindingValue(startDay)},
		{Name: "end_day", Value: dateBindingValue(endDay)},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: limit},
	}, scopeBindings...)

	topRows, err := client.Query(ctx, topQuery, topBindings)
	if err != nil {
		return nil, fmt.Errorf("heatmap: fetch hotspot risk (top): %w", err)
	}
	var files []string
	for topRows.Next() {
		var fileKey string
		var total float64
		if err := topRows.Scan(&fileKey, &total); err != nil {
			topRows.Close()
			return nil, fmt.Errorf("heatmap: scan hotspot risk top row: %w", err)
		}
		if fileKey != "" {
			files = append(files, fileKey)
		}
	}
	if err := topRows.Err(); err != nil {
		topRows.Close()
		return nil, fmt.Errorf("heatmap: iterate hotspot risk top rows: %w", err)
	}
	topRows.Close()
	if len(files) == 0 {
		return nil, nil
	}

	query := fmt.Sprintf(`
        SELECT
            toStartOfWeek(day) AS week,
            concat(repos.repo, ':', path) AS file_key,
            toFloat64(sum(hotspot_score)) AS value
        FROM file_metrics_daily FINAL
        INNER JOIN repos FINAL ON repos.id = file_metrics_daily.repo_id AND repos.org_id = {org_id:String}
        WHERE day >= {start_day:Date}
          AND day < {end_day:Date}
          AND concat(repos.repo, ':', path) IN {files:Array(String)}
          AND file_metrics_daily.org_id = {org_id:String}
        %s
        GROUP BY week, file_key
        ORDER BY week
        %s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: dateBindingValue(startDay)},
		{Name: "end_day", Value: dateBindingValue(endDay)},
		{Name: "org_id", Value: orgID},
		{Name: "files", Value: files},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("heatmap: fetch hotspot risk: %w", err)
	}
	defer rows.Close()

	out := make([]hotspotRiskRow, 0)
	for rows.Next() {
		var row hotspotRiskRow
		if err := rows.Scan(&row.Week, &row.FileKey, &row.Value); err != nil {
			return nil, fmt.Errorf("heatmap: scan hotspot risk row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("heatmap: iterate hotspot risk rows: %w", err)
	}
	return out, nil
}

// hotspotEvidenceRow is fetch_hotspot_evidence's row shape (queries/
// heatmap.py:189-224). Its own fetch result is always discarded by
// BuildResponse (see package doc comment); this type/function still
// exist so the read itself -- and any error it can raise -- is
// reproduced faithfully.
type hotspotEvidenceRow struct {
	Day          time.Time
	Repo         string
	Path         string
	Churn        int64
	Contributors int64
	CommitsCount int64
	HotspotScore float64
}

// fetchHotspotEvidence ports fetch_hotspot_evidence verbatim.
func fetchHotspotEvidence(ctx context.Context, client QueryClient, weekStart, weekEnd time.Time, fileKey, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, limit int, orgID string) ([]hotspotEvidenceRow, error) {
	query := fmt.Sprintf(`
        SELECT
            day,
            repos.repo AS repo,
            path,
            toInt64(churn) AS churn,
            toInt64(contributors) AS contributors,
            toInt64(commits_count) AS commits_count,
            toFloat64(hotspot_score) AS hotspot_score
        FROM file_metrics_daily FINAL
        INNER JOIN repos FINAL ON repos.id = file_metrics_daily.repo_id AND repos.org_id = {org_id:String}
        WHERE day >= {week_start:Date}
          AND day < {week_end:Date}
          AND concat(repos.repo, ':', path) = {file_key:String}
          AND file_metrics_daily.org_id = {org_id:String}
        %s
        ORDER BY day
        LIMIT {limit:UInt64}
        %s
    `, scopeFilterSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "week_start", Value: dateBindingValue(weekStart)},
		{Name: "week_end", Value: dateBindingValue(weekEnd)},
		{Name: "file_key", Value: fileKey},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: limit},
	}, scopeBindings...)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("heatmap: fetch hotspot evidence: %w", err)
	}
	defer rows.Close()

	out := make([]hotspotEvidenceRow, 0)
	for rows.Next() {
		var row hotspotEvidenceRow
		if err := rows.Scan(&row.Day, &row.Repo, &row.Path, &row.Churn, &row.Contributors, &row.CommitsCount, &row.HotspotScore); err != nil {
			return nil, fmt.Errorf("heatmap: scan hotspot evidence row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("heatmap: iterate hotspot evidence rows: %w", err)
	}
	return out, nil
}

// individualActiveHoursRow is fetch_individual_active_hours's row shape
// (queries/heatmap.py:227-250).
type individualActiveHoursRow struct {
	Weekday int32
	Hour    int32
	Value   float64
}

// fetchIndividualActiveHours ports fetch_individual_active_hours
// verbatim. identities empty returns (nil, nil) without querying,
// matching Python's own `if not identities: return []` guard.
func fetchIndividualActiveHours(ctx context.Context, client QueryClient, startTS, endTS time.Time, identities []string, orgID string) ([]individualActiveHoursRow, error) {
	if len(identities) == 0 {
		return nil, nil
	}

	query := fmt.Sprintf(`
        SELECT
            toInt32(toDayOfWeek(author_when)) AS weekday,
            toInt32(toHour(author_when)) AS hour,
            toFloat64(count()) AS value
        FROM git_commits FINAL
        INNER JOIN repos FINAL ON repos.id = git_commits.repo_id AND repos.org_id = {org_id:String}
        WHERE author_when >= {start_ts:DateTime64(3, 'UTC')}
          AND author_when < {end_ts:DateTime64(3, 'UTC')}
          AND (author_email IN {identities:Array(String)} OR author_name IN {identities:Array(String)})
        GROUP BY weekday, hour
        %s
    `, settingsMaxExecutionTime())
	bindings := []dhclickhouse.Binding{
		{Name: "start_ts", Value: startTS},
		{Name: "end_ts", Value: endTS},
		{Name: "identities", Value: identities},
		{Name: "org_id", Value: orgID},
	}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("heatmap: fetch individual active hours: %w", err)
	}
	defer rows.Close()

	out := make([]individualActiveHoursRow, 0)
	for rows.Next() {
		var row individualActiveHoursRow
		if err := rows.Scan(&row.Weekday, &row.Hour, &row.Value); err != nil {
			return nil, fmt.Errorf("heatmap: scan individual active hours row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("heatmap: iterate individual active hours rows: %w", err)
	}
	return out, nil
}

// fetchIndividualActiveEvidence ports fetch_individual_active_evidence
// verbatim. identities empty returns (nil, nil) without querying,
// matching Python's own `if not identities: return []` guard.
func fetchIndividualActiveEvidence(ctx context.Context, client QueryClient, startTS, endTS time.Time, weekday, hour int, identities []string, limit int, orgID string) ([]IndividualActiveEvidenceItem, error) {
	if len(identities) == 0 {
		return nil, nil
	}

	query := fmt.Sprintf(`
        SELECT
            repos.repo AS repo,
            git_commits.hash AS commit_hash,
            git_commits.message AS message,
            git_commits.author_name AS author_name,
            git_commits.author_email AS author_email,
            git_commits.author_when AS author_when
        FROM git_commits FINAL
        INNER JOIN repos FINAL ON repos.id = git_commits.repo_id AND repos.org_id = {org_id:String}
        WHERE author_when >= {start_ts:DateTime64(3, 'UTC')}
          AND author_when < {end_ts:DateTime64(3, 'UTC')}
          AND toDayOfWeek(author_when) = {weekday:UInt8}
          AND toHour(author_when) = {hour:UInt8}
          AND (author_email IN {identities:Array(String)} OR author_name IN {identities:Array(String)})
        ORDER BY author_when DESC
        LIMIT {limit:UInt64}
        %s
    `, settingsMaxExecutionTime())
	bindings := []dhclickhouse.Binding{
		{Name: "start_ts", Value: startTS},
		{Name: "end_ts", Value: endTS},
		{Name: "weekday", Value: weekday},
		{Name: "hour", Value: hour},
		{Name: "identities", Value: identities},
		{Name: "limit", Value: limit},
		{Name: "org_id", Value: orgID},
	}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("heatmap: fetch individual active evidence: %w", err)
	}
	defer rows.Close()

	out := make([]IndividualActiveEvidenceItem, 0)
	for rows.Next() {
		var item IndividualActiveEvidenceItem
		if err := rows.Scan(&item.Repo, &item.CommitHash, &item.Message, &item.AuthorName, &item.AuthorEmail, &item.AuthorWhen); err != nil {
			return nil, fmt.Errorf("heatmap: scan individual active evidence row: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("heatmap: iterate individual active evidence rows: %w", err)
	}
	return out, nil
}

// --- metricRow conversion helpers -------------------------------------

func reviewWaitDensityToMetricRows(rows []reviewWaitDensityRow) []metricRow {
	out := make([]metricRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, metricRow{X: int(r.Hour), Y: int(r.Weekday), Value: r.Value})
	}
	return out
}

func repoTouchpointsToMetricRows(rows []repoTouchpointsRow) []metricRow {
	out := make([]metricRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, metricRow{X: r.Day, Y: r.Repo, Value: r.Value})
	}
	return out
}

func hotspotRiskToMetricRows(rows []hotspotRiskRow) []metricRow {
	out := make([]metricRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, metricRow{X: r.Week, Y: r.FileKey, Value: r.Value})
	}
	return out
}

func individualActiveHoursToMetricRows(rows []individualActiveHoursRow) []metricRow {
	out := make([]metricRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, metricRow{X: int(r.Hour), Y: int(r.Weekday), Value: r.Value})
	}
	return out
}
