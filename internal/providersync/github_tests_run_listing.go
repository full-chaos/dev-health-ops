package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The keys and observation names a github tests/cicd unit result carries. They are exported so a reader's tests (the
// sync-coverage cells) seed a result from the same constants the producer writes: a rename breaks their build.
const (
	GitHubTestsResultKeyReportsComplete    = "reports_complete"
	GitHubTestsResultKeyIncomplete         = "incomplete"
	GitHubTestsResultKeyOpenRunsReread     = "open_runs_reread"
	GitHubTestsResultKeyOpenCapReached     = "open_runs_cap_reached"
	GitHubTestsResultKeyOpenPhaseRateFloor = "open_phase_rate_floor"
	GitHubTestsRunInventorySaturated       = githubTestsRunInventorySaturatedComponent
	GitHubTestsArtifactInventorySaturated  = githubTestsArtifactInventorySaturatedComponent
	GitHubTestsListingSaturated            = githubTestsListingSaturatedCause
	GitHubTestsRunReports                  = githubTestsRunReportsComponent
	GitHubTestsAggregateOfParts            = githubTestsAggregateOfPartsCause
	GitHubTestsPerRunCap                   = githubTestsPerRunCapCause
)

const (
	// githubTestsSearchResultCap is GitHub's documented limit of results for
	// one created search of the workflow-runs listing: the listing ends after
	// 1,000 runs with no next link, whatever the page budget says.
	githubTestsSearchResultCap = 1000
	// githubTestsRefreshLookbackDefault is how far before the window a run
	// created earlier is still listed by the runs phase, so a run that finishes
	// or is rerun soon after its creation is refreshed. Older open runs are
	// re-read by id (the open phase).
	githubTestsRefreshLookbackDefault = 6 * time.Hour
	// The open phase re-reads at most githubTestsOpenRunCapSteady runs a unit,
	// and githubTestsOpenRunCapBacklog while more than
	// githubTestsOpenRunBacklogThreshold open rows older than the lookback wait.
	githubTestsOpenRunCapSteady        = 100
	githubTestsOpenRunCapBacklog       = 300
	githubTestsOpenRunBacklogThreshold = 1000
	githubTestsOpenPhase               = "open"
)

// githubTestsRunsRangeLo is the start of the runs phase listing: the window
// start minus the refresh lookback, to the second ("*" for an open start).
func githubTestsRunsRangeLo(claim Claim, lookback time.Duration) string {
	if claim.SinceAt == nil {
		return "*"
	}
	return claim.SinceAt.UTC().Add(-lookback).Format(time.RFC3339)
}

// githubTestsRunsRangeHi is the end of a listing range: the peeled end when a
// saturated range moved it, else the window end ("*" for an open end).
func githubTestsRunsRangeHi(claim Claim, peeledHi string) string {
	if peeledHi != "" {
		return peeledHi
	}
	if claim.BeforeAt == nil {
		return "*"
	}
	return claim.BeforeAt.UTC().Format(time.RFC3339)
}

// githubTestsRunsQuery is the created range of one runs-phase listing.
func githubTestsRunsQuery(claim Claim, lookback time.Duration, peeledHi string) url.Values {
	query := url.Values{"per_page": {"100"}}
	lo, hi := githubTestsRunsRangeLo(claim, lookback), githubTestsRunsRangeHi(claim, peeledHi)
	if lo != "*" || hi != "*" {
		query.Set("created", lo+".."+hi)
	}
	return query
}

// githubTestsArtifactRunQueryAt is githubTestsArtifactRunQuery with the end of
// the range moved to peeledHi when a saturated range was peeled.
func githubTestsArtifactRunQueryAt(claim Claim, peeledHi string) url.Values {
	query := githubTestsArtifactRunQuery(claim)
	if peeledHi == "" || claim.SinceAt == nil {
		return query
	}
	query.Set("created", githubTestsArtifactRunsCreatedFrom(claim)+".."+peeledHi)
	return query
}

// githubTestsPeel decides what a finished range listing means. A listing of
// the documented cap of results may hide older runs: the next range ends at the
// oldest created time seen (inclusive, to the second, so runs sharing that
// second are listed again and kept apart by their id). A range whose oldest
// time did not move made no progress, so it is reported as stuck.
func githubTestsPeel(items int, oldest, rangeHi string) (nextHi string, saturated, stuck bool) {
	if items < githubTestsSearchResultCap {
		return "", false, false
	}
	if oldest == "" {
		return "", true, true
	}
	if rangeHi != "" && rangeHi != "*" {
		oldestTime, oldestErr := time.Parse(time.RFC3339, oldest)
		hiTime, hiErr := time.Parse(time.RFC3339, rangeHi)
		if oldestErr == nil && hiErr == nil && !oldestTime.Before(hiTime) {
			return "", true, true
		}
	}
	return oldest, true, false
}

// githubTestsRunStatusOpen reports a stored pipeline status that is not final.
func githubTestsRunStatusOpen(status *string) bool {
	if status == nil {
		return false
	}
	return *status == "running" || *status == "queued"
}

// githubTestsRunWanted is the window rule of the runs phase. The listing key is
// created, so a run created inside the window is kept whatever its latest
// attempt's start (a rerun or a run queued across the window end). A run
// created before the window (the refresh lookback part) is kept only when it
// changed since the window start or is still open.
func githubTestsRunWanted(run gitHubWorkflowRunPayload, pipeline githubTestsPipelineRow, claim Claim) bool {
	created := parseGitHubWorkflowTime(run.CreatedAt)
	if created == nil {
		created = &pipeline.StartedAt
	}
	if claim.BeforeAt != nil && created.UTC().After(claim.BeforeAt.UTC()) {
		return false
	}
	if claim.SinceAt == nil || !created.UTC().Before(claim.SinceAt.UTC()) {
		return true
	}
	updated := parseGitHubWorkflowTime(run.UpdatedAt)
	if updated == nil {
		return true
	}
	return !updated.UTC().Before(claim.SinceAt.UTC()) || githubTestsRunStatusOpen(pipeline.Status)
}

// GitHubOpenRunsReader reads the stored pipeline rows of one repository that
// are still open (running or queued) and started before a time: the runs the
// listing no longer reaches.
type GitHubOpenRunsReader interface {
	// OpenRuns returns up to limit open rows, oldest first, after the keyset
	// (afterStarted, afterRunID); a zero afterStarted starts from the beginning.
	OpenRuns(ctx context.Context, claim Claim, repoID string, startedBefore time.Time, afterStarted time.Time, afterRunID string, limit int) ([]githubTestsPipelineRow, error)
	// OpenRunBacklog counts the open rows started before the time, stopping at limit.
	OpenRunBacklog(ctx context.Context, claim Claim, repoID string, startedBefore time.Time, limit int) (int, error)
}

// GitHubOpenRunsClickHouseReader reads the open rows from ci_pipeline_runs on
// its key prefix (org_id, repo_id).
type GitHubOpenRunsClickHouseReader struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

const githubTestsOpenRunsColumns = `org_id,repo_id,run_id,pipeline_name,provider,status,queued_at,started_at,finished_at,duration_seconds,queue_seconds,retry_count,cancel_reason,trigger_source,commit_hash,branch,pr_number,team_id,service_id,last_synced`

const githubTestsOpenRunsWhere = `WHERE org_id = ? AND repo_id = ? AND provider IN ('github_actions', '')
  AND lower(trimBoth(ifNull(status, ''))) IN ('running', 'queued') AND started_at < ?`

func (reader GitHubOpenRunsClickHouseReader) OpenRuns(
	ctx context.Context, claim Claim, repoID string, startedBefore, afterStarted time.Time, afterRunID string, limit int,
) ([]githubTestsPipelineRow, error) {
	if ctx == nil || reader.Conn == nil || reader.Lease == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.OrgID == "" || repoID == "" || limit < 1 {
		return nil, ErrInvalidConfiguration
	}
	if err := reader.Lease.Assert(ctx); err != nil {
		return nil, err
	}
	rows, err := reader.Conn.Query(ctx, `SELECT `+githubTestsOpenRunsColumns+` FROM ci_pipeline_runs FINAL `+githubTestsOpenRunsWhere+`
  AND (started_at, run_id) > (?, ?)
ORDER BY started_at ASC, run_id ASC LIMIT ?
SETTINGS max_execution_time = 30, max_rows_to_read = 50000000`,
		claim.OrgID, repoID, startedBefore.UTC(), afterStarted.UTC(), afterRunID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]githubTestsPipelineRow, 0, limit)
	for rows.Next() {
		var row githubTestsPipelineRow
		if err := rows.Scan(&row.OrgID, &row.RepoID, &row.RunID, &row.PipelineName, &row.Provider, &row.Status, &row.QueuedAt,
			&row.StartedAt, &row.FinishedAt, &row.DurationSeconds, &row.QueueSeconds, &row.RetryCount, &row.CancelReason,
			&row.TriggerSource, &row.CommitHash, &row.Branch, &row.PRNumber, &row.TeamID, &row.ServiceID, &row.LastSynced); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (reader GitHubOpenRunsClickHouseReader) OpenRunBacklog(
	ctx context.Context, claim Claim, repoID string, startedBefore time.Time, limit int,
) (int, error) {
	if ctx == nil || reader.Conn == nil || reader.Lease == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.OrgID == "" || repoID == "" || limit < 1 {
		return 0, ErrInvalidConfiguration
	}
	if err := reader.Lease.Assert(ctx); err != nil {
		return 0, err
	}
	rows, err := reader.Conn.Query(ctx, `SELECT count() FROM (SELECT run_id FROM ci_pipeline_runs FINAL `+githubTestsOpenRunsWhere+` LIMIT ?)
SETTINGS max_execution_time = 30, max_rows_to_read = 50000000`,
		claim.OrgID, repoID, startedBefore.UTC(), limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var count uint64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, err
		}
	}
	return int(count), rows.Err()
}

// githubTestsOpenRunCap is the number of open runs one unit re-reads.
func githubTestsOpenRunCap(backlog int) int {
	if backlog > githubTestsOpenRunBacklogThreshold {
		return githubTestsOpenRunCapBacklog
	}
	return githubTestsOpenRunCapSteady
}

// githubTestsDeletedRow is the stored row of a run GitHub no longer has: the
// same row with the status testops.StatusDeleted and a later last_synced, so
// every reader of pipeline counts leaves it out.
func githubTestsDeletedRow(row githubTestsPipelineRow, at time.Time) githubTestsPipelineRow {
	deleted := testops.StatusDeleted
	row.Status = &deleted
	row.LastSynced = at
	return row
}

// githubTestsPhaseAfterArtifacts is the phase that follows the artifacts
// phase: the open phase when an open-runs reader is wired, else the end.
func githubTestsPhaseAfterArtifacts(handler GitHubTestsRouteHandler) string {
	if handler.OpenRuns != nil {
		return githubTestsOpenPhase
	}
	return "done"
}

// githubTestsOlderCreated keeps the older of the stored oldest created time
// (RFC3339, second resolution as the created filter reads it) and a new one.
func githubTestsOlderCreated(current string, created *time.Time) string {
	if created == nil {
		return current
	}
	candidate := created.UTC().Format(time.RFC3339)
	if current == "" {
		return candidate
	}
	currentTime, err := time.Parse(time.RFC3339, current)
	if err != nil || created.UTC().Before(currentTime) {
		return candidate
	}
	return current
}

// refreshLookback is how far before the window the runs phase lists.
func (handler GitHubTestsRouteHandler) refreshLookback() time.Duration {
	if handler.RefreshLookback > 0 {
		return handler.RefreshLookback
	}
	return githubTestsRefreshLookbackDefault
}

// recordGitHubTestsSaturated records a listing range that cannot be walked
// past GitHub's cap of results: more runs share one second than the cap allows.
// The cursor keeps one flag per phase; githubTestsFinalMetadataBatch turns the
// flags into the long observations of the unit result.
func recordGitHubTestsSaturated(
	cursor githubTestsChunkCursor, client *providerfoundation.HTTPClient, claim Claim,
	component, phase, rangeHi string, items int,
) githubTestsChunkCursor {
	if component == githubTestsRunInventorySaturatedComponent {
		cursor.SaturatedRuns = true
	} else {
		cursor.SaturatedArtifacts = true
	}
	if client != nil {
		client.Metrics.RecordInventoryPageCap(claim.Provider, claim.Dataset)
	}
	slog.Warn(
		"provider listing saturated: the created range cannot be walked past the result cap",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", cursor.Repo, "phase", phase, "component", component, "range_hi", rangeHi, "items", items,
	)
	return cursor
}

// githubTestsSaturatedObservations is the long form of the cursor's saturation
// flags, appended to the unit result's incomplete list.
func githubTestsSaturatedObservations(cursor githubTestsChunkCursor) []GitHubTestsIncomplete {
	var out []GitHubTestsIncomplete
	if cursor.SaturatedRuns {
		out = append(out, GitHubTestsIncomplete{Component: githubTestsRunInventorySaturatedComponent, Cause: githubTestsListingSaturatedCause, Count: 1})
	}
	if cursor.SaturatedArtifacts {
		out = append(out, GitHubTestsIncomplete{Component: githubTestsArtifactInventorySaturatedComponent, Cause: githubTestsListingSaturatedCause, Count: 1})
	}
	return out
}

// fetchGitHubTestsRun reads one run by id. found is false when GitHub no
// longer has it.
func fetchGitHubTestsRun(
	ctx context.Context, client *providerfoundation.HTTPClient, root, runID string,
) (run gitHubWorkflowRunPayload, found bool, err error) {
	response, err := client.DoRoute(ctx, http.MethodGet, githubRunRoute, root+"/actions/runs/"+url.PathEscape(runID), nil)
	if err != nil {
		if gitLabErrorTreeOnlyProviderClasses(err, providerfoundation.ErrorNotFound) {
			return run, false, nil
		}
		return run, false, err
	}
	defer response.Body.Close()
	body, err := providerfoundation.ReadResponseBody(response, nativeMaxObjectBytes)
	if err != nil {
		return run, false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if decoder.Decode(&run) != nil {
		return run, false, providerfoundation.ErrNormalizationInvalid
	}
	return run, true, nil
}

// collectGitHubTestsRunsPeeled lists every run of a created range for the
// non-chunked route, peeling a saturated range exactly as the chunked route
// does and keeping each run once by id. over is true when the page budget ran
// out or a range made no progress: the listing is then incomplete and the
// caller fails closed.
func collectGitHubTestsRunsPeeled(
	ctx context.Context, client *providerfoundation.HTTPClient, root string, pageBudget int,
	queryFor func(peeledHi string) url.Values,
) (items []json.RawMessage, pages int, over bool, err error) {
	seen := map[string]struct{}{}
	hi := ""
	for {
		page, collectErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path: root + "/actions/runs", Route: githubRunsRoute, Query: queryFor(hi), DataKey: "workflow_runs",
			MaxPages: pageBudget - pages,
		})
		if collectErr != nil {
			return nil, pages, false, collectErr
		}
		pages += page.Pages
		oldest, count := "", 0
		for _, raw := range page.Items {
			var run gitHubWorkflowRunPayload
			if json.Unmarshal(raw, &run) != nil {
				return nil, pages, false, providerfoundation.ErrNormalizationInvalid
			}
			count++
			oldest = githubTestsOlderCreated(oldest, parseGitHubWorkflowTime(run.CreatedAt))
			if _, duplicate := seen[run.ID.String()]; !duplicate {
				seen[run.ID.String()] = struct{}{}
				items = append(items, raw)
			}
		}
		if page.PageBudgetExhausted {
			return items, pages, true, nil
		}
		nextHi, saturated, stuck := githubTestsPeel(count, oldest, hi)
		switch {
		case !saturated:
			return items, pages, false, nil
		case stuck || pages >= pageBudget:
			return items, pages, true, nil
		}
		hi = nextHi
	}
}

// githubTestsRateObserver keeps the latest rate-limit headers seen on this
// invocation's own responses (no extra request). The open phase reads it as its
// governor: it stops when the remaining requests of the installation fall
// under the floor, and treats missing headers as the floor hit.
type githubTestsRateObserver struct {
	remaining, limit int64
	seen             bool
}

func (observer *githubTestsRateObserver) observe(headers http.Header) {
	rateLimit, present := parseRequestRateLimit(headers)
	if !present || rateLimit.Remaining == nil || rateLimit.Limit == nil || *rateLimit.Limit <= 0 {
		return
	}
	observer.remaining, observer.limit, observer.seen = *rateLimit.Remaining, *rateLimit.Limit, true
}

// githubTestsRateFloorMin is the least number of requests the open phase leaves
// to the normal sync; the floor is the larger of it and a fifth of the limit.
const githubTestsRateFloorMin = 500

func (observer *githubTestsRateObserver) floor() int64 {
	if !observer.seen {
		return githubTestsRateFloorMin
	}
	return max(githubTestsRateFloorMin, observer.limit/5)
}

// belowFloor reports whether the open phase must stop, and the remaining count
// (-1 when no header was seen: fail closed).
func (observer *githubTestsRateObserver) belowFloor() (bool, int64) {
	if !observer.seen {
		return true, -1
	}
	return observer.remaining < observer.floor(), observer.remaining
}
