package providersync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitLab retries a job inside the same pipeline: the retried job keeps its name, gets a higher id and the pipeline test_report
// shows only the latest job of each name. The attempt of a report slot is the retry rank of its job(s) among the listed jobs of
// that name (CHAOS-13875). Attempt 1 keeps the keys rows always had; attempt >= 2 folds in a digest, so a retry writes its own
// rows next to the earlier attempt's and the readers count the newest attempt of each slot once.

const gitLabTestsAttemptKeyTag = "gitlab-report-attempt"

// gitLabTestsRunJobs is the ranking of one pipeline's listed jobs (listed with include_retried).
type gitLabTestsRunJobs struct {
	rank       map[string]int64
	nameRank   map[string]int64
	anyRetried bool
	// idsByName holds the listed job ids of each name, ascending; nameOf is a job id's name.
	idsByName map[string][]string
	nameOf    map[string]string
	// retried counts the listed jobs GitLab marks retried: one per job retry of the pipeline.
	retried int
}

func gitLabTestsJobRetried(job gitLabTestsJobPayload) bool {
	value, ok := job.Retried.(bool)
	return ok && value
}

func gitLabTestsJobIDLess(left, right string) bool {
	leftNumber, leftErr := strconv.ParseInt(left, 10, 64)
	rightNumber, rightErr := strconv.ParseInt(right, 10, 64)
	if leftErr == nil && rightErr == nil {
		return leftNumber < rightNumber
	}
	return left < right
}

// newGitLabTestsRunJobs ranks the jobs of a name by ascending id (ids rise with creation, so a retry ranks higher). A pipeline
// without a retried job ranks every job 1. A list cut short can only lower a rank, never raise it.
func newGitLabTestsRunJobs(items []json.RawMessage) (gitLabTestsRunJobs, error) {
	result := gitLabTestsRunJobs{rank: map[string]int64{}, nameRank: map[string]int64{}, nameOf: map[string]string{}}
	idsByName := map[string][]string{}
	for _, raw := range items {
		job, err := decodeGitLabTestsJob(raw)
		if err != nil {
			return gitLabTestsRunJobs{}, providerfoundation.RouteFailure(gitlabPipelineJobsRoute, providerfoundation.ReasonBodyDecode, err)
		}
		id := stringValue(job.ID)
		if id == "" {
			continue
		}
		if gitLabTestsJobRetried(job) {
			result.anyRetried = true
			result.retried++
		}
		name := stringValue(job.Name)
		idsByName[name] = append(idsByName[name], id)
		result.nameOf[id] = name
	}
	for name, ids := range idsByName {
		sort.Slice(ids, func(i, j int) bool { return gitLabTestsJobIDLess(ids[i], ids[j]) })
		for index, id := range ids {
			rank := int64(index + 1)
			if !result.anyRetried {
				rank = 1
			}
			result.rank[id] = rank
			if rank > result.nameRank[name] {
				result.nameRank[name] = rank
			}
		}
	}
	result.idsByName = idsByName
	return result, nil
}

// runAttempt is the attempt of the pipeline's report-completeness row (CHAOS-13981): 1 + the number of retried jobs. A pipeline
// has no attempt number and every job retry adds one retried job, so each retry is a new key; the same job set is the same key.
// The highest job rank would not do: a retry of a second job name leaves it unchanged.
func (jobs gitLabTestsRunJobs) runAttempt() int64 {
	return 1 + int64(jobs.retried)
}

// gitLabTestsRunAttemptOf is runAttempt over a whole job listing, before a per-run cap cuts it.
func gitLabTestsRunAttemptOf(items []json.RawMessage) (int64, error) {
	jobs, err := newGitLabTestsRunJobs(items)
	if err != nil {
		return 0, err
	}
	return jobs.runAttempt(), nil
}

// earlierJobIDs are the listed ids of the job's name that rank below it: the jobs it retried, oldest first.
func (jobs gitLabTestsRunJobs) earlierJobIDs(jobID string) []string {
	name, ok := jobs.nameOf[jobID]
	if !ok {
		return nil
	}
	var out []string
	for _, id := range jobs.idsByName[name] {
		if id == jobID {
			return out
		}
		out = append(out, id)
	}
	return nil
}

// jobAttempt is the attempt of the report of one job id (its coverage); an unlisted job is attempt 1.
func (jobs gitLabTestsRunJobs) jobAttempt(jobID string) int64 {
	if rank := jobs.rank[jobID]; rank > 1 {
		return rank
	}
	return 1
}

// suiteAttempt is the attempt of a native test_report suite: the highest rank over the listed jobs behind it (build_ids of the
// test_report_summary suite of that name), else the rank of the job of that name, else 1.
func (jobs gitLabTestsRunJobs) suiteAttempt(suiteName string, summary map[string][]string) int64 {
	best := int64(0)
	for _, id := range summary[suiteName] {
		if rank := jobs.rank[id]; rank > best {
			best = rank
		}
	}
	if best == 0 {
		best = jobs.nameRank[suiteName]
	}
	if best < 1 {
		return 1
	}
	return best
}

func gitLabTestsAttemptKey(legacy string, attempt int64) string {
	if attempt < 2 {
		return legacy
	}
	return hashTestIdentifier(gitLabTestsAttemptKeyTag, legacy, strconv.FormatInt(attempt, 10))
}

type gitLabTestsSummaryPayload struct {
	Suites []struct {
		Name     any   `json:"name"`
		BuildIDs []any `json:"build_ids"`
	} `json:"test_suites"`
}

// fetchGitLabTestsReportSummary reads suite name -> the job ids behind the suite. The full test_report carries no build_ids;
// only the summary route does (GitLab TestSuiteSummaryEntity).
func fetchGitLabTestsReportSummary(ctx context.Context, client *providerfoundation.HTTPClient, root, runID string) (map[string][]string, error) {
	response, err := client.DoRoute(ctx, http.MethodGet, gitlabPipelinesRoute+"/{pipeline}/test_report_summary", root+"/pipelines/"+url.PathEscape(runID)+"/test_report_summary", nil)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	raw, err := providerfoundation.ReadResponseBody(response, nativeMaxObjectBytes)
	if err != nil {
		return nil, err
	}
	if len(raw) > nativeMaxObjectBytes {
		return nil, providerfoundation.ResponseInvalid(response, providerfoundation.ReasonBodyTooLarge)
	}
	var payload gitLabTestsSummaryPayload
	if err := providerfoundation.DecodeResponseJSON(response, raw, &payload, providerfoundation.DecodeUseNumber); err != nil {
		return nil, err
	}
	result := make(map[string][]string, len(payload.Suites))
	for _, suite := range payload.Suites {
		name := stringValue(suite.Name)
		for _, id := range suite.BuildIDs {
			if text := stringValue(id); text != "" {
				result[name] = append(result[name], text)
			}
		}
	}
	return result, nil
}

// parseGitLabTestsJobArtifact parses one job's artifact; only its coverage is kept (the native test_report owns suites and cases).
// The coverage slot is the job name, the attempt the retry rank of the job. Attempt 1 keeps the job id as the key scope (the
// key rows always had); a retry (attempt >= 2) hashes the slot and attempt instead, and its rows list the pre-slot keys under its
// own job id and under each job of the name it retried (CHAOS-13925): the writer before CHAOS-13875 keyed every job it read so.
func parseGitLabTestsJobArtifact(
	archive []byte, job gitLabTestsJobPayload, runJobs gitLabTestsRunJobs,
	repoID, runID, orgID string, started, finished *time.Time, at time.Time,
) (githubTestsReportRows, error) {
	jobID := stringValue(job.ID)
	attempt := runJobs.jobAttempt(jobID)
	name := firstNonEmpty(stringValue(job.Name), "job")
	scope := jobID
	var legacyScopes []string
	if attempt >= 2 {
		scope = hashTestIdentifier("gitlab-report-slot", name, strconv.FormatInt(attempt, 10))
		legacyScopes = append([]string{jobID}, runJobs.earlierJobIDs(jobID)...)
	}
	rows, err := parseGitHubTestsArchive(archive, scope, scope, legacyScopes, repoID, runID, orgID, started, finished, at)
	// The two size-guard skips are GitHub report_member causes (CHAOS-14030): GitLab treats any other skip as fatal, so it takes
	// them out here and records them in its own terms (gitLabReportMemberComponent) from GuardDropped.
	rows = rows.withoutMemberGuardSkips()
	for index := range rows.Coverage {
		rows.Coverage[index].ReportSlot, rows.Coverage[index].RunAttempt = name, attempt
	}
	return rows, err
}

// gitLabTestsSummaryFallback decides whether a failed test_report_summary request may be answered with the name join. The summary
// only refines the attempt of a suite, so a missing, failing or unreadable summary never refuses the run; a cancelled context,
// an authentication failure, a rate limit or a lease loss still does.
func gitLabTestsSummaryFallback(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if gitLabErrorTreeOnlyProviderClasses(err, providerfoundation.ErrorNotFound, providerfoundation.ErrorTransient, providerfoundation.ErrorPermanent) {
		return nil
	}
	return err
}
