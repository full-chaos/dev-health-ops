package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const githubTestsMaxJobsPerRun = 500

// githubTestsChunkCursor is deliberately route-owned. A provider Link URL is
// opaque, while the run index makes a crash between two runs on one page
// resumable without replaying the already committed prefix.
type githubTestsChunkCursor struct {
	Phase      string                  `json:"phase"`
	NextURL    string                  `json:"next_url,omitempty"`
	Index      int                     `json:"index,omitempty"`
	Pipelines  int                     `json:"pipelines"`
	Jobs       int                     `json:"jobs"`
	Acceptance int                     `json:"acceptance"`
	Suites     int                     `json:"suites"`
	Cases      int                     `json:"cases"`
	Coverage   int                     `json:"coverage"`
	Requests   int                     `json:"requests"`
	Pages      int                     `json:"pages"`
	Incomplete []GitHubTestsIncomplete `json:"incomplete,omitempty"`
}

func decodeGitHubTestsChunkCursor(raw string) (githubTestsChunkCursor, error) {
	if strings.TrimSpace(raw) == "" {
		return githubTestsChunkCursor{Phase: "runs"}, nil
	}
	var cursor githubTestsChunkCursor
	if json.Unmarshal([]byte(raw), &cursor) != nil ||
		(cursor.Phase != "runs" && cursor.Phase != "artifacts") ||
		cursor.Index < 0 || cursor.NextURL == "" && cursor.Index != 0 {
		return githubTestsChunkCursor{}, ErrChunkCheckpointConflict
	}
	return cursor, nil
}

func encodeGitHubTestsChunkCursor(cursor githubTestsChunkCursor) (string, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil || len(encoded) > maxChunkCursorBytes {
		return "", ErrChunkCheckpointConflict
	}
	return string(encoded), nil
}

// CollectChunks streams GitHub TestOps pages. It mirrors Collect's fetch and
// normalization rules, but emits one bounded run/report unit at a time and
// persists an opaque page URL plus item index after every emission.
func (handler GitHubTestsRouteHandler) CollectChunks(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
	resumeCursor string,
	emit func(ChunkRouteEmission) error,
) error {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		(claim.Dataset != "tests" && claim.Dataset != "cicd") || client == nil ||
		client.Provider != "github" || client.BaseURL == nil || normalizedAt.IsZero() || emit == nil {
		return ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	owner, repository, err := splitGitHubRepository(claim.SourceExternalID)
	if err != nil {
		return err
	}
	root := providerRelativePath(client, "repos", owner, repository)
	var repo gitHubRepositoryPayload
	if err := fetchObject(ctx, client, root, &repo); err != nil {
		return err
	}
	repoID, err := repositoryIdentity(repo.FullName)
	if err != nil {
		return err
	}
	maxRuns := handler.MaxRuns
	if maxRuns == 0 {
		maxRuns = githubTestsMaxRuns
	}
	maxArtifacts := handler.MaxArtifactsPerRun
	if maxArtifacts == 0 {
		maxArtifacts = githubTestsMaxArtifacts
	}
	jobPages := handler.MaxJobPages
	if jobPages == 0 {
		jobPages = nativeMaxPages
	}
	if maxRuns < 1 || maxRuns > githubTestsMaxRuns || maxArtifacts < 1 || maxArtifacts > githubTestsMaxArtifacts || jobPages < 1 || jobPages > nativeMaxPages {
		return ErrInvalidConfiguration
	}
	cursor, err := decodeGitHubTestsChunkCursor(resumeCursor)
	if err != nil {
		return err
	}
	if cursor.Requests == 0 {
		cursor.Requests = 1 // repository lookup above
	}
	policyCache := map[string]githubTestsPolicy{}
	emitCursor := func(before, after githubTestsChunkCursor, batch CompleteRouteBatch, final bool) error {
		beforeRaw, beforeErr := encodeGitHubTestsChunkCursor(before)
		if beforeErr != nil {
			return beforeErr
		}
		afterRaw, afterErr := encodeGitHubTestsChunkCursor(after)
		if afterErr != nil {
			return afterErr
		}
		return emit(ChunkRouteEmission{Batch: batch, CursorBefore: beforeRaw, CursorAfter: afterRaw, Final: final})
	}
	emitRunPage := func(page providerfoundation.PageVisit) error {
		cursor.Pages++
		start := 0
		if cursor.NextURL == page.CursorBefore {
			start = cursor.Index
		}
		if start > len(page.Items) {
			return ErrChunkCheckpointConflict
		}
		for index := start; index < len(page.Items); index++ {
			before := cursor
			var run gitHubWorkflowRunPayload
			decoder := json.NewDecoder(strings.NewReader(string(page.Items[index])))
			decoder.UseNumber()
			if decoder.Decode(&run) != nil {
				return providerfoundation.ErrNormalizationInvalid
			}
			pipeline, include := normalizeGitHubTestsPipeline(claim, repoID, run, normalizedAt)
			jobs := make([]githubTestsJobRow, 0)
			acceptance := make([]githubTestsAcceptanceRow, 0)
			if include && !ciPipelineRunOutsideWindow(pipeline.StartedAt, claim) {
				jobPage, pageErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
					Path:  root + "/actions/runs/" + url.PathEscape(pipeline.RunID) + "/jobs",
					Query: url.Values{"per_page": {"100"}}, DataKey: "jobs", MaxPages: jobPages,
					MaxItems: githubTestsMaxJobsPerRun + 1,
				})
				if pageErr != nil {
					return pageErr
				}
				cursor.Requests += jobPage.Pages
				cursor.Pages += jobPage.Pages
				if jobPage.CapReached || len(jobPage.Items) > githubTestsMaxJobsPerRun {
					return ErrPaginationCapExceeded
				}
				for _, jobRaw := range jobPage.Items {
					var job githubTestsJobPayload
					decoder := json.NewDecoder(strings.NewReader(string(jobRaw)))
					decoder.UseNumber()
					if decoder.Decode(&job) != nil {
						return providerfoundation.ErrNormalizationInvalid
					}
					row, ok := normalizeGitHubTestsJob(claim, repoID, pipeline.RunID, pipeline.RetryCount, job, normalizedAt)
					if ok {
						jobs = append(jobs, row)
					}
				}
				targetBranch, prNumber := gitHubTestsTarget(run)
				policy := githubTestsPolicy{provenance: "github.branch_protection.target_branch_unavailable"}
				if targetBranch != nil {
					cached, ok := policyCache[*targetBranch]
					if !ok {
						cached, err = fetchGitHubTestsPolicy(ctx, client, root, *targetBranch)
						cursor.Requests++
						if err != nil {
							return err
						}
						policyCache[*targetBranch] = cached
					}
					policy = cached
				}
				acceptance = projectGitHubTestsChecks(claim, repoID, pipeline, jobs, policy, targetBranch, prNumber, testsOptionalString(stringValue(run.HTMLURL)), normalizedAt)
				cursor.Pipelines++
				cursor.Jobs += len(jobs)
				cursor.Acceptance += len(acceptance)
			}
			effects, effectErr := testOpsEffects(
				func() []githubTestsPipelineRow {
					if include && !ciPipelineRunOutsideWindow(pipeline.StartedAt, claim) {
						return []githubTestsPipelineRow{pipeline}
					}
					return nil
				}(),
				jobs, acceptance, nil, nil, nil,
			)
			if effectErr != nil {
				return effectErr
			}
			after := cursor
			after.Index = index + 1
			after.NextURL = page.CursorBefore
			if after.Index >= len(page.Items) {
				after.Index = 0
				after.NextURL = page.CursorAfter
				if after.NextURL == "" {
					after.Phase = "artifacts"
				}
			}
			if err := emitCursor(before, after, CompleteRouteBatch{Effects: effects}, false); err != nil {
				return err
			}
			cursor = after
		}
		if len(page.Items) == 0 {
			cursor.NextURL = page.CursorAfter
			cursor.Index = 0
			if cursor.NextURL == "" {
				cursor.Phase = "artifacts"
			}
		}
		return nil
	}

	if cursor.Phase == "runs" {
		query := url.Values{"per_page": {"100"}}
		if claim.SinceAt != nil || claim.BeforeAt != nil {
			start, end := "*", "*"
			if claim.SinceAt != nil {
				start = claim.SinceAt.UTC().Format(time.RFC3339)
			}
			if claim.BeforeAt != nil {
				end = claim.BeforeAt.UTC().Format(time.RFC3339)
			}
			query.Set("created", start+".."+end)
		}
		pageOptions := providerfoundation.GitHubPageOptions{Path: root + "/actions/runs", Query: query, DataKey: "workflow_runs", MaxPages: (maxRuns + nativePerPage - 1) / nativePerPage, InitialURL: cursor.NextURL}
		collection, visitErr := providerfoundation.VisitGitHubLinkPages(ctx, client, pageOptions, emitRunPage)
		if visitErr != nil {
			return visitErr
		}
		if collection.CapReached {
			return ErrPaginationCapExceeded
		}
	}

	if cursor.Phase != "artifacts" {
		cursor.Phase = "artifacts"
		cursor.NextURL, cursor.Index = "", 0
	}
	if cursor.Phase == "artifacts" {
		artifactQuery := url.Values{"per_page": {"100"}}
		if repo.DefaultBranch != "" {
			artifactQuery.Set("branch", repo.DefaultBranch)
		}
		if claim.SinceAt != nil {
			artifactQuery.Set("created", ">="+claim.SinceAt.UTC().Format(time.DateOnly))
		}
		artifactPage := func(page providerfoundation.PageVisit) error {
			cursor.Pages++
			start := 0
			if cursor.NextURL == page.CursorBefore {
				start = cursor.Index
			}
			if start > len(page.Items) {
				return ErrChunkCheckpointConflict
			}
			for index := start; index < len(page.Items); index++ {
				before := cursor
				var run gitHubWorkflowRunPayload
				decoder := json.NewDecoder(strings.NewReader(string(page.Items[index])))
				decoder.UseNumber()
				if decoder.Decode(&run) != nil {
					return providerfoundation.ErrNormalizationInvalid
				}
				pipeline, include := normalizeGitHubTestsPipeline(claim, repoID, run, normalizedAt)
				suites := []testSuiteResultRow(nil)
				cases := []testCaseResultRow(nil)
				coverage := []coverageSnapshotRow(nil)
				if include && (claim.BeforeAt == nil || !pipeline.StartedAt.After(claim.BeforeAt.UTC())) {
					artPage, pageErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
						Path:  root + "/actions/runs/" + url.PathEscape(pipeline.RunID) + "/artifacts",
						Query: url.Values{"per_page": {"100"}}, DataKey: "artifacts", MaxPages: 1,
					})
					if pageErr != nil {
						return pageErr
					}
					cursor.Requests += artPage.Pages
					cursor.Pages += artPage.Pages
					if artPage.CapReached || len(artPage.Items) > maxArtifacts {
						return ErrPaginationCapExceeded
					}
					for _, artifactRaw := range artPage.Items {
						var artifact githubTestsArtifactPayload
						decoder := json.NewDecoder(strings.NewReader(string(artifactRaw)))
						decoder.UseNumber()
						if decoder.Decode(&artifact) != nil || artifact.ID == "" {
							return providerfoundation.ErrNormalizationInvalid
						}
						if artifact.Expired {
							continue
						}
						archive, used, downloadErr := downloadGitHubTestsArtifact(ctx, client, root, string(artifact.ID))
						cursor.Requests += used
						if downloadErr != nil {
							return downloadErr
						}
						if len(archive) == 0 {
							continue
						}
						rows, parseErr := parseGitHubTestsArtifact(archive, repoID, pipeline.RunID, claim.OrgID, pipeline.StartedAtPtr(), pipeline.FinishedAt, normalizedAt)
						if parseErr != nil {
							return fmt.Errorf("%w: reports skipped=%d: %v", ErrGitHubTestsIncomplete, rows.Skipped, parseErr)
						}
						reportIncomplete, optional := rows.optionalIncomplete()
						if !optional {
							return fmt.Errorf("%w: reports skipped=%d: unsafe archive bounds", ErrGitHubTestsIncomplete, rows.Skipped)
						}
						for _, observation := range reportIncomplete {
							cursor.Incomplete = mergeGitHubTestsIncomplete(cursor.Incomplete, observation)
						}
						suites = append(suites, rows.Suites...)
						cases = append(cases, rows.Cases...)
						coverage = append(coverage, rows.Coverage...)
						if len(suites)+len(cases)+len(coverage) > githubTestsMaxJobsPerRun {
							return ErrPaginationCapExceeded
						}
					}
				}
				cursor.Suites += len(suites)
				cursor.Cases += len(cases)
				cursor.Coverage += len(coverage)
				effects, effectErr := testOpsEffects(nil, nil, nil, suites, cases, coverage)
				if effectErr != nil {
					return effectErr
				}
				after := cursor
				after.Index = index + 1
				after.NextURL = page.CursorBefore
				if after.Index >= len(page.Items) {
					after.Index = 0
					after.NextURL = page.CursorAfter
				}
				if err := emitCursor(before, after, CompleteRouteBatch{Effects: effects}, false); err != nil {
					return err
				}
				cursor = after
			}
			if len(page.Items) == 0 {
				cursor.NextURL = page.CursorAfter
				cursor.Index = 0
			}
			return nil
		}
		collection, visitErr := providerfoundation.VisitGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{Path: root + "/actions/runs", Query: artifactQuery, DataKey: "workflow_runs", MaxPages: (maxRuns + nativePerPage - 1) / nativePerPage, InitialURL: cursor.NextURL}, artifactPage)
		if visitErr != nil {
			return visitErr
		}
		if collection.CapReached {
			return ErrPaginationCapExceeded
		}
	}

	result := map[string]any{
		"pipeline_runs_synced": cursor.Pipelines, "job_runs_synced": cursor.Jobs,
		"acceptance_checks_synced": cursor.Acceptance, "test_suites_synced": cursor.Suites,
		"test_cases_synced": cursor.Cases, "coverage_snapshots_synced": cursor.Coverage,
		"repo": repo.FullName, "reports_complete": len(cursor.Incomplete) == 0,
		"reports_skipped": githubTestsIncompleteCount(cursor.Incomplete), "incomplete": cursor.Incomplete,
	}
	effects, effectErr := testOpsEffects(nil, nil, nil, nil, nil, nil)
	if effectErr != nil {
		return effectErr
	}
	return emitCursor(cursor, cursor, CompleteRouteBatch{
		Effects: effects, Result: result,
		Watermark: func() *time.Time {
			if len(cursor.Incomplete) > 0 {
				return nil
			}
			return claim.BeforeAt
		}(),
		Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Requests: cursor.Requests, Pages: cursor.Pages, Records: cursor.Pipelines + cursor.Jobs + cursor.Acceptance + cursor.Suites + cursor.Cases + cursor.Coverage},
	}, true)
}

var _ ChunkedCompleteRouteHandler = GitHubTestsRouteHandler{}
