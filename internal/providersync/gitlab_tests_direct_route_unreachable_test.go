package providersync

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-14067: GitLabTestsRouteHandler.Collect (the direct route) accepts a report member the archive parse dropped at a
// size guard without a trace; only CollectChunks records it as report_member_dropped. The direct route is unreachable in
// production (Descriptor marks gitlab cicd/tests Chunked and the executor streams a ChunkedCompleteRouteHandler). These
// cells fail if production can reach the direct route while it still drops that loss silently.

func gitLabTestsDispatchTruncated(result map[string]any) []string {
	switch raw := result["inventory_truncated"].(type) {
	case []string:
		return raw
	case []any:
		out := make([]string, 0, len(raw))
		for _, item := range raw {
			if text, ok := item.(string); ok {
				out = append(out, text)
			}
		}
		return out
	}
	return nil
}

func TestGitLabTestsProductionDispatchRecordsGuardDroppedMembers(t *testing.T) {
	for cause, archive := range silentSkipArchives(t) {
		t.Run(cause, func(t *testing.T) {
			now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
			claim, session := completeRouteSessionFor(t, now, false, "gitlab", "cicd")
			pipelines := gitLabAllRefPipelines()[1:2]
			pipelines[0].ArtifactJobs = []gitLabAllRefArtifactJob{{ID: "21"}}
			doer := &gitLabAllRefDoer{t: t, archive: archive, pipelines: pipelines}
			executor := completeRouteExecutor(now, GitLabTestsRouteHandler{}, newChunkMemoryStore(), &memoryEffectSink{})
			executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "gitlab"}
			executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
			executor.Doer = fakehttp.Client(doer)
			executor.BudgetLimits = map[CostClass]int{claim.CostClass: 10}
			executor.Metrics = providerfoundation.NewMetrics()
			descriptor, ok := Descriptor("gitlab", "cicd")
			if !ok {
				t.Fatal("gitlab/cicd has no descriptor")
			}
			var (
				result CompleteRouteExecutionResult
				err    error
			)
			for attempt := 0; attempt < 8; attempt++ {
				result, err = executor.Execute(context.Background(), session, descriptor)
				if !errors.Is(err, ErrChunkContinuation) {
					break
				}
			}
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			if !slices.Contains(doer.requests, "artifact 21") {
				t.Fatalf("premise: requests %v, the artifact was not read", doer.requests)
			}
			if !slices.Contains(gitLabTestsDispatchTruncated(result.Result), gitLabReportMemberDroppedComponent) {
				t.Fatalf("CHAOS-14067: the production dispatch of gitlab/cicd accepted a %s member drop without %s "+
					"(the direct GitLabTestsRouteHandler.Collect drops it silently; record it there before production can reach it); result=%v",
					cause, gitLabReportMemberDroppedComponent, result.Result)
			}
		})
	}
}

func TestGitLabTestsRouteSelectionKeepsTheStreamingHandler(t *testing.T) {
	for _, dataset := range []string{"cicd", "tests"} {
		route, ok := SelectGitFamilyRoute("gitlab", dataset, GitFamilyDeps{})
		if !ok {
			t.Fatalf("gitlab/%s is not a git-family route", dataset)
		}
		if _, streaming := route.Handler.(ChunkedCompleteRouteHandler); !streaming {
			t.Fatalf("CHAOS-14067: gitlab/%s handler %T is not a ChunkedCompleteRouteHandler, so the executor would call its "+
				"direct Collect, which drops guard-skipped report members silently", dataset, route.Handler)
		}
	}
}
