package providersync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/spantest"
)

const (
	spanDownloadName   = "dev_health.github_tests.artifact_download"
	spanParseName      = "dev_health.github_tests.artifact_parse"
	spanPrepareName    = "dev_health.provider_unit.chunk_prepare"
	spanCommitName     = "dev_health.provider_unit.chunk_commit"
	spanRequestName    = "dev_health.provider.request"
	spanSecret         = "SECRETVALUE"
	spanOwner          = "acme-owner"
	spanRepo           = "widget-repo"
	spanRoot           = "/repos/" + spanOwner + "/" + spanRepo
	spanBlobHost       = "blob.example"
	spanArtifactRead   = "777001"
	spanArtifactGone   = "777002"
	spanArtifactOver   = "777003"
	spanArtifactCapped = 4096
)

var (
	spanDownloadKeys     = []string{"dev_health.artifact.bytes", "dev_health.artifact.listed_bytes", "dev_health.provider.requests", "outcome"}
	spanDownloadOutcomes = []string{"read", "range_read", "not_found", "oversized", "failed", "budget_contended", "cancelled"}
	spanChunkKeys        = []string{"dev_health.chunk.count", "dev_health.chunk.effects", "dev_health.chunk.payload_bytes", "outcome"}
	spanPrepareOutcomes  = []string{"prepared", "failed", "cancelled"}
	spanCommitOutcomes   = []string{"committed", "continued", "failed", "cancelled"}
)

type spanDoer func(*http.Request) (*http.Response, error)

func (doer spanDoer) Do(request *http.Request) (*http.Response, error) { return doer(request) }

func spanInt(t *testing.T, pair spantest.Pair, key string) int64 {
	t.Helper()
	value, ok := spantest.Value(pair.Raw.Attributes(), key)
	if !ok {
		t.Fatalf("%s has no %s", pair.Raw.Name(), key)
	}
	return value.AsInt64()
}

func spanOutcome(t *testing.T, pair spantest.Pair, allowed []string) string {
	t.Helper()
	value, ok := spantest.Value(pair.Raw.Attributes(), "outcome")
	if !ok || !slices.Contains(allowed, value.AsString()) {
		t.Fatalf("%s outcome = %q, want one of %v", pair.Raw.Name(), value.AsString(), allowed)
	}
	return value.AsString()
}

// spanArtifactDoer answers the artifact zip route with a 302 to a signed blob URL (read, oversized) or 404 (gone).
func spanArtifactDoer(t *testing.T, archive []byte) spanDoer {
	return func(request *http.Request) (*http.Response, error) {
		path := request.URL.Path
		if request.URL.Host == spanBlobHost {
			if strings.Contains(path, spanArtifactOver) {
				return &http.Response{StatusCode: http.StatusOK, ContentLength: spanArtifactCapped + 10, Request: request,
					Body: io.NopCloser(strings.NewReader(strings.Repeat("x", spanArtifactCapped+10)))}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, ContentLength: int64(len(archive)), Request: request,
				Body: io.NopCloser(strings.NewReader(string(archive)))}, nil
		}
		for _, id := range []string{spanArtifactRead, spanArtifactOver} {
			if path == spanRoot+"/actions/artifacts/"+id+"/zip" {
				return &http.Response{StatusCode: http.StatusFound, Request: request, Body: io.NopCloser(strings.NewReader("")),
					Header: http.Header{"Location": {"https://" + spanBlobHost + "/containers/" + spanOwner + "/" + id + ".zip?sig=" + spanSecret + "&se=2026"}}}, nil
			}
		}
		if path == spanRoot+"/actions/artifacts/"+spanArtifactGone+"/zip" {
			return &http.Response{StatusCode: http.StatusNotFound, Request: request, Body: io.NopCloser(strings.NewReader(`{"message":"Not Found"}`))}, nil
		}
		t.Errorf("unexpected request to %s", request.URL.Path)
		return nil, errors.New("unexpected request")
	}
}

// C5: one download span over both wire requests and the body read: bytes = the archive length, requests = the wire count
// the function returns, the outcome per branch. The request spans are its children; raw and exported, no span names the
// signature, the blob host, the owner, the repository or the artifact.
func TestGitHubTestsArtifactDownloadSpanMeasuresEachBranch(t *testing.T) {
	archive := githubTestsZip(t, map[string]string{"junit.xml": githubTestsMultiSuiteJUnit(2)})
	cases := []struct {
		artifact string
		listed   int64
		outcome  string
		bytes    int64
		requests int
	}{
		{spanArtifactRead, int64(len(archive)), "read", int64(len(archive)), 2},
		{spanArtifactGone, -1, "not_found", 0, 1},
		{spanArtifactOver, -1, "oversized", 0, 2},
	}
	for _, tc := range cases {
		t.Run(tc.outcome, func(t *testing.T) {
			pipeline := spantest.Install(t)
			client := githubTestsClient(t, spanArtifactDoer(t, archive))
			body, used, notFound, err := downloadGitHubTestsListedArtifact(pipeline.Parent, client, spanRoot, tc.artifact, spanArtifactCapped, tc.listed)
			if (tc.outcome == "oversized") != (err != nil) || notFound != (tc.outcome == "not_found") || int64(len(body)) != tc.bytes || used != tc.requests {
				t.Fatalf("body=%d used=%d notFound=%v err=%v, want the %s branch", len(body), used, notFound, err, tc.outcome)
			}
			pairs := pipeline.Pairs(spanDownloadName)
			if len(pairs) != 1 {
				t.Fatalf("download spans = %d, want 1", len(pairs))
			}
			pair := pairs[0]
			forbidden := []string{spanSecret, "sig", spanBlobHost, spanOwner, spanRepo, tc.artifact, "/actions/", "containers"}
			spantest.Check(t, pair, spanDownloadKeys, forbidden...)
			if got := spanOutcome(t, pair, spanDownloadOutcomes); got != tc.outcome {
				t.Errorf("outcome = %q, want %q", got, tc.outcome)
			}
			if got := spanInt(t, pair, "dev_health.artifact.bytes"); got != tc.bytes {
				t.Errorf("bytes = %d, want %d", got, tc.bytes)
			}
			if got := spanInt(t, pair, "dev_health.artifact.listed_bytes"); got != tc.listed {
				t.Errorf("listed_bytes = %d, want %d", got, tc.listed)
			}
			if got := spanInt(t, pair, "dev_health.provider.requests"); got != int64(used) {
				t.Errorf("requests = %d, want the wire count %d", got, used)
			}
			requests := pipeline.Pairs(spanRequestName)
			if len(requests) != tc.requests {
				t.Fatalf("request spans = %d, want %d", len(requests), tc.requests)
			}
			for _, request := range requests {
				if request.Raw.Parent().SpanID() != pair.Raw.SpanContext().SpanID() {
					t.Errorf("request span parent = %s, want the download span", request.Raw.Parent().SpanID())
				}
				for _, word := range forbidden {
					if strings.Contains(spantest.RawText(request.Raw), word) || strings.Contains(spantest.ExportedText(request.Exported), word) {
						t.Errorf("request span carries %q", word)
					}
				}
			}
		})
	}
}

// groupRecordingStore keeps every group PrepareChunkGroup returns, in order.
type groupRecordingStore struct {
	*chunkMemoryStore
	groups [][]PreparedProviderChunk
}

func (s *groupRecordingStore) PrepareChunkGroup(ctx context.Context, claim Claim, chunks []PreparedProviderChunk, now, leaseNow time.Time) ([]PreparedProviderChunk, error) {
	group, err := s.chunkMemoryStore.PrepareChunkGroup(ctx, claim, chunks, now, leaseNow)
	if err == nil {
		s.groups = append(s.groups, group)
	}
	return group, err
}

func spanChunkTotals(group []PreparedProviderChunk) (count, payload, effects int64) {
	for _, chunk := range group {
		count++
		payload += int64(chunk.PayloadBytes)
		effects += int64(len(chunk.Effects))
	}
	return count, payload, effects
}

func spanChunkExecution(t *testing.T, ctx context.Context, maxChunks int) (*groupRecordingStore, error) {
	t.Helper()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := Descriptor("github", "cicd")
	if !ok {
		t.Fatal("descriptor missing")
	}
	descriptor.ChunkPolicy = ChunkPolicy{
		MaxSourceItems: 2, MaxEffectRows: 2, MaxPreparedBytes: 64 << 10,
		MaxChunksPerAttempt: maxChunks, MaxWallTime: time.Minute,
	}
	store := &groupRecordingStore{chunkMemoryStore: newChunkMemoryStore()}
	executor := completeRouteExecutor(now, &streamingChunkRouteHandler{batch: chunkedTestBatch(t, claim)}, store, &memoryEffectSink{})
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	_, err := executor.Execute(ctx, session, descriptor)
	return store, err
}

// C7: one prepare span and one commit span per prepared group in the shared executor; count, payload bytes and effects
// equal the group the store prepared.
func TestChunkPrepareAndCommitSpansEqualThePreparedGroup(t *testing.T) {
	pipeline := spantest.Install(t)
	store, err := spanChunkExecution(t, pipeline.Parent, 8)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	prepares, commits := pipeline.Pairs(spanPrepareName), pipeline.Pairs(spanCommitName)
	if len(store.groups) == 0 || len(prepares) != len(store.groups) || len(commits) != len(store.groups) {
		t.Fatalf("groups = %d prepare spans = %d commit spans = %d, want one of each per group", len(store.groups), len(prepares), len(commits))
	}
	for index, group := range store.groups {
		count, payload, effects := spanChunkTotals(group)
		for _, pair := range []spantest.Pair{prepares[index], commits[index]} {
			spantest.Check(t, pair, spanChunkKeys, "fixture-token", "ci_pipeline_runs")
			for key, want := range map[string]int64{"dev_health.chunk.count": count, "dev_health.chunk.payload_bytes": payload, "dev_health.chunk.effects": effects} {
				if got := spanInt(t, pair, key); got != want {
					t.Errorf("%s group %d %s = %d, want %d", pair.Raw.Name(), index, key, got, want)
				}
			}
		}
		if got := spanOutcome(t, prepares[index], spanPrepareOutcomes); got != "prepared" {
			t.Errorf("prepare outcome = %q, want prepared", got)
		}
		if got := spanOutcome(t, commits[index], spanCommitOutcomes); got != "committed" {
			t.Errorf("commit outcome = %q, want committed", got)
		}
	}
}

// A chunk continuation is the attempt bound, not a failure: the commit span says continued, counts the chunks it
// committed and has no Error status.
func TestChunkCommitSpanOfAContinuationIsNotAnError(t *testing.T) {
	pipeline := spantest.Install(t)
	store, err := spanChunkExecution(t, pipeline.Parent, 1)
	if !errors.Is(err, ErrChunkContinuation) {
		t.Fatalf("execute err = %v, want a continuation", err)
	}
	commits := pipeline.Pairs(spanCommitName)
	if len(commits) != 1 || len(store.groups) != 1 {
		t.Fatalf("commit spans = %d groups = %d, want 1 and 1", len(commits), len(store.groups))
	}
	pair := commits[0]
	spantest.Check(t, pair, spanChunkKeys)
	if got := spanOutcome(t, pair, spanCommitOutcomes); got != "continued" {
		t.Errorf("outcome = %q, want continued", got)
	}
	if pair.Raw.Status().Code == codes.Error || len(pair.Raw.Events()) != 0 {
		t.Errorf("status = %v events = %d, want no Error status and no event", pair.Raw.Status().Code, len(pair.Raw.Events()))
	}
	if got, want := spanInt(t, pair, "dev_health.chunk.count"), int64(1); got != want {
		t.Errorf("count = %d, want %d (the chunks committed before the bound)", got, want)
	}
}
