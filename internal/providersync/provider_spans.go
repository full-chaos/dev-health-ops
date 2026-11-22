package providersync

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The spans of the GitHub tests artifact steps and of the shared chunked executor. Each starts only under a recording
// parent (never a new root) and carries counts and a fixed outcome word only: no artifact, run, owner or repository
// id, no URL and no error text. A routine end (a continuation, an artifact that is gone, oversized or unreadable, a
// contended budget, a cancellation) is an outcome word with no Error status; only a failure goes to EndWorkSpan.
const (
	providerSyncTracerName = "github.com/full-chaos/dev-health-ops/internal/providersync"

	artifactDownloadSpanName = "dev_health.github_tests.artifact_download"
	artifactParseSpanName    = "dev_health.github_tests.artifact_parse"
	chunkPrepareSpanName     = "dev_health.provider_unit.chunk_prepare"
	chunkCommitSpanName      = "dev_health.provider_unit.chunk_commit"

	spanAttrOutcome       = "outcome"
	spanAttrArtifactBytes = "dev_health.artifact.bytes"
	spanAttrListedBytes   = "dev_health.artifact.listed_bytes"
	spanAttrRequests      = "dev_health.provider.requests"
	spanAttrReports       = "dev_health.tests.reports"
	spanAttrSuites        = "dev_health.tests.suites"
	spanAttrCases         = "dev_health.tests.cases"
	spanAttrChunkCount    = "dev_health.chunk.count"
	spanAttrChunkPayload  = "dev_health.chunk.payload_bytes"
	spanAttrChunkEffects  = "dev_health.chunk.effects"
	spanOutcomeRead       = "read"
	spanOutcomeRangeRead  = "range_read"
	spanOutcomeNotFound   = "not_found"
	spanOutcomeOversized  = "oversized"
	spanOutcomeParsed     = "parsed"
	spanOutcomeUnreadable = "unreadable"
	spanOutcomePrepared   = "prepared"
	spanOutcomeCommitted  = "committed"
	spanOutcomeContinued  = "continued"
	spanOutcomeBudgetWait = "budget_contended"
	spanOutcomeCancelled  = "cancelled"
	spanOutcomeFailed     = "failed"
)

// endStepSpan sets attrs and the outcome and ends span; failure is non-nil only for a real failure.
func endStepSpan(span trace.Span, attrs []attribute.KeyValue, outcome string, failure error) {
	if span == nil {
		return
	}
	span.SetAttributes(append(attrs, attribute.String(spanAttrOutcome, outcome))...)
	tracing.EndWorkSpan(span, failure)
}

// stepOutcome maps an error every step shares to its word; ok is false for a failure the caller names. A cancellation
// counts when ctx is cancelled and the cause came back alone, as the transport's canceled reason, or wrapped next to the
// incomplete-read or chunk-store sentinel.
func stepOutcome(ctx context.Context, err error) (outcome string, ok bool) {
	switch {
	case errors.Is(err, providerfoundation.ErrBudgetContended):
		return spanOutcomeBudgetWait, true
	case providerfoundation.CallCancelled(ctx, err, ErrGitHubTestsIncomplete, ErrChunkCheckpointConflict):
		return spanOutcomeCancelled, true
	}
	return "", false
}

// startGitHubTestsDownloadSpan returns ctx with the download span as the parent of its request spans only: the span of
// ctx, which the download's log lines read, stays the caller's.
func startGitHubTestsDownloadSpan(ctx context.Context) (context.Context, trace.Span) {
	if ctx == nil || !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, nil
	}
	_, span := tracing.StartWorkSpan(ctx, providerSyncTracerName, artifactDownloadSpanName)
	return providerfoundation.WithRequestSpanParent(ctx, span), span
}

func endGitHubTestsDownloadSpan(ctx context.Context, span trace.Span, archiveBytes int, listedSize int64, requests int, notFound, rangeRead bool, err error) {
	if span == nil {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.Int(spanAttrArtifactBytes, archiveBytes),
		attribute.Int64(spanAttrListedBytes, listedSize),
		attribute.Int(spanAttrRequests, requests),
	}
	var oversized *githubTestsArtifactOversizedError
	outcome, routine := stepOutcome(ctx, err)
	switch {
	case routine:
		endStepSpan(span, attrs, outcome, nil)
	case err != nil && errors.As(err, &oversized):
		endStepSpan(span, attrs, spanOutcomeOversized, nil)
	case err != nil:
		endStepSpan(span, attrs, spanOutcomeFailed, err)
	case notFound:
		endStepSpan(span, attrs, spanOutcomeNotFound, nil)
	case rangeRead:
		endStepSpan(span, attrs, spanOutcomeRangeRead, nil)
	default:
		endStepSpan(span, attrs, spanOutcomeRead, nil)
	}
}

// parseGitHubTestsReportArtifactInSpan runs parse (parseGitHubTestsReportArtifact, which takes no context) inside the
// parse span of its caller's context and returns what parse returned.
func parseGitHubTestsReportArtifactInSpan(ctx context.Context, archive []byte, parse func() (githubTestsReportRows, error)) (githubTestsReportRows, error) {
	if ctx == nil || !trace.SpanFromContext(ctx).IsRecording() {
		return parse()
	}
	_, span := tracing.StartWorkSpan(ctx, providerSyncTracerName, artifactParseSpanName)
	rows, err := parse()
	attrs := []attribute.KeyValue{
		attribute.Int(spanAttrArtifactBytes, len(archive)),
		attribute.Int(spanAttrReports, rows.ReportsParsed),
		attribute.Int(spanAttrSuites, len(rows.Suites)),
		attribute.Int(spanAttrCases, len(rows.Cases)),
	}
	switch {
	case err == nil:
		endStepSpan(span, attrs, spanOutcomeParsed, nil)
	case errors.Is(err, ErrGitHubTestsArchiveUnreadable):
		endStepSpan(span, attrs, spanOutcomeUnreadable, nil)
	default:
		endStepSpan(span, attrs, spanOutcomeFailed, err)
	}
	return rows, err
}

func startChunkPrepareSpan(ctx context.Context) trace.Span {
	if ctx == nil || !trace.SpanFromContext(ctx).IsRecording() {
		return nil
	}
	_, span := tracing.StartWorkSpan(ctx, providerSyncTracerName, chunkPrepareSpanName)
	return span
}

func startChunkCommitSpan(ctx context.Context) trace.Span {
	if ctx == nil || !trace.SpanFromContext(ctx).IsRecording() {
		return nil
	}
	_, span := tracing.StartWorkSpan(ctx, providerSyncTracerName, chunkCommitSpanName)
	return span
}

func chunkSpanAttributes(group []PreparedProviderChunk) []attribute.KeyValue {
	payload, effects := 0, 0
	for _, chunk := range group {
		payload += chunk.PayloadBytes
		effects += len(chunk.Effects)
	}
	return []attribute.KeyValue{
		attribute.Int(spanAttrChunkCount, len(group)),
		attribute.Int(spanAttrChunkPayload, payload),
		attribute.Int(spanAttrChunkEffects, effects),
	}
}

func endChunkPrepareSpan(ctx context.Context, span trace.Span, group []PreparedProviderChunk, err error) {
	if span == nil {
		return
	}
	outcome, routine := stepOutcome(ctx, err)
	switch {
	case routine:
		endStepSpan(span, chunkSpanAttributes(group), outcome, nil)
	case err != nil:
		endStepSpan(span, chunkSpanAttributes(group), spanOutcomeFailed, err)
	default:
		endStepSpan(span, chunkSpanAttributes(group), spanOutcomePrepared, nil)
	}
}

// endChunkCommitSpan: committed are the chunks of the group this call confirmed committed (a mark that returned nil,
// or a lost acknowledgement whose read back shows the checkpoint past the chunk); fewer than the group at a
// continuation or a failure.
func endChunkCommitSpan(ctx context.Context, span trace.Span, committed []PreparedProviderChunk, err error) {
	if span == nil {
		return
	}
	outcome, routine := stepOutcome(ctx, err)
	switch {
	case routine:
		endStepSpan(span, chunkSpanAttributes(committed), outcome, nil)
	case errors.Is(err, ErrChunkContinuation):
		endStepSpan(span, chunkSpanAttributes(committed), spanOutcomeContinued, nil)
	case err != nil:
		endStepSpan(span, chunkSpanAttributes(committed), spanOutcomeFailed, err)
	default:
		endStepSpan(span, chunkSpanAttributes(committed), spanOutcomeCommitted, nil)
	}
}
