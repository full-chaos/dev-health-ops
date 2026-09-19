//go:build integration

package providersync

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// TestRequestUsageRowsLandOnceInClickHouse drives real executions against the
// migrated provider_request_usage table: a success, a failed retry of the same
// unit and a re-sent duplicate row, then reads the admin-analytics sums back.
func TestRequestUsageRowsLandOnceInClickHouse(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSession(t, now, false)
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")
	sink := ClickHouseRequestUsageSink{Conn: conn}
	opens := 0
	lazy := &LazyClickHouseRequestUsageSink{Open: func(context.Context) (driver.Conn, error) {
		opens++
		if opens == 1 {
			return nil, errScriptedTransport
		}
		return conn, nil
	}}
	writer := NewRequestUsageWriter(lazy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	writer.Start()

	limited := http.Header{}
	limited.Set("X-RateLimit-Remaining", "7")
	limited.Set("X-RateLimit-Limit", "10")
	limited.Set("Retry-After", "30")
	failing := completeRouteExecutor(now, &spendingCompleteRouteHandler{
		calls: 3, err: errScriptedTransport,
	}, &memoryEffectLedger{}, &memoryEffectSink{})
	failing.Doer = &scriptedDoer{replies: []scriptedReply{
		{status: 200}, {status: 429, headers: limited}, {err: errScriptedTransport},
	}}
	failing.RequestUsage = writer
	if _, err := failing.Execute(ctx, session, descriptor); err == nil {
		t.Fatal("failing execution returned no error")
	}

	succeeding := completeRouteExecutor(now, &spendingCompleteRouteHandler{
		calls: 2, batch: completeRouteFixture(t, claim),
	}, &memoryEffectLedger{}, &memoryEffectSink{})
	succeeding.Doer = &scriptedDoer{}
	succeeding.RequestUsage = writer
	result, err := succeeding.Execute(ctx, session, descriptor)
	if err != nil || result.RequestUsage != (RequestUsageTotals{Requests: 2, Responses: 2}) {
		t.Fatalf("success err=%v usage=%+v", err, result.RequestUsage)
	}

	writer.Close(10 * time.Second)

	var stored []RequestUsageRow
	rows, err := conn.Query(ctx, `SELECT execution_id, flush_seq, transport, requests, responses,
status_429, latest_status, rate_limit_remaining, rate_limit_limit, retry_after, window_started_at
FROM provider_request_usage FINAL WHERE org_id = ? AND unit_id = ? ORDER BY requests`, claim.OrgID, claim.ID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var row RequestUsageRow
		if err := rows.Scan(&row.ExecutionID, &row.FlushSeq, &row.Transport, &row.Requests, &row.Responses,
			&row.Status429, &row.LatestStatus, &row.RateLimitRemaining, &row.RateLimitLimit,
			&row.RetryAfter, &row.WindowStartedAt); err != nil {
			t.Fatal(err)
		}
		stored = append(stored, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(stored) != 2 || stored[0].ExecutionID == stored[1].ExecutionID {
		t.Fatalf("rows=%+v", stored)
	}
	failed := stored[1]
	if failed.Requests != 3 || failed.Responses != 2 || failed.Status429 != 1 ||
		failed.LatestStatus == nil || *failed.LatestStatus != 429 ||
		failed.RateLimitRemaining == nil || *failed.RateLimitRemaining != 7 ||
		failed.RateLimitLimit == nil || *failed.RateLimitLimit != 10 || failed.RetryAfter != "30" {
		t.Fatalf("failed execution row=%+v", failed)
	}

	// A write retried after an ambiguous failure re-sends the identical row.
	duplicate := RequestUsageRow{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset,
		IntegrationID: claim.IntegrationID, SyncRunID: claim.SyncRunID, UnitID: claim.ID,
		ExecutionID: stored[0].ExecutionID, Attempt: uint32(claim.Attempt), FlushSeq: stored[0].FlushSeq,
		Transport: stored[0].Transport, Requests: stored[0].Requests, Responses: stored[0].Responses,
		Status2xx: stored[0].Requests, LatestStatus: stored[0].LatestStatus,
		WindowStartedAt: stored[0].WindowStartedAt, RecordedAt: now,
	}
	if err := sink.WriteRequestUsage(ctx, []RequestUsageRow{duplicate}); err != nil {
		t.Fatal(err)
	}

	sums := func(query string, args ...any) (uint64, uint64) {
		var requests, responses uint64
		if err := conn.QueryRow(ctx, query, args...).Scan(&requests, &responses); err != nil {
			t.Fatal(err)
		}
		return requests, responses
	}
	if requests, responses := sums(`SELECT sum(requests), sum(responses) FROM provider_request_usage FINAL
WHERE org_id = ? AND unit_id = ?`, claim.OrgID, claim.ID); requests != 5 || responses != 4 {
		t.Fatalf("per unit requests=%d responses=%d", requests, responses)
	}
	if requests, _ := sums(`SELECT sum(requests), sum(responses) FROM provider_request_usage FINAL
WHERE org_id = ? AND sync_run_id = ?`, claim.OrgID, claim.SyncRunID); requests != 5 {
		t.Fatalf("per run requests=%d", requests)
	}
	if requests, _ := sums(`SELECT sum(requests), sum(responses) FROM provider_request_usage FINAL
WHERE org_id = ? AND provider = ? AND toDate(window_started_at) = toDate(?)`, claim.OrgID, claim.Provider, now); requests != 5 {
		t.Fatalf("per org/provider/day requests=%d", requests)
	}
	if requests, _ := sums(`SELECT sum(requests), sum(responses) FROM provider_request_usage FINAL
WHERE org_id = ?`, "another-org"); requests != 0 {
		t.Fatalf("another org sees requests=%d", requests)
	}
}
