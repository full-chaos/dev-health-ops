package jobcontract

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGoldenFixturesCrossDecodeAndReencode(t *testing.T) {
	t.Parallel()
	root := contractRoot(t)
	tests := []struct {
		kind    string
		fixture string
		payload any
	}{
		{KindHeartbeat, "examples/system.heartbeat.v1.json", HeartbeatPayload{ScheduledFor: "2026-07-21T12:00:00Z"}},
		{KindRetentionCleanup, "examples/system.retention_cleanup.v1.json", RetentionCleanupPayload{BatchSize: 250, DeleteBefore: "2026-07-14T12:00:00Z", RetentionPolicy: RetentionWorkerTerminal}},
		{KindReportExecuteOnDemand, "examples/report.execute_on_demand.v1.json", OnDemandReportExecutionPayload{ReportID: "00000000-0000-4000-8000-000000000002"}},
		{KindReportExecuteScheduled, "examples/report.execute_scheduled.v1.json", ScheduledReportExecutionPayload{ReportID: "00000000-0000-4000-8000-000000000004"}},
		{KindDailyMetricsDispatch, "examples/metrics.daily_dispatch.v1.json", DailyMetricsDispatchPayload{RunID: "00000000-0000-4000-8000-000000000001"}},
		{KindDailyMetricsPartition, "examples/metrics.daily_partition.v1.json", DailyMetricsPartitionPayload{PartitionID: "00000000-0000-4000-8000-000000000002"}},
		{KindDailyMetricsFinalize, "examples/metrics.daily_finalize.v1.json", DailyMetricsFinalizePayload{RunID: "00000000-0000-4000-8000-000000000001"}},
		{KindWorkGraphBuild, "examples/workgraph.build.v1.json", WorkGraphBuildPayload{RequestID: "00000000-0000-4000-8000-000000000101"}},
		{KindInvestmentMaterialize, "examples/investment.materialize.v1.json", InvestmentMaterializePayload{RequestID: "00000000-0000-4000-8000-000000000102"}},
		{KindInvestmentDispatch, "examples/investment.dispatch.v1.json", InvestmentDispatchPayload{RequestID: "00000000-0000-4000-8000-000000000103"}},
		{KindInvestmentChunk, "examples/investment.chunk.v1.json", InvestmentChunkPayload{ChunkID: "00000000-0000-4000-8000-000000000104"}},
		{KindInvestmentFinalize, "examples/investment.finalize.v1.json", InvestmentFinalizePayload{RunID: "00000000-0000-4000-8000-000000000105"}},
	}
	for _, kind := range []string{
		KindRemainingCapacity,
		KindRemainingComplexity,
		KindRemainingDORA,
		KindRemainingExtraMetrics,
		KindRemainingMembership,
		KindRemainingRecommendations,
		KindRemainingReleaseImpact,
		KindRemainingTeamMetrics,
	} {
		tests = append(tests, struct {
			kind    string
			fixture string
			payload any
		}{
			kind:    kind,
			fixture: "examples/metrics.remaining_partition.v1.json",
			payload: NewRemainingMetricsPartitionPayload(kind, "00000000-0000-4000-8000-000000000102"),
		})
	}
	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			t.Parallel()
			data, err := os.ReadFile(filepath.Join(root, test.fixture))
			if err != nil {
				t.Fatal(err)
			}
			envelope, err := Decode(test.kind, data)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if envelope.Payload != test.payload {
				t.Fatalf("payload = %#v, want %#v", envelope.Payload, test.payload)
			}
			canonical, err := MarshalCanonical(envelope)
			if err != nil {
				t.Fatalf("MarshalCanonical() error = %v", err)
			}
			if !bytes.Equal(canonical, data) {
				t.Fatalf("canonical fixture drift\ngot:  %s\nwant: %s", canonical, data)
			}
		})
	}
}

func TestDecodeRejectsUnsafeOrDriftedEnvelope(t *testing.T) {
	t.Parallel()
	valid := `{"contract_version":1,"correlation_id":"job-1","idempotency_key":"heartbeat:1","domain":{"type":"schedule_occurrence","id":"00000000-0000-4000-8000-000000000001"},"payload":{"scheduled_for":"2026-07-21T12:00:00Z"}}`
	tests := map[string]string{
		"unknown envelope field": strings.Replace(valid, `"payload":`, `"credential":"not-allowed","payload":`, 1),
		"unknown payload field":  strings.Replace(valid, `"scheduled_for":"2026-07-21T12:00:00Z"`, `"scheduled_for":"2026-07-21T12:00:00Z","extra":true`, 1),
		"unknown version":        strings.Replace(valid, `"contract_version":1`, `"contract_version":2`, 1),
		"duplicate key":          strings.Replace(valid, `"contract_version":1`, `"contract_version":1,"contract_version":1`, 1),
		"multiple values":        valid + `{}`,
		"non UTC timestamp":      strings.Replace(valid, `2026-07-21T12:00:00Z`, `2026-07-21T12:00:00-07:00`, 1),
		"tenant on global job":   strings.Replace(valid, `"correlation_id":`, `"organization_id":"00000000-0000-4000-8000-000000000009","correlation_id":`, 1),
		"unsafe identifier":      strings.Replace(valid, `"job-1"`, `"job 1\nsecret"`, 1),
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := Decode(KindHeartbeat, []byte(payload)); err == nil {
				t.Fatal("Decode() error = nil, want rejection")
			}
		})
	}
	oversized := []byte(strings.Repeat(" ", MaxEnvelopeBytes+1))
	if _, err := Decode(KindHeartbeat, oversized); err == nil {
		t.Fatal("oversized Decode() error = nil, want rejection")
	}
	if _, err := Decode("system.not_registered", []byte(valid)); err == nil {
		t.Fatal("unknown kind Decode() error = nil, want rejection")
	}
}

func TestStrictDecoderRejectsInvalidUTF8(t *testing.T) {
	t.Parallel()
	var destination struct {
		Value string `json:"value"`
	}
	data := []byte{'{', '"', 'v', 'a', 'l', 'u', 'e', '"', ':', '"', 0xff, '"', '}'}
	if err := decodeStrict(data, MaxEnvelopeBytes, &destination); err == nil {
		t.Fatal("decodeStrict() error = nil, want invalid UTF-8 rejection")
	}
}

func TestRetentionPayloadBounds(t *testing.T) {
	t.Parallel()
	fixture, err := os.ReadFile(filepath.Join(contractRoot(t), "examples/system.retention_cleanup.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, replacement := range []string{`"batch_size":0`, `"batch_size":1001`, `"retention_policy":"all_rows"`} {
		candidate := string(fixture)
		if strings.HasPrefix(replacement, `"batch_size"`) {
			candidate = strings.Replace(candidate, `"batch_size": 250`, strings.Replace(replacement, ":", ": ", 1), 1)
		} else {
			candidate = strings.Replace(candidate, `"retention_policy": "worker_job_terminal"`, strings.Replace(replacement, ":", ": ", 1), 1)
		}
		if _, err := Decode(KindRetentionCleanup, []byte(candidate)); err == nil {
			t.Fatalf("Decode() accepted %s", replacement)
		}
	}
}

func TestDecodeErrorsDoNotEchoArgumentValues(t *testing.T) {
	t.Parallel()
	heartbeat := `{"contract_version":1,"correlation_id":"job-1","idempotency_key":"heartbeat:1","domain":{"type":"schedule_occurrence","id":"00000000-0000-4000-8000-000000000001"},"payload":{"scheduled_for":"2026-07-21T12:00:00Z","credential-do-not-log":"top-secret"}}`
	retention := `{"contract_version":1,"correlation_id":"job-2","idempotency_key":"retention:1","domain":{"type":"maintenance_run","id":"00000000-0000-4000-8000-000000000002"},"payload":{"batch_size":250,"delete_before":"2026-07-14T12:00:00Z","retention_policy":"top-secret"}}`

	for _, test := range []struct {
		kind string
		data []byte
	}{
		{kind: KindHeartbeat, data: []byte(heartbeat)},
		{kind: KindRetentionCleanup, data: []byte(retention)},
		{kind: "top-secret-kind", data: []byte(`{}`)},
	} {
		_, err := Decode(test.kind, test.data)
		if err == nil {
			t.Fatal("Decode() error = nil, want rejection")
		}
		for _, forbidden := range []string{"credential-do-not-log", "top-secret"} {
			if strings.Contains(err.Error(), forbidden) {
				t.Fatalf("Decode() error leaked %q: %v", forbidden, err)
			}
		}
	}
}

func contractRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
