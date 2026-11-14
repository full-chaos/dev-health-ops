package fixed

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/jackc/pgx/v5"
)

type unmintedStubProducer struct{ outcome Outcome }

func (unmintedStubProducer) ID() string { return ProducerHeartbeat }

func (producer unmintedStubProducer) Produce(context.Context, pgx.Tx, Schedule, Occurrence) (Outcome, error) {
	return producer.outcome, nil
}

// The engine carries what a producer reports about passed-over instants into the
// schedule result of the window that committed the occurrence.
func TestEngineCarriesUnmintedInstantsIntoTheScheduleResult(t *testing.T) {
	schedule := heartbeatSchedule(t)
	reported := UnmintedInstants{
		ReportID:  skipTestReportID,
		MintedFor: mustTime(t, "2026-07-25T00:00:00Z"),
		Count:     1,
		Named:     []time.Time{mustTime(t, "2026-07-24T23:00:00Z")},
	}
	failed := UnmintedCountFailure{ReportID: "report-b", MintedFor: reported.MintedFor, Error: "count scan failed"}
	producers, err := NewProducerSet(unmintedStubProducer{outcome: Outcome{
		Handoffs: 1, Unminted: []UnmintedInstants{reported}, UnmintedCountFailed: []UnmintedCountFailure{failed},
	}})
	if err != nil {
		t.Fatal(err)
	}
	ledger := newMemoryLedger()
	seedAnchor(ledger, schedule, mustTime(t, "2026-07-24T00:00:00Z"))
	engine, err := newEngine(EngineConfig{
		Schedules: []Schedule{schedule},
		Producers: producers,
		Ledger:    ledger,
		Publisher: &recordingPublisher{},
		Registry:  testRegistry(t),
	}, stubBeginner{tx: &stubTx{}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Step(context.Background(), mustTime(t, "2026-07-25T09:00:00Z"))
	if err != nil || result.Failed() {
		t.Fatalf("Step() = %v / %v", err, result.Err())
	}
	got := result.Schedules[0]
	if len(got.Unminted) != 1 || got.Unminted[0].ReportID != reported.ReportID ||
		len(got.UnmintedCountFailed) != 1 || got.UnmintedCountFailed[0].Error != failed.Error {
		t.Fatalf("schedule result lost the producer's unminted report: %+v", got)
	}
}

// The loop writes one bounded WARN line per minted occurrence, one line per failed
// count, and adds the counts to a counter that has no report or instant label.
func TestLoopLogsAndCountsUnmintedInstants(t *testing.T) {
	var logs bytes.Buffer
	schedule := heartbeatSchedule(t)
	minted := mustTime(t, "2026-07-25T03:00:00Z")
	stepper := resultStepper{schedules: []Schedule{schedule}, results: []ScheduleResult{{
		ScheduleID: schedule.ID, Due: 1, Claimed: 1, Handoffs: 1,
		Unminted: []UnmintedInstants{
			{ReportID: "report-a", MintedFor: minted, Count: 4, Named: []time.Time{
				mustTime(t, "2026-07-24T23:00:00Z"), mustTime(t, "2026-07-25T02:00:00Z"),
			}},
			{ReportID: "report-c", MintedFor: minted, Count: 1024, Capped: true, Named: []time.Time{
				mustTime(t, "2026-07-24T00:01:00Z"), mustTime(t, "2026-07-24T17:04:00Z"),
			}},
		},
		UnmintedCountFailed: []UnmintedCountFailure{{ReportID: "report-b", MintedFor: minted, Error: "count scan failed"}},
	}}}
	config := DefaultLoopConfig(health.NewRegistry(time.Second))
	config.Logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	loop, err := newLoop(stepper, config, &fixedTestClock{now: mustTime(t, "2026-07-25T03:00:05Z")})
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.step(context.Background(), mustTime(t, "2026-07-25T03:00:05Z")); err != nil {
		t.Fatal(err)
	}

	var lines []map[string]any
	scanner := bufio.NewScanner(&logs)
	for scanner.Scan() {
		line := map[string]any{}
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", scanner.Text(), err)
		}
		lines = append(lines, line)
	}
	if len(lines) != 3 {
		t.Fatalf("log lines = %d, want 3 (two counts, one failure); lines = %v", len(lines), lines)
	}
	first := lines[0]
	if first["level"] != "WARN" || first["reason"] != "unminted_instants" || first["report_id"] != "report-a" ||
		first["scheduled_for"] != "2026-07-25T03:00:00Z" || first["unminted_count"] != float64(4) ||
		first["unminted_count_capped"] != false ||
		first["unminted_instants"] != "2026-07-24T23:00:00Z,2026-07-25T02:00:00Z" {
		t.Errorf("unminted line = %v", first)
	}
	if lines[1]["unminted_count_capped"] != true || lines[1]["unminted_count"] != float64(1024) {
		t.Errorf("capped line = %v", lines[1])
	}
	if lines[2]["reason"] != "unminted_count_failed" || lines[2]["report_id"] != "report-b" || lines[2]["error"] != "count scan failed" {
		t.Errorf("count failure line = %v", lines[2])
	}

	var metrics strings.Builder
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	want := "report_scheduler_unminted_instants_total 1028\n"
	if !strings.Contains(metrics.String(), "# TYPE report_scheduler_unminted_instants_total counter\n"+want) {
		t.Errorf("counter missing or wrong, want %q in:\n%s", want, metrics.String())
	}
}
