package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

type queueHealthSampler struct {
	snapshot riverstore.QueueTelemetrySnapshot
	err      error
}

func (sampler queueHealthSampler) Snapshot(context.Context) (riverstore.QueueTelemetrySnapshot, error) {
	return sampler.snapshot, sampler.err
}

func (queueHealthSampler) CheckAvailableContractVersions(context.Context) error { return nil }

func queueHealthRecords(t *testing.T, sampler queueTelemetrySampler) []map[string]any {
	t.Helper()
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	monitor := newQueueHealthMonitor(sampler, logger, "ops")
	if monitor == nil {
		t.Fatal("queue health monitor was not constructed")
	}
	monitor.sample()
	records := make([]map[string]any, 0)
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("queue health record is not JSON: %v", err)
		}
		records = append(records, record)
	}
	return records
}

func messages(records []map[string]any) []string {
	result := make([]string, 0, len(records))
	for _, record := range records {
		message, _ := record["msg"].(string)
		result = append(result, message)
	}
	return result
}

// TestQueueHealthPreservesCeleryAlertConditions pins the two thresholds the
// monitor-queue-depths Beat task alerted on, so existing alert rules keep
// firing on the same conditions after Celery is gone.
func TestQueueHealthPreservesCeleryAlertConditions(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		snapshot riverstore.QueueTelemetrySnapshot
		want     []string
	}{
		{
			name: "empty queues are not reported",
			snapshot: riverstore.QueueTelemetrySnapshot{
				Profile: "ops",
				Jobs:    []riverstore.QueueJobTelemetry{{Queue: "retention", Kind: "system.retention_cleanup"}},
				Queues:  []riverstore.QueueAgeTelemetry{{Queue: "retention"}},
			},
			want: []string{},
		},
		{
			name: "healthy queue reports depth only",
			snapshot: riverstore.QueueTelemetrySnapshot{
				Profile: "ops",
				Jobs: []riverstore.QueueJobTelemetry{
					{Queue: "webhooks", Kind: "operational.webhook_delivery", Available: 12},
				},
				Queues: []riverstore.QueueAgeTelemetry{{Queue: "webhooks", OldestAvailableAge: time.Minute}},
			},
			want: []string{"queue_depth"},
		},
		{
			name: "depth above the Celery threshold escalates",
			snapshot: riverstore.QueueTelemetrySnapshot{
				Profile: "ops",
				Jobs: []riverstore.QueueJobTelemetry{
					{Queue: "webhooks", Kind: "operational.webhook_delivery", Available: 120},
					{Queue: "webhooks", Kind: "operational.billing_notification", Available: 81},
				},
				Queues: []riverstore.QueueAgeTelemetry{{Queue: "webhooks", OldestAvailableAge: time.Second}},
			},
			want: []string{"queue_depth", "queue_backlog"},
		},
		{
			name: "age above the Celery threshold escalates a shallow queue",
			snapshot: riverstore.QueueTelemetrySnapshot{
				Profile: "ops",
				Jobs: []riverstore.QueueJobTelemetry{
					{Queue: "heartbeat", Kind: "system.heartbeat", Available: 1},
				},
				Queues: []riverstore.QueueAgeTelemetry{
					{Queue: "heartbeat", OldestAvailableAge: 601 * time.Second},
				},
			},
			want: []string{"queue_depth", "queue_backlog"},
		},
		{
			name: "saturation is reported without a backlog",
			snapshot: riverstore.QueueTelemetrySnapshot{
				Profile:             "ops",
				ExecutionSaturation: 0.95,
			},
			want: []string{"queue_saturation"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := messages(queueHealthRecords(t, queueHealthSampler{snapshot: test.snapshot}))
			if len(got) != len(test.want) {
				t.Fatalf("records = %v, want %v", got, test.want)
			}
			for index, message := range test.want {
				if got[index] != message {
					t.Fatalf("records = %v, want %v", got, test.want)
				}
			}
		})
	}
}

// TestQueueHealthDepthAggregatesEveryKindOnOneQueue keeps the depth comparable
// with the Celery task, which measured a whole queue rather than one job type.
func TestQueueHealthDepthAggregatesEveryKindOnOneQueue(t *testing.T) {
	t.Parallel()
	records := queueHealthRecords(t, queueHealthSampler{
		snapshot: riverstore.QueueTelemetrySnapshot{
			Profile: "ops",
			Jobs: []riverstore.QueueJobTelemetry{
				{Queue: "webhooks", Kind: "operational.webhook_delivery", Available: 5},
				{Queue: "webhooks", Kind: "operational.billing_notification", Available: 7},
			},
			Queues: []riverstore.QueueAgeTelemetry{{Queue: "webhooks"}},
		},
	})
	if len(records) != 1 || records[0]["depth"] != float64(12) {
		t.Fatalf("aggregated depth records = %#v", records)
	}
}

// TestQueueHealthSamplerFailureStaysBounded proves a telemetry outage cannot
// leak a DSN into operator logs and cannot stop the monitor.
func TestQueueHealthSamplerFailureStaysBounded(t *testing.T) {
	t.Parallel()
	records := queueHealthRecords(t, queueHealthSampler{
		err: errors.New("postgresql://queue:secret@db/app"),
	})
	if len(records) != 1 || records[0]["msg"] != queueHealthFailureLogging {
		t.Fatalf("failure records = %#v", records)
	}
	for _, value := range records[0] {
		if text, ok := value.(string); ok && strings.Contains(text, "secret") {
			t.Fatal("queue health logging leaked a credential")
		}
	}
}

func TestQueueHealthMonitorRequiresCompleteConstruction(t *testing.T) {
	t.Parallel()
	logger := slog.Default()
	if newQueueHealthMonitor(nil, logger, "ops") != nil {
		t.Fatal("monitor constructed without a telemetry sampler")
	}
	if newQueueHealthMonitor(queueHealthSampler{}, nil, "ops") != nil {
		t.Fatal("monitor constructed without a logger")
	}
	if newQueueHealthMonitor(queueHealthSampler{}, logger, "") != nil {
		t.Fatal("monitor constructed without a profile")
	}
}

func TestQueueHealthMonitorStopsOnShutdown(t *testing.T) {
	t.Parallel()
	monitor := newQueueHealthMonitor(queueHealthSampler{}, slog.Default(), "ops")
	if err := monitor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := monitor.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
