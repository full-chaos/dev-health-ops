package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

func TestWorkgraphCompatibilityHTTPClientUsesRiverExecutionDeadline(t *testing.T) {
	t.Parallel()
	client := workgraphCompatibilityHTTPClient(time.Second)
	if client == nil || client.Timeout != 0 {
		t.Fatalf("workgraph compatibility timeout=%v want=0", client.Timeout)
	}
}

func TestMetricCompatibilityHTTPClientUsesRiverExecutionDeadline(t *testing.T) {
	t.Parallel()
	client := metricCompatibilityHTTPClient(time.Second)
	if client == nil || client.Timeout != 0 {
		t.Fatalf("metric compatibility timeout=%v want=0", client.Timeout)
	}
}

func TestNativeChainCompatibilityBudgetsExceedOperationalConnectionBudget(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	registry, err := jobruntime.Load(filepath.Join("contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	for kind, minimum := range map[string]time.Duration{
		jobcontract.KindDailyMetricsPartition: 2 * time.Hour,
		jobcontract.KindDailyMetricsFinalize:  15 * time.Minute,
		jobcontract.KindRemainingComplexity:   2 * time.Hour,
		jobcontract.KindRemainingMembership:   2 * time.Hour,
		jobcontract.KindWorkGraphBuild:        time.Hour,
		jobcontract.KindInvestmentMaterialize: 2 * time.Hour,
	} {
		descriptor, ok := registry.Descriptor(kind)
		if !ok || descriptor.Timeout != minimum {
			t.Fatalf("%s timeout=%v found=%t want=%v", kind, descriptor.Timeout, ok, minimum)
		}
	}
}
