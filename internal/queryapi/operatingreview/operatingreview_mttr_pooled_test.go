package operatingreview

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

func resolvePooledMTTR(t *testing.T, client *fakeClient) map[string]any {
	t.Helper()
	// Stored daily values average 5 (incident) and 7 (repository): the tile must not read them.
	client.responses = append(mttrWeekScanners(5.0), mttrWeekScanners(5.0)...)
	review, err := Resolve(context.Background(), client, "org-1", nil, graphqldate.New(day("2026-08-24")))
	if err != nil {
		t.Fatal(err)
	}
	m := metricsByKey(t, review)["mttr_hours"]
	return map[string]any{"value": m.Value, "hasData": m.HasData, "reason": reasonOf(m.DegradedReason)}
}

func TestMTTRTileServesThePooledRead(t *testing.T) {
	for _, tc := range []struct {
		name      string
		p50, mean any
		want      float64
	}{
		{"incident median wins", 10.0, 8.4, 10},
		{"no incident median: the pooled bug mean", nil, 8.4, 8.4},
		{"no sample at all", nil, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeClient{pooledSet: true, pooledP50: tc.p50, pooledMean: tc.mean}
			got := resolvePooledMTTR(t, client)
			if got["value"] != tc.want || got["hasData"] != true || got["reason"] != "null" {
				t.Errorf("tile %v, want %v true null", got, tc.want)
			}
			if client.pooled != 4 {
				t.Errorf("%d pooled reads, want 4 (two per week)", client.pooled)
			}
		})
	}
}

func TestMTTRTileIsDegradedWhenThePooledReadFails(t *testing.T) {
	client := &fakeClient{pooledSet: true, pooledP50: 10.0, pooledMean: 8.4, pooledErr: errors.New("boom")}
	got := resolvePooledMTTR(t, client)
	if got["value"] != 0.0 || got["hasData"] != false || got["reason"] != "READ_FAILED" {
		t.Errorf("tile %v, want 0 false READ_FAILED", got)
	}
}
