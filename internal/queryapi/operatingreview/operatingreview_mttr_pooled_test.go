package operatingreview

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

type mttrTile struct {
	value   float64
	hasData bool
	reason  string
	prior   float64
	hasPrio bool
	priorR  string
}

// resolveMTTR resolves two weeks whose stored daily MTTR is storedP50 (incident) and storedMean (repository), nil = NULL,
// with the pooled reads answered by client.
func resolveMTTR(t *testing.T, client *fakeClient, storedP50, storedMean any, errs []error) mttrTile {
	t.Helper()
	week := func() []*fakeRowScanner {
		out := mttrWeekScanners(storedP50)
		out[2] = &fakeRowScanner{rows: [][]any{{uint64(0), 0.0, 0.0, 0.0, uint32(0), 0.0, storedMean, uint64(1)}}}
		return out
	}
	client.responses = append(week(), week()...)
	client.errs = errs
	review, err := Resolve(context.Background(), client, "org-1", nil, graphqldate.New(day("2026-08-24")))
	if err != nil {
		t.Fatal(err)
	}
	m := metricsByKey(t, review)["mttr_hours"]
	return mttrTile{m.Value, m.HasData, reasonOf(m.DegradedReason), m.Delta.PriorValue, m.Delta.HasPriorData, reasonOf(m.Delta.DegradedReason)}
}

func TestMTTRTileServesThePooledRead(t *testing.T) {
	for _, tc := range []struct {
		name      string
		p50, mean any
		want      float64
		hasData   bool
	}{
		{"incident median wins", 10.0, 8.4, 10, true},
		{"no incident median: the pooled bug mean", nil, 8.4, 8.4, true},
		{"a pooled zero is a measured zero", 0.0, nil, 0, true},
		{"no sample at all is no data", nil, nil, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeClient{pooledSet: true, pooledP50: tc.p50, pooledMean: tc.mean}
			// Stored daily values average 5 (incident) and 7 (repository): the tile must not read them.
			got := resolveMTTR(t, client, 5.0, 7.0, nil)
			want := mttrTile{tc.want, tc.hasData, "null", tc.want, tc.hasData, "null"}
			if got != want {
				t.Errorf("tile %+v, want %+v", got, want)
			}
			if client.pooled != 4 {
				t.Errorf("%d pooled reads, want 4 (two per week)", client.pooled)
			}
		})
	}
}

// The availability of the tile is the availability of the pooled result, not of the stored daily rows.
func TestMTTRTileDataIsThePooledResultsNotTheStoredRows(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		storedP50, storedMean any
		pooledP50, pooledMean any
		want                  float64
		hasData               bool
	}{
		{"pooled sample, stored NULL: data", nil, nil, nil, 4.0, 4, true},
		{"empty pool, stored non-NULL: no data, not a measured zero", 5.0, 7.0, nil, nil, 0, false},
		{"empty pool, stored zero: no data", 0.0, 0.0, nil, nil, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeClient{pooledSet: true, pooledP50: tc.pooledP50, pooledMean: tc.pooledMean}
			got := resolveMTTR(t, client, tc.storedP50, tc.storedMean, nil)
			want := mttrTile{tc.want, tc.hasData, "null", tc.want, tc.hasData, "null"}
			if got != want {
				t.Errorf("tile %+v, want %+v", got, want)
			}
		})
	}
}

// A failed read of a stored daily summary does not touch a tile that is read from the pool.
func TestMTTRTileIsNotDegradedByAFailedStoredRead(t *testing.T) {
	for _, call := range []int{2, 6} {
		for _, week := range []int{0, 11} {
			errs := make([]error, 22)
			errs[call+week] = errors.New("boom")
			client := &fakeClient{pooledSet: true, pooledP50: 10.0, pooledMean: 8.4}
			got := resolveMTTR(t, client, 5.0, 7.0, errs)
			want := mttrTile{10, true, "null", 10, true, "null"}
			if got != want {
				t.Errorf("stored read %d of week at %d failed: tile %+v, want %+v", call, week, got, want)
			}
		}
	}
}

func TestMTTRTileIsDegradedWhenThePooledReadFails(t *testing.T) {
	client := &fakeClient{pooledSet: true, pooledP50: 10.0, pooledMean: 8.4, pooledErr: errors.New("boom")}
	got := resolveMTTR(t, client, 5.0, 7.0, nil)
	want := mttrTile{0, false, "READ_FAILED", 0, false, "READ_FAILED"}
	if got != want {
		t.Errorf("tile %+v, want %+v", got, want)
	}
}

// Both weeks read the incident mappings at the one request instant, as the report does, not at the end of each week.
func TestMTTRTileReadsTheMappingsAtOneRequestInstant(t *testing.T) {
	client := &fakeClient{pooledSet: true, pooledP50: 10.0, pooledMean: 8.4}
	resolveMTTR(t, client, 5.0, 7.0, nil)
	if len(client.pooledAsOf) != 2 || client.pooledAsOf[0] != client.pooledAsOf[1] {
		t.Fatalf("as_of bindings %v, want the same instant for both weeks", client.pooledAsOf)
	}
	if client.pooledAsOf[0] == "2026-08-31 00:00:00.000000" || client.pooledAsOf[0] == "2026-08-24 00:00:00.000000" {
		t.Errorf("as_of %v is a week end, want the request instant", client.pooledAsOf[0])
	}
}
