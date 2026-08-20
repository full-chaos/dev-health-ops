package riverstore

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNormalizeQueueTelemetryConfig(t *testing.T) {
	t.Parallel()
	config := validQueueTelemetryConfig()
	normalized, err := normalizeQueueTelemetryConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.clientID != "worker-01" ||
		normalized.queryTimeout != defaultQueueTelemetryTimeout || normalized.executionCapacity != 4 {
		t.Fatalf("unexpected normalized scalar config: %#v", normalized)
	}
	if got, want := normalized.queueNames, []string{"heartbeat", "retention"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("queue order = %v, want %v", got, want)
	}
	if got, want := normalized.jobKinds, []string{"system.heartbeat", "system.retention_cleanup"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("job order = %v, want %v", got, want)
	}
	if got, want := normalized.supportedVersions, []int32{1, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("supported versions = %v, want %v", got, want)
	}
}

func TestNormalizeQueueTelemetryConfigRejectsUnsafeOrAmbiguousInputs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*QueueTelemetryConfig)
	}{
		{name: "schema", mutate: func(config *QueueTelemetryConfig) { config.Schema = `river".public` }},
		{name: "client missing", mutate: func(config *QueueTelemetryConfig) { config.ClientID = "" }},
		{name: "client too long", mutate: func(config *QueueTelemetryConfig) { config.ClientID = strings.Repeat("x", 101) }},
		{name: "client nul", mutate: func(config *QueueTelemetryConfig) { config.ClientID = "worker\x00secret" }},
		{name: "timeout", mutate: func(config *QueueTelemetryConfig) { config.QueryTimeout = 31 * time.Second }},
		{name: "no queues", mutate: func(config *QueueTelemetryConfig) { config.Queues = nil }},
		{name: "duplicate queue", mutate: func(config *QueueTelemetryConfig) { config.Queues[1].Name = config.Queues[0].Name }},
		{name: "zero workers", mutate: func(config *QueueTelemetryConfig) { config.Queues[0].MaxWorkers = 0 }},
		{name: "too many workers", mutate: func(config *QueueTelemetryConfig) { config.Queues[0].MaxWorkers = 10_001 }},
		{name: "unused queue", mutate: func(config *QueueTelemetryConfig) { config.Jobs = config.Jobs[:1] }},
		{name: "unknown queue", mutate: func(config *QueueTelemetryConfig) { config.Jobs[0].Queue = "unknown" }},
		{name: "duplicate job", mutate: func(config *QueueTelemetryConfig) { config.Jobs[1] = config.Jobs[0] }},
		{name: "no versions", mutate: func(config *QueueTelemetryConfig) { config.Jobs[0].SupportedVersions = nil }},
		{name: "unsorted versions", mutate: func(config *QueueTelemetryConfig) { config.Jobs[0].SupportedVersions = []int{2, 1} }},
		{name: "duplicate version", mutate: func(config *QueueTelemetryConfig) { config.Jobs[0].SupportedVersions = []int{1, 1} }},
		{name: "nonpositive version", mutate: func(config *QueueTelemetryConfig) { config.Jobs[0].SupportedVersions = []int{0} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := validQueueTelemetryConfig()
			test.mutate(&config)
			if _, err := normalizeQueueTelemetryConfig(config); !errors.Is(err, ErrQueueTelemetryConfiguration) {
				t.Fatalf("normalizeQueueTelemetryConfig() error = %v", err)
			}
		})
	}
	if _, err := NewQueueTelemetrySampler(nil, validQueueTelemetryConfig()); !errors.Is(err, ErrQueueTelemetryConfiguration) {
		t.Fatalf("nil pool constructor error = %v", err)
	}
}

func TestQueueTelemetrySamplerBuildsStableBoundedSnapshot(t *testing.T) {
	t.Parallel()
	sampler := testQueueTelemetrySampler(t, func(context.Context) ([]queueTelemetryRow, error) {
		return []queueTelemetryRow{
			{queue: "retention", kind: "system.retention_cleanup", available: 5, oldestAgeSeconds: 7.25, localRunning: 3, queueRunning: 2},
			{queue: "heartbeat", kind: "system.heartbeat", available: 2, oldestAgeSeconds: 12.5, localRunning: 3, queueRunning: 1},
		}, nil
	})
	snapshot, err := sampler.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LocalRunning != 3 || snapshot.ExecutionSaturation != 0.75 {
		t.Fatalf("unexpected snapshot scalars: %#v", snapshot)
	}
	wantJobs := []QueueJobTelemetry{
		{Queue: "heartbeat", Kind: "system.heartbeat", Available: 2},
		{Queue: "retention", Kind: "system.retention_cleanup", Available: 5},
	}
	if !reflect.DeepEqual(snapshot.Jobs, wantJobs) {
		t.Fatalf("jobs = %#v, want %#v", snapshot.Jobs, wantJobs)
	}
	wantQueues := []QueueAgeTelemetry{
		{Queue: "heartbeat", OldestAvailableAge: 12*time.Second + 500*time.Millisecond},
		{Queue: "retention", OldestAvailableAge: 7*time.Second + 250*time.Millisecond},
	}
	if !reflect.DeepEqual(snapshot.Queues, wantQueues) {
		t.Fatalf("queues = %#v, want %#v", snapshot.Queues, wantQueues)
	}
	wantCapacities := []QueueCapacityTelemetry{
		{Queue: "heartbeat", Capacity: 2, Running: 1, Saturation: 0.5},
		{Queue: "retention", Capacity: 2, Running: 2, Saturation: 1},
	}
	if !reflect.DeepEqual(snapshot.QueueCapacities, wantCapacities) {
		t.Fatalf("queue capacities = %#v, want %#v", snapshot.QueueCapacities, wantCapacities)
	}
	if err := sampler.CheckAvailableContractVersions(context.Background()); err != nil {
		t.Fatalf("supported version readiness error = %v", err)
	}
}

func TestQueueTelemetrySamplerCompatibilityAndQueryErrorsAreStable(t *testing.T) {
	t.Parallel()
	// The refusal now NAMES the offending queue/kind/version. Those three are
	// bounded, pre-registered labels this process already publishes as metric
	// dimensions; the arguments behind them stay unread. Withholding them cost
	// a production incident its diagnosis (CHAOS-3938).
	t.Run("unsupported", func(t *testing.T) {
		t.Parallel()
		offenders := []string{"sync/dispatch_sync_run@7"}
		sampler := testQueueTelemetrySampler(t, func(context.Context) ([]queueTelemetryRow, error) {
			return []queueTelemetryRow{
				{queue: "heartbeat", kind: "system.heartbeat", unsupportedOccupants: offenders},
				{queue: "retention", kind: "system.retention_cleanup", unsupportedOccupants: offenders},
			}, nil
		})
		err := sampler.CheckAvailableContractVersions(context.Background())
		if !errors.Is(err, ErrUnsupportedAvailableContractVersion) {
			t.Fatalf("compatibility error = %v", err)
		}
		if !strings.Contains(err.Error(), "sync/dispatch_sync_run@7") {
			t.Fatalf("compatibility error did not name the offending contract: %v", err)
		}
		var named *UnsupportedContractVersionError
		if !errors.As(err, &named) || !reflect.DeepEqual(named.Offenders, offenders) {
			t.Fatalf("compatibility error carried no offender labels: %v", err)
		}
		if strings.Contains(err.Error(), "payload") || strings.Contains(err.Error(), "encoded_args") {
			t.Fatalf("compatibility error leaked row detail: %v", err)
		}
	})
	// A label outside the bounded vocabulary is REPLACED, not echoed -- and
	// crucially it does not take the backlog metrics down with it. Refusing
	// the whole read would have broken this sampler's contract that an
	// unsupported contract never hides metrics, and it is reachable from a
	// pre-existing river_job row whose kind is not in the label character
	// class: an odd kind must cost the diagnostic its precision, never the
	// metrics their availability.
	t.Run("offender label out of vocabulary", func(t *testing.T) {
		t.Parallel()
		leaked := []string{"sync/dispatch_sync_run@{\"token\":\"secret\"}"}
		sampler := testQueueTelemetrySampler(t, func(context.Context) ([]queueTelemetryRow, error) {
			return []queueTelemetryRow{
				{queue: "heartbeat", kind: "system.heartbeat", unsupportedOccupants: leaked},
				{queue: "retention", kind: "system.retention_cleanup", unsupportedOccupants: leaked},
			}, nil
		})
		if _, err := sampler.Snapshot(context.Background()); err != nil {
			t.Fatalf("an odd offender label made the backlog metrics unavailable: %v", err)
		}
		err := sampler.CheckAvailableContractVersions(context.Background())
		if !errors.Is(err, ErrUnsupportedAvailableContractVersion) {
			t.Fatalf("out-of-vocabulary offender label was not refused: %v", err)
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "token") {
			t.Fatalf("out-of-vocabulary offender label leaked row detail: %v", err)
		}
		var named *UnsupportedContractVersionError
		if !errors.As(err, &named) || !reflect.DeepEqual(named.Offenders, []string{unprintableOffender}) {
			t.Fatalf("offenders = %v, want [%s]", named, unprintableOffender)
		}
	})
	t.Run("query error", func(t *testing.T) {
		t.Parallel()
		sampler := testQueueTelemetrySampler(t, func(context.Context) ([]queueTelemetryRow, error) {
			return nil, errors.New("encoded_args credential-secret")
		})
		_, err := sampler.Snapshot(context.Background())
		if err != ErrQueueTelemetryUnavailable || strings.Contains(err.Error(), "credential") {
			t.Fatalf("query error was not sanitized: %v", err)
		}
	})
}

func TestQueueTelemetrySamplerHonorsQueryTimeout(t *testing.T) {
	t.Parallel()
	config := validQueueTelemetryConfig()
	config.QueryTimeout = 10 * time.Millisecond
	normalized, err := normalizeQueueTelemetryConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	sampler := &QueueTelemetrySampler{
		config: normalized,
		read: func(ctx context.Context) ([]queueTelemetryRow, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	started := time.Now()
	if _, err := sampler.Snapshot(context.Background()); err != ErrQueueTelemetryUnavailable {
		t.Fatalf("timed out Snapshot() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("query timeout took %s", elapsed)
	}
}

func TestQueueTelemetrySamplerRejectsMalformedDatabaseSnapshots(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		rows []queueTelemetryRow
	}{
		{name: "missing row", rows: []queueTelemetryRow{{queue: "heartbeat", kind: "system.heartbeat"}}},
		{name: "unknown row", rows: []queueTelemetryRow{{queue: "heartbeat", kind: "unknown"}, {queue: "retention", kind: "system.retention_cleanup"}}},
		{name: "negative count", rows: []queueTelemetryRow{{queue: "heartbeat", kind: "system.heartbeat", available: -1}, {queue: "retention", kind: "system.retention_cleanup"}}},
		{name: "nan age", rows: []queueTelemetryRow{{queue: "heartbeat", kind: "system.heartbeat", oldestAgeSeconds: math.NaN()}, {queue: "retention", kind: "system.retention_cleanup"}}},
		{name: "inconsistent scalar", rows: []queueTelemetryRow{{queue: "heartbeat", kind: "system.heartbeat", localRunning: 1}, {queue: "retention", kind: "system.retention_cleanup", localRunning: 2}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sampler := testQueueTelemetrySampler(t, func(context.Context) ([]queueTelemetryRow, error) {
				return test.rows, nil
			})
			if _, err := sampler.Snapshot(context.Background()); err != ErrQueueTelemetryUnavailable {
				t.Fatalf("Snapshot() error = %v", err)
			}
		})
	}
}

func TestExecutionSaturationIsBounded(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		running  int64
		capacity int64
		want     float64
	}{
		{running: 0, capacity: 4, want: 0},
		{running: 1, capacity: 4, want: 0.25},
		{running: 4, capacity: 4, want: 1},
		{running: 5, capacity: 4, want: 1},
	} {
		if got := executionSaturation(test.running, test.capacity); got != test.want {
			t.Fatalf("executionSaturation(%d, %d) = %v, want %v", test.running, test.capacity, got, test.want)
		}
	}
}

func validQueueTelemetryConfig() QueueTelemetryConfig {
	return QueueTelemetryConfig{
		Schema:   "river",
		ClientID: "worker-01",
		Queues: []QueueTelemetryQueue{
			{Name: "retention", MaxWorkers: 2},
			{Name: "heartbeat", MaxWorkers: 2},
		},
		Jobs: []QueueTelemetryJob{
			{Queue: "retention", Kind: "system.retention_cleanup", SupportedVersions: []int{1, 2}},
			{Queue: "heartbeat", Kind: "system.heartbeat", SupportedVersions: []int{1}},
		},
	}
}

func testQueueTelemetrySampler(t *testing.T, read queueTelemetryReadFunc) *QueueTelemetrySampler {
	t.Helper()
	normalized, err := normalizeQueueTelemetryConfig(validQueueTelemetryConfig())
	if err != nil {
		t.Fatal(err)
	}
	return &QueueTelemetrySampler{config: normalized, read: read}
}
