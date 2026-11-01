package revisionfeed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

type call struct {
	typ, id  string
	revision int64
	all      bool
}

type recorder struct {
	mu    sync.Mutex
	calls []call
	// failures is how many of the next calls return errInvalidate (each still recorded).
	failures int
	// onInvalidate runs inside Invalidate, before it returns.
	onInvalidate func()
}

var errInvalidate = errors.New("invalidate failed")

func (r *recorder) next() error {
	if r.failures > 0 {
		r.failures--
		return errInvalidate
	}
	return nil
}

func (r *recorder) Invalidate(_ context.Context, typ, id string, revision int64) error {
	r.mu.Lock()
	hook := r.onInvalidate
	r.calls = append(r.calls, call{typ: typ, id: id, revision: revision})
	err := r.next()
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
	return err
}

func (r *recorder) InvalidateAll(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call{all: true})
	return r.next()
}

func (r *recorder) failNext(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = n
}

func (r *recorder) snapshot() []call {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]call(nil), r.calls...)
}

func newTestConsumer(t *testing.T) (*Consumer, *recorder) {
	t.Helper()
	rec := &recorder{}
	c, err := NewConsumer(Config{Invalidator: rec})
	if err != nil {
		t.Fatal(err)
	}
	return c, rec
}

func ev(typ, id string, revision int64) contracts.ProvisioningEvent {
	return contracts.ProvisioningEvent{
		AggregateType: typ, AggregateID: id,
		Payload: contracts.ProvisioningPayload{Revision: contracts.Revision(revision)},
	}
}

// The manifest is read, not copied (contracts/auth/v1/examples/provisioning-event/manifest.json).
type vectorFile struct {
	ConsumerVectors struct {
		Cases []struct {
			Name  string  `json:"name"`
			Steps [][]any `json:"steps"`
		} `json:"cases"`
	} `json:"consumer_vectors"`
}

func loadVectors(t *testing.T) vectorFile {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "contracts", "auth", "v1", "examples", "provisioning-event", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f vectorFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

// The one case where the Consumer differs from the bare cursor on purpose: after a Gap the consumer moves its mark to the
// gap revision (ResyncTo), so a repeat of the gap revision and the skipped revisions are Ignore, not Gap and Apply.
var consumerDeviation = map[string][]string{
	"gap is not applied and does not advance": {"apply", "gap", "ignore", "ignore", "ignore"},
}

func TestConsumerReplaysTheManifestVectors(t *testing.T) {
	vectors := loadVectors(t)
	if len(vectors.ConsumerVectors.Cases) != 7 {
		t.Fatalf("the manifest declares %d consumer cases, want 7", len(vectors.ConsumerVectors.Cases))
	}
	for _, tc := range vectors.ConsumerVectors.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			c, rec := newTestConsumer(t)
			want := consumerDeviation[tc.Name]
			applied, gapped := 0, 0
			for i, step := range tc.Steps {
				wantStep := step[3].(string)
				if want != nil {
					wantStep = want[i]
				}
				got, _ := c.Handle(context.Background(), ev(step[0].(string), step[1].(string), int64(step[2].(float64))))
				if string(got) != wantStep {
					t.Fatalf("step %d (%v): %q, want %q", i, step[:3], got, wantStep)
				}
				switch got {
				case contracts.Apply:
					applied++
				case contracts.Gap:
					gapped++
				}
			}
			if n := len(rec.snapshot()); n != applied+gapped {
				t.Fatalf("%d Invalidate calls, want %d (one per apply or gap, none per ignore)", n, applied+gapped)
			}
		})
	}
}

func TestDuplicateInvalidatesOnce(t *testing.T) {
	c, rec := newTestConsumer(t)
	for i, want := range []contracts.Disposition{contracts.Apply, contracts.Ignore, contracts.Ignore} {
		if got, _ := c.Handle(context.Background(), ev("principal", "p1", 1)); got != want {
			t.Fatalf("delivery %d: %q, want %q", i, got, want)
		}
	}
	if calls := rec.snapshot(); len(calls) != 1 || calls[0] != (call{typ: "principal", id: "p1", revision: 1}) {
		t.Fatalf("calls %+v, want one Invalidate(principal, p1, 1)", calls)
	}
}

func TestOutOfOrderIsAGapThenIgnoredAndTheCursorEndsAtTheLaterRevision(t *testing.T) {
	c, rec := newTestConsumer(t)
	ctx := context.Background()
	if got, _ := c.Handle(ctx, ev("principal", "p1", 1)); got != contracts.Apply {
		t.Fatalf("1: %q", got)
	}
	if got, _ := c.Handle(ctx, ev("principal", "p1", 3)); got != contracts.Gap {
		t.Fatalf("3: %q, want gap", got)
	}
	if got, _ := c.Handle(ctx, ev("principal", "p1", 2)); got != contracts.Ignore {
		t.Fatalf("2 after 3: %q, want ignore", got)
	}
	if got, _ := c.Handle(ctx, ev("principal", "p1", 4)); got != contracts.Apply {
		t.Fatalf("4: %q, want apply (the mark is at 3)", got)
	}
	want := []call{{typ: "principal", id: "p1", revision: 1}, {typ: "principal", id: "p1", revision: 3}, {typ: "principal", id: "p1", revision: 4}}
	got := rec.snapshot()
	if len(got) != len(want) {
		t.Fatalf("calls %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("call %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestAGapInvalidatesTheAggregateAndMovesTheMark(t *testing.T) {
	c, rec := newTestConsumer(t)
	ctx := context.Background()
	c.Handle(ctx, ev("organization", "o1", 1))
	if got, _ := c.Handle(ctx, ev("organization", "o1", 3)); got != contracts.Gap {
		t.Fatalf("3: %q, want gap", got)
	}
	calls := rec.snapshot()
	if len(calls) != 2 || calls[1] != (call{typ: "organization", id: "o1", revision: 3}) {
		t.Fatalf("calls %+v, want the gap to Invalidate(organization, o1, 3)", calls)
	}
	if got, _ := c.Handle(ctx, ev("organization", "o1", 3)); got != contracts.Ignore {
		t.Fatalf("the gap revision again: %q, want ignore (a gap repeats forever without the resync)", got)
	}
	if latest, ok := c.Watermarks().Latest("organization", "o1"); !ok || latest != 3 {
		t.Fatalf("watermark = %d, %v, want 3", latest, ok)
	}
}

func TestARestartedConsumerSeesFiveAsAGapThenSixApplies(t *testing.T) {
	first, _ := newTestConsumer(t)
	first.Handle(context.Background(), ev("principal", "p1", 1))
	second, rec := newTestConsumer(t)
	if got, _ := second.Handle(context.Background(), ev("principal", "p1", 5)); got != contracts.Gap {
		t.Fatalf("5 on a new consumer: %q, want gap", got)
	}
	if got, _ := second.Handle(context.Background(), ev("principal", "p1", 6)); got != contracts.Apply {
		t.Fatalf("6: %q, want apply", got)
	}
	if n := len(rec.snapshot()); n != 2 {
		t.Fatalf("%d Invalidate calls, want 2 (the gap purge and the apply)", n)
	}
}

func TestResubscribedInvalidatesEverythingOnce(t *testing.T) {
	c, rec := newTestConsumer(t)
	ctx := context.Background()
	c.Handle(ctx, ev("principal", "p1", 1))
	c.Resubscribed(ctx)
	calls := rec.snapshot()
	if len(calls) != 2 || !calls[1].all {
		t.Fatalf("calls %+v, want the apply then one InvalidateAll", calls)
	}
	if got, _ := c.Handle(ctx, ev("principal", "p1", 1)); got != contracts.Ignore {
		t.Fatalf("a duplicate after a resubscribe: %q, want ignore (the marks survive it)", got)
	}
}

func TestWatermarksKeepTheLatestRevisionPerAggregate(t *testing.T) {
	c, _ := newTestConsumer(t)
	ctx := context.Background()
	for _, e := range []contracts.ProvisioningEvent{ev("principal", "p1", 1), ev("principal", "p1", 2), ev("user", "p1", 1), ev("principal", "p1", 1)} {
		c.Handle(ctx, e)
	}
	marks := c.Watermarks()
	if got, ok := marks.Latest("principal", "p1"); !ok || got != 2 {
		t.Fatalf("principal p1 = %d, %v, want 2 (a stale delivery never lowers it)", got, ok)
	}
	if got, ok := marks.Latest("user", "p1"); !ok || got != 1 {
		t.Fatalf("user p1 = %d, %v, want 1 (same id, other type)", got, ok)
	}
	if _, ok := marks.Latest("principal", "p9"); ok {
		t.Fatal("an unseen aggregate has a watermark")
	}
}

func counterValue(t *testing.T, reader *metric.ManualReader, name, key, value string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum := m.Data.(metricdata.Sum[int64])
			for _, point := range sum.DataPoints {
				if v, ok := point.Attributes.Value(attribute.Key(key)); ok && v.AsString() == value {
					total += point.Value
				}
			}
		}
	}
	return total
}

func TestMetricsUseClosedLabelSets(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	rec := &recorder{}
	c, err := NewConsumer(Config{Invalidator: rec, Meter: provider.Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	c.Handle(ctx, ev("principal", "p1", 1))
	c.Handle(ctx, ev("principal", "p1", 1))
	c.Handle(ctx, ev("principal", "p1", 5))
	c.Resubscribed(ctx)
	for _, tc := range []struct {
		name, key, value string
		want             int64
	}{
		{"devhealth_auth_revision_feed_events_total", "disposition", "apply", 1},
		{"devhealth_auth_revision_feed_events_total", "disposition", "ignore", 1},
		{"devhealth_auth_revision_feed_events_total", "disposition", "gap", 1},
		{"devhealth_auth_revision_feed_resync_total", "cause", "gap", 1},
		{"devhealth_auth_revision_feed_resync_total", "cause", "resubscribe", 1},
	} {
		if got := counterValue(t, reader, tc.name, tc.key, tc.value); got != tc.want {
			t.Errorf("%s{%s=%s} = %d, want %d", tc.name, tc.key, tc.value, got, tc.want)
		}
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				if point.Attributes.Len() != 1 {
					t.Errorf("%s carries %d labels, want exactly one closed label", m.Name, point.Attributes.Len())
				}
			}
		}
	}
}

// A capturing log handler: nothing the consumer logs may hold the aggregate id or any payload value.
func TestNoLogLineCarriesThePayloadOrTheAggregateID(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	rec := &recorder{}
	c, err := NewConsumer(Config{Invalidator: rec, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	const id, secret = "prn_SENTINEL_ID_0001", "SENTINEL_PAYLOAD_VALUE"
	e := ev("principal", id, 1)
	e.Payload.Change = map[string]json.RawMessage{"note": json.RawMessage(`"` + secret + `"`)}
	e.Payload.ActorPrincipalID = "prn_SENTINEL_ACTOR"
	e.Payload.RequestID = "req_SENTINEL"
	ctx := context.Background()
	c.Handle(ctx, e)
	c.Handle(ctx, e)
	e.Payload.Revision = 9
	c.Handle(ctx, e)
	c.Resubscribed(ctx)
	if out.Len() == 0 {
		t.Fatal("nothing was logged; the test would pass vacuously")
	}
	for _, leak := range []string{id, secret, "SENTINEL_ACTOR", "req_SENTINEL"} {
		if strings.Contains(out.String(), leak) {
			t.Fatalf("a log line holds %q:\n%s", leak, out.String())
		}
	}
	for _, want := range []string{`"aggregate_type":"principal"`, `"revision"`, `"disposition"`} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the log lacks %s:\n%s", want, out.String())
		}
	}
}
