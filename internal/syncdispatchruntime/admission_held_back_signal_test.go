package syncdispatchruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// CHAOS-14403 unit cells: the held-back signal vocabulary, its pick order, the recheck statement's probes, and the
// bucket_decision line's form.

var allHeldBackSignals = []heldBackSignal{
	heldBackSignalNone, heldBackSignalRecentRun, heldBackSignalFinalizePending, heldBackSignalUnitHeartbeat,
	heldBackSignalUnitLease, heldBackSignalUnitDispatching, heldBackSignalWakeArmed, heldBackSignalRecheckedNone,
	heldBackSignalReadFailed,
}

func TestPickHeldBackSignalTakesTheFirstTrueProbeInOrder(t *testing.T) {
	for index, want := range heldBackSignalOrder {
		var probes [6]bool
		probes[index] = true
		if got := pickHeldBackSignal(probes); got != want {
			t.Fatalf("probe %d alone: %q, want %q", index, got, want)
		}
	}
	for _, overlap := range []struct {
		probes [6]bool
		want   heldBackSignal
	}{
		{[6]bool{false, false, true, false, false, true}, heldBackSignalUnitHeartbeat},
		{[6]bool{false, true, false, true, false, false}, heldBackSignalFinalizePending},
		{[6]bool{true, true, true, true, true, true}, heldBackSignalRecentRun},
		{[6]bool{}, heldBackSignalRecheckedNone},
	} {
		if got := pickHeldBackSignal(overlap.probes); got != overlap.want {
			t.Fatalf("probes %v: %q, want %q", overlap.probes, got, overlap.want)
		}
	}
	if want := [6]heldBackSignal{"recent_run", "finalize_pending", "unit_heartbeat", "unit_lease", "unit_dispatching", "wake_armed"}; heldBackSignalOrder != want {
		t.Fatalf("documented order = %v, want %v", heldBackSignalOrder, want)
	}
}

// Every signal logs as its own closed-vocabulary label, never "invalid", and two signals never share a label.
func TestHeldBackSignalsLogAsTheirClosedLabels(t *testing.T) {
	seen := map[string]bool{}
	for _, signal := range allHeldBackSignals {
		var buf bytes.Buffer
		synclog.New(slog.New(slog.NewTextHandler(&buf, nil))).Info(context.Background(), synclog.MsgDispatchGuardBucketDecision,
			synclog.Text(synclog.KeyGuardHeldBackSignal, signal.label()))
		want := "guard.held_back_signal=" + string(signal) + "\n"
		if !strings.HasSuffix(buf.String(), want) || seen[string(signal)] {
			t.Fatalf("signal %q logged as %q", signal, buf.String())
		}
		seen[string(signal)] = true
	}
}

// olderRunsInBucketSQL is unchanged by CHAOS-14403 (sha256 of base 510da49d8): its row set is the base row set.
func TestOlderRunsInBucketSQLIsTheBaseStatement(t *testing.T) {
	sum := sha256.Sum256([]byte(olderRunsInBucketSQL))
	if got := hex.EncodeToString(sum[:]); got != "965c1a42ccd0d861624c07cef3214e49828e8d660617d0a3a3c8b4078ed9682a" {
		t.Fatalf("olderRunsInBucketSQL sha256 = %s; the liveness read changed: re-run the held-back signal cells and re-pin", got)
	}
}

// The recheck's six probes are olderRunsInBucketSQL's six liveness probes, in heldBackSignalOrder, with $7 -> $2 (now)
// and $9 -> $3 (the bound in seconds): a probe edited in one statement and not the other fails here.
func TestHeldBackSignalProbesAreTheOlderRunsProbes(t *testing.T) {
	space := regexp.MustCompile(`\s+`)
	normal := func(text string) string { return strings.TrimSpace(space.ReplaceAllString(text, " ")) }
	start := strings.Index(olderRunsInBucketSQL, "    AND (\n        run.created_at")
	end := strings.Index(olderRunsInBucketSQL, "\n    )\nORDER BY")
	if start < 0 || end < start {
		t.Fatal("olderRunsInBucketSQL: liveness block not found; update this test's markers")
	}
	block := olderRunsInBucketSQL[start+len("    AND (") : end]
	probes := strings.Split(block, "\n        OR ")
	if len(probes) != 6 {
		t.Fatalf("olderRunsInBucketSQL has %d liveness probes, want 6 (one per heldBackSignalOrder entry)", len(probes))
	}
	renumber := strings.NewReplacer("$7", "$2", "$9", "$3")
	recheck := normal(heldBackSignalSQL)
	selectStart, fromStart := strings.Index(recheck, "SELECT "), strings.LastIndex(recheck, " FROM public.sync_runs AS run WHERE run.id = $1::uuid")
	if selectStart != 0 || fromStart < 0 {
		t.Fatalf("heldBackSignalSQL shape changed:\n%s", heldBackSignalSQL)
	}
	var want []string
	for _, probe := range probes {
		want = append(want, normal(renumber.Replace(probe)))
	}
	if got := strings.Split(recheck[len("SELECT "):fromStart], ", EXISTS"); len(got) != 6 {
		t.Fatalf("heldBackSignalSQL selects %d probes, want 6", len(got))
	} else {
		for index := 1; index < len(got); index++ {
			got[index] = "EXISTS" + got[index]
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("heldBackSignalSQL probes differ from olderRunsInBucketSQL's:\n got %q\nwant %q", got, want)
		}
	}
}

// signalEnv is admissionGuardEnv plus a counting heldBackSignal and a recording emit.
func signalEnv(t *testing.T, self runOrder, active map[dispatchBucket]int, older map[dispatchBucket][]olderRun, signal heldBackSignal) (guardEnv, *[]string, *[]string) {
	t.Helper()
	env, _ := admissionGuardEnv(self, active, older)
	reads, emitted := &[]string{}, &[]string{}
	env.heldBackSignal = func(runID string) heldBackSignal {
		*reads = append(*reads, runID)
		return signal
	}
	env.emit = func(bucket dispatchBucket, _, _, _, _, _, heldBack int, heldBackRun string, got heldBackSignal) {
		*emitted = append(*emitted, bucket.costClass+" "+string(rune('0'+heldBack))+" "+heldBackRun+" "+string(got))
	}
	return env, reads, emitted
}

// held == 0 -> the recheck is not run and the line says none; held > 0 -> one recheck, for the oldest held-for run.
func TestHeldBackSignalIsReadOnlyWhenTheBucketHeldSlotsBack(t *testing.T) {
	medium := dispatchBucket{orgID: admissionOrg, provider: "github", costClass: "medium"}
	self := runOrder{id: admissionRunB, createdAt: admissionNow}
	source := sliceSource(plannedUnits("u", 3, "github", "medium"))
	dead := olderRun{runOrder: runOrder{id: admissionRunA, createdAt: admissionNow.Add(-1)}, progressAt: admissionNow.Add(-2 * admissionLivenessBound()), waiting: 2}
	env, reads, emitted := signalEnv(t, self, nil, map[dispatchBucket][]olderRun{medium: {dead}}, heldBackSignalWakeArmed)
	if _, err := decideDispatchGuard(source, env, admissionOrg, admissionNow); err != nil {
		t.Fatal(err)
	}
	if len(*reads) != 0 || !reflect.DeepEqual(*emitted, []string{"medium 0  none"}) {
		t.Fatalf("nothing held back: recheck reads %v, emitted %v; want no read and none", *reads, *emitted)
	}

	live := dead
	live.progressAt = admissionNow
	older := live
	older.id, older.createdAt = admissionRunC, admissionNow.Add(-2)
	env, reads, emitted = signalEnv(t, self, nil, map[dispatchBucket][]olderRun{medium: {live, older}}, heldBackSignalWakeArmed)
	if _, err := decideDispatchGuard(source, env, admissionOrg, admissionNow); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*reads, []string{admissionRunC}) || !reflect.DeepEqual(*emitted, []string{"medium 2 " + admissionRunC + " wake_armed"}) {
		t.Fatalf("held back: recheck reads %v, emitted %v; want one read for the oldest run %s", *reads, *emitted, admissionRunC)
	}
}

// guard.held_back keeps its key and bare integer form (a roll script greps it followed by a number); the new keys
// follow it.
func TestBucketDecisionHeldBackValueFormIsUnchanged(t *testing.T) {
	bucket := dispatchBucket{orgID: "123e4567-e89b-12d3-a456-426614174000", provider: "github", costClass: "medium"}
	var text, json bytes.Buffer
	emitBucketDecision(context.Background(), synclog.New(slog.New(slog.NewTextHandler(&text, nil))), bucket, 0, 0, 3, 0, 0, 2, "14403a0e-0000-4000-8000-000000014403", heldBackSignalWakeArmed)
	emitBucketDecision(context.Background(), synclog.New(slog.New(slog.NewJSONHandler(&json, nil))), bucket, 0, 0, 3, 0, 0, 2, "14403a0e-0000-4000-8000-000000014403", heldBackSignalWakeArmed)
	if want := " guard.held_back=2 guard.held_back_run=14403a0e guard.held_back_signal=wake_armed\n"; !strings.HasSuffix(text.String(), want) {
		t.Fatalf("text line %q, want suffix %q", text.String(), want)
	}
	if want := `,"guard.held_back":2,"guard.held_back_run":"14403a0e","guard.held_back_signal":"wake_armed"}`; !strings.Contains(json.String(), want) {
		t.Fatalf("json line %q, want %q", json.String(), want)
	}
	if matches := regexp.MustCompile(`guard\.held_back[=":]+[0-9]`).FindAllString(text.String()+json.String(), -1); len(matches) != 2 {
		t.Fatalf("guard.held_back followed by a number appears %d times (%v), want once per line", len(matches), matches)
	}
}

// The span event carries the same two new attributes; guard.held_back stays an int.
func TestBucketDecisionSpanEventCarriesTheHeldBackRunAndSignal(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	ctx, span := provider.Tracer("held-back-signal-test").Start(context.Background(), "coordinator")
	bucket := dispatchBucket{orgID: "123e4567-e89b-12d3-a456-426614174000", provider: "github", costClass: "medium"}
	emitBucketDecision(ctx, synclog.New(slog.New(slog.DiscardHandler)), bucket, 0, 0, 3, 0, 0, 2, "14403a0e-0000-4000-8000-000000014403", heldBackSignalUnitLease)
	emitBucketDecision(ctx, synclog.New(slog.New(slog.DiscardHandler)), bucket, 0, 0, 1, 0, 0, 0, "", heldBackSignalNone)
	span.End()
	spans := exporter.GetSpans()
	if len(spans) != 1 || len(spans[0].Events) != 2 {
		t.Fatalf("spans = %d, events = %v; want one span with two bucket_decision events", len(spans), spans)
	}
	for index, want := range []struct {
		heldBack    int64
		run, signal string
	}{{2, "14403a0e", "unit_lease"}, {0, "none", "none"}} {
		got := map[string]attribute.Value{}
		for _, attr := range spans[0].Events[index].Attributes {
			got[string(attr.Key)] = attr.Value
		}
		if got["guard.held_back"].Type() != attribute.INT64 || got["guard.held_back"].AsInt64() != want.heldBack ||
			got["guard.held_back_run"].AsString() != want.run || got["guard.held_back_signal"].AsString() != want.signal {
			t.Fatalf("event %d attributes %v, want held_back=%d run=%q signal=%q", index, got, want.heldBack, want.run, want.signal)
		}
	}
}
