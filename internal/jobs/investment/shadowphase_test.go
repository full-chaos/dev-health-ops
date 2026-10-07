package investment

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// syncBuffer is a log sink that goroutines can share.
type syncBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func debugLogger(sink *syncBuffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(sink, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func countLines(logs, message string) int {
	return strings.Count(logs, `msg="`+message+`"`)
}

// The state the phase exists to reach: one shadow row for each gate-pass unit,
// with a mix or a named failure state, and attempt rows with tokens, cost and
// latency. Units under the gate are not asked.
func TestShadowPhaseWritesAShadowRowAndAttemptRowsForEachGatePassUnit(t *testing.T) {
	fake := newFakeJev(t, nil)
	logs := &syncBuffer{}
	phase := newTestShadowPhase(t, fake, shadowTestSettings(), debugLogger(logs))
	store := &memoryShadowStore{}
	cfg := shadowTestConfig()

	entries := shadowTestEntries(t, "u1", "u2", "u3")
	short := shadowTestBundle(t, "short", 1)
	if short.TextCharCount >= minEvidenceChars {
		t.Fatalf("the short bundle has %d characters: it must be under the gate", short.TextCharCount)
	}
	entries = append(entries, shadowEntry(3, "short", short))
	noSource := shadowTestBundle(t, "nosource", 6)
	noSource.TextSourceCount = 0
	entries = append(entries, shadowEntry(4, "nosource", noSource))

	summary := phase.run(context.Background(), store, cfg, entries)

	if fake.count() != 3 {
		t.Fatalf("requests = %d, want 3 (one for each gate-pass unit, none for the two under the gate)", fake.count())
	}
	if summary.StopReason != ShadowStopDone || summary.GatePass != 3 || summary.Eligible != 3 || summary.Attempted != 3 || summary.RowsWritten != 3 {
		t.Fatalf("summary = %+v", summary)
	}
	if len(store.records) != 3 {
		t.Fatalf("shadow rows = %d, want 3", len(store.records))
	}
	wantConfig := decision.IdentityFor("").Stamp()
	for _, unit := range []string{"u1", "u2", "u3"} {
		record := store.recordByUnit(t, unit)
		if record.State != decision.StateOK || record.CategorizationStatus != categorize.StatusOK || !record.CompleteStrict {
			t.Fatalf("%s: state %q status %q strict %v, want ok", unit, record.State, record.CategorizationStatus, record.CompleteStrict)
		}
		sum := 0.0
		for _, weight := range record.SubcategoryDistribution {
			sum += weight
		}
		if len(record.SubcategoryDistribution) != 15 || math.Abs(sum-1) > 1e-9 {
			t.Fatalf("%s: mix has %d keys and sum %v, want 15 keys and sum 1", unit, len(record.SubcategoryDistribution), sum)
		}
		if !(record.SubcategoryDistribution["quality.bugfix"] > record.SubcategoryDistribution["quality.reliability"]) ||
			record.SubcategoryDistribution["quality.reliability"] <= 0 {
			t.Fatalf("%s: mix does not follow the answered levels: %v", unit, record.SubcategoryDistribution)
		}
		if record.ShadowConfig != wantConfig || record.RubricSHA256 != decision.RubricSHA256 {
			t.Fatalf("%s: config %q rubric %q", unit, record.ShadowConfig, record.RubricSHA256)
		}
		if record.EvidenceSpanID == "" || record.EvidenceHandle == "" || record.EvidenceSourceType != "issue" || record.EvidenceSourceID != "ENG-"+unit {
			t.Fatalf("%s: evidence = %q %q %q %q", unit, record.EvidenceSpanID, record.EvidenceHandle, record.EvidenceSourceType, record.EvidenceSourceID)
		}
		if len(record.Levels) != 15 || record.Levels["quality.bugfix"] != 3 || len(record.LevelProbabilities) != 15 || record.SufficiencyLevel != 2 {
			t.Fatalf("%s: levels %v sufficiency %d", unit, record.Levels, record.SufficiencyLevel)
		}
		if record.ServedRunID != cfg.RunID || !record.ComputedAt.Equal(cfg.ComputedAt) || record.ModelReturned != decision.DefaultModel {
			t.Fatalf("%s: run %q at %v model %q", unit, record.ServedRunID, record.ComputedAt, record.ModelReturned)
		}
	}
	if len(store.attempts) != 3 || summary.AttemptRows != 3 {
		t.Fatalf("attempt rows = %d (summary %d), want 3", len(store.attempts), summary.AttemptRows)
	}
	for _, attempt := range store.attempts {
		if attempt.Role != chwrite.RoleShadow || attempt.Config != wantConfig || attempt.Kind != "first" || attempt.Attempt != 1 ||
			attempt.Provider != "typesafe" || attempt.APIMode != "systemone" || attempt.ModelRequested != decision.DefaultModel {
			t.Fatalf("attempt identity = %+v", attempt)
		}
		if attempt.InputTokens != 2383 || attempt.OutputTokens != 368 || attempt.LatencyMS == 0 || attempt.HTTPStatus != 200 || attempt.State != "ok" {
			t.Fatalf("attempt measures = %+v", attempt)
		}
		// 2383 tokens x USD 0.042 for each million.
		if attempt.BilledCostUSD != 100086e-9 || attempt.RatesVersion != shadowRatesVersion {
			t.Fatalf("attempt cost = %v (%s), want 0.000100086", attempt.BilledCostUSD, attempt.RatesVersion)
		}
		if attempt.RunID != cfg.RunID || !attempt.ComputedAt.Equal(cfg.ComputedAt) {
			t.Fatalf("attempt run = %q at %v", attempt.RunID, attempt.ComputedAt)
		}
	}
	if summary.BilledNanoUSD != 3*100086 || summary.InputTokens != 3*2383 {
		t.Fatalf("spend = %d nano-USD, tokens %d", summary.BilledNanoUSD, summary.InputTokens)
	}
	if countLines(logs.String(), "investment shadow phase complete") != 1 {
		t.Fatalf("want exactly one run log line:\n%s", logs.String())
	}
	for _, bearer := range fake.bearers {
		if bearer != "Bearer "+shadowTestKeyValue {
			t.Fatalf("the request did not carry the configured bearer")
		}
	}
}

// The eligibility gate, clause by clause: characters and text sources.
func TestShadowEligibleIsTheProductionGate(t *testing.T) {
	for _, tc := range []struct {
		name           string
		chars, sources int
		want           bool
	}{
		{"at the character bound with a source", minEvidenceChars, 1, true},
		{"one character under the bound", minEvidenceChars - 1, 1, false},
		{"enough characters and no source", minEvidenceChars + 500, 0, false},
		{"neither", 0, 0, false},
	} {
		got := shadowEligible(units.TextBundle{TextCharCount: tc.chars, TextSourceCount: tc.sources})
		if got != tc.want {
			t.Errorf("%s: eligible = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The gate through the phase: a unit that fails one clause only is never sent.
func TestAUnitUnderTheGateIsNeverSent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*units.TextBundle)
		want   int
	}{
		{"passes both clauses", func(*units.TextBundle) {}, 1},
		{"one character under the bound", func(b *units.TextBundle) { b.TextCharCount = minEvidenceChars - 1 }, 0},
		{"no text source", func(b *units.TextBundle) { b.TextSourceCount = 0 }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeJev(t, nil)
			phase := newTestShadowPhase(t, fake, shadowTestSettings(), testLogger())
			bundle := shadowTestBundle(t, "u1", 6)
			tc.mutate(&bundle)
			phase.run(context.Background(), &memoryShadowStore{}, shadowTestConfig(), []preprocessed{shadowEntry(0, "u1", bundle)})
			if fake.count() != tc.want {
				t.Fatalf("requests = %d, want %d", fake.count(), tc.want)
			}
		})
	}
}

// The switch, clause by clause: provider set, org allowed, unit in the sample.
func TestTheShadowSwitchNeedsAProviderAnAllowedOrgAndASampledUnit(t *testing.T) {
	on := ShadowSettings{Provider: "typesafe", OrgIDs: map[string]struct{}{"org-a": {}}, SamplePercent: 100}
	for _, tc := range []struct {
		name     string
		settings ShadowSettings
		org      string
		want     bool
	}{
		{"all three hold", on, "org-a", true},
		{"no provider", ShadowSettings{OrgIDs: on.OrgIDs, SamplePercent: 100}, "org-a", false},
		{"org not in the list", on, "org-b", false},
		{"empty org list", ShadowSettings{Provider: "typesafe", SamplePercent: 100}, "org-a", false},
		{"sample of zero", ShadowSettings{Provider: "typesafe", OrgIDs: on.OrgIDs, SamplePercent: 0}, "org-a", false},
		{"every org", ShadowSettings{Provider: "typesafe", AllOrgs: true, SamplePercent: 100}, "org-b", true},
		{"every org does not mean no org", ShadowSettings{Provider: "typesafe", AllOrgs: true, SamplePercent: 100}, "", false},
	} {
		if got := tc.settings.unitSelected(tc.org, "unit-1"); got != tc.want {
			t.Errorf("%s: selected = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The switch through the phase: each clause alone keeps every request back.
func TestAPhaseWithOneSwitchClauseOffSendsNothing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ShadowSettings)
		want   int
	}{
		{"all on", func(*ShadowSettings) {}, 2},
		{"no provider", func(s *ShadowSettings) { s.Provider = "" }, 0},
		{"org not allowed", func(s *ShadowSettings) { s.AllOrgs = false; s.OrgIDs = map[string]struct{}{"another-org": {}} }, 0},
		{"empty org list", func(s *ShadowSettings) { s.AllOrgs = false }, 0},
		{"sample of zero", func(s *ShadowSettings) { s.SamplePercent = 0 }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeJev(t, nil)
			settings := shadowTestSettings()
			tc.mutate(&settings)
			store := &memoryShadowStore{}
			phase := newTestShadowPhase(t, fake, settings, testLogger())
			phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1", "u2"))
			if fake.count() != tc.want || len(store.records) != tc.want {
				t.Fatalf("requests = %d, rows = %d, want %d", fake.count(), len(store.records), tc.want)
			}
		})
	}
}

// The sample is a property of the unit id: a unit is always in or always out,
// and the share follows the percentage.
func TestTheSampleIsStableForAUnitAndFollowsThePercentage(t *testing.T) {
	half := ShadowSettings{SamplePercent: 50}
	in := 0
	for index := 0; index < 2000; index++ {
		unit := fmt.Sprintf("unit-%d", index)
		first := half.inSample(unit)
		if first != half.inSample(unit) {
			t.Fatalf("%s changed sides", unit)
		}
		if first {
			in++
			if !(ShadowSettings{SamplePercent: 51}).inSample(unit) {
				t.Fatalf("%s is in at 50 and out at 51", unit)
			}
		}
	}
	if in < 850 || in > 1150 {
		t.Fatalf("%d of 2000 units in a 50 percent sample", in)
	}
	if (ShadowSettings{SamplePercent: 0}).inSample("unit-1") || !(ShadowSettings{SamplePercent: 100}).inSample("unit-1") {
		t.Fatal("0 percent must select nothing and 100 percent everything")
	}
}

// A unit that already has a terminal shadow row of this configuration is not
// asked again, and the lookup is made with the phase's own stamp.
func TestAUnitWithAnExistingShadowRowIsNotAskedAgain(t *testing.T) {
	fake := newFakeJev(t, nil)
	phase := newTestShadowPhase(t, fake, shadowTestSettings(), testLogger())
	entries := shadowTestEntries(t, "u1", "u2")
	store := &memoryShadowStore{existing: map[chquery.InvestmentKey]struct{}{
		{WorkUnitID: "u1", InputHash: entries[0].result.Bundle.InputHash}: {},
		// u2 has a row for ANOTHER input hash: its text changed, so it is asked.
		{WorkUnitID: "u2", InputHash: "another-input-hash"}: {},
	}}
	summary := phase.run(context.Background(), store, shadowTestConfig(), entries)
	if fake.count() != 1 || summary.SkippedExisting != 1 || summary.Attempted != 1 {
		t.Fatalf("requests = %d, summary = %+v", fake.count(), summary)
	}
	if len(store.records) != 1 || store.records[0].WorkUnitID != "u2" {
		t.Fatalf("rows = %+v", store.records)
	}
	if len(store.readConfigs) != 1 || store.readConfigs[0] != decision.IdentityFor("").Stamp() {
		t.Fatalf("the skip-existing lookup used config %q", store.readConfigs)
	}
}

// Every state other than ok is a row with a named state, an empty mix and no
// evidence: a failure row must not look like a mix.
func TestANonOkStateIsARowWithAnEmptyMix(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reply      jevReply
		wantState  string
		wantStatus string
		wantTokens uint32
	}{
		{"zero support", jevReply{supported: map[string]int{}, inputTokens: 2000}, decision.StateZeroSupport, categorize.StatusInsufficientChars, 2000},
		{"evidence none", jevReply{supported: map[string]int{"quality.bugfix": 3}, evidenceNone: true, inputTokens: 2000}, decision.StateEvidenceNone, categorize.StatusInvalidLLMOutput, 2000},
		{"model mismatch", jevReply{supported: map[string]int{"quality.bugfix": 3}, model: "another-model-9", inputTokens: 2000}, decision.StateRequestFailed, categorize.StatusLLMTaskFailed, 2000},
		{"body that is not JSON", jevReply{body: []byte("<html>not json</html>")}, decision.StateRequestFailed, categorize.StatusLLMTaskFailed, 0},
		{"an answer of an unknown type", jevReply{supported: map[string]int{"quality.bugfix": 3}, extraAnswers: map[string]string{"support__quality__bugfix": "refusal"}, inputTokens: 2000}, decision.StateAnswerInvalid, categorize.StatusInvalidLLMOutput, 2000},
		{"invalid request (422)", jevReply{status: http.StatusUnprocessableEntity, body: []byte(`{"error":"bad"}`)}, decision.StateRequestFailed, categorize.StatusLLMTaskFailed, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeJev(t, func(int, []byte) jevReply { return tc.reply })
			phase := newTestShadowPhase(t, fake, shadowTestSettings(), testLogger())
			store := &memoryShadowStore{}
			summary := phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1"))
			record := store.recordByUnit(t, "u1")
			if record.State != tc.wantState || record.CategorizationStatus != tc.wantStatus {
				t.Fatalf("state %q status %q, want %q %q", record.State, record.CategorizationStatus, tc.wantState, tc.wantStatus)
			}
			if len(record.SubcategoryDistribution) != 0 || record.CompleteStrict ||
				record.EvidenceSpanID != "" || record.EvidenceHandle != "" || record.EvidenceSourceType != "" || record.EvidenceSourceID != "" {
				t.Fatalf("a %s row carries a mix or evidence: %+v", tc.wantState, record)
			}
			if len(chwrite.ShadowThemeDistribution(record.SubcategoryDistribution)) != 0 {
				t.Fatal("the theme map of a failure row is not empty")
			}
			if len(record.ErrorCodes) == 0 {
				t.Fatal("a failure row names no error code")
			}
			if summary.States[tc.wantState] != 1 || summary.StopReason != ShadowStopDone {
				t.Fatalf("summary = %+v", summary)
			}
			if len(store.attempts) != 1 || store.attempts[0].State != tc.wantState || store.attempts[0].InputTokens != tc.wantTokens {
				t.Fatalf("attempts = %+v", store.attempts)
			}
		})
	}
}

// The stamp is built one time from configured values. A response field (the
// returned model, here with a dated suffix that the model rule accepts) is
// stored in its own column and never moves the key.
func TestTheConfigStampNeverHoldsAResponseField(t *testing.T) {
	fake := newFakeJev(t, func(n int, _ []byte) jevReply {
		reply := okReply()
		if n == 1 {
			reply.model = decision.DefaultModel + "-20261001"
		}
		return reply
	})
	settings := shadowTestSettings()
	settings.Concurrency = 1
	phase := newTestShadowPhase(t, fake, settings, testLogger())
	store := &memoryShadowStore{}
	phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1", "u2"))
	want := shadowConfigStamp(decision.IdentityFor(""))
	if want != "provider=typesafe;api=systemone;model=jev-1.13.0;taxonomy=investment-taxonomy-v1;prompt=decision-support-v1d@73ace2d4e437;adapter=decision-adapter-v3;map=support-map-v1;level=presence-floor:0.4" {
		t.Fatalf("the stamp changed: %q", want)
	}
	models := map[string]bool{}
	for _, record := range store.records {
		if record.ShadowConfig != want {
			t.Fatalf("row config = %q", record.ShadowConfig)
		}
		models[record.ModelReturned] = true
	}
	if !models[decision.DefaultModel+"-20261001"] || !models[decision.DefaultModel] {
		t.Fatalf("returned models = %v", models)
	}
	for _, attempt := range store.attempts {
		if attempt.Config != want || attempt.ModelRequested != decision.DefaultModel {
			t.Fatalf("attempt config = %q requested %q", attempt.Config, attempt.ModelRequested)
		}
	}
}

// A deterministic failure (a rejected key) on the first request stops the
// phase: one request, one loud line, the rest of the units not asked.
func TestADeterministicFailureStopsThePhaseWithOneLoudLine(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden, http.StatusNotFound} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			fake := newFakeJev(t, func(int, []byte) jevReply { return jevReply{status: status, body: []byte(`{}`)} })
			logs := &syncBuffer{}
			settings := shadowTestSettings()
			settings.Concurrency = 1
			phase := newTestShadowPhase(t, fake, settings, debugLogger(logs))
			observer := &recordingShadowObserver{}
			phase.SetObserver(observer)
			store := &memoryShadowStore{}
			summary := phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1", "u2", "u3"))
			if fake.count() != 1 {
				t.Fatalf("requests = %d, want 1: the phase must stop asking", fake.count())
			}
			if summary.StopReason != ShadowStopDeterministicFailure || summary.Attempted != 1 {
				t.Fatalf("summary = %+v", summary)
			}
			if len(store.records) != 1 || store.records[0].State != decision.StateRequestFailed {
				t.Fatalf("rows = %+v", store.records)
			}
			if got := countLines(logs.String(), "investment shadow phase stopped"); got != 1 {
				t.Fatalf("stop lines = %d, want exactly 1:\n%s", got, logs.String())
			}
			if !strings.Contains(logs.String(), "stop_reason=deterministic_failure") {
				t.Fatalf("the stop line does not name the reason:\n%s", logs.String())
			}
			if len(observer.counts) != 1 || observer.counts[0].StopReason != ShadowStopDeterministicFailure {
				t.Fatalf("metric = %+v", observer.counts)
			}
		})
	}
}

// A retried request is two attempt rows: the first with its failure class and
// no state, the second with the state, the usage and the wait before it.
func TestARetriedRequestIsTwoAttemptRows(t *testing.T) {
	fake := newFakeJev(t, func(n int, _ []byte) jevReply {
		if n == 1 {
			return jevReply{status: http.StatusTooManyRequests, headers: map[string]string{"Retry-After": "0.02", "X-Typesafe-Request-Id": "req-first"}, body: []byte(`{}`)}
		}
		reply := okReply()
		reply.headers = map[string]string{"X-Typesafe-Request-Id": "req-second"}
		return reply
	})
	phase := newTestShadowPhase(t, fake, shadowTestSettings(), testLogger())
	observer := &recordingShadowObserver{}
	phase.SetObserver(observer)
	store := &memoryShadowStore{}
	summary := phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1"))
	if len(store.attempts) != 2 || summary.Attempts != 2 {
		t.Fatalf("attempt rows = %+v", store.attempts)
	}
	first, second := store.attempts[0], store.attempts[1]
	if first.Attempt != 1 || first.Kind != "first" || first.HTTPStatus != 429 || first.ErrorClass != "rate_limit" ||
		first.State != "" || first.InputTokens != 0 || first.BilledCostUSD != 0 || first.RequestID != "req-first" {
		t.Fatalf("first attempt = %+v", first)
	}
	if second.Attempt != 2 || second.Kind != "retry" || second.HTTPStatus != 200 || second.ErrorClass != "" ||
		second.State != "ok" || second.InputTokens != 2383 || second.RetryWaitMS == 0 || second.RequestID != "req-second" {
		t.Fatalf("second attempt = %+v", second)
	}
	if len(observer.counts) != 1 || observer.counts[0].AttemptsByState["retried"] != 1 || observer.counts[0].AttemptsByState["ok"] != 1 ||
		len(observer.counts[0].AttemptLatencies) != 2 {
		t.Fatalf("metric = %+v", observer.counts)
	}
}

// panicClassifier panics for one unit and passes every other one on.
type panicClassifier struct {
	inner   shadowClassifier
	panicOn string
}

func (classifier panicClassifier) Classify(ctx context.Context, bundle units.TextBundle) (decision.Classification, error) {
	if strings.Contains(bundle.SourceBlock, classifier.panicOn) {
		panic("planted panic with " + shadowSourceSentinel)
	}
	return classifier.inner.Classify(ctx, bundle)
}

// A panic inside one classification ends that unit as adapter_defect. The
// process survives, the other units are classified, the counter moves, and the
// panic value (which can hold source text) is not logged.
func TestAPanicInAClassificationEndsThatUnitAsAnAdapterDefect(t *testing.T) {
	fake := newFakeJev(t, nil)
	logs := &syncBuffer{}
	phase := newTestShadowPhase(t, fake, shadowTestSettings(), debugLogger(logs))
	phase.classifier = panicClassifier{inner: phase.classifier, panicOn: "Export job times out u2"}
	observer := &recordingShadowObserver{}
	phase.SetObserver(observer)
	store := &memoryShadowStore{}
	summary := phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1", "u2", "u3"))
	if summary.PanicsRecovered != 1 || summary.Attempted != 3 || summary.StopReason != ShadowStopDone {
		t.Fatalf("summary = %+v", summary)
	}
	defect := store.recordByUnit(t, "u2")
	if defect.State != decision.StateAdapterDefect || len(defect.SubcategoryDistribution) != 0 ||
		len(defect.ErrorCodes) != 1 || defect.ErrorCodes[0] != "adapter_defect:panic" {
		t.Fatalf("the unit of the panic = %+v", defect)
	}
	for _, unit := range []string{"u1", "u3"} {
		if store.recordByUnit(t, unit).State != decision.StateOK {
			t.Fatalf("%s was not classified after the panic", unit)
		}
	}
	if len(observer.counts) != 1 || observer.counts[0].PanicsRecovered != 1 {
		t.Fatalf("metric = %+v", observer.counts)
	}
	if countLines(logs.String(), "investment shadow adapter defect") != 1 {
		t.Fatalf("want one ERROR line for the defect:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), shadowSourceSentinel) || strings.Contains(logs.String(), "planted panic") {
		t.Fatalf("the panic value reached a log line:\n%s", logs.String())
	}
}

// deadlineClassifier records the deadline of the context it is given.
type deadlineClassifier struct {
	mu        sync.Mutex
	deadlines []time.Duration
}

func (classifier *deadlineClassifier) Classify(ctx context.Context, _ units.TextBundle) (decision.Classification, error) {
	deadline, ok := ctx.Deadline()
	classifier.mu.Lock()
	defer classifier.mu.Unlock()
	if !ok {
		classifier.deadlines = append(classifier.deadlines, -1)
	} else {
		classifier.deadlines = append(classifier.deadlines, time.Until(deadline))
	}
	return decision.Classification{State: decision.StateZeroSupport, SufficiencyLevel: -1}, nil
}

// The budget is a deadline on the context of every classification, and no
// setting can put it above the clamp: 600 s asked, 120 s given.
func TestTheBudgetIsADeadlineThatNoSettingLiftsAboveTheClamp(t *testing.T) {
	for _, tc := range []struct {
		name     string
		budget   time.Duration
		min, max time.Duration
	}{
		{"600 s is clamped to 120 s", 600 * time.Second, 110 * time.Second, 120 * time.Second},
		{"the default", 0, 50 * time.Second, 60 * time.Second},
		{"30 s stays 30 s", 30 * time.Second, 20 * time.Second, 30 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeJev(t, nil)
			settings := shadowTestSettings()
			settings.Budget = tc.budget
			phase := newTestShadowPhase(t, fake, settings, testLogger())
			// The struct is filled the way a caller that skips the env parser
			// would fill it: the phase must clamp on its own.
			phase.settings.Budget = tc.budget
			probe := &deadlineClassifier{}
			phase.classifier = probe
			phase.run(context.Background(), &memoryShadowStore{}, shadowTestConfig(), shadowTestEntries(t, "u1"))
			if len(probe.deadlines) != 1 {
				t.Fatalf("classifications = %d", len(probe.deadlines))
			}
			if got := probe.deadlines[0]; got < tc.min || got > tc.max {
				t.Fatalf("the classification context ends in %v, want between %v and %v", got, tc.min, tc.max)
			}
		})
	}
	settings, err := ShadowSettingsFromEnv(envLookup(map[string]string{EnvShadowProvider: "typesafe", EnvShadowOrgIDs: "*", EnvShadowMaxSeconds: "600"}))
	if err != nil || settings.Budget != 120*time.Second {
		t.Fatalf("INVESTMENT_SHADOW_MAX_SECONDS=600 gives %v (%v), want 120 s", settings.Budget, err)
	}
}

// A provider that does not answer cannot hold the run: the phase ends at its
// budget, says so, and writes no row for a unit it did not get an answer for.
func TestAPhaseThatRunsOutOfBudgetStopsAndSaysSo(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	fake := newFakeJev(t, func(int, []byte) jevReply { return jevReply{wait: release} })
	logs := &syncBuffer{}
	settings := shadowTestSettings()
	settings.Budget = 300 * time.Millisecond
	phase := newTestShadowPhase(t, fake, settings, debugLogger(logs))
	store := &memoryShadowStore{}
	started := time.Now()
	summary := phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1", "u2", "u3"))
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("the phase took %v with a budget of 300 ms", elapsed)
	}
	if summary.StopReason != ShadowStopBudget || summary.Attempted != 0 || len(store.records) != 0 {
		t.Fatalf("summary = %+v, rows = %d", summary, len(store.records))
	}
	if !strings.Contains(logs.String(), "stop_reason=budget") || !strings.Contains(logs.String(), "units_not_reached=3") {
		t.Fatalf("the run line does not report the gap:\n%s", logs.String())
	}
}

// The phase context is a CHILD of the run context: when the run context is
// cancelled (a lost lease) nothing more is written, not even rows already
// classified.
func TestNothingIsWrittenAfterTheRunContextIsCancelled(t *testing.T) {
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	fake := newFakeJev(t, func(n int, _ []byte) jevReply {
		if n == 1 {
			return okReply()
		}
		// The second request cancels the run, then never answers.
		cancelRun()
		return jevReply{wait: release}
	})
	settings := shadowTestSettings()
	settings.Concurrency = 1
	phase := newTestShadowPhase(t, fake, settings, testLogger())
	observer := &recordingShadowObserver{}
	phase.SetObserver(observer)
	store := &memoryShadowStore{}
	started := time.Now()
	summary := phase.run(runCtx, store, shadowTestConfig(), shadowTestEntries(t, "u1", "u2", "u3"))
	// The budget of this phase is 30 s. A phase whose context is a child of the
	// run context ends at once when the run is cancelled, and asks nobody else.
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("the phase went on for %v after the run context was cancelled: its context is not a child of the run context", elapsed)
	}
	if fake.count() != 2 {
		t.Fatalf("requests = %d, want 2: no request may start after the cancel", fake.count())
	}
	if summary.StopReason != ShadowStopCancelled {
		t.Fatalf("stop reason = %q, want cancelled", summary.StopReason)
	}
	if summary.Attempted != 1 {
		t.Fatalf("attempted = %d: the first unit was answered before the cancel", summary.Attempted)
	}
	if store.writes != 0 || store.flushes != 0 || store.cancelledCalls != 0 || len(store.records) != 0 || len(store.attempts) != 0 {
		t.Fatalf("a store write happened after the run context was cancelled: writes %d flushes %d rows %d attempts %d",
			store.writes, store.flushes, len(store.records), len(store.attempts))
	}
	if len(observer.counts) != 1 || observer.counts[0].StopReason != ShadowStopCancelled {
		t.Fatalf("metric = %+v", observer.counts)
	}
}

// The spend cap is checked BEFORE a send: a cap under the price of one request
// sends nothing, and a cap that one request fills stops the phase there.
func TestTheSpendCapIsReservedBeforeEachSend(t *testing.T) {
	t.Run("a cap under one request sends nothing", func(t *testing.T) {
		fake := newFakeJev(t, nil)
		settings := shadowTestSettings()
		settings.MaxNanoUSD = 1
		phase := newTestShadowPhase(t, fake, settings, testLogger())
		store := &memoryShadowStore{}
		summary := phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1", "u2"))
		if fake.count() != 0 || len(store.records) != 0 || len(store.attempts) != 0 {
			t.Fatalf("requests = %d rows = %d attempts = %d, want none", fake.count(), len(store.records), len(store.attempts))
		}
		if summary.StopReason != ShadowStopCap || summary.BilledNanoUSD != 0 {
			t.Fatalf("summary = %+v", summary)
		}
	})
	t.Run("a cap that one request fills stops the phase", func(t *testing.T) {
		fake := newFakeJev(t, nil)
		settings := shadowTestSettings()
		settings.Concurrency = 1
		// One reservation fits; after the first bill (2383 tokens) a second
		// reservation does not.
		settings.MaxNanoUSD = 2383*shadowNanoUSDPerInputToken + 1000
		phase := newTestShadowPhase(t, fake, settings, testLogger())
		entries := shadowTestEntries(t, "u1", "u2", "u3")
		store := &memoryShadowStore{}
		// The cap must be above one reservation, or the first send is refused too.
		probe := &memoryShadowStore{}
		probeFake := newFakeJev(t, nil)
		probePhase := newTestShadowPhase(t, probeFake, shadowTestSettings(), testLogger())
		probePhase.run(context.Background(), probe, shadowTestConfig(), entries[:1])
		if reservation := shadowReservationNanoUSD(probeFake.bodies[0]); reservation > settings.MaxNanoUSD {
			settings.MaxNanoUSD = reservation + 1000
			phase = newTestShadowPhase(t, fake, settings, testLogger())
		}
		summary := phase.run(context.Background(), store, shadowTestConfig(), entries)
		if fake.count() != 1 || len(store.records) != 1 {
			t.Fatalf("requests = %d rows = %d, want 1 and 1", fake.count(), len(store.records))
		}
		if summary.StopReason != ShadowStopCap || summary.BilledNanoUSD != 2383*shadowNanoUSDPerInputToken {
			t.Fatalf("summary = %+v", summary)
		}
	})
}

// The ledger never lets billed + reserved pass the limit, also with
// concurrent reservations.
func TestTheLedgerNeverPassesItsLimit(t *testing.T) {
	ledger := &shadowLedger{limit: 1000}
	var wg sync.WaitGroup
	var mu sync.Mutex
	granted := 0
	for index := 0; index < 64; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ledger.reserve(100) {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if granted != 10 {
		t.Fatalf("granted %d reservations of 100 under a limit of 1000, want 10", granted)
	}
	ledger.settle(100, 40)
	if !ledger.reserve(60) || ledger.reserve(1) {
		t.Fatal("after a settle of 100 to 40 exactly 60 must be free")
	}
	if ledger.spent() != 40 {
		t.Fatalf("spent = %d", ledger.spent())
	}
}

// A missing shadow table (a migration that is not applied) stops the phase.
// It is not an error of the run: run has no error to return.
func TestAMissingShadowTableStopsThePhase(t *testing.T) {
	t.Run("on the skip-existing read", func(t *testing.T) {
		fake := newFakeJev(t, nil)
		logs := &syncBuffer{}
		phase := newTestShadowPhase(t, fake, shadowTestSettings(), debugLogger(logs))
		store := &memoryShadowStore{readErr: fmt.Errorf("query: %w", chquery.ErrShadowTableMissing)}
		summary := phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1", "u2"))
		if fake.count() != 0 || summary.StopReason != ShadowStopTableMissing {
			t.Fatalf("requests = %d, summary = %+v", fake.count(), summary)
		}
		if !strings.Contains(logs.String(), "table_missing=true") {
			t.Fatalf("the store line does not say the table is missing:\n%s", logs.String())
		}
	})
	t.Run("on the shadow write", func(t *testing.T) {
		fake := newFakeJev(t, nil)
		phase := newTestShadowPhase(t, fake, shadowTestSettings(), testLogger())
		store := &memoryShadowStore{writeErr: fmt.Errorf("send: %w", chwrite.ErrShadowTableMissing)}
		summary := phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1"))
		if summary.StopReason != ShadowStopTableMissing || summary.RowsWritten != 0 {
			t.Fatalf("summary = %+v", summary)
		}
	})
	t.Run("on the attempt flush", func(t *testing.T) {
		fake := newFakeJev(t, nil)
		phase := newTestShadowPhase(t, fake, shadowTestSettings(), testLogger())
		observer := &recordingShadowObserver{}
		phase.SetObserver(observer)
		store := &memoryShadowStore{flushResult: &chwrite.AttemptFlushResult{Failed: true, TableMissing: true, Err: chwrite.ErrShadowTableMissing}}
		summary := phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1"))
		if summary.StopReason != ShadowStopTableMissing || !summary.AttemptWriteErr {
			t.Fatalf("summary = %+v", summary)
		}
		if len(observer.counts) != 1 || observer.counts[0].AttemptWriteErrors != 1 {
			t.Fatalf("metric = %+v", observer.counts)
		}
	})
	t.Run("another store error is its own reason", func(t *testing.T) {
		fake := newFakeJev(t, nil)
		logs := &syncBuffer{}
		phase := newTestShadowPhase(t, fake, shadowTestSettings(), debugLogger(logs))
		store := &memoryShadowStore{readErr: fmt.Errorf("row value %s leaked into an error", shadowSourceSentinel)}
		summary := phase.run(context.Background(), store, shadowTestConfig(), shadowTestEntries(t, "u1"))
		if fake.count() != 0 || summary.StopReason != ShadowStopStoreError {
			t.Fatalf("requests = %d, summary = %+v", fake.count(), summary)
		}
		if strings.Contains(logs.String(), shadowSourceSentinel) {
			t.Fatalf("the text of a store error reached a log line:\n%s", logs.String())
		}
	})
}

// recordingShadowObserver keeps what the phase reports.
type recordingShadowObserver struct {
	mu     sync.Mutex
	counts []ShadowPhaseCounts
}

func (observer *recordingShadowObserver) ObserveShadowPhase(counts ShadowPhaseCounts) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.counts = append(observer.counts, counts)
}

func envLookup(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

// explodingConn is a ClickHouse connection that panics when it is used.
type explodingConn struct{}

func (explodingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	panic("planted panic of the store with " + shadowSourceSentinel)
}

func (explodingConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	panic("planted panic of the store with " + shadowSourceSentinel)
}

// A panic of the phase itself (not of one classification) ends the phase. It
// does not reach Materializer.Run, it is counted, and its value is not logged.
func TestAPanicOfThePhaseItselfDoesNotReachTheRun(t *testing.T) {
	fake := newFakeJev(t, nil)
	logs := &syncBuffer{}
	phase := newTestShadowPhase(t, fake, shadowTestSettings(), debugLogger(logs))
	observer := &recordingShadowObserver{}
	phase.SetObserver(observer)
	reader, err := chquery.NewReader(explodingConn{})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := chwrite.NewWriter(explodingConn{})
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewMaterializer(reader, writer, categorize.MockProvider{}, debugLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	materializer.SetShadow(phase)
	// Must return: a panic here would end the test binary, as it would end the
	// worker process.
	materializer.runShadow(context.Background(), shadowTestConfig(), shadowTestEntries(t, "u1"))
	if countLines(logs.String(), "investment shadow phase panicked") != 1 {
		t.Fatalf("want one ERROR line:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), shadowSourceSentinel) || strings.Contains(logs.String(), "planted panic") {
		t.Fatalf("the panic value reached a log line:\n%s", logs.String())
	}
	if len(observer.counts) != 1 || observer.counts[0].StopReason != ShadowStopPanic || observer.counts[0].PanicsRecovered != 1 {
		t.Fatalf("metric = %+v", observer.counts)
	}
	// With no phase attached the call does nothing at all.
	bare, err := NewMaterializer(reader, writer, categorize.MockProvider{}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	bare.runShadow(context.Background(), shadowTestConfig(), shadowTestEntries(t, "u1"))
	if fake.count() != 0 {
		t.Fatal("a materializer with no phase sent a request")
	}
}
