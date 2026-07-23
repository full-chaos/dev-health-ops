package sync

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"
)

func at(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return parsed
}

func pointer(value time.Time) *time.Time { return &value }

func TestNextOccurrencePythonGoldenVectors(t *testing.T) {
	for _, test := range []struct {
		name, expression, timezone string
		base, want                 time.Time
		fallback                   bool
	}{
		{"utc", "0 0 * * *", "UTC", at("2026-06-26T12:00:00Z"), at("2026-06-27T00:00:00Z"), false},
		{"empty timezone", "0 0 * * *", "", at("2026-06-26T12:00:00Z"), at("2026-06-27T00:00:00Z"), false},
		{"whitespace timezone fallback", "0 0 * * *", " ", at("2026-06-26T12:00:00Z"), at("2026-06-27T00:00:00Z"), true},
		{"local pseudo-zone fallback", "0 0 * * *", "Local", at("2026-06-26T12:00:00Z"), at("2026-06-27T00:00:00Z"), true},
		{"local wall clock", "0 0 * * *", "America/Los_Angeles", at("2026-06-26T12:00:00Z"), at("2026-06-27T07:00:00Z"), false},
		{"unknown timezone", "0 0 * * *", "Not/AZone", at("2026-06-26T12:00:00Z"), at("2026-06-27T00:00:00Z"), true},
		{"fall fold zero", "30 1 * * *", "America/Los_Angeles", at("2026-11-01T06:00:00Z"), at("2026-11-01T08:30:00Z"), false},
		{"spring gap", "30 2 * * *", "America/Los_Angeles", at("2026-03-08T09:00:00Z"), at("2026-03-08T10:30:00Z"), false},
		{"day-of-month weekday or", "0 0 13 * 5", "UTC", at("2026-02-12T12:00:00Z"), at("2026-02-13T00:00:00Z"), false},
		{"sunday seven", "0 0 * * 7", "UTC", at("2026-01-02T12:00:00Z"), at("2026-01-04T00:00:00Z"), false},
		{"leap day", "0 0 29 2 *", "UTC", at("2026-03-01T12:00:00Z"), at("2028-02-29T00:00:00Z"), false},
		{"value step expands through maximum", "5/15 * * * *", "UTC", at("2026-01-01T00:05:00Z"), at("2026-01-01T00:20:00Z"), false},
		{"last day of month", "0 0 L * *", "UTC", at("2026-01-10T00:00:00Z"), at("2026-01-31T00:00:00Z"), false},
		{"last day mixed after literal", "0 0 1,L * *", "UTC", at("2026-01-02T00:00:00Z"), at("2026-01-31T00:00:00Z"), false},
		{"literal after last day", "0 0 1,L * *", "UTC", at("2026-01-31T00:00:00Z"), at("2026-02-01T00:00:00Z"), false},
		{"last day mixed before literal", "0 0 L,15 * *", "UTC", at("2026-01-15T00:00:00Z"), at("2026-01-31T00:00:00Z"), false},
		{"nearest weekday", "0 0 15W * *", "UTC", at("2026-02-01T00:00:00Z"), at("2026-02-16T00:00:00Z"), false},
		{"nearest weekday clamped", "0 0 31W * *", "UTC", at("2026-04-01T00:00:00Z"), at("2026-04-30T00:00:00Z"), false},
		{"nth named weekday", "0 0 * * MON#1", "UTC", at("2026-01-10T00:00:00Z"), at("2026-02-02T00:00:00Z"), false},
		{"fifth numeric weekday", "0 0 * * 5#5", "UTC", at("2026-01-01T00:00:00Z"), at("2026-01-30T00:00:00Z"), false},
		{"last weekday", "0 0 * * L5", "UTC", at("2026-01-01T00:00:00Z"), at("2026-01-30T00:00:00Z"), false},
		{"last sunday zero", "0 0 * * L0", "UTC", at("2026-01-01T00:00:00Z"), at("2026-01-25T00:00:00Z"), false},
		{"last sunday seven", "0 0 * * L7", "UTC", at("2026-01-01T00:00:00Z"), at("2026-01-25T00:00:00Z"), false},
		{"multiple nth weekdays", "0 0 * * MON#1,FRI#2", "UTC", at("2026-01-05T00:00:00Z"), at("2026-01-09T00:00:00Z"), false},
		{"multiple last weekdays", "0 0 * * L1,L5", "UTC", at("2026-01-26T00:00:00Z"), at("2026-01-30T00:00:00Z"), false},
		{"mixed last and nth weekdays", "0 0 * * L1,5#2", "UTC", at("2026-01-09T00:00:00Z"), at("2026-01-26T00:00:00Z"), false},
		{"question-mark wildcards", "0 0 ? * ?", "UTC", at("2026-01-01T00:00:00Z"), at("2026-01-02T00:00:00Z"), false},
		{"wrapped month range", "0 0 * NOV-FEB *", "UTC", at("2026-03-01T00:00:00Z"), at("2026-11-01T00:00:00Z"), false},
		{"wrapped weekday range", "0 0 * * FRI-MON", "UTC", at("2026-01-05T00:00:00Z"), at("2026-01-09T00:00:00Z"), false},
		{"wrapped named weekday step dedupes sunday", "0 0 * * FRI-MON/2", "UTC", at("2026-01-04T00:00:00Z"), at("2026-01-09T00:00:00Z"), false},
		{"wrapped numeric weekday step dedupes sunday", "0 0 * * 6-0/2", "UTC", at("2026-01-03T00:00:00Z"), at("2026-01-10T00:00:00Z"), false},
		{"named month step", "0 0 * JAN/2 *", "UTC", at("2026-02-01T00:00:00Z"), at("2026-03-01T00:00:00Z"), false},
		{"named weekday step", "0 0 * * MON/2", "UTC", at("2026-01-05T00:00:00Z"), at("2026-01-07T00:00:00Z"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, fallback, err := NextOccurrence(test.expression, test.base, test.timezone)
			if err != nil || !got.Equal(test.want) || fallback != test.fallback {
				t.Fatalf("NextOccurrence() = %s fallback=%v err=%v, want %s fallback=%v", got, fallback, err, test.want, test.fallback)
			}
		})
	}
}

func TestNextOccurrenceRejectsCroniterInvalidFiveFieldFormsAndExtendedFields(t *testing.T) {
	for _, expression := range []string{
		"0 0 L-2 * *",
		"0 0 LW * *",
		"0 0 1,15W * *",
		"0 0 L,15W * *",
		"0 0 ?,L * *",
		"0 0 * * 5L",
		"0 0 * * MONL",
		"0 0 * * MON#6",
		"0 0 * * 1#1,2",
		"0 0 * * L1,2",
		"0 0 * * L1,MON-FRI",
		"? 0 * * *",
		"0 ? * * *",
		"H * * * *",
		"0 0 * * * 2026",
		"0 0 * * * 0 2026",
	} {
		t.Run(expression, func(t *testing.T) {
			if _, _, err := NextOccurrence(expression, at("2026-01-01T00:00:00Z"), "UTC"); err == nil {
				t.Fatalf("NextOccurrence(%q) accepted invalid expression", expression)
			}
		})
	}
}

func TestRandomCronIsExplicitlyUnsupported(t *testing.T) {
	for _, expression := range []string{
		"R * * * *",
		"r/5 * * * *",
		"R(10-20) * * * *",
		"R(10-20)/5 * * * *",
		"0 R * * *",
		"0 0 R * *",
		"0 0 * R *",
		"0 0 * * R",
	} {
		t.Run(expression, func(t *testing.T) {
			if _, _, err := NextOccurrence(expression, at("2026-01-01T00:00:00Z"), "UTC"); !errors.Is(err, ErrUnsupportedRandomCron) {
				t.Fatalf("NextOccurrence(%q) err=%v", expression, err)
			}
			got := Evaluate(Candidate{
				ConfigID:     "random",
				Active:       true,
				ScheduleCron: expression,
				CreatedAt:    at("2026-01-01T00:00:00Z"),
			}, at("2026-01-02T00:00:00Z"))
			if got.Decision != DecisionUnsupportedCron || got.TimingEligible ||
				got.CronGrammar != CronGrammarVersion {
				t.Fatalf("Evaluate(%q) = %#v", expression, got)
			}
		})
	}

	got := Evaluate(Candidate{
		ConfigID:     "hashed",
		Active:       true,
		ScheduleCron: "H * * * *",
		CreatedAt:    at("2026-01-01T00:00:00Z"),
	}, at("2026-01-02T00:00:00Z"))
	if got.Decision != DecisionInvalidCron {
		t.Fatalf("hashed cron classification = %#v", got)
	}
}

func TestEvaluateDueManualAndRunningMarkers(t *testing.T) {
	now := at("2026-01-01T12:00:00Z")
	base := at("2026-01-01T10:00:00Z")
	fresh := now.Add(-staleRunningTTL)
	stale := fresh.Add(-time.Nanosecond)
	future := now.Add(time.Hour)
	for _, test := range []struct {
		name                string
		candidate           Candidate
		decision            Decision
		due, timingEligible bool
		running             RunningMarkerState
	}{
		{"not due", Candidate{ConfigID: "a", Active: true, ScheduleCron: "0 13 * * *", CreatedAt: base}, DecisionNotDue, false, false, RunningNotSet},
		{"due from last sync", Candidate{ConfigID: "b", Active: true, ScheduleCron: "0 * * * *", CreatedAt: now, LastSyncAt: pointer(base)}, DecisionScheduleDue, true, true, RunningNotSet},
		{"manual", Candidate{ConfigID: "c", Active: true, CreatedAt: base}, DecisionManual, false, false, RunningNotSet},
		{"fresh running exact threshold", Candidate{ConfigID: "d", Active: true, ScheduleCron: "0 * * * *", CreatedAt: now, LastSyncAt: pointer(base), Job: &Job{Status: activeJobStatus, IsRunning: true, LastRunAt: pointer(fresh)}}, DecisionFreshRunning, false, false, RunningFresh},
		{"stale running", Candidate{ConfigID: "e", Active: true, ScheduleCron: "0 * * * *", CreatedAt: now, LastSyncAt: pointer(base), Job: &Job{ScheduleCron: "0 * * * *", Status: activeJobStatus, IsRunning: true, LastRunAt: pointer(stale)}}, DecisionScheduleDue, true, true, RunningStale},
		{"future persisted run precedes malformed cron", Candidate{ConfigID: "f", Active: true, ScheduleCron: "0 * * * *", CreatedAt: base, Job: &Job{ScheduleCron: "malformed", Timezone: "UTC", Status: activeJobStatus, NextRunAt: pointer(future)}}, DecisionNextRunGate, false, false, RunningNotSet},
		{"future persisted run precedes cron not due", Candidate{ConfigID: "g", Active: true, ScheduleCron: "0 * * * *", CreatedAt: base, Job: &Job{ScheduleCron: "0 13 * * *", Timezone: "UTC", Status: activeJobStatus, NextRunAt: pointer(future)}}, DecisionNextRunGate, false, false, RunningNotSet},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := Evaluate(test.candidate, now)
			if got.Decision != test.decision || got.Due != test.due || got.TimingEligible != test.timingEligible || got.RunningMarker != test.running {
				t.Fatalf("Evaluate() = %#v", got)
			}
			if got.EligibilityScope != ScheduleMarkerEvaluationScope {
				t.Fatalf("eligibility scope = %q", got.EligibilityScope)
			}
			if got.CronGrammar != CronGrammarVersion {
				t.Fatalf("cron grammar = %q", got.CronGrammar)
			}
		})
	}
}

func TestEvaluatePreservesPythonTimezoneFallbackSemantics(t *testing.T) {
	base := at("2026-01-01T10:00:00Z")
	now := at("2026-01-01T12:00:00Z")
	for _, test := range []struct {
		name, timezone string
		fallback       bool
	}{
		{"empty is UTC without fallback", "", false},
		{"whitespace is invalid and falls back", " ", true},
		{"local pseudo-zone is invalid and falls back", "Local", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := Evaluate(Candidate{
				ConfigID:     "timezone",
				Active:       true,
				ScheduleCron: "0 * * * *",
				ScheduleTZ:   test.timezone,
				CreatedAt:    base,
			}, now)
			if got.Timezone != "UTC" || got.TimezoneFallback != test.fallback ||
				got.Decision != DecisionScheduleDue || !got.TimingEligible {
				t.Fatalf("Evaluate() = %#v", got)
			}
		})
	}
}

func TestSnapshotSortsBoundsAndDigestsDeterministically(t *testing.T) {
	observed := at("2026-01-01T12:00:00Z")
	candidates := []Candidate{
		{ConfigID: "b", Active: true, ScheduleCron: "0 * * * *", CreatedAt: at("2026-01-01T10:00:00Z")},
		{ConfigID: "a", Active: true, ScheduleCron: "0 * * * *", CreatedAt: at("2026-01-01T11:00:00Z")},
		{ConfigID: "c", Active: true, ScheduleCron: "0 * * * *", CreatedAt: at("2026-01-01T11:00:00Z")},
	}
	snapshot, err := BuildSnapshot(observed, 2, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Truncated || len(snapshot.Candidates) != 2 || snapshot.Candidates[0].Candidate.ConfigID != "a" || snapshot.Candidates[1].Candidate.ConfigID != "b" {
		t.Fatalf("snapshot order = %#v", snapshot)
	}
	if snapshot.DigestVersion != TimingDigestVersion || snapshot.EvaluationVersion != EvaluationVersion ||
		snapshot.EligibilityScope != ScheduleMarkerEvaluationScope ||
		snapshot.CronGrammar != CronGrammarVersion ||
		!strings.HasPrefix(snapshot.CandidateDigest, "sha256:") || len(snapshot.CandidateDigest) != len("sha256:")+64 {
		t.Fatalf("snapshot digest = %#v", snapshot)
	}
	if snapshot.CandidateDigest != "sha256:83f190e6252b829ece384aff328d489a1d09883d5c171348bdc280b39ba1c191" {
		t.Fatalf("candidate digest = %s", snapshot.CandidateDigest)
	}
	again, err := BuildSnapshot(observed, 2, []Candidate{candidates[2], candidates[0], candidates[1]})
	if err != nil || snapshot.CandidateDigest != again.CandidateDigest {
		t.Fatalf("deterministic digest first=%s second=%s err=%v", snapshot.CandidateDigest, again.CandidateDigest, err)
	}
	if _, err := BuildSnapshot(observed, 0, nil); err == nil {
		t.Fatal("zero limit accepted")
	}
	if _, err := BuildSnapshot(observed, 101, nil); err == nil {
		t.Fatal("oversized limit accepted")
	}
}

type cancelAfterChecksContext struct {
	context.Context
	checks   int
	cancelOn int
}

func (ctx *cancelAfterChecksContext) Err() error {
	ctx.checks++
	if ctx.checks >= ctx.cancelOn {
		return context.Canceled
	}
	return nil
}

func TestCronEvaluationHonorsCancellationDuringBoundedSearch(t *testing.T) {
	ctx := &cancelAfterChecksContext{Context: context.Background(), cancelOn: 3}
	_, _, err := nextOccurrenceContext(
		ctx,
		"0 0 31 2 *",
		at("2026-03-01T00:00:00Z"),
		"UTC",
	)
	if !errors.Is(err, context.Canceled) || ctx.checks < ctx.cancelOn {
		t.Fatalf("nextOccurrenceContext() err=%v checks=%d", err, ctx.checks)
	}
}

type cancelingCandidateRows struct {
	yielded bool
	cancel  context.CancelFunc
}

func (rows *cancelingCandidateRows) Next() bool {
	if rows.yielded {
		return false
	}
	rows.yielded = true
	return true
}

func (rows *cancelingCandidateRows) Scan(...any) error {
	rows.cancel()
	return nil
}

func (*cancelingCandidateRows) Err() error { return nil }

func TestRepositorySnapshotContextBoundaries(t *testing.T) {
	if _, err := (&Repository{}).Snapshot(nil, time.Now(), 1); err == nil || !strings.Contains(err.Error(), "context") {
		t.Fatalf("Snapshot(nil) err=%v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	rows := &cancelingCandidateRows{cancel: cancel}
	if _, err := readCandidates(ctx, rows, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("readCandidates() err=%v", err)
	}
}

func TestRepositoryStatementIsBoundedReadOnlyExactJoin(t *testing.T) {
	statement := strings.ToUpper(schedulerSnapshotSQL)
	for _, want := range []string{
		"SELECT", "FROM PUBLIC.SYNC_CONFIGURATIONS AS CONFIG", "LEFT JOIN PUBLIC.SCHEDULED_JOBS AS JOB",
		"JOB.ORG_ID = CONFIG.ORG_ID", "JOB.SYNC_CONFIG_ID = CONFIG.ID", "JOB.JOB_TYPE = 'SYNC'",
		"WHERE CONFIG.IS_ACTIVE = TRUE", "ORDER BY CONFIG.ID", "LIMIT $1",
	} {
		if !strings.Contains(statement, want) {
			t.Fatalf("query missing %q: %s", want, schedulerSnapshotSQL)
		}
	}
	for _, forbidden := range []string{"INSERT", "UPDATE", "DELETE", "FOR UPDATE", "LOCK", "ADVISORY"} {
		if regexp.MustCompile(`\b` + forbidden + `\b`).MatchString(statement) {
			t.Fatalf("read query contains %q", forbidden)
		}
	}
}
