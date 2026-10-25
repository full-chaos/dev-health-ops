package syncadmin

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestBackfillOccurrenceIDReadsTheOccurrenceMarker(t *testing.T) {
	text := func(value string) *string { return &value }
	for _, tc := range []struct {
		task *string
		want string
	}{
		{nil, ""}, {text(""), ""}, {text("sync_run:x"), ""}, {text("occurrence:"), ""},
		{text("occurrence:abc"), "abc"}, {text("idem:ff;occurrence:abc"), "abc"}, {text("occurrence:abc;x"), "abc"},
	} {
		if got := backfillOccurrenceID(&backfillJob{CeleryTaskID: tc.task}); got != tc.want {
			t.Errorf("%v: %q, want %q", tc.task, got, tc.want)
		}
	}
}

func TestBackfillTaskMarkerCarriesTheKeyHashOnlyWhenKeyed(t *testing.T) {
	if got := backfillTaskMarker("occ", ""); got != "occurrence:occ" {
		t.Errorf("unkeyed: %q", got)
	}
	if got := backfillTaskMarker("occ", "ab12"); got != "idem:ab12;occurrence:occ" {
		t.Errorf("keyed: %q", got)
	}
}

func TestIdempotencyKeyIsBoundedAndScopedToOrgAndConfig(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want bool
	}{
		{"", true}, {"a", true}, {"req-1.2_3:4", true}, {strings.Repeat("k", 128), true},
		{strings.Repeat("k", 129), false}, {"has space", false}, {"semi;colon", false}, {"line\nbreak", false},
	} {
		if _, ok := parseIdempotencyKey(tc.raw); ok != tc.want {
			t.Errorf("%q: ok=%v, want %v", tc.raw, ok, tc.want)
		}
	}
	config := uuid.New()
	base := idempotencyHash("org-a", config, "k")
	if base != idempotencyHash("org-a", config, "k") {
		t.Error("the hash is not stable")
	}
	for name, other := range map[string]string{
		"org": idempotencyHash("org-b", config, "k"), "config": idempotencyHash("org-a", uuid.New(), "k"),
		"key": idempotencyHash("org-a", config, "j"),
	} {
		if other == base {
			t.Errorf("the hash ignores the %s", name)
		}
	}
	if len(base) != 32 || strings.ContainsAny(base, ";: ") {
		t.Errorf("hash %q is not 32 plain hex characters", base)
	}
}

func TestBackfillScaleRefusalIsOnlyForARunTheSchedulerRefuses(t *testing.T) {
	// A run needs at least one unit per source per 14-day window (the widest
	// planner chunk), so only a run over that floor is refused at submit.
	for _, tc := range []struct {
		name               string
		days, sources, cap int64
		refused            bool
	}{
		{"no cap set", 100000, 5000, 0, false},
		{"under the cap", 28, 10, 100, false},
		{"exactly at the cap", 14 * 10, 10, 100, false},
		{"one window over", 14*10 + 1, 10, 100, true},
		{"many sources", 14, 101, 100, true},
		{"no sources", 36500, 0, 100, false},
	} {
		err := checkBackfillScale(tc.days, tc.sources, tc.cap)
		if (err != nil) != tc.refused {
			t.Errorf("%s: err=%v, refused want %v", tc.name, err, tc.refused)
		}
		if err != nil {
			var raised *answer
			if !errors.As(err, &raised) || raised.status != http.StatusUnprocessableEntity {
				t.Errorf("%s: %v is not a 422", tc.name, err)
			}
		}
	}
}

// occReader serves a job whose task names a pending occurrence and records
// what the occurrence read was asked.
type occReader struct {
	faultReader
	task          string
	jobOrg        string
	org, occ      string
	plan          occurrencePlan
	occurrenceHit int
}

func (o *occReader) backfillJobByID(context.Context, string, uuid.UUID) (*backfillJob, error) {
	return &backfillJob{ID: uuid.New(), OrgID: o.jobOrg, CeleryTaskID: &o.task}, nil
}

func (o *occReader) occurrencePlanOf(_ context.Context, org, occurrence string) (occurrencePlan, error) {
	o.occurrenceHit++
	o.org, o.occ = org, occurrence
	return o.plan, nil
}

func TestPendingOccurrenceJobReadsOneOrgScopedOccurrence(t *testing.T) {
	org := uuid.NewString()
	reader := &occReader{task: "idem:ab;occurrence:occ-1", jobOrg: org}
	h := &handlers{store: reader, logger: quiet()}
	request := newRequest(t, "/x", org)
	request.SetPathValue("job_id", uuid.NewString())
	recorder := record(h.getBackfillJob, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("%d %s", recorder.Code, recorder.Body.String())
	}
	if reader.occurrenceHit != 1 || reader.org != org || reader.occ != "occ-1" {
		t.Fatalf("occurrence read: hits=%d org=%q occ=%q, want one read for the caller's org and occ-1", reader.occurrenceHit, reader.org, reader.occ)
	}
	if reader.calls["syncRunByID"] != 0 {
		t.Error("an unplanned occurrence read a sync run")
	}

	planned := faultRunID
	reader = &occReader{task: "occurrence:occ-2", jobOrg: org, plan: occurrencePlan{RunID: &planned}}
	h = &handlers{store: reader, logger: quiet()}
	recorder = record(h.getBackfillJob, request)
	if recorder.Code != http.StatusOK || reader.calls["syncRunByID"] != 1 {
		t.Fatalf("planned occurrence: %d, syncRunByID calls %d", recorder.Code, reader.calls["syncRunByID"])
	}
}

func TestSyncRunMarkerNeverReadsAnOccurrence(t *testing.T) {
	reader := &occReader{task: "sync_run:" + faultRunID.String()}
	h := &handlers{store: reader, logger: quiet()}
	request := newRequest(t, "/x", uuid.NewString())
	request.SetPathValue("job_id", uuid.NewString())
	record(h.getBackfillJob, request)
	if reader.occurrenceHit != 0 {
		t.Errorf("a sync_run job read %d occurrences", reader.occurrenceHit)
	}
}

func TestAcceptedBackfillBodyNamesTheJobAndTheOccurrenceNotARun(t *testing.T) {
	config, job := uuid.New(), uuid.New()
	window := backfillWindow{LegacySince: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), LegacyBefore: time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)}
	body := marshal(t, acceptedBackfillBody(config, job, "occ-9", window))
	for _, want := range []string{`"status":"accepted"`, `"config_id":"` + config.String() + `"`, `"backfill_job_id":"` + job.String() + `"`,
		`"occurrence_id":"occ-9"`, `"task_id":"occurrence:occ-9"`, `"mode":"fanout"`, `"since":"2026-09-01"`, `"before":"2026-09-30"`} {
		if !strings.Contains(body, want) {
			t.Errorf("%s lacks %s", body, want)
		}
	}
	if strings.Contains(body, "sync_run_id") {
		t.Errorf("%s names a run the submit did not wait for", body)
	}
}

func TestQuarantinedOccurrenceJobReadsFailedWithItsCode(t *testing.T) {
	org := uuid.NewString()
	reader := &occReader{task: "occurrence:occ-q", jobOrg: org, plan: occurrencePlan{ErrorCode: "invalid_plan"}}
	h := &handlers{store: reader, logger: quiet()}
	request := newRequest(t, "/x", org)
	request.SetPathValue("job_id", uuid.NewString())
	recorder := record(h.getBackfillJob, request)
	body := recorder.Body.String()
	if recorder.Code != http.StatusOK || !strings.Contains(body, `"status":"failed"`) ||
		!strings.Contains(body, "scheduled sync occurrence quarantined: invalid_plan") {
		t.Fatalf("%d %s, want a failed job naming the quarantine code", recorder.Code, body)
	}
	if reader.calls["syncRunByID"] != 0 || reader.org != org {
		t.Errorf("a quarantined occurrence read a run (%d) or the wrong org %q", reader.calls["syncRunByID"], reader.org)
	}
}

func TestExplicitlyEmptyScopeIsNotScaleChecked(t *testing.T) {
	// Two sources, 28 days, cap 3 is over the floor (2 x 2 = 4) ...
	if checkBackfillScale(28, 2, 3) == nil {
		t.Fatal("the floor case must be refused")
	}
	// ... but an explicitly empty dataset or source selector plans no unit.
	for name, window := range map[string]backfillWindow{
		"empty datasets": {Structured: true, DatasetKeysSet: true},
		"empty sources":  {Structured: true, SourceIDsSet: true},
	} {
		if !scaleCheckApplies(&window) {
			continue
		}
		t.Errorf("%s: the scale check applies", name)
	}
	if !scaleCheckApplies(&backfillWindow{Structured: true, DatasetKeysSet: true, DatasetKeys: []string{"commits"}}) ||
		!scaleCheckApplies(&backfillWindow{}) {
		t.Error("a named or legacy scope is checked")
	}
}
