package joboutbox

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	undeliveredOutboxA  = "11111111-1111-4111-8111-111111111111"
	undeliveredOutboxB  = "22222222-2222-4222-8222-222222222222"
	undeliveredOutboxC  = "33333333-3333-4333-8333-333333333333"
	undeliveredRequestA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	undeliveredRequestB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
)

// undeliveredRepairWith builds a repair that runs ONLY the undelivered sweep:
// no shapes, so every queue query is either the survey, the dead-mark, or the
// trailing retired-kind observation.
func undeliveredRepairWith(
	queryQueue func(context.Context, string, ...any) (pgx.Rows, error),
	queryDomain func(context.Context, string, ...any) (pgx.Rows, error),
) *StrandRepair {
	return &StrandRepair{
		beginQueue:         func(context.Context) (pgx.Tx, error) { return nil, errors.New("unused") },
		queryQueue:         queryQueue,
		queryDomain:        queryDomain,
		client:             riverDeleteAdapter{},
		undeliveredCeiling: DefaultUndeliveredCeiling,
	}
}

func TestNewStrandRepairEnablesTheUndeliveredSweepByDefault(t *testing.T) {
	// The sweep is the only path that reaches a fenced row whose prerequisite
	// can never complete; a constructor that left it off would ship the leak.
	if DefaultUndeliveredCeiling <= 0 {
		t.Fatalf("DefaultUndeliveredCeiling = %s, want a positive ceiling", DefaultUndeliveredCeiling)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://unused@127.0.0.1:1/unused")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	repair, err := NewStrandRepair(pool, pool, "river")
	if err != nil {
		t.Fatal(err)
	}
	if repair.undeliveredCeiling != DefaultUndeliveredCeiling {
		t.Fatalf("NewStrandRepair ceiling = %s, want %s; the undelivered sweep would not run",
			repair.undeliveredCeiling, DefaultUndeliveredCeiling)
	}
}

func TestUndeliveredSweepCancelsTheRequestBeforeTheOutboxRowGoesDead(t *testing.T) {
	var order []string
	now := time.Date(2026, time.September, 12, 12, 0, 0, 0, time.UTC)
	queryQueue := func(_ context.Context, statement string, args ...any) (pgx.Rows, error) {
		switch statement {
		case undeliveredSurveySQL:
			order = append(order, "survey")
			cutoff, _ := args[0].(time.Time)
			if !cutoff.Equal(now.Add(-DefaultUndeliveredCeiling)) {
				t.Fatalf("survey cutoff = %s, want now - ceiling", cutoff)
			}
			return &fakeStrandSurveyRows{rows: [][]any{
				{undeliveredOutboxA, "workgraph.build", undeliveredRequestA, UndeliveredReasonPrerequisiteFailed, int64(4)},
				{undeliveredOutboxB, "metrics.remaining.membership_backfill", "", UndeliveredReasonPrerequisiteExpired, int64(4)},
				{undeliveredOutboxC, "investment.materialize", undeliveredRequestB, "blocked", int64(4)},
			}}, nil
		case undeliveredMarkDeadSQL:
			order = append(order, "mark_dead")
			ids, _ := args[1].([]string)
			reasons, _ := args[2].([]string)
			details, _ := args[3].([]string)
			if len(ids) != 2 || ids[0] != undeliveredOutboxA || ids[1] != undeliveredOutboxB ||
				reasons[0] != UndeliveredReasonPrerequisiteFailed || reasons[1] != UndeliveredReasonPrerequisiteExpired ||
				details[0] == "" || details[1] == "" {
				t.Fatalf("mark dead args = %v %v %v", ids, reasons, details)
			}
			return &fakeStrandSurveyRows{rows: [][]any{{undeliveredOutboxA}, {undeliveredOutboxB}}}, nil
		case retiredKindsSQL:
			return &fakeStrandSurveyRows{}, nil
		default:
			t.Fatalf("unexpected queue statement: %s", statement)
			return nil, nil
		}
	}
	queryDomain := func(_ context.Context, statement string, args ...any) (pgx.Rows, error) {
		if statement != undeliveredCancelRequestsSQL {
			t.Fatalf("unexpected domain statement: %s", statement)
		}
		order = append(order, "cancel")
		requests, _ := args[0].([]string)
		outboxes, _ := args[1].([]string)
		if len(requests) != 1 || requests[0] != undeliveredRequestA || outboxes[0] != undeliveredOutboxA {
			t.Fatalf("cancel args = %v %v, want only the work-graph candidate", requests, outboxes)
		}
		return &fakeStrandSurveyRows{rows: [][]any{{undeliveredOutboxA}}}, nil
	}

	result, err := undeliveredRepairWith(queryQueue, queryDomain).Step(context.Background(), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(order, ",") != "survey,cancel,mark_dead" {
		t.Fatalf("write order = %v, want the request canceled before the outbox row goes dead", order)
	}
	if result.UndeliveredBlocked != 4 || result.UndeliveredRaceLost != 0 {
		t.Fatalf("result = %+v, want blocked=4 race_lost=0", result)
	}
	want := []UndeliveredResolution{
		{OutboxID: undeliveredOutboxA, JobKind: "workgraph.build", RequestID: undeliveredRequestA,
			Reason: UndeliveredReasonPrerequisiteFailed, RequestCanceled: true, OutboxDead: true},
		{OutboxID: undeliveredOutboxB, JobKind: "metrics.remaining.membership_backfill",
			Reason: UndeliveredReasonPrerequisiteExpired, OutboxDead: true},
	}
	if len(result.UndeliveredResolutions) != len(want) {
		t.Fatalf("resolutions = %+v, want %+v", result.UndeliveredResolutions, want)
	}
	for i := range want {
		if result.UndeliveredResolutions[i] != want[i] {
			t.Fatalf("resolution[%d] = %+v, want %+v", i, result.UndeliveredResolutions[i], want[i])
		}
	}
}

func TestUndeliveredSweepRoutesEachReasonToItsOwnWrites(t *testing.T) {
	// request_terminal: the request already is terminal, so only the outbox
	// row moves. delivery_dead: the outbox row already is dead, so only the
	// request moves.
	domainCalls, deadCalls := 0, 0
	queryQueue := func(_ context.Context, statement string, args ...any) (pgx.Rows, error) {
		switch statement {
		case undeliveredSurveySQL:
			return &fakeStrandSurveyRows{rows: [][]any{
				{undeliveredOutboxA, "investment.materialize", undeliveredRequestA, UndeliveredReasonRequestTerminal, int64(0)},
				{undeliveredOutboxB, "workgraph.build", undeliveredRequestB, UndeliveredReasonDeliveryDead, int64(0)},
			}}, nil
		case undeliveredMarkDeadSQL:
			deadCalls++
			ids, _ := args[1].([]string)
			if len(ids) != 1 || ids[0] != undeliveredOutboxA {
				t.Fatalf("mark dead ids = %v, want only the request_terminal row", ids)
			}
			return &fakeStrandSurveyRows{rows: [][]any{{undeliveredOutboxA}}}, nil
		case retiredKindsSQL:
			return &fakeStrandSurveyRows{}, nil
		}
		t.Fatalf("unexpected queue statement: %s", statement)
		return nil, nil
	}
	queryDomain := func(_ context.Context, statement string, args ...any) (pgx.Rows, error) {
		domainCalls++
		requests, _ := args[0].([]string)
		if len(requests) != 1 || requests[0] != undeliveredRequestB {
			t.Fatalf("cancel requests = %v, want only the delivery_dead request", requests)
		}
		return &fakeStrandSurveyRows{rows: [][]any{{undeliveredOutboxB}}}, nil
	}
	result, err := undeliveredRepairWith(queryQueue, queryDomain).Step(context.Background(), time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if domainCalls != 1 || deadCalls != 1 || len(result.UndeliveredResolutions) != 2 {
		t.Fatalf("domain=%d dead=%d result=%+v", domainCalls, deadCalls, result)
	}
	if !result.UndeliveredResolutions[0].OutboxDead || result.UndeliveredResolutions[0].RequestCanceled ||
		result.UndeliveredResolutions[1].OutboxDead || !result.UndeliveredResolutions[1].RequestCanceled {
		t.Fatalf("resolutions = %+v", result.UndeliveredResolutions)
	}
}

func TestUndeliveredSweepCountsACandidateBothWritesRefusedAsARaceLoss(t *testing.T) {
	queryQueue := func(_ context.Context, statement string, _ ...any) (pgx.Rows, error) {
		switch statement {
		case undeliveredSurveySQL:
			return &fakeStrandSurveyRows{rows: [][]any{
				{undeliveredOutboxA, "workgraph.build", undeliveredRequestA, UndeliveredReasonPrerequisiteExpired, int64(0)},
			}}, nil
		case undeliveredMarkDeadSQL, retiredKindsSQL:
			return &fakeStrandSurveyRows{}, nil
		}
		t.Fatalf("unexpected queue statement: %s", statement)
		return nil, nil
	}
	queryDomain := func(context.Context, string, ...any) (pgx.Rows, error) {
		return &fakeStrandSurveyRows{}, nil
	}
	result, err := undeliveredRepairWith(queryQueue, queryDomain).Step(context.Background(), time.Now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.UndeliveredRaceLost != 1 || result.UndeliveredResolutions != nil {
		t.Fatalf("result = %+v, want one race loss and no resolution", result)
	}
}

func TestUndeliveredSweepKeepsCommittedCancelsWhenTheDeadMarkFails(t *testing.T) {
	queryQueue := func(_ context.Context, statement string, _ ...any) (pgx.Rows, error) {
		switch statement {
		case undeliveredSurveySQL:
			return &fakeStrandSurveyRows{rows: [][]any{
				{undeliveredOutboxA, "workgraph.build", undeliveredRequestA, UndeliveredReasonPrerequisiteFailed, int64(2)},
			}}, nil
		case undeliveredMarkDeadSQL:
			return nil, errors.New("queue pool unavailable")
		}
		t.Fatalf("unexpected queue statement: %s", statement)
		return nil, nil
	}
	queryDomain := func(context.Context, string, ...any) (pgx.Rows, error) {
		return &fakeStrandSurveyRows{rows: [][]any{{undeliveredOutboxA}}}, nil
	}
	result, err := undeliveredRepairWith(queryQueue, queryDomain).Step(context.Background(), time.Now(), 10)
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "undelivered: mark dead") {
		t.Fatalf("Step() error = %v, want the named mark-dead stage", err)
	}
	if len(result.UndeliveredResolutions) != 1 || !result.UndeliveredResolutions[0].RequestCanceled ||
		result.UndeliveredResolutions[0].OutboxDead || result.UndeliveredBlocked != 2 {
		t.Fatalf("result = %+v, want the committed cancel kept despite the mark-dead error", result)
	}
}

func TestUndeliveredSweepRefusesAReasonOutsideItsVocabulary(t *testing.T) {
	queryQueue := func(_ context.Context, statement string, _ ...any) (pgx.Rows, error) {
		if statement != undeliveredSurveySQL {
			t.Fatalf("a write ran after an unclassified survey row: %s", statement)
		}
		return &fakeStrandSurveyRows{rows: [][]any{
			{undeliveredOutboxA, "workgraph.build", undeliveredRequestA, "invented_reason", int64(0)},
		}}, nil
	}
	queryDomain := func(context.Context, string, ...any) (pgx.Rows, error) {
		t.Fatal("a domain write ran after an unclassified survey row")
		return nil, nil
	}
	_, err := undeliveredRepairWith(queryQueue, queryDomain).Step(context.Background(), time.Now(), 10)
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), undeliveredOutboxA) ||
		!strings.Contains(err.Error(), "undelivered: survey") {
		t.Fatalf("Step() error = %v, want ErrUnavailable naming the stage and the outbox row", err)
	}
}

func TestUndeliveredVocabularyHasADetailForEveryOutboxReason(t *testing.T) {
	for _, reason := range []string{
		UndeliveredReasonPrerequisiteFailed,
		UndeliveredReasonPrerequisiteExpired,
		UndeliveredReasonRequestTerminal,
	} {
		detail := undeliveredReasonDetail[reason]
		// last_error_code is varchar(64), last_error_detail varchar(256).
		if detail == "" || len(detail) > 256 || len(reason) > 64 {
			t.Fatalf("reason %q detail %q violates the outbox error columns", reason, detail)
		}
	}
}

func TestReconcilerLoopLogsAndCountsEveryUndeliveredResolutionAndDeadDelivery(t *testing.T) {
	clock := &testReconcilerClock{now: time.Date(2026, time.September, 12, 12, 0, 0, 0, time.UTC)}
	loop, _ := newTestReconcilerLoop(t, loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		return StepResult{
			UndeliveredResolutions: []UndeliveredResolution{
				{OutboxID: undeliveredOutboxA, JobKind: "workgraph.build", RequestID: undeliveredRequestA,
					Reason: UndeliveredReasonPrerequisiteFailed, RequestCanceled: true, OutboxDead: true},
				{OutboxID: undeliveredOutboxB, JobKind: "workgraph.build", RequestID: undeliveredRequestB,
					Reason: UndeliveredReasonDeliveryDead, RequestCanceled: true},
			},
			UndeliveredBlocked:  7,
			UndeliveredRaceLost: 1,
			Dead:                1,
			DeadDeliveries: []DeadDelivery{
				{OutboxID: undeliveredOutboxC, JobKind: "workgraph.build", Reason: "river_insert_failed", Attempts: 10},
			},
		}, nil
	}), clock)
	var logs bytes.Buffer
	loop.config.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	if err := loop.step(context.Background(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"worker_outbox_reconciler_undelivered_outbox_dead_total 1",
		"worker_outbox_reconciler_undelivered_requests_canceled_total 2",
		"worker_outbox_reconciler_undelivered_race_lost_total 1",
		"worker_outbox_reconciler_undelivered_blocked 7",
		"worker_outbox_reconciler_dead_total 1",
	} {
		if !strings.Contains(metrics.String(), want) {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
	}
	text := logs.String()
	if strings.Count(text, `msg="outbox undelivered row terminalized"`) != 2 ||
		!strings.Contains(text, "reason="+UndeliveredReasonPrerequisiteFailed) ||
		!strings.Contains(text, "reason="+UndeliveredReasonDeliveryDead) ||
		strings.Count(text, `msg="outbox delivery dead"`) != 1 ||
		!strings.Contains(text, "reason=river_insert_failed") {
		t.Fatalf("log lines = %s", text)
	}
}
