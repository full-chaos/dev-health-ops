package sync

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeLockedRows struct {
	rows   [][]any
	index  int
	closed bool
	err    error
}

func (rows *fakeLockedRows) Next() bool {
	return rows.index < len(rows.rows)
}

func (rows *fakeLockedRows) Scan(dest ...any) error {
	values := rows.rows[rows.index]
	rows.index++
	for index, value := range values {
		switch target := dest[index].(type) {
		case *string:
			*target = value.(string)
		case *bool:
			*target = value.(bool)
		case *int:
			*target = value.(int)
		case *time.Time:
			*target = value.(time.Time)
		case **time.Time:
			if value == nil {
				*target = nil
			} else {
				copied := value.(time.Time)
				*target = &copied
			}
		default:
			return errors.New("unsupported fake scan destination")
		}
	}
	return nil
}

func (rows *fakeLockedRows) Err() error { return rows.err }

func (rows *fakeLockedRows) Close() { rows.closed = true }

type fakeSchedulerTransaction struct {
	rows           *fakeLockedRows
	events         []string
	queryArgs      []any
	execArgs       [][]any
	execTag        pgconn.CommandTag
	execErr        error
	commitErr      error
	committed      bool
	rolledBack     bool
	queryStatement string
	openRuns       openScheduledRuns
	openRunsErr    error
}

func mutationRepository(transaction schedulerTransaction) *Repository {
	return &Repository{
		ownership: reviewedGoMutationOwnershipPolicy(),
		begin: func(context.Context) (schedulerTransaction, error) {
			return transaction, nil
		},
	}
}

func (transaction *fakeSchedulerTransaction) queryCandidates(
	_ context.Context,
	statement string,
	args ...any,
) (lockedCandidateRows, error) {
	transaction.queryStatement = statement
	transaction.queryArgs = args
	return transaction.rows, nil
}

func (transaction *fakeSchedulerTransaction) Exec(
	_ context.Context,
	_ string,
	args ...any,
) (pgconn.CommandTag, error) {
	transaction.events = append(transaction.events, "marker")
	transaction.execArgs = append(transaction.execArgs, args)
	return transaction.execTag, transaction.execErr
}

// QueryRow answers the open-scheduled-run read, the only row read the kernel
// makes itself. The default answer is "no open run".
func (transaction *fakeSchedulerTransaction) QueryRow(_ context.Context, statement string, _ ...any) pgx.Row {
	if statement != schedulerOpenScheduledRunsSQL {
		panic("unexpected QueryRow")
	}
	transaction.events = append(transaction.events, "open_runs")
	return fakeOpenRunsRow{open: transaction.openRuns, err: transaction.openRunsErr}
}

type fakeOpenRunsRow struct {
	open openScheduledRuns
	err  error
}

func (row fakeOpenRunsRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	values := []any{
		row.open.blocking, row.open.blockingOccurrenceID, row.open.blockingSyncRunID, row.open.blockingOpenSeconds,
		row.open.pastBound, row.open.pastBoundOccurrenceID, row.open.pastBoundSyncRunID,
		string(row.open.pastBoundReason), row.open.pastBoundOpenSeconds,
	}
	if len(dest) != len(values) {
		return fmt.Errorf("open runs scan wants %d columns, got %d", len(values), len(dest))
	}
	for index, value := range values {
		switch target := dest[index].(type) {
		case *int:
			*target = value.(int)
		case *int64:
			*target = value.(int64)
		case *string:
			*target = value.(string)
		default:
			return fmt.Errorf("open runs scan column %d has unsupported type %T", index, target)
		}
	}
	return nil
}

func (transaction *fakeSchedulerTransaction) Commit(context.Context) error {
	transaction.events = append(transaction.events, "commit")
	transaction.committed = transaction.commitErr == nil
	return transaction.commitErr
}

func (transaction *fakeSchedulerTransaction) Rollback(context.Context) error {
	transaction.rolledBack = true
	return nil
}

func lockedRow(
	configID, orgID, jobID, cron string,
	createdAt, lastSyncAt time.Time,
) []any {
	return []any{
		configID,
		orgID,
		true,
		true, // planner_managed: these fixtures represent real schedulable configs
		cron,
		"UTC",
		lastSyncAt,
		createdAt,
		jobID,
		cron,
		"UTC",
		activeJobStatus,
		false,
		nil,
		createdAt,
		nil,
	}
}

func TestHandoffDuePersistsHandoffBeforeAdvancingMarker(t *testing.T) {
	observedAt := at("2026-01-01T12:00:00Z")
	transaction := &fakeSchedulerTransaction{
		rows: &fakeLockedRows{rows: [][]any{
			lockedRow("config-due", "org-a", "job-a", "0 * * * *", at("2026-01-01T09:00:00Z"), at("2026-01-01T10:00:00Z")),
			lockedRow("config-future", "org-b", "job-b", "0 13 * * *", at("2026-01-01T09:00:00Z"), at("2026-01-01T10:00:00Z")),
		}},
		execTag: pgconn.NewCommandTag("UPDATE 1"),
	}
	repository := mutationRepository(transaction)
	var received Occurrence
	coordinator := CoordinatorFunc(func(
		_ context.Context,
		handoff HandoffTransaction,
		occurrence Occurrence,
	) (HandoffOutcome, error) {
		if handoff != transaction {
			t.Fatal("coordinator did not receive the locking transaction")
		}
		transaction.events = append(transaction.events, "handoff")
		received = occurrence
		return OccurrenceMinted, nil
	})

	occurrences, err := repository.HandoffDue(context.Background(), observedAt, 2, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if len(occurrences) != 1 || occurrences[0] != received {
		t.Fatalf("occurrences = %#v, received = %#v", occurrences, received)
	}
	if received.ConfigID != "config-due" || received.OrgID != "org-a" ||
		received.JobID != "job-a" ||
		!received.ScheduledFor.Equal(at("2026-01-01T11:00:00Z")) ||
		!received.NextRunAt.Equal(at("2026-01-01T13:00:00Z")) {
		t.Fatalf("occurrence = %#v", received)
	}
	if strings.Join(transaction.events, ",") != "open_runs,handoff,marker,commit" {
		t.Fatalf("transaction events = %v", transaction.events)
	}
	if !transaction.committed || !transaction.rolledBack || !transaction.rows.closed {
		t.Fatalf(
			"transaction committed=%v rolledBack=%v rowsClosed=%v",
			transaction.committed,
			transaction.rolledBack,
			transaction.rows.closed,
		)
	}
	if len(transaction.execArgs) != 1 ||
		!transaction.execArgs[0][0].(time.Time).Equal(received.NextRunAt) ||
		!transaction.execArgs[0][1].(time.Time).Equal(observedAt) ||
		transaction.execArgs[0][2] != "job-a" {
		t.Fatalf("marker args = %#v", transaction.execArgs)
	}
}

func TestHandoffDueRollsBackWithoutMarkerWhenCoordinatorFails(t *testing.T) {
	observedAt := at("2026-01-01T12:00:00Z")
	transaction := &fakeSchedulerTransaction{
		rows: &fakeLockedRows{rows: [][]any{
			lockedRow("config-a", "org-a", "job-a", "0 * * * *", at("2026-01-01T09:00:00Z"), at("2026-01-01T10:00:00Z")),
		}},
		execTag: pgconn.NewCommandTag("UPDATE 1"),
	}
	repository := mutationRepository(transaction)
	handoffErr := errors.New("durable handoff unavailable")

	_, err := repository.HandoffDue(
		context.Background(),
		observedAt,
		1,
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
			transaction.events = append(transaction.events, "handoff")
			return "", handoffErr
		}),
	)
	if !errors.Is(err, handoffErr) {
		t.Fatalf("HandoffDue() err = %v", err)
	}
	if transaction.committed || !transaction.rolledBack || len(transaction.execArgs) != 0 {
		t.Fatalf(
			"transaction committed=%v rolledBack=%v markerCalls=%d",
			transaction.committed,
			transaction.rolledBack,
			len(transaction.execArgs),
		)
	}
	if strings.Join(transaction.events, ",") != "open_runs,handoff" {
		t.Fatalf("transaction events = %v", transaction.events)
	}
}

func TestHandoffDueResultLeavesUnsupportedCronForCeleryWithoutMarkerMutation(t *testing.T) {
	observedAt := at("2026-01-01T12:00:00Z")
	transaction := &fakeSchedulerTransaction{
		rows: &fakeLockedRows{rows: [][]any{
			lockedRow("config-random", "org-a", "job-a", "R * * * *", at("2026-01-01T09:00:00Z"), at("2026-01-01T10:00:00Z")),
		}},
		execTag: pgconn.NewCommandTag("UPDATE 1"),
	}
	result, err := mutationRepository(transaction).HandoffDueResult(
		context.Background(), observedAt, 1,
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
			t.Fatal("unsupported cron reached coordinator")
			return OccurrenceMinted, nil
		}),
	)
	if !errors.Is(err, ErrSchedulerFallbackRequired) {
		t.Fatalf("HandoffDueResult() error = %v", err)
	}
	if result.Candidates != 1 || result.UnsupportedCron != 1 || result.InvalidCron != 0 || len(result.HandedOff) != 0 {
		t.Fatalf("handoff result = %#v", result)
	}
	if len(transaction.execArgs) != 0 || transaction.committed || !transaction.rolledBack {
		t.Fatalf(
			"unsupported fallback marker calls=%d committed=%v rolledBack=%v",
			len(transaction.execArgs),
			transaction.committed,
			transaction.rolledBack,
		)
	}
}

func TestHandoffDueResultFailsClosedBeforeMixedFallbackWindowWrites(t *testing.T) {
	observedAt := at("2026-01-01T12:00:00Z")
	transaction := &fakeSchedulerTransaction{
		rows: &fakeLockedRows{rows: [][]any{
			lockedRow("config-unsupported", "org-a", "job-a", "R * * * *", at("2026-01-01T09:00:00Z"), at("2026-01-01T10:00:00Z")),
			lockedRow("config-valid", "org-b", "job-b", "0 * * * *", at("2026-01-01T09:00:00Z"), at("2026-01-01T10:00:00Z")),
		}},
		execTag: pgconn.NewCommandTag("UPDATE 1"),
	}
	coordinatorCalls := 0
	result, err := mutationRepository(transaction).HandoffDueResult(
		context.Background(), observedAt, 2,
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
			coordinatorCalls++
			return OccurrenceMinted, nil
		}),
	)
	if !errors.Is(err, ErrSchedulerFallbackRequired) {
		t.Fatalf("HandoffDueResult() error = %v", err)
	}
	if result.Candidates != 2 || result.UnsupportedCron != 1 || result.TimingEligible != 1 {
		t.Fatalf("handoff result = %#v", result)
	}
	if coordinatorCalls != 0 || len(transaction.execArgs) != 0 || transaction.committed || !transaction.rolledBack {
		t.Fatalf(
			"mixed fallback wrote: coordinator=%d marker=%d committed=%v rolledBack=%v",
			coordinatorCalls,
			len(transaction.execArgs),
			transaction.committed,
			transaction.rolledBack,
		)
	}
}

func TestHandoffDueRejectsLostMarkerAndRollsBackHandoff(t *testing.T) {
	observedAt := at("2026-01-01T12:00:00Z")
	transaction := &fakeSchedulerTransaction{
		rows: &fakeLockedRows{rows: [][]any{
			lockedRow("config-a", "org-a", "job-a", "0 * * * *", at("2026-01-01T09:00:00Z"), at("2026-01-01T10:00:00Z")),
		}},
		execTag: pgconn.NewCommandTag("UPDATE 0"),
	}
	repository := mutationRepository(transaction)

	_, err := repository.HandoffDue(
		context.Background(),
		observedAt,
		1,
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
			transaction.events = append(transaction.events, "handoff")
			return OccurrenceMinted, nil
		}),
	)
	if !errors.Is(err, ErrScheduleMarkerLost) {
		t.Fatalf("HandoffDue() err = %v", err)
	}
	if transaction.committed || !transaction.rolledBack {
		t.Fatalf("transaction committed=%v rolledBack=%v", transaction.committed, transaction.rolledBack)
	}
	if strings.Join(transaction.events, ",") != "open_runs,handoff,marker" {
		t.Fatalf("transaction events = %v", transaction.events)
	}
}

func TestOccurrenceIdentityIsDeterministicForConfigAndCronOccurrence(t *testing.T) {
	scheduledFor := at("2026-01-01T11:00:00Z")
	first := newOccurrence(
		"config-a",
		"org-a",
		"job-a",
		scheduledFor,
		at("2026-01-01T12:00:00Z"),
		at("2026-01-01T13:00:00Z"),
	)
	retry := newOccurrence(
		"config-a",
		"org-b",
		"replacement-job",
		scheduledFor.In(time.FixedZone("offset", -8*60*60)),
		at("2026-01-01T12:30:00Z"),
		at("2026-01-01T14:00:00Z"),
	)
	next := newOccurrence(
		"config-a",
		"org-a",
		"job-a",
		at("2026-01-01T12:00:00Z"),
		at("2026-01-01T13:00:00Z"),
		at("2026-01-01T14:00:00Z"),
	)
	otherConfig := newOccurrence(
		"config-b",
		"org-a",
		"job-a",
		scheduledFor,
		at("2026-01-01T12:00:00Z"),
		at("2026-01-01T13:00:00Z"),
	)

	if first.ID != retry.ID {
		t.Fatalf("retry identity changed: %s != %s", first.ID, retry.ID)
	}
	if first.ID != "sha256:27478ac7c7bbcfc33caa3922492910d97220984911632d754944fdeaf405f0f9" {
		t.Fatalf("golden identity changed: %s", first.ID)
	}
	if first.ID == next.ID || first.ID == otherConfig.ID {
		t.Fatalf("identity collision: first=%s next=%s other=%s", first.ID, next.ID, otherConfig.ID)
	}
	if first.IdentityVersion != OccurrenceIdentityVersion ||
		!strings.HasPrefix(first.ID, "sha256:") || len(first.ID) != len("sha256:")+64 {
		t.Fatalf("identity = %#v", first)
	}
}

func TestHandoffStatementIsBoundedAndMultiReplicaSafe(t *testing.T) {
	statement := strings.ToUpper(schedulerHandoffCandidatesSQL)
	for _, want := range []string{
		"JOIN PUBLIC.SCHEDULED_JOBS AS JOB",
		"JOB.SYNC_CONFIG_ID = CONFIG.ID",
		"JOB.JOB_TYPE = 'SYNC'",
		"FOR UPDATE OF CONFIG, JOB SKIP LOCKED",
		"LIMIT $2",
	} {
		if !strings.Contains(statement, want) {
			t.Fatalf("handoff query missing %q: %s", want, schedulerHandoffCandidatesSQL)
		}
	}
	for _, forbidden := range []string{"INSERT", "DELETE", "ADVISORY"} {
		if regexp.MustCompile(`\b` + forbidden + `\b`).MatchString(statement) {
			t.Fatalf("handoff query contains %q", forbidden)
		}
	}
}

func TestHandoffDueValidatesRequestBeforeOpeningTransaction(t *testing.T) {
	calls := 0
	repository := &Repository{
		ownership: reviewedGoMutationOwnershipPolicy(),
		begin: func(context.Context) (schedulerTransaction, error) {
			calls++
			return nil, errors.New("unexpected begin")
		},
	}
	coordinator := CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
		return OccurrenceMinted, nil
	})
	for _, test := range []struct {
		name        string
		ctx         context.Context
		observedAt  time.Time
		limit       int
		coordinator Coordinator
	}{
		{"nil context", nil, time.Now(), 1, coordinator},
		{"zero observation", context.Background(), time.Time{}, 1, coordinator},
		{"zero limit", context.Background(), time.Now(), 0, coordinator},
		{"oversized limit", context.Background(), time.Now(), maximumSnapshotLimit + 1, coordinator},
		{"nil coordinator", context.Background(), time.Now(), 1, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := repository.HandoffDue(test.ctx, test.observedAt, test.limit, test.coordinator); !errors.Is(err, ErrInvalidTransactionRequest) {
				t.Fatalf("HandoffDue() err = %v", err)
			}
		})
	}
	if calls != 0 {
		t.Fatalf("transactions opened = %d", calls)
	}
}

func TestDefaultOwnershipPreventsHandoffBeforeOpeningTransaction(t *testing.T) {
	calls := 0
	repository := &Repository{
		ownership: DefaultOwnershipPolicy(),
		begin: func(context.Context) (schedulerTransaction, error) {
			calls++
			return nil, errors.New("unexpected begin")
		},
	}
	_, err := repository.HandoffDue(
		context.Background(),
		time.Now(),
		1,
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
			return OccurrenceMinted, nil
		}),
	)
	if !errors.Is(err, ErrSchedulerMutationDisabled) {
		t.Fatalf("HandoffDue() err = %v", err)
	}
	if calls != 0 {
		t.Fatalf("transactions opened = %d", calls)
	}
}

func dueOpenRunTransaction(open openScheduledRuns) *fakeSchedulerTransaction {
	return &fakeSchedulerTransaction{
		rows: &fakeLockedRows{rows: [][]any{
			lockedRow("config-due", "org-a", "job-a", "0 * * * *", at("2026-01-01T09:00:00Z"), at("2026-01-01T10:00:00Z")),
		}},
		execTag:  pgconn.NewCommandTag("UPDATE 1"),
		openRuns: open,
	}
}

func TestHandoffDueSkipsTheTickAndAdvancesTheMarkerWhileAScheduledRunIsOpen(t *testing.T) {
	transaction := dueOpenRunTransaction(openScheduledRuns{
		blocking: 1, blockingOccurrenceID: "sha256:open", blockingSyncRunID: "run-open", blockingOpenSeconds: 3600,
		// A second, past-bound run of the same configuration does not turn the
		// skip into a start: one run inside its bound is enough to wait.
		pastBound: 1, pastBoundOccurrenceID: "sha256:old", pastBoundReason: OpenRunAgeCap,
	})
	coordinator := CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
		t.Fatal("the coordinator was asked to mint while a scheduled run is open")
		return "", nil
	})
	result, err := mutationRepository(transaction).HandoffDueResult(
		context.Background(), at("2026-01-01T12:00:00Z"), 2, coordinator,
	)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(transaction.events, ",") != "open_runs,marker,commit" {
		t.Fatalf("transaction events = %v", transaction.events)
	}
	if len(result.HandedOff) != 0 || result.Minted() != 0 || len(result.OpenRunsPastBound) != 0 {
		t.Fatalf("result = %#v", result)
	}
	want := OpenRunSkip{
		ConfigID: "config-due", OccurrenceID: "sha256:open", SyncRunID: "run-open",
		OpenSeconds: 3600, NextRunAt: at("2026-01-01T13:00:00Z"),
	}
	if len(result.SkippedOpenRun) != 1 || result.SkippedOpenRun[0] != want {
		t.Fatalf("SkippedOpenRun = %#v, want %#v", result.SkippedOpenRun, want)
	}
	if len(transaction.execArgs) != 1 || len(transaction.execArgs[0]) != 3 {
		t.Fatalf("marker args = %#v", transaction.execArgs)
	}
	if next, ok := transaction.execArgs[0][0].(time.Time); !ok || !next.Equal(at("2026-01-01T13:00:00Z")) {
		t.Fatalf("marker next_run_at = %#v, want the next cron instant", transaction.execArgs[0][0])
	}
	if transaction.execArgs[0][2] != "job-a" {
		t.Fatalf("marker job = %#v", transaction.execArgs[0][2])
	}
	if result.idleDue() {
		t.Fatal("a tick skipped for an open run was reported as an idle due window")
	}
}

func TestHandoffDueStartsARunAndNamesTheOpenRunPastItsBound(t *testing.T) {
	for _, reason := range []OpenRunBoundReason{OpenRunNoProgress, OpenRunAgeCap} {
		t.Run(string(reason), func(t *testing.T) {
			transaction := dueOpenRunTransaction(openScheduledRuns{
				pastBound: 2, pastBoundOccurrenceID: "sha256:old", pastBoundSyncRunID: "run-old",
				pastBoundReason: reason, pastBoundOpenSeconds: 90000,
			})
			coordinator := CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
				transaction.events = append(transaction.events, "handoff")
				return OccurrenceMinted, nil
			})
			result, err := mutationRepository(transaction).HandoffDueResult(
				context.Background(), at("2026-01-01T12:00:00Z"), 2, coordinator,
			)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(transaction.events, ",") != "open_runs,handoff,marker,commit" {
				t.Fatalf("transaction events = %v", transaction.events)
			}
			if result.Minted() != 1 || len(result.SkippedOpenRun) != 0 {
				t.Fatalf("result = %#v", result)
			}
			want := OpenRunPastBound{
				ConfigID: "config-due", OccurrenceID: "sha256:old", SyncRunID: "run-old",
				Reason: reason, OpenSeconds: 90000, OpenRuns: 2,
			}
			if len(result.OpenRunsPastBound) != 1 || result.OpenRunsPastBound[0] != want {
				t.Fatalf("OpenRunsPastBound = %#v, want %#v", result.OpenRunsPastBound, want)
			}
		})
	}
}

func TestHandoffDueDoesNotNameAPastBoundRunWhenItStartedNothing(t *testing.T) {
	transaction := dueOpenRunTransaction(openScheduledRuns{
		pastBound: 1, pastBoundOccurrenceID: "sha256:old", pastBoundReason: OpenRunNoProgress,
	})
	coordinator := CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
		return OccurrenceRepeated, nil
	})
	result, err := mutationRepository(transaction).HandoffDueResult(
		context.Background(), at("2026-01-01T12:00:00Z"), 2, coordinator,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.OpenRunsPastBound) != 0 {
		t.Fatalf("a window that minted nothing reported a run started past the bound: %#v", result.OpenRunsPastBound)
	}
}

func TestHandoffDueFailsTheWindowWhenTheOpenRunReadFails(t *testing.T) {
	failure := errors.New("open run read failed")
	transaction := dueOpenRunTransaction(openScheduledRuns{})
	transaction.openRunsErr = failure
	coordinator := CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
		t.Fatal("the coordinator was asked to mint after the open run read failed")
		return "", nil
	})
	_, err := mutationRepository(transaction).HandoffDueResult(
		context.Background(), at("2026-01-01T12:00:00Z"), 2, coordinator,
	)
	if !errors.Is(err, failure) {
		t.Fatalf("err = %v, want the open run read failure", err)
	}
	if transaction.committed || len(transaction.execArgs) != 0 {
		t.Fatalf("a failed read wrote: committed=%v marker=%v", transaction.committed, transaction.execArgs)
	}
}

func TestHandoffDueRejectsAnUnknownOpenRunBoundReason(t *testing.T) {
	transaction := dueOpenRunTransaction(openScheduledRuns{pastBound: 1, pastBoundReason: "sideways"})
	coordinator := CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) (HandoffOutcome, error) {
		return OccurrenceMinted, nil
	})
	_, err := mutationRepository(transaction).HandoffDueResult(
		context.Background(), at("2026-01-01T12:00:00Z"), 2, coordinator,
	)
	if !errors.Is(err, ErrInvalidTransactionRequest) {
		t.Fatalf("err = %v, want ErrInvalidTransactionRequest for a reason outside the label vocabulary", err)
	}
	if transaction.committed {
		t.Fatal("a window with an unknown bound reason committed")
	}
}

func TestResumedOccurrenceInstantPrefersADueMarkerThatIsLater(t *testing.T) {
	evaluated := at("2026-01-01T10:00:00Z")
	observed := at("2026-01-01T13:00:05Z")
	for _, tc := range []struct {
		name   string
		marker *time.Time
		want   time.Time
	}{
		{name: "no marker", marker: nil, want: evaluated},
		{name: "marker equal to the evaluated instant", marker: pointer(evaluated), want: evaluated},
		{name: "marker earlier than the evaluated instant", marker: pointer(at("2026-01-01T09:00:00Z")), want: evaluated},
		{name: "marker later and due: ticks were skipped", marker: pointer(at("2026-01-01T13:00:00Z")), want: at("2026-01-01T13:00:00Z")},
		{name: "marker exactly at the observed time is due", marker: pointer(observed), want: observed},
		{name: "marker later and not yet due", marker: pointer(at("2026-01-01T14:00:00Z")), want: evaluated},
		{name: "marker in another zone", marker: pointer(at("2026-01-01T05:00:00-08:00")), want: at("2026-01-01T13:00:00Z")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := resumedOccurrenceInstant(evaluated, tc.marker, observed)
			if !got.Equal(tc.want) {
				t.Fatalf("resumedOccurrenceInstant() = %s, want %s", got, tc.want)
			}
		})
	}
}
