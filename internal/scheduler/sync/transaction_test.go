package sync

import (
	"context"
	"errors"
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
}

func mutationRepository(transaction schedulerTransaction) *Repository {
	return &Repository{
		ownership: OwnershipPolicy{owner: schedulerOwnerGo, mode: schedulerModeMutation},
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

func (*fakeSchedulerTransaction) QueryRow(context.Context, string, ...any) pgx.Row {
	panic("unexpected QueryRow")
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
	) error {
		if handoff != transaction {
			t.Fatal("coordinator did not receive the locking transaction")
		}
		transaction.events = append(transaction.events, "handoff")
		received = occurrence
		return nil
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
	if strings.Join(transaction.events, ",") != "handoff,marker,commit" {
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
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) error {
			transaction.events = append(transaction.events, "handoff")
			return handoffErr
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
	if strings.Join(transaction.events, ",") != "handoff" {
		t.Fatalf("transaction events = %v", transaction.events)
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
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) error {
			transaction.events = append(transaction.events, "handoff")
			return nil
		}),
	)
	if !errors.Is(err, ErrScheduleMarkerLost) {
		t.Fatalf("HandoffDue() err = %v", err)
	}
	if transaction.committed || !transaction.rolledBack {
		t.Fatalf("transaction committed=%v rolledBack=%v", transaction.committed, transaction.rolledBack)
	}
	if strings.Join(transaction.events, ",") != "handoff,marker" {
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
		ownership: OwnershipPolicy{owner: schedulerOwnerGo, mode: schedulerModeMutation},
		begin: func(context.Context) (schedulerTransaction, error) {
			calls++
			return nil, errors.New("unexpected begin")
		},
	}
	coordinator := CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) error {
		return nil
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
		CoordinatorFunc(func(context.Context, HandoffTransaction, Occurrence) error { return nil }),
	)
	if !errors.Is(err, ErrSchedulerMutationDisabled) {
		t.Fatalf("HandoffDue() err = %v", err)
	}
	if calls != 0 {
		t.Fatalf("transactions opened = %d", calls)
	}
}
