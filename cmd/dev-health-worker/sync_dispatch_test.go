package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/riverqueue/river"
)

func TestPostSyncRemainingScopeMatchesBoundedFamilyContract(t *testing.T) {
	t.Parallel()
	plan := syncdispatchruntime.PostSyncPlan{
		TargetDay:    time.Date(2026, 7, 23, 23, 59, 0, 0, time.UTC),
		BackfillDays: 180,
	}
	complexity, err := postSyncRemainingScope("complexity", plan)
	if err != nil || string(complexity) != `{"version":1,"day":"2026-07-23","backfill_days":1}` {
		t.Fatalf("complexity=%s err=%v", complexity, err)
	}
	dora, err := postSyncRemainingScope("dora", plan)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if json.Unmarshal(dora, &decoded) != nil || decoded["backfill_days"] != float64(90) ||
		decoded["sink"] != "auto" || decoded["interval"] != "daily" {
		t.Fatalf("dora=%s", dora)
	}
	membership, err := postSyncRemainingScope("membership_backfill", plan)
	if err != nil || string(membership) != `{"version":1,"repo_ids":[]}` {
		t.Fatalf("membership=%s err=%v", membership, err)
	}
}

func TestPostSyncWorkGraphScopePreservesLegacyWindowShape(t *testing.T) {
	t.Parallel()
	from := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	to := time.Date(2026, 1, 14, 23, 0, 0, 0, time.UTC)
	plan := syncdispatchruntime.PostSyncPlan{From: &from, To: &to}
	build, err := postSyncWorkGraphScope(workgraph.KindBuild, plan)
	if err != nil || string(build) != `{"from_date":"2026-01-01T03:00:00Z","to_date":"2026-01-14T23:00:00Z"}` {
		t.Fatalf("build=%s err=%v", build, err)
	}
	investment, err := postSyncWorkGraphScope(workgraph.KindMaterialize, plan)
	if err != nil || string(investment) != `{"from_date":"2026-01-01","to_date":"2026-01-14"}` {
		t.Fatalf("investment=%s err=%v", investment, err)
	}
}

func TestPostSyncRequestIDsMatchCrossLanguagePlanner(t *testing.T) {
	t.Parallel()
	const runID = "00000000-0000-4000-8000-000000000004"
	if got, want := postSyncRequestID(runID, "workgraph"), "02be9bc9-c26b-5735-8ace-04e72d4c80a8"; got != want {
		t.Fatalf("workgraph id=%s want=%s", got, want)
	}
	if got, want := postSyncRequestID(runID, "investment"), "c53e7bf8-705f-583e-828e-f2540336645a"; got != want {
		t.Fatalf("investment id=%s want=%s", got, want)
	}
}

func TestPostSyncRemainingScopeRejectsUnownedFamily(t *testing.T) {
	t.Parallel()
	if _, err := postSyncRemainingScope("recommendations", syncdispatchruntime.PostSyncPlan{}); !errors.Is(err, syncdispatchruntime.ErrPostSyncUnavailable) {
		t.Fatalf("err=%v", err)
	}
}

// TestSyncCoordinatorReportsItsRegisteredKind closes the second registration
// blind spot: sync.team_autoimport is a bounded registry kind whose worker
// lives in the coordinator's own private River client. Before CUT-02 that
// builder returned only a lifecycle component, so the kind was constructed but
// unobservable to startup validation.
func TestSyncCoordinatorReportsItsRegisteredKind(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	for _, test := range []struct {
		name      string
		promote   []string
		wantKinds []string
	}{
		{
			name:      "celery routed kind is not consumed at all",
			wantKinds: nil,
		},
		{
			name:      "promoted kind is registered and reported",
			promote:   []string{jobcontract.KindTeamAutoimport},
			wantKinds: []string{jobcontract.KindTeamAutoimport},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// sync.team_autoimport now ships at go_default in the checked-in
			// contract, so the "not consumed at all" case has to demote it
			// back to Celery explicitly; promotedContractRoot alone would be
			// a no-op against an already-promoted production tree.
			var registry *jobruntime.Registry
			if len(test.promote) == 0 {
				registry, _ = demotedContractRoot(t, jobcontract.KindTeamAutoimport)
			} else {
				registry, _ = promotedContractRoot(t, test.promote...)
			}
			family, err := buildSyncCoordinatorWorker(
				config.Config{
					Queues:                         []string{"sync", "sync_provider"},
					WorkerQueueConcurrency:         map[string]int{"sync": 13, "sync_provider": 7},
					RiverDatabaseSchema:            "river",
					OperationalBridgeURL:           "http://localhost",
					OperationalBridgeToken:         secrets.NewValue("test-bridge-token"),
					OperationalBridgeTimeout:       time.Second,
					OperationalBridgeAllowInsecure: true,
				},
				reportBuilderDatabase(t),
				registry,
				reportTestObserver(t),
				slog.Default(),
				river.NewWorkers(),
			)
			if err != nil {
				t.Fatalf("buildSyncCoordinatorWorker: %v", err)
			}
			if len(family.queues) == 0 {
				t.Fatal("coordinator did not declare its selected queue")
			}
			if len(family.handlers) != len(test.wantKinds) {
				t.Fatalf("reported handlers = %#v, want %v", family.handlers, test.wantKinds)
			}
			for index, kind := range test.wantKinds {
				if family.handlers[index].Kind != kind {
					t.Fatalf("reported handler %d = %s, want %s", index, family.handlers[index].Kind, kind)
				}
			}
			// The coordinator's native, non-registry dispatch handlers also
			// consume the sync queue. Its queue budget remains present even
			// while team auto-import still routes to Celery.
			if len(family.queues) != 1 || family.queues[0].Queue != syncCoordinatorQueue ||
				family.queues[0].MaxWorkers != 13 {
				t.Fatalf("reported queues = %#v", family.queues)
			}
		})
	}
}

// recordingTx is a pgx.Tx stub that records the statements a caller stages in
// the transaction it owns. It exists so the post-sync writers can be asserted
// on their EFFECT -- the outbox row that reaches the transaction -- rather than
// on which producer method a mock observed being called. The route the writer
// picks produces an identical row either way; the only observable difference
// is whether the row is written at all or the producer rejects the publish, so
// that is exactly what these tests measure.
type recordingTx struct {
	statements []string
	arguments  [][]any
}

func (tx *recordingTx) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("recordingTx: Begin not implemented")
}
func (tx *recordingTx) Commit(context.Context) error   { return nil }
func (tx *recordingTx) Rollback(context.Context) error { return nil }
func (tx *recordingTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("recordingTx: CopyFrom not implemented")
}
func (tx *recordingTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (tx *recordingTx) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (tx *recordingTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("recordingTx: Prepare not implemented")
}

func (tx *recordingTx) Exec(_ context.Context, statement string, arguments ...any) (pgconn.CommandTag, error) {
	tx.statements = append(tx.statements, statement)
	tx.arguments = append(tx.arguments, arguments)
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (tx *recordingTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("recordingTx: Query not implemented")
}
func (tx *recordingTx) QueryRow(context.Context, string, ...any) pgx.Row { return nil }
func (tx *recordingTx) Conn() *pgx.Conn                                  { return nil }

// outboxRowsFor returns the job kinds of every worker_job_outbox row staged in
// the transaction, in the order they were written.
func (tx *recordingTx) outboxRowsFor(t *testing.T) []string {
	t.Helper()
	kinds := []string{}
	for index, statement := range tx.statements {
		if !strings.Contains(statement, "INSERT INTO public.worker_job_outbox") {
			continue
		}
		arguments := tx.arguments[index]
		if len(arguments) < 3 {
			t.Fatalf("outbox insert %d carried %d arguments", index, len(arguments))
		}
		kind, ok := arguments[2].(string)
		if !ok {
			t.Fatalf("outbox insert %d job_kind argument is %T", index, arguments[2])
		}
		kinds = append(kinds, kind)
	}
	return kinds
}

// TestTeamAutoimportPostSyncWriterStagesItsHandoffOnBothCheckedInRoutes is the
// effect-level regression for CHAOS-3946.
//
// The writer called producer.PublishDeferred unconditionally. The deferred
// route is only legal while a kind is still pinned to Celery on both its route
// and its rollback route, and sync.team_autoimport has been go_default/river
// since the cutover, so every publish was rejected with
// publish_not_permitted_for_route -- and since Fanout stages the whole
// generation in ONE transaction with this publish LAST, that rejection
// discarded the complexity run, the daily dispatch, the workgraph build, the
// investment materialize, the membership backfill and the DORA run staged
// alongside it, for every organization with auto_import_teams enabled.
//
// Both halves are asserted. The checked-in tree must stage the row; a tree
// rolled back to Celery must ALSO stage it, through the deferred route. A test
// that only pinned the first half would pass against a writer that dropped the
// branch entirely and always published executable, which would break the
// rollback the branch exists to preserve.
func TestTeamAutoimportPostSyncWriterStagesItsHandoffOnBothCheckedInRoutes(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	const (
		organizationID = "6d1f2b0e-1f5c-4e5a-9d21-0c9f5c3b8a41"
		syncRunID      = "b6b2f5d4-1f0e-4a1c-9f77-3f4a4d2b6c58"
	)
	for _, testCase := range []struct {
		name     string
		registry *jobruntime.Registry
	}{
		{
			name: "checked-in contract routes the kind to River",
			registry: func() *jobruntime.Registry {
				registry, err := jobruntime.Load(defaultContractRoot)
				if err != nil {
					t.Fatalf("load checked-in contracts: %v", err)
				}
				return registry
			}(),
		},
		{
			name: "a rollback pins the kind back to Celery",
			registry: func() *jobruntime.Registry {
				registry, _ := demotedContractRoot(t, jobcontract.KindTeamAutoimport)
				return registry
			}(),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			producer, err := joboutbox.NewTransactionProducer(testCase.registry)
			if err != nil {
				t.Fatal(err)
			}
			writer := teamAutoimportPostSyncWriter{producer: producer, registry: testCase.registry}
			tx := &recordingTx{}
			plan := syncdispatchruntime.PostSyncPlan{
				OrganizationID: organizationID,
				SyncRunID:      syncRunID,
				TeamAutoimport: true,
			}
			if err := writer.PublishTx(context.Background(), tx, plan); err != nil {
				t.Fatalf(
					"PublishTx staged no handoff: %v\n"+
						"The whole post-sync fanout commits in this one transaction, so this "+
						"error discards every other handoff staged before it.", err,
				)
			}
			staged := tx.outboxRowsFor(t)
			if len(staged) != 1 || staged[0] != jobcontract.KindTeamAutoimport {
				t.Fatalf("staged outbox rows = %v, want exactly [%s]",
					staged, jobcontract.KindTeamAutoimport)
			}
			dedupeKey, ok := tx.arguments[0][1].(string)
			if !ok || dedupeKey != "post-sync:"+syncRunID+":"+jobcontract.KindTeamAutoimport {
				t.Fatalf("dedupe_key = %v, want the post-sync idempotency key", tx.arguments[0][1])
			}
		})
	}
}
