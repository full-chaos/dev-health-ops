//go:build integration

package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// CHAOS-14113, on the real schema and the real Materialize: the domain graph of an occurrence commits, the attempt
// fails before the coordinator records readiness, and the executed-proof snapshot changes before the replay. The
// replay keeps the unit set the committed graph holds. It is never quarantined as ErrInvalidPlan.
const (
	proofReplayOrgID         = "00000000-0000-4000-8000-0000000000aa"
	proofReplayConfigID      = "00000000-0000-4000-8000-000000005001"
	proofReplayIntegrationID = "00000000-0000-4000-8000-000000005002"
	proofReplaySourceID      = "00000000-0000-4000-8000-000000005003"
	proofReplayJobID         = "00000000-0000-4000-8000-000000005005"
)

type proofReplayHarness struct {
	pool         *pgxpool.Pool
	materializer *NativeMaterializer
	occurrence   PendingOccurrence
	fixture      materializerFixture
}

// newProofReplayHarness seeds one GitHub source with work-items and cicd enabled, each 72 h behind.
func newProofReplayHarness(t *testing.T) *proofReplayHarness {
	t.Helper()
	ctx := context.Background()
	scheduledFor := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	pool := startTestOpsReplayPostgres(t)
	seedPlannerGraph(ctx, t, pool, plannerGraph{
		orgID: proofReplayOrgID, integrationID: proofReplayIntegrationID, provider: "github", configID: proofReplayConfigID,
		jobID: proofReplayJobID, featureID: "00000000-0000-4000-8000-000000005007",
		datasetID: "00000000-0000-4000-8000-000000005004", datasetKey: "work-items",
		targets: `["work-items","cicd"]`,
	})
	mustExec(t, pool, `INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES ('00000000-0000-4000-8000-000000005006'::uuid, $1, $2::uuid, 'cicd', TRUE, '{}'::json)`, proofReplayOrgID, proofReplayIntegrationID)
	mustExec(t, pool, `INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES ($1::uuid,$2,$3::uuid,'github','repository','acme/api','api','acme/api',TRUE,'{}'::json,now(),now())`,
		proofReplaySourceID, proofReplayOrgID, proofReplayIntegrationID)
	for _, dataset := range []string{"work-items", "cicd"} {
		mustExec(t, pool, `INSERT INTO sync_watermarks (id,org_id,source_id,dataset_key,repo_id,target,last_synced_at)
VALUES (gen_random_uuid(),$1,'acme/api',$2,'acme/api',$2,$3)`, proofReplayOrgID, dataset, scheduledFor.Add(-72*time.Hour))
	}
	occurrence := PendingOccurrence{
		ID: "occurrence:v1:proof-replay", IdentityVersion: OccurrenceIdentityVersion,
		OrgID: proofReplayOrgID, ConfigID: proofReplayConfigID, JobID: proofReplayJobID,
		ScheduledFor: scheduledFor, ConfigActive: true, ConfigPlannerManaged: true, JobType: "sync",
	}
	materializer, err := NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	h := &proofReplayHarness{pool: pool, materializer: materializer, occurrence: occurrence,
		fixture: materializerFixture{pool: pool, occurrence: occurrence}}
	// The scheduled security dataset rides every GitHub plan; prove it so each cell flips only its own pair.
	h.ledger(t, "security", true)
	return h
}

// ledger sets the durable executed-proof row of a github pair.
func (h *proofReplayHarness) ledger(t *testing.T, dataset string, proven bool) {
	t.Helper()
	mustExec(t, h.pool, `INSERT INTO sync_executed_proof_ledger (provider, dataset_key, attempted_at, proven_at)
VALUES ('github', $1, now(), CASE WHEN $2::boolean THEN now() END)
ON CONFLICT (provider, dataset_key) DO UPDATE SET proven_at = EXCLUDED.proven_at`, dataset, proven)
}

func (h *proofReplayHarness) refresh(t *testing.T) {
	t.Helper()
	if err := h.materializer.RefreshExecutedProof(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func (h *proofReplayHarness) crashAfterCommit(t *testing.T) {
	t.Helper()
	crash := errors.New("injected crash after domain commit")
	h.materializer.afterDomainCommit = func() error { return crash }
	if _, err := materializeAndCommit(t, h.fixture, h.materializer, h.occurrence); !errors.Is(err, crash) {
		t.Fatalf("first attempt err=%v, want the injected crash", err)
	}
	h.materializer.afterDomainCommit = nil
}

// committedDatasets lists the dataset keys of the occurrence's units in unit order.
func (h *proofReplayHarness) committedDatasets(t *testing.T) []string {
	t.Helper()
	rows, err := h.pool.Query(context.Background(), `SELECT unit.dataset_key FROM sync_run_units AS unit
JOIN sync_runs AS run ON run.id = unit.sync_run_id WHERE run.org_id = $1 ORDER BY unit.id`, proofReplayOrgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var datasets []string
	for rows.Next() {
		var dataset string
		if err := rows.Scan(&dataset); err != nil {
			t.Fatal(err)
		}
		datasets = append(datasets, dataset)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return datasets
}

func equalDatasets(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func TestNativeMaterializerExecutedProofReplayKeepsTheCommittedUnitSet(t *testing.T) {
	for _, test := range []struct {
		name string
		// before sets the ledger and snapshot of the first attempt; flip changes the snapshot before the replay.
		before, flip func(*testing.T, *proofReplayHarness)
		want         []string
	}{
		{
			name: "cicd proven, the replaying replica's evidence load failed (degraded, empty)",
			before: func(t *testing.T, h *proofReplayHarness) {
				h.ledger(t, "work-items", true)
				h.ledger(t, "cicd", true)
				h.refresh(t)
			},
			flip: func(t *testing.T, h *proofReplayHarness) {
				h.materializer.executedProof.Store(&providersync.ExecutedProofEvidence{Degraded: true})
			},
			want: []string{"work-items", "security", "cicd"},
		},
		{
			name: "cicd never attempted (bootstrap), the refresh then reads it attempted and unproven",
			before: func(t *testing.T, h *proofReplayHarness) {
				h.ledger(t, "work-items", true)
				h.refresh(t)
			},
			flip: func(t *testing.T, h *proofReplayHarness) { h.refresh(t) },
			want: []string{"work-items", "security", "cicd"},
		},
		{
			name: "work-items is waived: never attempted, then attempted and unproven, plans the same (guard)",
			before: func(t *testing.T, h *proofReplayHarness) {
				h.ledger(t, "cicd", true)
				h.refresh(t)
			},
			flip: func(t *testing.T, h *proofReplayHarness) { h.refresh(t) },
			want: []string{"work-items", "security", "cicd"},
		},
		{
			name: "cicd attempted and unproven, another run proves it before the replay",
			before: func(t *testing.T, h *proofReplayHarness) {
				h.ledger(t, "work-items", true)
				h.ledger(t, "cicd", false)
				h.refresh(t)
			},
			flip: func(t *testing.T, h *proofReplayHarness) {
				h.ledger(t, "cicd", true)
				h.refresh(t)
			},
			want: []string{"work-items", "security"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newProofReplayHarness(t)
			test.before(t, h)
			h.crashAfterCommit(t)
			committed := h.committedDatasets(t)
			if !equalDatasets(committed, test.want) {
				t.Fatalf("first attempt committed %v, want %v", committed, test.want)
			}
			test.flip(t, h)
			if _, err := materializeAndCommit(t, h.fixture, h.materializer, h.occurrence); err != nil {
				t.Fatalf("replay: %v (ErrInvalidPlan = %v, quarantined)", err, errors.Is(err, ErrInvalidPlan))
			}
			if got := h.committedDatasets(t); !equalDatasets(got, committed) {
				t.Fatalf("units after the replay %v, want the committed %v", got, committed)
			}
		})
	}
}

// A committed unit set the planner cannot produce is still refused: the committed graph is trusted only for the
// executed-proof decision, never for a unit no other gate admits.
func TestNativeMaterializerExecutedProofReplayRefusesAUnitSetThePlannerCannotProduce(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate string
	}{
		{"a committed unit moved to a dataset the org did not enable",
			`UPDATE sync_run_units SET dataset_key = 'deployments' WHERE dataset_key = 'cicd'`},
		{"an extra committed unit for a dataset the org did not enable",
			`INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,cost_class,mode,status,attempts,created_at,updated_at)
SELECT gen_random_uuid(),org_id,sync_run_id,integration_id,source_id,provider,'deployments',cost_class,mode,'planned',0,created_at,updated_at
FROM sync_run_units WHERE dataset_key = 'cicd'`},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newProofReplayHarness(t)
			h.ledger(t, "work-items", true)
			h.refresh(t)
			h.crashAfterCommit(t)
			h.refresh(t)
			mustExec(t, h.pool, test.mutate)
			if _, err := materializeAndCommit(t, h.fixture, h.materializer, h.occurrence); !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("replay err = %v, want ErrInvalidPlan", err)
			}
		})
	}
}

// A failed read of the committed rows fails the attempt with a plain error (retried), never ErrInvalidPlan.
func TestNativeMaterializerExecutedProofReplayReadFailureIsRetryable(t *testing.T) {
	h := newProofReplayHarness(t)
	h.ledger(t, "work-items", true)
	h.refresh(t)
	h.crashAfterCommit(t)
	h.refresh(t)

	ctx := context.Background()
	config := h.pool.Config().Copy()
	config.ConnConfig.RuntimeParams["lock_timeout"] = "200ms"
	reader, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	ids, err := deterministicMaterializationIDs(h.occurrence.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"sync_runs", "sync_run_units"} {
		t.Run(table, func(t *testing.T) {
			locker, err := h.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = locker.Rollback(ctx) }()
			if _, err := locker.Exec(ctx, `LOCK TABLE public.`+table+` IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			replay, err := loadCommittedUnits(ctx, reader, ids.SyncRunID)
			if err == nil || replay != nil || errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("read under a held lock: replay %+v err %v, want a plain error", replay, err)
			}
		})
	}
	if _, err := materializeAndCommit(t, h.fixture, h.materializer, h.occurrence); err != nil {
		t.Fatalf("replay once the read works: %v", err)
	}
}
