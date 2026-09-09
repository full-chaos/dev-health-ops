//go:build integration

package syncreconciler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// The live instance, column for column, from lane-stack-owner's read of the
// compose stack on 2026-09-09 (AFTER-syncrun-1410329c-shape.txt). These are
// not invented identifiers: they are the row this repair was written for.
const (
	liveRunID      = "1410329c-51aa-5895-89c7-b36442da361e"
	liveUnitID     = "e784daf9-592c-5fca-b6f7-81d2c9de940e"
	liveOutboxID   = "74e5f374-2c24-5561-8b76-5c7067fe856c"
	liveOrgID      = "70d529e0-3c06-4597-8480-794fd02328b6"
	liveSiblingOne = "e784daf9-592c-5fca-b6f7-81d2c9de9401"
	liveSiblingTwo = "e784daf9-592c-5fca-b6f7-81d2c9de9402"

	orphanRolePassword = "orphan-role-pass"
)

// liveBaseKey is the key NativeDispatchSyncRunService writes for this unit
// (jobcontract.KindSyncProviderUnit + ":" + unit.id). Written out rather than
// derived so a change to the dispatcher's convention fails these tests loudly
// instead of silently following it.
const liveBaseKey = "sync.provider_unit:" + liveUnitID

type orphanHarness struct {
	admin  *pgxpool.Pool
	domain *pgxpool.Pool
	queue  *pgxpool.Pool
	repair *OrphanedUnitRepair
	river  *river.Client[pgx.Tx]
}

// providerUnitRiverArgs is the minimal River-facing shape needed to make River
// itself mint a job row of the right kind. The job's ARGS are not read by this
// repair -- it reads id, kind, state and finalized_at -- but the row has to be
// produced BY River rather than hand-inserted, so that "the cleaner deleted a
// completed row" is modelled as River deleting a row River made, not as an id
// that never existed (the same discipline 5456's backstop harness records).
type providerUnitRiverArgs struct {
	UnitID string `json:"unit_id"`
}

func (providerUnitRiverArgs) Kind() string { return jobcontract.KindSyncProviderUnit }

func startOrphanHarness(t *testing.T, ctx context.Context) *orphanHarness {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	createOrphanFixture(t, ctx, admin)

	database, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{
		"sync_runs": true, "sync_run_units": true, "worker_job_outbox": true,
	}
	pools := map[string]*pgxpool.Pool{}
	for _, cfg := range []struct {
		name    string
		posture postgres.RolePosture
		isQueue bool
	}{
		// The postures are read from the SAME declaration
		// internal/storage/river/migrate.go derives production's GRANT
		// statements from, so this harness cannot drift into granting a
		// privilege production does not. That matters more here than in any
		// sibling test: this repair's entire pool split is an assertion about
		// those two manifests, and TestOrphanedUnitRepairPoolSplitIsForcedByGrants
		// below turns it into a measurement.
		{"orphan_domain", postgres.DomainPosture(), false},
		{"orphan_queue", postgres.QueuePosture(), true},
	} {
		role, err := containers.RoleName(cfg.name, instance)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
		statements := []string{
			"CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + orphanRolePassword + "'",
			"GRANT CONNECT ON DATABASE " + database + " TO " + role,
			"GRANT USAGE ON SCHEMA public TO " + role,
		}
		statements = append(statements, splitGrantStatements(role, cfg.posture, present)...)
		if cfg.isQueue {
			statements = append(statements,
				"GRANT USAGE ON SCHEMA river TO "+role,
				"GRANT SELECT, UPDATE ON ALL TABLES IN SCHEMA river TO "+role)
		}
		for _, sql := range statements {
			if _, err := admin.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
		pool, err := pgxpool.New(ctx, kernelRoleURI(t, instance.URI, role, orphanRolePassword))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		pools[cfg.name] = pool
	}

	repair, err := NewOrphanedUnitRepair(pools["orphan_domain"], pools["orphan_queue"], "river")
	if err != nil {
		t.Fatal(err)
	}
	client, err := river.NewClient(riverpgxv5.New(admin), &river.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	return &orphanHarness{
		admin: admin, domain: pools["orphan_domain"], queue: pools["orphan_queue"],
		repair: repair, river: client,
	}
}

// createOrphanFixture builds the three public tables this repair reads and
// writes, and the REAL River schema via rivermigrate.
//
// worker_job_outbox is copied from src/dev_health_ops/models/worker_job_outbox.py
// constraint for constraint, not trimmed to the columns the predicate happens
// to touch. Two of those constraints are load-bearing here and a fixture
// without them would prove nothing:
//
//   - ck_worker_job_outbox_delivery_state makes status='delivered' and
//     river_job_id IS NOT NULL the same statement, which is what lets the
//     survey read a NULL job.id as "the row was reaped" rather than as "the
//     link was never recorded".
//   - uq_worker_job_outbox_dedupe_key is the reason this repair mints a new key
//     at all. A fixture without it would happily accept a second row under the
//     same key and the whole design would look unnecessary.
//
// CREATE SCHEMA river comes BEFORE rivermigrate: rivermigrate does not create a
// non-default schema and fails with SQLSTATE 3F000 without it, which is a
// harness failure masquerading as product RED (the 09-08 invalid repro 5456
// recorded).
func createOrphanFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE public.sync_runs (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			status text NOT NULL,
			total_units int NOT NULL DEFAULT 0,
			completed_units int NOT NULL DEFAULT 0,
			failed_units int NOT NULL DEFAULT 0,
			created_at timestamptz NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE public.sync_run_units (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			sync_run_id uuid NOT NULL REFERENCES public.sync_runs(id),
			provider text NOT NULL,
			dataset_key text NOT NULL,
			cost_class text NOT NULL,
			mode text NOT NULL,
			status text NOT NULL,
			attempts integer NOT NULL DEFAULT 0,
			available_at timestamptz,
			error text,
			lease_owner text,
			lease_expires_at timestamptz,
			last_heartbeat_at timestamptz,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL
		)`,
		`CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY,
			dedupe_key varchar(256) NOT NULL,
			job_kind varchar(96) NOT NULL,
			contract_version integer NOT NULL,
			args json NOT NULL,
			payload_hash varchar(71) NOT NULL,
			queue varchar(96) NOT NULL,
			priority smallint NOT NULL,
			max_attempts smallint NOT NULL,
			scheduled_at timestamptz NOT NULL,
			status varchar(16) NOT NULL,
			claim_token uuid,
			claimed_at timestamptz,
			claim_expires_at timestamptz,
			attempt_count integer NOT NULL DEFAULT 0,
			first_attempt_at timestamptz,
			last_attempt_at timestamptz,
			next_attempt_at timestamptz NOT NULL,
			last_error_code varchar(64),
			last_error_detail varchar(256),
			last_error_at timestamptz,
			river_job_id bigint,
			delivered_at timestamptz,
			prerequisite_completion_key varchar(256),
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			CONSTRAINT uq_worker_job_outbox_dedupe_key UNIQUE (dedupe_key),
			CONSTRAINT uq_worker_job_outbox_river_job_id UNIQUE (river_job_id),
			CONSTRAINT ck_worker_job_outbox_status
				CHECK (status IN ('pending', 'claimed', 'delivered', 'dead')),
			CONSTRAINT ck_worker_job_outbox_contract_version CHECK (contract_version > 0),
			CONSTRAINT ck_worker_job_outbox_priority CHECK (priority BETWEEN 1 AND 4),
			CONSTRAINT ck_worker_job_outbox_max_attempts CHECK (max_attempts BETWEEN 1 AND 25),
			CONSTRAINT ck_worker_job_outbox_attempt_count CHECK (attempt_count >= 0),
			CONSTRAINT ck_worker_job_outbox_payload_hash
				CHECK (length(payload_hash) = 71 AND payload_hash LIKE 'sha256:%'),
			CONSTRAINT ck_worker_job_outbox_args_size
				CHECK (length(CAST(args AS TEXT)) <= 16384),
			CONSTRAINT ck_worker_job_outbox_claim_state CHECK (
				(status = 'claimed' AND claim_token IS NOT NULL AND claimed_at IS NOT NULL
					AND claim_expires_at IS NOT NULL)
				OR (status <> 'claimed' AND claim_token IS NULL AND claimed_at IS NULL
					AND claim_expires_at IS NULL)),
			CONSTRAINT ck_worker_job_outbox_delivery_state CHECK (
				(status = 'delivered' AND river_job_id IS NOT NULL AND delivered_at IS NOT NULL)
				OR (status <> 'delivered' AND river_job_id IS NULL AND delivered_at IS NULL)),
			CONSTRAINT ck_worker_job_outbox_error_state CHECK (
				(last_error_code IS NULL AND last_error_detail IS NULL AND last_error_at IS NULL)
				OR (last_error_code IS NOT NULL AND last_error_detail IS NOT NULL
					AND last_error_at IS NOT NULL))
		)`,
		`CREATE SCHEMA river`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		t.Fatal(err)
	}
}

// liveShape is the seed knob set. Its ZERO-adjusted default (liveShapeDefault)
// is the production row; every guard-matrix arm changes exactly ONE field, so a
// failing arm names the predicate it broke and nothing else.
type liveShape struct {
	runStatus string

	unitStatus      string
	unitAttempts    int
	unitLeaseOwner  *string
	unitLeaseExpiry *time.Time
	unitAvailableAt *time.Time
	unitHeartbeat   *time.Time
	unitCreatedAt   time.Time
	unitUpdatedAt   time.Time
	unitOrgID       string

	outboxStatus      string
	outboxKey         string
	outboxJobKind     string
	outboxDomainType  string
	outboxDomainID    string
	outboxOrgID       string
	outboxAttempts    int
	outboxPrerequisit string
	// outboxRawArgs bypasses jobcontract.MarshalCanonical. It exists for the
	// one guard-matrix arm whose shape the Go contract layer REFUSES to mint --
	// a domain.type other than "sync_run_unit" -- while Postgres will happily
	// hold it: worker_job_outbox.args is a json column with a size CHECK and
	// nothing else, and these rows were originally written by the Python
	// producer. "The current Go encoder cannot produce it" is not "the table
	// cannot contain it", and the predicate has to survive the difference.
	outboxRawArgs string

	// riverState is one of "missing", "completed", "completed_unfinalized",
	// "running", "discarded", "cancelled". "missing" models River's cleaner
	// deleting a row it had itself completed.
	riverState string
	riverKind  string
}

func liveShapeDefault(now time.Time) liveShape {
	// The live row's own clock: created 2026-08-30 12:00:00Z, delivered
	// 12:04:14Z, last touched by the redispatch loop 2026-09-09 02:36:19Z.
	// Expressed relative to `now` (CHAOS's own hardcoded-fixture-date trap:
	// an absolute date plus a rolling window is a bomb with a UTC-midnight
	// fuse).
	heartbeat := now.Add(-9 * 24 * time.Hour)
	return liveShape{
		runStatus:        "dispatching",
		unitStatus:       "dispatching",
		unitAttempts:     2,
		unitHeartbeat:    &heartbeat,
		unitCreatedAt:    now.Add(-10 * 24 * time.Hour),
		unitUpdatedAt:    now.Add(-time.Minute), // the redispatch loop, one minute ago
		unitOrgID:        liveOrgID,
		outboxStatus:     "delivered",
		outboxKey:        liveBaseKey,
		outboxJobKind:    jobcontract.KindSyncProviderUnit,
		outboxDomainType: "sync_run_unit",
		outboxDomainID:   liveUnitID,
		outboxOrgID:      liveOrgID,
		outboxAttempts:   1,
		riverState:       "missing",
		riverKind:        jobcontract.KindSyncProviderUnit,
	}
}

func providerUnitEnvelopeJSON(t *testing.T, orgID, domainType, domainID, key string) string {
	t.Helper()
	org := orgID
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		OrganizationID:  &org,
		CorrelationID:   "sync-run:" + liveRunID,
		IdempotencyKey:  key,
		Domain:          jobcontract.DomainLink{Type: domainType, ID: domainID},
		Payload:         jobcontract.ProviderUnitPayload{UnitID: domainID},
	}
	encoded, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

// rawPayloadHash hashes an args string that jobcontract cannot round-trip. It
// satisfies ck_worker_job_outbox_payload_hash without claiming the row is
// relay-acceptable -- the arm using it asserts the row is REFUSED, so it never
// reaches the relay.
func rawPayloadHash(args string) string {
	digest := sha256.Sum256([]byte(args))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func payloadHashOf(t *testing.T, args string) string {
	t.Helper()
	_, hash, err := reclaimEnvelopeForTest(args)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

// reclaimEnvelopeForTest re-hashes an args string WITHOUT changing its key, so
// the fixture's payload_hash is computed by the same function the product uses
// -- a hand-written hash would let a canonicalisation change pass unnoticed.
func reclaimEnvelopeForTest(args string) (string, string, error) {
	decoded, err := jobcontract.Decode(jobcontract.KindSyncProviderUnit, []byte(args))
	if err != nil {
		return "", "", err
	}
	return reclaimEnvelope(args, decoded.IdempotencyKey)
}

func (h *orphanHarness) seed(t *testing.T, ctx context.Context, now time.Time, shape liveShape) int64 {
	t.Helper()
	if _, err := h.admin.Exec(ctx, `
		DELETE FROM public.worker_job_outbox;
		DELETE FROM public.sync_run_units;
		DELETE FROM public.sync_runs;
		DELETE FROM river.river_job;`); err != nil {
		t.Fatal(err)
	}
	if _, err := h.admin.Exec(ctx,
		`INSERT INTO public.sync_runs (id, org_id, status, total_units, completed_units, failed_units, created_at)
		 VALUES ($1, $2, $3, 63, 62, 0, $4)`,
		liveRunID, liveOrgID, shape.runStatus, now.Add(-10*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// Two of the 62 successful siblings. They are not read by any predicate;
	// they are here so the fixture is recognisably 1410329c and so a query that
	// accidentally selected on the RUN rather than the UNIT would show up.
	for _, sibling := range []string{liveSiblingOne, liveSiblingTwo} {
		if _, err := h.admin.Exec(ctx, `
			INSERT INTO public.sync_run_units
				(id, org_id, sync_run_id, provider, dataset_key, cost_class, mode, status,
				 attempts, created_at, updated_at)
			VALUES ($1, $2, $3, 'github', 'repositories', 'standard', 'incremental', 'success',
				1, $4, $4)`,
			sibling, liveOrgID, liveRunID, now.Add(-10*24*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.admin.Exec(ctx, `
		INSERT INTO public.sync_run_units
			(id, org_id, sync_run_id, provider, dataset_key, cost_class, mode, status,
			 attempts, available_at, error, lease_owner, lease_expires_at, last_heartbeat_at,
			 created_at, updated_at)
		VALUES ($1, $2, $3, 'github', 'cicd', 'standard', 'incremental', $4,
			$5, $6, 'provider_unit_retryable', $7, $8, $9, $10, $11)`,
		liveUnitID, shape.unitOrgID, liveRunID, shape.unitStatus,
		shape.unitAttempts, shape.unitAvailableAt, shape.unitLeaseOwner, shape.unitLeaseExpiry,
		shape.unitHeartbeat, shape.unitCreatedAt, shape.unitUpdatedAt); err != nil {
		t.Fatal(err)
	}

	var jobID int64
	if shape.riverState != "" {
		inserted, err := h.river.Insert(ctx, providerUnitRiverArgs{UnitID: liveUnitID},
			&river.InsertOpts{Queue: "sync_provider"})
		if err != nil {
			t.Fatal(err)
		}
		jobID = inserted.Job.ID
		if shape.riverKind != jobcontract.KindSyncProviderUnit {
			if _, err := h.admin.Exec(ctx,
				`UPDATE river.river_job SET kind=$2 WHERE id=$1`, jobID, shape.riverKind); err != nil {
				t.Fatal(err)
			}
		}
		finalized := now.Add(-9 * 24 * time.Hour)
		switch shape.riverState {
		case "missing":
			if _, err := h.admin.Exec(ctx,
				`UPDATE river.river_job SET state='completed', finalized_at=$2 WHERE id=$1`,
				jobID, finalized); err != nil {
				t.Fatal(err)
			}
			if _, err := h.admin.Exec(ctx, `DELETE FROM river.river_job WHERE id=$1`, jobID); err != nil {
				t.Fatal(err)
			}
		case "completed", "discarded", "cancelled":
			if _, err := h.admin.Exec(ctx,
				`UPDATE river.river_job SET state=$2, finalized_at=$3 WHERE id=$1`,
				jobID, shape.riverState, finalized); err != nil {
				t.Fatal(err)
			}
		case "completed_unfinalized":
			// River's own CHECK forbids this on the real table, which is the
			// point: if it ever stops forbidding it, this seed starts
			// succeeding and the arm below becomes a live guard instead of a
			// documented impossibility.
			if _, err := h.admin.Exec(ctx,
				`UPDATE river.river_job SET state='completed', finalized_at=NULL WHERE id=$1`,
				jobID); err != nil {
				t.Skipf("river forbids a completed job with no finalized_at: %v", err)
			}
		case "running":
			if _, err := h.admin.Exec(ctx,
				`UPDATE river.river_job SET state='running', attempted_at=$2 WHERE id=$1`,
				jobID, now.Add(-time.Minute)); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unknown river state %q", shape.riverState)
		}
	}

	args := shape.outboxRawArgs
	hash := ""
	if args == "" {
		args = providerUnitEnvelopeJSON(t, shape.outboxOrgID, shape.outboxDomainType,
			shape.outboxDomainID, shape.outboxKey)
		hash = payloadHashOf(t, args)
	} else {
		hash = rawPayloadHash(args)
	}
	var riverJobID any
	var deliveredAt any
	if shape.outboxStatus == "delivered" {
		riverJobID = jobID
		deliveredAt = now.Add(-10 * 24 * time.Hour).Add(4 * time.Minute)
	}
	var prerequisite any
	if shape.outboxPrerequisit != "" {
		prerequisite = shape.outboxPrerequisit
	}
	if _, err := h.admin.Exec(ctx, `
		INSERT INTO public.worker_job_outbox
			(id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority,
			 max_attempts, scheduled_at, status, attempt_count, next_attempt_at, river_job_id,
			 delivered_at, prerequisite_completion_key, created_at, updated_at)
		VALUES ($1, $2, $3, 1, $4::json, $5, 'sync_provider', 2, 5, $6, $7, $8, $6, $9, $10, $11, $6, $6)`,
		liveOutboxID, shape.outboxKey, shape.outboxJobKind, args, hash,
		now.Add(-10*24*time.Hour), shape.outboxStatus, shape.outboxAttempts,
		riverJobID, deliveredAt, prerequisite); err != nil {
		t.Fatal(err)
	}
	return jobID
}

func (h *orphanHarness) step(t *testing.T, ctx context.Context, now time.Time) OrphanedUnitRepairResult {
	t.Helper()
	result, err := h.repair.Step(ctx, now, 10)
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	return result
}

func (h *orphanHarness) rowsForUnit(t *testing.T, ctx context.Context) []map[string]any {
	t.Helper()
	rows, err := h.admin.Query(ctx, `
		SELECT id::text, dedupe_key, job_kind, contract_version, args::text, payload_hash,
			queue, priority, max_attempts, status, attempt_count, river_job_id,
			COALESCE(prerequisite_completion_key, '')
		FROM public.worker_job_outbox
		ORDER BY dedupe_key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, key, kind, args, hash, queue, status, prerequisite string
		var version, priority, maxAttempts, attemptCount int
		var riverJobID *int64
		if err := rows.Scan(&id, &key, &kind, &version, &args, &hash, &queue, &priority,
			&maxAttempts, &status, &attemptCount, &riverJobID, &prerequisite); err != nil {
			t.Fatal(err)
		}
		out = append(out, map[string]any{
			"id": id, "dedupe_key": key, "job_kind": kind, "contract_version": version,
			"args": args, "payload_hash": hash, "queue": queue, "priority": priority,
			"max_attempts": maxAttempts, "status": status, "attempt_count": attemptCount,
			"river_job_id": riverJobID, "prerequisite_completion_key": prerequisite,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// ---------------------------------------------------------------------------
// GREEN: the live shape, both River verdicts
// ---------------------------------------------------------------------------

func TestOrphanedUnitRepairRearmsTheLive1410329cShape(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h := startOrphanHarness(t, ctx)
	now := time.Date(2026, time.September, 9, 3, 0, 0, 0, time.UTC)

	for _, verdict := range []struct {
		state          string
		wantMissing    int
		wantCompleted  int
		wantEvidenceIn string
	}{
		{"missing", 1, 0, dispositionOrphanMissing},
		{"completed", 0, 1, dispositionOrphanCompleted},
	} {
		t.Run(verdict.state, func(t *testing.T) {
			shape := liveShapeDefault(now)
			shape.riverState = verdict.state
			h.seed(t, ctx, now, shape)

			result := h.step(t, ctx, now)
			if result.Found != 1 || result.ReArmed != 1 {
				t.Fatalf("found=%d rearmed=%d, want 1/1 (%+v)", result.Found, result.ReArmed, result)
			}
			if result.DeliveryMissing != verdict.wantMissing ||
				result.DeliveryCompleted != verdict.wantCompleted {
				t.Fatalf("missing=%d completed=%d, want %d/%d",
					result.DeliveryMissing, result.DeliveryCompleted,
					verdict.wantMissing, verdict.wantCompleted)
			}

			rows := h.rowsForUnit(t, ctx)
			if len(rows) != 2 {
				t.Fatalf("outbox rows = %d, want 2 (the terminal row plus its replacement)", len(rows))
			}
			// The terminal row is untouched: never patched in place.
			source := rows[0]
			if source["dedupe_key"] != liveBaseKey || source["status"] != "delivered" ||
				source["river_job_id"] == nil || source["attempt_count"] != 1 {
				t.Fatalf("the terminal row was mutated: %+v", source)
			}
			replacement := rows[1]
			wantKey := liveBaseKey + "/reclaim/1"
			if replacement["dedupe_key"] != wantKey {
				t.Fatalf("replacement key = %v, want %q", replacement["dedupe_key"], wantKey)
			}
			if replacement["status"] != "pending" || replacement["attempt_count"] != 0 ||
				replacement["river_job_id"] != (*int64)(nil) {
				t.Fatalf("replacement is not an executable pending row: %+v", replacement)
			}
			// Immutable envelope fields are COPIED, not re-derived.
			for _, field := range []string{"job_kind", "contract_version", "queue", "priority", "max_attempts"} {
				if fmt.Sprint(replacement[field]) != fmt.Sprint(source[field]) {
					t.Errorf("%s = %v on the replacement, %v on the source", field,
						replacement[field], source[field])
				}
			}
			assertRelayAcceptable(t, replacement)
			assertUnitIsClaimable(t, ctx, h, now)
		})
	}
}

// assertUnitIsClaimable is the reachability half of the proof, and it is the
// half that is easy to skip: writing a replacement row proves a row was
// written, not that the strand can clear.
//
// It re-states the candidate predicate of
// internal/providersync/repository_postgres.go's claimUnitSQL -- the CTE the
// provider-unit handler's own Claim runs -- and asserts the unit this repair
// just re-armed satisfies it. If it did not, the replacement delivery would
// reach the handler, fail to claim, and return ErrUnitNotClaimable: a fresh
// no-op in place of the old one, and every other assertion in this file would
// still pass.
//
// ops/AGENTS.md states this bar directly: "a cited constructor is not proof of
// capability... prove reachability."
func assertUnitIsClaimable(t *testing.T, ctx context.Context, h *orphanHarness, now time.Time) {
	t.Helper()
	var claimable bool
	if err := h.admin.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM public.sync_run_units AS unit
			JOIN public.sync_runs AS run
				ON run.id = unit.sync_run_id AND run.org_id = unit.org_id
			WHERE unit.id = $1::uuid
				AND run.status NOT IN ('success', 'partial_failed', 'failed')
				AND unit.status = 'dispatching'
				AND (unit.available_at IS NULL OR unit.available_at <= $2)
		)`, liveUnitID, now).Scan(&claimable); err != nil {
		t.Fatal(err)
	}
	if !claimable {
		t.Fatal("the re-armed unit does not satisfy claimUnitSQL's candidate predicate, so the " +
			"replacement delivery would reach the handler and be refused as ErrUnitNotClaimable -- " +
			"a new no-op replacing the old one")
	}
}

// assertRelayAcceptable re-runs, on the row this repair wrote, exactly the
// three identity checks internal/joboutbox's relay (prepareRow) runs before it
// will insert anything into River. A replacement row that fails any of them
// would be refused forever -- a strand replacing a strand -- and nothing else
// in this test would notice, because every OTHER assertion here is about the
// row's shape rather than about whether the relay will accept it.
func assertRelayAcceptable(t *testing.T, row map[string]any) {
	t.Helper()
	key, _ := row["dedupe_key"].(string)
	args, _ := row["args"].(string)
	envelope, err := jobcontract.Decode(jobcontract.KindSyncProviderUnit, []byte(args))
	if err != nil {
		t.Fatalf("relay would reject the replacement args: %v", err)
	}
	if envelope.IdempotencyKey != key {
		t.Errorf("envelope idempotency_key %q != dedupe_key %q -- prepareRow refuses this row",
			envelope.IdempotencyKey, key)
	}
	_, hash, err := reclaimEnvelope(args, key)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if row["payload_hash"] != hash {
		t.Errorf("payload_hash %v != canonical hash %v -- prepareRow refuses this row",
			row["payload_hash"], hash)
	}
	if row["id"] != joboutbox.OutboxRowID(key).String() {
		t.Errorf("row id %v is not OutboxRowID(%q) = %v -- the producer would mint a DIFFERENT id "+
			"for this key, so a later republish would collide instead of dedupe",
			row["id"], key, joboutbox.OutboxRowID(key))
	}
	// The domain link must still name the unit. A replacement that re-pointed
	// it would deliver work against the wrong row.
	if envelope.Domain.Type != "sync_run_unit" || envelope.Domain.ID != liveUnitID {
		t.Errorf("replacement domain link = %+v, want sync_run_unit/%s", envelope.Domain, liveUnitID)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(args), &payload); err != nil {
		t.Fatal(err)
	}
	if correlation, _ := payload["correlation_id"].(string); correlation != "sync-run:"+liveRunID {
		t.Errorf("correlation_id = %q, want the source run's -- the replacement belongs to the "+
			"same investigation as the delivery it replaces", correlation)
	}
}

// ---------------------------------------------------------------------------
// PREMISE: the shape really is unreachable by the paths that already exist
// ---------------------------------------------------------------------------

// TestLive1410329cShapeIsUnreachableByEveryExistingRepairPath is the RED half.
// It does not argue from the source that the sibling repairs cannot see this
// row; it RUNS their own SQL against the fixture and measures zero.
//
// Each subtest names the structural reason, so a future change that makes one
// of them able to reach this population fails here rather than silently
// creating a second claimant on the same row.
func TestLive1410329cShapeIsUnreachableByEveryExistingRepairPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h := startOrphanHarness(t, ctx)
	now := time.Date(2026, time.September, 9, 3, 0, 0, 0, time.UTC)
	shape := liveShapeDefault(now)
	h.seed(t, ctx, now, shape)

	// joboutbox.StrandRepair's provider-unit shape and UnreclaimableSweep's
	// dead-delivery read are both INNER JOINs onto river_job, and both
	// additionally require unit.attempts = 0. The live unit has attempts = 2
	// and no river_job row at all, so each of them fails on TWO independent
	// clauses. Run the queries directly through the queue role rather than
	// constructing the whole components: the components need coordinator
	// tables and route rows this fixture deliberately does not have, and it is
	// the PREDICATE that is on trial here, not their wiring.
	for _, probe := range []struct {
		name  string
		query string
	}{
		{
			name: "strand_repair_inner_joins_river_job",
			query: `SELECT count(*) FROM public.worker_job_outbox AS outbox
				JOIN public.sync_run_units AS unit ON unit.id::text = outbox.args #>> '{domain,id}'
				JOIN river.river_job AS job ON job.id = outbox.river_job_id
				WHERE outbox.job_kind = 'sync.provider_unit'
					AND outbox.status = 'delivered'
					AND unit.status = 'dispatching'`,
		},
		{
			name: "strand_repair_requires_unit_attempts_zero",
			query: `SELECT count(*) FROM public.sync_run_units AS unit
				WHERE unit.id::text = $1 AND unit.status = 'dispatching' AND unit.attempts = 0`,
		},
		{
			name: "sweep_requires_unit_attempts_zero",
			query: `SELECT count(*) FROM public.sync_run_units AS unit
				WHERE unit.id::text = $1 AND unit.status = 'dispatching'
					AND unit.lease_owner IS NULL AND unit.attempts = 0`,
		},
		{
			name: "terminal_delivery_repair_requires_a_discarded_job",
			query: `SELECT count(*) FROM public.worker_job_outbox AS outbox
				JOIN river.river_job AS job ON job.id = outbox.river_job_id
				WHERE outbox.dedupe_key = $1 AND job.state::text = 'discarded'`,
		},
	} {
		t.Run(probe.name, func(t *testing.T) {
			var count int
			args := []any{}
			if strings.Contains(probe.query, "$1") {
				if strings.Contains(probe.query, "dedupe_key") {
					args = append(args, liveBaseKey)
				} else {
					args = append(args, liveUnitID)
				}
			}
			if err := h.queue.QueryRow(ctx, probe.query, args...).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("%s selected %d rows; this population is supposed to be "+
					"unreachable by it, and if that has changed the disjointness argument "+
					"in orphaned_unit_repair.go needs rewriting, not this assertion relaxing",
					probe.name, count)
			}
		})
	}

	// ...and this repair reaches it.
	if result := h.step(t, ctx, now); result.ReArmed != 1 {
		t.Fatalf("OrphanedUnitRepair rearmed %d, want 1 -- the population no path could "+
			"reach must be reachable by this one, or the ticket is not fixed", result.ReArmed)
	}
}

// ---------------------------------------------------------------------------
// The pool split is a GRANT, not a preference
// ---------------------------------------------------------------------------

// TestOrphanedUnitRepairPoolSplitIsForcedByGrants measures the claim the whole
// two-pool design rests on, against roles built from the SAME posture
// declarations production's migration derives its GRANTs from
// (postgres.DomainPosture / postgres.QueuePosture).
//
// If either half of this ever stops holding, the component should collapse to
// one pool -- and this test failing is how anyone would find out. Today it is
// asserted in a doc comment and nowhere else, which is exactly the shape
// CHAOS-4035 shipped.
func TestOrphanedUnitRepairPoolSplitIsForcedByGrants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h := startOrphanHarness(t, ctx)
	now := time.Date(2026, time.September, 9, 3, 0, 0, 0, time.UTC)
	h.seed(t, ctx, now, liveShapeDefault(now))

	t.Run("queue_role_cannot_insert_the_replacement", func(t *testing.T) {
		_, err := h.queue.Exec(ctx, `
			INSERT INTO public.worker_job_outbox
				(id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority,
				 max_attempts, scheduled_at, status, attempt_count, next_attempt_at, created_at, updated_at)
			SELECT gen_random_uuid(), 'probe', job_kind, contract_version, args, payload_hash, queue,
				priority, max_attempts, scheduled_at, 'pending', 0, scheduled_at, scheduled_at, scheduled_at
			FROM public.worker_job_outbox LIMIT 1`)
		assertInsufficientPrivilege(t, err, "queue role INSERT on worker_job_outbox")
	})

	t.Run("domain_role_cannot_read_river_job", func(t *testing.T) {
		var count int
		err := h.domain.QueryRow(ctx, `SELECT count(*) FROM river.river_job`).Scan(&count)
		assertInsufficientPrivilege(t, err, "domain role SELECT on river.river_job")
	})
}

func assertInsufficientPrivilege(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s SUCCEEDED. The two-pool split in orphaned_unit_repair.go is justified "+
			"by this being impossible; if the grants have widened, the justification is stale "+
			"and the component should be reconsidered, not this test deleted.", what)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("%s failed with a non-Postgres error %v", what, err)
	}
	// 42501 insufficient_privilege, 3F000 invalid_schema_name (a role with no
	// USAGE on the schema cannot even resolve it).
	if pgErr.Code != "42501" && pgErr.Code != "3F000" {
		t.Fatalf("%s failed with SQLSTATE %s, want 42501 or 3F000", what, pgErr.Code)
	}
}

// ---------------------------------------------------------------------------
// GUARD MATRIX: one arm per predicate, each arm mutating exactly one fact
// ---------------------------------------------------------------------------

// TestOrphanedUnitRepairGuardMatrix is the pin behind every clause of
// selectOrphanedUnitDeliverySQL, lockOrphanedUnitSQL and
// readOrphanedSourceRowSQL. Each arm changes ONE field of the live shape and
// states which counter must move; deleting the corresponding clause from the
// product turns that arm -- and only that arm -- red.
//
// The arms are DATA mutations rather than source mutations on purpose. A source
// mutation proves a clause is load-bearing; a data mutation proves the same
// thing AND stays in the tree as a regression test, which is what the standing
// "the repro becomes the regression test" rule asks for. The source-mutation
// sweep that cross-checks this matrix is recorded in the lane's evidence, not
// here.
func TestOrphanedUnitRepairGuardMatrix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	h := startOrphanHarness(t, ctx)
	now := time.Date(2026, time.September, 9, 3, 0, 0, 0, time.UTC)

	leaseOwner := "worker-1"
	leaseFuture := now.Add(time.Hour)
	availableFuture := now.Add(time.Hour)
	freshHeartbeat := now.Add(-time.Minute)
	oldCreated := now.Add(-10 * 24 * time.Hour)
	otherUnit := liveSiblingOne

	for _, arm := range []struct {
		name    string
		clause  string
		mutate  func(*liveShape)
		want    func(OrphanedUnitRepairResult) error
		preSeed func(*testing.T, context.Context)
	}{
		{
			name:   "river_job_still_running",
			clause: "the completed/finalized branch of the disposition CASE",
			mutate: func(s *liveShape) { s.riverState = "running" },
			want:   wantCounters(1, 0, func(r OrphanedUnitRepairResult) int { return r.SkippedJobLive }, "SkippedJobLive"),
		},
		{
			name:   "river_job_discarded_belongs_to_strand_repair",
			clause: "the 'discarded'/'cancelled' -> skip_other_repair disjointness arm",
			mutate: func(s *liveShape) { s.riverState = "discarded" },
			want:   wantCounters(1, 0, func(r OrphanedUnitRepairResult) int { return r.SkippedOtherRepair }, "SkippedOtherRepair"),
		},
		{
			name:   "river_job_cancelled_belongs_to_strand_repair",
			clause: "the 'discarded'/'cancelled' -> skip_other_repair disjointness arm",
			mutate: func(s *liveShape) { s.riverState = "cancelled" },
			want:   wantCounters(1, 0, func(r OrphanedUnitRepairResult) int { return r.SkippedOtherRepair }, "SkippedOtherRepair"),
		},
		{
			name:   "river_job_of_another_kind",
			clause: "job.kind <> outbox.job_kind -> skip_job_identity",
			mutate: func(s *liveShape) {
				s.riverState = "completed"
				s.riverKind = "metrics.daily_finalize"
			},
			want: wantCounters(1, 0, func(r OrphanedUnitRepairResult) int { return r.SkippedJobIdentity }, "SkippedJobIdentity"),
		},
		{
			name:   "unit_is_running",
			clause: "unit.status = 'dispatching'",
			mutate: func(s *liveShape) { s.unitStatus = "running" },
			want:   wantCounters(0, 0, nil, ""),
		},
		{
			name:   "unit_holds_a_live_lease",
			clause: "unit.lease_owner IS NULL AND unit.lease_expires_at IS NULL",
			mutate: func(s *liveShape) {
				s.unitLeaseOwner = &leaseOwner
				s.unitLeaseExpiry = &leaseFuture
			},
			want: wantCounters(0, 0, nil, ""),
		},
		// The two lease clauses get an arm EACH. The combined arm above sets
		// both fields, so deleting either clause alone still leaves the other
		// refusing the row -- the mutation sweep measured exactly that
		// (M06/M07 both survived until these arms existed). Two look-alike
		// predicates that mask each other's mutations is the finding 5456's
		// guard matrix recorded, and the remedy there was to collapse them;
		// here they cannot be collapsed, because they are the contract the
		// sibling provider-unit repairs state, so each is pinned separately
		// instead.
		{
			name:   "unit_holds_a_lease_owner_with_no_expiry",
			clause: "unit.lease_owner IS NULL, on its own",
			mutate: func(s *liveShape) { s.unitLeaseOwner = &leaseOwner },
			want:   wantCounters(0, 0, nil, ""),
		},
		{
			name:   "unit_holds_a_lease_expiry_with_no_owner",
			clause: "unit.lease_expires_at IS NULL, on its own",
			mutate: func(s *liveShape) { s.unitLeaseExpiry = &leaseFuture },
			want:   wantCounters(0, 0, nil, ""),
		},
		{
			name:   "run_already_terminal",
			clause: "nonterminalSyncRunStatusPredicate",
			mutate: func(s *liveShape) { s.runStatus = "success" },
			want:   wantCounters(0, 0, nil, ""),
		},
		{
			name:   "unit_available_at_in_the_future",
			clause: "unit.available_at IS NULL OR unit.available_at <= $1",
			mutate: func(s *liveShape) { s.unitAvailableAt = &availableFuture },
			want:   wantCounters(0, 0, nil, ""),
		},
		{
			name:   "heartbeat_is_fresh",
			clause: "COALESCE(unit.last_heartbeat_at, unit.created_at) <= $2",
			mutate: func(s *liveShape) { s.unitHeartbeat = &freshHeartbeat },
			want:   wantCounters(0, 0, nil, ""),
		},
		{
			name:   "no_heartbeat_and_freshly_created",
			clause: "the created_at fallback inside the idle COALESCE",
			mutate: func(s *liveShape) {
				s.unitHeartbeat = nil
				s.unitCreatedAt = now.Add(-time.Minute)
			},
			want: wantCounters(0, 0, nil, ""),
		},
		{
			name:   "no_heartbeat_but_long_lived",
			clause: "the created_at fallback admits a unit that never heartbeat",
			mutate: func(s *liveShape) {
				s.unitHeartbeat = nil
				s.unitCreatedAt = oldCreated
			},
			want: wantRearm(1),
		},
		{
			name: "redispatch_loop_keeps_updated_at_fresh",
			clause: "the idle gate hangs off last_heartbeat_at/created_at and NOT updated_at " +
				"(CHAOS-5453 remedy 3): the false-success redispatch loop bumps updated_at " +
				"every pass, so an updated_at gate would refuse this row forever",
			mutate: func(s *liveShape) { s.unitUpdatedAt = now },
			want:   wantRearm(1),
		},
		{
			name:   "outbox_row_is_not_delivered",
			clause: "outbox.status = 'delivered'",
			mutate: func(s *liveShape) {
				s.outboxStatus = "pending"
				s.riverState = ""
			},
			want: wantCounters(0, 0, nil, ""),
		},
		{
			name:   "outbox_row_names_another_kind",
			clause: "outbox.job_kind = 'sync.provider_unit'",
			mutate: func(s *liveShape) { s.outboxJobKind = "metrics.daily_finalize" },
			want:   wantCounters(0, 0, nil, ""),
		},
		{
			name:   "envelope_domain_type_is_wrong",
			clause: "outbox.args #>> '{domain,type}' = 'sync_run_unit'",
			mutate: func(s *liveShape) {
				s.outboxRawArgs = `{"contract_version":1,"correlation_id":"sync-run:` + liveRunID +
					`","domain":{"id":"` + liveUnitID + `","type":"daily_metrics_run"},` +
					`"idempotency_key":"` + liveBaseKey + `","organization_id":"` + liveOrgID +
					`","payload":{"unit_id":"` + liveUnitID + `"}}`
			},
			want: wantCounters(0, 0, nil, ""),
		},
		{
			// This one pins the CASE guard rather than the join value. Without
			// the regex, `(outbox.args #>> '{domain,id}')::uuid` faults on a
			// non-UUID and takes the WHOLE survey down with SQLSTATE 22P02 --
			// one poison row stopping every other unit's recovery, which is
			// strictly worse than the strand it was trying to fix. Seeded as
			// raw args because jobcontract will not mint a malformed domain id.
			name:   "envelope_domain_id_is_not_a_uuid",
			clause: "the uuid format guard on the domain-id join",
			mutate: func(s *liveShape) {
				s.outboxRawArgs = `{"contract_version":1,"correlation_id":"sync-run:` + liveRunID +
					`","domain":{"id":"not-a-uuid","type":"sync_run_unit"},` +
					`"idempotency_key":"` + liveBaseKey + `","organization_id":"` + liveOrgID +
					`","payload":{"unit_id":"` + liveUnitID + `"}}`
			},
			want: wantCounters(0, 0, nil, ""),
		},
		{
			name:   "envelope_names_a_different_unit",
			clause: "the domain-id join in providerUnitDomainIdentity",
			mutate: func(s *liveShape) {
				s.outboxDomainID = otherUnit
				s.outboxKey = "sync.provider_unit:" + otherUnit
			},
			want: wantCounters(0, 0, nil, ""),
		},
		{
			// The arm above moves the KEY with the domain id, so a mutation
			// that breaks the join still fails the key predicate and survives
			// (measured: M16). This one disagrees on the domain id ALONE --
			// the key still names the live unit -- so the join is the only
			// thing standing between the row and a delivery minted against
			// the wrong domain row. Raw args, because jobcontract will not
			// mint an envelope whose payload.unit_id and domain.id disagree.
			name:   "envelope_domain_id_disagrees_with_the_key",
			clause: "the domain-id join, isolated from the key predicate",
			mutate: func(s *liveShape) {
				s.outboxRawArgs = `{"contract_version":1,"correlation_id":"sync-run:` + liveRunID +
					`","domain":{"id":"` + otherUnit + `","type":"sync_run_unit"},` +
					`"idempotency_key":"` + liveBaseKey + `","organization_id":"` + liveOrgID +
					`","payload":{"unit_id":"` + otherUnit + `"}}`
			},
			want: wantCounters(0, 0, nil, ""),
		},
		{
			name:   "envelope_org_disagrees_with_the_unit",
			clause: "unit.org_id = outbox.args ->> 'organization_id'",
			mutate: func(s *liveShape) { s.outboxOrgID = "00000000-0000-4000-8000-00000000dead" },
			want:   wantCounters(0, 0, nil, ""),
		},
		{
			name:   "a_live_delivery_already_exists_for_the_unit",
			clause: "noLiveDeliveryForUnit",
			mutate: func(s *liveShape) {},
			preSeed: func(t *testing.T, ctx context.Context) {
				seedExtraOutboxRow(t, ctx, h, now, liveBaseKey+"/reclaim/1", "pending")
			},
			want: wantCounters(0, 0, nil, ""),
		},
		{
			name:   "reclaim_budget_is_spent",
			clause: "maxProviderUnitReclaims",
			mutate: func(s *liveShape) {
				s.outboxKey = liveBaseKey + "/reclaim/" + fmt.Sprint(maxProviderUnitReclaims)
			},
			want: wantCounters(1, 0, func(r OrphanedUnitRepairResult) int { return r.SkippedReclaimBudget }, "SkippedReclaimBudget"),
		},
		{
			name:   "source_row_carries_a_prerequisite_fence",
			clause: "the prerequisite_completion_key refusal",
			mutate: func(s *liveShape) { s.outboxPrerequisit = "metrics.daily_finalize:done" },
			want:   wantCounters(1, 0, func(r OrphanedUnitRepairResult) int { return r.SkippedPrerequisite }, "SkippedPrerequisite"),
		},
		{
			name:   "the_replacement_key_already_exists",
			clause: "the NOT EXISTS on the replacement key in readOrphanedSourceRowSQL",
			mutate: func(s *liveShape) {},
			preSeed: func(t *testing.T, ctx context.Context) {
				seedExtraOutboxRow(t, ctx, h, now, liveBaseKey+"/reclaim/1", "dead")
			},
			want: wantCounters(1, 0, func(r OrphanedUnitRepairResult) int { return r.SkippedRaceLost }, "SkippedRaceLost"),
		},
	} {
		t.Run(arm.name, func(t *testing.T) {
			shape := liveShapeDefault(now)
			arm.mutate(&shape)
			h.seed(t, ctx, now, shape)
			if arm.preSeed != nil {
				arm.preSeed(t, ctx)
			}
			result := h.step(t, ctx, now)
			if err := arm.want(result); err != nil {
				t.Fatalf("clause under test: %s\n%v\nresult: %+v", arm.clause, err, result)
			}
		})
	}
}

func wantCounters(
	found, rearmed int,
	counter func(OrphanedUnitRepairResult) int,
	counterName string,
) func(OrphanedUnitRepairResult) error {
	return func(result OrphanedUnitRepairResult) error {
		if result.Found != found {
			return fmt.Errorf("Found = %d, want %d", result.Found, found)
		}
		if result.ReArmed != rearmed {
			return fmt.Errorf("ReArmed = %d, want %d", result.ReArmed, rearmed)
		}
		if counter != nil && counter(result) != 1 {
			return fmt.Errorf("%s = %d, want 1 -- the refusal must be COUNTED, not filtered away",
				counterName, counter(result))
		}
		return nil
	}
}

func wantRearm(n int) func(OrphanedUnitRepairResult) error {
	return func(result OrphanedUnitRepairResult) error {
		if result.ReArmed != n {
			return fmt.Errorf("ReArmed = %d, want %d", result.ReArmed, n)
		}
		return nil
	}
}

func seedExtraOutboxRow(
	t *testing.T, ctx context.Context, h *orphanHarness, now time.Time, key, status string,
) {
	t.Helper()
	args := providerUnitEnvelopeJSON(t, liveOrgID, "sync_run_unit", liveUnitID, key)
	if _, err := h.admin.Exec(ctx, `
		INSERT INTO public.worker_job_outbox
			(id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority,
			 max_attempts, scheduled_at, status, attempt_count, next_attempt_at, created_at, updated_at)
		VALUES ($1, $2, 'sync.provider_unit', 1, $3::json, $4, 'sync_provider', 2, 5, $5, $6, 0, $5, $5, $5)`,
		joboutbox.OutboxRowID(key), key, args, payloadHashOf(t, args),
		now.Add(-time.Hour), status); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Bound and idempotence
// ---------------------------------------------------------------------------

// TestOrphanedUnitRepairIsIdempotentWithinAPass proves the same strand is not
// re-armed twice: the row this pass minted is 'pending', which
// noLiveDeliveryForUnit refuses, so the next pass finds nothing at all. Without
// that clause a reconciler ticking once a second would mint a delivery a second
// until the generation bound stopped it -- the "requeue into a void" failure in
// its fastest form.
func TestOrphanedUnitRepairIsIdempotentWithinAPass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h := startOrphanHarness(t, ctx)
	now := time.Date(2026, time.September, 9, 3, 0, 0, 0, time.UTC)
	h.seed(t, ctx, now, liveShapeDefault(now))

	if first := h.step(t, ctx, now); first.ReArmed != 1 {
		t.Fatalf("first pass rearmed %d, want 1", first.ReArmed)
	}
	second := h.step(t, ctx, now)
	if second.Found != 0 || second.ReArmed != 0 {
		t.Fatalf("second pass found=%d rearmed=%d, want 0/0 -- the replacement is pending, "+
			"so this unit already has an executable delivery", second.Found, second.ReArmed)
	}
	if rows := h.rowsForUnit(t, ctx); len(rows) != 2 {
		t.Fatalf("outbox rows = %d after two passes, want 2", len(rows))
	}
}

// TestOrphanedUnitRepairStopsAtItsReclaimBound walks the whole generation chain
// the way production would: each replacement is itself delivered and dies the
// same way, and the repair keeps recovering until its bound, then stops.
//
// The stop is the point. A recovery loop with no bound turns a broken transport
// into an invisible, self-renewing strand; this one turns it into a visible,
// finite one that a human is eventually required to look at.
func TestOrphanedUnitRepairStopsAtItsReclaimBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	h := startOrphanHarness(t, ctx)
	now := time.Date(2026, time.September, 9, 3, 0, 0, 0, time.UTC)
	h.seed(t, ctx, now, liveShapeDefault(now))

	for generation := 1; generation <= maxProviderUnitReclaims; generation++ {
		result := h.step(t, ctx, now)
		if result.ReArmed != 1 {
			t.Fatalf("generation %d: rearmed %d, want 1 (%+v)", generation, result.ReArmed, result)
		}
		key := fmt.Sprintf("%s/reclaim/%d", liveBaseKey, generation)
		// Model the replacement being delivered and then dying the same way:
		// a completed River job whose row the cleaner then removed.
		inserted, err := h.river.Insert(ctx, providerUnitRiverArgs{UnitID: liveUnitID},
			&river.InsertOpts{Queue: "sync_provider"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.admin.Exec(ctx,
			`UPDATE public.worker_job_outbox
			 SET status='delivered', river_job_id=$2, delivered_at=$3, attempt_count=1
			 WHERE dedupe_key=$1`, key, inserted.Job.ID, now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := h.admin.Exec(ctx,
			`UPDATE river.river_job SET state='completed', finalized_at=$2 WHERE id=$1`,
			inserted.Job.ID, now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := h.admin.Exec(ctx,
			`DELETE FROM river.river_job WHERE id=$1`, inserted.Job.ID); err != nil {
			t.Fatal(err)
		}
	}

	final := h.step(t, ctx, now)
	if final.ReArmed != 0 || final.SkippedReclaimBudget != 1 {
		t.Fatalf("past the bound: rearmed=%d skipped_reclaim_budget=%d, want 0/1 (%+v)",
			final.ReArmed, final.SkippedReclaimBudget, final)
	}
	rows := h.rowsForUnit(t, ctx)
	if len(rows) != maxProviderUnitReclaims+1 {
		t.Fatalf("outbox rows = %d, want %d (the original plus %d bounded replacements)",
			len(rows), maxProviderUnitReclaims+1, maxProviderUnitReclaims)
	}
}
