//go:build integration

package joboutbox

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

const (
	outboxDomainRole     = "outbox_domain_runtime"
	outboxQueueRole      = "outbox_queue_runtime"
	outboxDomainPassword = "outbox_domain_password"
	outboxQueuePassword  = "outbox_queue_password"
)

type failingRiverClient struct{ err error }

func (client failingRiverClient) InsertTx(
	context.Context,
	pgx.Tx,
	river.JobArgs,
	*river.InsertOpts,
) (*rivertype.JobInsertResult, error) {
	return nil, client.err
}

func TestGenericOutboxLiveFailureInjectionMatrix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()

	adminPool := openIntegrationPool(t, ctx, instance.URI)
	defer adminPool.Close()
	createOutboxSchema(t, ctx, adminPool)
	createOutboxRoles(t, ctx, adminPool)
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, riverstore.MigrationOptions{
		Schema:     "river",
		DomainRole: outboxDomainRole,
		QueueRole:  outboxQueueRole,
	}); err != nil {
		t.Fatal(err)
	}
	queueURI := integrationRoleURI(t, instance.URI, outboxQueueRole, outboxQueuePassword)
	queuePool := openIntegrationPool(t, ctx, queueURI)
	defer queuePool.Close()
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	repository, err := NewRepository(queuePool)
	if err != nil {
		t.Fatal(err)
	}
	inserter, err := NewRiverInserter(queuePool, "river", registry)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("concurrent claimers never overlap", func(t *testing.T) {
		resetOutboxTables(t, ctx, adminPool)
		now := time.Now().UTC().Truncate(time.Microsecond)
		for index := 1; index <= 20; index++ {
			seedOutbox(t, ctx, adminPool, normalSeed(index, now))
		}
		var wait sync.WaitGroup
		wait.Add(2)
		results := make(chan []Claim, 2)
		for range 2 {
			go func() {
				defer wait.Done()
				claims, claimErr := repository.ClaimDue(ctx, now, 20, 30*time.Second)
				if claimErr != nil {
					t.Errorf("ClaimDue(): %v", claimErr)
				}
				results <- claims
			}()
		}
		wait.Wait()
		close(results)
		seen := map[string]string{}
		for claims := range results {
			for _, claim := range claims {
				if prior, duplicate := seen[claim.ID]; duplicate {
					t.Fatalf("row claimed twice with tokens %s and %s", prior, claim.ClaimToken)
				}
				seen[claim.ID] = claim.ClaimToken
			}
		}
		if len(seen) != 20 {
			t.Fatalf("claimed %d rows, want 20", len(seen))
		}
	})

	t.Run("claim-only crash expires and stale owner cannot mark", func(t *testing.T) {
		resetOutboxTables(t, ctx, adminPool)
		now := time.Now().UTC().Truncate(time.Microsecond)
		seedOutbox(t, ctx, adminPool, normalSeed(30, now))
		first := claimOne(t, ctx, repository, now, 2*time.Second)
		if claims, err := repository.ClaimDue(ctx, now.Add(time.Second), 1, 2*time.Second); err != nil || len(claims) != 0 {
			t.Fatalf("unexpired claim was reclaimed: %#v %v", claims, err)
		}
		second := claimOne(t, ctx, repository, now.Add(3*time.Second), 2*time.Second)
		if second.ClaimToken == first.ClaimToken || second.AttemptCount != 2 {
			t.Fatalf("expired lease did not create a fresh attempt: %#v", second)
		}
		if _, err := repository.Dispatch(ctx, first, now.Add(3*time.Second), inserter.Insert); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("stale Dispatch() error = %v", err)
		}
		if _, err := repository.Dispatch(ctx, second, now.Add(3*time.Second), inserter.Insert); err != nil {
			t.Fatal(err)
		}
		assertCounts(t, ctx, adminPool, statusDelivered, 1, 1)
	})

	t.Run("dispatch cannot commit after its lease expires", func(t *testing.T) {
		resetOutboxTables(t, ctx, adminPool)
		now := time.Now().UTC().Truncate(time.Microsecond)
		seedOutbox(t, ctx, adminPool, normalSeed(35, now))
		claim := claimOne(t, ctx, repository, now, time.Second)
		slowInsert := func(ctx context.Context, tx pgx.Tx, row Row) (int64, error) {
			time.Sleep(1100 * time.Millisecond)
			return inserter.Insert(ctx, tx, row)
		}
		if _, err := repository.Dispatch(ctx, claim, now, slowInsert); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("expired in-flight Dispatch() error = %v", err)
		}
		assertCounts(t, ctx, adminPool, statusClaimed, 1, 0)
	})

	for _, crash := range []struct {
		name   string
		faults repositoryFaults
	}{
		{name: "after insert before mark", faults: repositoryFaults{afterInsert: injectedFault}},
		{name: "after mark before commit", faults: repositoryFaults{afterMark: injectedFault}},
	} {
		t.Run(crash.name+" rolls back both sides and reclaims", func(t *testing.T) {
			resetOutboxTables(t, ctx, adminPool)
			now := time.Now().UTC().Truncate(time.Microsecond)
			seedOutbox(t, ctx, adminPool, normalSeed(40, now))
			claim := claimOne(t, ctx, repository, now, 2*time.Second)
			faulting := &Repository{pool: queuePool, faults: crash.faults}
			if _, err := faulting.Dispatch(ctx, claim, now, inserter.Insert); !errors.Is(err, errInjectedCrash) {
				t.Fatalf("faulting Dispatch() error = %v", err)
			}
			assertCounts(t, ctx, adminPool, statusClaimed, 1, 0)
			reclaimed := claimOne(t, ctx, repository, now.Add(3*time.Second), 2*time.Second)
			if _, err := repository.Dispatch(ctx, reclaimed, now.Add(3*time.Second), inserter.Insert); err != nil {
				t.Fatal(err)
			}
			assertCounts(t, ctx, adminPool, statusDelivered, 1, 1)
		})
	}

	t.Run("commit-before-ack crash is already converged", func(t *testing.T) {
		resetOutboxTables(t, ctx, adminPool)
		now := time.Now().UTC().Truncate(time.Microsecond)
		seedOutbox(t, ctx, adminPool, normalSeed(50, now))
		claim := claimOne(t, ctx, repository, now, 2*time.Second)
		faulting := &Repository{pool: queuePool, faults: repositoryFaults{afterCommit: injectedFault}}
		if _, err := faulting.Dispatch(ctx, claim, now, inserter.Insert); !errors.Is(err, errInjectedCrash) {
			t.Fatalf("post-commit Dispatch() error = %v", err)
		}
		assertCounts(t, ctx, adminPool, statusDelivered, 1, 1)
		claims, err := repository.ClaimDue(ctx, now.Add(10*time.Second), 1, 2*time.Second)
		if err != nil || len(claims) != 0 {
			t.Fatalf("delivered row became claimable: %#v %v", claims, err)
		}
	})

	t.Run("duplicate retry verifies and reuses one River job", func(t *testing.T) {
		resetOutboxTables(t, ctx, adminPool)
		now := time.Now().UTC().Truncate(time.Microsecond)
		seed := normalSeed(60, now)
		seedOutbox(t, ctx, adminPool, seed)
		first := claimOne(t, ctx, repository, now, 2*time.Second)
		jobID, err := repository.Dispatch(ctx, first, now, inserter.Insert)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := adminPool.Exec(ctx, `
			UPDATE public.worker_job_outbox
			SET status='pending', river_job_id=NULL, delivered_at=NULL,
				claim_token=NULL, claimed_at=NULL, claim_expires_at=NULL,
				next_attempt_at=$2, updated_at=$2
			WHERE id=$1`, seed.ID, now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		second := claimOne(t, ctx, repository, now.Add(time.Second), 2*time.Second)
		duplicateID, err := repository.Dispatch(ctx, second, now.Add(time.Second), inserter.Insert)
		if err != nil {
			t.Fatal(err)
		}
		if duplicateID != jobID {
			t.Fatalf("duplicate insert returned job %d, want %d", duplicateID, jobID)
		}
		assertCounts(t, ctx, adminPool, statusDelivered, 1, 1)
	})

	t.Run("existing unique job with different outbox identity fails closed", func(t *testing.T) {
		resetOutboxTables(t, ctx, adminPool)
		now := time.Now().UTC().Truncate(time.Microsecond)
		firstSeed := normalSeed(70, now)
		seedOutbox(t, ctx, adminPool, firstSeed)
		first := claimOne(t, ctx, repository, now, 2*time.Second)
		if _, err := repository.Dispatch(ctx, first, now, inserter.Insert); err != nil {
			t.Fatal(err)
		}
		if _, err := adminPool.Exec(ctx, "DELETE FROM public.worker_job_outbox WHERE id=$1", firstSeed.ID); err != nil {
			t.Fatal(err)
		}
		secondSeed := firstSeed
		secondSeed.ID = integrationUUID(71)
		seedOutbox(t, ctx, adminPool, secondSeed)
		relay, err := NewRelay(repository, inserter, DefaultRelayConfig())
		if err != nil {
			t.Fatal(err)
		}
		result, err := relay.Step(ctx, now.Add(time.Second), 1)
		if err != nil || result.Dead != 1 {
			t.Fatalf("Step() = %#v, %v", result, err)
		}
		assertCounts(t, ctx, adminPool, statusDead, 1, 1)
	})

	t.Run("unknown kind and version become redacted terminal rows", func(t *testing.T) {
		resetOutboxTables(t, ctx, adminPool)
		now := time.Now().UTC().Truncate(time.Microsecond)
		unknown := normalSeed(80, now)
		unknown.Kind = "secret.unknown"
		seedOutbox(t, ctx, adminPool, unknown)
		version := normalSeed(81, now)
		version.Version = 99
		seedOutbox(t, ctx, adminPool, version)
		relay, err := NewRelay(repository, inserter, DefaultRelayConfig())
		if err != nil {
			t.Fatal(err)
		}
		result, err := relay.Step(ctx, now, 10)
		if err != nil || result.Dead != 2 {
			t.Fatalf("Step() = %#v, %v", result, err)
		}
		assertErrorEvidence(t, ctx, adminPool, "contract_rejected", "stored job contract was rejected")
	})

	t.Run("driver error is never persisted or returned", func(t *testing.T) {
		resetOutboxTables(t, ctx, adminPool)
		now := time.Now().UTC().Truncate(time.Microsecond)
		seedOutbox(t, ctx, adminPool, normalSeed(90, now))
		secret := "postgres://queue:super-secret@example.invalid/app"
		failingInserter := &RiverInserter{
			client:   failingRiverClient{err: errors.New(secret)},
			registry: registry,
		}
		relay, err := NewRelay(repository, failingInserter, DefaultRelayConfig())
		if err != nil {
			t.Fatal(err)
		}
		result, err := relay.Step(ctx, now, 1)
		if err != nil || result.Retried != 1 || strings.Contains(fmt.Sprint(err), secret) {
			t.Fatalf("Step() = %#v, %v", result, err)
		}
		assertErrorEvidence(t, ctx, adminPool, "river_insert_failed", "queue insertion failed")
		var stored string
		if err := adminPool.QueryRow(ctx, "SELECT last_error_detail FROM public.worker_job_outbox").Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(stored, "secret") || strings.Contains(stored, "postgres://") {
			t.Fatalf("stored error leaked driver value: %q", stored)
		}
	})

	t.Run("terminal retention is bounded", func(t *testing.T) {
		resetOutboxTables(t, ctx, adminPool)
		now := time.Now().UTC().Truncate(time.Microsecond)
		for index := 100; index < 103; index++ {
			seedOutbox(t, ctx, adminPool, normalSeed(index, now))
			claim := claimOne(t, ctx, repository, now, 2*time.Second)
			if _, err := repository.Dispatch(ctx, claim, now, inserter.Insert); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := adminPool.Exec(ctx, `
			UPDATE public.worker_job_outbox
			SET delivered_at=$1, updated_at=$1
			WHERE status='delivered'`, now.Add(-48*time.Hour)); err != nil {
			t.Fatal(err)
		}
		deleted, err := repository.DeleteTerminalBefore(ctx, now.Add(-24*time.Hour), 2)
		if err != nil || deleted != 2 {
			t.Fatalf("DeleteTerminalBefore() = %d, %v", deleted, err)
		}
		assertCounts(t, ctx, adminPool, statusDelivered, 1, 3)
	})
}

type outboxSeed struct {
	ID          string
	DedupeKey   string
	Kind        string
	Version     int
	Args        []byte
	PayloadHash string
	Queue       string
	Priority    int
	MaxAttempts int
	ScheduledAt time.Time
}

func normalSeed(index int, now time.Time) outboxSeed {
	idempotency := fmt.Sprintf("heartbeat:integration:%d", index)
	envelope := jobcontract.Envelope{
		ContractVersion: 1,
		CorrelationID:   fmt.Sprintf("outbox-integration-%d", index),
		IdempotencyKey:  idempotency,
		Domain: jobcontract.DomainLink{
			Type: "schedule_occurrence",
			ID:   integrationUUID(1000 + index),
		},
		Payload: jobcontract.HeartbeatPayload{ScheduledFor: now.Format(time.RFC3339)},
	}
	args, err := jobcontract.MarshalCanonical(envelope)
	if err != nil {
		panic(err)
	}
	return outboxSeed{
		ID:          integrationUUID(index),
		DedupeKey:   idempotency,
		Kind:        jobcontract.KindHeartbeat,
		Version:     1,
		Args:        args,
		PayloadHash: canonicalHash(args),
		Queue:       "heartbeat",
		Priority:    2,
		MaxAttempts: 1,
		ScheduledAt: now,
	}
}

func integrationUUID(index int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", index)
}

func seedOutbox(t *testing.T, ctx context.Context, pool *pgxpool.Pool, seed outboxSeed) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO public.worker_job_outbox (
			id, dedupe_key, job_kind, contract_version, args, payload_hash,
			queue, priority, max_attempts, scheduled_at, status, attempt_count,
			next_attempt_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5::json, $6, $7, $8, $9, $10,
			'pending', 0, $10, $10, $10)`,
		seed.ID, seed.DedupeKey, seed.Kind, seed.Version, string(seed.Args), seed.PayloadHash,
		seed.Queue, seed.Priority, seed.MaxAttempts, seed.ScheduledAt)
	if err != nil {
		t.Fatal(err)
	}
}

func claimOne(t *testing.T, ctx context.Context, repository *Repository, now time.Time, lease time.Duration) Claim {
	t.Helper()
	claims, err := repository.ClaimDue(ctx, now, 1, lease)
	if err != nil || len(claims) != 1 {
		t.Fatalf("ClaimDue() = %#v, %v", claims, err)
	}
	return claims[0]
}

func assertCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, status string, outbox, riverJobs int) {
	t.Helper()
	var outboxCount, riverCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM public.worker_job_outbox WHERE status=$1", status).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM river.river_job").Scan(&riverCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != outbox || riverCount != riverJobs {
		t.Fatalf("counts = outbox %d River %d, want %d/%d", outboxCount, riverCount, outbox, riverJobs)
	}
}

func assertErrorEvidence(t *testing.T, ctx context.Context, pool *pgxpool.Pool, code, detail string) {
	t.Helper()
	rows, err := pool.Query(ctx, "SELECT last_error_code, last_error_detail FROM public.worker_job_outbox ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var actualCode, actualDetail string
		if err := rows.Scan(&actualCode, &actualDetail); err != nil {
			t.Fatal(err)
		}
		values = append(values, actualCode+":"+actualDetail)
	}
	sort.Strings(values)
	for _, value := range values {
		if value != code+":"+detail {
			t.Fatalf("error evidence = %q", value)
		}
	}
}

func injectedFault() error { return errors.New("simulated process crash") }

func resetOutboxTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, "TRUNCATE public.worker_job_outbox, river.river_job RESTART IDENTITY"); err != nil {
		t.Fatal(err)
	}
}

func createOutboxRoles(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	statements := []string{
		"CREATE ROLE " + outboxDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + outboxDomainPassword + "'",
		"CREATE ROLE " + outboxQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + outboxQueuePassword + "'",
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func createOutboxSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY,
			dedupe_key varchar(256) NOT NULL UNIQUE,
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
			attempt_count integer NOT NULL,
			first_attempt_at timestamptz,
			last_attempt_at timestamptz,
			next_attempt_at timestamptz NOT NULL,
			last_error_code varchar(64),
			last_error_detail varchar(256),
			last_error_at timestamptz,
			river_job_id bigint UNIQUE,
			delivered_at timestamptz,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			CONSTRAINT worker_job_outbox_status CHECK (status IN ('pending','claimed','delivered','dead')),
			CONSTRAINT worker_job_outbox_claim CHECK (
				(status='claimed' AND claim_token IS NOT NULL AND claimed_at IS NOT NULL AND claim_expires_at IS NOT NULL)
				OR (status<>'claimed' AND claim_token IS NULL AND claimed_at IS NULL AND claim_expires_at IS NULL)
			),
			CONSTRAINT worker_job_outbox_delivery CHECK (
				(status='delivered' AND river_job_id IS NOT NULL AND delivered_at IS NOT NULL)
				OR (status<>'delivered' AND river_job_id IS NULL AND delivered_at IS NULL)
			)
		)`)
	if err != nil {
		t.Fatal(err)
	}
}

func openIntegrationPool(t *testing.T, ctx context.Context, uri string) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(uri)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	return pool
}

func integrationRoleURI(t *testing.T, raw, role, password string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, password)
	return parsed.String()
}
