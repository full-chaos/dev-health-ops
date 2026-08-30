// Package providersyncschema creates the provider-sync Postgres fixture schema
// that internal/providersync's integration tests and cmd/dev-health-worker's
// end-to-end provider-unit tests share. ONE copy: the DDL below is pinned to
// the alembic migrations by tests/test_providersync_fixture_ddl_matches_migrations.py.
package providersyncschema

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Create is DERIVED FROM THE ALEMBIC MIGRATIONS, column
// for column and constraint for constraint, not hand-guessed to match what
// the repository code happens to read today. This is the CHAOS-4050
// conversion, mirroring #1836 (internal/joboutbox) and #1844 (internal/jobs/
// metrics/daily): a prior version of this fixture invented its own schema
// for nine tables independently of alembic, the exact class of gap that let
// CHAOS-4041 (a nonexistent org_id column) and CHAOS-4043 (a text/varchar
// type conflict) ship green. The per-table authorities:
//
//   - integration_credentials: stub only (id uuid PK) -- alembic 0001 owns
//     its full shape, out of scope here; it exists solely so the real FK
//     below on integrations.credential_id is enforceable.
//   - integrations / integration_sources / integration_datasets / sync_runs /
//     sync_run_units: alembic 0015, plus 0106's unavailable_* columns on
//     integration_datasets and 0093's tenant-fence unique constraint on
//     sync_run_units. Columns alembic requires but this suite's repository
//     code never reads or writes (integrations.name/provider/is_active/
//     schedule_cron/timezone/created_at/updated_at; integration_sources.
//     provider/is_enabled/discovered_at/last_seen_at/last_sync_*;
//     integration_datasets.is_enabled; sync_runs.triggered_by/mode/
//     started_at/completed_at/result/created_at/trace_parent;
//     sync_run_units.retry_exhausted_at) are deliberately NOT reproduced --
//     adding them would force every INSERT in this package to invent values
//     for columns no assertion here ever observes, without buying back any
//     additional safety. sync_runs.total_units/completed_units/failed_units
//     are the CHAOS-4559 exception: the per-unit terminal commit now writes
//     them (bumpSyncRunRollupSQL), so a suite without them cannot observe
//     that write at all. NOT NULL with no server default, matching alembic
//     0015 exactly -- a codex adversarial review caught an earlier draft
//     adding a fixture-only DEFAULT 0 the real migration does not have,
//     which would let a raw INSERT omitting these columns pass here and
//     fail against a migrated database. Every real INSERT path
//     (processors/sync.py, sync/planner.py, and this package's own fixture
//     inserts) always supplies 0 explicitly at row creation.
//     sync_runs.integration_id is the other exception: a codex adversarial
//     review caught that omitting it left the fixture's central sync_runs row a
//     shape that cannot exist in a migrated production database (the column
//     is NOT NULL there), so it is kept even though providersync's own
//     queries never read it. Every other real FK/unique key that depends
//     only on columns already present is kept (integration_sources/
//     integration_datasets/sync_runs -> integrations; sync_run_units ->
//     integration_sources, sync_runs; integration_datasets'
//     uq_integration_datasets_org_integration_dataset). The
//     uq_integration_sources_org_integration_provider_external constraint is
//     skipped because it is keyed partly on the omitted provider column.
//   - sync_run_unit_chunk_checkpoints / sync_run_unit_effect_chunks: alembic
//     0102. aggregate_digest and manifest_digest are varchar(64) in 0102, not
//     the unbounded text this fixture previously declared -- the same
//     hand-authored-text-masks-a-varchar-conflict shape CHAOS-4043 hit on
//     daily_metrics_runs.status, just not yet exploited by a live query here.
//   - sync_run_unit_effect_snapshots: alembic 0092+0093 (snapshotFixtureDDL,
//     unchanged, already guarded by tests/test_effect_snapshot_migration.py).
//   - sync_watermarks: alembic 0001+0015. Both real unique constraints are
//     kept -- 0001's uq_sync_watermark_org_repo_target (the original
//     org_id/repo_id/target key) and 0015's uq_sync_watermark_org_source_dataset
//     (org_id/source_id/dataset_key), since 0015 adds the newer columns and
//     their constraint without ever dropping the legacy one. An earlier draft
//     of this fix treated the legacy constraint as fabricated and dropped it
//     -- codex's adversarial review caught that the fixture would then be
//     MORE permissive than production on exactly the legacy-key uniqueness
//     CHAOS-4041's class is about: two rows sharing an (org_id, repo_id,
//     target) but differing in source_id/dataset_key would pass here and
//     violate a real constraint in production.
//   - sync_dispatch_outbox / sync_dispatch_transport_routes: alembic 0020+
//     0049. The prior fixture had no transport-route table, no
//     ck_sync_dispatch_outbox_claim_route_coherence /
//     ck_sync_dispatch_outbox_dispatched_route_coherence CHECK constraints,
//     and no trg_sync_dispatch_outbox_route_fence trigger -- production
//     refuses to let an outbox row claim a transport when its kind's Celery
//     route is paused, and this suite had no way to observe that at all.
//
// tests/test_providersync_fixture_ddl_matches_migrations.py fails if this
// drifts from the migrations above.
// snapshotFixtureDDL mirrors the 0092 table plus 0093 tenant FK.
const snapshotFixtureDDL = `CREATE TABLE public.sync_run_unit_effect_snapshots (
	org_id text NOT NULL,
	sync_run_unit_id uuid NOT NULL,
	generation text NOT NULL,
	provider text NOT NULL,
	dataset_key text NOT NULL,
	schema_version text NOT NULL,
	content_digest varchar(64) NOT NULL,
	payload_bytes integer NOT NULL,
	payload bytea NOT NULL,
	created_at timestamptz NOT NULL,
	CONSTRAINT ck_sync_run_unit_effect_snapshots_payload_bytes
		CHECK (payload_bytes >= 1 AND payload_bytes <= 67108864),
	CONSTRAINT ck_sync_run_unit_effect_snapshots_payload_length
		CHECK (length(payload) = payload_bytes),
	CONSTRAINT ck_sync_run_unit_effect_snapshots_schema_version
		CHECK (schema_version = 'v1'),
	PRIMARY KEY (org_id, sync_run_unit_id, generation),
	CONSTRAINT fk_sync_run_unit_effect_snapshots_tenant_unit
		FOREIGN KEY (org_id, sync_run_unit_id)
		REFERENCES public.sync_run_units(org_id, id)
		ON DELETE CASCADE
)`

func Create(ctx context.Context, pool *pgxpool.Pool) error {
	for _, statement := range []string{
		`CREATE TABLE public.integration_credentials (
			id uuid PRIMARY KEY
		)`,
		`CREATE TABLE public.integrations (
			id uuid PRIMARY KEY, org_id text NOT NULL,
			credential_id uuid REFERENCES public.integration_credentials(id),
			config jsonb NOT NULL DEFAULT '{}'::jsonb
		)`,
		`CREATE TABLE public.integration_sources (
			id uuid PRIMARY KEY, org_id text NOT NULL,
			integration_id uuid NOT NULL REFERENCES public.integrations(id),
			external_id text NOT NULL, full_name text NOT NULL,
			metadata jsonb NOT NULL DEFAULT '{}'::jsonb
		)`,
		`CREATE TABLE public.integration_datasets (
			id uuid PRIMARY KEY, org_id text NOT NULL,
			integration_id uuid NOT NULL REFERENCES public.integrations(id),
			dataset_key text NOT NULL, options jsonb NOT NULL DEFAULT '{}'::jsonb,
			unavailable_reason varchar(64), unavailable_since timestamptz,
			unavailable_last_seen_at timestamptz,
			CONSTRAINT uq_integration_datasets_org_integration_dataset
				UNIQUE (org_id, integration_id, dataset_key)
		)`,
		`CREATE TABLE public.sync_runs (
			id uuid PRIMARY KEY, org_id text NOT NULL,
			integration_id uuid NOT NULL REFERENCES public.integrations(id),
			status text NOT NULL,
			credential_id uuid, credential_fingerprint text, auth_source text,
			total_units integer NOT NULL,
			completed_units integer NOT NULL,
			failed_units integer NOT NULL
		)`,
		`CREATE TABLE public.sync_run_units (
			id uuid PRIMARY KEY, org_id text NOT NULL,
			sync_run_id uuid NOT NULL REFERENCES public.sync_runs(id),
			integration_id uuid NOT NULL,
			source_id uuid NOT NULL REFERENCES public.integration_sources(id),
			provider text NOT NULL,
			dataset_key text NOT NULL, cost_class text NOT NULL, mode text NOT NULL,
			since_at timestamptz, before_at timestamptz, status text NOT NULL,
			attempts integer NOT NULL DEFAULT 0, available_at timestamptz,
			error text, result json, processor_flags jsonb, lease_owner text,
			lease_expires_at timestamptz, last_heartbeat_at timestamptz,
			duration_seconds integer, rate_limit_deferrals integer NOT NULL DEFAULT 0,
			rate_limit_first_seen_at timestamptz,
			budget_deferrals integer NOT NULL DEFAULT 0,
			budget_first_deferred_at timestamptz,
			first_blocked_at timestamptz,
			expired_lease_retry_count integer NOT NULL DEFAULT 0,
			last_retry_reason text, updated_at timestamptz NOT NULL,
			-- CHAOS-4114: alembic 0015 has had this column all along; the
			-- fixture simply never needed it until the executed-proof
			-- ledger backfill started deriving attempted_at from it.
			created_at timestamptz NOT NULL DEFAULT now(),
			CONSTRAINT uq_sync_run_units_org_id_id_effect_snapshots
				UNIQUE (org_id, id)
		)`,
		// CHAOS-4114: the maintained executed-proof projection. It is in
		// domainPosture's manifest, and the scheduler/worker write paths stamp
		// it inside the same transaction that writes sync_run_units, so a venue
		// without it fails those writes outright.
		`CREATE TABLE public.sync_executed_proof_ledger (
			provider text NOT NULL,
			dataset_key text NOT NULL,
			attempted_at timestamptz NOT NULL,
			proven_at timestamptz,
			PRIMARY KEY (provider, dataset_key),
			CONSTRAINT ck_sync_executed_proof_ledger_provider_normalized
				CHECK (provider = lower(provider) AND btrim(provider) <> ''),
			CONSTRAINT ck_sync_executed_proof_ledger_dataset_normalized
				CHECK (dataset_key = lower(dataset_key) AND btrim(dataset_key) <> '')
		)`,
		`CREATE TABLE public.sync_run_unit_chunk_checkpoints (
			org_id text NOT NULL, sync_run_unit_id uuid NOT NULL, schema_version text NOT NULL DEFAULT 'v1', generation text NOT NULL,
			provider text NOT NULL, dataset_key text NOT NULL, route_version text NOT NULL,
			normalized_at timestamptz NOT NULL, next_cursor text NOT NULL DEFAULT '',
			inventory_complete boolean NOT NULL DEFAULT false, next_ordinal integer NOT NULL DEFAULT 0,
			prepared_chunks integer NOT NULL DEFAULT 0, total_chunks integer NOT NULL DEFAULT 0,
			final_ordinal integer NOT NULL DEFAULT -1, aggregate_result jsonb,
			aggregate_digest varchar(64), committed_rows bigint NOT NULL DEFAULT 0,
			owner text NOT NULL, lease_expires_at timestamptz NOT NULL,
			created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
			CONSTRAINT ck_sync_chunk_checkpoint_next_ordinal CHECK (next_ordinal >= 0),
			CONSTRAINT ck_sync_chunk_checkpoint_prepared_chunks CHECK (prepared_chunks >= 0),
			CONSTRAINT ck_sync_chunk_checkpoint_total_chunks CHECK (total_chunks >= 0),
			CONSTRAINT ck_sync_chunk_checkpoint_final_ordinal CHECK (final_ordinal >= -1),
			CONSTRAINT ck_sync_chunk_checkpoint_cursor CHECK (length(next_cursor) <= 4096),
			CONSTRAINT ck_sync_chunk_checkpoint_committed_rows CHECK (committed_rows >= 0),
			CONSTRAINT ck_sync_chunk_checkpoint_complete_fence CHECK (
				inventory_complete = false OR
				(total_chunks > 0 AND next_ordinal = total_chunks AND prepared_chunks = total_chunks)),
			PRIMARY KEY (org_id, sync_run_unit_id, generation),
			FOREIGN KEY (org_id, sync_run_unit_id) REFERENCES public.sync_run_units(org_id, id) ON DELETE CASCADE
		)`,
		`CREATE TABLE public.sync_run_unit_effect_chunks (
			org_id text NOT NULL, sync_run_unit_id uuid NOT NULL, schema_version text NOT NULL DEFAULT 'v1', generation text NOT NULL,
			route_version text NOT NULL, ordinal integer NOT NULL, total_chunks integer NOT NULL DEFAULT 0,
			cursor_before text NOT NULL DEFAULT '', cursor_after text NOT NULL DEFAULT '',
			inventory_complete boolean NOT NULL DEFAULT false, payload jsonb NOT NULL, ledger jsonb NOT NULL,
			payload_bytes integer NOT NULL, manifest_digest varchar(64) NOT NULL,
			status text NOT NULL DEFAULT 'pending', created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
			CONSTRAINT ck_sync_chunk_ordinal CHECK (ordinal >= 0),
			CONSTRAINT ck_sync_chunk_total CHECK (total_chunks = 0 OR ordinal < total_chunks),
			CONSTRAINT ck_sync_chunk_cursors CHECK (
				length(cursor_before) <= 4096 AND length(cursor_after) <= 4096),
			CONSTRAINT ck_sync_chunk_payload_bytes CHECK (
				payload_bytes >= 1 AND payload_bytes <= 2097152),
			CONSTRAINT ck_sync_chunk_payload_object CHECK (jsonb_typeof(payload) = 'object'),
			CONSTRAINT ck_sync_chunk_ledger_object CHECK (jsonb_typeof(ledger) = 'object'),
			CONSTRAINT ck_sync_chunk_status CHECK (status IN ('pending', 'writing', 'committed')),
			PRIMARY KEY (org_id, sync_run_unit_id, generation, ordinal),
			FOREIGN KEY (org_id, sync_run_unit_id, generation)
				REFERENCES public.sync_run_unit_chunk_checkpoints(org_id, sync_run_unit_id, generation) ON DELETE CASCADE
		)`,
		// Must stay equivalent to the 0092+0093 snapshot schema. This fixture
		// previously dropped all three CHECK constraints and widened
		// content_digest to text, so every integration test here ran against a
		// schema more permissive than production -- the class of gap where a
		// test proves the code works on a table that does not exist anywhere
		// real. tests/test_effect_snapshot_migration.py::
		// test_integration_fixture_ddl_matches_snapshot_migrations fails if the two
		// drift apart.
		snapshotFixtureDDL,
		`CREATE TABLE public.sync_watermarks (
			id uuid PRIMARY KEY, org_id text NOT NULL, repo_id text NOT NULL,
			source_id text NOT NULL, target text NOT NULL, dataset_key text NOT NULL,
			last_synced_at timestamptz, updated_at timestamptz NOT NULL,
			CONSTRAINT uq_sync_watermark_org_repo_target
				UNIQUE (org_id, repo_id, target),
			CONSTRAINT uq_sync_watermark_org_source_dataset
				UNIQUE (org_id, source_id, dataset_key)
		)`,
		// sync_dispatch_transport_routes plus the two trigger functions below
		// are alembic 0049 verbatim. Production refuses to let an outbox row
		// claim a transport while its kind's Celery route is paused (or has
		// no active route at all) -- this table, the seeded rows, and the
		// trigger are what enforce that, and none of it existed in this
		// fixture before CHAOS-4050.
		`CREATE TABLE public.sync_dispatch_transport_routes (
			kind text PRIMARY KEY, transport text NOT NULL, generation bigint NOT NULL,
			paused boolean NOT NULL, paused_at timestamptz,
			rollback_transport text NOT NULL,
			created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
			CONSTRAINT ck_sync_dispatch_transport_routes_kind CHECK (
				kind IN ('dispatch_sync_run', 'finalize_sync_run', 'post_sync', 'reference_discovery')),
			CONSTRAINT ck_sync_dispatch_transport_routes_transport CHECK (
				transport IN ('celery', 'river')),
			CONSTRAINT ck_sync_dispatch_transport_routes_rollback CHECK (
				rollback_transport = 'celery'),
			CONSTRAINT ck_sync_dispatch_transport_routes_generation CHECK (generation >= 1),
			CONSTRAINT ck_sync_dispatch_transport_routes_pause_timestamp CHECK (
				(paused AND paused_at IS NOT NULL) OR (NOT paused AND paused_at IS NULL))
		)`,
		`INSERT INTO public.sync_dispatch_transport_routes
			(kind, transport, generation, paused, paused_at, rollback_transport, created_at, updated_at)
		SELECT kind, 'celery', 1, false, NULL, 'celery', now(), now()
		FROM unnest(ARRAY['dispatch_sync_run', 'finalize_sync_run', 'post_sync', 'reference_discovery']) AS kind`,
		`CREATE FUNCTION enforce_sync_dispatch_route_generation()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF NEW.generation < OLD.generation THEN
				RAISE EXCEPTION 'sync dispatch route generation cannot decrease';
			END IF;
			IF (
				NEW.transport IS DISTINCT FROM OLD.transport
				OR NEW.paused IS DISTINCT FROM OLD.paused
				OR NEW.paused_at IS DISTINCT FROM OLD.paused_at
				OR NEW.rollback_transport IS DISTINCT FROM OLD.rollback_transport
			) AND NEW.generation <= OLD.generation THEN
				RAISE EXCEPTION
					'sync dispatch route state change requires generation increase';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER trg_sync_dispatch_route_generation
		BEFORE UPDATE ON public.sync_dispatch_transport_routes
		FOR EACH ROW
		EXECUTE FUNCTION enforce_sync_dispatch_route_generation()`,
		`CREATE FUNCTION enforce_sync_dispatch_outbox_route_fence()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		DECLARE
			active_transport text;
			active_generation bigint;
		BEGIN
			IF (NEW.claim_token IS NULL) <> (NEW.claim_expires_at IS NULL) THEN
				RAISE EXCEPTION
					'sync dispatch claim token and expiry must change together';
			END IF;

			IF NEW.claim_token IS NOT NULL
			   AND (
				   NEW.claim_transport IS NULL
				   OR NEW.claim_route_generation IS NULL
			   ) THEN
				SELECT transport, generation
				INTO active_transport, active_generation
				FROM public.sync_dispatch_transport_routes
				WHERE kind = NEW.kind
				  AND transport = 'celery'
				  AND paused = FALSE;
				IF NOT FOUND THEN
					RAISE EXCEPTION
						'sync dispatch kind has no active celery route';
				END IF;
				NEW.claim_transport := active_transport;
				NEW.claim_route_generation := active_generation;
			END IF;

			IF NEW.status = 'dispatched'
			   AND NEW.last_error IS DISTINCT FROM 'feature_disabled' THEN
				NEW.dispatched_transport := COALESCE(
					NEW.dispatched_transport,
					NEW.claim_transport,
					OLD.claim_transport
				);
				NEW.dispatched_route_generation := COALESCE(
					NEW.dispatched_route_generation,
					NEW.claim_route_generation,
					OLD.claim_route_generation
				);
			ELSE
				NEW.dispatched_transport := NULL;
				NEW.dispatched_route_generation := NULL;
				NEW.transport_job_id := NULL;
			END IF;

			IF NEW.claim_token IS NULL THEN
				NEW.claim_transport := NULL;
				NEW.claim_route_generation := NULL;
			END IF;
			RETURN NEW;
		END;
		$$`,
		`CREATE TABLE public.sync_dispatch_outbox (
			id uuid PRIMARY KEY, org_id text NOT NULL,
			sync_run_id uuid NOT NULL REFERENCES public.sync_runs(id),
			kind text NOT NULL, status text NOT NULL, available_at timestamptz NOT NULL,
			attempts integer NOT NULL, last_error text, dispatched_at timestamptz,
			claim_token text, claim_expires_at timestamptz, claim_transport text,
			claim_route_generation bigint, dispatched_transport text,
			dispatched_route_generation bigint, transport_job_id text,
			created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
			CONSTRAINT uq_sync_dispatch_outbox_run_kind UNIQUE (sync_run_id, kind),
			CONSTRAINT ck_sync_dispatch_outbox_claim_route_coherence CHECK (
				(claim_token IS NULL AND claim_expires_at IS NULL
					AND claim_transport IS NULL AND claim_route_generation IS NULL)
				OR (claim_token IS NOT NULL AND claim_expires_at IS NOT NULL
					AND claim_transport IS NOT NULL AND claim_route_generation IS NOT NULL)),
			CONSTRAINT ck_sync_dispatch_outbox_dispatched_route_coherence CHECK (
				(status = 'dispatched' AND (
					(last_error = 'feature_disabled' AND dispatched_transport IS NULL
						AND dispatched_route_generation IS NULL AND transport_job_id IS NULL)
					OR ((last_error IS NULL OR last_error <> 'feature_disabled')
						AND dispatched_transport IS NOT NULL
						AND dispatched_route_generation IS NOT NULL)))
				OR (status <> 'dispatched' AND dispatched_transport IS NULL
					AND dispatched_route_generation IS NULL AND transport_job_id IS NULL))
		)`,
		`CREATE TRIGGER trg_sync_dispatch_outbox_route_fence
		BEFORE INSERT OR UPDATE ON public.sync_dispatch_outbox
		FOR EACH ROW
		EXECUTE FUNCTION enforce_sync_dispatch_outbox_route_fence()`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
