//go:build integration

package sync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Phase-A eligibility gates that Python applied BEFORE minting and Go did not.
//
// Python's scheduler refuses two things before it writes anything: a config
// whose organization no longer exists (workers/sync_scheduler.py:204-205 via
// workers/org_guard.py:14-36) and a config whose sync targets need the
// canonical-incident feature when that feature is not entitled for the org
// (workers/sync_scheduler.py:207-219). Go reached neither check until the
// occurrence was already minted and had to be rejected downstream, so it wrote
// occurrence-ledger rows and failed sync_runs where Python wrote nothing at all.
//
// The kernel was never supposed to own these. transaction.go:126-132 says the
// Coordinator owns "every non-timing eligibility decision, including
// organization existence and feature entitlement", and evaluate.go:10-13 says
// Evaluate deliberately omits them. The contract was documented and the sole
// production Coordinator implemented neither half of it. These tests hold the
// Coordinator to it.
//
// Each test asserts the two things that together mean "refused, not merely
// unlucky": no occurrence row was written, AND the schedule marker did not
// advance. A refused schedule must stay due, exactly as it does in Python,
// which returns False at :205 and :219 before the marker is stamped at :303-305.

const (
	gateOrgID     = "3f1c9d4a-0000-4000-8000-00000000a001"
	gateConfigID  = "3f1c9d4a-0000-4000-8000-00000000b001"
	gateJobID     = "3f1c9d4a-0000-4000-8000-00000000c001"
	gateFeatureID = "3f1c9d4a-0000-4000-8000-00000000d001"
)

type eligibilityFixture struct {
	// seedOrganization writes the organizations row. False means the org_id on
	// the config points at nothing, which is the deleted-organization case.
	seedOrganization bool
	// orgTier is the organizations.tier value when the row is seeded.
	orgTier string
	// syncTargets is the config's sync_targets JSON. "operational" and
	// "incidents" are the targets gated by the canonical-incident feature
	// (sync/canonical_incident_gate.py:25).
	syncTargets string
	// seedFeatureFlag writes the canonical_incident_ingestion feature_flags row.
	seedFeatureFlag bool
	// featureGloballyEnabled is that row's is_enabled value.
	featureGloballyEnabled bool
	// featureMinTier is that row's min_tier value.
	featureMinTier string
}

func startEligibilityPostgres(t *testing.T, ctx context.Context, fixture eligibilityFixture) *pgxpool.Pool {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := seedEligibilityFixture(ctx, pool, fixture); err != nil {
		t.Fatal(err)
	}
	return pool
}

func seedEligibilityFixture(ctx context.Context, pool *pgxpool.Pool, fixture eligibilityFixture) error {
	statements := []string{
		`CREATE TABLE public.sync_configurations (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			is_active boolean NOT NULL,
			-- CHAOS-4174: defaults TRUE (unlike prod migration 0018's
			-- server_default FALSE) so this file's org_missing/feature_disabled
			-- fixtures, which never name the column, keep exercising the
			-- Coordinator refusals they were written for.
			planner_managed boolean NOT NULL DEFAULT TRUE,
			sync_targets jsonb NOT NULL,
			sync_options jsonb NOT NULL,
			last_sync_at timestamptz,
			created_at timestamptz NOT NULL
		)`,
		`CREATE TABLE public.scheduled_jobs (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			sync_config_id uuid NOT NULL,
			job_type text NOT NULL,
			schedule_cron text NOT NULL,
			timezone text NOT NULL,
			status integer NOT NULL,
			is_running boolean NOT NULL,
			last_run_at timestamptz,
			updated_at timestamptz,
			next_run_at timestamptz
		)`,
		`CREATE TABLE public.scheduled_sync_occurrences (
			occurrence_id text PRIMARY KEY,
			identity_version text NOT NULL,
			org_id text NOT NULL,
			sync_config_id uuid NOT NULL,
			scheduled_job_id uuid NOT NULL,
			scheduled_for timestamptz NOT NULL,
			job_run_id uuid,
			sync_run_id uuid,
			created_at timestamptz NOT NULL,
			UNIQUE (sync_config_id, scheduled_for)
		)`,
		`CREATE TABLE public.organizations (id uuid PRIMARY KEY, tier text)`,
		`CREATE TABLE public.feature_flags (
			id uuid PRIMARY KEY,
			key text NOT NULL,
			min_tier text NOT NULL,
			is_enabled boolean NOT NULL
		)`,
		`CREATE TABLE public.org_feature_overrides (
			org_id uuid NOT NULL,
			feature_id uuid NOT NULL,
			is_enabled boolean,
			expires_at timestamptz
		)`,
		`CREATE TABLE public.org_licenses (
			org_id uuid PRIMARY KEY,
			tier text,
			features_override jsonb
		)`,
		fmt.Sprintf(`INSERT INTO public.sync_configurations (
			id, org_id, is_active, sync_targets, sync_options, last_sync_at, created_at
		) VALUES (
			'%s', '%s', TRUE, '%s'::jsonb,
			'{"schedule_cron":"0 * * * *","timezone":"UTC"}'::jsonb,
			'2026-01-01T10:00:00Z', '2026-01-01T09:00:00Z'
		)`, gateConfigID, gateOrgID, fixture.syncTargets),
		fmt.Sprintf(`INSERT INTO public.scheduled_jobs (
			id, org_id, sync_config_id, job_type, schedule_cron, timezone,
			status, is_running, updated_at
		) VALUES (
			'%s', '%s', '%s', 'sync', '0 * * * *', 'UTC', 0, FALSE,
			'2026-01-01T09:00:00Z'
		)`, gateJobID, gateOrgID, gateConfigID),
	}
	if fixture.seedOrganization {
		statements = append(statements, fmt.Sprintf(
			`INSERT INTO public.organizations (id, tier) VALUES ('%s', '%s')`,
			gateOrgID, fixture.orgTier,
		))
	}
	if fixture.seedFeatureFlag {
		statements = append(statements, fmt.Sprintf(
			`INSERT INTO public.feature_flags (id, key, min_tier, is_enabled)
			 VALUES ('%s', 'canonical_incident_ingestion', '%s', %t)`,
			gateFeatureID, fixture.featureMinTier, fixture.featureGloballyEnabled,
		))
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return fmt.Errorf("seed eligibility fixture: %w", err)
		}
	}
	return nil
}

// runOneHandoffWindow runs a single window with Go mutation ownership and
// returns how many occurrence rows exist afterwards and whether the marker moved.
func runOneHandoffWindow(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (HandoffResult, int, *time.Time) {
	t.Helper()
	repository, err := newRepositoryWithOwnership(pool, reviewedGoMutationOwnershipPolicy())
	if err != nil {
		t.Fatal(err)
	}
	result, err := repository.HandoffDueResult(
		ctx,
		at("2026-01-01T12:00:00Z"),
		10,
		NewOccurrenceCoordinator(),
	)
	if err != nil {
		t.Fatalf("HandoffDueResult() error = %v", err)
	}
	var occurrences int
	var nextRunAt *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM public.scheduled_sync_occurrences),
			next_run_at
		FROM public.scheduled_jobs WHERE id = $1
	`, gateJobID).Scan(&occurrences, &nextRunAt); err != nil {
		t.Fatal(err)
	}
	return result, occurrences, nextRunAt
}

// TestHandoffRefusesAConfigWhoseOrganizationDoesNotExist is GAP-3, red-first.
//
// Python skips a config whose organization row is gone before it mints
// anything (sync_scheduler.py:204-205). The config here is active, has a valid
// hourly cron, an ACTIVE job marker, and is due -- every timing gate passes.
// The ONLY reason to refuse it is that its org_id points at no organizations
// row. On today's tree this mints an occurrence, because no production code
// path performs the check the Coordinator's own doc comment promises.
func TestHandoffRefusesAConfigWhoseOrganizationDoesNotExist(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startEligibilityPostgres(t, ctx, eligibilityFixture{
		seedOrganization: false,
		syncTargets:      `["git"]`,
		seedFeatureFlag:  false,
	})

	result, occurrences, nextRunAt := runOneHandoffWindow(t, ctx, pool)

	if occurrences != 0 {
		t.Errorf("minted %d occurrence(s) for a config whose organization does "+
			"not exist, want 0: Python refuses this before minting "+
			"(workers/sync_scheduler.py:204-205)", occurrences)
	}
	if nextRunAt != nil {
		t.Errorf("schedule marker advanced to %v for a refused config, want it "+
			"left unset: a refused schedule must stay due, as it does in Python "+
			"where :205 returns before the marker is stamped at :303-305", nextRunAt)
	}
	if result.SkippedOrgMissing != 1 {
		t.Errorf("SkippedOrgMissing = %d, want 1: a skip nobody counts is a "+
			"skip nobody can alert on", result.SkippedOrgMissing)
	}
}

// TestHandoffRefusesAConfigWhoseCanonicalIncidentFeatureIsDisabled is GAP-4,
// red-first. The org exists and every timing gate passes; the config's targets
// are gated ones ("operational") and the feature is globally disabled, which is
// the case Python refuses at sync_scheduler.py:207-219.
func TestHandoffRefusesAConfigWhoseCanonicalIncidentFeatureIsDisabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startEligibilityPostgres(t, ctx, eligibilityFixture{
		seedOrganization:       true,
		orgTier:                "enterprise",
		syncTargets:            `["operational"]`,
		seedFeatureFlag:        true,
		featureGloballyEnabled: false,
		featureMinTier:         "community",
	})

	result, occurrences, nextRunAt := runOneHandoffWindow(t, ctx, pool)

	if occurrences != 0 {
		t.Errorf("minted %d occurrence(s) for a config whose canonical-incident "+
			"feature is disabled, want 0: Python refuses this before minting "+
			"(workers/sync_scheduler.py:207-219)", occurrences)
	}
	if nextRunAt != nil {
		t.Errorf("schedule marker advanced to %v for a refused config, want it "+
			"left unset", nextRunAt)
	}
	if result.SkippedFeatureDisabled != 1 {
		t.Errorf("SkippedFeatureDisabled = %d, want 1", result.SkippedFeatureDisabled)
	}
}

// TestHandoffStillMintsForAnEntitledOrganization is the positive control. A
// gate that refuses everything would pass both tests above and be worthless.
func TestHandoffStillMintsForAnEntitledOrganization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startEligibilityPostgres(t, ctx, eligibilityFixture{
		seedOrganization:       true,
		orgTier:                "enterprise",
		syncTargets:            `["operational"]`,
		seedFeatureFlag:        true,
		featureGloballyEnabled: true,
		featureMinTier:         "community",
	})

	result, occurrences, nextRunAt := runOneHandoffWindow(t, ctx, pool)

	if occurrences != 1 {
		t.Errorf("minted %d occurrence(s) for an entitled organization, want 1", occurrences)
	}
	if nextRunAt == nil {
		t.Error("schedule marker did not advance for a minted occurrence")
	}
	if result.SkippedOrgMissing != 0 || result.SkippedFeatureDisabled != 0 {
		t.Errorf("entitled organization was counted as skipped: orgMissing=%d featureDisabled=%d",
			result.SkippedOrgMissing, result.SkippedFeatureDisabled)
	}
}

// TestHandoffFeatureGateOnlyAppliesToGatedTargets is the second control, and
// the one that keeps GAP-4's fix from becoming a blanket outage. A config whose
// targets are NOT canonical-incident targets must mint even when the feature is
// disabled -- Python only consults the feature when
// sync_targets_require_canonical_incident_feature is true
// (sync/canonical_incident_gate.py:37-42, applied at sync_scheduler.py:207).
func TestHandoffFeatureGateOnlyAppliesToGatedTargets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startEligibilityPostgres(t, ctx, eligibilityFixture{
		seedOrganization:       true,
		orgTier:                "enterprise",
		syncTargets:            `["git"]`,
		seedFeatureFlag:        true,
		featureGloballyEnabled: false,
		featureMinTier:         "community",
	})

	_, occurrences, _ := runOneHandoffWindow(t, ctx, pool)

	if occurrences != 1 {
		t.Errorf("minted %d occurrence(s) for a non-gated target set with the "+
			"canonical-incident feature disabled, want 1: the feature gate must "+
			"scope to gated targets, not stop every schedule in the fleet",
			occurrences)
	}
}

// --- Findings from the adversarial review of this changeset -----------------

// TestOneMalformedTargetListDoesNotAbortTheWholeWindow is a review finding,
// red-first. The first version of configuredSyncTargets decoded sync_targets
// straight into []string, so a config whose targets were not JSON strings
// returned a decode error, which aborted HandoffDueResult and rolled back the
// ENTIRE window -- every other due config in it included.
//
// Python cannot do that twice over: it coerces each target with str()
// (workers/sync_scheduler.py:206) so non-string elements are simply stringified,
// and it wraps each config in its own try/except with the comment "One bad
// config (e.g. a malformed cron expression) must not abort dispatch for the
// remaining configs" (:415-421). A single malformed row taking down the whole
// fleet's scheduling window is a strictly worse failure than the one this
// changeset set out to fix.
func TestOneMalformedTargetListDoesNotAbortTheWholeWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startEligibilityPostgres(t, ctx, eligibilityFixture{
		seedOrganization: true,
		orgTier:          "enterprise",
		// Numbers, not strings. Python would stringify these to "1" and "2",
		// neither of which is a gated target.
		syncTargets:     `[1, 2]`,
		seedFeatureFlag: false,
	})

	_, occurrences, _ := runOneHandoffWindow(t, ctx, pool)

	if occurrences != 1 {
		t.Errorf("minted %d occurrence(s) for a config whose sync_targets are "+
			"not JSON strings, want 1: Python stringifies them "+
			"(workers/sync_scheduler.py:206) and isolates per-config failures "+
			"(:415-421). Aborting the window here would stop every OTHER due "+
			"config in the fleet because of one malformed row", occurrences)
	}
}

// TestRefusedAndAcceptedCandidatesCoexistInOneWindow is the mixed-window case
// the first round of tests did not cover: a refusal must skip exactly its own
// candidate and leave the rest of the window intact, marker advancement
// included. Without this, a refusal that aborted or short-circuited the loop
// would pass every single-candidate test in this file.
func TestRefusedAndAcceptedCandidatesCoexistInOneWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := startEligibilityPostgres(t, ctx, eligibilityFixture{
		seedOrganization: true,
		orgTier:          "enterprise",
		syncTargets:      `["git"]`,
		seedFeatureFlag:  false,
	})

	// A second config in an organization that does not exist. It must be
	// refused while the first config above still mints.
	const (
		missingOrgID    = "3f1c9d4a-0000-4000-8000-00000000e001"
		refusedConfigID = "3f1c9d4a-0000-4000-8000-00000000e002"
		refusedJobID    = "3f1c9d4a-0000-4000-8000-00000000e003"
	)
	for _, statement := range []string{
		fmt.Sprintf(`INSERT INTO public.sync_configurations (
			id, org_id, is_active, sync_targets, sync_options, last_sync_at, created_at
		) VALUES ('%s','%s',TRUE,'["git"]'::jsonb,
			'{"schedule_cron":"0 * * * *","timezone":"UTC"}'::jsonb,
			'2026-01-01T10:00:00Z','2026-01-01T09:00:00Z')`, refusedConfigID, missingOrgID),
		fmt.Sprintf(`INSERT INTO public.scheduled_jobs (
			id, org_id, sync_config_id, job_type, schedule_cron, timezone,
			status, is_running, updated_at
		) VALUES ('%s','%s','%s','sync','0 * * * *','UTC',0,FALSE,
			'2026-01-01T09:00:00Z')`, refusedJobID, missingOrgID, refusedConfigID),
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	result, occurrences, acceptedNextRunAt := runOneHandoffWindow(t, ctx, pool)

	if occurrences != 1 {
		t.Errorf("occurrences = %d, want 1: the entitled config must still mint "+
			"when another candidate in the same window is refused", occurrences)
	}
	if acceptedNextRunAt == nil {
		t.Error("the accepted config's marker did not advance")
	}
	if result.SkippedOrgMissing != 1 {
		t.Errorf("SkippedOrgMissing = %d, want 1", result.SkippedOrgMissing)
	}
	var refusedNextRunAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT next_run_at FROM public.scheduled_jobs WHERE id = $1`, refusedJobID,
	).Scan(&refusedNextRunAt); err != nil {
		t.Fatal(err)
	}
	if refusedNextRunAt != nil {
		t.Errorf("the refused config's marker advanced to %v; a refusal must "+
			"leave its own schedule due while its neighbours proceed", refusedNextRunAt)
	}
}
