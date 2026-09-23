//go:build integration

package sync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// createCanonicalIncidentDecisionTables is a lean, dedicated schema for
// canonicalIncidentDecision's four read tables -- this file tests the
// decision function directly, not through the scheduler's
// config/job/occurrence machinery eligibility_gate_integration_test.go
// exercises, so it does not need those tables.
func createCanonicalIncidentDecisionTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE feature_flags (
	id uuid PRIMARY KEY, key text NOT NULL, min_tier text NOT NULL, is_enabled boolean NOT NULL
);
CREATE TABLE org_feature_overrides (
	org_id uuid NOT NULL, feature_id uuid NOT NULL, is_enabled boolean, expires_at timestamptz,
	config json
);
CREATE TABLE organizations (id uuid PRIMARY KEY, tier text);
CREATE TABLE org_licenses (
	org_id uuid PRIMARY KEY, tier text, features_override jsonb
)`)
	if err != nil {
		t.Fatal(err)
	}
}

const (
	decisionFeatureID = "00000000-0000-4000-8000-0000000002f1"
	decisionOrgID     = "00000000-0000-4000-8000-0000000002f2"
)

func seedCanonicalIncidentFeatureFlag(t *testing.T, ctx context.Context, pool *pgxpool.Pool, minTier string, enabled bool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO feature_flags (id, key, min_tier, is_enabled) VALUES ($1, 'canonical_incident_ingestion', $2, $3)`,
		decisionFeatureID, minTier, enabled); err != nil {
		t.Fatal(err)
	}
}

func seedCanonicalIncidentOrganization(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tier string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, tier) VALUES ($1, $2)`, decisionOrgID, tier); err != nil {
		t.Fatal(err)
	}
}

func withDecisionTx(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx)) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	fn(tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func decideCanonicalIncident(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (bool, FeatureDecisionReason) {
	t.Helper()
	var allowed bool
	var reason FeatureDecisionReason
	withDecisionTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		allowed, reason, err = CanonicalIncidentDecisionForUpdate(ctx, tx, decisionOrgID, time.Now().UTC())
		if err != nil {
			t.Fatalf("CanonicalIncidentDecisionForUpdate: %v", err)
		}
	})
	return allowed, reason
}

// TestCanonicalIncidentDecisionCoversEveryReachableReason pins one reason
// per reachable decide_feature branch for canonical_incident_ingestion
// (see FeatureDecisionReason's doc comment for which branches this feature
// key can never reach). Table-driven because every case shares the same
// decide-and-assert shape; each row is its own fresh database so seed
// order between rows can never leak.
func TestCanonicalIncidentDecisionCoversEveryReachableReason(t *testing.T) {
	cases := canonicalIncidentDecisionReasonCases()

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			instance, err := containers.StartPostgres(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer instance.Close(context.Background())
			pool, err := pgxpool.New(ctx, instance.URI)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			createCanonicalIncidentDecisionTables(t, ctx, pool)
			testCase.seed(t, ctx, pool)

			allowed, reason := decideCanonicalIncident(t, ctx, pool)
			if allowed != testCase.wantAllowed {
				t.Errorf("allowed=%v want=%v", allowed, testCase.wantAllowed)
			}
			if reason != testCase.wantReason {
				t.Errorf("reason=%q want=%q", reason, testCase.wantReason)
			}

			// CanonicalIncidentAllowedForUpdate's bool must always agree with
			// the reason-carrying sibling's bool -- proving the "byte-identical
			// existing wrapper" delegation is actually wired, not just declared.
			var boolOnlyAllowed bool
			withDecisionTx(t, ctx, pool, func(tx pgx.Tx) {
				var err error
				boolOnlyAllowed, err = CanonicalIncidentAllowedForUpdate(ctx, tx, decisionOrgID, time.Now().UTC())
				if err != nil {
					t.Fatalf("CanonicalIncidentAllowedForUpdate: %v", err)
				}
			})
			if boolOnlyAllowed != allowed {
				t.Errorf("CanonicalIncidentAllowedForUpdate=%v disagrees with CanonicalIncidentDecisionForUpdate=%v", boolOnlyAllowed, allowed)
			}
		})
	}
}

// decideCanonicalIncidentNonLocking exercises the non-locking form
// (CanonicalIncidentDecision), which since CHAOS-6286's scheduler widening
// delegates to internal/api/licensing instead of this file's own SQL.
func decideCanonicalIncidentNonLocking(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (bool, FeatureDecisionReason) {
	t.Helper()
	var allowed bool
	var reason FeatureDecisionReason
	withDecisionTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		allowed, reason, err = CanonicalIncidentDecision(ctx, tx, decisionOrgID, time.Now().UTC())
		if err != nil {
			t.Fatalf("CanonicalIncidentDecision: %v", err)
		}
	})
	return allowed, reason
}

// TestCanonicalIncidentDecisionNonLockingMatchesLockingForEveryReachableReason
// proves the non-locking form -- now delegating to internal/api/licensing --
// still agrees with the locking form's own local SQL for every reachable
// reason this feature key can produce, run through the identical case table
// TestCanonicalIncidentDecisionCoversEveryReachableReason pins for the
// locking path.
func TestCanonicalIncidentDecisionNonLockingMatchesLockingForEveryReachableReason(t *testing.T) {
	cases := canonicalIncidentDecisionReasonCases()

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			instance, err := containers.StartPostgres(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer instance.Close(context.Background())
			pool, err := pgxpool.New(ctx, instance.URI)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			createCanonicalIncidentDecisionTables(t, ctx, pool)
			testCase.seed(t, ctx, pool)

			allowed, reason := decideCanonicalIncidentNonLocking(t, ctx, pool)
			if allowed != testCase.wantAllowed {
				t.Errorf("allowed=%v want=%v", allowed, testCase.wantAllowed)
			}
			if reason != testCase.wantReason {
				t.Errorf("reason=%q want=%q", reason, testCase.wantReason)
			}

			var boolOnlyAllowed bool
			withDecisionTx(t, ctx, pool, func(tx pgx.Tx) {
				var err error
				boolOnlyAllowed, err = CanonicalIncidentAllowed(ctx, tx, decisionOrgID, time.Now().UTC())
				if err != nil {
					t.Fatalf("CanonicalIncidentAllowed: %v", err)
				}
			})
			if boolOnlyAllowed != allowed {
				t.Errorf("CanonicalIncidentAllowed=%v disagrees with CanonicalIncidentDecision=%v", boolOnlyAllowed, allowed)
			}
		})
	}
}

// TestCanonicalIncidentDecisionNonLockingDeniesNumericLicenseOverride is the
// CHAOS-6286 regression case: the non-locking form's local `map[string]bool`
// decode of org_licenses.features_override used to fail outright on a
// numeric override value (JSON `0` is not a valid bool), silently discarding
// the whole overrides map and falling through to tier -- so a numeric-0
// override was admitted (enabled_by_tier) instead of denied
// (license_override_disabled). licensing.LoadState/Decide, via
// jsonTruth, treats JSON `0` as Python's bool(0) == False, matching the
// shared engine and the worker's own execution-time recheck. Red on the
// pre-CHAOS-6286-widening code (asserted here against CanonicalIncidentDecision
// only -- the still-local locking form is intentionally NOT covered by this
// case, see this file's package doc / the PR's RISK-NOTES).
func TestCanonicalIncidentDecisionNonLockingDeniesNumericLicenseOverride(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createCanonicalIncidentDecisionTables(t, ctx, pool)
	seedCanonicalIncidentFeatureFlag(t, ctx, pool, "enterprise", true)
	if _, err := pool.Exec(ctx, `
INSERT INTO organizations (id, tier) VALUES ($1, 'community')`, decisionOrgID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO org_licenses (org_id, tier, features_override)
VALUES ($1, 'community', '{"canonical_incident_ingestion":0}'::jsonb)`, decisionOrgID); err != nil {
		t.Fatal(err)
	}

	allowed, reason := decideCanonicalIncidentNonLocking(t, ctx, pool)
	if allowed {
		t.Fatalf("allowed=true reason=%q want denied: a numeric-0 license override must not fall through to tier", reason)
	}
	if reason != FeatureDecisionReasonLicenseOverrideDisabled {
		t.Fatalf("reason=%q want=%q", reason, FeatureDecisionReasonLicenseOverrideDisabled)
	}
}

// canonicalIncidentDecisionReasonCases is the shared case table both the
// locking and non-locking coverage tests run: every case must produce an
// identical (allowed, reason) answer from both forms, since they read the
// same tables for the same feature key and differ only in locking + (for
// the non-locking form) which code computes the answer.
func canonicalIncidentDecisionReasonCases() []struct {
	name        string
	seed        func(t *testing.T, ctx context.Context, pool *pgxpool.Pool)
	wantAllowed bool
	wantReason  FeatureDecisionReason
} {
	return []struct {
		name        string
		seed        func(t *testing.T, ctx context.Context, pool *pgxpool.Pool)
		wantAllowed bool
		wantReason  FeatureDecisionReason
	}{
		{
			name:        "no feature flag row",
			seed:        func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {},
			wantAllowed: false,
			wantReason:  FeatureDecisionReasonFeatureNotRegistered,
		},
		{
			name: "unrecognized min_tier is invalid state",
			seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
				seedCanonicalIncidentFeatureFlag(t, ctx, pool, "not_a_real_tier", true)
				seedCanonicalIncidentOrganization(t, ctx, pool, "community")
			},
			wantAllowed: false,
			wantReason:  FeatureDecisionReasonInvalidFeatureState,
		},
		{
			name: "globally disabled",
			seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
				seedCanonicalIncidentFeatureFlag(t, ctx, pool, "community", false)
				seedCanonicalIncidentOrganization(t, ctx, pool, "community")
			},
			wantAllowed: false,
			wantReason:  FeatureDecisionReasonGlobalDisabled,
		},
		{
			name: "org override enabled and not expired",
			seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
				seedCanonicalIncidentFeatureFlag(t, ctx, pool, "enterprise", true)
				seedCanonicalIncidentOrganization(t, ctx, pool, "community")
				if _, err := pool.Exec(ctx, `
INSERT INTO org_feature_overrides (org_id, feature_id, is_enabled, expires_at) VALUES ($1, $2, true, NULL)`,
					decisionOrgID, decisionFeatureID); err != nil {
					t.Fatal(err)
				}
			},
			wantAllowed: true,
			wantReason:  FeatureDecisionReasonEnabledByOrgOverride,
		},
		{
			name: "org override disabled and not expired",
			seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
				seedCanonicalIncidentFeatureFlag(t, ctx, pool, "community", true)
				seedCanonicalIncidentOrganization(t, ctx, pool, "enterprise")
				if _, err := pool.Exec(ctx, `
INSERT INTO org_feature_overrides (org_id, feature_id, is_enabled, expires_at) VALUES ($1, $2, false, NULL)`,
					decisionOrgID, decisionFeatureID); err != nil {
					t.Fatal(err)
				}
			},
			wantAllowed: false,
			wantReason:  FeatureDecisionReasonOrgOverrideDisabled,
		},
		{
			name: "expired org override falls through to tier",
			seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
				seedCanonicalIncidentFeatureFlag(t, ctx, pool, "community", true)
				seedCanonicalIncidentOrganization(t, ctx, pool, "enterprise")
				if _, err := pool.Exec(ctx, `
INSERT INTO org_feature_overrides (org_id, feature_id, is_enabled, expires_at)
VALUES ($1, $2, false, '2000-01-01T00:00:00Z')`,
					decisionOrgID, decisionFeatureID); err != nil {
					t.Fatal(err)
				}
			},
			wantAllowed: true,
			wantReason:  FeatureDecisionReasonEnabledByTier,
		},
		{
			name: "license override enabled, no org override",
			seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
				seedCanonicalIncidentFeatureFlag(t, ctx, pool, "enterprise", true)
				if _, err := pool.Exec(ctx, `
INSERT INTO organizations (id, tier) VALUES ($1, 'community')`, decisionOrgID); err != nil {
					t.Fatal(err)
				}
				if _, err := pool.Exec(ctx, `
INSERT INTO org_licenses (org_id, tier, features_override)
VALUES ($1, 'community', '{"canonical_incident_ingestion":true}'::jsonb)`, decisionOrgID); err != nil {
					t.Fatal(err)
				}
			},
			wantAllowed: true,
			wantReason:  FeatureDecisionReasonEnabledByLicenseOverride,
		},
		{
			name: "license override disabled, no org override",
			seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
				seedCanonicalIncidentFeatureFlag(t, ctx, pool, "community", true)
				if _, err := pool.Exec(ctx, `
INSERT INTO organizations (id, tier) VALUES ($1, 'enterprise')`, decisionOrgID); err != nil {
					t.Fatal(err)
				}
				if _, err := pool.Exec(ctx, `
INSERT INTO org_licenses (org_id, tier, features_override)
VALUES ($1, 'enterprise', '{"canonical_incident_ingestion":false}'::jsonb)`, decisionOrgID); err != nil {
					t.Fatal(err)
				}
			},
			wantAllowed: false,
			wantReason:  FeatureDecisionReasonLicenseOverrideDisabled,
		},
		{
			name: "no overrides, tier allowed",
			seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
				seedCanonicalIncidentFeatureFlag(t, ctx, pool, "team", true)
				seedCanonicalIncidentOrganization(t, ctx, pool, "enterprise")
			},
			wantAllowed: true,
			wantReason:  FeatureDecisionReasonEnabledByTier,
		},
		{
			name: "no overrides, tier required",
			seed: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
				seedCanonicalIncidentFeatureFlag(t, ctx, pool, "enterprise", true)
				seedCanonicalIncidentOrganization(t, ctx, pool, "community")
			},
			wantAllowed: false,
			wantReason:  FeatureDecisionReasonTierRequired,
		},
	}
}

// TestCanonicalIncidentDecisionInvalidOrgID pins the pre-flight guard: an
// unparseable org_id (Python: require_canonical_incident_feature_for_update_sync's
// own uuid.UUID(str(org_id)) ValueError catch, before evaluate_org_feature_sync
// is ever reached) is INVALID_FEATURE_STATE, not FEATURE_NOT_REGISTERED or a
// hard error.
func TestCanonicalIncidentDecisionInvalidOrgID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createCanonicalIncidentDecisionTables(t, ctx, pool)
	seedCanonicalIncidentFeatureFlag(t, ctx, pool, "community", true)

	var allowed bool
	var reason FeatureDecisionReason
	withDecisionTx(t, ctx, pool, func(tx pgx.Tx) {
		var err error
		allowed, reason, err = CanonicalIncidentDecisionForUpdate(ctx, tx, "not-a-uuid", time.Now().UTC())
		if err != nil {
			t.Fatalf("CanonicalIncidentDecisionForUpdate: %v", err)
		}
	})
	if allowed {
		t.Fatal("allowed=true want=false for an unparseable org_id")
	}
	if reason != FeatureDecisionReasonInvalidFeatureState {
		t.Fatalf("reason=%q want=%q", reason, FeatureDecisionReasonInvalidFeatureState)
	}
}
