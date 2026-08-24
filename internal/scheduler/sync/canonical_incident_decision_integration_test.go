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
	org_id uuid NOT NULL, feature_id uuid NOT NULL, is_enabled boolean, expires_at timestamptz
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
	cases := []struct {
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
