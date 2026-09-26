//go:build integration

package fixed

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAskDevRetentionAdmissionUsesEntitlementForFirstUseButNeverStrandsState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	const (
		flagID = "10000000-0000-4000-8000-000000000001"
		orgID  = "20000000-0000-4000-8000-000000000001"
	)
	pgschema.Apply(ctx, t, pool)
	// The migrations register ask_dev; the cases below drive its row and the org's tier directly.
	pgseed.SetFeatureFlag(ctx, t, pool, flagID, "ask_dev", "community", true)
	pgseed.Org(ctx, t, pool, orgID, "community")

	admission := NewPostgresAskDevRetentionAdmission()
	read := func() AskDevRetentionState {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		state, err := admission.State(ctx, tx)
		if err != nil {
			t.Fatal(err)
		}
		return state
	}

	type overrideCase struct {
		IsEnabled bool    `json:"is_enabled"`
		ExpiresAt *string `json:"expires_at"`
	}
	type featureCase struct {
		ID              string        `json:"id"`
		StorageValid    bool          `json:"storage_valid"`
		GloballyEnabled bool          `json:"globally_enabled"`
		OrgTier         string        `json:"org_tier"`
		LicenseTier     *string       `json:"license_tier"`
		LicenseOverride *bool         `json:"license_override"`
		OrgOverride     *overrideCase `json:"org_override"`
	}
	truth, falsity := true, false
	past, future := "2000-01-01T00:00:00Z", "2100-01-01T00:00:00Z"
	community := "community"
	cases := []featureCase{
		{ID: "no_explicit_enable", StorageValid: true, GloballyEnabled: true, OrgTier: "community"},
		{ID: "license_enable", StorageValid: true, GloballyEnabled: true, OrgTier: "community", LicenseTier: &community, LicenseOverride: &truth},
		{ID: "global_kill", StorageValid: true, GloballyEnabled: false, OrgTier: "community", LicenseTier: &community, LicenseOverride: &truth},
		{ID: "active_org_enable_wins", StorageValid: true, GloballyEnabled: true, OrgTier: "community", LicenseTier: &community, LicenseOverride: &falsity, OrgOverride: &overrideCase{IsEnabled: true, ExpiresAt: &future}},
		{ID: "active_org_disable_wins", StorageValid: true, GloballyEnabled: true, OrgTier: "community", LicenseTier: &community, LicenseOverride: &truth, OrgOverride: &overrideCase{IsEnabled: false, ExpiresAt: &future}},
		{ID: "expired_override_falls_back", StorageValid: true, GloballyEnabled: true, OrgTier: "community", LicenseTier: &community, LicenseOverride: &truth, OrgOverride: &overrideCase{IsEnabled: false, ExpiresAt: &past}},
		{ID: "invalid_storage", StorageValid: false, GloballyEnabled: true, OrgTier: "community", LicenseTier: &community, LicenseOverride: &truth},
	}
	want := pythonAskDevFeatureDecisions(t, cases)
	for _, test := range cases {
		minTier := "community"
		if !test.StorageValid {
			minTier = "invalid"
		}
		licenseJSON := "{}"
		if test.LicenseOverride != nil {
			if *test.LicenseOverride {
				licenseJSON = `{"ask_dev": true}`
			} else {
				licenseJSON = `{"ask_dev": false}`
			}
		}
		if _, err := pool.Exec(ctx,
			"UPDATE feature_flags SET min_tier = $1, is_enabled = $2 WHERE key = 'ask_dev'",
			minTier, test.GloballyEnabled,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx,
			"UPDATE organizations SET tier = $1",
			test.OrgTier,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, "DELETE FROM org_licenses"); err != nil {
			t.Fatal(err)
		}
		if test.LicenseTier != nil {
			if _, err := pool.Exec(ctx, `
INSERT INTO org_licenses (id, org_id, tier, features_override, created_at, updated_at)
VALUES (gen_random_uuid(), '20000000-0000-4000-8000-000000000001', $1, $2::json, now(), now())
`, *test.LicenseTier, licenseJSON); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := pool.Exec(ctx, "DELETE FROM org_feature_overrides"); err != nil {
			t.Fatal(err)
		}
		if test.OrgOverride != nil {
			if _, err := pool.Exec(ctx, `
INSERT INTO org_feature_overrides
    (id, org_id, feature_id, is_enabled, expires_at, created_at, updated_at)
VALUES
    ('40000000-0000-4000-8000-000000000001',
     '20000000-0000-4000-8000-000000000001',
     '10000000-0000-4000-8000-000000000001', $1, $2, now(), now())
`, test.OrgOverride.IsEnabled, test.OrgOverride.ExpiresAt); err != nil {
				t.Fatal(err)
			}
		}
		state := read()
		if state.FeatureEnabled != want[test.ID] || state.HasPersistedState {
			t.Errorf("%s SQL state = %+v, live Python decision = %t", test.ID, state, want[test.ID])
		}
	}

	pgseed.User(ctx, t, pool, "50000000-0000-4000-8000-000000000001")
	pgseed.DevConversation(ctx, t, pool, "30000000-0000-4000-8000-000000000001", orgID, "50000000-0000-4000-8000-000000000001")
	if _, err := pool.Exec(ctx, `UPDATE feature_flags SET is_enabled = FALSE WHERE key = 'ask_dev'`); err != nil {
		t.Fatal(err)
	}
	state := read()
	if state.FeatureEnabled || !state.HasPersistedState || !state.Eligible() {
		t.Fatalf("disabled-after-use state = %+v, persisted cleanup must remain eligible", state)
	}
}

func pythonAskDevFeatureDecisions[T any](t *testing.T, cases []T) map[string]bool {
	t.Helper()
	encoded, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate Ask Dev Python parity oracle")
	}
	script := filepath.Join(filepath.Dir(currentFile), "testdata", "python_ask_dev_feature_oracle.py")
	python := pyoracle.Resolve(t, repositoryRoot(t))
	command := exec.Command(python, script)
	command.Stdin = bytes.NewReader(encoded)
	output, err := command.Output()
	if err != nil {
		t.Fatalf("execute live Python Ask Dev feature oracle: %v", pyoracle.RunError(python, err, nil))
	}
	var decisions map[string]bool
	if err := json.Unmarshal(output, &decisions); err != nil {
		t.Fatalf("decode live Python Ask Dev feature oracle: %v", err)
	}
	if len(decisions) != len(cases) {
		t.Fatalf("live Python Ask Dev feature oracle returned %d decisions for %d cases", len(decisions), len(cases))
	}
	return decisions
}
