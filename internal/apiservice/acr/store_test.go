package acr

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestPostgresEntitlementStoreLookupRejectsMalformedOrgIDBeforeTouchingThePool(t *testing.T) {
	// Pool is nil (unconfigured), and Lookup must still return
	// ErrOrgNotFound, not ErrUnavailable, for a malformed org_id -- the
	// format check runs before the pool is touched (store.go's own comment).
	store := PostgresEntitlementStore{Pool: nil}
	_, err := store.Lookup(context.Background(), "not-a-uuid")
	if !errors.Is(err, ErrOrgNotFound) {
		t.Fatalf("Lookup(malformed) error = %v, want ErrOrgNotFound", err)
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup(malformed) error = %v, must not also be ErrUnavailable", err)
	}
}

func TestPostgresEntitlementStoreLookupWithNilPoolAndValidOrgID(t *testing.T) {
	store := PostgresEntitlementStore{Pool: nil}
	_, err := store.Lookup(context.Background(), uuid.NewString())
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup(valid, nil pool) error = %v, want ErrUnavailable", err)
	}
}

// fakeRow implements pgx.Row over a canned Scan outcome, for loadFeatureState
// unit tests that do not need a real Postgres connection.
type fakeRow struct {
	values []any
	err    error
}

func (row fakeRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(dest) != len(row.values) {
		panic("fakeRow: dest/values length mismatch")
	}
	for i, d := range dest {
		switch target := d.(type) {
		case *string:
			*target = row.values[i].(string)
		case **string:
			*target, _ = row.values[i].(*string)
		case **bool:
			*target, _ = row.values[i].(*bool)
		case **time.Time:
			*target, _ = row.values[i].(*time.Time)
		case *[]byte:
			*target, _ = row.values[i].([]byte)
		default:
			panic("fakeRow: unsupported scan target type")
		}
	}
	return nil
}

type fakeQueryer struct {
	row fakeRow
}

func (q fakeQueryer) QueryRow(context.Context, string, ...any) pgx.Row {
	return q.row
}

var _ featureStateQueryer = fakeQueryer{}

func ptr[T any](v T) *T { return &v }

func TestLoadFeatureStateOrgNotFound(t *testing.T) {
	queryer := fakeQueryer{row: fakeRow{err: pgx.ErrNoRows}}
	_, found, err := loadFeatureState(context.Background(), queryer, uuid.NewString(), evaluatedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("found = true, want false on pgx.ErrNoRows")
	}
}

func TestLoadFeatureStateRegisteredWithLicenseOverride(t *testing.T) {
	queryer := fakeQueryer{row: fakeRow{values: []any{
		"community",                     // organization.tier
		ptr("community"),                // feature.min_tier
		ptr(true),                       // feature.is_enabled
		(*bool)(nil), (*time.Time)(nil), // org_override.is_enabled, expires_at
		ptr("enterprise"),                         // license.tier
		[]byte(`{"agent_context_runtime": true}`), // license.features_override
	}}}
	state, found, err := loadFeatureState(context.Background(), queryer, uuid.NewString(), evaluatedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("found = false, want true")
	}
	if !state.Registered || !state.GloballyEnabled {
		t.Fatalf("state = %+v, want Registered && GloballyEnabled", state)
	}
	if state.OrgTier != "enterprise" {
		t.Fatalf("OrgTier = %q, want license tier %q (license row present takes priority)", state.OrgTier, "enterprise")
	}
	if state.LicenseOverride == nil || !*state.LicenseOverride {
		t.Fatalf("LicenseOverride = %v, want true", state.LicenseOverride)
	}
	decision := decideAgentContextRuntimeFeature(state)
	if !decision.Allowed || decision.Reason != "enabled_by_license_override" {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestLoadFeatureStateFeatureNotRegisteredFallsBackToOrganizationTier(t *testing.T) {
	queryer := fakeQueryer{row: fakeRow{values: []any{
		"team",                       // organization.tier
		(*string)(nil), (*bool)(nil), // feature.min_tier, is_enabled: no row
		(*bool)(nil), (*time.Time)(nil), // org_override
		(*string)(nil), []byte(nil), // no org_licenses row
	}}}
	state, found, err := loadFeatureState(context.Background(), queryer, uuid.NewString(), evaluatedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !found {
		t.Fatal("found = false, want true (organization row matched)")
	}
	if state.Registered {
		t.Fatalf("Registered = true, want false: no feature_flags row matched")
	}
	if state.OrgTier != "team" {
		t.Fatalf("OrgTier = %q, want organizations.tier fallback %q (no org_licenses row)", state.OrgTier, "team")
	}
	decision := decideAgentContextRuntimeFeature(state)
	if decision.Allowed || decision.Reason != "feature_not_registered" {
		t.Fatalf("decision = %+v, want closed feature_not_registered", decision)
	}
}

func TestLoadFeatureStateMalformedFeaturesOverrideJSONIsUnavailable(t *testing.T) {
	queryer := fakeQueryer{row: fakeRow{values: []any{
		"community",
		ptr("community"), ptr(true),
		(*bool)(nil), (*time.Time)(nil),
		ptr("community"), []byte(`not json`),
	}}}
	_, _, err := loadFeatureState(context.Background(), queryer, uuid.NewString(), evaluatedAt)
	if err == nil {
		t.Fatal("expected an error decoding malformed features_override JSON")
	}
}
