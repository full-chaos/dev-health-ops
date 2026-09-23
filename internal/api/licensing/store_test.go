package licensing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPostgresStoreDecideNilPool(t *testing.T) {
	store := PostgresStore{Pool: nil}
	_, err := store.Decide(context.Background(), "org-1", "agent_context_runtime")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("error = %v, want ErrUnavailable", err)
	}
}

// fakeRow implements pgx.Row over a canned Scan outcome.
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

type fakeQueryer struct{ row fakeRow }

func (q fakeQueryer) QueryRow(context.Context, string, ...any) pgx.Row { return q.row }

var _ stateQueryer = fakeQueryer{}

func ptr[T any](v T) *T { return &v }

func TestLoadStateFeatureNotRegistered(t *testing.T) {
	queryer := fakeQueryer{row: fakeRow{err: pgx.ErrNoRows}}
	state, err := loadState(context.Background(), queryer, "org-1", "agent_context_runtime", evaluatedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Registered {
		t.Fatal("Registered = true, want false on pgx.ErrNoRows")
	}
	decision := Decide("agent_context_runtime", state)
	if decision.Allowed || decision.Reason != ReasonFeatureNotRegistered {
		t.Fatalf("decision = %+v, want closed feature_not_registered", decision)
	}
}

func TestLoadStateLicenseTierTakesPriorityOverOrganizationTier(t *testing.T) {
	queryer := fakeQueryer{row: fakeRow{values: []any{
		ptr("community"), ptr(true), // feature.min_tier, is_enabled
		(*bool)(nil), (*time.Time)(nil), // org_override
		ptr("enterprise"), []byte(`{"agent_context_runtime": true}`), // license.tier, features_override
		ptr("community"), // organization.tier
	}}}
	state, err := loadState(context.Background(), queryer, "org-1", "agent_context_runtime", evaluatedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.OrgTier != "enterprise" {
		t.Fatalf("OrgTier = %q, want license tier %q", state.OrgTier, "enterprise")
	}
	if state.LicenseOverride == nil || !*state.LicenseOverride {
		t.Fatalf("LicenseOverride = %v, want true", state.LicenseOverride)
	}
}

func TestLoadStateFallsBackToOrganizationTierWhenNoLicenseRow(t *testing.T) {
	queryer := fakeQueryer{row: fakeRow{values: []any{
		(*string)(nil), (*bool)(nil), // no feature_flags row
		(*bool)(nil), (*time.Time)(nil),
		(*string)(nil), []byte(nil), // no org_licenses row
		ptr("team"), // organization.tier
	}}}
	state, err := loadState(context.Background(), queryer, "org-1", "agent_context_runtime", evaluatedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.OrgTier != "team" {
		t.Fatalf("OrgTier = %q, want organizations.tier fallback %q", state.OrgTier, "team")
	}
}

func TestLoadStateMalformedFeaturesOverrideJSONIsAnError(t *testing.T) {
	queryer := fakeQueryer{row: fakeRow{values: []any{
		ptr("community"), ptr(true),
		(*bool)(nil), (*time.Time)(nil),
		ptr("community"), []byte(`not json`),
		ptr("community"),
	}}}
	_, err := loadState(context.Background(), queryer, "org-1", "agent_context_runtime", evaluatedAt)
	if err == nil {
		t.Fatal("expected an error decoding malformed features_override JSON")
	}
}
