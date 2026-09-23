package licensing

import (
	"context"
	"encoding/json"
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

// row is the fakeRow.values builder matching loadState's own Scan order:
// feature.min_tier, feature.is_enabled, org_override.is_enabled,
// org_override.expires_at, org_override.config, license.tier,
// license.features_override, organization.tier.
func row(
	featureMinTier, featureEnabled, overrideEnabled any, overrideExpiresAt *time.Time,
	overrideConfig []byte, licenseTier any, featuresOverride []byte, orgTier any,
) []any {
	return []any{featureMinTier, featureEnabled, overrideEnabled, overrideExpiresAt, overrideConfig, licenseTier, featuresOverride, orgTier}
}

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
	queryer := fakeQueryer{row: fakeRow{values: row(
		ptr("community"), ptr(true),
		(*bool)(nil), nil, nil,
		ptr("enterprise"), []byte(`{"agent_context_runtime": true}`),
		ptr("community"),
	)}}
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
	queryer := fakeQueryer{row: fakeRow{values: row(
		(*string)(nil), (*bool)(nil),
		(*bool)(nil), nil, nil,
		(*string)(nil), []byte(nil),
		ptr("team"),
	)}}
	state, err := loadState(context.Background(), queryer, "org-1", "agent_context_runtime", evaluatedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.OrgTier != "team" {
		t.Fatalf("OrgTier = %q, want organizations.tier fallback %q", state.OrgTier, "team")
	}
}

func TestLoadStateMalformedFeaturesOverrideJSONIsAnError(t *testing.T) {
	queryer := fakeQueryer{row: fakeRow{values: row(
		ptr("community"), ptr(true),
		(*bool)(nil), nil, nil,
		ptr("community"), []byte(`not json`),
		ptr("community"),
	)}}
	_, err := loadState(context.Background(), queryer, "org-1", "agent_context_runtime", evaluatedAt)
	if err == nil {
		t.Fatal("expected an error decoding malformed features_override JSON")
	}
}

// TestLoadStateNonObjectFeaturesOverrideIsSilentlyNoOverride is the
// regression proof for the confirmed P1: gating.py's own guard treats
// raw_license_overrides as the override source only when it is a dict,
// else as {} -- valid JSON that decodes to something other than an object
// is a no-override state, never a read failure. Live-confirmed with
// features_override exactly `[]`: an active org override was denied with a
// false ErrUnavailable before this fix.
func TestLoadStateNonObjectFeaturesOverrideIsSilentlyNoOverride(t *testing.T) {
	for _, encoded := range []string{`[]`, `[1]`, `"x"`, `5`, `true`, `null`} {
		t.Run(encoded, func(t *testing.T) {
			queryer := fakeQueryer{row: fakeRow{values: row(
				ptr("community"), ptr(true),
				ptr(true), nil, nil, // an active org override, so the decision
				// is Allowed once the (wrongly-erroring) license decode is
				// out of the way.
				ptr("community"), []byte(encoded),
				ptr("community"),
			)}}
			state, err := loadState(context.Background(), queryer, "org-1", "agent_context_runtime", evaluatedAt)
			if err != nil {
				t.Fatalf("unexpected error decoding valid non-object JSON %q: %v", encoded, err)
			}
			if state.LicenseOverride != nil {
				t.Fatalf("LicenseOverride = %v, want nil (non-object JSON carries no override)", *state.LicenseOverride)
			}
			decision := Decide("agent_context_runtime", state)
			if !decision.Allowed || decision.Reason != ReasonEnabledByOrgOverride {
				t.Fatalf("decision = %+v, want allowed by the active org override, unaffected by features_override %q", decision, encoded)
			}
		})
	}
}

// TestLoadStateOverflowingLicenseOverrideNumberIsStillDecoded is the
// regression proof for the confirmed P1 that reached live Postgres: a
// features_override value of {"agent_context_runtime": 1e10000} (a
// magnitude that overflows float64) must NOT error the whole column --
// Python's json.loads decodes the literal to Inf and keeps going. Before
// this fix, encoding/json's default float64 number handling refused the
// whole Unmarshal, so this exact row returned ErrUnavailable (a false 503)
// for an org whose license genuinely granted the feature.
func TestLoadStateOverflowingLicenseOverrideNumberIsStillDecoded(t *testing.T) {
	queryer := fakeQueryer{row: fakeRow{values: row(
		ptr("community"), ptr(true),
		(*bool)(nil), nil, nil,
		ptr("community"), []byte(`{"agent_context_runtime": 1e10000}`),
		ptr("community"),
	)}}
	state, err := loadState(context.Background(), queryer, "org-1", "agent_context_runtime", evaluatedAt)
	if err != nil {
		t.Fatalf("unexpected error decoding an overflowing-but-valid JSON number: %v", err)
	}
	if state.LicenseOverride == nil || !*state.LicenseOverride {
		t.Fatalf("LicenseOverride = %v, want true (1e10000 overflows to +Inf, which is truthy)", state.LicenseOverride)
	}
	decision := Decide("agent_context_runtime", state)
	if !decision.Allowed || decision.Reason != ReasonEnabledByLicenseOverride {
		t.Fatalf("decision = %+v, want allowed by license override", decision)
	}
}

// TestLoadStatePopulatesOrgOverrideConfig is the regression proof for the
// confirmed P2: org_feature_overrides.config must reach Decision.Config on
// the enabled_by_org_override path, matching Python's
// `config=context.org_override.config`. Before this fix, loadState never
// selected the column at all.
func TestLoadStatePopulatesOrgOverrideConfig(t *testing.T) {
	queryer := fakeQueryer{row: fakeRow{values: row(
		ptr("community"), ptr(true),
		ptr(true), nil, []byte(`{"customer_limit": 17}`),
		(*string)(nil), []byte(nil),
		ptr("community"),
	)}}
	state, err := loadState(context.Background(), queryer, "org-1", "agent_context_runtime", evaluatedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.OrgOverride == nil || state.OrgOverride.Config == nil {
		t.Fatalf("OrgOverride.Config = %v, want the decoded config map", state.OrgOverride)
	}
	if (*state.OrgOverride.Config)["customer_limit"].(json.Number).String() != "17" {
		t.Fatalf("Config = %+v, want customer_limit=17", *state.OrgOverride.Config)
	}
	decision := Decide("agent_context_runtime", state)
	if !decision.Allowed || decision.Config == nil {
		t.Fatalf("decision = %+v, want allowed with Config propagated", decision)
	}
}

// TestLoadStateNonObjectOrgOverrideConfigIsSilentlyNil mirrors the
// features_override guard for org_feature_overrides.config: gating.py's
// _override_snapshot applies the identical isinstance(dict) check to
// override.config.
func TestLoadStateNonObjectOrgOverrideConfigIsSilentlyNil(t *testing.T) {
	queryer := fakeQueryer{row: fakeRow{values: row(
		ptr("community"), ptr(true),
		ptr(true), nil, []byte(`[]`),
		(*string)(nil), []byte(nil),
		ptr("community"),
	)}}
	state, err := loadState(context.Background(), queryer, "org-1", "agent_context_runtime", evaluatedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.OrgOverride.Config != nil {
		t.Fatalf("Config = %v, want nil for non-object JSON", *state.OrgOverride.Config)
	}
}
