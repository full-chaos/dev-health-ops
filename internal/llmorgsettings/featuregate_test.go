package llmorgsettings

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func boolPtr(b bool) *bool { return &b }

// TestDecideByoLLM covers evaluate_org_feature_sync's precedence
// (decide_feature) as narrowed to byo_llm in decideByoLLM's own doc
// comment: registered/enabled gates, then org override, then license
// override, then tier -- matching team-lead's requested precedence matrix
// (flag off / override on / override off / tier below floor / tier at
// floor / no license) plus the "unregistered" backward-compat state
// byo_llm_flag_state's caller treats as ungated.
func TestDecideByoLLM(t *testing.T) {
	now := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	cases := []struct {
		name  string
		state byoLLMFeatureState
		want  string
	}{
		{
			name:  "feature row absent -> unregistered (backward compat, ungated)",
			state: byoLLMFeatureState{registered: false, minTier: "community", orgTier: "team"},
			want:  flagStateUnregistered,
		},
		{
			name: "invalid stored min_tier -> disabled",
			state: byoLLMFeatureState{
				registered: true, minTier: "not-a-tier", globallyEnabled: true, orgTier: "enterprise",
			},
			want: flagStateDisabled,
		},
		{
			name: "globally disabled -> disabled even at enterprise tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: false, orgTier: "enterprise",
			},
			want: flagStateDisabled,
		},
		{
			name: "team tier, no override, no license -> enabled by tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "team",
			},
			want: flagStateEnabled,
		},
		{
			name: "community tier, no override -> tier required, disabled",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "community",
			},
			want: flagStateDisabled,
		},
		{
			name: "org override enabled wins over insufficient tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "community",
				overrideEnabled: boolPtr(true),
			},
			want: flagStateEnabled,
		},
		{
			name: "org override disabled wins over sufficient tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "enterprise",
				overrideEnabled: boolPtr(false),
			},
			want: flagStateDisabled,
		},
		{
			name: "expired org override falls through to tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "team",
				overrideEnabled: boolPtr(false), overrideExpires: &past,
			},
			want: flagStateEnabled,
		},
		{
			name: "org override not yet expired still applies",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "team",
				overrideEnabled: boolPtr(false), overrideExpires: &future,
			},
			want: flagStateDisabled,
		},
		{
			name: "license override enabled wins over insufficient tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "community",
				licenseOverride: boolPtr(true),
			},
			want: flagStateEnabled,
		},
		{
			name: "license override disabled wins over sufficient tier",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "enterprise",
				licenseOverride: boolPtr(false),
			},
			want: flagStateDisabled,
		},
		{
			name: "org override beats license override",
			state: byoLLMFeatureState{
				registered: true, minTier: "team", globallyEnabled: true, orgTier: "enterprise",
				overrideEnabled: boolPtr(true), licenseOverride: boolPtr(false),
			},
			want: flagStateEnabled,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decideByoLLM(tc.state, now)
			if got != tc.want {
				t.Fatalf("decideByoLLM() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDecideByoLLM_FloorBypassSanity documents WHY byoLLMFlagState cannot
// simply call decideByoLLM alone: decideByoLLM (== decide_feature, no
// external floor) allows a COMMUNITY-tier org through on a positive org
// override, same as evaluate_org_feature_sync would for any ordinary
// feature. byo_llm's TEAM-tier floor is a SEPARATE, EARLIER check in
// byoLLMFlagState (mirroring feature_flag_state's own min_tier
// short-circuit) specifically so this case does NOT enable byo_llm for a
// COMMUNITY org. The floor check plus the real DB read together are
// covered end-to-end by the bigboy integration test (seeds this exact
// override-on/community-tier row and asserts the org is refused).
func TestDecideByoLLM_FloorBypassSanity(t *testing.T) {
	state := byoLLMFeatureState{
		registered: true, minTier: "team", globallyEnabled: true, orgTier: "community",
		overrideEnabled: boolPtr(true),
	}
	if got := decideByoLLM(state, time.Now()); got != flagStateEnabled {
		t.Fatalf("decideByoLLM (no floor) = %q, want enabled -- this is exactly why "+
			"byoLLMFlagState needs its own floor check ahead of it", got)
	}
}

// TestJsonTruthy is the codex round 1 P2 regression: a text-match on a
// fixed literal set ("0", "[]", "{}", ...) missed every OTHER falsy
// spelling of the same JSON value (0.0, -0, 0e0, whitespace padding) --
// this table decodes the JSON and applies Python's real bool(value)
// truthiness instead.
func TestJsonTruthy(t *testing.T) {
	cases := []struct {
		raw  string
		want bool
	}{
		{`null`, false},
		{`false`, false},
		{`0`, false},
		{`0.0`, false},
		{`-0`, false},
		{`0e0`, false},
		{`  0  `, false},
		{`""`, false},
		{`[]`, false},
		{`{}`, false},
		{`true`, true},
		{`1`, true},
		{`0.1`, true},
		{`"false"`, true}, // a non-empty STRING, even one spelling "false", is truthy
		{`"0"`, true},     // same: non-empty string
		{`[0]`, true},     // non-empty list, even one containing a falsy element
		{`{"a":false}`, true},
		{`not-valid-json`, false}, // malformed: fail closed, not treated as an override

		// codex round 2, P2: a magnitude too large for float64 is valid
		// JSON syntax (features_override has no magnitude constraint) --
		// Python's json.loads/float() silently overflow to +-Inf rather
		// than raising, and bool(inf) is True. Must NOT fail closed like
		// a genuinely malformed literal does.
		{`1e400`, true},  // overflows to +Inf -- nonzero, truthy
		{`-1e400`, true}, // overflows to -Inf -- nonzero, truthy
		{`0e400`, false}, // zero mantissa: exactly representable as 0, no overflow at all
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			if got := jsonTruthy(json.RawMessage(tc.raw)); got != tc.want {
				t.Errorf("jsonTruthy(%s) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

// fakeQueryRow implements pgx.Row for the hermetic regression test below
// (codex round 2, P3). scanValues assign to the real Scan destinations
// positionally via reflection -- a nil entry leaves a nullable (**T)
// destination nil, mirroring a SQL NULL.
type fakeQueryRow struct {
	values []any
	err    error
}

func (r fakeQueryRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, d := range dest {
		if i >= len(r.values) {
			continue
		}
		if err := assignScanDest(d, r.values[i]); err != nil {
			return err
		}
	}
	return nil
}

// assignScanDest assigns value into dest -- a pointer produced by one of
// this package's own QueryRow(...).Scan(&x) call sites. Handles the two
// shapes those sites actually use: *T (a plain destination) and **T (a
// destination that is itself a *T field/var, for a nullable column).
func assignScanDest(dest, value any) error {
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr || dv.IsNil() {
		return fmt.Errorf("scan destination is not a non-nil pointer: %T", dest)
	}
	elem := dv.Elem()
	if elem.Kind() == reflect.Ptr {
		if value == nil {
			elem.Set(reflect.Zero(elem.Type()))
			return nil
		}
		inner := reflect.New(elem.Type().Elem())
		inner.Elem().Set(reflect.ValueOf(value))
		elem.Set(inner)
		return nil
	}
	if value == nil {
		elem.Set(reflect.Zero(elem.Type()))
		return nil
	}
	elem.Set(reflect.ValueOf(value))
	return nil
}

// sequencedFakeQueryer is a featureRowQueryer test double, hermetic (no
// Postgres): it dispatches on a distinctive SQL substring and lets the
// "organizations" query return a DIFFERENT tier on each successive call
// -- the one thing a real container-backed test cannot cheaply prove
// (that the two reads genuinely observe different points in time), but a
// fake naturally can.
type sequencedFakeQueryer struct {
	t *testing.T

	organizationsCalls      int
	organizationsTierByCall []string // tier returned on the Nth call (1-indexed); last entry repeats past the end
}

func (f *sequencedFakeQueryer) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	switch {
	case strings.Contains(sql, "to_regclass"):
		return fakeQueryRow{values: []any{true}}
	case strings.Contains(sql, "org_licenses"):
		return fakeQueryRow{err: pgx.ErrNoRows}
	case strings.Contains(sql, "FROM organizations"):
		f.organizationsCalls++
		idx := f.organizationsCalls - 1
		if idx >= len(f.organizationsTierByCall) {
			idx = len(f.organizationsTierByCall) - 1
		}
		return fakeQueryRow{values: []any{f.organizationsTierByCall[idx]}}
	case strings.Contains(sql, "feature_flags"):
		return fakeQueryRow{values: []any{"team", true, nil, nil}}
	default:
		f.t.Fatalf("unexpected query: %s", sql)
		return nil
	}
}

// TestByoLLMFlagState_TierReadsAreIndependent is the codex round 2, P3
// regression test: the round-1 torn-read fix (splitting the TEAM-tier
// floor read from the main decision's own tier read) had no committed
// test proving the two reads are genuinely INDEPENDENT, only that the
// final DECISION was correct for a few static states. This fakes a tier
// that changes BETWEEN the two reads (team at the floor check, community
// by the main decision -- an org downgraded mid-resolution) and asserts
// the FRESH value drives the main decision, matching Python's own
// double-read shape. If byoLLMFlagState ever regresses to sharing one
// read for both purposes, this fails: the floor's stale "team" would
// also (wrongly) drive the main decision, producing "enabled" instead of
// "disabled".
func TestByoLLMFlagState_TierReadsAreIndependent(t *testing.T) {
	queryer := &sequencedFakeQueryer{
		t:                       t,
		organizationsTierByCall: []string{"team", "community"},
	}
	orgID := uuid.New()

	state, err := byoLLMFlagState(context.Background(), queryer, orgID, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state != flagStateDisabled {
		t.Fatalf("state = %q, want %q -- the main decision must use the FRESH "+
			"(second) tier read, not the floor check's stale first read", state, flagStateDisabled)
	}
	if queryer.organizationsCalls != 2 {
		t.Fatalf("organizations query called %d time(s), want exactly 2 "+
			"(one for the floor check, one for the main decision, genuinely independent)",
			queryer.organizationsCalls)
	}
}

// orderedCallQueryer is a featureRowQueryer test double (codex round 3, P1
// regression) that records the ORDER query kinds are dispatched in,
// classified by a distinctive SQL substring, and returns one fixed row per
// kind.
type orderedCallQueryer struct {
	t     *testing.T
	calls []string
}

func (f *orderedCallQueryer) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	switch {
	case strings.Contains(sql, "FROM feature_flags"):
		f.calls = append(f.calls, "feature_flags")
		return fakeQueryRow{values: []any{"team", true, nil, nil}}
	case strings.Contains(sql, "org_licenses"):
		f.calls = append(f.calls, "org_licenses")
		return fakeQueryRow{values: []any{"community", nil}}
	default:
		f.t.Fatalf("unexpected query: %s", sql)
		return nil
	}
}

// TestLoadByoLLMFeatureState_QueriesFeatureBeforeTier is the codex round 3,
// P1 regression: load_feature_rows_sync (feature_decision_store.py:39-65)
// queries FeatureFlag/OrgFeatureOverride BEFORE OrgLicense/
// Organization.tier, in that fixed order, within one function call. An
// earlier version of loadByoLLMFeatureState read tier/license FIRST and
// features SECOND -- the reverse order -- which admits a torn-read
// interleaving Python's own order cannot produce: a concurrent transaction
// that downgrades the org's tier AND deletes a disabling override,
// committing between the two reads, lands as OLD tier + NEW (absent)
// override in the old (reversed) Go order, wrongly enabling BYO -- a
// combination Python can never observe because it always reads the
// override before the tier. If loadByoLLMFeatureState ever regresses to
// the old order, this test's call-order assertion fails even though the
// single-threaded, non-concurrent decision value alone would look correct.
func TestLoadByoLLMFeatureState_QueriesFeatureBeforeTier(t *testing.T) {
	queryer := &orderedCallQueryer{t: t}
	state, err := loadByoLLMFeatureState(context.Background(), queryer, uuid.New())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"feature_flags", "org_licenses"}
	if !reflect.DeepEqual(queryer.calls, want) {
		t.Fatalf("call order = %v, want %v -- loadByoLLMFeatureState must query the "+
			"feature/override row BEFORE org tier/license, matching load_feature_rows_sync's "+
			"own order", queryer.calls, want)
	}
	if state.orgTier != "community" {
		t.Fatalf("state.orgTier = %q, want %q", state.orgTier, "community")
	}
}

// singleRowQueryer is a featureRowQueryer test double that answers any
// query matching a fixed SQL substring with one canned row, and fails the
// test on any other query -- used below to exercise
// resolveOrgTierAndLicenseOverrides in isolation.
type singleRowQueryer struct {
	t   *testing.T
	sql string
	row fakeQueryRow
}

func (f *singleRowQueryer) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	if !strings.Contains(sql, f.sql) {
		f.t.Fatalf("unexpected query: %s", sql)
	}
	return f.row
}

// TestResolveOrgTierAndLicenseOverrides_NonObjectOverride is the codex
// round 3, P2 regression: org_licenses.features_override is a free-form
// JSON column (models/licensing.py:347), and _decisions_from_rows
// (feature_decisions.py:79) only treats it as a license-override map when
// isinstance(raw, dict) -- any other JSON type (array, string, number,
// bool, null) falls through to {} there, meaning "no license override",
// NOT a lookup failure. The old code unmarshaled straight into
// map[string]json.RawMessage and returned a hard error for any non-object
// top-level value, wrongly making a BYO-configured org's flag resolution
// fail closed on a value Python treats as simply absent.
func TestResolveOrgTierAndLicenseOverrides_NonObjectOverride(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"array", `[true]`},
		{"string", `"enabled"`},
		{"number", `42`},
		{"bool", `true`},
		{"null", `null`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			queryer := &singleRowQueryer{
				t: t, sql: "org_licenses",
				row: fakeQueryRow{values: []any{"team", []byte(tc.raw)}},
			}
			tier, overrides, err := resolveOrgTierAndLicenseOverrides(context.Background(), queryer, uuid.New())
			if err != nil {
				t.Fatalf("unexpected error for non-object features_override %s: %v", tc.raw, err)
			}
			if tier != "team" {
				t.Fatalf("tier = %q, want %q", tier, "team")
			}
			if len(overrides) != 0 {
				t.Fatalf("overrides = %v, want none -- a non-object features_override "+
					"must be ignored (no license override), not parsed as one", overrides)
			}
		})
	}
}

// TestResolveOrgTierAndLicenseOverrides_MalformedOverrideStillErrors proves
// the P2 fix above didn't widen into swallowing genuinely malformed JSON
// too: Python's ORM already validates JSON on write, so there is no
// documented Python behavior for unparseable column content -- this must
// still fail closed (propagate an error), not silently treat corrupt
// storage as "no override".
func TestResolveOrgTierAndLicenseOverrides_MalformedOverrideStillErrors(t *testing.T) {
	queryer := &singleRowQueryer{
		t: t, sql: "org_licenses",
		row: fakeQueryRow{values: []any{"team", []byte(`{not-valid-json`)}},
	}
	_, _, err := resolveOrgTierAndLicenseOverrides(context.Background(), queryer, uuid.New())
	if err == nil {
		t.Fatalf("expected an error for genuinely malformed JSON, got nil")
	}
}
