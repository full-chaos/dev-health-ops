package workgraph

// Oracle: every case here is translated from
// tests/graphql/test_team_attribution_graphql.py's
// test_maps_rows_to_provenance / test_every_source_enum_maps /
// test_every_confidence_enum_maps / test_unknown_enum_values_degrade_safely /
// test_null_team_id_becomes_none / test_query_is_org_scoped_final_and_bounded
// -- an independent check against the Python resolver's own test suite,
// not a hand-invented Go-only expectation.

import (
	"context"
	"errors"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

type witaFakeRow struct {
	workItemID, provider, teamID, teamName, source, confidence, evidence string
	isPrimary                                                            uint8
}

type witaFakeScanner struct {
	rows []witaFakeRow
	i    int
}

func (f *witaFakeScanner) Next() bool {
	if f.i >= len(f.rows) {
		return false
	}
	f.i++
	return true
}

func (f *witaFakeScanner) Scan(dest ...any) error {
	r := f.rows[f.i-1]
	if len(dest) != 8 {
		return errors.New("witaFakeScanner: dest arity mismatch")
	}
	*(dest[0].(*string)) = r.workItemID
	*(dest[1].(*string)) = r.provider
	*(dest[2].(*string)) = r.teamID
	*(dest[3].(*string)) = r.teamName
	*(dest[4].(*string)) = r.source
	*(dest[5].(*string)) = r.confidence
	*(dest[6].(*uint8)) = r.isPrimary
	*(dest[7].(*string)) = r.evidence
	return nil
}

func (f *witaFakeScanner) Err() error   { return nil }
func (f *witaFakeScanner) Close() error { return nil }

type witaFakeClient struct {
	scanner    *witaFakeScanner
	lastQuery  string
	lastParams []dhclickhouse.Binding
}

func (c *witaFakeClient) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.lastQuery = statement
	c.lastParams = bindings
	return c.scanner, nil
}

func witaRow(overrides map[string]any) witaFakeRow {
	r := witaFakeRow{
		workItemID: "linear:CHAOS-1", provider: "linear", teamID: "team-a", teamName: "Team A",
		source: "native_team", confidence: "high", isPrimary: 1, evidence: "native_team_key=CHAOS",
	}
	for k, v := range overrides {
		switch k {
		case "workItemID":
			r.workItemID = v.(string)
		case "teamID":
			r.teamID = v.(string)
		case "teamName":
			r.teamName = v.(string)
		case "source":
			r.source = v.(string)
		case "confidence":
			r.confidence = v.(string)
		case "isPrimary":
			r.isPrimary = v.(uint8)
		case "evidence":
			r.evidence = v.(string)
		}
	}
	return r
}

// Port of test_maps_rows_to_provenance.
func TestResolveWorkItemTeamAttributions_MapsRowsToProvenance(t *testing.T) {
	rows := []witaFakeRow{
		witaRow(map[string]any{"source": "native_team", "confidence": "high", "isPrimary": uint8(1)}),
		witaRow(map[string]any{"source": "manual_fallback", "confidence": "manual", "isPrimary": uint8(0), "evidence": "scope_type=repo"}),
	}
	client := &witaFakeClient{scanner: &witaFakeScanner{rows: rows}}
	got, err := ResolveWorkItemTeamAttributions(context.Background(), client, "test-org", []string{"linear:CHAOS-1"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	if got[0].Source != model.TeamAttributionSourceNativeTeam {
		t.Errorf("got[0].Source = %q, want NATIVE_TEAM", got[0].Source)
	}
	if got[0].Confidence != model.TeamAttributionConfidenceHigh {
		t.Errorf("got[0].Confidence = %q, want HIGH", got[0].Confidence)
	}
	if !got[0].IsPrimary {
		t.Error("got[0].IsPrimary = false, want true")
	}
	if got[0].TeamID == nil || *got[0].TeamID != "team-a" {
		t.Errorf("got[0].TeamID = %v, want team-a", got[0].TeamID)
	}
	if got[1].Source != model.TeamAttributionSourceManualFallback {
		t.Errorf("got[1].Source = %q, want MANUAL_FALLBACK", got[1].Source)
	}
	if got[1].Confidence != model.TeamAttributionConfidenceManual {
		t.Errorf("got[1].Confidence = %q, want MANUAL", got[1].Confidence)
	}
	if got[1].IsPrimary {
		t.Error("got[1].IsPrimary = true, want false")
	}
	if got[1].Evidence != "scope_type=repo" {
		t.Errorf("got[1].Evidence = %q, want scope_type=repo", got[1].Evidence)
	}
}

// Port of test_every_source_enum_maps.
func TestResolveWorkItemTeamAttributions_EverySourceEnumMaps(t *testing.T) {
	cases := []struct {
		raw  string
		want model.TeamAttributionSource
	}{
		{"native_team", model.TeamAttributionSourceNativeTeam},
		{"issue_project", model.TeamAttributionSourceIssueProject},
		{"project_ownership", model.TeamAttributionSourceProjectOwnership},
		{"repo_ownership", model.TeamAttributionSourceRepoOwnership},
		{"assignee_membership", model.TeamAttributionSourceAssigneeMembership},
		{"linked_issue", model.TeamAttributionSourceLinkedIssue},
		{"author_membership", model.TeamAttributionSourceAuthorMembership},
		{"manual_fallback", model.TeamAttributionSourceManualFallback},
		{"unassigned", model.TeamAttributionSourceUnassigned},
	}
	for _, c := range cases {
		client := &witaFakeClient{scanner: &witaFakeScanner{rows: []witaFakeRow{witaRow(map[string]any{"source": c.raw})}}}
		got, err := ResolveWorkItemTeamAttributions(context.Background(), client, "test-org", nil, nil)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", c.raw, err)
		}
		if got[0].Source != c.want {
			t.Errorf("source %q -> %q, want %q", c.raw, got[0].Source, c.want)
		}
	}
}

// Port of test_every_confidence_enum_maps.
func TestResolveWorkItemTeamAttributions_EveryConfidenceEnumMaps(t *testing.T) {
	cases := []struct {
		raw  string
		want model.TeamAttributionConfidence
	}{
		{"high", model.TeamAttributionConfidenceHigh},
		{"medium", model.TeamAttributionConfidenceMedium},
		{"low", model.TeamAttributionConfidenceLow},
		{"manual", model.TeamAttributionConfidenceManual},
		{"none", model.TeamAttributionConfidenceNone},
	}
	for _, c := range cases {
		client := &witaFakeClient{scanner: &witaFakeScanner{rows: []witaFakeRow{witaRow(map[string]any{"confidence": c.raw})}}}
		got, err := ResolveWorkItemTeamAttributions(context.Background(), client, "test-org", nil, nil)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", c.raw, err)
		}
		if got[0].Confidence != c.want {
			t.Errorf("confidence %q -> %q, want %q", c.raw, got[0].Confidence, c.want)
		}
	}
}

// Port of test_unknown_enum_values_degrade_safely.
func TestResolveWorkItemTeamAttributions_UnknownEnumValuesDegradeSafely(t *testing.T) {
	client := &witaFakeClient{scanner: &witaFakeScanner{rows: []witaFakeRow{
		witaRow(map[string]any{"source": "some_future_source", "confidence": "ultra"}),
	}}}
	got, err := ResolveWorkItemTeamAttributions(context.Background(), client, "test-org", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got[0].Source != model.TeamAttributionSourceUnassigned {
		t.Errorf("Source = %q, want UNASSIGNED fallback", got[0].Source)
	}
	if got[0].Confidence != model.TeamAttributionConfidenceNone {
		t.Errorf("Confidence = %q, want NONE fallback", got[0].Confidence)
	}
}

// Port of test_null_team_id_becomes_none. The Go query uses
// ifNull(team_id, ”) -- an empty string on the wire is the NULL case,
// same convention rowToWorkItemTeamAttribution documents.
func TestResolveWorkItemTeamAttributions_NullTeamIdBecomesNone(t *testing.T) {
	client := &witaFakeClient{scanner: &witaFakeScanner{rows: []witaFakeRow{
		witaRow(map[string]any{"teamID": "", "teamName": "", "source": "unassigned", "confidence": "none"}),
	}}}
	got, err := ResolveWorkItemTeamAttributions(context.Background(), client, "test-org", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got[0].TeamID != nil {
		t.Errorf("TeamID = %v, want nil", *got[0].TeamID)
	}
	if got[0].TeamName != nil {
		t.Errorf("TeamName = %v, want nil", *got[0].TeamName)
	}
}

// Port of test_query_is_org_scoped_final_and_bounded.
func TestResolveWorkItemTeamAttributions_QueryIsOrgScopedFinalAndBounded(t *testing.T) {
	client := &witaFakeClient{scanner: &witaFakeScanner{rows: nil}}
	teamX := "team-x"
	if _, err := ResolveWorkItemTeamAttributions(context.Background(), client, "test-org", []string{"a", "b"}, &teamX); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sql := client.lastQuery
	for _, want := range []string{
		"work_item_team_attributions FINAL",
		"org_id = {org_id:String}",
		"work_item_id IN {work_item_ids:Array(String)}",
		"team_id = {team_id:String}",
		"LIMIT {limit:UInt64}",
		"max(computed_at)",
		"GROUP BY work_item_id",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("query missing %q; got:\n%s", want, sql)
		}
	}
	// Latest-compute snapshot guard: the team filter must NOT scope the
	// snapshot subquery (CHAOS-2605's own finding, ported).
	idx := strings.Index(sql, "max(computed_at)")
	if idx < 0 {
		t.Fatalf("max(computed_at) not found in query")
	}
	snapshot := sql[idx:]
	if strings.Contains(snapshot, "team_id = {team_id:String}") {
		t.Errorf("snapshot subquery must not filter by team_id; got:\n%s", snapshot)
	}
	foundOrgID := false
	for _, b := range client.lastParams {
		if b.Name == "org_id" && b.Value == "test-org" {
			foundOrgID = true
		}
	}
	if !foundOrgID {
		t.Errorf("bindings = %+v, want org_id=test-org", client.lastParams)
	}
}

// Port of test_resolve_recommendations_empty_on_no_rows' shape: an empty
// result must be an empty slice, never nil (schema.py's list return
// contract).
func TestResolveWorkItemTeamAttributions_EmptyOnNoRows(t *testing.T) {
	client := &witaFakeClient{scanner: &witaFakeScanner{rows: nil}}
	got, err := ResolveWorkItemTeamAttributions(context.Background(), client, "test-org", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Error("got = nil, want an empty non-nil slice")
	}
	if len(got) != 0 {
		t.Errorf("len(got) = %d, want 0", len(got))
	}
}
