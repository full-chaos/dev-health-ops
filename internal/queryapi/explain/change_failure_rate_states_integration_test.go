//go:build integration

package explain

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
)

// /explain serves change failure rate with the state that says why it has a
// value or not. Unknown, not applicable and a window with no stored counts
// all have no value: only rate_state tells them apart, and a measured 0 is
// not one of them. Rows come from the real writer.
func TestExplainChangeFailureRateStates_LiveEngine(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)

	const orgID = "org-explain-change-failure-states"
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)

	cases := []struct {
		name   string
		repo   uuid.UUID
		counts *changefailure.Counts // nil: no stored row
		value  float64
		data   bool
		state  string
		tier   string
	}{
		{"unknown: deployments, no incident evidence", uuid.New(), &changefailure.Counts{Deployments: 4}, 0, false, `"unknown_no_incident_evidence"`, `null`},
		{"not applicable: an incident, no deployment", uuid.New(), &changefailure.Counts{IncidentsDirect: 1}, 0, false, `"not_applicable_no_deployments"`, `null`},
		{"not applicable: a retraction row of zeros", uuid.New(), &changefailure.Counts{}, 0, false, `"not_applicable_no_deployments"`, `null`},
		{"no stored row", uuid.New(), nil, 0, false, `null`, `null`},
		{"measured zero", uuid.New(), &changefailure.Counts{Deployments: 4, IncidentsDirect: 1}, 0, true, `"measured"`, `null`},
		{"measured, heuristic link", uuid.New(), &changefailure.Counts{Deployments: 4, FailedHeuristic: 2, IncidentsViaDeployment: 1}, 50, true, `"measured"`, `"heuristic"`},
		{"measured, native link", uuid.New(), &changefailure.Counts{Deployments: 4, FailedNative: 1, IncidentsDirect: 1}, 25, true, `"measured"`, `"native"`},
	}

	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	var rows []repouser.ChangeFailureDaily
	for i, tc := range cases {
		if tc.counts != nil {
			rows = append(rows, repouser.ChangeFailureDaily{RepoID: tc.repo, Day: day, ComputedAt: computedAt, Counts: *tc.counts})
		}
		// A repository scope is applied only for a repository the organization
		// has, so every case has its repos row.
		if err := admin.Exec(ctx, fmt.Sprintf(
			"INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES ('%s', 'acme/states-%d', 'github', '%s', now64(3), now64(3))",
			tc.repo, i, orgID)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := writer.WriteChangeFailure(ctx, rows, orgID); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := BuildExplainResponse(ctx, reader, orgID, Params{
				Metric: "change_failure_rate", StartDay: day, EndDay: day.AddDate(0, 0, 7),
				CompareStart: day.AddDate(0, 0, -7), CompareEnd: day,
				ScopeLevel: "repo", ScopeIDs: []string{tc.repo.String()},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got.HasData != tc.data || !near(got.Value, tc.value) || got.HasPriorData {
				t.Errorf("value %v, has_data %v, has_prior_data %v; want %v, %v, false", got.Value, got.HasData, got.HasPriorData, tc.value, tc.data)
			}
			// The served body, as a client reads it.
			body, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			if want := `"link_tier":` + tc.tier + `,"rate_state":` + tc.state + `}`; !strings.HasSuffix(string(body), want) {
				t.Errorf("body ends %s, want %s", tail(string(body), 90), want)
			}
		})
	}

	// The organization: 16 deployments and incident evidence in the window, 3
	// failed. The states of single repositories do not leak into the sum, and
	// the weakest contributing tier is named.
	org, err := BuildExplainResponse(ctx, reader, orgID, Params{
		Metric: "change_failure_rate", StartDay: day, EndDay: day.AddDate(0, 0, 7),
		CompareStart: day.AddDate(0, 0, -7), CompareEnd: day,
	})
	if err != nil {
		t.Fatal(err)
	}
	if org.RateState == nil || *org.RateState != string(changefailure.StateMeasured) || !near(org.Value, 100*3.0/16.0) ||
		org.LinkTier == nil || *org.LinkTier != changefailure.TierHeuristic {
		t.Errorf("organization: value %v, rate_state %v, link_tier %v; want 18.75, measured, heuristic", org.Value, org.RateState, org.LinkTier)
	}

	// The comparison window has its own presence flag, and the state is the
	// current window's. The "native link" repository is measured at 25% in the
	// first week and at 50% in the second; the "unknown" repository has no
	// stored counts in the second week.
	native, unknown := cases[6].repo, cases[0].repo
	if _, err := writer.WriteChangeFailure(ctx, []repouser.ChangeFailureDaily{
		{RepoID: native, Day: day.AddDate(0, 0, 7), ComputedAt: computedAt, Counts: changefailure.Counts{Deployments: 4, FailedNative: 2, IncidentsDirect: 1}},
	}, orgID); err != nil {
		t.Fatal(err)
	}
	secondWeek := func(repo uuid.UUID) *Response {
		t.Helper()
		got, err := BuildExplainResponse(ctx, reader, orgID, Params{
			Metric: "change_failure_rate", StartDay: day.AddDate(0, 0, 7), EndDay: day.AddDate(0, 0, 14),
			CompareStart: day, CompareEnd: day.AddDate(0, 0, 7),
			ScopeLevel: "repo", ScopeIDs: []string{repo.String()},
		})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := secondWeek(native); !got.HasData || !got.HasPriorData || !near(got.Value, 50) || !near(got.DeltaPct, 100) ||
		got.RateState == nil || *got.RateState != string(changefailure.StateMeasured) {
		t.Errorf("second week, measured after measured: value %v delta_pct %v has_data %v has_prior_data %v rate_state %v; want 50, 100, true, true, measured",
			got.Value, got.DeltaPct, got.HasData, got.HasPriorData, got.RateState)
	}
	// An unknown prior week is not prior data, and the empty current week has
	// no state: the prior week's "unknown" is not carried into it.
	if got := secondWeek(unknown); got.HasData || got.HasPriorData || got.RateState != nil || got.DeltaPct != 0 {
		t.Errorf("second week of the unknown repository: has_data %v has_prior_data %v rate_state %v delta_pct %v; want false, false, null, 0",
			got.HasData, got.HasPriorData, got.RateState, got.DeltaPct)
	}

	// Another metric carries no state.
	revert, err := BuildExplainResponse(ctx, reader, orgID, Params{
		Metric: "revert_rate", StartDay: day, EndDay: day.AddDate(0, 0, 7),
		CompareStart: day.AddDate(0, 0, -7), CompareEnd: day,
	})
	if err != nil {
		t.Fatal(err)
	}
	if revert.RateState != nil || revert.LinkTier != nil {
		t.Errorf("revert_rate carries rate_state %v, link_tier %v; want none", revert.RateState, revert.LinkTier)
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
