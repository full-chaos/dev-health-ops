//go:build integration

package operatingreview

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// The review's three failure ratios, read from rows the real writers store
// (CHAOS-8981):
//
//	change_failure_rate      the week's summed change-failure counts; the
//	                         prior week has deployments and no incident
//	                         evidence, so it has no data (never 0%)
//	deployment_failure_rate  failed deployment runs / deployments
//	revert_rate              total reverted / total merged pull requests over
//	                         the days with a stored revert rate; the newest
//	                         version of a day wins; a day with merges and no
//	                         stored rate is unknown and is never read from the
//	                         deprecated change_failure_rate column
func TestResolveReadsChangeFailureDeploymentFailureAndRevertRates(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)

	const org = "org-review-change-failure"
	week := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	prior := week.AddDate(0, 0, -7)
	repoA, repoB := uuid.New(), uuid.New()
	older := week.AddDate(0, 0, 8)
	newer := older.Add(time.Hour)

	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	rate := func(v float64) *float64 { return &v }
	// Each version of a key goes in its own insert: one insert block of a
	// ReplacingMergeTree already keeps one row per key, and the readers must
	// be right while two versions are still stored.
	writeCounts := func(rows ...repouser.ChangeFailureDaily) {
		t.Helper()
		if _, err := writer.WriteChangeFailure(ctx, rows, org); err != nil {
			t.Fatal(err)
		}
	}
	writeMetrics := func(rows ...repouser.RepoMetric) {
		t.Helper()
		if _, _, _, err := writer.WriteResult(ctx, repouser.Result{RepoMetrics: rows}, org); err != nil {
			t.Fatal(err)
		}
	}
	// An older version of repo A's Monday says every deployment failed.
	writeCounts(repouser.ChangeFailureDaily{RepoID: repoA, Day: week, ComputedAt: older, Counts: changefailure.Counts{Deployments: 2, FailedNative: 2, IncidentsDirect: 1}})
	writeCounts(
		repouser.ChangeFailureDaily{RepoID: repoA, Day: week, ComputedAt: newer, Counts: changefailure.Counts{Deployments: 2, FailedHeuristic: 1, IncidentsDirect: 1}},
		repouser.ChangeFailureDaily{RepoID: repoB, Day: week.AddDate(0, 0, 2), ComputedAt: newer, Counts: changefailure.Counts{Deployments: 6}},
		// The prior week deploys with no incident evidence: unknown.
		repouser.ChangeFailureDaily{RepoID: repoA, Day: prior, ComputedAt: newer, Counts: changefailure.Counts{Deployments: 5}},
	)
	// Prior week: an older version of a day holds a revert rate (0.5), and the
	// newest version of the same day is what the writer stores today: 4
	// merged, no revert rate, 0.25 in the deprecated column. The newest version
	// wins, so the prior week has no revert rate: not the older 0.5, and not
	// the 0.25 of the deprecated column.
	writeMetrics(repouser.RepoMetric{RepoID: repoA, Day: prior, PRsMerged: 2, ChangeFailureRate: 0.5, RevertRate: rate(0.5), ComputedAt: older})
	writeMetrics(
		repouser.RepoMetric{RepoID: repoA, Day: week, PRsMerged: 4, ChangeFailureRate: 0.25, RevertRate: rate(0.25), ComputedAt: newer},
		repouser.RepoMetric{RepoID: repoB, Day: week, PRsMerged: 6, RevertRate: rate(0), ComputedAt: newer},
		repouser.RepoMetric{RepoID: repoA, Day: prior, PRsMerged: 4, ChangeFailureRate: 0.25, RevertRate: nil, ComputedAt: newer},
		// A day of the current week with 90 merged pull requests and no stored
		// revert rate: unknown. It adds nothing: (1 + 0) / (4 + 6) = 0.1, not
		// 0.19 (the deprecated 0.2 read as a rate) and not 0.01 (its merges
		// counted with no revert).
		repouser.RepoMetric{RepoID: repoB, Day: week.AddDate(0, 0, 1), PRsMerged: 90, ChangeFailureRate: 0.2, RevertRate: nil, ComputedAt: newer},
		// A day that merged nothing adds nothing.
		repouser.RepoMetric{RepoID: repoA, Day: week.AddDate(0, 0, 3), PRsMerged: 0, CommitsCount: 1, ComputedAt: newer},
	)
	var storedVersions uint64
	if err := admin.QueryRow(ctx, "SELECT count() FROM repo_change_failure_daily WHERE org_id = ? AND repo_id = ? AND day = ?", org, repoA, week).Scan(&storedVersions); err != nil {
		t.Fatal(err)
	}
	if storedVersions != 2 {
		t.Fatalf("repo A's Monday holds %d stored version(s), want 2: the newest-version read is not measured", storedVersions)
	}
	if err := admin.Exec(ctx, `INSERT INTO deploy_metrics_daily
		(repo_id, day, deployments_count, failed_deployments_count, org_id, computed_at)
		VALUES (?, ?, 8, 2, ?, ?)`, repoA, week, org, newer); err != nil {
		t.Fatal(err)
	}

	review, err := Resolve(ctx, client, org, nil, graphqldate.New(week))
	if err != nil {
		t.Fatal(err)
	}
	type want struct {
		value, prior      float64
		hasData, hasPrior bool
	}
	wants := map[string]want{
		"change_failure_rate":     {1.0 / 8.0, 0, true, false},
		"deployment_failure_rate": {0.25, 0, true, false},
		"revert_rate":             {0.1, 0, true, false},
	}
	seen := 0
	for _, section := range review.Sections {
		for _, m := range section.Metrics {
			w, ok := wants[m.Key]
			if !ok {
				continue
			}
			seen++
			if !nearly(m.Value, w.value) || m.HasData != w.hasData || m.Delta.HasPriorData != w.hasPrior || !nearly(m.Delta.PriorValue, w.prior) {
				t.Errorf("%s: value %v hasData %v prior %v hasPriorData %v, want %v %v %v %v",
					m.Key, m.Value, m.HasData, m.Delta.PriorValue, m.Delta.HasPriorData, w.value, w.hasData, w.prior, w.hasPrior)
			}
			// Only change failure rate carries a state, and it is the current
			// week's: measured here. The prior week is unknown, which the
			// metric's hasPriorData false already says.
			wantState := "null"
			if m.Key == "change_failure_rate" {
				wantState = "measured"
			}
			if got := servedState(m.RateState); got != wantState {
				t.Errorf("%s: rateState %s, want %s", m.Key, got, wantState)
			}
			if !w.hasPrior && m.Delta.Status != "" {
				t.Errorf("%s: status %q, want no claim against a prior week with no data", m.Key, m.Delta.Status)
			}
		}
	}
	if seen != len(wants) {
		t.Fatalf("checked %d metrics, want %d", seen, len(wants))
	}
}

// What every organization reads today: pull requests merge, and no writer
// measures a revert rate. The review shows revert rate as no data, in the
// current and in the prior week, never 0%.
func TestResolveShowsNoRevertRateWhenNoneIsMeasured(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)

	const org = "org-review-no-revert-rate"
	week := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	repo := uuid.New()
	if _, _, _, err := writer.WriteResult(ctx, repouser.Result{RepoMetrics: []repouser.RepoMetric{
		{RepoID: repo, Day: week, PRsMerged: 12, CommitsCount: 3, ComputedAt: week.AddDate(0, 0, 8)},
		{RepoID: repo, Day: week.AddDate(0, 0, -7), PRsMerged: 7, CommitsCount: 3, ComputedAt: week.AddDate(0, 0, 8)},
	}}, org); err != nil {
		t.Fatal(err)
	}
	// A day recomputed without a revert rate retracts the one stored before:
	// the newest version wins, also when it holds NULL (a bare argMax skips the
	// NULL and would return the older 50%).
	retracted := uuid.New()
	earlier := 0.5
	for _, row := range []repouser.RepoMetric{
		{RepoID: retracted, Day: week, PRsMerged: 4, CommitsCount: 1, RevertRate: &earlier, ComputedAt: week.AddDate(0, 0, 7)},
		{RepoID: retracted, Day: week, PRsMerged: 4, CommitsCount: 1, ComputedAt: week.AddDate(0, 0, 8)},
	} {
		if _, _, _, err := writer.WriteResult(ctx, repouser.Result{RepoMetrics: []repouser.RepoMetric{row}}, org); err != nil {
			t.Fatal(err)
		}
	}
	review, err := Resolve(ctx, client, org, nil, graphqldate.New(week))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, section := range review.Sections {
		for _, m := range section.Metrics {
			if m.Key != "revert_rate" {
				continue
			}
			found = true
			if m.HasData || m.Delta.HasPriorData || m.Value != 0 || m.Delta.Status != "" {
				t.Errorf("revert_rate with 12 and 7 merged pull requests and no measured rate: value %v hasData %v hasPriorData %v status %q; want no data and no claim",
					m.Value, m.HasData, m.Delta.HasPriorData, m.Delta.Status)
			}
		}
	}
	if !found {
		t.Fatal("the review has no revert_rate metric")
	}
}

func servedState(state *string) string {
	if state == nil {
		return "null"
	}
	return *state
}

// The review serves change failure rate with the state that says why it has
// a value or not. Unknown, not applicable and a week with no stored counts
// all have no value: only rateState tells them apart, and a measured 0 is not
// one of them. The review is organization-wide, so each state is its own
// organization; rows come from the real writer.
func TestResolveServesTheStateOfChangeFailureRate(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)

	week := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	computedAt := week.AddDate(0, 0, 8)
	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		org     string
		counts  []changefailure.Counts // one repository each; none: no stored row
		value   float64
		hasData bool
		state   string
	}{
		{"org-review-state-unknown", []changefailure.Counts{{Deployments: 4}}, 0, false, "unknown_no_incident_evidence"},
		{"org-review-state-not-applicable", []changefailure.Counts{{IncidentsDirect: 1}}, 0, false, "not_applicable_no_deployments"},
		{"org-review-state-retracted", []changefailure.Counts{{}}, 0, false, "not_applicable_no_deployments"},
		{"org-review-state-no-row", nil, 0, false, "null"},
		{"org-review-state-measured-zero", []changefailure.Counts{{Deployments: 4, IncidentsDirect: 1}}, 0, true, "measured"},
		{"org-review-state-measured", []changefailure.Counts{{Deployments: 4, FailedHeuristic: 1, IncidentsViaDeployment: 1}}, 0.25, true, "measured"},
		// One repository's deployments and another's incident are one
		// organization view: a measured 0, not unknown plus not applicable.
		{"org-review-state-two-repositories", []changefailure.Counts{{Deployments: 4}, {IncidentsDirect: 1}}, 0, true, "measured"},
	} {
		t.Run(tc.org, func(t *testing.T) {
			var rows []repouser.ChangeFailureDaily
			for _, counts := range tc.counts {
				rows = append(rows, repouser.ChangeFailureDaily{RepoID: uuid.New(), Day: week.AddDate(0, 0, 1), ComputedAt: computedAt, Counts: counts})
			}
			if len(rows) > 0 {
				if _, err := writer.WriteChangeFailure(ctx, rows, tc.org); err != nil {
					t.Fatal(err)
				}
			}
			review, err := Resolve(ctx, client, tc.org, nil, graphqldate.New(week))
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, section := range review.Sections {
				for _, m := range section.Metrics {
					if m.Key != "change_failure_rate" {
						if m.RateState != nil {
							t.Errorf("%s carries rateState %s", m.Key, *m.RateState)
						}
						continue
					}
					found = true
					if got := servedState(m.RateState); got != tc.state || m.HasData != tc.hasData || !nearly(m.Value, tc.value) || m.Delta.HasPriorData {
						t.Errorf("change_failure_rate: value %v hasData %v rateState %s hasPriorData %v; want %v %v %s false",
							m.Value, m.HasData, got, m.Delta.HasPriorData, tc.value, tc.hasData, tc.state)
					}
				}
			}
			if !found {
				t.Fatal("the review has no change_failure_rate metric")
			}
		})
	}
}

func nearly(got, want float64) bool {
	diff := got - want
	return diff < 1e-12 && diff > -1e-12
}

// A delta states a move between two measured values (deltarule). When the
// current or the prior week has no value, the delta numbers are 0 and the flags
// say why: the same contract Home and /explain serve. A measured prior stays
// as priorValue, a fact; two measured weeks keep their move.
func TestResolveServesNoDeltaNumbersWhenAWeekHasNoValue(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)

	week := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	computedAt := week.AddDate(0, 0, 8)
	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	measured := changefailure.Counts{Deployments: 10, FailedHeuristic: 5, IncidentsDirect: 1}
	quarter := changefailure.Counts{Deployments: 4, FailedHeuristic: 1, IncidentsDirect: 1}
	for _, tc := range []struct {
		org                string
		prior, current     *changefailure.Counts
		wantMove           bool
		wantAbsolute       float64
		wantPercent        float64
		wantPriorValue     float64
		wantHasPriorData   bool
		wantCurrentHasData bool
	}{
		{"org-delta-current-unknown", &measured, &changefailure.Counts{Deployments: 4}, false, 0, 0, 0.5, true, false},
		{"org-delta-current-not-applicable", &measured, &changefailure.Counts{IncidentsDirect: 1}, false, 0, 0, 0.5, true, false},
		{"org-delta-current-no-counts", &measured, nil, false, 0, 0, 0.5, true, false},
		{"org-delta-prior-unknown", &changefailure.Counts{Deployments: 4}, &quarter, false, 0, 0, 0, false, true},
		{"org-delta-prior-not-applicable", &changefailure.Counts{IncidentsDirect: 1}, &quarter, false, 0, 0, 0, false, true},
		{"org-delta-prior-no-counts", nil, &quarter, false, 0, 0, 0, false, true},
		// Control: two measured weeks keep their move.
		{"org-delta-both-measured", &measured, &quarter, true, -0.25, -50, 0.5, true, true},
	} {
		t.Run(tc.org, func(t *testing.T) {
			var rows []repouser.ChangeFailureDaily
			if tc.prior != nil {
				rows = append(rows, repouser.ChangeFailureDaily{RepoID: uuid.New(), Day: week.AddDate(0, 0, -6), ComputedAt: computedAt, Counts: *tc.prior})
			}
			if tc.current != nil {
				rows = append(rows, repouser.ChangeFailureDaily{RepoID: uuid.New(), Day: week.AddDate(0, 0, 1), ComputedAt: computedAt, Counts: *tc.current})
			}
			if _, err := writer.WriteChangeFailure(ctx, rows, tc.org); err != nil {
				t.Fatal(err)
			}
			review, err := Resolve(ctx, client, tc.org, nil, graphqldate.New(week))
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, section := range review.Sections {
				for _, m := range section.Metrics {
					if m.Key != "change_failure_rate" {
						continue
					}
					found = true
					d := m.Delta
					if m.HasData != tc.wantCurrentHasData || d.HasPriorData != tc.wantHasPriorData {
						t.Errorf("hasData %v hasPriorData %v, want %v %v", m.HasData, d.HasPriorData, tc.wantCurrentHasData, tc.wantHasPriorData)
					}
					// A week with no stored value has no percent (null, CHAOS-9111); two measured weeks keep theirs.
					percentOK := (!tc.wantMove && d.Percent == nil) || (tc.wantMove && d.Percent != nil && nearly(*d.Percent, tc.wantPercent))
					if !nearly(d.Absolute, tc.wantAbsolute) || !percentOK || !nearly(d.PriorValue, tc.wantPriorValue) {
						t.Errorf("delta absolute %v percent %v priorValue %v, want %v %v %v", d.Absolute, d.Percent, d.PriorValue, tc.wantAbsolute, tc.wantPercent, tc.wantPriorValue)
					}
					if !tc.wantMove && d.Status != "" {
						t.Errorf("delta status %q for a week without a value, want none", d.Status)
					}
					if tc.wantMove && d.Status != "improved" {
						t.Errorf("delta status %q for two measured weeks, want improved", d.Status)
					}
				}
			}
			if !found {
				t.Fatal("the review has no change_failure_rate metric")
			}
		})
	}
}
