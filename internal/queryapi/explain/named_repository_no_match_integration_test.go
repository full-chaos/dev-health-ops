//go:build integration

package explain

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// CHAOS-9093: explain names the repositories the request carries. A named
// repository that resolves to nothing leaves nothing: no value, no driver, no
// contributor; never the organization's unfiltered answer. (A request that names no
// repository still reads the whole organization.)
func TestExplainNamedRepositoryThatResolvesToNothingLeavesNothing(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	const org = "explain-named-no-match"
	current := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	prior := current.AddDate(0, 0, -1)
	computedAt := current.AddDate(0, 0, 3)
	for repo, rows := range map[uuid.UUID]map[time.Time]uint32{
		uuid.New(): {current: 5, prior: 10},
		uuid.New(): {current: 100, prior: 80},
	} {
		for day, churn := range rows {
			if err := admin.Exec(ctx,
				`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
				repo, day, churn, computedAt, org); err != nil {
				t.Fatal(err)
			}
		}
	}
	base := Params{Metric: "churn", StartDay: current, EndDay: current.AddDate(0, 0, 1), CompareStart: prior, CompareEnd: current}
	whole, err := BuildExplainResponse(ctx, reader, org, base)
	if err != nil {
		t.Fatal(err)
	}
	if !whole.HasData || whole.Value != 105 {
		t.Fatalf("no repository named: value %v has_data %v, want the organization's 105", whole.Value, whole.HasData)
	}
	for name, params := range map[string]Params{
		"repo scope, unknown id":   {ScopeLevel: "repo", ScopeIDs: []string{uuid.New().String()}},
		"what.repos, unknown id":   {WhatRepos: []string{uuid.New().String()}},
		"what.repos, unknown name": {WhatRepos: []string{"acme/nothing"}},
	} {
		params.Metric, params.StartDay, params.EndDay, params.CompareStart, params.CompareEnd = base.Metric, base.StartDay, base.EndDay, base.CompareStart, base.CompareEnd
		got, err := BuildExplainResponse(ctx, reader, org, params)
		if err != nil {
			t.Fatal(err)
		}
		if got.HasData || got.Value != 0 || len(got.Drivers) != 0 || len(got.Contributors) != 0 {
			t.Errorf("%s: value %v has_data %v drivers %d contributors %d, want nothing (never the unfiltered 105)", name, got.Value, got.HasData, len(got.Drivers), len(got.Contributors))
		}
	}
}
