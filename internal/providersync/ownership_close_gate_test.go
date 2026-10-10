package providersync

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// staticScopeCensus answers CountActiveSiblingIntegrations with a fixed count
// or a fixed error, and counts its calls.
type staticScopeCensus struct {
	siblings int
	err      error
	calls    *int
}

func (census staticScopeCensus) CountActiveSiblingIntegrations(context.Context, string, string, string) (int, error) {
	if census.calls != nil {
		*census.calls++
	}
	return census.siblings, census.err
}

// activeIntegrationCensus is a scope census over a fixed set of the
// organization's integrations of one provider: id -> is active. It counts the
// ACTIVE ones other than the run's own, as the worker's census reads
// public.integrations.is_active (workerservice.teamCatalogScopeCensus, pinned
// against a real Postgres by
// TestTeamCatalogScopeCensusCountsTheOrgsOtherActiveIntegrationsOfOneProvider).
type activeIntegrationCensus map[string]bool

func (census activeIntegrationCensus) CountActiveSiblingIntegrations(_ context.Context, _, _, integrationID string) (int, error) {
	siblings := 0
	for id, active := range census {
		if active && id != integrationID {
			siblings++
		}
	}
	return siblings, nil
}

func closeLegReasons(legs []DegradedLeg) []string {
	var reasons []string
	for _, leg := range legs {
		if leg.Leg != ownershipCloseLeg || leg.Outcome != "skipped" {
			continue
		}
		reasons = append(reasons, leg.Reason)
	}
	return reasons
}

func TestOwnershipListingProvesEndNeedsTheEndSignalAndNoBound(t *testing.T) {
	for _, test := range []struct {
		name  string
		pages providerfoundation.PageCollection
		want  bool
	}{
		{"the provider's end signal", providerfoundation.PageCollection{Pages: 1, EndProven: true}, true},
		{"a stop without the end signal", providerfoundation.PageCollection{Pages: 1}, false},
		{"the page budget ran out", providerfoundation.PageCollection{Pages: 1, EndProven: true, PageBudgetExhausted: true}, false},
		{"the item cap was reached", providerfoundation.PageCollection{Pages: 1, EndProven: true, ItemCapReached: true}, false},
	} {
		if got := ownershipListingProvesEnd(test.pages); got != test.want {
			t.Errorf("%s: ownershipListingProvesEnd = %v, want %v", test.name, got, test.want)
		}
	}
}

func TestDecideOwnershipCloseClosesOnlyProvenListingsOfAnUnsharedScope(t *testing.T) {
	ctx := context.Background()
	ref := TeamCatalogReference{OrgID: "org", IntegrationID: "integration-a"}
	for _, test := range []struct {
		name         string
		census       OwnershipScopeCensus
		ref          TeamCatalogReference
		listed       []string
		unproven     []string
		wantClosable []string
		wantReasons  []string
		wantCalls    int
	}{
		{name: "no sibling, every listing proven: every listed team closes",
			census: staticScopeCensus{}, ref: ref, listed: []string{"gl:a", "gl:b"},
			wantClosable: []string{"gl:a", "gl:b"}, wantCalls: 1},
		{name: "an unproven listing keeps that team open, the rest close",
			census: staticScopeCensus{}, ref: ref, listed: []string{"gl:a", "gl:b"}, unproven: []string{"gl:b"},
			wantClosable: []string{"gl:a"}, wantReasons: []string{OwnershipCloseSkippedListingIncomplete}, wantCalls: 1},
		{name: "every listing unproven: nothing closes and the census is not read",
			census: staticScopeCensus{}, ref: ref, listed: []string{"gl:a"}, unproven: []string{"gl:a"},
			wantReasons: []string{OwnershipCloseSkippedListingIncomplete}},
		{name: "one other active integration of the provider: nothing closes",
			census: staticScopeCensus{siblings: 1},
			ref:    ref, listed: []string{"gl:a"}, wantReasons: []string{OwnershipCloseSkippedScopeShared}, wantCalls: 1},
		{name: "several other active integrations of the provider: nothing closes",
			census: staticScopeCensus{siblings: 3},
			ref:    ref, listed: []string{"gl:a", "gl:b"}, wantReasons: []string{OwnershipCloseSkippedScopeShared}, wantCalls: 1},
		{name: "a failed census read: nothing closes",
			census: staticScopeCensus{err: errors.New("census read failed")},
			ref:    ref, listed: []string{"gl:a"}, wantReasons: []string{OwnershipCloseSkippedCensusFailed}, wantCalls: 1},
		{name: "no census: nothing closes",
			census: nil, ref: ref, listed: []string{"gl:a"}, wantReasons: []string{OwnershipCloseSkippedCensusUnavailable}},
		{name: "a run without an integration: nothing closes",
			census: staticScopeCensus{}, ref: TeamCatalogReference{OrgID: "org"}, listed: []string{"gl:a"},
			wantReasons: []string{OwnershipCloseSkippedCensusUnavailable}},
		{name: "an unproven listing and a shared scope: both are said",
			census: staticScopeCensus{siblings: 1},
			ref:    ref, listed: []string{"gl:a", "gl:b"}, unproven: []string{"gl:a"},
			wantReasons: []string{OwnershipCloseSkippedListingIncomplete, OwnershipCloseSkippedScopeShared}, wantCalls: 1},
		{name: "nothing listed: nothing to decide", census: staticScopeCensus{}, ref: ref},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			census := test.census
			if static, ok := census.(staticScopeCensus); ok {
				static.calls = &calls
				census = static
			}
			decision := decideOwnershipClose(ctx, census, ownershipCloseRequest{
				ref: test.ref, provider: "gitlab",
				listed: test.listed, unproven: test.unproven, responses: testOneResponseEach(test.listed),
			})
			if !reflect.DeepEqual(decision.read, test.listed) {
				t.Errorf("read = %v, want every listed team %v", decision.read, test.listed)
			}
			if !reflect.DeepEqual(decision.closable, test.wantClosable) {
				t.Errorf("closable = %v, want %v", decision.closable, test.wantClosable)
			}
			if got := closeLegReasons(decision.legs); !reflect.DeepEqual(got, test.wantReasons) {
				t.Errorf("skip reasons = %v, want %v", got, test.wantReasons)
			}
			if calls != test.wantCalls {
				t.Errorf("census calls = %d, want %d", calls, test.wantCalls)
			}
		})
	}
}

// The gate's snapshot is the grant kind of the closable teams: a team outside
// it is of no kind, so its open rows never close, and a gate that lets no
// team close carries the reason of every skip.
func TestOwnershipCloseDecisionSnapshotClosesOnlyTheClosableTeams(t *testing.T) {
	at := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	before := at.Add(-time.Hour)
	open := []OwnershipSnapshotRow{
		{TeamID: "gl:a", ProjectID: testPID("a/1"), Source: "provider_access", ValidFrom: before},
		{TeamID: "gl:b", ProjectID: testPID("b/1"), Source: "provider_access", ValidFrom: before},
		{TeamID: "gl:a", ProjectID: testPID("a/2"), Source: "manual", ValidFrom: before},
	}
	ref := TeamCatalogReference{OrgID: "org-1", IntegrationID: "integration-a"}
	decide := func(census OwnershipScopeCensus, listed, unproven []string) SnapshotPlan {
		decision := decideOwnershipClose(context.Background(), census, ownershipCloseRequest{
			ref: ref, provider: "gitlab", listed: listed, unproven: unproven, responses: testOneResponseEach(listed),
		})
		return PlanOwnershipSnapshot(nil, open, at, decision.snapshot(GitLabGroupProjectGrantKind, gitlabGroupProjectsWalk, nil))
	}
	closedTeams := func(plan SnapshotPlan) []string {
		out := []string{}
		for _, retraction := range plan.Retract {
			out = append(out, open[retraction.Open].TeamID+"/"+open[retraction.Open].Source)
		}
		return out
	}
	for _, c := range []struct {
		name             string
		census           OwnershipScopeCensus
		listed, unproven []string
		wantClosed       []string
		wantReasons      []string
	}{
		{"both listings proven: an empty answer of a listed team closes its grants",
			staticScopeCensus{}, []string{"gl:a", "gl:b"}, nil, []string{"gl:a/provider_access", "gl:b/provider_access"}, nil},
		{"one listing not proven: only the proven team closes",
			staticScopeCensus{}, []string{"gl:a", "gl:b"}, []string{"gl:b"}, []string{"gl:a/provider_access"}, nil},
		// The scope gate is asked only when a listing proved its end: before
		// that nothing can close, and the scope stays not proven.
		{"no listing proven", staticScopeCensus{}, []string{"gl:a", "gl:b"}, []string{"gl:a", "gl:b"}, []string{},
			[]string{ScopeNotProven, OwnershipCloseSkippedListingIncomplete}},
		{"no census", nil, []string{"gl:a", "gl:b"}, nil, []string{}, []string{OwnershipCloseSkippedCensusUnavailable}},
		{"the census read failed", staticScopeCensus{err: errors.New("down")}, []string{"gl:a"}, nil, []string{},
			[]string{OwnershipCloseSkippedCensusFailed}},
		{"another integration shares the scope", staticScopeCensus{siblings: 1}, []string{"gl:a"}, nil, []string{},
			[]string{OwnershipCloseSkippedScopeShared}},
		{"no team listed", staticScopeCensus{}, nil, nil, []string{}, []string{ScopeNotProven, OwnershipCloseSkippedNoTeamListed}},
		{"an inactive second integration does not block the close: the census counts active ones only",
			activeIntegrationCensus{"integration-a": true, "integration-b": false}, []string{"gl:a", "gl:b"}, nil,
			[]string{"gl:a/provider_access", "gl:b/provider_access"}, nil},
		{"an active second integration blocks the close",
			activeIntegrationCensus{"integration-a": true, "integration-b": true}, []string{"gl:a", "gl:b"}, nil, []string{},
			[]string{OwnershipCloseSkippedScopeShared}},
	} {
		t.Run(c.name, func(t *testing.T) {
			plan := decide(c.census, c.listed, c.unproven)
			if got := closedTeams(plan); !reflect.DeepEqual(got, c.wantClosed) {
				t.Errorf("closed = %v, want %v", got, c.wantClosed)
			}
			var reasons []string
			for _, outcome := range plan.Kinds {
				reasons = append(reasons, outcome.Abandoned...)
			}
			if !reflect.DeepEqual(reasons, c.wantReasons) {
				t.Errorf("abandon reasons = %v, want %v", reasons, c.wantReasons)
			}
		})
	}
}

// ProveSoleScope is the one scope gate: only a census that answers zero other
// active integrations proves the scope. No census, no integration id, a
// failed read and one sibling each give their own reason; a failed read is
// never zero.
func TestProveSoleScopeNamesWhyAScopeIsNotProven(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name          string
		census        OwnershipScopeCensus
		integrationID string
		want          []string
	}{
		{"the only active integration", staticScopeCensus{}, "integration-a", nil},
		{"an inactive second integration", activeIntegrationCensus{"integration-a": true, "integration-b": false}, "integration-a", nil},
		{"an active second integration", activeIntegrationCensus{"integration-a": true, "integration-b": true}, "integration-a",
			[]string{OwnershipCloseSkippedScopeShared}},
		{"no census", nil, "integration-a", []string{OwnershipCloseSkippedCensusUnavailable}},
		{"no integration id", staticScopeCensus{}, " ", []string{OwnershipCloseSkippedCensusUnavailable}},
		{"a failed census read", staticScopeCensus{err: errors.New("down")}, "integration-a", []string{OwnershipCloseSkippedCensusFailed}},
	} {
		scope := ProveSoleScope(ctx, c.census, "org-1", "linear", c.integrationID)
		if scope.Proven() != (len(c.want) == 0) || !reflect.DeepEqual(scope.Missing(), append([]string(nil), c.want...)) && len(c.want) != 0 {
			t.Errorf("%s: proven=%v missing=%v, want missing %v", c.name, scope.Proven(), scope.Missing(), c.want)
		}
	}
	if zero := (ScopeProof{}); zero.Proven() || !reflect.DeepEqual(zero.Missing(), []string{ScopeNotProven}) {
		t.Errorf("the zero scope proof: proven=%v missing=%v, want not proven", zero.Proven(), zero.Missing())
	}
}
