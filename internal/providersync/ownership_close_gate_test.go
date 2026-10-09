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
				listed: test.listed, unproven: test.unproven,
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

func TestRetainClosableRetractionsDropsRetractionsOfOtherTeams(t *testing.T) {
	at := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	before := at.Add(-time.Hour)
	open := []OwnershipSnapshotRow{
		{TeamID: "gl:a", ProjectID: testPID("a/1"), Source: "provider_access", ValidFrom: before},
		{TeamID: "gl:b", ProjectID: testPID("b/1"), Source: "provider_access", ValidFrom: before},
	}
	plan := PlanOwnershipSnapshot(OwnershipSnapshot{Complete: true}, open, at)
	if len(plan.Retract) != 2 {
		t.Fatalf("seed plan retracts %d rows, want 2", len(plan.Retract))
	}
	kept := retainClosableRetractions(plan, open, []string{"gl:a"})
	if len(kept.Retract) != 1 || open[kept.Retract[0].Open].TeamID != "gl:a" {
		t.Fatalf("kept retractions = %+v, want only gl:a's row", kept.Retract)
	}
	if none := retainClosableRetractions(plan, open, nil); len(none.Retract) != 0 {
		t.Fatalf("no closable team kept %d retractions", len(none.Retract))
	}
}
