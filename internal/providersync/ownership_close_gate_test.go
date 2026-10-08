package providersync

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// staticScopeCensus answers ActiveSiblingIntegrations with fixed siblings or
// a fixed error, and counts its calls.
type staticScopeCensus struct {
	siblings []OwnershipSiblingIntegration
	err      error
	calls    *int
}

func (census staticScopeCensus) ActiveSiblingIntegrations(context.Context, string, string, string) ([]OwnershipSiblingIntegration, error) {
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
		{"the provider's end signal", providerfoundation.PageCollection{Pages: 1}, true},
		{"an unconfirmed end", providerfoundation.PageCollection{Pages: 1, EndUnconfirmed: true}, false},
		{"the page budget ran out", providerfoundation.PageCollection{Pages: 1, PageBudgetExhausted: true}, false},
		{"the item cap was reached", providerfoundation.PageCollection{Pages: 1, ItemCapReached: true}, false},
	} {
		if got := ownershipListingProvesEnd(test.pages); got != test.want {
			t.Errorf("%s: ownershipListingProvesEnd = %v, want %v", test.name, got, test.want)
		}
	}
}

func TestDecideOwnershipCloseClosesOnlyProvenListingsOfAnUnsharedScope(t *testing.T) {
	ctx := context.Background()
	ref := TeamCatalogReference{OrgID: "org", IntegrationID: "integration-a"}
	gitlabKey := gitlabSiblingScopeKey
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
		{name: "a sibling on the same group path, in another case: nothing closes",
			census: staticScopeCensus{siblings: []OwnershipSiblingIntegration{{IntegrationID: "b", SyncOptions: map[string]any{"group_path": "ORG"}}}},
			ref:    ref, listed: []string{"gl:a"}, wantReasons: []string{OwnershipCloseSkippedScopeShared}, wantCalls: 1},
		{name: "a sibling whose group path is not known: nothing closes",
			census: staticScopeCensus{siblings: []OwnershipSiblingIntegration{{IntegrationID: "b"}}},
			ref:    ref, listed: []string{"gl:a"}, wantReasons: []string{OwnershipCloseSkippedScopeShared}, wantCalls: 1},
		{name: "a sibling on another group path does not stop the close",
			census: staticScopeCensus{siblings: []OwnershipSiblingIntegration{{IntegrationID: "b", SyncOptions: map[string]any{"group_path": "other"}}}},
			ref:    ref, listed: []string{"gl:a"}, wantClosable: []string{"gl:a"}, wantCalls: 1},
		{name: "a failed census read: nothing closes",
			census: staticScopeCensus{err: errors.New("census read failed")},
			ref:    ref, listed: []string{"gl:a"}, wantReasons: []string{OwnershipCloseSkippedCensusFailed}, wantCalls: 1},
		{name: "no census: nothing closes",
			census: nil, ref: ref, listed: []string{"gl:a"}, wantReasons: []string{OwnershipCloseSkippedCensusUnavailable}},
		{name: "a run without an integration: nothing closes",
			census: staticScopeCensus{}, ref: TeamCatalogReference{OrgID: "org"}, listed: []string{"gl:a"},
			wantReasons: []string{OwnershipCloseSkippedCensusUnavailable}},
		{name: "an unproven listing and a shared scope: both are said",
			census: staticScopeCensus{siblings: []OwnershipSiblingIntegration{{IntegrationID: "b", CredentialConfig: map[string]string{"group_path": "org"}}}},
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
				ref: test.ref, provider: "gitlab", scopeKey: "org",
				listed: test.listed, unproven: test.unproven, siblingScopeKey: gitlabKey,
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

func TestSiblingScopeKeysUseEachCollectorsOwnPrecedence(t *testing.T) {
	for _, test := range []struct {
		name      string
		key       func(OwnershipSiblingIntegration) (string, bool)
		sibling   OwnershipSiblingIntegration
		want      string
		wantKnown bool
	}{
		{"gitlab: credential config outranks sync_options", gitlabSiblingScopeKey, OwnershipSiblingIntegration{
			CredentialConfig: map[string]string{"group": "from-credential"}, SyncOptions: map[string]any{"group_path": "from-options"},
		}, "from-credential", true},
		{"gitlab: group_path outranks group and owner in sync_options", gitlabSiblingScopeKey, OwnershipSiblingIntegration{
			SyncOptions: map[string]any{"owner": "o", "group": "g", "group_path": "gp"},
		}, "gp", true},
		{"gitlab: nothing configured is not known", gitlabSiblingScopeKey, OwnershipSiblingIntegration{}, "", false},
		{"github: the credential's plain config names the org", githubSiblingScopeKey, OwnershipSiblingIntegration{
			CredentialConfig: map[string]string{"owner": "acme"},
		}, "acme", true},
		{"github: an org only in sync_options is not known (the encrypted fields outrank it)", githubSiblingScopeKey, OwnershipSiblingIntegration{
			SyncOptions: map[string]any{"org": "acme"},
		}, "", false},
	} {
		got, known := test.key(test.sibling)
		if got != test.want || known != test.wantKnown {
			t.Errorf("%s: got (%q, %v), want (%q, %v)", test.name, got, known, test.want, test.wantKnown)
		}
	}
}

func TestRetainClosableRetractionsDropsRetractionsOfOtherTeams(t *testing.T) {
	at := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	before := at.Add(-time.Hour)
	open := []OwnershipSnapshotRow{
		{TeamID: "gl:a", ProjectID: "a/1", Source: "provider_access", ValidFrom: before},
		{TeamID: "gl:b", ProjectID: "b/1", Source: "provider_access", ValidFrom: before},
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
