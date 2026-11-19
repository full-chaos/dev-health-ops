//go:build integration

package providersync

import (
	"testing"
	"time"
)

// CHAOS-9349 against the real migration chain: a derived row whose input rows
// are missing from the run (not changed) stays open; a derived row whose input
// row is in the run and no longer derives it is retracted.

func (org *retractionGuardOrg) deleteRows(table, workItemID string) {
	org.t.Helper()
	if err := org.conn.Exec(org.ctx, `ALTER TABLE `+table+` DELETE WHERE org_id = ? AND work_item_id = ? SETTINGS mutations_sync = 2`,
		org.orgID, workItemID); err != nil {
		org.t.Fatalf("delete %s rows of %s: %v", table, workItemID, err)
	}
}

var missingInputTables = []string{"work_items", "work_graph_issue_pr"}

// TestDerivedRepoOwnershipMissingRowInsideOneProviderKeepsTheRow: one
// provider's team reaches two repositories through two items. The second
// item's row (or its pull request link) is missing from the next run; the
// first item still proves the provider's kind.
func TestDerivedRepoOwnershipMissingRowInsideOneProviderKeepsTheRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, provider := range retractionGuardIntegrationProviders {
		for _, table := range missingInputTables {
			t.Run(provider+"_"+table, func(t *testing.T) {
				org := newRetractionGuardOrg(t, ctx, conn, "chaos-9349-inside-"+provider+"-"+table, "acme/repo-1", "acme/repo-2")
				org.own(provider, provider+"-proj-a", "team-a", "acme/repo-1")
				org.own(provider, provider+"-proj-a", "team-a", "acme/repo-2")
				if written, retracted, _, _ := org.derive(); written != 2 || retracted != 0 {
					t.Fatalf("first Derive: written=%d retracted=%d, want 2 and 0", written, retracted)
				}

				org.deleteRows(table, provider+":G-2")
				written, retracted, inputsReady, stats := org.derive()
				t.Logf("missing %s row of %s:G-2: inputsReady=%v written=%d retracted=%d held=%d",
					table, provider, inputsReady, written, retracted, stats.RetractionsHeld)
				org.assertOpenOwner("acme/repo-1", "team-a")
				org.assertOpenOwner("acme/repo-2", "team-a")
			})
		}
	}
}

// TestDerivedRepoOwnershipProviderInputsMissingKeepTheRow: provider A's
// project link stays open, but A's work items (or A's issue links to pull
// requests) are not in the run. Provider B's inputs are.
func TestDerivedRepoOwnershipProviderInputsMissingKeepTheRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, a := range retractionGuardIntegrationProviders {
		for _, b := range retractionGuardIntegrationProviders {
			if a == b {
				continue
			}
			for _, table := range missingInputTables {
				t.Run(a+"_"+table+"_missing_"+b+"_present", func(t *testing.T) {
					org := newRetractionGuardOrg(t, ctx, conn, "chaos-9349-provider-"+a+"-"+b+"-"+table, "acme/repo-a", "acme/repo-b")
					org.own(a, a+"-proj-a", "team-a", "acme/repo-a")
					org.own(b, b+"-proj-b", "team-b", "acme/repo-b")
					if written, retracted, _, _ := org.derive(); written != 2 || retracted != 0 {
						t.Fatalf("first Derive: written=%d retracted=%d, want 2 and 0", written, retracted)
					}

					org.deleteRows(table, a+":G-1")
					written, retracted, inputsReady, stats := org.derive()
					t.Logf("missing %s rows of provider %s: inputsReady=%v written=%d retracted=%d held=%d",
						table, a, inputsReady, written, retracted, stats.RetractionsHeld)
					org.assertOpenOwner("acme/repo-a", "team-a")
					org.assertOpenOwner("acme/repo-b", "team-b")
				})
			}
		}
	}
}

// TestDerivedRepoOwnershipRealRemovalStillRetracts is the CHAOS-4365 R3
// guard: the single-provider org's only pull request is in the run and lost
// its project_id. The input says the claim is gone, so the row is retracted.
func TestDerivedRepoOwnershipRealRemovalStillRetracts(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, provider := range retractionGuardIntegrationProviders {
		t.Run(provider, func(t *testing.T) {
			org := newRetractionGuardOrg(t, ctx, conn, "chaos-9349-r3-"+provider, "acme/repo-a")
			repoID := org.repos["acme/repo-a"]
			prID := provider + ":PR-1"
			seedTeamProjectOwnership(t, ctx, conn, org.orgID, provider, provider+"-proj-a", "team-a", true, org.at)
			seedWorkItemOfType(t, ctx, conn, org.orgID, prID, provider, "pr", repoID, provider+"-proj-a", org.at)
			if written, retracted, _, _ := org.derive(); written != 1 || retracted != 0 {
				t.Fatalf("first Derive: written=%d retracted=%d, want 1 and 0", written, retracted)
			}

			seedWorkItemOfType(t, ctx, conn, org.orgID, prID, provider, "pr", repoID, "", org.at.Add(time.Hour))
			written, retracted, inputsReady, stats := org.derive()
			t.Logf("pull request lost its project_id: inputsReady=%v written=%d retracted=%d held=%d",
				inputsReady, written, retracted, stats.RetractionsHeld)
			if retracted != 1 || stats.RetractionsHeld != 0 {
				t.Fatalf("real removal: retracted=%d held=%d, want 1 and 0", retracted, stats.RetractionsHeld)
			}
			org.assertNoOpenOwner("acme/repo-a")
		})
	}
}
