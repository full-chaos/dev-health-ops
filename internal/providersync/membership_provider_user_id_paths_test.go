package providersync

import (
	"testing"
	"time"
)

// CHAOS-9144: the pure steps that copy a membership row must keep provider_user_id.

// A row closed by a snapshot (a departure or a retired duplicate) keeps the id of the open row it closes.
func TestSnapshotWritersKeepTheProviderUserIDOnAClosedRow(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	id := "7"
	open := openMembership{OrgID: "o", Provider: "p", TeamID: "t", MemberID: "m", ProviderUserID: &id, ValidFrom: at.Add(-time.Hour), UpdatedAt: at.Add(-time.Hour)}
	github := githubMembershipWriter.Closed(open, at, at)
	gitlab := gitlabMembershipWriter.Closed(open, at, at)
	linear := linearMembershipWriter.Closed(open, at, at)
	for name, got := range map[string]*string{
		"github": githubMembershipWriter.ProviderUserID(github), "gitlab": gitlabMembershipWriter.ProviderUserID(gitlab), "linear": linearMembershipWriter.ProviderUserID(linear),
	} {
		if got == nil || *got != "7" {
			t.Errorf("%s: closed row provider_user_id = %v, want 7", name, got)
		}
	}
}

// Every provider's drift view carries the id, and the staged payload names it.
func TestEveryDriftViewConverterCarriesTheProviderUserID(t *testing.T) {
	id := "42"
	views := map[string]teamDriftMembershipView{
		"github": githubMembershipRowToDriftView(githubMembershipRow{ProviderUserID: &id}),
		"gitlab": gitlabMembershipRowToDriftView(gitlabTeamCatalogMembershipRow{ProviderUserID: &id}),
		"linear": linearMembershipRowToDriftView(linearReferenceMembershipRow{ProviderUserID: &id}),
		"jira":   jiraMembershipRowToDriftView(jiraTeamCatalogMembershipRow{ProviderUserID: &id}),
		"export": TeamDriftMembershipView{ProviderUserID: &id}.toInternal(),
	}
	for name, view := range views {
		if view.ProviderUserID == nil || *view.ProviderUserID != "42" {
			t.Errorf("%s: drift view provider_user_id = %v, want 42", name, view.ProviderUserID)
			continue
		}
		if got := membershipPayloadJSON("o", view)["provider_user_id"]; got == nil {
			t.Errorf("%s: staged payload has no provider_user_id", name)
		}
	}
}
