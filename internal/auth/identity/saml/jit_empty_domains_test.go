package saml_test

import (
	"net/http"
	"reflect"
	"regexp"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
)

var reasonCode = regexp.MustCompile(`"reason_code":"[^"]*"`)

// An empty or NULL allowed_domains makes no new account (chris D1 = B on CHAOS-11010): with JIT on, a free NameID
// gets the answer, the refusal reason code and the provider failure record of the same row with auto-provisioning
// off, and nothing is written.
func TestSAMLJITNewAccountWithNoDomainListIsAutoProvisionOff(t *testing.T) {
	for name, list := range map[string][]string{"empty": {}, "null": nil} {
		t.Run(name, func(t *testing.T) {
			post := func(autoProvision bool) (acsRun, *joinSeam) {
				members := &memberSet{ids: map[uuid.UUID]bool{}}
				seam := &joinSeam{members: members}
				return postACSRow(t, &lookupStore{}, members, seam, func(p *saml.Provider) {
					p.AllowedDomains, p.AutoProvision = list, autoProvision
				}), seam
			}
			want, _ := post(false)
			if want.status != http.StatusBadRequest {
				t.Fatalf("auto-provisioning off = %d %q, want its 400 (harness control)", want.status, want.detail)
			}
			got, seam := post(true)
			if got.status != want.status || got.detail != want.detail {
				t.Fatalf("ACS = %d %q (provisions %d), want the auto-provisioning-off answer %d %q",
					got.status, got.detail, len(seam.provisioned), want.status, want.detail)
			}
			if g, w := reasonCode.FindAllString(got.logs, -1), reasonCode.FindAllString(want.logs, -1); !reflect.DeepEqual(g, w) {
				t.Fatalf("refusal reason codes %v, want the auto-provisioning-off codes %v", g, w)
			}
			if len(seam.provisioned) != 0 || seam.joins != 0 || len(got.members.ids) != 0 || len(got.sessions.got) != 0 {
				t.Fatalf("provisions %d, joins %d, members %d, sessions %d; want none",
					len(seam.provisioned), seam.joins, len(got.members.ids), len(got.sessions.got))
			}

			record := func(autoProvision bool) (acsAnswer, *recordedFailures) {
				failures := &recordedFailures{}
				return postACSCase(t, acsCase{email: "u@unlisted.example", list: list, failures: failures,
					provider: func(p *saml.Provider) { p.AutoProvision = autoProvision }}), failures
			}
			wantA, wantRows := record(false)
			gotA, gotRows := record(true)
			if gotA.status != wantA.status || gotA.body != wantA.body || gotA.sessions != 0 {
				t.Fatalf("ACS = %d %s with %d sessions, want the auto-provisioning-off answer %d %s",
					gotA.status, gotA.body, gotA.sessions, wantA.status, wantA.body)
			}
			if len(gotRows.rows) != len(wantRows.rows) || len(wantRows.rows) != 1 ||
				gotRows.rows[0].Stage != wantRows.rows[0].Stage || gotRows.rows[0].ErrorMessage != wantRows.rows[0].ErrorMessage ||
				gotRows.rows[0].Protocol != wantRows.rows[0].Protocol {
				t.Fatalf("failure records %+v, want the auto-provisioning-off record %+v", gotRows.rows, wantRows.rows)
			}
		})
	}
}

// A NameID the row already holds still signs in and joins with no domain list: the list decides only new accounts.
func TestSAMLJITLinkedNonMemberWithNoDomainListStillJoins(t *testing.T) {
	for name, list := range map[string][]string{"empty": {}, "null": nil} {
		t.Run(name, func(t *testing.T) {
			members := &memberSet{ids: map[uuid.UUID]bool{}}
			seam := &joinSeam{members: members}
			linked := saml.Principal{ID: uuid.New()}
			run := postACSRow(t, &lookupStore{hit: &linked}, members, seam, func(p *saml.Provider) { p.AllowedDomains = list })
			if run.status != http.StatusOK || seam.joins != 1 || len(seam.provisioned) != 0 {
				t.Fatalf("ACS = %d %q joins %d provisions %d, want 200, one join, no account", run.status, run.detail, seam.joins, len(seam.provisioned))
			}
			if len(run.sessions.got) != 1 || run.sessions.got[0].PrincipalID != linked.ID ||
				run.sessions.got[0].OrganizationID == nil || *run.sessions.got[0].OrganizationID != run.org {
				t.Fatalf("sessions %+v, want one in org %s for the linked account", run.sessions.got, run.org)
			}
		})
	}
}

// With JIT off no sign-in makes an account; a free NameID under a row with no domain list keeps its
// membership_required answer, and nothing is written.
func TestSAMLJITOffNewAccountWithNoDomainListKeepsMembershipRequired(t *testing.T) {
	for name, list := range map[string][]string{"empty": {}, "null": nil} {
		t.Run(name, func(t *testing.T) {
			members := &memberSet{ids: map[uuid.UUID]bool{}}
			run := postACSRow(t, &lookupStore{}, members, nil, func(p *saml.Provider) { p.AllowedDomains = list })
			run.refused(t, http.StatusForbidden, detailNotMember, "membership_required")
			if len(members.ids) != 0 {
				t.Fatalf("members %d, want none", len(members.ids))
			}
		})
	}
}
