package repoidentity

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

const (
	orgA = "11111111-1111-4111-8111-111111111111"
	orgB = "22222222-2222-4222-8222-222222222222"
)

type fakeSource struct {
	repos []Repo
	err   error
}

func (f fakeSource) Repos(context.Context) ([]Repo, error) { return f.repos, f.err }

type fakeSink struct {
	existing []uuid.UUID
	got      map[uuid.UUID][]Repo
	fail     map[uuid.UUID]bool
	res      OrgResult
}

func (f *fakeSink) OrgsWithIdentities(context.Context) ([]uuid.UUID, error) { return f.existing, nil }

func (f *fakeSink) Apply(_ context.Context, org uuid.UUID, repos []Repo) (OrgResult, error) {
	if f.fail[org] {
		return OrgResult{}, errors.New("sink")
	}
	if f.got == nil {
		f.got = map[uuid.UUID][]Repo{}
	}
	f.got[org] = repos
	return f.res, nil
}

func TestGroupKeepsValidRowsOnce(t *testing.T) {
	groups, skipped := Group([]Repo{
		{orgA, "github", "42", "acme/api"},
		{orgA, "github", "42", "acme/renamed"},
		{orgA, "gitlab", "42", "acme/other"},
		{orgB, "github", "7", "acme/x/y"},
		{"default", "github", "1", "acme/a"},
		{orgA, "GitHub", "1", "acme/a"},
		{orgA, "github", "x1", "acme/a"},
		{orgA, "github", "9", "Acme/Bad"},
		{orgA, "github", "9", "*"},
		{orgA, "github", "", "acme/a"},
	})
	if skipped != 6 {
		t.Errorf("skipped %d, want 6", skipped)
	}
	a := groups[uuid.MustParse(orgA)]
	if len(a) != 2 || a[0].Path != "acme/renamed" && a[0].Path != "acme/api" || a[1].Provider != "gitlab" {
		t.Errorf("org a: %+v", a)
	}
	for _, r := range a {
		if r.Provider == "github" && r.Path != "acme/renamed" {
			t.Errorf("the last row of one id must win: %+v", r)
		}
	}
	if len(groups[uuid.MustParse(orgB)]) != 1 {
		t.Errorf("org b: %+v", groups[uuid.MustParse(orgB)])
	}
}

func TestRunCountsFailedOrgAndGoesOn(t *testing.T) {
	sink := &fakeSink{fail: map[uuid.UUID]bool{uuid.MustParse(orgA): true}, res: OrgResult{Changed: true, UnresolvedTotal: 2, Unresolved: []uuid.UUID{uuid.MustParse(orgB)}}}
	rep, err := Run(context.Background(), fakeSource{repos: []Repo{{orgA, "github", "1", "acme/a"}, {orgB, "github", "2", "acme/b"}}}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Orgs != 2 || rep.OrgsFailed != 1 || rep.OrgsChanged != 1 || rep.UnresolvedTotal != 2 || len(rep.Unresolved) != 1 {
		t.Errorf("%+v", rep)
	}
	if len(sink.got[uuid.MustParse(orgB)]) != 1 {
		t.Error("org b not applied after org a failed")
	}
}

func TestRunSourceErrorStops(t *testing.T) {
	if _, err := Run(context.Background(), fakeSource{err: errors.New("down")}, &fakeSink{}); err == nil {
		t.Fatal("source error swallowed")
	}
}

func TestReportLinesListIdsOnly(t *testing.T) {
	id := uuid.MustParse(orgA)
	lines := Report{Orgs: 1, Repos: 3, UnresolvedTotal: 1, Unresolved: []uuid.UUID{id}}.Lines()
	if len(lines) != 2 || !strings.Contains(lines[0], "unresolved_grants=1") || !strings.HasSuffix(lines[1], orgA) {
		t.Errorf("%v", lines)
	}
}

func TestRunCapsListedUnresolvedIDs(t *testing.T) {
	var many []uuid.UUID
	for i := 0; i < MaxListedGrants+5; i++ {
		many = append(many, uuid.New())
	}
	rep, _ := Run(context.Background(), fakeSource{repos: []Repo{{orgA, "github", "1", "acme/a"}}}, &fakeSink{res: OrgResult{UnresolvedTotal: len(many), Unresolved: many}})
	if len(rep.Unresolved) != MaxListedGrants || rep.UnresolvedTotal != len(many) {
		t.Errorf("listed %d total %d", len(rep.Unresolved), rep.UnresolvedTotal)
	}
}

func TestSyncAuditRowMatchesTheWireSchema(t *testing.T) {
	org := orgA
	auditwire.MustValidate(t, audit.AuditEvent{EventType: "auth.repository_identity.synced", Outcome: audit.OutcomeAllowed, OrganizationID: &org})
}

func TestRunAppliesAnEmptySetToAnOrgWithIdentitiesAndNoSourceRows(t *testing.T) {
	a, b := uuid.MustParse(orgA), uuid.MustParse(orgB)
	sink := &fakeSink{existing: []uuid.UUID{a, b}}
	rep, err := Run(context.Background(), fakeSource{repos: []Repo{{orgB, "github", "2", "acme/b"}}}, sink)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := sink.got[a]
	if !ok || len(got) != 0 {
		t.Errorf("org a (identities, no source rows) was not applied an empty set: %v %v", ok, got)
	}
	if len(sink.got[b]) != 1 || rep.Orgs != 2 {
		t.Errorf("org b %v, orgs %d", sink.got[b], rep.Orgs)
	}
	sink = &fakeSink{existing: []uuid.UUID{a}}
	if _, err := Run(context.Background(), fakeSource{}, sink); err != nil {
		t.Fatal(err)
	}
	if _, ok := sink.got[a]; !ok {
		t.Error("an empty source left the identities of org a")
	}
}
