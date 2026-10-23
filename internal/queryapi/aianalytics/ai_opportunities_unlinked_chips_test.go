package aianalytics

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

const chipRepo = "11111111-1111-4111-8111-111111111111"

type linkReadClient struct {
	linked     []string
	err        error
	statements []string
	bindings   [][]clickhouse.Binding
}

func (c *linkReadClient) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statements = append(c.statements, st)
	c.bindings = append(c.bindings, b)
	if c.err != nil {
		return nil, c.err
	}
	var rows [][]any
	for _, id := range c.linked {
		rows = append(rows, []any{id})
	}
	return &scriptedRows{rows: rows}, nil
}

func chip(repo, number string) model.AIWorkGraphDrilldownRef {
	return model.AIWorkGraphDrilldownRef{RootType: "pr", RootID: repo + ":" + number, Label: "PR " + number}
}

func chipsOf(recs []model.AIOpportunity) [][]string {
	out := [][]string{}
	for _, r := range recs {
		ids := []string{}
		for _, c := range r.WorkGraphDrilldowns {
			ids = append(ids, c.RootID)
		}
		out = append(out, ids)
	}
	return out
}

func TestHideUnlinkedPullRequestChips_KeepsOnlyLinkedChipsAndLeavesEvidenceRefs(t *testing.T) {
	refs := []string{"git_pull_requests:" + chipRepo + ":1", "git_pull_requests:" + chipRepo + ":2", "git_pull_requests:" + chipRepo + ":3", "git_pull_requests:" + chipRepo + ":4", "git_pull_requests:" + chipRepo + ":5"}
	rec := model.AIOpportunity{EvidenceRefs: refs, WorkGraphDrilldowns: []model.AIWorkGraphDrilldownRef{
		chip(chipRepo, "1"), chip(chipRepo, "2"), chip(chipRepo, "3"), chip(chipRepo, "4"), chip(chipRepo, "5")}}
	client := &linkReadClient{linked: []string{chipRepo + ":2", chipRepo + ":4"}}
	recs := []model.AIOpportunity{rec}
	if err := hideUnlinkedPullRequestChips(context.Background(), client, "org-1", recs); err != nil {
		t.Fatal(err)
	}
	if got, want := chipsOf(recs), [][]string{{chipRepo + ":2", chipRepo + ":4"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("chips = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(recs[0].EvidenceRefs, refs) {
		t.Fatalf("evidence refs changed: %v", recs[0].EvidenceRefs)
	}
}

func TestHideUnlinkedPullRequestChips_NoLinkRowGivesNoChipAndAnEmptyList(t *testing.T) {
	recs := []model.AIOpportunity{{WorkGraphDrilldowns: []model.AIWorkGraphDrilldownRef{chip(chipRepo, "9")}}}
	if err := hideUnlinkedPullRequestChips(context.Background(), &linkReadClient{}, "org-1", recs); err != nil {
		t.Fatal(err)
	}
	if recs[0].WorkGraphDrilldowns == nil || len(recs[0].WorkGraphDrilldowns) != 0 {
		t.Fatalf("chips = %#v, want an empty non-nil list", recs[0].WorkGraphDrilldowns)
	}
}

func TestHideUnlinkedPullRequestChips_ReadCarriesTheOrgPredicateFinalAndOneDistinctIdList(t *testing.T) {
	client := &linkReadClient{linked: []string{chipRepo + ":1"}}
	recs := []model.AIOpportunity{
		{WorkGraphDrilldowns: []model.AIWorkGraphDrilldownRef{chip(chipRepo, "2"), chip(chipRepo, "1")}},
		{WorkGraphDrilldowns: []model.AIWorkGraphDrilldownRef{chip(chipRepo, "1")}},
	}
	if err := hideUnlinkedPullRequestChips(context.Background(), client, "org-1", recs); err != nil {
		t.Fatal(err)
	}
	if len(client.statements) != 1 {
		t.Fatalf("%d reads, want exactly one for the page", len(client.statements))
	}
	for _, want := range []string{"org_id = {org_id:String}", "FROM work_graph_issue_pr FINAL", "{pr_ids:Array(String)}"} {
		if !strings.Contains(client.statements[0], want) {
			t.Errorf("statement lacks %q:\n%s", want, client.statements[0])
		}
	}
	got := map[string]any{}
	for _, b := range client.bindings[0] {
		got[b.Name] = b.Value
	}
	if got["org_id"] != "org-1" || !reflect.DeepEqual(got["pr_ids"], []string{chipRepo + ":1", chipRepo + ":2"}) {
		t.Fatalf("bindings = %v, want org-1 and the two distinct sorted ids", got)
	}
}

func TestHideUnlinkedPullRequestChips_NoPullRequestChipMeansNoRead(t *testing.T) {
	client := &linkReadClient{}
	recs := []model.AIOpportunity{{WorkGraphDrilldowns: []model.AIWorkGraphDrilldownRef{}}, {}}
	if err := hideUnlinkedPullRequestChips(context.Background(), client, "org-1", recs); err != nil {
		t.Fatal(err)
	}
	if len(client.statements) != 0 {
		t.Fatalf("a read was made for a page without pull request chips: %v", client.statements)
	}
}

func TestHideUnlinkedPullRequestChips_UppercaseUUIDAndLeadingZerosAreLookedUpCanonically(t *testing.T) {
	upper := strings.ToUpper(chipRepo)
	client := &linkReadClient{linked: []string{chipRepo + ":7"}}
	recs := []model.AIOpportunity{{WorkGraphDrilldowns: []model.AIWorkGraphDrilldownRef{chip(upper, "007")}}}
	if err := hideUnlinkedPullRequestChips(context.Background(), client, "org-1", recs); err != nil {
		t.Fatal(err)
	}
	ids := client.bindings[0][1].Value.([]string)
	if !reflect.DeepEqual(ids, []string{chipRepo + ":7"}) {
		t.Fatalf("bound ids = %v, want the lower-case uuid and the canonical number", ids)
	}
	if len(recs[0].WorkGraphDrilldowns) != 1 {
		t.Fatalf("a linked chip was dropped: %v", recs[0].WorkGraphDrilldowns)
	}
}

func TestHideUnlinkedPullRequestChips_ANumberPastUInt32IsHiddenAndNeverBound(t *testing.T) {
	client := &linkReadClient{linked: []string{chipRepo + ":1"}}
	recs := []model.AIOpportunity{{WorkGraphDrilldowns: []model.AIWorkGraphDrilldownRef{chip(chipRepo, "1"), chip(chipRepo, "4294967296")}}}
	if err := hideUnlinkedPullRequestChips(context.Background(), client, "org-1", recs); err != nil {
		t.Fatal(err)
	}
	if got := chipsOf(recs); !reflect.DeepEqual(got, [][]string{{chipRepo + ":1"}}) {
		t.Fatalf("chips = %v", got)
	}
	if ids := client.bindings[0][1].Value.([]string); !reflect.DeepEqual(ids, []string{chipRepo + ":1"}) {
		t.Fatalf("bound ids = %v", ids)
	}
}

func TestHideUnlinkedPullRequestChips_AFullPageOfRefsIsOneReadUnderTheClientCap(t *testing.T) {
	const opportunities, perPage = 100, 5
	var recs []model.AIOpportunity
	var linked []string
	for i := 0; i < opportunities; i++ {
		var chips []model.AIWorkGraphDrilldownRef
		for j := 0; j < perPage; j++ {
			number := strconv.Itoa(i*perPage + j + 1)
			chips = append(chips, chip(chipRepo, number))
			if j == 0 {
				linked = append(linked, chipRepo+":"+number)
			}
		}
		recs = append(recs, model.AIOpportunity{WorkGraphDrilldowns: chips})
	}
	client := &linkReadClient{linked: linked}
	if err := hideUnlinkedPullRequestChips(context.Background(), client, "org-1", recs); err != nil {
		t.Fatal(err)
	}
	if len(client.statements) != 1 {
		t.Fatalf("%d reads, want one", len(client.statements))
	}
	if ids := client.bindings[0][1].Value.([]string); len(ids) != opportunities*perPage || len(ids) >= 1000 {
		t.Fatalf("%d ids bound, want %d and under the 1,000-row cap", len(ids), opportunities*perPage)
	}
	for i, r := range recs {
		if len(r.WorkGraphDrilldowns) != 1 {
			t.Fatalf("opportunity %d keeps %d chips, want its one linked chip", i, len(r.WorkGraphDrilldowns))
		}
	}
}

func TestHideUnlinkedPullRequestChips_AReadErrorIsReturned(t *testing.T) {
	boom := errors.New("boom")
	recs := []model.AIOpportunity{{WorkGraphDrilldowns: []model.AIWorkGraphDrilldownRef{chip(chipRepo, "1")}}}
	err := hideUnlinkedPullRequestChips(context.Background(), &linkReadClient{err: boom}, "org-1", recs)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap the read error", err)
	}
}

func TestAiOpportunities_HidesThePullRequestChipsWithNoIssueLink(t *testing.T) {
	all := namesClientD(t, "ai/everything")
	before := runOpportunities(t, all)
	var chipsBefore, refsBefore int
	for _, r := range before {
		chipsBefore += len(r.WorkGraphDrilldowns)
		refsBefore += len(r.EvidenceRefs)
	}
	if chipsBefore == 0 {
		t.Fatal("the recorded case has no pull request chip, so the test would prove nothing")
	}
	none := namesClientD(t, "ai/everything")
	none.linkedPRs = []string{}
	after := runOpportunities(t, none)
	var chipsAfter, refsAfter int
	for _, r := range after {
		chipsAfter += len(r.WorkGraphDrilldowns)
		refsAfter += len(r.EvidenceRefs)
		if r.WorkGraphDrilldowns == nil {
			t.Errorf("%s: chips = nil, want an empty list", r.OpportunityID)
		}
	}
	if chipsAfter != 0 {
		t.Errorf("%d chips left with no linked pull request, want 0", chipsAfter)
	}
	if refsAfter != refsBefore || len(after) != len(before) {
		t.Errorf("evidence refs %d -> %d, opportunities %d -> %d: only chips may change", refsBefore, refsAfter, len(before), len(after))
	}
	if len(none.linkedStatements) != 1 {
		t.Errorf("%d linked pull request reads, want 1", len(none.linkedStatements))
	}
}
