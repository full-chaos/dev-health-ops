package aianalytics

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-13679: the AI analytics team scope and the team of a row follow one rule,
// the one ownership rule of package teamscope (team_repo_ownership): the
// repository's owner, else no team. teams.repo_patterns is never read.

const (
	ownedRepo    = "aaaaaaaa-0000-0000-0000-000000000001"
	patternRepo  = "aaaaaaaa-0000-0000-0000-000000000002"
	coOwnedRepo  = "aaaaaaaa-0000-0000-0000-000000000003"
	unownedRepo  = "aaaaaaaa-0000-0000-0000-000000000004"
	scopedTeamID = "team-owner"
)

// ownerClient answers the repository catalogue and the teamscope ownership reads
// from an ownership table; every other statement fails, so a test sees what ran.
type ownerClient struct {
	repos      map[string]string   // repo id -> full name
	owners     map[string][]string // repo id -> owning teams, in the server's order
	statements []string
}

func (c *ownerClient) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statements = append(c.statements, st)
	strs := func(name string) []string {
		for _, x := range b {
			if x.Name == name {
				v, _ := x.Value.([]string)
				return v
			}
		}
		return nil
	}
	var rows [][]any
	switch {
	case strings.Contains(st, "{held_repo_ids:Array(String)}"):
		scope := map[string]bool{}
		for _, id := range strs(teamscope.BindingTeamIDs) {
			scope[id] = true
		}
		for _, id := range strs("held_repo_ids") {
			for _, team := range c.owners[id] {
				if scope[team] {
					rows = append(rows, []any{id})
					break
				}
			}
		}
	case strings.Contains(st, "{owner_repo_ids:Array(String)}"):
		for _, id := range strs("owner_repo_ids") {
			for _, team := range c.owners[id] {
				rows = append(rows, []any{id, team})
			}
		}
	case strings.Contains(st, "AS full_name"):
		ids := make([]string, 0, len(c.repos))
		for id := range c.repos {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			rows = append(rows, []any{id, c.repos[id]})
		}
	default:
		return nil, errors.New("ownerClient: unscripted statement")
	}
	return &scriptedRows{rows: rows}, nil
}

func newOwnerClient() *ownerClient {
	return &ownerClient{
		repos: map[string]string{
			ownedRepo:   "acme/owned",
			patternRepo: "acme/pattern-match",
			coOwnedRepo: "acme/shared",
			unownedRepo: "acme/nobody",
		},
		owners: map[string][]string{
			ownedRepo:   {scopedTeamID},
			patternRepo: {"team-other"},
			coOwnedRepo: {"team-main", scopedTeamID},
		},
	}
}

func TestTeamRepoIDs_AreTheOwnedRepositories(t *testing.T) {
	client := newOwnerClient()
	ids, ok := teamRepoIDs(context.Background(), client, "org-1", scopedTeamID, "test")
	if !ok {
		t.Fatal("the team scope must resolve")
	}
	sort.Strings(ids)
	if want := []string{ownedRepo, coOwnedRepo}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("team repositories = %v, want %v (owned and co-owned only)", ids, want)
	}
	held := false
	for _, st := range client.statements {
		if strings.Contains(st, "repo_patterns") {
			t.Fatalf("the team scope read teams.repo_patterns:\n%s", st)
		}
		held = held || strings.Contains(st, teamscope.Marker)
	}
	if !held {
		t.Fatal("the team scope did not read team_repo_ownership")
	}
}

func TestTeamRepoIDs_ATeamThatOwnsNothingHasNoRepositories(t *testing.T) {
	ids, ok := teamRepoIDs(context.Background(), newOwnerClient(), "org-1", "team-none", "test")
	if !ok || len(ids) != 0 {
		t.Fatalf("team-none = %v %v, want an empty, available scope", ids, ok)
	}
}

func TestTeamRepoIDs_AFailedOwnershipReadIsUnavailable(t *testing.T) {
	client := &failingOwnershipClient{ownerClient: newOwnerClient()}
	if _, ok := teamRepoIDs(context.Background(), client, "org-1", scopedTeamID, "test"); ok {
		t.Fatal("a failed ownership read must leave the scope unavailable, never empty")
	}
}

type failingOwnershipClient struct{ *ownerClient }

func (c *failingOwnershipClient) Query(ctx context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if strings.Contains(st, teamscope.Marker) {
		return nil, errors.New("boom")
	}
	return c.ownerClient.Query(ctx, st, b)
}

func TestOwnerTeams_FirstRowWinsAndUnownedIsNil(t *testing.T) {
	client := newOwnerClient()
	got, err := ownerTeams(context.Background(), client, "org-1", []string{ownedRepo, coOwnedRepo, unownedRepo}, "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if nameOrNil(got[ownedRepo]) != scopedTeamID || nameOrNil(got[coOwnedRepo]) != "team-main" {
		t.Fatalf("owners = %s, %s; want %s, team-main", nameOrNil(got[ownedRepo]), nameOrNil(got[coOwnedRepo]), scopedTeamID)
	}
	if v, ok := got[unownedRepo]; !ok || v != nil {
		t.Fatalf("an unowned repository must map to nil, got %v %v", v, ok)
	}
	st := client.statements[len(client.statements)-1]
	for _, want := range []string{teamscope.Marker, "(os.owner_team_id = {owner_prefer_team:String}) DESC, os.owner_share DESC, os.owner_team_id"} {
		if !strings.Contains(st, want) {
			t.Errorf("the owner read lacks %q:\n%s", want, st)
		}
	}
}

func TestAttributedPrs_ATeamThatOwnsNothingAnswersEmptyWithoutReadingRows(t *testing.T) {
	client := newOwnerClient()
	team := "team-none"
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	got, err := AttributedPrs(context.Background(), client, "org-1", dr, &model.AIScopeInput{TeamID: &team}, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != 0 {
		t.Fatalf("rows = %d, want none", len(got.Rows))
	}
}
