package atlassianteams

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"atlassian/atlassian"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// The gateway prover against the answers the gateway gave
// (../providersync/testdata/provider_answers/atlassian), served to the real
// client by the package's gateway double. A NOT FOUND answer is the recorded
// body, verbatim; a FOUND answer is constructed from the recorded shape.

type recordedGatewayAnswer struct {
	Case         string          `json:"case"`
	Status       int             `json:"status"`
	BodyShape    json.RawMessage `json:"body_shape"`
	BodyVerbatim string          `json:"body_verbatim"`
}

func loadGatewayAnswer(t testing.TB, name string) recordedGatewayAnswer {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "providersync", "testdata", "provider_answers", "atlassian", name+".json"))
	if err != nil {
		t.Fatalf("the recorded answer %s is missing: %v", name, err)
	}
	var answer recordedGatewayAnswer
	if err := json.Unmarshal(raw, &answer); err != nil {
		t.Fatal(err)
	}
	return answer
}

// teamByIDFound builds the answer of TeamById for an existing team from the
// recorded shape: the keys of the recording, the values of the test.
func teamByIDFound(t testing.TB, state string) any {
	t.Helper()
	answer := loadGatewayAnswer(t, "team_by_id_found")
	var shape struct {
		Data struct {
			Team struct {
				TeamV2 map[string]any `json:"teamV2"`
			} `json:"team"`
		} `json:"data"`
	}
	if err := json.Unmarshal(answer.BodyShape, &shape); err != nil {
		t.Fatal(err)
	}
	node := map[string]any{}
	for key := range shape.Data.Team.TeamV2 {
		node[key] = "x"
	}
	node["state"] = state
	return map[string]any{"data": map[string]any{"team": map[string]any{"teamV2": node}}}
}

// teamByIDNotFound is the recorded answer for a team that does not exist, verbatim.
func teamByIDNotFound(t testing.TB) any {
	t.Helper()
	answer := loadGatewayAnswer(t, "team_by_id_not_found")
	if answer.Status != 200 || answer.BodyVerbatim == "" {
		t.Fatalf("the recorded not-found answer is not the one the prover reads: %+v", answer)
	}
	return json.RawMessage(answer.BodyVerbatim)
}

type gatewayScript struct {
	teamByID   func(teamARI string) (int, any)
	users      func(teamARI string) (int, any)
	containers func(teamARI string) (int, any)
}

func (script gatewayScript) serve(req request) (int, any) {
	switch req.Operation {
	case "TeamById":
		id, _ := req.Variables["teamId"].(string)
		return script.teamByID(id)
	case "TeamworkGraphTeamUsers":
		id, _ := req.Variables["teamId"].(string)
		return script.users(id)
	case "TeamConnectedContainers":
		id, _ := req.Variables["id"].(string)
		return script.containers(id)
	}
	return 500, map[string]any{"errors": []any{map[string]any{"message": "unexpected " + req.Operation}}}
}

const (
	storedTeamA = "jira:aaaaaaaa-0000-4000-8000-000000000001"
	storedTeamB = "jira:bbbbbbbb-0000-4000-8000-000000000002"
)

func TestTeamAbsenceReadsTheRecordedTeamByIDAnswers(t *testing.T) {
	notFound := teamByIDNotFound(t)
	forbidden := json.RawMessage(`{"errors":[{"message":"x","extensions":{"errorSource":"UNDERLYING_SERVICE","statusCode":403,"classification":"Forbidden"}}],"data":{"team":{"teamV2":null}}}`)
	cases := []struct {
		name   string
		status int
		body   any
		want   providersync.SnapshotAbsence
	}{
		{"the recorded answer for a team that does not exist", 200, notFound, providersync.SnapshotAbsenceProven},
		{"an ACTIVE team", 200, teamByIDFound(t, "ACTIVE"), providersync.SnapshotFactStillHeld},
		{"an archived team has no members and no links: the catalog fact is gone", 200, teamByIDFound(t, "ARCHIVED"), providersync.SnapshotAbsenceProven},
		{"a team with no state proves nothing", 200, teamByIDFound(t, ""), providersync.SnapshotAbsenceNotProven},
		{"another error classification proves nothing", 200, forbidden, providersync.SnapshotAbsenceNotProven},
		{"HTTP 500 proves nothing", 500, map[string]any{"errors": []any{map[string]any{"message": "boom"}}}, providersync.SnapshotAbsenceNotProven},
	}
	for _, c := range cases {
		g := newGateway(t, gatewayScript{teamByID: func(string) (int, any) { return c.status, c.body }}.serve)
		absence := &GatewayAbsence{Reads: g.client(), SiteID: "site-uuid"}
		if got := absence.TeamAbsence(context.Background(), storedTeamA); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
		// One question per team and run.
		absence.TeamAbsence(context.Background(), storedTeamA)
		if n := g.count("TeamById", ""); n != 1 {
			t.Errorf("%s: the gateway was asked %d times, want 1", c.name, n)
		}
	}
	if got := (&GatewayAbsence{}).TeamAbsence(context.Background(), storedTeamA); got != providersync.SnapshotAbsenceNotProven {
		t.Errorf("no reads: %v, want not proven", got)
	}
	g := newGateway(t, gatewayScript{teamByID: func(string) (int, any) { return 200, notFound }}.serve)
	if got := (&GatewayAbsence{Reads: g.client(), SiteID: "site-uuid"}).TeamAbsence(context.Background(), "gh:not-a-jira-team"); got != providersync.SnapshotAbsenceNotProven {
		t.Errorf("a team id that is no Atlassian team: %v, want not proven", got)
	}
	if n := g.count("TeamById", ""); n != 0 {
		t.Errorf("a team id that is no Atlassian team was asked for %d times, want 0", n)
	}
	// The question names the team (the client sends "team/<uuid>", the form it gave when the answers were
	// recorded) and the site.
	g = newGateway(t, gatewayScript{teamByID: func(string) (int, any) { return 200, notFound }}.serve)
	(&GatewayAbsence{Reads: g.client(), SiteID: "site-uuid"}).TeamAbsence(context.Background(), storedTeamA)
	if len(g.requests) != 1 || g.requests[0].Variables["teamId"] != "team/aaaaaaaa-0000-4000-8000-000000000001" || g.requests[0].Variables["siteId"] != "site-uuid" {
		t.Errorf("the question = %+v", g.requests)
	}
}

func TestMemberAndLinkAbsenceProveATeamGoneWithoutReadingItsLists(t *testing.T) {
	g := newGateway(t, gatewayScript{
		teamByID:   func(string) (int, any) { return 200, teamByIDNotFound(t) },
		users:      func(string) (int, any) { return 500, nil },
		containers: func(string) (int, any) { return 500, nil },
	}.serve)
	absence := &GatewayAbsence{Reads: g.client(), SiteID: "site-uuid"}
	if got := absence.MembershipAbsence(context.Background(), storedTeamA, "jira:alice-1"); got != providersync.SnapshotAbsenceProven {
		t.Errorf("member of a team that is gone: %v, want proven", got)
	}
	if got := absence.LinkAbsence(context.Background(), storedTeamA, providersync.ProjectID{}); got != providersync.SnapshotAbsenceProven {
		t.Errorf("link of a team that is gone: %v, want proven", got)
	}
	if g.count("TeamworkGraphTeamUsers", "")+g.count("TeamConnectedContainers", "") != 0 || g.count("TeamById", "") != 1 {
		t.Errorf("a team that is gone needs one TeamById and no list read; requests = %d", len(g.requests))
	}
}

func TestMembershipAbsenceIsAFreshCompleteReadOfTheTeamsMembers(t *testing.T) {
	members := func(accounts ...string) func(string) (int, any) {
		return func(string) (int, any) {
			edges := make([]map[string]any, 0, len(accounts))
			for _, account := range accounts {
				edges = append(edges, userEdge(storedTeamA, account))
			}
			return 200, connection("teamworkGraph_teamUsers", "", edges...)
		}
	}
	active := func(string) (int, any) { return 200, teamByIDFound(t, "ACTIVE") }
	cases := []struct {
		name   string
		script gatewayScript
		member string
		want   providersync.SnapshotAbsence
	}{
		{"the member is on the re-read (the first walk lost it)", gatewayScript{teamByID: active, users: members("alice-1", "bob-2")}, "jira:bob-2", providersync.SnapshotFactStillHeld},
		{"the stored member id is the lower-cased account", gatewayScript{teamByID: active, users: members("Alice-1")}, "jira:alice-1", providersync.SnapshotFactStillHeld},
		{"the member is not on the re-read of an existing team", gatewayScript{teamByID: active, users: members("alice-1")}, "jira:bob-2", providersync.SnapshotAbsenceProven},
		{"a team with no member left", gatewayScript{teamByID: active, users: members()}, "jira:bob-2", providersync.SnapshotAbsenceProven},
		{"a re-read that fails proves nothing", gatewayScript{teamByID: active, users: func(string) (int, any) { return 500, nil }}, "jira:bob-2", providersync.SnapshotAbsenceNotProven},
		{"a team whose existence is not answered proves nothing", gatewayScript{
			teamByID: func(string) (int, any) { return 500, nil }, users: members()}, "jira:bob-2", providersync.SnapshotAbsenceNotProven},
	}
	for _, c := range cases {
		g := newGateway(t, c.script.serve)
		absence := &GatewayAbsence{Reads: g.client(), SiteID: "site-uuid"}
		if got := absence.MembershipAbsence(context.Background(), storedTeamA, c.member); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
	// One re-read of a team's members for all its candidates.
	g := newGateway(t, gatewayScript{teamByID: active, users: members("alice-1")}.serve)
	absence := &GatewayAbsence{Reads: g.client(), SiteID: "site-uuid"}
	for _, member := range []string{"jira:bob-2", "jira:carol-3", "jira:alice-1"} {
		absence.MembershipAbsence(context.Background(), storedTeamA, member)
	}
	if n := g.count("TeamworkGraphTeamUsers", ""); n != 1 {
		t.Errorf("the members of one team were read %d times for 3 candidates, want 1", n)
	}
}

func TestLinkAbsenceIsAFreshCompleteReadOfTheTeamsContainers(t *testing.T) {
	links := func(nativeIDs ...string) func(string) (int, any) {
		return func(string) (int, any) {
			edges := make([]map[string]any, 0, len(nativeIDs))
			for _, id := range nativeIDs {
				edges = append(edges, projectEdge(storedTeamA, "KEY"+id, id))
			}
			return 200, containerPage("", edges...)
		}
	}
	active := func(string) (int, any) { return 200, teamByIDFound(t, "ACTIVE") }
	cases := []struct {
		name   string
		script gatewayScript
		link   string
		want   providersync.SnapshotAbsence
	}{
		{"the project is on the re-read", gatewayScript{teamByID: active, containers: links("10001", "10002")}, "10002", providersync.SnapshotFactStillHeld},
		{"the project is not on the re-read of an existing team", gatewayScript{teamByID: active, containers: links("10001")}, "10002", providersync.SnapshotAbsenceProven},
		{"the key-built id of the retired form is on no container", gatewayScript{teamByID: active, containers: links("10001")}, "org-1:jira:PLAT", providersync.SnapshotAbsenceProven},
		{"a re-read that fails proves nothing", gatewayScript{teamByID: active, containers: func(string) (int, any) { return 500, nil }}, "10002", providersync.SnapshotAbsenceNotProven},
	}
	for _, c := range cases {
		g := newGateway(t, c.script.serve)
		absence := &GatewayAbsence{Reads: g.client(), SiteID: "site-uuid"}
		projectID, _ := providersync.JiraProjectID(c.link)
		if got := absence.LinkAbsence(context.Background(), storedTeamA, projectID); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// stateReads answers TeamById with a team of the given state, whatever the real
// client would let through (it refuses an empty state itself): the adapter's own
// rule for a state it cannot read.
type stateReads struct {
	GatewayReads
	state string
}

func (reads stateReads) GetTeamByID(context.Context, string, string) (*atlassian.AtlassianTeam, error) {
	return &atlassian.AtlassianTeam{ID: "x", DisplayName: "x", State: reads.state}, nil
}

func TestATeamWithNoStateIsNoAnswer(t *testing.T) {
	for state, want := range map[string]providersync.SnapshotAbsence{
		"": providersync.SnapshotAbsenceNotProven, "  ": providersync.SnapshotAbsenceNotProven, "active": providersync.SnapshotFactStillHeld,
		"DISBANDED": providersync.SnapshotAbsenceProven,
	} {
		absence := &GatewayAbsence{Reads: stateReads{state: state}, SiteID: "site-uuid"}
		if got := absence.TeamAbsence(context.Background(), storedTeamA); got != want {
			t.Errorf("state %q: %v, want %v", state, got, want)
		}
	}
}
