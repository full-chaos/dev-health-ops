//go:build integration

package atlassianteams

import (
	"context"
	"testing"
	"time"
)

// CHAOS-9124: a fact the run no longer holds closes only on the gateway's own
// answer. The first run writes teams A (alice-1 and bob-2, project PLAT) and C
// (carol-3); the second run no longer returns alice-1, the link and team C.
// The gateway's answers are the recorded ones (testdata/provider_answers/
// atlassian): TeamById of a team that does not exist verbatim; a team that
// exists, a member read and a container read built from the recorded shape and
// the package's own gateway double.
func TestARunClosesAnAbsentFactOnlyOnTheGatewaysAnswer(t *testing.T) {
	first := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
	notFound := teamByIDNotFound(t)
	teamIs := func(state string, gone ...string) func(string) (int, any) {
		return func(id string) (int, any) {
			for _, team := range gone {
				if id == "team/"+team {
					return 200, notFound
				}
			}
			if state == "" {
				return 500, map[string]any{"errors": []any{map[string]any{"message": "boom"}}}
			}
			return 200, teamByIDFound(t, state)
		}
	}
	const uuidC = "cccccccc-0000-4000-8000-000000000003"
	// The second run's search: A and B (archived) only.
	shrunk := func(users func(string) (int, any), byID func(string) (int, any)) func(request) (int, any) {
		return func(req request) (int, any) {
			switch req.Operation {
			case "TeamSearchV2":
				return 200, searchPage("", teamNode(teamA, "Platform", "ACTIVE"), teamNode(teamB, "Old", "ARCHIVED"))
			case "TeamworkGraphTeamUsers":
				return users(req.Variables["teamId"].(string))
			case "TeamConnectedContainers":
				return 200, containerPage("")
			case "TeamById":
				return byID(req.Variables["teamId"].(string))
			}
			return 500, nil
		}
	}
	bobOnly := func(string) (int, any) {
		return 200, connection("teamworkGraph_teamUsers", "", userEdge(teamA, "bob-2"))
	}
	aliceAndBob := func(string) (int, any) {
		return 200, connection("teamworkGraph_teamUsers", "", userEdge(teamA, "alice-1"), userEdge(teamA, "bob-2"))
	}
	for _, c := range []struct {
		name                        string
		respond                     func(request) (int, any)
		memberships, links, deactiv int
	}{
		{"the control: team C is gone, alice-1 and the link are not on the re-reads",
			shrunk(bobOnly, teamIs("ACTIVE", uuidC)), 2, 1, 1},
		{"the gateway says team C still exists and the re-read still holds alice-1: only the link closes",
			shrunk(aliceAndBob, teamIs("ACTIVE")), 0, 1, 0},
		{"the gateway answers an error to every question: nothing closes",
			shrunk(bobOnly, teamIs("")), 0, 0, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			conn := openClickHouse(t)
			ctx := context.Background()
			g := newGateway(t, standard)
			rows, err := Collect(ctx, g.client(), func() Params { p := params(everything); p.Now = first; return p }())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Write(ctx, conn, "org-1", rows, everything, soleScope(), everyAbsentFactIsGone{}); err != nil {
				t.Fatal(err)
			}
			g.respond = c.respond
			p := params(everything)
			p.Now = first.Add(time.Hour)
			rows, err = Collect(ctx, g.client(), p)
			if err != nil {
				t.Fatal(err)
			}
			prover := &GatewayAbsence{Reads: g.client(), SiteID: "site-uuid"}
			result, err := Write(ctx, conn, "org-1", rows, everything, soleScope(), prover)
			if err != nil {
				t.Fatal(err)
			}
			if result.ExpiredMemberships != c.memberships || result.ExpiredOwnership != c.links || result.DeactivatedTeams != c.deactiv {
				t.Errorf("closed %d memberships, %d links, deactivated %d teams; want %d, %d, %d",
					result.ExpiredMemberships, result.ExpiredOwnership, result.DeactivatedTeams, c.memberships, c.links, c.deactiv)
			}
		})
	}
	// No prover: nothing closes on the end of a walk (red on main, which closed all three).
	conn := openClickHouse(t)
	ctx := context.Background()
	g := newGateway(t, standard)
	p := params(everything)
	p.Now = first
	rows, err := Collect(ctx, g.client(), p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Write(ctx, conn, "org-1", rows, everything, soleScope(), everyAbsentFactIsGone{}); err != nil {
		t.Fatal(err)
	}
	g.respond = shrunk(bobOnly, teamIs("ACTIVE", uuidC))
	p.Now = first.Add(time.Hour)
	if rows, err = Collect(ctx, g.client(), p); err != nil {
		t.Fatal(err)
	}
	result, err := Write(ctx, conn, "org-1", rows, everything, soleScope(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.ExpiredMemberships != 0 || result.ExpiredOwnership != 0 || result.DeactivatedTeams != 0 {
		t.Errorf("with no prover the run closed %+v, want nothing: the end of a cursor walk is no proof", result)
	}
}
