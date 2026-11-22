//go:build integration

package analytics

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// teamKeyBlankSet is written out from ECMA-262 (section "White Space": TAB, VT, FF, SP, NBSP, ZWNBSP, any Zs; section
// "Line Terminators": LF, CR, LS, PS), the set String.prototype.trim removes. It is NOT derived from the SQL pattern.
var teamKeyBlankSet = []rune{
	0x0009, 0x000A, 0x000B, 0x000C, 0x000D, 0x0020, 0x00A0, 0x1680,
	0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200A,
	0x2028, 0x2029, 0x202F, 0x205F, 0x3000, 0xFEFF,
}

func TestFlowMatrixTeamKey_BlankTeamIDsAreFilteredLikeEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	const org = "chaos-14395-blank"
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	chschema.Apply(ctx, t, inst)

	day := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	repoBlankX, repoBlankY, repoP, repoQ := uuid.New(), uuid.New(), uuid.New(), uuid.New()

	blank := []string{""}
	for _, r := range teamKeyBlankSet {
		blank = append(blank, string(r))
	}
	blank = append(blank, " \t\r\n\u00a0\u3000"+string(rune(0xFEFF))+" ")
	keep := []string{"a", " a ", "\u200b"}

	n := 0
	seed := func(teamID string, repo uuid.UUID) {
		n++
		id := fmt.Sprintf("wi-%d", n)
		if err := conn.Exec(ctx, `
INSERT INTO work_item_cycle_times
    (work_item_id, provider, day, work_scope_id, type, status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours, computed_at, org_id)
VALUES (?, 'github', ?, 'scope-a', 'story', 'done', ?, ?, ?, 5, 6, ?, ?)`,
			id, day, day, day, day.Add(time.Hour), day.Add(2*time.Hour), org); err != nil {
			t.Fatalf("seed cycle time: %v", err)
		}
		if err := conn.Exec(ctx, `
INSERT INTO work_items (repo_id, work_item_id, provider, title, type, status, status_raw, project_key, project_id, created_at, updated_at, last_synced, org_id)
VALUES (?, ?, 'github', 't', 'story', 'done', 'done', 'k', 'k', ?, ?, ?, ?)`,
			repo, id, day, day, day, org); err != nil {
			t.Fatalf("seed work item: %v", err)
		}
		if err := conn.Exec(ctx, `
INSERT INTO work_item_team_attributions
    (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at)
VALUES (?, ?, ?, 'github', ?, 'n', 'native_team', 1, 'high', '', ?)`,
			org, repo, id, teamID, day.Add(3*time.Hour)); err != nil {
			t.Fatalf("seed attribution: %v", err)
		}
	}
	for _, tv := range blank {
		seed(tv, repoBlankX)
		seed(tv, repoBlankY)
	}
	for _, tv := range keep {
		seed(tv, repoP)
		seed(tv, repoQ)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer func() { _ = client.Close() }()
	nodesFor := func(dim Dimension) ([]model.SankeyNode, []model.SankeyEdge) {
		nq, eq, err := CompileFlowMatrix(FlowMatrixRequest{
			Dimension: dim, Measure: MeasureCount,
			StartDate: mustGraphQLDate("2026-09-01"), EndDate: mustGraphQLDate("2026-09-08"),
			MaxNodes: 200, MaxEdges: 2000,
		}, org, 30, nil)
		if err != nil {
			t.Fatalf("CompileFlowMatrix %s: %v", dim, err)
		}
		nodes, edges, err := ExecuteFlowMatrix(ctx, client, nq, eq)
		if err != nil {
			t.Fatalf("ExecuteFlowMatrix %s: %v", dim, err)
		}
		return nodes, edges
	}

	teamNodes, teamEdges := nodesFor(DimensionTeam)
	want := map[string]bool{}
	for _, k := range keep {
		want["TEAM:"+k] = true
	}
	for _, nd := range teamNodes {
		if !want[nd.ID] {
			t.Errorf("TEAM node %q leaked (blank or empty team id)", nd.ID)
		}
		delete(want, nd.ID)
	}
	for id := range want {
		t.Errorf("TEAM node %q missing (a real team id was dropped)", id)
	}
	for _, e := range teamEdges {
		if !strings.HasPrefix(e.Source, "TEAM:") || strings.TrimSpace(e.Source) == "TEAM:" || e.Source == "TEAM:" || e.Target == "TEAM:" {
			t.Errorf("TEAM edge leaks %q -> %q", e.Source, e.Target)
		}
	}
	for _, e := range teamEdges {
		if !(e.Source == "TEAM:a" || e.Source == "TEAM: a " || e.Source == "TEAM:\u200b") ||
			!(e.Target == "TEAM:a" || e.Target == "TEAM: a " || e.Target == "TEAM:\u200b") {
			t.Errorf("TEAM edge leaks a non-kept team: %q -> %q", e.Source, e.Target)
		}
	}
	if len(teamEdges) != 6 {
		t.Errorf("TEAM edges = %d, want 6 (3 kept teams, ordered pairs)", len(teamEdges))
	}

	_, repoEdges := nodesFor(DimensionRepo)
	pq, qp := "REPO:"+repoP.String()+">REPO:"+repoQ.String(), "REPO:"+repoQ.String()+">REPO:"+repoP.String()
	got := map[string]bool{}
	for _, e := range repoEdges {
		got[e.Source+">"+e.Target] = true
	}
	if !got[pq] || !got[qp] {
		t.Errorf("REPO edges missing the kept-team bridge P<->Q: %v", got)
	}
	for _, e := range repoEdges {
		if strings.Contains(e.Source, repoBlankX.String()) || strings.Contains(e.Source, repoBlankY.String()) ||
			strings.Contains(e.Target, repoBlankX.String()) || strings.Contains(e.Target, repoBlankY.String()) {
			t.Errorf("REPO edge bridged through a blank team id: %q -> %q", e.Source, e.Target)
		}
	}
}
