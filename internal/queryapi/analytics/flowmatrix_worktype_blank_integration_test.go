//go:build integration

package analytics

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// jsTrimSet is written out from ECMA-262 (section "White Space": TAB, VT, FF, SP,
// NBSP, ZWNBSP, any Zs; section "Line Terminators": LF, CR, LS, PS), the set
// String.prototype.trim removes. It is NOT derived from the SQL pattern.
var jsTrimSet = []struct {
	name string
	r    rune
}{
	{"TAB", 0x0009}, {"LF", 0x000A}, {"VT", 0x000B}, {"FF", 0x000C}, {"CR", 0x000D},
	{"SP", 0x0020}, {"NBSP", 0x00A0}, {"OGHAM", 0x1680},
	{"EN-QUAD", 0x2000}, {"EM-QUAD", 0x2001}, {"EN-SPACE", 0x2002}, {"EM-SPACE", 0x2003},
	{"3-PER-EM", 0x2004}, {"4-PER-EM", 0x2005}, {"6-PER-EM", 0x2006}, {"FIGURE", 0x2007},
	{"PUNCT", 0x2008}, {"THIN", 0x2009}, {"HAIR", 0x200A},
	{"LS", 0x2028}, {"PS", 0x2029}, {"NNBSP", 0x202F}, {"MMSP", 0x205F},
	{"IDEOGRAPHIC", 0x3000}, {"FEFF", 0xFEFF},
}

func TestFlowMatrixWorkType_BlankTypesAreFilteredLikeEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	const orgID = "chaos-14393-blank"
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

	types := map[string]string{"wi-empty": ""}
	for _, c := range jsTrimSet {
		types["wi-ws-"+c.name] = string(c.r)
		types["wi-ws2-"+c.name] = string(c.r) + string(c.r)
	}
	types["wi-mixed"] = " \t\r\n\u00a0\u3000" + string(rune(0xFEFF)) + " "
	keep := map[string]string{
		"wi-zwsp":   "\u200b",
		"wi-a":      "a",
		"wi-padded": " a ",
	}
	for id, v := range keep {
		types[id] = v
	}
	seedWorkItems(t, ctx, conn, orgID, types)
	rows := make([]seededCycleTimeRow, 0, len(types))
	for id := range types {
		rows = append(rows, seededCycleTimeRow{workItemID: id, day: flowMatrixSeedLiveDay, computedAt: "2026-09-07 01:00:00", workType: "x"})
	}
	seedCycleTimeRowsEachInItsOwnPart(t, ctx, conn, orgID, rows)

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer func() { _ = client.Close() }()

	nq, eq, err := CompileFlowMatrix(workTypeFlowMatrixRequest(), orgID, 30, nil)
	if err != nil {
		t.Fatalf("CompileFlowMatrix: %v", err)
	}
	nodes, edges, err := ExecuteFlowMatrix(ctx, client, nq, eq)
	if err != nil {
		t.Fatalf("ExecuteFlowMatrix: %v", err)
	}

	want := map[string]bool{}
	for _, v := range keep {
		want["WORK_TYPE:"+v] = true
	}
	for _, n := range nodes {
		if !want[n.ID] {
			t.Errorf("unexpected WORK_TYPE node %q (a whitespace-only or empty type leaked)", n.ID)
		}
		delete(want, n.ID)
	}
	for id := range want {
		t.Errorf("missing WORK_TYPE node %q (a real type was dropped)", id)
	}
	kept := map[string]bool{}
	for _, v := range keep {
		kept["WORK_TYPE:"+v] = true
	}
	for _, e := range edges {
		if !kept[e.Source] || !kept[e.Target] {
			t.Errorf("edge leaks a non-kept type: %q -> %q", e.Source, e.Target)
		}
	}
	if len(edges) != 6 {
		t.Errorf("len(edges) = %d, want 6 (3 kept types, all ordered pairs): %s", len(edges), fmt.Sprint(edges))
	}
}
