//go:build integration

package workgraph

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

// TestBatchResolveMembership_TupledMatchExcludesCrossProductRows is
// CHAOS-4655's red-first proof against a REAL ClickHouse engine (not a
// fake client, and not merely a changed SQL string): seeds
// work_unit_membership with the two endpoints an edge actually requests
// PLUS two "phantom" rows that satisfy an INDEPENDENT node_type-IN /
// node_id-IN filter (each phantom's type is in the request's type-set and
// its id is in the request's id-set) while NOT themselves being a
// requested (node_type, node_id) pair -- the exact cross-product shape
// codex's engine-level repro on CHAOS-4647's PR demonstrated (100k edges /
// 200k ids / 3 node types -> ClickHouse Code 396, max_result_rows
// exceeded).
//
// The pair-exactness `concat(hex(node_type),':',hex(node_id)) IN
// {node_pairs:...}` filter this fix adds must exclude both phantoms even
// though the kept node_type-only IN prefilter (retained for primary-key
// index pruning -- see batchResolveMembership's doc comment) still matches
// them; the independent-IN-ONLY shape this query had before CHAOS-4655
// matches them all the way through to the result.
//
// RED ON ORIGIN/MAIN (512c4e77b): with membership.go and membership_test.go
// reverted to that commit (this file left in place -- membershipKey,
// edgeEndpoint, newFilterScope and batchResolveMembership's signature are
// all unchanged by the fix, so this file compiles against either shape),
// this test FAILS: the result carries 4 entries (both phantoms included)
// instead of 2. Restoring the fix turns it green. See the PR's
// TEST-EVIDENCE for the exact commands run to produce that red, and
// membershipPairsLiteral's and TestBatchResolveMembership_QueriesAPairBoundMatch's
// (membership_test.go) fake-client test for the corresponding SQL-shape
// regression guard that runs in the ordinary unit gate.
func TestBatchResolveMembership_TupledMatchExcludesCrossProductRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const orgID = "org-4655"
	const runID = "run-4655"
	now := time.Now().UTC()

	// Marker row FIRST, matching this table's write protocol
	// (047_work_unit_membership_run_id.sql: a run is visible to readers
	// only once its completion marker exists) -- not load-bearing for
	// this single-run fixture, but writing it in protocol order keeps the
	// seed honest about what a real materializer write looks like.
	if err := admin.Exec(ctx, `
        INSERT INTO work_unit_membership_runs (org_id, run_id, completed_at)
        VALUES (?, ?, ?)
    `, orgID, runID, now); err != nil {
		t.Fatalf("seed work_unit_membership_runs: %v", err)
	}

	type seedRow struct {
		nodeType, nodeID, category string
	}
	// requested-1/requested-2 are the edge's real endpoints. phantom-1
	// (issue, ep-2) and phantom-2 (pr, ep-1) are CROSS-PRODUCT rows: their
	// node_type is in {issue, pr} and their node_id is in {ep-1, ep-2} --
	// exactly what an independent IN/IN filter matches -- but neither
	// pair was ever requested by the edge.
	seeds := []seedRow{
		{"issue", "ep-1", "requested-1"},
		{"pr", "ep-2", "requested-2"},
		{"issue", "ep-2", "phantom-1"},
		{"pr", "ep-1", "phantom-2"},
	}
	batch, err := admin.PrepareBatch(ctx, `
        INSERT INTO work_unit_membership (
            org_id, node_type, node_id, work_unit_id, category_kind, category,
            weight, is_dominant, categorization_status, computed_at, run_id
        )
    `)
	if err != nil {
		t.Fatalf("prepare membership batch: %v", err)
	}
	for _, seed := range seeds {
		if err := batch.Append(
			orgID, seed.nodeType, seed.nodeID, "wu-"+seed.category, "theme", seed.category,
			1.0, uint8(1), "ok", now, runID,
		); err != nil {
			t.Fatalf("append membership row %+v: %v", seed, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send membership batch: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	edgeRows := []edgeEndpoint{{sourceType: "issue", sourceID: "ep-1", targetType: "pr", targetID: "ep-2"}}
	result, err := batchResolveMembership(ctx, client, orgID, edgeRows, newFilterScope(nil, nil))
	if err != nil {
		t.Fatalf("batchResolveMembership: %v", err)
	}

	if _, ok := result[membershipKey{nodeType: "issue", nodeID: "ep-2"}]; ok {
		t.Fatalf("phantom cross-product row (issue, ep-2) leaked into the result -- "+
			"independent-IN shape, not a tupled match: %+v", result)
	}
	if _, ok := result[membershipKey{nodeType: "pr", nodeID: "ep-1"}]; ok {
		t.Fatalf("phantom cross-product row (pr, ep-1) leaked into the result -- "+
			"independent-IN shape, not a tupled match: %+v", result)
	}
	if len(result) != 2 {
		t.Fatalf("got %d membership entries, want exactly the 2 requested endpoints: %+v", len(result), result)
	}
	if got := result[membershipKey{nodeType: "issue", nodeID: "ep-1"}].dominantTheme; got != "requested-1" {
		t.Fatalf("issue/ep-1 dominantTheme = %q, want %q", got, "requested-1")
	}
	if got := result[membershipKey{nodeType: "pr", nodeID: "ep-2"}].dominantTheme; got != "requested-2" {
		t.Fatalf("pr/ep-2 dominantTheme = %q, want %q", got, "requested-2")
	}
}

// TestBatchResolveMembership_RoundTripsHostileStringsThroughTheRealEngine
// is team-lead's required proof (CHAOS-4655 review, 2026-09-01): the
// node_pairs encoding must be verified against the REAL ClickHouse
// parameter parser for adversarial node_id content, not assumed correct by
// analogy to dev-health-go's clickHouseStringArray. That analogy is
// exactly what broke, three separate times, all reproduced directly
// against a real engine:
//  1. A quoted `Array(Tuple(String,String))` literal escaping a quote as
//     `\'` (clickHouseStringArray's own convention) -- REJECTED outright
//     (`Cannot parse escape sequence`), for String, Array(String), AND
//     Array(Tuple(...)) parameters alike. This is CHAOS-4745: a LIVE bug
//     in dev-health-go's own Array(String) binding helper, not specific to
//     this query.
//  2. SQL-standard doubled-quote escaping (two single quotes in a row)
//     fixed that, but broke on
//     a literal backslash directly adjacent to a doubled quote -- `Cannot
//     parse input`/`Cannot read array from text`, for Array(String) AND
//     Array(Tuple(...)) alike.
//  3. ClickHouse `\x`-hex-byte escapes (`\x27`, `\x5c`) -- no ambiguous
//     adjacency, but STILL broken for an Array(String) ELEMENT
//     specifically: `\x27` alone breaks the same case (2) broke, even
//     though it works fine for a bare top-level String parameter; `\x5c`
//     SILENTLY DROPS the escaped backslash byte inside an array element
//     (no error, wrong data -- a worse class than an outright rejection).
//
// No hand-rolled escaping scheme survives ClickHouse's native-protocol
// Array(String)/Array(Tuple) PARAMETER grammar for arbitrary content.
// membershipPairsLiteral sidesteps string escaping entirely instead:
// hex(nodeType)+":"+hex(nodeID), a character set ClickHouse's
// string-literal grammar never treats specially, so no escaping logic
// exists to get wrong. This is also why node_id carries NO sargable
// prefilter (unlike node_type, which is validated against a closed enum
// and therefore safe to bind normally) -- see batchResolveMembership's
// doc comment.
//
// CLI-VS-DRIVER TRAP (recorded here so it isn't repeated): `clickhouse-client
// --param_x=...` decodes `\x`/backslash escapes CLIENT-SIDE before they
// reach the wire, so probing escaping via the CLI is NOT a valid proxy for
// `stdclickhouse.WithParameters` (the native-protocol mechanism
// dev-health-go's Client actually uses) -- two of the three failures above
// were found ONLY by testing through the real driver in this file, after
// CLI probes gave false confidence.
//
// This test exercises the FULL batchResolveMembership path (not a
// hand-rolled probe query) against a real ClickHouse container seeded via
// chschema's real migration chain, with adversarial node_id content: single
// quote, backslash, comma, parentheses, non-ASCII (incl. a 4-byte emoji),
// and backslash directly adjacent to a quote -- node_type stays a real
// valid value (issue/pr) throughout, since an invalid one is now rejected
// before the query ever runs (TestBatchResolveMembership_RejectsUnknownNodeType,
// membership_test.go). Alongside each hostile pair, a DECOY row differing
// by exactly one character is seeded too (e.g. missing byte, swapped
// quote/backslash) -- if hex+concat matching were ambiguous (e.g. the ':'
// separator were reachable inside a hex block, or encode/decode
// disagreed), a decoy would leak into the result for a hostile key it does
// not belong to, or a hostile key would resolve to the wrong category.
func TestBatchResolveMembership_RoundTripsHostileStringsThroughTheRealEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const orgID = "org-4655-hostile"
	const runID = "run-4655-hostile"
	now := time.Now().UTC()
	if err := admin.Exec(ctx, `
        INSERT INTO work_unit_membership_runs (org_id, run_id, completed_at)
        VALUES (?, ?, ?)
    `, orgID, runID, now); err != nil {
		t.Fatalf("seed work_unit_membership_runs: %v", err)
	}

	type keyedRow struct {
		key      membershipKey
		category string // "" for a decoy: seeded, but must never be looked up
	}
	rows := []keyedRow{
		{key: membershipKey{nodeType: "issue", nodeID: "a'b"}, category: "quote"},
		{key: membershipKey{nodeType: "issue", nodeID: "a"}, category: ""}, // decoy: truncated at the quote
		{key: membershipKey{nodeType: "pr", nodeID: `back\slash`}, category: "backslash"},
		{key: membershipKey{nodeType: "issue", nodeID: "id,with,commas"}, category: "commas"},
		{key: membershipKey{nodeType: "pr", nodeID: "id(with)parens"}, category: "parens"},
		{key: membershipKey{nodeType: "issue", nodeID: "日本語-\U0001F4A1"}, category: "unicode"},
		{key: membershipKey{nodeType: "pr", nodeID: `x\'both`}, category: "backslash-quote"},
		{key: membershipKey{nodeType: "pr", nodeID: `x'both`}, category: ""}, // decoy: missing the backslash
		{key: membershipKey{nodeType: "issue", nodeID: "decoy"}, category: ""},
	}

	batch, err := admin.PrepareBatch(ctx, `
        INSERT INTO work_unit_membership (
            org_id, node_type, node_id, work_unit_id, category_kind, category,
            weight, is_dominant, categorization_status, computed_at, run_id
        )
    `)
	if err != nil {
		t.Fatalf("prepare membership batch: %v", err)
	}
	for i, row := range rows {
		category := row.category
		if category == "" {
			category = fmt.Sprintf("decoy-%d", i)
		}
		if err := batch.Append(
			orgID, row.key.nodeType, row.key.nodeID, fmt.Sprintf("wu-%d", i), "theme", category,
			1.0, uint8(1), "ok", now, runID,
		); err != nil {
			t.Fatalf("append row %+v: %v", row, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send membership batch: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Request only the hostile keys (never the decoys) -- pack them as
	// edgeEndpoint source/target pairs, wrapping around if the count is
	// odd.
	var hostile []membershipKey
	for _, row := range rows {
		if row.category != "" {
			hostile = append(hostile, row.key)
		}
	}
	var edgeRows []edgeEndpoint
	for i := 0; i < len(hostile); i += 2 {
		j := i + 1
		if j >= len(hostile) {
			j = 0 // odd count: pair the last one with the first again (harmless duplicate endpoint)
		}
		edgeRows = append(edgeRows, edgeEndpoint{
			sourceType: hostile[i].nodeType, sourceID: hostile[i].nodeID,
			targetType: hostile[j].nodeType, targetID: hostile[j].nodeID,
		})
	}

	result, err := batchResolveMembership(ctx, client, orgID, edgeRows, newFilterScope(nil, nil))
	if err != nil {
		t.Fatalf("batchResolveMembership: %v", err)
	}

	if len(result) != len(hostile) {
		t.Fatalf("got %d resolved endpoints, want exactly the %d hostile keys (no extra/fewer): %+v", len(result), len(hostile), result)
	}
	for _, row := range rows {
		entry, ok := result[row.key]
		if row.category == "" {
			if ok {
				t.Fatalf("decoy %+v leaked into the result as %+v -- hex+concat matching is ambiguous", row.key, entry)
			}
			continue
		}
		if !ok {
			t.Fatalf("hostile key %+v did not round-trip -- missing from result %+v", row.key, result)
		}
		if entry.dominantTheme != row.category {
			t.Fatalf("hostile key %+v resolved to theme %q, want %q -- hex+concat matched the wrong row", row.key, entry.dominantTheme, row.category)
		}
	}
}
