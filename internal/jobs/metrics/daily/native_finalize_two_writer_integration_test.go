//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The CHAOS-4290 two-writer reachability proof, against a real ClickHouse.
//
// The mechanism's unit tests show the skip list REACHES the bridge. They
// cannot show why that matters, because the consequence is a property of the
// STORE: user_metrics_daily is append-only and read back through
// `ORDER BY computed_at DESC LIMIT 1 BY (org_id, repo_id, author_email, day)`.
// Two writers therefore never collide or error -- the later one simply wins,
// silently. A natively computed family that the bridge then recomputed would
// be correct, green, and invisibly superseded in production.
//
// So this test asserts the property that actually protects the feature: after
// a finalize where a native family ran, the row a reader gets back is the
// NATIVE one. The mutation leg (documented at the bottom) removes the skip so
// the bridge writes too, and the read flips to the bridge's row -- which is
// the failure this whole PR exists to prevent.
//
// A fake ClickHouse cannot show this: the collapse is performed by the engine
// at read time, not by any code under test.

const twoWriterDDL = `CREATE TABLE user_metrics_daily (
    repo_id UUID, day Date, author_email String, identity_id String,
    team_id String, loc_touched UInt32, delivery_units UInt32,
    computed_at DateTime, org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, author_email, day)`

// dedupRead mirrors clickhouse_dedup.dedup_from("user_metrics_daily") exactly
// -- the natural key comes from _APPEND_ONLY_DAILY_KEYS, read from the current
// helper rather than from a migration comment.
const dedupRead = `SELECT identity_id FROM (
    SELECT * FROM user_metrics_daily
    ORDER BY computed_at DESC
    LIMIT 1 BY org_id, repo_id, author_email, day
) WHERE org_id = ? AND author_email = ?`

type chWritingFinalizeFamily struct {
	conn  clickhousestore.Conn
	orgID string
	when  time.Time
}

func (family *chWritingFinalizeFamily) ComputeFinalizeFamily(ctx context.Context, _ Run) (int, error) {
	err := family.conn.Exec(ctx, `INSERT INTO user_metrics_daily
        (repo_id, day, author_email, identity_id, team_id, loc_touched, delivery_units, computed_at, org_id)
        VALUES (generateUUIDv4(), '2026-09-04', 'ic@example.com', 'NATIVE', 't', 1, 1, ?, ?)`,
		family.when, family.orgID)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// chWritingBridge stands in for the Python compatibility bridge. It honours
// skipFamilies exactly as the real bridge now does (worker_metrics.py's
// validator plus run_daily_metrics_finalize's gate) and, when NOT skipped,
// writes its own row with a LATER computed_at -- which is what a real Python
// finalize running after the native family would produce.
type chWritingBridge struct {
	fakeCompatibility
	conn  clickhousestore.Conn
	orgID string
	when  time.Time
	wrote bool
}

func (bridge *chWritingBridge) Finalize(ctx context.Context, _ Run, skipFamilies []string) error {
	for _, name := range skipFamilies {
		if name == "ic_finalize" {
			return nil
		}
	}
	bridge.wrote = true
	return bridge.conn.Exec(ctx, `INSERT INTO user_metrics_daily
        (repo_id, day, author_email, identity_id, team_id, loc_touched, delivery_units, computed_at, org_id)
        VALUES (generateUUIDv4(), '2026-09-04', 'ic@example.com', 'BRIDGE', 't', 9, 9, ?, ?)`,
		bridge.when, bridge.orgID)
}

func TestNativeFinalizeFamilySurvivesTheBridgeReadback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, twoWriterDDL); err != nil {
		t.Fatal(err)
	}

	const orgID = "00000000-0000-4000-8000-000000000900"
	native := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	// The bridge writes LATER, which is the whole hazard: on this table a
	// later computed_at wins the readback outright.
	bridgeAt := native.Add(time.Minute)

	store := finalizeStoreWithClaim()
	bridge := &chWritingBridge{conn: conn, orgID: orgID, when: bridgeAt}
	handler, err := NewFinalizeHandler(store, bridge)
	if err != nil {
		t.Fatal(err)
	}
	handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalize": &chWritingFinalizeFamily{conn: conn, orgID: orgID, when: native},
	})

	if err := handler.Work(ctx, finalizeExecutionFor(testRunID)); err != nil {
		t.Fatalf("Work = %v, want success", err)
	}

	if bridge.wrote {
		t.Fatal("the bridge wrote despite ic_finalize being skipped -- its row " +
			"would supersede the native one on the next read")
	}

	var winner string
	if err := conn.QueryRow(ctx, dedupRead, orgID, "ic@example.com").Scan(&winner); err != nil {
		t.Fatal(err)
	}
	if winner != "NATIVE" {
		t.Fatalf("dedup readback returned %q, want NATIVE -- the natively "+
			"computed row was superseded, which is exactly the silent failure "+
			"this mechanism exists to prevent", winner)
	}

	// Control: prove the readback CAN return BRIDGE, so the assertion above is
	// discriminating rather than reading a table only one writer ever touched.
	// Without this, a test against a store where the bridge could never write
	// would pass for the wrong reason.
	if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
        (repo_id, day, author_email, identity_id, team_id, loc_touched, delivery_units, computed_at, org_id)
        VALUES (generateUUIDv4(), '2026-09-04', 'ic@example.com', 'BRIDGE', 't', 9, 9, ?, ?)`,
		bridgeAt, orgID); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, dedupRead, orgID, "ic@example.com").Scan(&winner); err != nil {
		t.Fatal(err)
	}
	if winner != "BRIDGE" {
		t.Fatalf("control: a later-computed_at row did NOT win the readback (got %q) -- "+
			"the dedup form under test is not behaving as the production reader does, "+
			"so the main assertion proves nothing", winner)
	}
}

// MUTATION M22 (run on bigboy, not committed): in
// computeNativeFinalizeFamilies, drop the append so the skip list is always
// empty. The bridge then writes its later row, bridge.wrote becomes true, and
// the readback returns BRIDGE -- the test fails on the first assertion. That
// is the production failure reproduced: a correct native family, silently
// overwritten, with nothing erroring.
