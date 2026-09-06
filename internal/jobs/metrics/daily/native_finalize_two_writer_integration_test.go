//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The CHAOS-4290 two-writer reachability proof, against a real ClickHouse --
// HISTORY, kept for context. It used to prove that a natively-computed
// finalize family survived a SECOND write from the Python compatibility
// bridge on the same append-only, computed_at-deduped table
// (user_metrics_daily). CHAOS-3092 (PR-A') deleted that bridge call entirely
// -- there is no second writer left AT ALL, so the race this test defended
// against is now closed by construction rather than by a skip list reaching
// a still-live bridge. What remains worth proving against a real ClickHouse
// is narrower: the native family's own write actually lands and reads back
// correctly through Work's real lease/claim/complete path -- a fake
// ClickHouse cannot show that a real MergeTree/ORDER BY/dedup read behaves
// as expected, even with only one writer.

const twoWriterDDL = `CREATE TABLE user_metrics_daily (
    repo_id UUID, day Date, author_email String, identity_id String,
    team_id String, loc_touched UInt32, delivery_units UInt32,
    computed_at DateTime, org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, author_email, day)`

// twoWriterRepoID is FIXED and SHARED by both writers, and that is the whole
// point of this test rather than an incidental detail.
//
// Both writers previously inserted generateUUIDv4(). repo_id is IN the dedup key
// below, so two random ids are two DIFFERENT keys: the rows could not supersede
// one another, and the readback was choosing between two surviving rows by
// arbitrary ordering rather than resolving one key. The main assertion passed
// because only one row existed, and the "control" passed by ordering luck --
// neither was measuring supersession, which is the only thing this test claims.
//
// One shared id puts both writers on ONE key, so LIMIT 1 BY genuinely resolves
// them and the later computed_at genuinely wins. Found by codex r1 on #2241
// (Finding 4); the same uuid4-in-a-sorting-key mechanism this lane reported
// against compute_ic.py, missed here one file away.
const twoWriterRepoID = "3f6b1c04-9a27-4d55-8e13-71b0c2a4d9e6"

// dedupRead mirrors clickhouse_dedup.dedup_from("user_metrics_daily") exactly
// -- the natural key comes from _APPEND_ONLY_DAILY_KEYS, read from the current
// helper rather than from a migration comment.
const dedupRead = `SELECT identity_id FROM (
    SELECT * FROM user_metrics_daily
    ORDER BY computed_at DESC
    LIMIT 1 BY org_id, repo_id, author_email, day
) WHERE org_id = ? AND author_email = ?`

type chWritingFinalizeFamily struct {
	conn  driver.Conn
	orgID string
	when  time.Time
}

func (family *chWritingFinalizeFamily) ComputeFinalizeFamily(ctx context.Context, _ Run) (int, error) {
	err := family.conn.Exec(ctx, `INSERT INTO user_metrics_daily
        (repo_id, day, author_email, identity_id, team_id, loc_touched, delivery_units, computed_at, org_id)
        VALUES (toUUID(?), '2026-09-04', 'ic@example.com', 'NATIVE', 't', 1, 1, ?, ?)`,
		twoWriterRepoID, family.when, family.orgID)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// CHAOS-3092 (PR-A'): TestNativeFinalizeFamilySurvivesTheBridgeReadback used
// to live here, proving a native family's row survived a second, later
// write from chWritingBridge (a Python-compatibility-bridge stand-in). That
// bridge call is deleted -- FinalizeHandler no longer takes a compatibility
// executor at all -- so there is no second writer to prove survival
// against. TestNativeFinalizeFamilyWriteIsReadable below proves the
// narrower, still-real thing: Work's real lease/claim/complete path
// actually persists the native family's row through a real ClickHouse
// MergeTree/dedup read, with the sole writer being the native executor.
func TestNativeFinalizeFamilyWriteIsReadable(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

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

	store := finalizeStoreWithClaim()
	handler, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalize": &chWritingFinalizeFamily{conn: conn, orgID: orgID, when: native},
	}); err != nil {
		t.Fatal(err)
	}

	if err := handler.Work(ctx, finalizeExecutionFor(testRunID)); err != nil {
		t.Fatalf("Work = %v, want success", err)
	}

	var winner string
	if err := conn.QueryRow(ctx, dedupRead, orgID, "ic@example.com").Scan(&winner); err != nil {
		t.Fatal(err)
	}
	if winner != "NATIVE" {
		t.Fatalf("dedup readback returned %q, want NATIVE -- Work's real "+
			"lease/claim/complete path did not persist the native family's row",
			winner)
	}
}
