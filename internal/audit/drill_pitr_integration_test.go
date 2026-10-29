//go:build integration

package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// TestDrillAuthPITR is the driver of scripts/drill-auth-pitr.sh (backup,
// restore and point-in-time recovery of the auth schema). It does nothing
// unless DRILL_PHASE is set, and it connects to the database the script
// names in DRILL_DSN: it never starts a container. Credentials come from the
// environment at run time only.
//
// Phases: migrate | write | snapshot | verify | post.
const (
	drillOrgs        = 6
	drillCommitsEach = 3
	drillCreds       = 4
)

type drillSnapshot struct {
	Label     string            `json:"label"`
	Revisions map[string]int64  `json:"revisions"`
	Revoked   []string          `json:"revoked_credentials"`
	Keys      []string          `json:"outbox_idempotency_keys"`
	KeyDigest map[string]string `json:"outbox_key_content_digest"`
	Counts    map[string]int64  `json:"row_counts"`
	Digest    map[string]string `json:"digest"`
}

func TestDrillAuthPITR(t *testing.T) {
	phase := os.Getenv("DRILL_PHASE")
	if phase == "" {
		t.Skip("DRILL_PHASE not set: this is the drill driver, run by scripts/drill-auth-pitr.sh")
	}
	dsn := os.Getenv("DRILL_DSN")
	dir := os.Getenv("DRILL_EVIDENCE_DIR")
	if dsn == "" || dir == "" {
		t.Fatal("DRILL_DSN and DRILL_EVIDENCE_DIR are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	defer admin.Close()
	role := os.Getenv("DRILL_RUNTIME_ROLE")
	if role == "" {
		role = "drill_runtime"
	}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}

	switch phase {
	case "migrate":
		drillMigrate(t, ctx, admin, role)
	case "write":
		drillWrite(t, ctx, drillRuntimePool(t, ctx, admin, role), schema, admin, os.Getenv("DRILL_LABEL"))
	case "snapshot":
		drillSnapshotPhase(t, ctx, admin, dir, os.Getenv("DRILL_LABEL"))
	case "verify":
		drillVerify(t, ctx, admin, dir, os.Getenv("DRILL_EXPECT"), os.Getenv("DRILL_MODE"))
	case "post":
		drillPost(t, ctx, drillRuntimePool(t, ctx, admin, role), schema, admin, dir, os.Getenv("DRILL_EXPECT"), os.Getenv("DRILL_FINAL"))
	default:
		t.Fatalf("unknown DRILL_PHASE %q", phase)
	}
}

var drillPasswordShape = regexp.MustCompile(`^[A-Za-z0-9]{16,128}$`)

func drillMigrate(t *testing.T, ctx context.Context, admin *pgxpool.Pool, role string) {
	t.Helper()
	password := os.Getenv("DRILL_RUNTIME_PASSWORD")
	if !drillPasswordShape.MatchString(password) {
		t.Fatal("DRILL_RUNTIME_PASSWORD must be 16-128 alphanumeric characters")
	}
	if !regexp.MustCompile(`^[a-z_][a-z0-9_]{0,30}$`).MatchString(role) {
		t.Fatalf("bad runtime role name %q", role)
	}
	if os.Getenv("DRILL_FENCE") == "1" {
		// The restore fence carries the legacy public tables as well (refresh tokens, invites, reset tokens, ...): the
		// fenced drill runs on the full migrated schema, as production does, or apply refuses on the missing tables.
		pgschema.ApplyURI(ctx, t, admin.Config().ConnString())
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatalf("create runtime role: %v", err)
	}
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		t.Fatalf("authschema.Apply: %v", err)
	}
}

func drillRuntimePool(t *testing.T, ctx context.Context, admin *pgxpool.Pool, role string) *pgxpool.Pool {
	t.Helper()
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = os.Getenv("DRILL_RUNTIME_PASSWORD")
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("runtime pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func drillOrgID(i int) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("drill-11232-org-%d", i)))
}

func drillOrgUpdate(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, i int, name string) error {
	id := drillOrgID(i)
	return Commit(ctx, pool, schema, Mutation{
		Apply: func(ctx context.Context, tx TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE auth.organizations SET name = $2 WHERE id = $1`, id, name)
			return err
		},
		Audit: AuditEvent{EventType: "organization.updated", Outcome: OutcomeAllowed},
		Events: []OutboxEvent{{AggregateType: "organization", AggregateID: "org_" + id.String(),
			EventType: "organization.updated"}},
	})
}

func drillWrite(t *testing.T, ctx context.Context, rt *pgxpool.Pool, schema authschema.ValidatedIdentifier, admin *pgxpool.Pool, label string) {
	t.Helper()
	if label == "" {
		t.Fatal("DRILL_LABEL is required")
	}
	principal := uuid.NewSHA1(uuid.NameSpaceURL, []byte("drill-11232-principal"))
	if _, err := admin.Exec(ctx,
		`INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'drill') ON CONFLICT (id) DO NOTHING`, principal); err != nil {
		t.Fatalf("seed principal: %v", err)
	}
	for i := 0; i < drillOrgs; i++ {
		id := drillOrgID(i)
		err := Commit(ctx, rt, schema, Mutation{
			Apply: func(ctx context.Context, tx TxOps) error {
				_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES ($1, $2, $2) ON CONFLICT (id) DO NOTHING`,
					id, "drill-"+id.String())
				return err
			},
			Audit: AuditEvent{EventType: "organization.created", Outcome: OutcomeAllowed},
			Events: []OutboxEvent{{AggregateType: "organization", AggregateID: "org_" + id.String(),
				EventType: "organization.updated"}},
		})
		if err != nil && label == "A" {
			t.Fatalf("create org %d: %v", i, err)
		}
		for j := 0; j < drillCommitsEach; j++ {
			if err := drillOrgUpdate(ctx, rt, schema, i, fmt.Sprintf("drill-%s-%d-%d", label, i, j)); err != nil {
				t.Fatalf("update org %d: %v", i, err)
			}
		}
	}
	for k := 0; k < drillCreds; k++ {
		cred := uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("drill-11232-cred-%s-%d", label, k)))
		err := Commit(ctx, rt, schema, Mutation{
			Apply: func(ctx context.Context, tx TxOps) error {
				_, err := tx.Exec(ctx, `INSERT INTO auth.credential_registry (id, principal_id, credential_class, secret_hash, hash_algorithm, fingerprint)
VALUES ($1, $2, 'workload_token', decode(md5(random()::text || clock_timestamp()::text), 'hex'), 'sha256', '0123456789abcdef')`,
					cred, principal)
				return err
			},
			Audit: AuditEvent{EventType: "credential.created", Outcome: OutcomeAllowed},
			Events: []OutboxEvent{{AggregateType: "organization", AggregateID: "org_" + drillOrgID(0).String(),
				EventType: "organization.updated"}},
		})
		if err != nil {
			t.Fatalf("create credential: %v", err)
		}
		if k%2 == 1 {
			continue
		}
		reason := "compromised"
		if k == 2 {
			reason = "owner_request"
		}
		err = Commit(ctx, rt, schema, Mutation{
			Apply: func(ctx context.Context, tx TxOps) error {
				_, err := tx.Exec(ctx, `UPDATE auth.credential_registry SET revoked_at = now(), revoked_reason = $2 WHERE id = $1`, cred, reason)
				return err
			},
			Audit: AuditEvent{EventType: "credential.revoked", Outcome: OutcomeAllowed},
			Events: []OutboxEvent{{AggregateType: "organization", AggregateID: "org_" + drillOrgID(1).String(),
				EventType: "organization.updated"}},
		})
		if err != nil {
			t.Fatalf("revoke credential: %v", err)
		}
	}
}

func drillRead(ctx context.Context, admin *pgxpool.Pool, label string) (drillSnapshot, error) {
	tx, err := admin.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return drillSnapshot{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	s := drillSnapshot{Label: label, Revisions: map[string]int64{}, Counts: map[string]int64{}, Digest: map[string]string{}}
	rows, err := tx.Query(ctx, `SELECT aggregate_type || ':' || aggregate_id, revision FROM auth.aggregate_revisions`)
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var k string
		var r int64
		if err := rows.Scan(&k, &r); err != nil {
			return s, err
		}
		s.Revisions[k] = r
	}
	rows.Close()
	for _, q := range []struct {
		dst *[]string
		sql string
	}{
		{&s.Revoked, `SELECT id::text FROM auth.credential_registry WHERE revoked_at IS NOT NULL ORDER BY id`},
		{&s.Keys, `SELECT idempotency_key FROM auth.auth_outbox_events ORDER BY idempotency_key`},
	} {
		rs, err := tx.Query(ctx, q.sql)
		if err != nil {
			return s, err
		}
		for rs.Next() {
			var v string
			if err := rs.Scan(&v); err != nil {
				return s, err
			}
			*q.dst = append(*q.dst, v)
		}
		rs.Close()
	}
	s.KeyDigest = map[string]string{}
	kd, err := tx.Query(ctx, `SELECT idempotency_key, md5(event_type || payload::text || created_at::text) FROM auth.auth_outbox_events`)
	if err != nil {
		return s, err
	}
	for kd.Next() {
		var k, d string
		if err := kd.Scan(&k, &d); err != nil {
			return s, err
		}
		s.KeyDigest[k] = d
	}
	kd.Close()
	for _, tbl := range []string{"aggregate_revisions", "auth_outbox_events", "credential_registry", "organizations", "security_audit_events", "principals"} {
		var n int64
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM auth.`+tbl).Scan(&n); err != nil {
			return s, err
		}
		s.Counts[tbl] = n
	}
	for name, sql := range map[string]string{
		"aggregate_revisions": `SELECT coalesce(md5(string_agg(aggregate_type || aggregate_id || revision::text, ',' ORDER BY aggregate_type, aggregate_id)), '') FROM auth.aggregate_revisions`,
		"credential_revoked":  `SELECT coalesce(md5(string_agg(id::text || revoked_at::text || coalesce(revoked_reason, ''), ',' ORDER BY id)), '') FROM auth.credential_registry WHERE revoked_at IS NOT NULL`,
		"outbox_keys":         `SELECT coalesce(md5(string_agg(idempotency_key, ',' ORDER BY idempotency_key)), '') FROM auth.auth_outbox_events`,
	} {
		var d string
		if err := tx.QueryRow(ctx, sql).Scan(&d); err != nil {
			return s, err
		}
		s.Digest[name] = d
	}
	return s, nil
}

func drillWriteJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func drillLoad(t *testing.T, dir, label string) drillSnapshot {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "snap-"+label+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var s drillSnapshot
	if err := json.Unmarshal(b, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

func drillSnapshotPhase(t *testing.T, ctx context.Context, admin *pgxpool.Pool, dir, label string) {
	t.Helper()
	if label == "" {
		t.Fatal("DRILL_LABEL is required")
	}
	var at string
	if err := admin.QueryRow(ctx, `SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') || '+00'`).Scan(&at); err != nil {
		t.Fatal(err)
	}
	var lsn string
	if err := admin.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsn); err != nil {
		t.Fatal(err)
	}
	s, err := drillRead(ctx, admin, label)
	if err != nil {
		t.Fatal(err)
	}
	drillWriteJSON(t, filepath.Join(dir, "snap-"+label+".json"), s)
	if err := os.WriteFile(filepath.Join(dir, "time-"+label+".txt"), []byte(at+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "lsn-"+label+".txt"), []byte(lsn+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("snapshot %s at %s lsn %s: %d aggregates, %d revoked, %d outbox keys", label, at, lsn, len(s.Revisions), len(s.Revoked), len(s.Keys))
}

type drillResult struct {
	Expect     string   `json:"expect"`
	Mode       string   `json:"mode"`
	Violations []string `json:"violations"`
	Pass       bool     `json:"pass"`
}

func drillVerify(t *testing.T, ctx context.Context, admin *pgxpool.Pool, dir, expect, mode string) {
	t.Helper()
	if mode != "exact" && mode != "atleast" {
		t.Fatalf("DRILL_MODE must be exact or atleast, got %q", mode)
	}
	want := drillLoad(t, dir, expect)
	got, err := drillRead(ctx, admin, "now")
	if err != nil {
		t.Fatal(err)
	}
	var bad []string
	for k, r := range want.Revisions {
		g, ok := got.Revisions[k]
		switch {
		case !ok:
			bad = append(bad, fmt.Sprintf("a: %s missing after restore (was revision %d)", k, r))
		case g < r:
			bad = append(bad, fmt.Sprintf("a: %s went backwards %d -> %d", k, r, g))
		case mode == "exact" && g != r:
			bad = append(bad, fmt.Sprintf("d: %s revision %d, want exactly %d", k, g, r))
		}
	}
	revoked := map[string]bool{}
	for _, id := range got.Revoked {
		revoked[id] = true
	}
	for _, id := range want.Revoked {
		if !revoked[id] {
			bad = append(bad, "b: credential "+id+" is no longer revoked")
		}
	}
	if mode == "exact" {
		for _, name := range []string{"aggregate_revisions", "credential_revoked", "outbox_keys"} {
			if got.Digest[name] != want.Digest[name] {
				bad = append(bad, fmt.Sprintf("d: digest %s %s != %s", name, got.Digest[name], want.Digest[name]))
			}
		}
		for tbl, n := range want.Counts {
			if got.Counts[tbl] != n {
				bad = append(bad, fmt.Sprintf("d: row count %s %d != %d", tbl, got.Counts[tbl], n))
			}
		}
	}
	seen := map[string]bool{}
	for _, k := range got.Keys {
		if seen[k] {
			bad = append(bad, "c: duplicate idempotency key "+k)
		}
		seen[k] = true
	}
	res := drillResult{Expect: expect, Mode: mode, Violations: bad, Pass: len(bad) == 0}
	drillWriteJSON(t, filepath.Join(dir, fmt.Sprintf("verify-%s-%s.json", expect, mode)), res)
	if !res.Pass {
		t.Fatalf("verify %s/%s: %d violation(s), first: %s", expect, mode, len(bad), bad[0])
	}
}

type drillPostResult struct {
	Expect                string   `json:"expect"`
	Final                 string   `json:"final"`
	ContinuityOK          bool     `json:"continuity_ok"`
	NoDuplicateKeys       bool     `json:"no_duplicate_keys"`
	ReusedKeys            []string `json:"keys_reissued_that_final_had_published"`
	LostRevisions         int64    `json:"revisions_lost_by_pitr_total"`
	RevokesAfterTComeBack int      `json:"revokes_after_target_that_came_back"`
	ReissuedDifferent     int      `json:"keys_reissued_with_different_content"`
	// Fenced is set by DRILL_FENCE=1: the restore fence (auth-migrate restore-fence export and apply) ran before
	// these writes. FenceRowsForLiveTimeline is the number of restore_fence_state rows the runtime role reads for the
	// timeline the server is on now: exactly one (the apply row) on a fenced restore.
	Fenced                   bool   `json:"fenced"`
	FenceRowsForLiveTimeline *int64 `json:"fence_rows_for_live_timeline,omitempty"`
	Verdict                  string `json:"verdict"`
}

// drillPost runs new writes after a restore: every aggregate must continue at
// exactly its restored revision + 1 (one writer, no reuse inside the restored
// timeline). The keys the restored timeline re-issues, that the pre-restore
// timeline (DRILL_FINAL) had already published, are reported, not failed: that
// is the PITR caveat the drill measures.
//
// With DRILL_FENCE=1 the restore fence ran first (CHAOS-11271): the next write of every aggregate is
// max(restored revision, floor) + 1, where the floor is the end-of-WAL revision (DRILL_FINAL), no key is re-issued
// with different content, no revocation made after T is live, and the runtime role reads exactly one
// restore_fence_state row for the live timeline.
func drillPost(t *testing.T, ctx context.Context, rt *pgxpool.Pool, schema authschema.ValidatedIdentifier, admin *pgxpool.Pool, dir, expect, final string) {
	t.Helper()
	want := drillLoad(t, dir, expect)
	fin := drillLoad(t, dir, final)
	fenced := os.Getenv("DRILL_FENCE") == "1"
	res := drillPostResult{Expect: expect, Final: final, ContinuityOK: true, Fenced: fenced}
	for i := 0; i < drillOrgs; i++ {
		if err := drillOrgUpdate(ctx, rt, schema, i, fmt.Sprintf("drill-post-%d", i)); err != nil {
			t.Fatalf("post write org %d: %v", i, err)
		}
	}
	got, err := drillRead(ctx, admin, "post")
	if err != nil {
		t.Fatal(err)
	}
	for k, r := range want.Revisions {
		if got.Revisions[k] < r {
			res.ContinuityOK = false
		}
	}
	for i := 0; i < drillOrgs; i++ {
		k := "organization:org_" + drillOrgID(i).String()
		next := want.Revisions[k] + 1
		if fenced {
			next = max(want.Revisions[k], fin.Revisions[k]) + 1
		}
		if got.Revisions[k] != next {
			res.ContinuityOK = false
			t.Errorf("%s: revision %d after one post-restore write, want %d", k, got.Revisions[k], next)
		}
		res.LostRevisions += fin.Revisions[k] - want.Revisions[k]
	}
	finKeys := map[string]bool{}
	for _, k := range fin.Keys {
		finKeys[k] = true
	}
	seen := map[string]bool{}
	res.NoDuplicateKeys = true
	for _, k := range got.Keys {
		if seen[k] {
			res.NoDuplicateKeys = false
		}
		seen[k] = true
	}
	wantKeys := map[string]bool{}
	for _, k := range want.Keys {
		wantKeys[k] = true
	}
	for _, k := range got.Keys {
		if !wantKeys[k] && finKeys[k] {
			res.ReusedKeys = append(res.ReusedKeys, k)
		}
	}
	sort.Strings(res.ReusedKeys)
	for _, k := range res.ReusedKeys {
		if got.KeyDigest[k] != fin.KeyDigest[k] {
			res.ReissuedDifferent++
		}
	}
	wasRevoked := map[string]bool{}
	for _, id := range want.Revoked {
		wasRevoked[id] = true
	}
	nowRevoked := map[string]bool{}
	for _, id := range got.Revoked {
		nowRevoked[id] = true
	}
	for _, id := range fin.Revoked {
		if !wasRevoked[id] && !nowRevoked[id] {
			res.RevokesAfterTComeBack++
		}
	}
	res.Verdict = "PASS"
	if res.RevokesAfterTComeBack != 0 || res.ReissuedDifferent != 0 {
		res.Verdict = "GAP"
	}
	if fenced {
		var rows int64
		if err := rt.QueryRow(ctx, `SELECT count(*) FROM `+authschema.Quote(schema)+`.restore_fence_state
WHERE timeline_id = ('x' || substr(pg_walfile_name(pg_current_wal_insert_lsn()), 1, 8))::bit(32)::bigint`).Scan(&rows); err != nil {
			t.Fatalf("read the fence rows of the live timeline as the runtime role: %v", err)
		}
		res.FenceRowsForLiveTimeline = &rows
	}
	drillWriteJSON(t, filepath.Join(dir, "post-"+expect+".json"), res)
	if fenced {
		if res.RevokesAfterTComeBack != 0 || res.ReissuedDifferent != 0 || len(res.ReusedKeys) != 0 {
			t.Fatalf("fenced restore: %d revocation(s) after T came back, %d key(s) re-issued with different content, %d re-issued key(s)",
				res.RevokesAfterTComeBack, res.ReissuedDifferent, len(res.ReusedKeys))
		}
		if res.FenceRowsForLiveTimeline == nil || *res.FenceRowsForLiveTimeline != 1 {
			t.Fatalf("fenced restore: restore_fence_state has %v row(s) for the live timeline, want exactly 1", res.FenceRowsForLiveTimeline)
		}
	}
	if !res.ContinuityOK || !res.NoDuplicateKeys {
		t.Fatalf("post-restore writes: continuity=%v noDuplicateKeys=%v", res.ContinuityOK, res.NoDuplicateKeys)
	}
}
