//go:build integration

package restorefence

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func platPrincipal(name string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-platform-principal-"+name))
}

func platAssignment(name string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-platform-assignment-"+name))
}

func (d *fenceDB) platUser(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	d.must(t, ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', $2)`, platPrincipal(name), name)
}

// platGrant inserts a platform_admin assignment inside audit.Commit (the table is guarded). expires is SQL or "NULL".
func (d *fenceDB) platGrant(t *testing.T, ctx context.Context, assignment, principal, expires string) {
	t.Helper()
	d.commit(t, ctx, "platform_role.granted", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `INSERT INTO auth.platform_role_assignments (id, principal_id, role_key, expires_at) VALUES ($1, $2, 'platform_admin', `+expires+`)`,
			platAssignment(assignment), platPrincipal(principal))
		return err
	})
}

func (d *fenceDB) platRevoke(t *testing.T, ctx context.Context, assignment string) {
	t.Helper()
	d.commit(t, ctx, "platform_role.revoked", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `DELETE FROM auth.platform_role_assignments WHERE id = $1`, platAssignment(assignment))
		return err
	})
}

func (d *fenceDB) platDisable(t *testing.T, ctx context.Context, principal string) {
	t.Helper()
	d.commit(t, ctx, "principal.disabled", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.principals SET disabled_at = now() WHERE id = $1`, platPrincipal(principal))
		return err
	})
}

func (d *fenceDB) platHas(t *testing.T, ctx context.Context, assignment string) bool {
	t.Helper()
	return d.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE id = '`+platAssignment(assignment).String()+`'`) == 1
}

func (d *fenceDB) platIDs(t *testing.T, ctx context.Context) []string {
	t.Helper()
	rows, err := d.admin.Query(ctx, `SELECT id::text FROM auth.platform_role_assignments ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

// platPair is a restored and a source database at T holding the same principals (alice, bob, carol) and the given
// assignments (name -> principal), the same in both.
func platPair(t *testing.T, ctx context.Context, atT map[string]string) (restored, end *fenceDB) {
	t.Helper()
	restored, end = newFenceDB(t, ctx), newFenceDB(t, ctx)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		for _, p := range []string{"alice", "bob", "carol"} {
			d.platUser(t, ctx, p)
		}
		for a, p := range atT {
			d.platGrant(t, ctx, a, p, "NULL")
		}
	}
	return restored, end
}

func actionIDs(report Report, kind ActionKind) ([]string, bool) {
	for _, a := range report.Actions {
		if a.Kind == kind && a.Source == "auth.platform_role_assignments" {
			return a.IDs, true
		}
	}
	return nil, false
}

func platSection(report Report) SectionReport {
	for _, s := range report.Sections {
		if s.Name == "auth.platform_role_assignments" {
			return s
		}
	}
	return SectionReport{}
}

func TestARevokeAfterTIsDeletedByThePresenceSection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := platPair(t, ctx, map[string]string{"ga": "alice", "gb": "bob"})
	end.platRevoke(t, ctx, "gb")
	report, err := Apply(ctx, restored.admin, restored.schema, exportEnd(t, ctx, end), Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !restored.platHas(t, ctx, "ga") || restored.platHas(t, ctx, "gb") {
		t.Errorf("restored assignments %v: want ga kept and the revoked gb deleted", restored.platIDs(t, ctx))
	}
	if s := platSection(report); s.Changed != 1 || s.Absent != 0 {
		t.Errorf("section report %+v, want changed 1 absent 0", s)
	}
	if _, ok := actionIDs(report, ActionRegrantPlatformRole); ok {
		t.Errorf("nothing was granted after T, yet regrant_platform_role is named: %+v", report.Actions)
	}
	if _, ok := actionIDs(report, ActionNoActivePlatformAdmin); ok {
		t.Errorf("alice is still an active admin: %+v", report.Actions)
	}
}

func TestAGrantAfterTIsReportedAndNotInserted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := platPair(t, ctx, map[string]string{"ga": "alice"})
	end.platGrant(t, ctx, "gc", "carol", "NULL")
	report, err := Apply(ctx, restored.admin, restored.schema, exportEnd(t, ctx, end), Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if restored.platHas(t, ctx, "gc") || !restored.platHas(t, ctx, "ga") {
		t.Errorf("restored assignments %v: the fence must never insert a grant", restored.platIDs(t, ctx))
	}
	ids, ok := actionIDs(report, ActionRegrantPlatformRole)
	if !ok || !slices.Equal(ids, []string{platAssignment("gc").String()}) {
		t.Errorf("regrant_platform_role = %v (%v), want [gc]", ids, ok)
	}
	if s := platSection(report); s.Changed != 0 || s.Absent != 1 {
		t.Errorf("section report %+v, want changed 0 absent 1", s)
	}
}

func TestAReGrantDeletesTheOldIDAndReportsTheNewOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := platPair(t, ctx, map[string]string{"ga": "alice", "gb": "bob"})
	end.platRevoke(t, ctx, "gb")
	end.platGrant(t, ctx, "gb2", "bob", "NULL")
	report, err := Apply(ctx, restored.admin, restored.schema, exportEnd(t, ctx, end), Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if restored.platHas(t, ctx, "gb") || restored.platHas(t, ctx, "gb2") || !restored.platHas(t, ctx, "ga") {
		t.Errorf("restored assignments %v: want only ga", restored.platIDs(t, ctx))
	}
	ids, _ := actionIDs(report, ActionRegrantPlatformRole)
	if !slices.Equal(ids, []string{platAssignment("gb2").String()}) {
		t.Errorf("regrant_platform_role = %v, want [gb2]", ids)
	}
}

func TestNoActivePlatformAdminLineFiresWhenTheFenceLeavesZero(t *testing.T) {
	type cell struct {
		name    string
		prepare func(t *testing.T, ctx context.Context, restored, end *fenceDB)
		atT     map[string]string
		expired string
		owned   bool
		want    bool
		wantIDs []string
	}
	cells := []cell{
		{name: "switch off, table in use: the only admin is revoked after T", atT: map[string]string{"gb": "bob"}, want: true,
			prepare: func(t *testing.T, ctx context.Context, _, end *fenceDB) { end.platRevoke(t, ctx, "gb") }},
		{name: "the only admin expired", atT: map[string]string{}, expired: "ge", want: true, wantIDs: []string{"ge"}},
		{name: "the only admin principal is disabled after T", atT: map[string]string{"ga": "alice"}, want: true, wantIDs: []string{"ga"},
			prepare: func(t *testing.T, ctx context.Context, _, end *fenceDB) { end.platDisable(t, ctx, "alice") }},
		{name: "one of two admins is revoked and the other stays active", atT: map[string]string{"ga": "alice", "gb": "bob"}, want: false,
			prepare: func(t *testing.T, ctx context.Context, _, end *fenceDB) { end.platRevoke(t, ctx, "gb") }},
		{name: "one admin revoked and the other expired", atT: map[string]string{"gb": "bob"}, expired: "ge", want: true, wantIDs: []string{"ge"},
			prepare: func(t *testing.T, ctx context.Context, _, end *fenceDB) { end.platRevoke(t, ctx, "gb") }},
		{name: "switch off, empty table: silent", atT: map[string]string{}, want: false},
		{name: "switch on, empty table: fires", atT: map[string]string{}, owned: true, want: true},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			restored, end := platPair(t, ctx, c.atT)
			if c.expired != "" {
				for _, d := range []*fenceDB{restored, end} {
					d.platGrant(t, ctx, c.expired, "carol", "now() - interval '1 hour'")
				}
			}
			if c.prepare != nil {
				c.prepare(t, ctx, restored, end)
			}
			report, err := Apply(ctx, restored.admin, restored.schema, exportEnd(t, ctx, end), Options{Apply: true, PlatformRolesOwned: c.owned})
			if err != nil {
				t.Fatalf("apply: %v", err)
			}
			ids, fired := actionIDs(report, ActionNoActivePlatformAdmin)
			if fired != c.want {
				t.Fatalf("no_active_platform_admin fired = %v, want %v (actions %+v)", fired, c.want, report.Actions)
			}
			if !fired {
				return
			}
			want := []string{}
			for _, n := range c.wantIDs {
				want = append(want, platAssignment(n).String())
			}
			if !slices.Equal(append([]string{}, ids...), want) {
				t.Errorf("lockout ids = %v, want %v", ids, want)
			}
			stored, err := PendingActions(ctx, restored.admin, restored.schema)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.ContainsFunc(stored, func(a OperatorAction) bool { return a.Kind == ActionNoActivePlatformAdmin }) {
				t.Errorf("the stored actions %+v lack the lockout line (status prints them)", stored)
			}
		})
	}
}

// D6593: a dry run (apply without -apply) names the revoke and the regrant and writes nothing.
func TestAPlatformRoleDryRunWritesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := platPair(t, ctx, map[string]string{"ga": "alice", "gb": "bob"})
	end.platRevoke(t, ctx, "gb")
	end.platGrant(t, ctx, "gc", "carol", "NULL")
	before := restored.platIDs(t, ctx)
	states := restored.count(t, ctx, `SELECT count(*) FROM auth.restore_fence_state`)
	report, err := Apply(ctx, restored.admin, restored.schema, exportEnd(t, ctx, end), Options{})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if !report.DryRun || platSection(report).Changed != 1 {
		t.Errorf("dry run report %+v: want the deletion counted", platSection(report))
	}
	if ids, _ := actionIDs(report, ActionRegrantPlatformRole); !slices.Equal(ids, []string{platAssignment("gc").String()}) {
		t.Errorf("the dry run names regrant ids %v, want [gc]", ids)
	}
	if after := restored.platIDs(t, ctx); !slices.Equal(before, after) {
		t.Errorf("a dry run changed the assignments: %v -> %v", before, after)
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM auth.restore_fence_state`); n != states {
		t.Errorf("a dry run wrote the fence state (%d -> %d rows)", states, n)
	}
}

// D6593: the status reads (timeline, readiness, stored actions) run in a READ ONLY transaction and change nothing.
func TestThePlatformRoleStatusReadsRunReadOnlyAndChangeNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := platPair(t, ctx, map[string]string{"ga": "alice", "gb": "bob"})
	end.platRevoke(t, ctx, "gb")
	if _, err := Apply(ctx, restored.admin, restored.schema, exportEnd(t, ctx, end), Options{Apply: true}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	before := restored.platIDs(t, ctx)
	tx, err := restored.admin.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := Timeline(ctx, tx); err != nil {
		t.Errorf("Timeline in a read-only tx: %v", err)
	}
	if err := Check(ctx, tx, restored.schema); err != nil {
		t.Errorf("Check in a read-only tx: %v", err)
	}
	if _, err := PendingActions(ctx, tx, restored.schema); err != nil {
		t.Errorf("PendingActions in a read-only tx: %v", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM auth.platform_role_assignments`); err == nil {
		t.Error("the read-only guard did not refuse a write: the proof is void")
	}
	_ = tx.Rollback(ctx)
	if after := restored.platIDs(t, ctx); !slices.Equal(before, after) {
		t.Errorf("the status reads changed the assignments: %v -> %v", before, after)
	}
}

// A blind margin has no source state: nothing is deleted and every restored assignment is listed by id for review.
func TestABlindMarginKeepsAllPlatformRolesAndListsThem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	db.platUser(t, ctx, "alice")
	db.platUser(t, ctx, "bob")
	db.platGrant(t, ctx, "ga", "alice", "NULL")
	db.platGrant(t, ctx, "gb", "bob", "NULL")
	report, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if !db.platHas(t, ctx, "ga") || !db.platHas(t, ctx, "gb") {
		t.Errorf("a blind margin deleted assignments: %v", db.platIDs(t, ctx))
	}
	var listed []string
	for _, a := range report.Actions {
		if a.Kind == ActionReviewAfterBlindMargin && a.Source == "auth.platform_role_assignments" {
			listed = a.IDs
		}
	}
	want := []string{platAssignment("ga").String(), platAssignment("gb").String()}
	slices.Sort(want)
	if !slices.Equal(listed, want) {
		t.Errorf("review_after_blind_margin lists %v, want %v", listed, want)
	}
	if _, ok := actionIDs(report, ActionNoActivePlatformAdmin); ok {
		t.Errorf("two active admins remain: %+v", report.Actions)
	}
}

func TestAPlatformRoleApplyIsIdempotentAndTheDeletePrivilegeIsPreflighted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := platPair(t, ctx, map[string]string{"ga": "alice", "gb": "bob"})
	end.platRevoke(t, ctx, "gb")
	manifest := exportEnd(t, ctx, end)
	if _, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true}); err != nil {
		t.Fatal(err)
	}
	again, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil || !again.AlreadyApplied || again.RowsRemoved != 0 {
		t.Fatalf("second run = %+v, %v, want already applied with no removal", again, err)
	}
	restored.must(t, ctx, `REVOKE DELETE ON auth.platform_role_assignments FROM `+`"`+restored.runtimeRole+`"`)
	missing, err := Preflight(ctx, restored.runtime, restored.schema)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(missing, "auth.platform_role_assignments DELETE") {
		t.Errorf("preflight missing = %v, want auth.platform_role_assignments DELETE", missing)
	}
}
