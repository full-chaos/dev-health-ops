//go:build integration

package restorefence

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func scopeID(name string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-scope-"+name))
}

func (d *fenceDB) workload(t *testing.T, ctx context.Context, name string, audiences, actions []string) {
	t.Helper()
	d.must(t, ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'workload', $2)`, scopeID(name), name)
	d.commit(t, ctx, "workload.created", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `INSERT INTO auth.workloads (principal_id, organization_id, name, audiences, allowed_actions) VALUES ($1, $2, $3, $4, $5)`,
			scopeID(name), fenceOrgID(0), name, audiences, actions)
		return err
	})
}

func (d *fenceDB) setWorkload(t *testing.T, ctx context.Context, name string, audiences, actions []string) {
	t.Helper()
	d.commit(t, ctx, "workload.audiences_changed", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.workloads SET audiences = $2, allowed_actions = $3 WHERE principal_id = $1`, scopeID(name), audiences, actions)
		return err
	})
}

func (d *fenceDB) workloadScopes(t *testing.T, ctx context.Context, name string) (audiences, actions []string) {
	t.Helper()
	if err := d.admin.QueryRow(ctx, `SELECT audiences, allowed_actions FROM auth.workloads WHERE principal_id = $1`, scopeID(name)).Scan(&audiences, &actions); err != nil {
		t.Fatalf("workload %s: %v", name, err)
	}
	return audiences, actions
}

func (d *fenceDB) scopedCredential(t *testing.T, ctx context.Context, name string, scopes, audiences []string) {
	t.Helper()
	principal := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-principal"))
	d.commit(t, ctx, "credential.created", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `INSERT INTO auth.credential_registry (id, principal_id, credential_class, secret_hash, hash_algorithm, fingerprint, scopes, audiences)
VALUES ($1, $2, 'workload_token', decode(md5($1::uuid::text), 'hex'), 'sha256', '0123456789abcdef', $3, $4)`, scopeID(name), principal, scopes, audiences)
		return err
	})
}

func (d *fenceDB) credentialScopes(t *testing.T, ctx context.Context, name string) (scopes, audiences []string) {
	t.Helper()
	if err := d.admin.QueryRow(ctx, `SELECT scopes::text[], audiences::text[] FROM auth.credential_registry WHERE id = $1`, scopeID(name)).Scan(&scopes, &audiences); err != nil {
		t.Fatalf("credential %s: %v", name, err)
	}
	return scopes, audiences
}

func reviewIDs(report Report, kind, source string) []string {
	var ids []string
	for _, a := range report.Actions {
		if string(a.Kind) == kind && (source == "" || a.Source == source) {
			ids = append(ids, a.IDs...)
		}
	}
	sort.Strings(ids)
	out := ids[:0]
	for i, id := range ids {
		if i == 0 || id != ids[i-1] {
			out = append(out, id)
		}
	}
	return out
}

func sortedIDs(ids ...uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	sort.Strings(out)
	return out
}

func sameSet(t *testing.T, what string, got, want []string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

// G1 + C1 + Q2: a workload's audiences and allowed actions narrowed after T are narrowed again; a value added after T is
// not granted; a swap after T leaves the empty set and the workload is named for review; a NULL source narrows to empty.
func TestTheFenceNarrowsWorkloadScopesAndNamesTheEmptiedOnes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.workload(t, ctx, "wl-narrow", []string{"a.one", "a.two"}, []string{"m.read", "m.write"})
		d.workload(t, ctx, "wl-swap", []string{"a.one"}, []string{"m.read"})
		d.workload(t, ctx, "wl-widen", []string{"a.one"}, []string{"m.read"})
		d.workload(t, ctx, "wl-null", []string{"a.one", "a.two"}, []string{})
		d.workload(t, ctx, "wl-same", []string{"a.one"}, []string{"m.read"})
	}
	end.setWorkload(t, ctx, "wl-narrow", []string{"a.one"}, []string{"m.read"})
	end.setWorkload(t, ctx, "wl-swap", []string{"a.two"}, []string{"m.read"})
	end.setWorkload(t, ctx, "wl-widen", []string{"a.one", "a.two"}, []string{"m.read", "m.write"})
	manifest := exportEnd(t, ctx, end)
	// C1: the column is NOT NULL, so a NULL source value can only come from a manifest; it must narrow to empty.
	for i, s := range manifest.Sections {
		if s.Name != "auth.workloads.scope" {
			continue
		}
		for _, row := range manifest.Sections[i].Rows {
			if row["principal_id"] == scopeID("wl-null").String() {
				row["audiences"] = nil
			}
		}
	}
	if err := manifest.Seal(); err != nil {
		t.Fatal(err)
	}
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for name, want := range map[string][2]string{
		"wl-narrow": {"a.one", "m.read"}, "wl-swap": {"", "m.read"}, "wl-widen": {"a.one", "m.read"},
		"wl-null": {"", ""}, "wl-same": {"a.one", "m.read"},
	} {
		aud, act := restored.workloadScopes(t, ctx, name)
		if got := [2]string{strings.Join(aud, " "), strings.Join(act, " ")}; got != want {
			t.Errorf("workload %s = %q, want %q", name, got, want)
		}
	}
	sameSet(t, "review_scope_emptied (workloads)", reviewIDs(report, "review_scope_emptied", "auth.workloads.scope"), sortedIDs(scopeID("wl-swap"), scopeID("wl-null")))
	for _, s := range report.Sections {
		if s.Name == "auth.workloads.scope" && s.Changed != 3 {
			t.Errorf("the workload scope section changed %d rows, want 3 (narrow, swap, null)", s.Changed)
		}
	}
}

// G2: credential scopes and audiences narrowed in place after T are narrowed again; an unchanged credential is left.
func TestTheFenceNarrowsCredentialScopesInPlace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.scopedCredential(t, ctx, "c-narrow", []string{"api:read", "api:write"}, []string{"query-api", "ops-api"})
		d.scopedCredential(t, ctx, "c-same", []string{"api:read"}, []string{"query-api"})
	}
	end.commit(t, ctx, "credential.narrowed", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.credential_registry SET scopes = '{api:read}', audiences = '{query-api}' WHERE id = $1`, scopeID("c-narrow"))
		return err
	})
	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for name, want := range map[string][2]string{"c-narrow": {"api:read", "query-api"}, "c-same": {"api:read", "query-api"}} {
		sc, au := restored.credentialScopes(t, ctx, name)
		if got := [2]string{strings.Join(sc, " "), strings.Join(au, " ")}; got != want {
			t.Errorf("credential %s = %q, want %q", name, got, want)
		}
	}
	for _, s := range report.Sections {
		if s.Name == "auth.credential_registry.scope" && s.Changed != 1 {
			t.Errorf("the credential scope section changed %d rows, want 1", s.Changed)
		}
	}
}

// G3: an allowlist entry whose range was edited after T is not held: it is switched off while its org keeps a shared
// active entry, and kept + named otherwise; a shorter source expiry is carried and a longer one is not.
func TestTheFenceNeverWidensAnAllowlistEditedAfterT(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.legacyOrg(t, ctx, "ed-a", true)
		d.legacyOrg(t, ctx, "ed-b", true)
		d.allowlistEntry(t, ctx, "ed-a", "ea1", "10.5.1.0/24")
		d.allowlistEntry(t, ctx, "ed-a", "ea2", "10.5.2.0/24")
		d.allowlistEntry(t, ctx, "ed-a", "ea3", "10.5.3.0/24")
		d.allowlistEntry(t, ctx, "ed-b", "eb1", "10.6.1.0/24")
		d.must(t, ctx, `UPDATE public.org_ip_allowlist SET expires_at = '2027-01-01T00:00:00Z' WHERE id = $1`, flagID("ea2"))
		d.must(t, ctx, `UPDATE public.org_ip_allowlist SET expires_at = '2026-11-01T00:00:00Z' WHERE id = $1`, flagID("ea3"))
	}
	end.must(t, ctx, `UPDATE public.org_ip_allowlist SET ip_range = '10.5.1.0/25' WHERE id = $1`, flagID("ea1"))
	end.must(t, ctx, `UPDATE public.org_ip_allowlist SET ip_range = '10.6.1.0/25' WHERE id = $1`, flagID("eb1"))
	end.must(t, ctx, `UPDATE public.org_ip_allowlist SET expires_at = '2026-11-01T00:00:00Z' WHERE id = $1`, flagID("ea2"))
	end.must(t, ctx, `UPDATE public.org_ip_allowlist SET expires_at = '2027-06-01T00:00:00Z' WHERE id = $1`, flagID("ea3"))
	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for name, on := range map[string]bool{"ea1": false, "ea2": true, "ea3": true, "eb1": true} {
		if got := restored.flag(t, ctx, "org_ip_allowlist", name); got != on {
			t.Errorf("allowlist entry %s is_active = %v, want %v", name, got, on)
		}
	}
	short := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{"ea2", "ea3"} {
		var at time.Time
		if err := restored.admin.QueryRow(ctx, `SELECT expires_at FROM public.org_ip_allowlist WHERE id = $1`, flagID(name)).Scan(&at); err != nil {
			t.Fatal(err)
		}
		if !at.Equal(short) {
			t.Errorf("allowlist entry %s expires %v, want the earlier %v", name, at, short)
		}
	}
	sameSet(t, "review_ip_allowlist", reviewIDs(report, "review_ip_allowlist", ""), sortedIDs(flagID("ed-b")))
}

// memberPair puts one membership into both tables (the D6358 mirror): auth.memberships and public.memberships.
func (d *fenceDB) memberPair(t *testing.T, ctx context.Context, org uuid.UUID, user, role string) {
	t.Helper()
	p := scopeID("user-" + user)
	d.commit(t, ctx, "membership.created", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `INSERT INTO auth.memberships (id, organization_id, principal_id, role, status) VALUES ($1, $2, $3, $4, 'active')`,
			scopeID("auth-m-"+org.String()+"-"+user), org, p, role)
		return err
	})
	d.must(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, $4)`, scopeID("pub-m-"+org.String()+"-"+user), p, org, role)
}

func (d *fenceDB) setRole(t *testing.T, ctx context.Context, org uuid.UUID, user, role string) {
	t.Helper()
	d.commit(t, ctx, "membership.role_changed", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.memberships SET role = $2 WHERE id = $1`, scopeID("auth-m-"+org.String()+"-"+user), role)
		return err
	})
	d.must(t, ctx, `UPDATE public.memberships SET role = $2 WHERE id = $1`, scopeID("pub-m-"+org.String()+"-"+user), role)
}

func (d *fenceDB) roles(t *testing.T, ctx context.Context, org uuid.UUID, user string) (authRole, publicRole string) {
	t.Helper()
	if err := d.admin.QueryRow(ctx, `SELECT role FROM auth.memberships WHERE id = $1`, scopeID("auth-m-"+org.String()+"-"+user)).Scan(&authRole); err != nil {
		t.Fatal(err)
	}
	if err := d.admin.QueryRow(ctx, `SELECT role FROM public.memberships WHERE id = $1`, scopeID("pub-m-"+org.String()+"-"+user)).Scan(&publicRole); err != nil {
		t.Fatal(err)
	}
	return authRole, publicRole
}

func (d *fenceDB) scopeUsers(t *testing.T, ctx context.Context, users ...string) {
	t.Helper()
	for _, u := range users {
		p := scopeID("user-" + u)
		d.must(t, ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', $2)`, p, u)
		d.must(t, ctx, `INSERT INTO public.users (id, email) VALUES ($1, $2)`, p, u+"@scope.example.invalid")
	}
}

// G5 + Q4 + Q5 + C4: a role lowered after T is lowered again in both tables; a role raised after T is not carried; an
// org's last owner is kept (review_membership_owner names the org); an unknown legacy role is left and named; the auth
// and the legacy table never disagree after the apply.
func TestMembershipRolesGoDownInBothTablesAndNeverDisagree(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	orgA, orgB, orgC := fenceOrgID(1), fenceOrgID(2), fenceOrgID(0)
	users := []string{"o1", "n1", "p", "q", "v", "b1", "b2", "u"}
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		for i, org := range []uuid.UUID{orgA, orgB, orgC} {
			d.must(t, ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, $2)`, org, "fence-scope-org-"+string(rune('a'+i)))
		}
		d.scopeUsers(t, ctx, users...)
		d.memberPair(t, ctx, orgA, "o1", "owner")
		d.memberPair(t, ctx, orgA, "n1", "member")
		d.memberPair(t, ctx, orgA, "p", "admin")
		d.memberPair(t, ctx, orgA, "q", "member")
		d.memberPair(t, ctx, orgA, "v", "viewer")
		d.memberPair(t, ctx, orgB, "b1", "owner")
		d.memberPair(t, ctx, orgB, "b2", "owner")
		d.must(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`, scopeID("pub-unknown"), scopeID("user-u"), orgC)
	}
	restored.must(t, ctx, `UPDATE public.memberships SET role = 'superadmin' WHERE id = $1`, scopeID("pub-unknown"))
	end.setRole(t, ctx, orgA, "o1", "admin")
	end.setRole(t, ctx, orgA, "n1", "owner")
	end.setRole(t, ctx, orgA, "p", "member")
	end.setRole(t, ctx, orgA, "q", "admin")
	end.setRole(t, ctx, orgB, "b1", "member")
	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for _, c := range []struct {
		org        uuid.UUID
		user, want string
	}{
		{orgA, "o1", "owner"}, {orgA, "n1", "member"}, {orgA, "p", "member"}, {orgA, "q", "member"}, {orgA, "v", "viewer"},
		{orgB, "b1", "member"}, {orgB, "b2", "owner"},
	} {
		a, p := restored.roles(t, ctx, c.org, c.user)
		if a != c.want || p != c.want {
			t.Errorf("%s in org %s: auth %s, public %s, want %s in both", c.user, c.org, a, p, c.want)
		}
	}
	var disagree int64
	if err := restored.admin.QueryRow(ctx, `SELECT count(*) FROM auth.memberships a JOIN public.memberships p ON p.user_id = a.principal_id AND p.org_id = a.organization_id
WHERE a.role IS DISTINCT FROM p.role`).Scan(&disagree); err != nil {
		t.Fatal(err)
	}
	if disagree != 0 {
		t.Errorf("%d mirrored memberships disagree on the role after the apply", disagree)
	}
	var unknown string
	if err := restored.admin.QueryRow(ctx, `SELECT role FROM public.memberships WHERE id = $1`, scopeID("pub-unknown")).Scan(&unknown); err != nil || unknown != "superadmin" {
		t.Errorf("an unknown legacy role is left as restored, got %q (%v)", unknown, err)
	}
	sameSet(t, "review_membership_owner", reviewIDs(report, "review_membership_owner", ""), sortedIDs(orgA))
	sameSet(t, "review_membership_role", reviewIDs(report, "review_membership_role", ""), sortedIDs(scopeID("pub-unknown")))
}

// G6: a superuser flag removed after T stays removed; one granted after T is not carried.
func TestASuperuserRemovedAfterTStaysRemoved(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		for name, su := range map[string]bool{"su-removed": true, "su-kept": true, "su-granted": false} {
			d.must(t, ctx, `INSERT INTO public.users (id, email, is_superuser) VALUES ($1, $2, $3)`, scopeID(name), name+"@scope.example.invalid", su)
		}
	}
	end.must(t, ctx, `UPDATE public.users SET is_superuser = false WHERE id = $1`, scopeID("su-removed"))
	end.must(t, ctx, `UPDATE public.users SET is_superuser = true WHERE id = $1`, scopeID("su-granted"))
	manifest := exportEnd(t, ctx, end)
	if _, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	for name, want := range map[string]bool{"su-removed": false, "su-kept": true, "su-granted": false} {
		var got bool
		if err := restored.admin.QueryRow(ctx, `SELECT is_superuser FROM public.users WHERE id = $1`, scopeID(name)).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s is_superuser = %v, want %v", name, got, want)
		}
	}
}

// Q3: a blind margin has no source state: scopes, roles and superuser flags stay as restored, and every row that holds
// one is named (ids only) in review_after_blind_margin.
func TestABlindMarginLeavesScopesRolesAndSuperusersAndNamesThemForReview(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	db.workload(t, ctx, "blind-wl", []string{"a.one"}, []string{"m.read"})
	db.workload(t, ctx, "blind-wl-empty", []string{}, []string{})
	db.scopedCredential(t, ctx, "blind-cred", []string{"api:read"}, []string{"query-api"})
	db.must(t, ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, 'fence-scope-blind', 'fence-scope-blind')`, fenceOrgID(1))
	db.scopeUsers(t, ctx, "bo", "bm")
	db.memberPair(t, ctx, fenceOrgID(1), "bo", "owner")
	db.memberPair(t, ctx, fenceOrgID(1), "bm", "member")
	db.must(t, ctx, `UPDATE public.users SET is_superuser = true WHERE id = $1`, scopeID("user-bo"))
	report, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if aud, act := db.workloadScopes(t, ctx, "blind-wl"); strings.Join(aud, ",") != "a.one" || strings.Join(act, ",") != "m.read" {
		t.Errorf("a blind margin changed a workload's scopes: %v %v", aud, act)
	}
	if sc, au := db.credentialScopes(t, ctx, "blind-cred"); strings.Join(sc, ",") != "api:read" || strings.Join(au, ",") != "query-api" {
		t.Errorf("a blind margin changed a credential's scopes: %v %v", sc, au)
	}
	if a, p := db.roles(t, ctx, fenceOrgID(1), "bo"); a != "owner" || p != "owner" {
		t.Errorf("a blind margin changed a role: %s %s", a, p)
	}
	for source, query := range map[string]string{
		"auth.workloads.scope":           `SELECT principal_id::text FROM auth.workloads WHERE cardinality(audiences) > 0 OR cardinality(allowed_actions) > 0 ORDER BY 1`,
		"auth.credential_registry.scope": `SELECT id::text FROM auth.credential_registry WHERE revoked_at IS NULL AND (cardinality(scopes) > 0 OR cardinality(audiences) > 0) ORDER BY 1`,
		"auth.memberships.role":          `SELECT id::text FROM auth.memberships WHERE role IN ('admin', 'owner') ORDER BY 1`,
		"public.memberships.role":        `SELECT id::text FROM public.memberships WHERE role IN ('admin', 'owner') ORDER BY 1`,
		"public.users.superuser":         `SELECT id::text FROM public.users WHERE is_superuser ORDER BY 1`,
	} {
		rows, err := db.admin.Query(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		var want []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			want = append(want, id)
		}
		rows.Close()
		if len(want) == 0 {
			t.Fatalf("fixture: no row for %s", source)
		}
		sameSet(t, "blind-margin review of "+source, reviewIDs(report, "review_after_blind_margin", source), want)
	}
}
