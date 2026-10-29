//go:build integration

package restorefence

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// flagSecret is the stored provider secret of the fixture. It must never appear in a manifest, a report, the stored
// operator actions or a log line.
const flagSecret = "FENCE-SECRET-SENTINEL-6f1d"

func flagID(name string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-flag-"+name))
}

func (d *fenceDB) legacyOrg(t *testing.T, ctx context.Context, name string, active bool) {
	t.Helper()
	d.must(t, ctx, `INSERT INTO public.organizations (id, slug, name, is_active) VALUES ($1, $2, $2, $3)`, flagID(name), "fence-flag-"+name, active)
}

func (d *fenceDB) allowlistEntry(t *testing.T, ctx context.Context, org, name, cidr string) {
	t.Helper()
	d.must(t, ctx, `INSERT INTO public.org_ip_allowlist (id, org_id, ip_range) VALUES ($1, $2, $3)`, flagID(name), flagID(org), cidr)
}

func (d *fenceDB) integrationCredential(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	d.must(t, ctx, `INSERT INTO public.integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, created_at, updated_at)
VALUES ($1, 'fence-org', 'github', $2, true, $3, now(), now())`, flagID(name), name, flagSecret)
}

func (d *fenceDB) integration(t *testing.T, ctx context.Context, name string, active bool) {
	t.Helper()
	d.must(t, ctx, `INSERT INTO public.integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, 'fence-org', 'github', $2, '{}', $3, now(), now())`, flagID(name), name, active)
}

func (d *fenceDB) installation(t *testing.T, ctx context.Context, name string, installationID int64, suspendedAt *time.Time) {
	t.Helper()
	d.must(t, ctx, `INSERT INTO public.github_app_installations (id, installation_id, suspended_at, created_at, updated_at) VALUES ($1, $2, $3, now(), now())`,
		flagID(name), installationID, suspendedAt)
}

func (d *fenceDB) flag(t *testing.T, ctx context.Context, table, name string) bool {
	t.Helper()
	var on bool
	if err := d.admin.QueryRow(ctx, `SELECT is_active FROM public.`+table+` WHERE id = $1`, flagID(name)).Scan(&on); err != nil {
		t.Fatalf("%s %s: %v", table, name, err)
	}
	return on
}

func (d *fenceDB) suspendedAt(t *testing.T, ctx context.Context, name string) *time.Time {
	t.Helper()
	var at *time.Time
	if err := d.admin.QueryRow(ctx, `SELECT suspended_at FROM public.github_app_installations WHERE id = $1`, flagID(name)).Scan(&at); err != nil {
		t.Fatalf("installation %s: %v", name, err)
	}
	return at
}

func (d *fenceDB) secretKept(t *testing.T, ctx context.Context, name string) bool {
	t.Helper()
	var kept bool
	if err := d.admin.QueryRow(ctx, `SELECT credentials_encrypted IS NOT NULL FROM public.integration_credentials WHERE id = $1`, flagID(name)).Scan(&kept); err != nil {
		t.Fatalf("credential %s: %v", name, err)
	}
	return kept
}

// assertNoSecret fails when the stored provider secret appears in the manifest, the report or the stored actions.
func assertNoSecret(t *testing.T, ctx context.Context, d *fenceDB, m *Manifest, report Report) {
	t.Helper()
	if m != nil {
		raw, err := m.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), flagSecret) || strings.Contains(string(raw), "credentials_encrypted") {
			t.Error("the manifest carries the stored provider secret or its column")
		}
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), flagSecret) {
		t.Error("the apply report carries the stored provider secret")
	}
	if n := d.count(t, ctx, `SELECT count(*) FROM auth.restore_fence_state WHERE operator_actions::text LIKE '%`+flagSecret+`%'`); n != 0 {
		t.Error("the stored operator actions carry the stored provider secret")
	}
}

// 11872: the legacy access flags the source switched off after T are off again; a flag switched on after T stays off;
// a row the source deleted is switched off (never deleted); an earlier suspension is kept; the secret the source
// cleared is cleared; nothing else changes.
func TestTheFenceCarriesTheLegacyAccessFlags(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	early := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.legacyOrg(t, ctx, "org-off", true)
		d.legacyOrg(t, ctx, "org-on", false)
		d.legacyOrg(t, ctx, "org-gone", true)
		d.legacyOrg(t, ctx, "org-kept", true)
		for _, n := range []string{"cred-off", "cred-cleared", "cred-gone", "cred-kept"} {
			d.integrationCredential(t, ctx, n)
		}
		d.integration(t, ctx, "int-off", true)
		d.integration(t, ctx, "int-on", false)
		d.integration(t, ctx, "int-gone", true)
		d.integration(t, ctx, "int-kept", true)
		d.installation(t, ctx, "inst-suspended", 101, nil)
		d.installation(t, ctx, "inst-early", 102, &early)
		d.installation(t, ctx, "inst-on", 103, &early)
		d.installation(t, ctx, "inst-kept", 104, nil)
	}
	end.must(t, ctx, `UPDATE public.organizations SET is_active = false WHERE id = $1`, flagID("org-off"))
	end.must(t, ctx, `UPDATE public.organizations SET is_active = true WHERE id = $1`, flagID("org-on"))
	end.must(t, ctx, `DELETE FROM public.organizations WHERE id = $1`, flagID("org-gone"))
	end.legacyOrg(t, ctx, "org-new", true)
	end.must(t, ctx, `UPDATE public.integration_credentials SET is_active = false WHERE id = $1`, flagID("cred-off"))
	end.must(t, ctx, `UPDATE public.integration_credentials SET credentials_encrypted = NULL WHERE id = $1`, flagID("cred-cleared"))
	end.must(t, ctx, `DELETE FROM public.integration_credentials WHERE id = $1`, flagID("cred-gone"))
	end.must(t, ctx, `UPDATE public.integrations SET is_active = false WHERE id = $1`, flagID("int-off"))
	end.must(t, ctx, `UPDATE public.integrations SET is_active = true WHERE id = $1`, flagID("int-on"))
	end.must(t, ctx, `DELETE FROM public.integrations WHERE id = $1`, flagID("int-gone"))
	end.must(t, ctx, `UPDATE public.github_app_installations SET suspended_at = now() WHERE id IN ($1, $2)`, flagID("inst-suspended"), flagID("inst-early"))
	end.must(t, ctx, `UPDATE public.github_app_installations SET suspended_at = NULL WHERE id = $1`, flagID("inst-on"))

	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for table, want := range map[string]map[string]bool{
		"organizations":           {"org-off": false, "org-on": false, "org-gone": false, "org-kept": true},
		"integration_credentials": {"cred-off": false, "cred-cleared": true, "cred-gone": false, "cred-kept": true},
		"integrations":            {"int-off": false, "int-on": false, "int-gone": false, "int-kept": true},
	} {
		for name, on := range want {
			if got := restored.flag(t, ctx, table, name); got != on {
				t.Errorf("%s %s is_active = %v, want %v", table, name, got, on)
			}
		}
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM public.organizations WHERE id = '`+flagID("org-new").String()+`'`); n != 0 {
		t.Error("an org created after T must stay absent: the fence never inserts")
	}
	for name, kept := range map[string]bool{"cred-off": true, "cred-cleared": false, "cred-gone": false, "cred-kept": true} {
		if got := restored.secretKept(t, ctx, name); got != kept {
			t.Errorf("credential %s keeps its stored secret = %v, want %v", name, got, kept)
		}
	}
	if at := restored.suspendedAt(t, ctx, "inst-suspended"); at == nil {
		t.Error("an installation suspended after T is not suspended again")
	}
	if at := restored.suspendedAt(t, ctx, "inst-early"); at == nil || !at.Equal(early) {
		t.Errorf("an installation suspended before T keeps its earlier suspension, got %v", at)
	}
	if at := restored.suspendedAt(t, ctx, "inst-on"); at == nil {
		t.Error("an installation un-suspended after T must stay suspended (the fence never re-activates)")
	}
	if at := restored.suspendedAt(t, ctx, "inst-kept"); at != nil {
		t.Error("an installation the source never suspended was suspended")
	}
	assertNoSecret(t, ctx, restored, manifest, report)
	again, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil || !again.AlreadyApplied {
		t.Fatalf("a second apply = %+v, %v; want already applied", again, err)
	}
}

// Q2: 0 active entries = the org accepts every address. An entry is switched off only while its org keeps an active
// entry the source also holds active; otherwise the entry stays and the org is named in review_ip_allowlist.
func TestTheFenceNeverOpensAnIPAllowlist(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		for _, org := range []string{"al-a", "al-b", "al-c", "al-d"} {
			d.legacyOrg(t, ctx, org, true)
		}
		d.allowlistEntry(t, ctx, "al-a", "a1", "10.0.1.0/24")
		d.allowlistEntry(t, ctx, "al-a", "a2", "10.0.2.0/24")
		d.allowlistEntry(t, ctx, "al-b", "b1", "10.1.1.0/24")
		d.allowlistEntry(t, ctx, "al-c", "c1", "10.2.1.0/24")
		d.allowlistEntry(t, ctx, "al-c", "c2", "10.2.2.0/24")
		d.allowlistEntry(t, ctx, "al-d", "d1", "10.3.1.0/24")
	}
	end.must(t, ctx, `UPDATE public.org_ip_allowlist SET is_active = false WHERE id IN ($1, $2)`, flagID("a1"), flagID("b1"))
	end.allowlistEntry(t, ctx, "al-b", "b2", "10.1.2.0/24")
	end.must(t, ctx, `DELETE FROM public.org_ip_allowlist WHERE id IN ($1, $2)`, flagID("c1"), flagID("d1"))

	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for name, on := range map[string]bool{"a1": false, "a2": true, "b1": true, "c1": false, "c2": true, "d1": true} {
		if got := restored.flag(t, ctx, "org_ip_allowlist", name); got != on {
			t.Errorf("allowlist entry %s is_active = %v, want %v", name, got, on)
		}
	}
	var reviewed []string
	for _, a := range report.Actions {
		if a.Kind == ActionKind("review_ip_allowlist") {
			reviewed = append(reviewed, a.IDs...)
		}
	}
	sort.Strings(reviewed)
	want := []string{flagID("al-b").String(), flagID("al-d").String()}
	sort.Strings(want)
	if strings.Join(reviewed, ",") != strings.Join(want, ",") {
		t.Errorf("review_ip_allowlist names %v, want the orgs whose last entry was kept %v", reviewed, want)
	}
}

// Q1: a blind margin has no source state. The legacy flags stay as restored (switching them off is an outage, and for
// the allowlist it opens the org); the apply names every row that is active now, by id only, for review.
func TestABlindMarginLeavesTheLegacyFlagsAloneAndNamesThemForReview(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	early := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	db.legacyOrg(t, ctx, "blind-org-on", true)
	db.legacyOrg(t, ctx, "blind-org-off", false)
	db.allowlistEntry(t, ctx, "blind-org-on", "blind-entry", "10.9.0.0/16")
	db.integrationCredential(t, ctx, "blind-cred")
	db.integration(t, ctx, "blind-int-on", true)
	db.integration(t, ctx, "blind-int-off", false)
	db.installation(t, ctx, "blind-inst-live", 201, nil)
	db.installation(t, ctx, "blind-inst-suspended", 202, &early)

	report, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for table, want := range map[string]map[string]bool{
		"organizations":           {"blind-org-on": true, "blind-org-off": false},
		"org_ip_allowlist":        {"blind-entry": true},
		"integration_credentials": {"blind-cred": true},
		"integrations":            {"blind-int-on": true, "blind-int-off": false},
	} {
		for name, on := range want {
			if got := db.flag(t, ctx, table, name); got != on {
				t.Errorf("a blind margin changed %s %s to is_active = %v", table, name, got)
			}
		}
	}
	if !db.secretKept(t, ctx, "blind-cred") || db.suspendedAt(t, ctx, "blind-inst-live") != nil {
		t.Error("a blind margin changed a credential secret or an installation")
	}
	got := map[string][]string{}
	for _, a := range report.Actions {
		if a.Kind == ActionKind("review_after_blind_margin") {
			got[a.Source] = append(got[a.Source], a.IDs...)
		}
	}
	for source, query := range map[string]string{
		"public.organizations":            `SELECT id::text FROM public.organizations WHERE is_active ORDER BY 1`,
		"public.org_ip_allowlist.ids":     `SELECT id::text FROM public.org_ip_allowlist WHERE is_active ORDER BY 1`,
		"public.integrations":             `SELECT id::text FROM public.integrations WHERE is_active ORDER BY 1`,
		"public.integration_credentials":  `SELECT id::text FROM public.integration_credentials WHERE is_active ORDER BY 1`,
		"public.github_app_installations": `SELECT id::text FROM public.github_app_installations WHERE suspended_at IS NULL ORDER BY 1`,
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
		ids := append([]string(nil), got[source]...)
		sort.Strings(ids)
		if len(want) == 0 || strings.Join(ids, ",") != strings.Join(want, ",") {
			t.Errorf("blind-margin review of %s = %v, want every active id %v", source, ids, want)
		}
	}
	for _, line := range ActionLines(report.Actions) {
		if strings.Contains(line, "review_after_blind_margin") && (strings.Contains(line, "fence-flag-") || strings.Contains(line, flagSecret)) {
			t.Errorf("the review line carries a name or a value, not ids only: %s", line)
		}
	}
	assertNoSecret(t, ctx, db, nil, report)
}

// Q4 / 11878 (4): RotateBootstrap shortens an expiry after T; the restore brings the long one back. The fence keeps the
// earlier of the two and never extends an expiry.
func TestTheFenceKeepsTheEarlierCredentialExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	long := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	short := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	setExpiry := func(d *fenceDB, cred int, at *time.Time) {
		d.commit(t, ctx, "credential.expiry_changed", 0, func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE auth.credential_registry SET expires_at = $2 WHERE id = $1`, fenceCredID(cred), at)
			return err
		})
	}
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		setExpiry(d, 1, &long)
		setExpiry(d, 2, &short)
	}
	setExpiry(end, 1, &short)
	setExpiry(end, 2, &long)
	setExpiry(end, 3, &short)

	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	expiry := func(cred int) *time.Time {
		var at *time.Time
		if err := restored.admin.QueryRow(ctx, `SELECT expires_at FROM auth.credential_registry WHERE id = $1`, fenceCredID(cred)).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	for cred, want := range map[int]*time.Time{1: &short, 2: &short, 3: &short, 0: nil} {
		got := expiry(cred)
		if (got == nil) != (want == nil) || got != nil && !got.Equal(*want) {
			t.Errorf("credential %d expires_at = %v, want %v", cred, got, want)
		}
	}
	for _, s := range report.Sections {
		if s.Name == "auth.credential_registry.expiry" && s.Changed != 2 {
			t.Errorf("the expiry section changed %d credentials, want 2 (shortened after T, and set after T)", s.Changed)
		}
	}
}

// 11878 (2): the blind-margin digest names the margin, so a retry with a larger margin is applied (the revisions rise
// again) and the same margin again is a no-op.
func TestALargerBlindMarginIsNewWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	before := db.revisions(t, ctx)
	if _, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000}); err != nil {
		t.Fatal(err)
	}
	larger, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 2000})
	if err != nil || larger.AlreadyApplied || larger.MarginRows != int64(len(before)) {
		t.Fatalf("a larger blind margin = %+v, %v; want it applied to every revision", larger, err)
	}
	for k, v := range before {
		if got := db.revisions(t, ctx)[k]; got != v+3000 {
			t.Errorf("%s = %d, want %d", k, got, v+3000)
		}
	}
	if again, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 2000}); err != nil || !again.AlreadyApplied {
		t.Fatalf("the same margin again = %+v, %v; want already applied", again, err)
	}
	if n := db.count(t, ctx, `SELECT count(*) FROM auth.restore_fence_state WHERE source = 'blind_margin' AND manifest_sha256 ~ '^[0-9a-f]{64}$'`); n != 2 {
		t.Errorf("%d blind-margin rows with a 64-hex digest, want 2", n)
	}
}
