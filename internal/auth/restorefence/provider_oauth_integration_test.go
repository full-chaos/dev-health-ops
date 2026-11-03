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
)

// oauthSentinel is the stored token of the fixture, built at run time; it must never reach a manifest, a report or the
// stored operator actions.
var oauthSentinel = strings.Join([]string{"fence", "oauth", "sentinel", "7c2e"}, "-")

type oauthKey struct{ org, name string }

func oauthBinding() string { return strings.ReplaceAll(uuid.NewString(), "-", "") }

func (d *fenceDB) oauthRow(t *testing.T, ctx context.Context, k oauthKey, version int, binding string) {
	t.Helper()
	d.must(t, ctx, `INSERT INTO public.provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, binding_id)
VALUES ($1, 'pagerduty', $2, $3, $4, now(), now(), $5)`, k.org, k.name, oauthSentinel, version, binding)
}

func (d *fenceDB) oauthPresent(t *testing.T, ctx context.Context, k oauthKey) bool {
	t.Helper()
	return d.count(t, ctx, `SELECT count(*) FROM public.provider_oauth_credentials WHERE org_id = '`+strings.ReplaceAll(k.org, "'", "''")+
		`' AND provider = 'pagerduty' AND credential_name = '`+strings.ReplaceAll(k.name, "'", "''")+`'`) == 1
}

func oauthID(k oauthKey) string {
	enc := strings.NewReplacer("%", "%25", "/", "%2F", ",", "%2C", "\n", "%0A", "\r", "%0D").Replace
	return enc(k.org) + "/pagerduty/" + enc(k.name)
}

func oauthIDs(keys ...oauthKey) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = oauthID(k)
	}
	sort.Strings(out)
	return out
}

func assertNoOAuthToken(t *testing.T, ctx context.Context, d *fenceDB, m *Manifest, report Report) {
	t.Helper()
	if m != nil {
		raw, err := m.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), oauthSentinel) || strings.Contains(string(raw), "token_encrypted") {
			t.Error("the manifest carries the provider token or its column")
		}
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), oauthSentinel) {
		t.Error("the apply report carries the provider token")
	}
	if n := d.count(t, ctx, `SELECT count(*) FROM auth.restore_fence_state WHERE operator_actions::text LIKE '%`+oauthSentinel+`%'`); n != 0 {
		t.Error("the stored operator actions carry the provider token")
	}
}

// Q1: a provider credential rotated after T (refresh: version + 1), reconnected (new binding), disconnected (deleted),
// or deleted and connected again (version 1 again, new binding) comes back with the old token. The fence deletes the
// restored row. A refresh (same binding) is named for the manual revoke (requeue_provider_revocation); a disconnect or a
// new binding is queued for the revocation worker and named provider_revocation_queued_from_credential (encoded ids). An
// unchanged credential is kept; one created after T stays absent.
func TestTheFenceDeletesAProviderTokenRotatedOrRemovedAfterT(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	keep, rot, rec, dis, again := oauthKey{"fence-org", "keep"}, oauthKey{"fence-org", "rot"}, oauthKey{"fence-org", "rec"}, oauthKey{"fence-org", "dis"}, oauthKey{"fence-org", "again"}
	odd := oauthKey{"fence/org,1", "team/a,b"}
	b1 := oauthBinding()
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		for _, k := range []oauthKey{keep, rot, rec, dis, again, odd} {
			d.oauthRow(t, ctx, k, 1, b1)
		}
	}
	end.must(t, ctx, `UPDATE public.provider_oauth_credentials SET version = version + 1 WHERE credential_name IN ('rot', 'team/a,b')`)
	end.must(t, ctx, `UPDATE public.provider_oauth_credentials SET version = version + 1, binding_id = $1 WHERE credential_name = 'rec'`, oauthBinding())
	end.must(t, ctx, `DELETE FROM public.provider_oauth_credentials WHERE credential_name IN ('dis', 'again')`)
	end.oauthRow(t, ctx, again, 1, oauthBinding())
	end.oauthRow(t, ctx, oauthKey{"fence-org", "new"}, 1, oauthBinding())
	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for k, want := range map[oauthKey]bool{keep: true, rot: false, rec: false, dis: false, again: false, odd: false, {"fence-org", "new"}: false} {
		if got := restored.oauthPresent(t, ctx, k); got != want {
			t.Errorf("provider credential %v present = %v, want %v", k, got, want)
		}
	}
	for _, s := range report.Sections {
		if s.Name == "public.provider_oauth_credentials" && s.Changed != 5 {
			t.Errorf("the provider credential section deleted %d rows, want 5", s.Changed)
		}
	}
	sameSet(t, "requeue_provider_revocation (credentials)", reviewIDs(report, "requeue_provider_revocation", "public.provider_oauth_credentials"), oauthIDs(rot, odd))
	sameSet(t, "provider_revocation_queued_from_credential", reviewIDs(report, "provider_revocation_queued_from_credential", "public.provider_oauth_credentials"), oauthIDs(rec, dis, again))
	assertNoOAuthToken(t, ctx, restored, manifest, report)
}

// C4: a credential the source holds unchanged is kept with no write.
func TestAnUnchangedProviderCredentialIsKeptWithNoWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	k := oauthKey{"fence-org", "steady"}
	b := oauthBinding()
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.oauthRow(t, ctx, k, 3, b)
	}
	var before time.Time
	if err := restored.admin.QueryRow(ctx, `SELECT updated_at FROM public.provider_oauth_credentials WHERE credential_name = 'steady'`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	report, err := Apply(ctx, restored.admin, restored.schema, exportEnd(t, ctx, end), Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for _, s := range report.Sections {
		if s.Name == "public.provider_oauth_credentials" && s.Changed != 0 {
			t.Errorf("an unchanged credential: %d rows written, want 0", s.Changed)
		}
	}
	var after time.Time
	if err := restored.admin.QueryRow(ctx, `SELECT updated_at FROM public.provider_oauth_credentials WHERE credential_name = 'steady'`).Scan(&after); err != nil || !after.Equal(before) {
		t.Errorf("the unchanged credential was written (%v -> %v, %v)", before, after, err)
	}
	if ids := reviewIDs(report, "requeue_provider_revocation", "public.provider_oauth_credentials"); len(ids) != 0 {
		t.Errorf("an unchanged credential was named: %v", ids)
	}
}

// Q3: a blind margin has no source state: no provider credential is deleted, and every one is listed (encoded ids) in
// review_after_blind_margin.
func TestABlindMarginKeepsProviderCredentialsAndListsThem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	keys := []oauthKey{{"fence-org", "one"}, {"fence/org,2", "two/b,c"}}
	for _, k := range keys {
		db.oauthRow(t, ctx, k, 2, oauthBinding())
	}
	report, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if !db.oauthPresent(t, ctx, k) {
			t.Errorf("a blind margin deleted provider credential %v", k)
		}
	}
	sameSet(t, "blind-margin review of provider credentials", reviewIDs(report, "review_after_blind_margin", "public.provider_oauth_credentials"), oauthIDs(keys...))
	assertNoOAuthToken(t, ctx, db, nil, report)
}
