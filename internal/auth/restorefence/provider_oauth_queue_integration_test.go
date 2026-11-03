//go:build integration

package restorefence

import (
	"context"
	"strings"
	"testing"
	"time"
)

type queuedRow struct {
	purpose, ciphertext, keyVersion, status string
	attempts                                int
	lastError                               *string
}

func (d *fenceDB) queuedRevocations(t *testing.T, ctx context.Context, k oauthKey) []queuedRow {
	t.Helper()
	rows, err := d.admin.Query(ctx, `SELECT purpose, token_encrypted, token_key_version, status, attempts, last_error
FROM public.provider_oauth_revocations WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2 ORDER BY purpose`, k.org, k.name)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []queuedRow
	for rows.Next() {
		var r queuedRow
		if err := rows.Scan(&r.purpose, &r.ciphertext, &r.keyVersion, &r.status, &r.attempts, &r.lastError); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// Q4 (cases A-D): a bound PagerDuty credential the source no longer holds (A) is queued as a disconnect; one the source
// holds under another binding (B) as a replacement: the stored ciphertext as is, key version oauth-v1, pending, attempts 0,
// no error, in the apply transaction. The same binding at a higher or a lower version (C), a NULL binding on either side,
// and a row the source holds unchanged (D) queue nothing. A second apply of the same manifest queues nothing more.
func TestTheFenceQueuesTheOldTokenOfADisconnectedOrReboundPagerDutyCredential(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	gone, rebound, up, down, held := oauthKey{"q-org", "gone"}, oauthKey{"q-org", "rebound"}, oauthKey{"q-org", "up"}, oauthKey{"q-org", "down"}, oauthKey{"q-org", "held"}
	nullT, nullS := oauthKey{"q-org", "null-restored"}, oauthKey{"q-org", "null-source"}
	b := oauthBinding()
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		for _, k := range []oauthKey{gone, rebound, held, nullS} {
			d.oauthRow(t, ctx, k, 2, b)
		}
		d.oauthRow(t, ctx, up, 2, b)
		d.oauthRow(t, ctx, down, 2, b)
		d.must(t, ctx, `INSERT INTO public.provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, binding_id)
VALUES ('q-org', 'pagerduty', 'null-restored', $1, 2, now(), now(), NULL)`, oauthSentinel)
	}
	end.must(t, ctx, `DELETE FROM public.provider_oauth_credentials WHERE credential_name = 'gone'`)
	end.must(t, ctx, `UPDATE public.provider_oauth_credentials SET binding_id = $1 WHERE credential_name = 'rebound'`, oauthBinding())
	end.must(t, ctx, `UPDATE public.provider_oauth_credentials SET version = 3 WHERE credential_name = 'up'`)
	end.must(t, ctx, `UPDATE public.provider_oauth_credentials SET version = 1 WHERE credential_name = 'down'`)
	end.must(t, ctx, `UPDATE public.provider_oauth_credentials SET version = 3 WHERE credential_name = 'null-restored'`)
	end.must(t, ctx, `UPDATE public.provider_oauth_credentials SET version = 3, binding_id = NULL WHERE credential_name = 'null-source'`)
	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for k, purpose := range map[oauthKey]string{gone: "disconnect", rebound: "replacement"} {
		got := restored.queuedRevocations(t, ctx, k)
		if len(got) != 1 || got[0].purpose != purpose || got[0].ciphertext != oauthSentinel || got[0].keyVersion != "oauth-v1" ||
			got[0].status != "pending" || got[0].attempts != 0 || got[0].lastError != nil {
			t.Errorf("%v: queued %+v, want one pending %s row, the stored ciphertext, oauth-v1, attempts 0", k, got, purpose)
		}
	}
	for _, k := range []oauthKey{up, down, held, nullT, nullS} {
		if got := restored.queuedRevocations(t, ctx, k); len(got) != 0 {
			t.Errorf("%v: queued %d rows, want none", k, len(got))
		}
	}
	if !restored.oauthPresent(t, ctx, held) {
		t.Error("the held credential was deleted")
	}
	sameSet(t, "queued", reviewIDs(report, "provider_revocation_queued_from_credential", oauthSection), oauthIDs(gone, rebound))
	sameSet(t, "manual", reviewIDs(report, "requeue_provider_revocation", oauthSection), oauthIDs(up, down, nullT, nullS))
	for _, s := range report.Sections {
		if s.Name == oauthSection+".queued" && s.Changed != 2 {
			t.Errorf("the queued count is %d, want 2", s.Changed)
		}
	}
	again, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil || !again.AlreadyApplied {
		t.Fatalf("second apply: already=%v err=%v", again.AlreadyApplied, err)
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM public.provider_oauth_revocations WHERE org_id = 'q-org'`); n != 2 {
		t.Errorf("after a second apply %d revocation rows, want 2", n)
	}
	assertNoOAuthToken(t, ctx, restored, manifest, report)
	if strings.Contains(strings.Join(ActionLines(report.Actions), "\n"), oauthSentinel) {
		t.Error("an action line carries the provider token")
	}
}

// A dry run queues nothing: the statement runs in the rolled-back transaction.
func TestADryRunQueuesNoCredentialRevocation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	k := oauthKey{"q-org", "dry"}
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.oauthRow(t, ctx, k, 1, oauthBinding())
	}
	end.must(t, ctx, `DELETE FROM public.provider_oauth_credentials WHERE credential_name = 'dry'`)
	report, err := Apply(ctx, restored.admin, restored.schema, exportEnd(t, ctx, end), Options{})
	if err != nil {
		t.Fatal(err)
	}
	sameSet(t, "dry-run queued", reviewIDs(report, "provider_revocation_queued_from_credential", oauthSection), oauthIDs(k))
	if n := restored.count(t, ctx, `SELECT count(*) FROM public.provider_oauth_revocations`); n != 0 {
		t.Errorf("a dry run left %d revocation rows", n)
	}
}
