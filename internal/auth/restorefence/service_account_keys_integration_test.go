//go:build integration

package restorefence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func fenceKeyID(name string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-sa-key-"+name))
}

func (d *fenceDB) insertKey(t *testing.T, ctx context.Context, name string, expires time.Time, parent *uuid.UUID) {
	t.Helper()
	principal := uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-principal"))
	d.commit(t, ctx, "credential.created", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `INSERT INTO auth.credential_registry
(id, principal_id, credential_class, display_prefix, secret_hash, hash_algorithm, fingerprint, expires_at, rotation_parent_id)
VALUES ($1, $2, 'service_account_key', 'fcsak_', decode(md5($1::uuid::text), 'hex'), 'sha256', substr(md5($1::uuid::text), 1, 16), $3, $4)`,
			fenceKeyID(name), principal, expires, parent)
		return err
	})
}

// A restore to T drops the key writes after it. The fence keeps a revocation and a shortened parent; a key issued or
// rotated in after T stays gone: nothing brings it back, so the fence never leaves a key live that the source had
// ended, and it leaves the empty scope sets as they are, naming no review.
func TestTheFenceOnServiceAccountKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := newFenceDB(t, ctx), newFenceDB(t, ctx)
	long := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	short := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.insertKey(t, ctx, "rotated", long, nil)
		d.insertKey(t, ctx, "revoked", long, nil)
		d.insertKey(t, ctx, "kept", long, nil)
	}
	parent := fenceKeyID("rotated")
	end.commit(t, ctx, "credential.rotated", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.credential_registry SET expires_at = $2 WHERE id = $1`, parent, short)
		return err
	})
	end.insertKey(t, ctx, "successor", long, &parent)
	end.insertKey(t, ctx, "issued-after-t", long, nil)
	end.commit(t, ctx, "credential.revoked", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.credential_registry SET revoked_at = now(), revoked_reason = 'compromised' WHERE id = $1`, fenceKeyID("revoked"))
		return err
	})

	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	state := func(name string) (expires time.Time, reason *string, found bool) {
		found = true
		err := restored.admin.QueryRow(ctx, `SELECT expires_at, revoked_reason FROM auth.credential_registry WHERE id = $1`, fenceKeyID(name)).Scan(&expires, &reason)
		if err != nil {
			if strings.Contains(err.Error(), "no rows") {
				return time.Time{}, nil, false
			}
			t.Fatal(err)
		}
		return expires, reason, found
	}
	if at, _, found := state("rotated"); !found || !at.Equal(short) {
		t.Errorf("the rotated parent expires %s (found %v), want the source's %s", at, found, short)
	}
	if _, reason, found := state("revoked"); !found || reason == nil || *reason != "compromised" {
		t.Errorf("the revoked key: reason %v, found %v; want compromised", reason, found)
	}
	if at, reason, found := state("kept"); !found || reason != nil || !at.Equal(long) {
		t.Errorf("the untouched key changed: %s %v %v", at, reason, found)
	}
	for _, name := range []string{"successor", "issued-after-t"} {
		if _, _, found := state(name); found {
			t.Errorf("the key %s written after T came back", name)
		}
	}
	var widened int
	if err := restored.admin.QueryRow(ctx, `SELECT count(*) FROM auth.credential_registry
WHERE credential_class = 'service_account_key' AND (cardinality(scopes) > 0 OR cardinality(audiences) > 0)`).Scan(&widened); err != nil || widened != 0 {
		t.Errorf("%d keys hold a scope or an audience (%v)", widened, err)
	}
	for _, action := range report.Actions {
		if action.Kind == ActionRotateKeysAfterBlindMargin {
			t.Errorf("a manifest apply named the key count: %+v", action)
		}
		for _, id := range action.IDs {
			for _, name := range []string{"rotated", "revoked", "kept", "successor", "issued-after-t"} {
				if strings.Contains(id, fenceKeyID(name).String()) {
					t.Errorf("action %s names the key %s", action.Kind, name)
				}
			}
		}
	}
	assertNoSecret(t, ctx, restored, manifest, report)
}

func (d *fenceDB) insertOtherClass(t *testing.T, ctx context.Context, name string, expires time.Time) {
	t.Helper()
	d.commit(t, ctx, "credential.created", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `INSERT INTO auth.credential_registry (id, principal_id, credential_class, secret_hash, hash_algorithm, fingerprint, expires_at)
VALUES ($1, $2, 'workload_token', decode(md5($1::uuid::text), 'hex'), 'sha256', '0123456789abcdef', $3)`,
			fenceKeyID(name), uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-principal")), expires)
		return err
	})
}

func (d *fenceDB) revokeKey(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	d.commit(t, ctx, "credential.revoked", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE auth.credential_registry SET revoked_at = now(), revoked_reason = 'compromised' WHERE id = $1`, fenceKeyID(name))
		return err
	})
}

func keyCountActions(report Report) []OperatorAction {
	var out []OperatorAction
	for _, action := range report.Actions {
		if action.Kind == ActionRotateKeysAfterBlindMargin {
			out = append(out, action)
		}
	}
	return out
}

// A blind margin has no source state, so it cannot undo a revocation made after T. It names the count of live keys:
// not a revoked key, not an expired key, not a credential of another class, and no id.
func TestABlindMarginNamesTheCountOfLiveKeysAndNoKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	future := time.Now().Add(24 * time.Hour)
	db.insertKey(t, ctx, "live-1", future, nil)
	db.insertKey(t, ctx, "live-2", future, nil)
	db.insertKey(t, ctx, "live-3", future, nil)
	db.insertKey(t, ctx, "revoked", future, nil)
	db.revokeKey(t, ctx, "revoked")
	db.insertKey(t, ctx, "expired", time.Now().Add(-time.Hour), nil)
	db.insertOtherClass(t, ctx, "other-class", future)

	report, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	found := keyCountActions(report)
	if len(found) != 1 || found[0].Count != 3 || len(found[0].IDs) != 0 || found[0].Source != "auth.credential_registry" {
		t.Fatalf("key actions = %+v, want one with count 3 and no id", found)
	}
	read, err := PendingActions(ctx, db.admin, db.schema)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Join(ActionLines(read), "\n")
	if !strings.Contains(lines, liveKeysLine) {
		t.Errorf("status lines hold no key line: %s", lines)
	}
	for _, name := range []string{"live-1", "live-2", "live-3", "revoked", "expired", "other-class"} {
		if strings.Contains(lines, fenceKeyID(name).String()) {
			t.Errorf("a line names the key %s", name)
		}
	}
	if strings.Contains(lines, "fcsak_") {
		t.Error("a line holds a key prefix")
	}
	assertNoSecret(t, ctx, db, nil, report)
}

func TestABlindMarginWithNoLiveKeyNamesNoKeyAction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db := newFenceDB(t, ctx)
	db.stateT(t, ctx)
	db.insertKey(t, ctx, "revoked", time.Now().Add(24*time.Hour), nil)
	db.revokeKey(t, ctx, "revoked")
	db.insertKey(t, ctx, "expired", time.Now().Add(-time.Hour), nil)
	report, err := Apply(ctx, db.admin, db.schema, nil, Options{Apply: true, BlindMargin: 1000})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if found := keyCountActions(report); len(found) != 0 {
		t.Errorf("no live key, but the report names %+v", found)
	}
}
