//go:build integration

package serviceaccount

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/orgdeprovision"
)

const keyDay = 24 * time.Hour

func (e *env) account(t *testing.T) (uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()
	orgID := e.seedOrg(t)
	owner := e.member(t, orgID)
	return orgID, owner, e.create(t, orgID, owner)
}

func (e *env) issue(t *testing.T, id uuid.UUID) IssuedKey {
	t.Helper()
	k, err := e.sa.IssueKey(ctxT(t), id, keyDay, okActor())
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}
	return k
}

func (e *env) rotate(t *testing.T, id uuid.UUID, overlap time.Duration) IssuedKey {
	t.Helper()
	k, err := e.sa.RotateKey(ctxT(t), id, overlap, keyDay, okActor())
	if err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	return k
}

func (e *env) keyWrites(t *testing.T, id uuid.UUID) int {
	t.Helper()
	return e.count(t, `SELECT (SELECT count(*) FROM auth.credential_registry WHERE principal_id = $1)
+ (SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type LIKE 'auth.service_account.key_%')
+ (SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_id = $2 AND event_type = 'service_account.key_changed')`, id, wire(id))
}

func (e *env) resolve(t *testing.T, k IssuedKey) (ResolvedKey, string) {
	t.Helper()
	key, err := e.sa.ResolveKey(ctxT(t), KeyDigest(k.Secret.Reveal()), time.Now())
	var refusal *KeyRefusal
	switch {
	case err == nil:
		return key, ""
	case errors.As(err, &refusal):
		return key, refusal.Reason
	}
	t.Fatalf("ResolveKey: %v", err)
	return key, ""
}

func (e *env) suspend(t *testing.T, orgID uuid.UUID) {
	t.Helper()
	e.commitSeed(t, `UPDATE auth.organizations SET disabled_at = clock_timestamp() WHERE id = $1`, orgID)
}

func deprovisionAllowed(context.Context, audit.TxOps, uuid.UUID) error { return nil }

func (e *env) deprovision(t *testing.T, orgID uuid.UUID) {
	t.Helper()
	if _, err := e.deprov.Deprovision(ctxT(t), orgdeprovision.Input{OrgID: orgID, Authorize: deprovisionAllowed}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
}

func (e *env) reactivate(t *testing.T, orgID uuid.UUID) {
	t.Helper()
	e.commitSeed(t, `UPDATE auth.organizations SET disabled_at = NULL WHERE id = $1`, orgID)
}

func TestSAKeyIssueWritesRowAuditOutboxRevision(t *testing.T) {
	e := getEnv(t)
	orgID, owner, id := e.account(t)
	actor := e.member(t, orgID)
	before := e.count(t, `SELECT coalesce(max(revision), 0)::int FROM auth.aggregate_revisions WHERE aggregate_type = 'service_account' AND aggregate_id = $1`, wire(id))
	k, err := e.sa.IssueKey(ctxT(t), id, keyDay, okActorFor(&actor))
	if err != nil {
		t.Fatal(err)
	}
	if k.Key.Principal != id || k.Key.ParentID != nil || k.Key.ExpiresAt.Before(time.Now().Add(23*time.Hour)) {
		t.Fatalf("key %+v", k.Key)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE id = $1 AND principal_id = $2 AND credential_class = 'service_account_key'
AND domain_owner = 'platform' AND created_by = $3 AND revoked_at IS NULL`, k.Key.ID, id, actor); n != 1 {
		t.Fatalf("%d registry rows", n)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type = 'auth.service_account.key_issued'
AND organization_id = $2 AND actor_principal_id = $3 AND attributes->>'reason_code' = 'credential_issued' AND attributes->>'credential_class' = 'service_account_key'
AND attributes->>'credential_id' = $4 AND attributes->>'principal_id' = $5 AND attributes->>'new_status' = 'active'`, id, orgID, actor, k.Key.ID.String(), owner.String()); n != 1 {
		t.Fatalf("%d audit rows", n)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_id = $1 AND event_type = 'service_account.key_changed'
AND payload->'change'->>'status' = 'active' AND payload->'change'->>'credential_id' = $2`, wire(id), "crd_"+k.Key.ID.String()); n != 1 {
		t.Fatalf("%d outbox rows", n)
	}
	if after := e.count(t, `SELECT coalesce(max(revision), 0)::int FROM auth.aggregate_revisions WHERE aggregate_type = 'service_account' AND aggregate_id = $1`, wire(id)); after != before+1 {
		t.Fatalf("revision %d, want %d", after, before+1)
	}
}

func TestSAKeyRowHoldsDigestPrefixFingerprintNoValue(t *testing.T) {
	e := getEnv(t)
	_, _, id := e.account(t)
	k := e.issue(t, id)
	value := k.Secret.Reveal()
	digest := KeyDigest(value)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE id = $1 AND secret_hash = $2 AND hash_algorithm = 'sha256'
AND display_prefix = 'fcsak_' AND fingerprint = encode($3::bytea, 'hex') AND audiences = '{}' AND scopes = '{}'`, k.Key.ID, digest[:], digest[:8]); n != 1 {
		t.Fatalf("%d rows hold the digest, prefix, fingerprint and empty sets", n)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry r WHERE id = $1 AND to_jsonb(r)::text LIKE '%' || $2 || '%'`, k.Key.ID, value[len(KeyPrefix):]); n != 0 {
		t.Fatal("the row holds the value")
	}
}

func TestSAKeyRefusalsWriteNothing(t *testing.T) {
	e := getEnv(t)
	cases := []struct {
		name   string
		live   bool
		set    func(t *testing.T, orgID, owner, id uuid.UUID)
		issue  error
		rotate error
	}{
		{"account disabled", true, func(t *testing.T, _, _, id uuid.UUID) {
			if err := e.sa.Disable(ctxT(t), id, okActor()); err != nil {
				t.Fatal(err)
			}
		}, ErrAccountDisabled, ErrAccountDisabled},
		{"principal disabled only", true, func(t *testing.T, _, _, id uuid.UUID) {
			e.commitSeed(t, `UPDATE auth.principals SET disabled_at = clock_timestamp() WHERE id = $1`, id)
		}, ErrAccountDisabled, ErrAccountDisabled},
		{"org suspended", true, func(t *testing.T, orgID, _, _ uuid.UUID) { e.suspend(t, orgID) }, ErrOrgNotActive, ErrOrgNotActive},
		{"owner not a member", true, func(t *testing.T, orgID, owner, _ uuid.UUID) { e.removeMember(t, orgID, owner) }, ErrOwnerNotMember, ErrOwnerNotMember},
		{"owner disabled", true, func(t *testing.T, _, owner, _ uuid.UUID) {
			e.commitSeed(t, `UPDATE auth.principals SET disabled_at = clock_timestamp() WHERE id = $1`, owner)
		}, ErrOwnerDisabled, ErrOwnerDisabled},
		{"one live key", true, func(*testing.T, uuid.UUID, uuid.UUID, uuid.UUID) {}, ErrKeyLive, nil},
		{"no key", false, func(*testing.T, uuid.UUID, uuid.UUID, uuid.UUID) {}, nil, ErrKeyNone},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			orgID, owner, id := e.account(t)
			if c.live {
				e.issue(t, id)
			}
			c.set(t, orgID, owner, id)
			before := e.keyWrites(t, id)
			if c.issue != nil {
				if _, err := e.sa.IssueKey(ctxT(t), id, keyDay, okActor()); !errors.Is(err, c.issue) {
					t.Fatalf("IssueKey: %v, want %v", err, c.issue)
				}
			}
			if c.rotate != nil {
				if _, err := e.sa.RotateKey(ctxT(t), id, time.Hour, keyDay, okActor()); !errors.Is(err, c.rotate) {
					t.Fatalf("RotateKey: %v, want %v", err, c.rotate)
				}
			}
			if after := e.keyWrites(t, id); after != before {
				t.Fatalf("a refusal wrote %d rows", after-before)
			}
		})
	}
	_, _, id := e.account(t)
	for _, d := range []time.Duration{0, -time.Second, 8760*time.Hour + time.Second} {
		if _, err := e.sa.IssueKey(ctxT(t), id, d, okActor()); !errors.Is(err, ErrKeyExpiry) {
			t.Fatalf("IssueKey %s: %v", d, err)
		}
	}
	e.issue(t, id)
	for _, d := range []time.Duration{-time.Second, 24*time.Hour + time.Second} {
		if _, err := e.sa.RotateKey(ctxT(t), id, d, keyDay, okActor()); !errors.Is(err, ErrKeyOverlap) {
			t.Fatalf("RotateKey overlap %s: %v", d, err)
		}
	}
	if _, err := e.sa.IssueKey(ctxT(t), uuid.New(), keyDay, okActor()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("IssueKey of no account: %v", err)
	}
}

func TestSAKeyRotateLineageAndParentExpiryOnlyLowers(t *testing.T) {
	e := getEnv(t)
	expiry := func(t *testing.T, key uuid.UUID) time.Time {
		var at time.Time
		if err := e.admin.QueryRow(ctxT(t), `SELECT expires_at FROM auth.credential_registry WHERE id = $1`, key).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	t.Run("long parent, overlap 1 h", func(t *testing.T) {
		_, _, id := e.account(t)
		parent := e.issue(t, id)
		next := e.rotate(t, id, time.Hour)
		if next.Key.ParentID == nil || *next.Key.ParentID != parent.Key.ID {
			t.Fatalf("parent %v, want %s", next.Key.ParentID, parent.Key.ID)
		}
		if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE id = $1 AND rotation_parent_id = $2 AND audiences = '{}' AND scopes = '{}'`, next.Key.ID, parent.Key.ID); n != 1 {
			t.Fatalf("%d successor rows", n)
		}
		got := expiry(t, parent.Key.ID)
		if !got.Equal(next.ParentExpiresAt) || got.After(time.Now().Add(time.Hour)) || got.Before(time.Now().Add(59*time.Minute)) {
			t.Fatalf("parent expires %s, answer %s, want about 1 h from now", got, next.ParentExpiresAt)
		}
	})
	t.Run("short parent, overlap 24 h keeps it", func(t *testing.T) {
		_, _, id := e.account(t)
		k, err := e.sa.IssueKey(ctxT(t), id, time.Hour, okActor())
		if err != nil {
			t.Fatal(err)
		}
		next := e.rotate(t, id, 24*time.Hour)
		if got := expiry(t, k.Key.ID); !got.Equal(k.Key.ExpiresAt) || !next.ParentExpiresAt.Equal(k.Key.ExpiresAt) {
			t.Fatalf("parent expires %s, want %s unchanged", got, k.Key.ExpiresAt)
		}
	})
	t.Run("overlap 0 cuts over", func(t *testing.T) {
		_, _, id := e.account(t)
		parent := e.issue(t, id)
		next := e.rotate(t, id, 0)
		if why := func() string { _, r := e.resolve(t, parent); return r }(); why != "expired" {
			t.Fatalf("the parent after overlap 0: %q", why)
		}
		if _, why := e.resolve(t, next); why != "" {
			t.Fatalf("the successor: %q", why)
		}
		if n := e.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type = 'auth.service_account.key_rotated'
AND attributes->>'reason_code' = 'credential_rotated' AND attributes->>'credential_id' = $2`, id, next.Key.ID.String()); n != 1 {
			t.Fatalf("%d rotate audit rows", n)
		}
	})
}

func TestSAKeySecondRotateRefusedInOverlap(t *testing.T) {
	e := getEnv(t)
	_, _, id := e.account(t)
	e.issue(t, id)
	e.rotate(t, id, time.Hour)
	before := e.keyWrites(t, id)
	if _, err := e.sa.RotateKey(ctxT(t), id, time.Hour, keyDay, okActor()); !errors.Is(err, ErrKeyRotating) {
		t.Fatalf("second rotate: %v", err)
	}
	if _, err := e.sa.IssueKey(ctxT(t), id, keyDay, okActor()); !errors.Is(err, ErrKeyLive) {
		t.Fatalf("issue in overlap: %v", err)
	}
	if after := e.keyWrites(t, id); after != before {
		t.Fatalf("a refusal wrote %d rows", after-before)
	}
}

func TestSAKeyRotateAfterParentExpiredRefused(t *testing.T) {
	e := getEnv(t)
	_, _, id := e.account(t)
	k := e.issue(t, id)
	e.commitSeed(t, `UPDATE auth.credential_registry SET expires_at = clock_timestamp() - interval '1 second' WHERE id = $1`, k.Key.ID)
	if _, err := e.sa.RotateKey(ctxT(t), id, time.Hour, keyDay, okActor()); !errors.Is(err, ErrKeyNone) {
		t.Fatalf("rotate of an expired key: %v", err)
	}
	if next, err := e.sa.IssueKey(ctxT(t), id, keyDay, okActor()); err != nil || next.Key.ParentID != nil {
		t.Fatalf("issue after expiry: %+v, %v", next.Key, err)
	}
}

func TestSAKeyRevokeInOverlapEitherKey(t *testing.T) {
	e := getEnv(t)
	for name, pick := range map[string]func(parent, next IssuedKey) (IssuedKey, IssuedKey){
		"parent":    func(p, n IssuedKey) (IssuedKey, IssuedKey) { return p, n },
		"successor": func(p, n IssuedKey) (IssuedKey, IssuedKey) { return n, p },
	} {
		t.Run(name, func(t *testing.T) {
			_, _, id := e.account(t)
			parent := e.issue(t, id)
			next := e.rotate(t, id, time.Hour)
			gone, kept := pick(parent, next)
			if err := e.sa.RevokeKey(ctxT(t), gone.Key.ID, KeyReasonCompromised, okActor()); err != nil {
				t.Fatal(err)
			}
			if _, why := e.resolve(t, gone); why != "revoked" {
				t.Fatalf("revoked key: %q", why)
			}
			if _, why := e.resolve(t, kept); why != "" {
				t.Fatalf("other key: %q", why)
			}
			if n := e.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type = 'auth.service_account.key_revoked'
AND attributes->>'reason_code' = 'compromised' AND attributes->>'new_status' = 'revoked' AND attributes->>'credential_id' = $2`, id, gone.Key.ID.String()); n != 1 {
				t.Fatalf("%d revoke audit rows", n)
			}
			if n := e.count(t, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_id = $1 AND event_type = 'service_account.key_changed'
AND payload->'change'->>'status' = 'revoked' AND payload->'change'->>'credential_id' = $2`, wire(id), "crd_"+gone.Key.ID.String()); n != 1 {
				t.Fatalf("%d revoke outbox rows", n)
			}
		})
	}
}

func TestSAKeyRevokeAllowedDisabledSuspendedLostOwnerNotOwned(t *testing.T) {
	e := getEnv(t)
	for name, set := range map[string]func(t *testing.T, orgID, owner, id uuid.UUID) *Store{
		"disabled": func(t *testing.T, _, _, id uuid.UUID) *Store {
			if err := e.sa.Disable(ctxT(t), id, okActor()); err != nil {
				t.Fatal(err)
			}
			return e.sa
		},
		"suspended":  func(t *testing.T, orgID, _, _ uuid.UUID) *Store { e.suspend(t, orgID); return e.sa },
		"lost owner": func(t *testing.T, orgID, owner, _ uuid.UUID) *Store { e.removeMember(t, orgID, owner); return e.sa },
		"not owned":  func(*testing.T, uuid.UUID, uuid.UUID, uuid.UUID) *Store { return e.notOwned },
	} {
		t.Run(name, func(t *testing.T) {
			orgID, owner, id := e.account(t)
			k := e.issue(t, id)
			store := set(t, orgID, owner, id)
			if err := store.RevokeKey(ctxT(t), k.Key.ID, KeyReasonOwnerRequest, okActor()); err != nil {
				t.Fatalf("RevokeKey: %v", err)
			}
			if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE id = $1 AND revoked_at IS NOT NULL AND revoked_reason = 'owner_request'`, k.Key.ID); n != 1 {
				t.Fatal("not revoked")
			}
		})
	}
	if _, err := e.notOwned.IssueKey(ctxT(t), uuid.New(), keyDay, okActor()); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("IssueKey not owned: %v", err)
	}
	if _, err := e.notOwned.RotateKey(ctxT(t), uuid.New(), time.Hour, keyDay, okActor()); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("RotateKey not owned: %v", err)
	}
	if err := e.sa.RevokeKey(ctxT(t), uuid.New(), KeyReasonOwnerRequest, okActor()); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("RevokeKey of no key: %v", err)
	}
	if err := e.sa.RevokeKey(ctxT(t), uuid.New(), "because", okActor()); !errors.Is(err, ErrKeyReason) {
		t.Fatalf("RevokeKey with a bad reason: %v", err)
	}
}

func TestSAKeyRepeatRevokeWritesNothing(t *testing.T) {
	e := getEnv(t)
	_, _, id := e.account(t)
	k := e.issue(t, id)
	if err := e.sa.RevokeKey(ctxT(t), k.Key.ID, KeyReasonOwnerRequest, okActor()); err != nil {
		t.Fatal(err)
	}
	before := e.keyWrites(t, id)
	if err := e.sa.RevokeKey(ctxT(t), k.Key.ID, KeyReasonCompromised, okActor()); err != nil {
		t.Fatalf("repeat: %v", err)
	}
	if after := e.keyWrites(t, id); after != before {
		t.Fatalf("a repeat wrote %d rows", after-before)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE id = $1 AND revoked_reason = 'owner_request'`, k.Key.ID); n != 1 {
		t.Fatal("a repeat changed the reason")
	}
}

func TestSAKeyListShowsLiveKeysOnly(t *testing.T) {
	e := getEnv(t)
	_, _, id := e.account(t)
	if keys, err := e.sa.ListKeys(ctxT(t), id); err != nil || len(keys) != 0 {
		t.Fatalf("no keys: %v, %v", keys, err)
	}
	a := e.issue(t, id)
	b := e.rotate(t, id, time.Hour)
	keys, err := e.sa.ListKeys(ctxT(t), id)
	if err != nil || len(keys) != 2 || keys[0].ID != a.Key.ID || keys[1].ID != b.Key.ID || keys[1].ParentID == nil || *keys[1].ParentID != a.Key.ID {
		t.Fatalf("two live keys: %+v, %v", keys, err)
	}
	if err := e.sa.RevokeKey(ctxT(t), a.Key.ID, KeyReasonOwnerRequest, okActor()); err != nil {
		t.Fatal(err)
	}
	if keys, err = e.sa.ListKeys(ctxT(t), id); err != nil || len(keys) != 1 || keys[0].ID != b.Key.ID {
		t.Fatalf("after a revoke: %+v, %v", keys, err)
	}
	e.commitSeed(t, `UPDATE auth.credential_registry SET expires_at = clock_timestamp() - interval '1 second' WHERE id = $1`, b.Key.ID)
	if keys, err = e.sa.ListKeys(ctxT(t), id); err != nil || len(keys) != 0 {
		t.Fatalf("after an expiry: %+v, %v", keys, err)
	}
	if _, err := e.sa.ListKeys(ctxT(t), uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no account: %v", err)
	}
}

func TestSAKeyResolveRefusals(t *testing.T) {
	e := getEnv(t)
	cases := []struct {
		name string
		set  func(t *testing.T, orgID, id uuid.UUID, k IssuedKey)
		want string
	}{
		{"revoked", func(t *testing.T, _, _ uuid.UUID, k IssuedKey) {
			if err := e.sa.RevokeKey(ctxT(t), k.Key.ID, KeyReasonOwnerRequest, okActor()); err != nil {
				t.Fatal(err)
			}
		}, "revoked"},
		{"expired", func(t *testing.T, _, _ uuid.UUID, k IssuedKey) {
			e.commitSeed(t, `UPDATE auth.credential_registry SET expires_at = clock_timestamp() - interval '1 second' WHERE id = $1`, k.Key.ID)
		}, "expired"},
		{"account disabled", func(t *testing.T, _, id uuid.UUID, _ IssuedKey) {
			if err := e.sa.Disable(ctxT(t), id, okActor()); err != nil {
				t.Fatal(err)
			}
		}, "account_disabled"},
		{"principal disabled", func(t *testing.T, _, id uuid.UUID, _ IssuedKey) {
			e.commitSeed(t, `UPDATE auth.principals SET disabled_at = clock_timestamp() WHERE id = $1`, id)
		}, "principal_disabled"},
		{"org suspended", func(t *testing.T, orgID, _ uuid.UUID, _ IssuedKey) { e.suspend(t, orgID) }, "org_suspended"},
		{"org deprovisioned", func(t *testing.T, orgID, _ uuid.UUID, _ IssuedKey) {
			e.deprovision(t, orgID)
		}, "org_suspended"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			orgID, _, id := e.account(t)
			k := e.issue(t, id)
			if key, why := e.resolve(t, k); why != "" || key.Principal != id || key.OrgID != orgID || key.CredentialID != k.Key.ID {
				t.Fatalf("live: %+v, %q", key, why)
			}
			c.set(t, orgID, id, k)
			if _, why := e.resolve(t, k); why != c.want {
				t.Fatalf("refusal %q, want %q", why, c.want)
			}
		})
	}
	t.Run("unknown digest", func(t *testing.T) {
		value, _ := MintKey()
		var refusal *KeyRefusal
		if _, err := e.sa.ResolveKey(ctxT(t), KeyDigest(value), time.Now()); !errors.As(err, &refusal) || refusal.Reason != "unknown" {
			t.Fatalf("%v", err)
		}
	})
	t.Run("a bootstrap digest on a workload is unknown", func(t *testing.T) {
		value, _ := MintKey()
		digest := KeyDigest(value)
		workload := uuid.New()
		e.commitSeed(t, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'workload', 'w')`, workload)
		e.commitSeed(t, `INSERT INTO auth.credential_registry (principal_id, credential_class, secret_hash, hash_algorithm, fingerprint, expires_at)
VALUES ($1, 'workload_bootstrap_credential', $2, 'sha256', encode($3::bytea, 'hex'), now() + interval '1 hour')`, workload, digest[:], digest[:8])
		var refusal *KeyRefusal
		if _, err := e.sa.ResolveKey(ctxT(t), digest, time.Now()); !errors.As(err, &refusal) || refusal.Reason != "unknown" {
			t.Fatalf("%v", err)
		}
	})
	t.Run("another class on a service account", func(t *testing.T) {
		_, _, id := e.account(t)
		value, _ := MintKey()
		digest := KeyDigest(value)
		e.commitSeed(t, `INSERT INTO auth.credential_registry (principal_id, credential_class, secret_hash, hash_algorithm, fingerprint, expires_at)
VALUES ($1, 'workload_bootstrap_credential', $2, 'sha256', encode($3::bytea, 'hex'), now() + interval '1 hour')`, id, digest[:], digest[:8])
		var refusal *KeyRefusal
		if _, err := e.sa.ResolveKey(ctxT(t), digest, time.Now()); !errors.As(err, &refusal) || refusal.Reason != "wrong_class" {
			t.Fatalf("%v", err)
		}
	})
}

func TestSAKeyResolveLiveAfterEnableAndReactivate(t *testing.T) {
	e := getEnv(t)
	orgID, _, id := e.account(t)
	k := e.issue(t, id)
	if err := e.sa.Disable(ctxT(t), id, okActor()); err != nil {
		t.Fatal(err)
	}
	if err := e.sa.Enable(ctxT(t), id, okActor()); err != nil {
		t.Fatal(err)
	}
	if _, why := e.resolve(t, k); why != "" {
		t.Fatalf("after Enable: %q", why)
	}
	e.suspend(t, orgID)
	if _, why := e.resolve(t, k); why != "org_suspended" {
		t.Fatalf("suspended: %q", why)
	}
	e.reactivate(t, orgID)
	key, why := e.resolve(t, k)
	if why != "" || key.Audiences == nil || key.Scopes == nil || len(key.Scopes) != 0 || key.HasScope("api:access") {
		t.Fatalf("after reactivate: %+v, %q", key, why)
	}
}

func TestSAKeyLostOwnerKeepsTheKeyLive(t *testing.T) {
	e := getEnv(t)
	orgID, owner, id := e.account(t)
	k := e.issue(t, id)
	e.removeMember(t, orgID, owner)
	if _, why := e.resolve(t, k); why != "" {
		t.Fatalf("lost owner: %q", why)
	}
	next := e.member(t, orgID)
	if err := e.sa.SetOwner(ctxT(t), id, next, okActor()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.sa.RotateKey(ctxT(t), id, time.Hour, keyDay, okActor()); err != nil {
		t.Fatalf("rotate after SetOwner: %v", err)
	}
}

func TestSAKeyNoCascadeRow(t *testing.T) {
	e := getEnv(t)
	orgID, _, id := e.account(t)
	k := e.issue(t, id)
	row := func() string {
		var s string
		if err := e.admin.QueryRow(ctxT(t), `SELECT to_jsonb(r)::text FROM auth.credential_registry r WHERE id = $1`, k.Key.ID).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	before := row()
	if err := e.sa.Disable(ctxT(t), id, okActor()); err != nil {
		t.Fatal(err)
	}
	if err := e.sa.Enable(ctxT(t), id, okActor()); err != nil {
		t.Fatal(err)
	}
	e.deprovision(t, orgID)
	if row() != before {
		t.Fatal("a lifecycle verb wrote the registry row")
	}
}

func TestSAKeyAuthorizeSeesTheOwnerUnderTheLocks(t *testing.T) {
	e := getEnv(t)
	orgID, owner, id := e.account(t)
	var seen []Locked
	see := func(_ context.Context, _ audit.TxOps, l Locked) error { seen = append(seen, l); return nil }
	k, err := e.sa.IssueKey(ctxT(t), id, keyDay, Actor{Authorize: see})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.sa.RotateKey(ctxT(t), id, time.Hour, keyDay, Actor{Authorize: see}); err != nil {
		t.Fatal(err)
	}
	if err := e.sa.RevokeKey(ctxT(t), k.Key.ID, KeyReasonOwnerRequest, Actor{Authorize: see}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 3 {
		t.Fatalf("Authorize ran %d times, want 3", len(seen))
	}
	for i, l := range seen {
		if l.OrgID != orgID || l.Owner != owner {
			t.Fatalf("call %d saw %+v", i, l)
		}
	}
}

func TestSAKeyNilAuthorizeRefusesAndWritesNothing(t *testing.T) {
	e := getEnv(t)
	_, _, id := e.account(t)
	k := e.issue(t, id)
	before := e.keyWrites(t, id)
	if _, err := e.sa.RotateKey(ctxT(t), id, time.Hour, keyDay, Actor{}); !errors.Is(err, ErrDenied) {
		t.Fatalf("RotateKey: %v", err)
	}
	if err := e.sa.RevokeKey(ctxT(t), k.Key.ID, KeyReasonOwnerRequest, Actor{}); !errors.Is(err, ErrDenied) {
		t.Fatalf("RevokeKey: %v", err)
	}
	_, _, fresh := e.account(t)
	if _, err := e.sa.IssueKey(ctxT(t), fresh, keyDay, Actor{}); !errors.Is(err, ErrDenied) {
		t.Fatalf("IssueKey: %v", err)
	}
	if after := e.keyWrites(t, id) + e.keyWrites(t, fresh); after != before {
		t.Fatalf("a nil Authorize wrote %d rows", after-before)
	}
}

func TestSAKeyActorDeniedWritesNothing(t *testing.T) {
	e := getEnv(t)
	_, _, id := e.account(t)
	k := e.issue(t, id)
	before := e.keyWrites(t, id)
	deny := Actor{Authorize: func(context.Context, audit.TxOps, Locked) error { return ErrDenied }}
	if err := e.sa.RevokeKey(ctxT(t), k.Key.ID, KeyReasonOwnerRequest, deny); !errors.Is(err, ErrDenied) {
		t.Fatalf("RevokeKey: %v", err)
	}
	if _, err := e.sa.RotateKey(ctxT(t), id, time.Hour, keyDay, deny); !errors.Is(err, ErrDenied) {
		t.Fatalf("RotateKey: %v", err)
	}
	if after := e.keyWrites(t, id); after != before {
		t.Fatalf("a denied actor wrote %d rows", after-before)
	}
}

func TestSAKeyStateChangedBeforeLocks(t *testing.T) {
	e := getEnv(t)
	orgID, _, id := e.account(t)
	next := e.member(t, orgID)
	raced := *e.sa
	raced.read = func(ctx context.Context, id uuid.UUID) (*Info, error) {
		info, err := e.sa.Lookup(ctx, id)
		if err == nil {
			e.commitSeed(t, `UPDATE auth.service_accounts SET owner_principal_id = $2 WHERE principal_id = $1`, id, next)
		}
		return info, err
	}
	before := e.keyWrites(t, id)
	if _, err := raced.IssueKey(ctxT(t), id, keyDay, okActor()); !errors.Is(err, ErrStateChanged) {
		t.Fatalf("IssueKey: %v", err)
	}
	if after := e.keyWrites(t, id); after != before {
		t.Fatalf("wrote %d rows", after-before)
	}
}

func TestSAKeyRevokeReadsAgainOnOneRace(t *testing.T) {
	e := getEnv(t)
	orgID, _, id := e.account(t)
	next := e.member(t, orgID)
	k := e.issue(t, id)
	moves := 0
	raced := *e.sa
	raced.read = func(ctx context.Context, id uuid.UUID) (*Info, error) {
		info, err := e.sa.Lookup(ctx, id)
		if err == nil && moves == 0 {
			moves++
			e.commitSeed(t, `UPDATE auth.service_accounts SET owner_principal_id = $2 WHERE principal_id = $1`, id, next)
		}
		return info, err
	}
	if err := raced.RevokeKey(ctxT(t), k.Key.ID, KeyReasonCompromised, okActor()); err != nil {
		t.Fatalf("RevokeKey after one race: %v", err)
	}
	if n := e.count(t, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type = 'auth.service_account.key_revoked'
AND attributes->>'principal_id' = $2`, id, next.String()); n != 1 {
		t.Fatalf("%d revoke rows name the owner read under the lock", n)
	}
}

func TestSAKeyAuditFailureLeavesNoRow(t *testing.T) {
	e := getEnv(t)
	_, _, id := e.account(t)
	failing := *e.sa
	failing.commit = func(ctx context.Context, m audit.Mutation) error {
		m.Audit.Attributes["not_on_the_allowlist"] = "x"
		return audit.Commit(ctx, e.runtime, e.schema, m)
	}
	if _, err := failing.IssueKey(ctxT(t), id, keyDay, okActor()); !errors.Is(err, ErrStore) {
		t.Fatalf("IssueKey: %v, want ErrStore", err)
	}
	if n := e.keyWrites(t, id); n != 0 {
		t.Fatalf("%d rows after a refused audit row", n)
	}
}

func TestSAKeyDuplicateDigestFaultHoldsNoDigest(t *testing.T) {
	e := getEnv(t)
	_, _, a := e.account(t)
	_, _, b := e.account(t)
	value, err := MintKey()
	if err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	same := *e.sa
	same.mint = func() (string, error) { return value, nil }
	same.logger = slog.New(slog.NewTextHandler(&log, nil))
	if _, err := same.IssueKey(ctxT(t), a, keyDay, okActor()); err != nil {
		t.Fatal(err)
	}
	_, err = same.IssueKey(ctxT(t), b, keyDay, okActor())
	if !errors.Is(err, ErrStore) {
		t.Fatalf("a repeated digest: %v, want ErrStore", err)
	}
	digest := KeyDigest(value)
	for _, text := range []string{err.Error(), log.String()} {
		if !strings.Contains(text, "23505") || strings.Contains(strings.ToLower(text), "secret_hash") {
			t.Fatalf("the fault text %q has no SQLSTATE or names the column", text)
		}
	}
	if n := e.keyWrites(t, b); n != 0 {
		t.Fatalf("%d rows for the second account (%x)", n, digest[:2])
	}
}

func TestSAKeyDirectWriteRefused(t *testing.T) {
	e := getEnv(t)
	_, _, id := e.account(t)
	value, _ := MintKey()
	digest := KeyDigest(value)
	_, err := e.runtime.Exec(ctxT(t), `INSERT INTO auth.credential_registry (principal_id, credential_class, secret_hash, hash_algorithm, fingerprint, expires_at)
VALUES ($1, 'service_account_key', $2, 'sha256', encode($3::bytea, 'hex'), now() + interval '1 hour')`, id, digest[:], digest[:8])
	if err == nil {
		t.Fatal("a registry insert outside audit.Commit succeeded")
	}
}

func TestSAKeySecretAbsentFromAuditOutboxAndLogs(t *testing.T) {
	e := getEnv(t)
	_, _, id := e.account(t)
	var log bytes.Buffer
	logged := *e.sa
	logged.logger = slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	k, err := logged.IssueKey(ctxT(t), id, keyDay, okActor())
	if err != nil {
		t.Fatal(err)
	}
	next, err := logged.RotateKey(ctxT(t), id, time.Hour, keyDay, okActor())
	if err != nil {
		t.Fatal(err)
	}
	if err := logged.RevokeKey(ctxT(t), k.Key.ID, KeyReasonOwnerRequest, okActor()); err != nil {
		t.Fatal(err)
	}
	if _, err := logged.ResolveKey(ctxT(t), KeyDigest(k.Secret.Reveal()), time.Now()); err == nil {
		t.Fatal("a revoked key resolved")
	}
	for _, value := range []string{k.Secret.Reveal(), next.Secret.Reveal()} {
		body := value[len(KeyPrefix):]
		if strings.Contains(log.String(), body) {
			t.Fatal("a log line holds the value")
		}
		if n := e.count(t, `SELECT (SELECT count(*) FROM auth.security_audit_events r WHERE subject_principal_id = $1 AND to_jsonb(r)::text LIKE '%' || $2 || '%')
+ (SELECT count(*) FROM auth.auth_outbox_events r WHERE aggregate_id = $3 AND to_jsonb(r)::text LIKE '%' || $2 || '%')`, id, body, wire(id)); n != 0 {
			t.Fatalf("%d audit or outbox rows hold the value", n)
		}
	}
}

// keyPair runs two verbs at once. A store fault, a deadlock among them, fails the cell.
func keyPair(t *testing.T, cell string, first, second func() error) {
	t.Helper()
	answers := make(chan error, 2)
	go func() { answers <- first() }()
	go func() { answers <- second() }()
	for i := 0; i < 2; i++ {
		select {
		case err := <-answers:
			if errors.Is(err, ErrStore) || isDeadlock(err) {
				t.Fatalf("%s: %v", cell, err)
			}
		case <-time.After(60 * time.Second):
			t.Fatalf("%s: a verb did not finish", cell)
		}
	}
}

func TestSAKeyPairRotateRotate(t *testing.T) {
	e := getEnv(t)
	_, _, id := e.account(t)
	e.issue(t, id)
	rotate := func() error { _, err := e.sa.RotateKey(ctxT(t), id, time.Hour, keyDay, okActor()); return err }
	keyPair(t, "rotate/rotate", rotate, rotate)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE principal_id = $1 AND revoked_at IS NULL AND expires_at > now()`, id); n != 2 {
		t.Fatalf("%d live keys after two rotations at once, want 2", n)
	}
}

func TestSAKeyPairRotateRevoke(t *testing.T) {
	e := getEnv(t)
	_, _, id := e.account(t)
	k := e.issue(t, id)
	keyPair(t, "rotate/revoke",
		func() error { _, err := e.sa.RotateKey(ctxT(t), id, time.Hour, keyDay, okActor()); return err },
		func() error { return e.sa.RevokeKey(ctxT(t), k.Key.ID, KeyReasonCompromised, okActor()) })
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE id = $1 AND revoked_at IS NOT NULL`, k.Key.ID); n != 1 {
		t.Fatal("the parent is not revoked")
	}
}

func TestSAKeyPairIssueDisable(t *testing.T) {
	e := getEnv(t)
	_, _, id := e.account(t)
	keyPair(t, "issue/disable",
		func() error { _, err := e.sa.IssueKey(ctxT(t), id, keyDay, okActor()); return err },
		func() error { return e.sa.Disable(ctxT(t), id, okActor()) })
	if n := e.count(t, `SELECT count(*) FROM auth.service_accounts WHERE principal_id = $1 AND status = 'disabled'`, id); n != 1 {
		t.Fatal("the account is not disabled")
	}
}

func TestSAKeyPairIssueSetOwner(t *testing.T) {
	e := getEnv(t)
	orgID, _, id := e.account(t)
	next := e.member(t, orgID)
	keyPair(t, "issue/set owner",
		func() error { _, err := e.sa.IssueKey(ctxT(t), id, keyDay, okActor()); return err },
		func() error { return e.sa.SetOwner(ctxT(t), id, next, okActor()) })
}

func TestSAKeyPairIssueOwnerRemoval(t *testing.T) {
	e := getEnv(t)
	orgID, owner, id := e.account(t)
	keyPair(t, "issue/owner removal",
		func() error { _, err := e.sa.IssueKey(ctxT(t), id, keyDay, okActor()); return err },
		func() error {
			_, err := e.members.Remove(ctxT(t), membership.Change{OrgID: orgID, PrincipalID: owner, Authorize: allowAll})
			return err
		})
}

func TestSAKeyPairIssueOrgDeprovision(t *testing.T) {
	e := getEnv(t)
	orgID, _, id := e.account(t)
	keyPair(t, "issue/deprovision",
		func() error { _, err := e.sa.IssueKey(ctxT(t), id, keyDay, okActor()); return err },
		func() error {
			_, err := e.deprov.Deprovision(ctxT(t), orgdeprovision.Input{OrgID: orgID, Authorize: deprovisionAllowed})
			return err
		})
}

// KeyByID reads a key in any state. The id is the caller's: a credential of another class is not found.
func TestSAKeyByIDReadsAKeyAndFiltersTheClass(t *testing.T) {
	e := getEnv(t)
	actor := uuid.New()
	e.commitSeed(t, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'keyactor')`, actor)
	_, _, id := e.account(t)
	k, err := e.sa.IssueKey(ctxT(t), id, keyDay, okActorFor(&actor))
	if err != nil {
		t.Fatal(err)
	}
	digest := KeyDigest(k.Secret.Reveal())
	got, err := e.sa.KeyByID(ctxT(t), k.Key.ID)
	if err != nil || got.ID != k.Key.ID || got.Principal != id || got.Fingerprint != hex.EncodeToString(digest[:8]) || got.RevokedAt != nil ||
		got.CreatedBy == nil || *got.CreatedBy != actor || !got.ExpiresAt.Equal(k.Key.ExpiresAt) {
		t.Fatalf("KeyByID: %+v, %v", got, err)
	}
	if k.Key.Fingerprint != got.Fingerprint || k.Key.CreatedBy == nil || *k.Key.CreatedBy != actor {
		t.Fatalf("the issue answer: %+v", k.Key)
	}
	if err := e.sa.RevokeKey(ctxT(t), k.Key.ID, KeyReasonCompromised, okActor()); err != nil {
		t.Fatal(err)
	}
	if got, err = e.sa.KeyByID(ctxT(t), k.Key.ID); err != nil || got.RevokedAt == nil || got.RevokedReason == nil || *got.RevokedReason != "compromised" {
		t.Fatalf("KeyByID after a revoke: %+v, %v", got, err)
	}
	if _, err := e.sa.KeyByID(ctxT(t), uuid.New()); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("no such id: %v", err)
	}
	value, _ := MintKey()
	other := KeyDigest(value)
	credential := uuid.New()
	e.commitSeed(t, `INSERT INTO auth.credential_registry (id, principal_id, credential_class, secret_hash, hash_algorithm, fingerprint, expires_at)
VALUES ($1, $2, 'workload_bootstrap_credential', $3, 'sha256', encode($4::bytea, 'hex'), now() + interval '1 hour')`, credential, id, other[:], other[:8])
	if _, err := e.sa.KeyByID(ctxT(t), credential); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("a credential of another class: %v", err)
	}
}
