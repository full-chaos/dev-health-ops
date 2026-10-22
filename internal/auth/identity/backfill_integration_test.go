//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

func bcryptFor(t *testing.T, material string) *string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(material), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	s := string(h)
	return &s
}

func TestBackfillInsertsThenRerunWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	target := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bf"), DisplayName: "Display One", Active: true, Verified: true,
		PasswordHash: bcryptFor(t, "backfill-test-material"), Identity: &ExternalKey{Issuer: "https://github.com", Provider: ProviderGitHub, Subject: uuid.NewString()}}
	res, err := e.store.BackfillUser(ctx, target, false)
	if err != nil || !res.Inserted || !res.IdentityLinked {
		t.Fatalf("first run: %+v %v", res, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND password_algorithm = 'bcrypt' AND password_updated_at IS NULL AND email_verified_at IS NOT NULL`, target.ID); n != 1 {
		t.Fatalf("user row not as mapped: %d", n)
	}
	auditBefore := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1`, target.ID)
	res, err = e.store.BackfillUser(ctx, target, false)
	if err != nil || res != (BackfillResult{}) {
		t.Fatalf("rerun wrote something: %+v %v", res, err)
	}
	if after := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1`, target.ID); after != auditBefore {
		t.Fatalf("rerun wrote audit rows: %d -> %d", auditBefore, after)
	}
}

func TestBackfillDryRunWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	target := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfd"), Active: true}
	res, err := e.store.BackfillUser(ctx, target, true)
	if err != nil || !res.Inserted {
		t.Fatalf("dry run: %+v %v", res, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.principals WHERE id = $1`, target.ID); n != 0 {
		t.Fatalf("dry run wrote a row")
	}
}

func TestBackfillUpdatesOnlyWhatDiffers(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	target := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfu"), Active: true, Verified: false}
	if _, err := e.store.BackfillUser(ctx, target, false); err != nil {
		t.Fatal(err)
	}
	target.Active, target.Verified, target.PasswordHash = false, true, bcryptFor(t, "second-test-material")
	res, err := e.store.BackfillUser(ctx, target, false)
	if err != nil || !res.Updated || res.Inserted {
		t.Fatalf("update: %+v %v", res, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.principals p JOIN auth.users u ON u.principal_id = p.id
WHERE p.id = $1 AND p.disabled_at IS NOT NULL AND u.email_verified_at IS NOT NULL AND u.password_hash IS NOT NULL`, target.ID); n != 1 {
		t.Fatalf("row not updated: %d", n)
	}
	target.DisplayName = "Renamed"
	res, err = e.store.BackfillUser(ctx, target, false)
	if err != nil || !res.DisplayNameDrift || res.Updated {
		t.Fatalf("name drift: %+v %v", res, err)
	}
}

func TestBackfillEmailCollisionAndIdentityConflictAreRefused(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	email := uniqueEmail("bfc")
	first := BackfillTarget{ID: uuid.New(), Email: email, Active: true,
		Identity: &ExternalKey{Issuer: "https://gitlab.com", Provider: ProviderGitLab, Subject: uuid.NewString()}}
	if _, err := e.store.BackfillUser(ctx, first, false); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.BackfillUser(ctx, BackfillTarget{ID: uuid.New(), Email: email, Active: true}, false); !errors.Is(err, ErrEmailCollision) {
		t.Fatalf("collision: %v", err)
	}
	second := BackfillTarget{ID: uuid.New(), Email: uniqueEmail("bfc"), Active: true, Identity: first.Identity}
	if _, err := e.store.BackfillUser(ctx, second, false); !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("identity conflict: %v", err)
	}
}
