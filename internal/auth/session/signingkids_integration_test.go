//go:build integration

package session

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func (e *env) signingKids(t *testing.T, ctx context.Context, id uuid.UUID) *[]string {
	t.Helper()
	var kids *[]string
	if err := e.admin.QueryRow(ctx, `SELECT signing_kids FROM auth.sessions WHERE id = $1`, id.String()).Scan(&kids); err != nil {
		t.Fatal(err)
	}
	return kids
}

func TestARecordedKidIsAppendedOnceAndNewSessionsStartUnrecorded(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	issued := e.create(t, ctx, e.principal(t, ctx))
	id := issued.Session.ID
	if e.signingKids(t, ctx, id) != nil {
		t.Fatal("a new session has recorded kids; NULL means never recorded")
	}
	for _, kid := range []string{"a", "a", "b", "a", "b"} {
		if err := e.store.RecordSigningKid(ctx, id, kid); err != nil {
			t.Fatalf("record %s: %v", kid, err)
		}
	}
	got := e.signingKids(t, ctx, id)
	if got == nil || len(*got) != 2 || (*got)[0] != "a" || (*got)[1] != "b" {
		t.Fatalf("signing_kids = %v, want [a b]", got)
	}
}

func TestAnHS256MintMakesTheSetEmptyAndALaterKidAppendsToIt(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	id := e.create(t, ctx, e.principal(t, ctx)).Session.ID
	if err := e.store.RecordSigningKid(ctx, id, ""); err != nil {
		t.Fatal(err)
	}
	if got := e.signingKids(t, ctx, id); got == nil || len(*got) != 0 {
		t.Fatalf("after an hs256 mint signing_kids = %v, want {} (not NULL)", got)
	}
	if err := e.store.RecordSigningKid(ctx, id, ""); err != nil {
		t.Fatal(err)
	}
	if err := e.store.RecordSigningKid(ctx, id, "k1"); err != nil {
		t.Fatal(err)
	}
	if got := e.signingKids(t, ctx, id); got == nil || len(*got) != 1 || (*got)[0] != "k1" {
		t.Fatalf("signing_kids = %v, want [k1]", got)
	}
	if err := e.store.RecordSigningKid(ctx, id, ""); err != nil {
		t.Fatal(err)
	}
	if got := e.signingKids(t, ctx, id); got == nil || len(*got) != 1 {
		t.Fatalf("an hs256 mint changed a recorded set: %v", got)
	}
}

func TestConcurrentRecordsOfOneKidLeaveItOnce(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	id := e.create(t, ctx, e.principal(t, ctx)).Session.ID
	var wg sync.WaitGroup
	errs := make([]error, 16)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			kid := "a"
			if i%2 == 1 {
				kid = "b"
			}
			errs[i] = e.store.RecordSigningKid(ctx, id, kid)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := e.signingKids(t, ctx, id); got == nil || len(*got) != 2 {
		t.Fatalf("signing_kids = %v, want a and b once each", got)
	}
}

func TestRecordingAKidOfAMissingSessionIsRefused(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	if err := e.store.RecordSigningKid(ctx, uuid.New(), "a"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("err = %v, want ErrSessionNotFound", err)
	}
	if err := e.store.RecordSigningKid(ctx, uuid.New(), ""); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("empty kid: err = %v, want ErrSessionNotFound", err)
	}
}
