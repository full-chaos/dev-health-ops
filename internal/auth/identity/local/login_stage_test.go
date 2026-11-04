package local

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

type recordFails struct{ *fakeAttempts }

func (recordFails) RecordFailure(context.Context, []byte) error { return errors.New("write refused") }

// The three errors that come before the password is compared carry the stage
// mark (the login route gives the limiter charge back for them), and the
// error of a step after the compare does not.
func TestLoginErrorsBeforeTheCompareCarryTheStageMark(t *testing.T) {
	boom := errors.New("store down")
	t.Run("address that cannot be normalized", func(t *testing.T) {
		r := newRig(t, local(t))
		_, err := r.svc.Login(context.Background(), Credentials{Email: "not an address", Password: goodPassword})
		if !errors.Is(err, ErrBeforeCredentialCheck) {
			t.Fatalf("err = %v, want the stage mark", err)
		}
	})
	t.Run("user lookup", func(t *testing.T) {
		r := newRig(t, local(t))
		r.users.err = boom
		_, err := r.svc.Login(context.Background(), Credentials{Email: "a@example.com", Password: goodPassword})
		if !errors.Is(err, ErrBeforeCredentialCheck) || !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the stage mark and the cause", err)
		}
		if len(r.c.compares) != 0 {
			t.Fatal("a password was compared")
		}
	})
	t.Run("lockout check", func(t *testing.T) {
		r := newRig(t, local(t))
		r.attempts.checkErr = boom
		_, err := r.svc.Login(context.Background(), Credentials{Email: "a@example.com", Password: goodPassword})
		if !errors.Is(err, ErrBeforeCredentialCheck) || !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the stage mark and the cause", err)
		}
		if len(r.c.compares) != 0 {
			t.Fatal("a password was compared")
		}
	})
	t.Run("recording the failed attempt, after the compare", func(t *testing.T) {
		c := &calls{}
		users := &fakeUsers{c: c, user: local(t)}
		svc, err := New(Config{Users: users, Sessions: &fakeSessions{c: c}, Attempts: recordFails{&fakeAttempts{c: c}},
			LockoutKey: contracts.NewSecretString(testKey)})
		if err != nil {
			t.Fatal(err)
		}
		_, err = svc.Login(context.Background(), Credentials{Email: "a@example.com", Password: "wrong"})
		if err == nil || errors.Is(err, ErrBeforeCredentialCheck) {
			t.Fatalf("err = %v, want an error without the stage mark", err)
		}
	})
}
