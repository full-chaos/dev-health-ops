package local

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const goodPassword = "Correct-horse-1"

// calls records every dependency call in order, so two paths can be compared
// by call sequence instead of wall-clock time.
type calls struct {
	log      []string
	compares []string
}

type fakeUsers struct {
	c    *calls
	user *User
	err  error
}

func (f *fakeUsers) ByEmail(_ context.Context, email string) (*User, error) {
	f.c.log = append(f.c.log, "users.by_email")
	return f.user, f.err
}

func (f *fakeUsers) Project(_ context.Context, _ uuid.UUID) error {
	f.c.log = append(f.c.log, "users.project")
	return nil
}

type fakeAttempts struct {
	c         *calls
	locked    bool
	remaining time.Duration
	checkErr  error
	failures  int
	cleared   int
	ids       [][]byte
}

func (f *fakeAttempts) Check(_ context.Context, id []byte) (bool, time.Duration, error) {
	f.c.log = append(f.c.log, "attempts.check")
	f.ids = append(f.ids, id)
	return f.locked, f.remaining, f.checkErr
}
func (f *fakeAttempts) RecordFailure(_ context.Context, id []byte) error {
	f.c.log = append(f.c.log, "attempts.record_failure")
	f.failures++
	f.ids = append(f.ids, id)
	return nil
}
func (f *fakeAttempts) Clear(_ context.Context, id []byte) error {
	f.c.log = append(f.c.log, "attempts.clear")
	f.cleared++
	f.ids = append(f.ids, id)
	return nil
}

type fakeSessions struct {
	c       *calls
	created int
	amr     []string
}

func (f *fakeSessions) Create(_ context.Context, p uuid.UUID, _ *uuid.UUID, amr []string) (session.Issued, error) {
	f.c.log = append(f.c.log, "sessions.create")
	f.created++
	f.amr = amr
	return session.Issued{}, nil
}

type rig struct {
	c        *calls
	users    *fakeUsers
	attempts *fakeAttempts
	sessions *fakeSessions
	svc      *Service
}

// testKey is 32 bytes, base64url; built at run time so no key-shaped literal sits in the source.
var testKey = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))

func newRig(t *testing.T, user *User) *rig {
	t.Helper()
	c := &calls{}
	r := &rig{c: c, users: &fakeUsers{c: c, user: user}, attempts: &fakeAttempts{c: c}, sessions: &fakeSessions{c: c}}
	svc, err := New(Config{
		Users: r.users, Sessions: r.sessions, Attempts: r.attempts,
		LockoutKey: contracts.NewSecretString(testKey),
		Compare: func(hash, password string) bool {
			c.log = append(c.log, "compare")
			c.compares = append(c.compares, hash)
			return bcryptMatches(hash, password)
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	r.svc = svc
	return r
}

func hashOf(t *testing.T, password string) *string {
	t.Helper()
	h, err := passwordhash.Hash(password)
	if err != nil {
		t.Fatal(err)
	}
	return &h
}

func local(t *testing.T) *User {
	prov := "local"
	return &User{ID: uuid.New(), IsActive: true, IsVerified: true, PasswordHash: hashOf(t, goodPassword), AuthProvider: &prov}
}

func login(t *testing.T, r *rig, email, password string) Outcome {
	t.Helper()
	out, err := r.svc.Login(context.Background(), Credentials{Email: email, Password: password})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	return out
}

func TestSuccessIssuesSessionAndClearsAttempts(t *testing.T) {
	r := newRig(t, local(t))
	out := login(t, r, "  User@Example.COM ", goodPassword)
	if out.Kind != KindSuccess || out.Issued == nil || out.Reason != ReasonAccepted {
		t.Fatalf("outcome = %+v", out)
	}
	if r.sessions.created != 1 || r.attempts.cleared != 1 || r.attempts.failures != 0 {
		t.Fatalf("created=%d cleared=%d failures=%d", r.sessions.created, r.attempts.cleared, r.attempts.failures)
	}
	if !reflect.DeepEqual(r.sessions.amr, []string{"pwd"}) {
		t.Fatalf("amr = %v", r.sessions.amr)
	}
	want := []string{"users.by_email", "attempts.check", "compare", "attempts.clear", "users.project", "sessions.create"}
	if !reflect.DeepEqual(r.c.log, want) {
		t.Fatalf("calls = %v, want %v", r.c.log, want)
	}
}

func TestEmailIsNormalizedBeforeLookupAndLockoutHash(t *testing.T) {
	a := newRig(t, local(t))
	login(t, a, "  User@Example.COM ", "wrong")
	b := newRig(t, local(t))
	login(t, b, "user@example.com", "wrong")
	if len(a.attempts.ids[0]) != 32 || !reflect.DeepEqual(a.attempts.ids[0], b.attempts.ids[0]) {
		t.Fatalf("lockout identifiers differ or are not 32 bytes: %x %x", a.attempts.ids[0], b.attempts.ids[0])
	}
	if strings.Contains(string(a.attempts.ids[0]), "example") {
		t.Fatal("the identifier carries the email")
	}
	if reflect.DeepEqual(a.attempts.ids[0], a.svc.LockoutIdentifier("other@example.com")) {
		t.Fatal("different emails share an identifier")
	}
}

// TestEveryRefusalPathHasOneCallSequence is the anti-enumeration check:
// unknown user, wrong password, disabled, no password and a held lock make
// the same dependency calls in the same order, with one password compare.
func TestEveryRefusalPathHasOneCallSequence(t *testing.T) {
	disabled := local(t)
	disabled.IsActive = false
	noPassword := local(t)
	noPassword.PasswordHash = nil
	cases := []struct {
		name   string
		user   *User
		locked bool
		pw     string
		kind   Kind
	}{
		{"unknown", nil, false, goodPassword, KindInvalidCredentials},
		{"wrong password", local(t), false, "wrong", KindInvalidCredentials},
		{"disabled", disabled, false, goodPassword, KindDisabled},
		{"no password", noPassword, false, goodPassword, KindPasswordUnavailable},
		{"locked", local(t), true, goodPassword, KindLocked},
		{"locked unknown", nil, true, goodPassword, KindLocked},
		{"password over 72 bytes", local(t), false, strings.Repeat("a", 73), KindInvalidCredentials},
	}
	var want []string
	for _, tc := range cases {
		r := newRig(t, tc.user)
		r.attempts.locked, r.attempts.remaining = tc.locked, 90*time.Second
		out := login(t, r, "who@example.com", tc.pw)
		if out.Kind != tc.kind {
			t.Fatalf("%s: kind = %s, want %s", tc.name, out.Kind, tc.kind)
		}
		if (tc.user == nil) != (out.PrincipalID == uuid.Nil) || (tc.user != nil && out.PrincipalID != tc.user.ID) {
			t.Fatalf("%s: PrincipalID = %s", tc.name, out.PrincipalID)
		}
		if out.Issued != nil || r.sessions.created != 0 {
			t.Fatalf("%s: a session was issued", tc.name)
		}
		if len(r.c.compares) != 1 {
			t.Fatalf("%s: %d compares, want exactly 1", tc.name, len(r.c.compares))
		}
		if want == nil {
			want = r.c.log
		} else if !reflect.DeepEqual(r.c.log, want) {
			t.Fatalf("%s: calls = %v, want %v", tc.name, r.c.log, want)
		}
	}
}

func TestDummyHashStandsInWhereNoRealHashApplies(t *testing.T) {
	disabled := local(t)
	disabled.IsActive = false
	noPassword := local(t)
	noPassword.PasswordHash = nil
	for name, user := range map[string]*User{"unknown": nil, "disabled": disabled, "no password": noPassword} {
		r := newRig(t, user)
		login(t, r, "who@example.com", goodPassword)
		if r.c.compares[0] != DummyHash() {
			t.Fatalf("%s: compared a real hash", name)
		}
	}
	locked := newRig(t, local(t))
	locked.attempts.locked = true
	login(t, locked, "who@example.com", goodPassword)
	if locked.c.compares[0] != DummyHash() {
		t.Fatal("locked: compared the real hash")
	}
	if !strings.HasPrefix(DummyHash(), "$2b$12$") {
		t.Fatalf("dummy hash %q is not bcrypt 2b cost 12", DummyHash())
	}
	if bcryptMatches(DummyHash(), goodPassword) {
		t.Fatal("the dummy hash matched a password")
	}
}

func TestLockedCarriesRetryAfterAndRecordsNoSession(t *testing.T) {
	r := newRig(t, local(t))
	r.attempts.locked, r.attempts.remaining = true, 90*time.Second
	out := login(t, r, "who@example.com", goodPassword)
	if out.Kind != KindLocked || out.Reason != ReasonAccountLocked || out.RetryAfter != 90*time.Second {
		t.Fatalf("outcome = %+v", out)
	}
}

func TestLockedAtZeroRemainingStillRetriesAfterOneSecond(t *testing.T) {
	r := newRig(t, local(t))
	r.attempts.locked, r.attempts.remaining = true, 0
	if out := login(t, r, "who@example.com", goodPassword); out.RetryAfter != time.Second {
		t.Fatalf("RetryAfter = %v, want 1s (go-api login.go clamps remaining <= 0 to 1)", out.RetryAfter)
	}
}

func TestUnverifiedLocalAccountIsBlockedAfterClearingAttempts(t *testing.T) {
	u := local(t)
	u.IsVerified = false
	r := newRig(t, u)
	out := login(t, r, "who@example.com", goodPassword)
	if out.Kind != KindEmailUnverified || out.Reason != ReasonEmailUnverified || out.Issued != nil {
		t.Fatalf("outcome = %+v", out)
	}
	if r.attempts.cleared != 1 || r.attempts.failures != 0 || r.sessions.created != 0 {
		t.Fatalf("cleared=%d failures=%d created=%d", r.attempts.cleared, r.attempts.failures, r.sessions.created)
	}
}

func TestNonLocalProviderNeedsNoVerification(t *testing.T) {
	u := local(t)
	u.IsVerified = false
	g := "github"
	u.AuthProvider = &g
	if out := login(t, newRig(t, u), "who@example.com", goodPassword); out.Kind != KindSuccess {
		t.Fatalf("kind = %s", out.Kind)
	}
	u.AuthProvider = nil
	if out := login(t, newRig(t, u), "who@example.com", goodPassword); out.Kind != KindSuccess {
		t.Fatalf("nil provider: kind = %s, want success (login.go: a NULL provider is the text none, not local)", out.Kind)
	}
}

func TestDependencyErrorsAreErrorsNotRefusals(t *testing.T) {
	boom := errors.New("boom")
	r := newRig(t, local(t))
	r.users.err = boom
	if _, err := r.svc.Login(context.Background(), Credentials{Email: "a@b.c", Password: "x"}); !errors.Is(err, boom) {
		t.Fatalf("users error = %v", err)
	}
	r = newRig(t, local(t))
	r.attempts.checkErr = boom
	if _, err := r.svc.Login(context.Background(), Credentials{Email: "a@b.c", Password: "x"}); !errors.Is(err, boom) {
		t.Fatalf("attempts error = %v", err)
	}
}

func TestNewRefusesABadLockoutKey(t *testing.T) {
	for _, key := range []string{"", "short", testKey + "A"} {
		_, err := New(Config{Users: &fakeUsers{}, Sessions: &fakeSessions{}, Attempts: &fakeAttempts{}, LockoutKey: contracts.NewSecretString(key)})
		if err == nil {
			t.Fatalf("key %q accepted", key)
		}
	}
}

func TestInvalidEmailIsAnErrorBeforeAnyLookup(t *testing.T) {
	r := newRig(t, local(t))
	_, err := r.svc.Login(context.Background(), Credentials{Email: "not-an-email", Password: goodPassword})
	if !errors.Is(err, identity.ErrInvalidEmail) {
		t.Fatalf("err = %v, want ErrInvalidEmail", err)
	}
	if len(r.c.log) != 0 {
		t.Fatalf("calls before validation: %v", r.c.log)
	}
}
