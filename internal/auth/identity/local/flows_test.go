package local

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

type flowRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *flowRecorder) add(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, name)
}

func (r *flowRecorder) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

type flowAccounts struct {
	rec      *flowRecorder
	byEmail  map[string]*User
	byID     map[uuid.UUID]*User
	taken    bool
	provided []ProvisionLocalRequest
	verified []uuid.UUID
	hashes   []string
	newID    uuid.UUID
	writeErr error
	locker   *flowLocker
	outside  int
}

func (a *flowAccounts) ByEmail(_ context.Context, email string) (*User, error) {
	a.rec.add("ByEmail")
	return a.byEmail[email], nil
}

func (a *flowAccounts) ByID(_ context.Context, id uuid.UUID) (*User, error) {
	a.rec.add("ByID")
	return a.byID[id], nil
}

func (a *flowAccounts) ProvisionLocal(_ context.Context, req ProvisionLocalRequest) (uuid.UUID, error) {
	a.rec.add("ProvisionLocal")
	if a.taken {
		return uuid.Nil, identity.ErrEmailTaken
	}
	a.provided = append(a.provided, req)
	return a.newID, nil
}

func (a *flowAccounts) SetEmailVerified(_ context.Context, id uuid.UUID) error {
	a.rec.add("SetEmailVerified")
	if a.writeErr != nil {
		return a.writeErr
	}
	a.verified = append(a.verified, id)
	return nil
}

func (a *flowAccounts) SetPasswordHash(_ context.Context, _ uuid.UUID, hash string) error {
	a.rec.add("SetPasswordHash")
	if a.locker != nil && !a.locker.active {
		a.outside++
	}
	if a.writeErr != nil {
		return a.writeErr
	}
	a.hashes = append(a.hashes, hash)
	return nil
}

type flowLinks struct {
	rec      *flowRecorder
	issued   []IssueRequest
	redeemed []RedeemRequest
	redeem   map[[32]byte]uuid.UUID
}

func (l *flowLinks) Issue(_ context.Context, req IssueRequest) error {
	l.rec.add("Issue")
	l.issued = append(l.issued, req)
	return nil
}

func (l *flowLinks) Redeem(_ context.Context, req RedeemRequest) (uuid.UUID, bool, error) {
	l.rec.add("Redeem")
	l.redeemed = append(l.redeemed, req)
	id, ok := l.redeem[req.TokenHash]
	if ok {
		delete(l.redeem, req.TokenHash)
	}
	return id, ok, nil
}

type flowSessions struct {
	rec       *flowRecorder
	revoked   []uuid.UUID
	created   []uuid.UUID
	revokeErr error
	attempts  int
}

type flowLocker struct {
	active  bool
	locked  []uuid.UUID
	outside int
}

func (l *flowLocker) WithPrincipalLock(ctx context.Context, id uuid.UUID, fn func(context.Context) error) error {
	l.locked = append(l.locked, id)
	l.active = true
	defer func() { l.active = false }()
	return fn(ctx)
}

func (s *flowSessions) Create(_ context.Context, id uuid.UUID, _ *uuid.UUID, amr []string) (session.Issued, error) {
	s.rec.add("Create")
	s.created = append(s.created, id)
	return session.Issued{}, nil
}

func (s *flowSessions) RevokeAll(_ context.Context, id uuid.UUID, _ session.Actor) (int, error) {
	s.rec.add("RevokeAll")
	s.attempts++
	if s.revokeErr != nil {
		return 0, s.revokeErr
	}
	s.revoked = append(s.revoked, id)
	return 1, nil
}

type flowMail struct {
	rec  *flowRecorder
	sent []Message
}

func (m *flowMail) Send(_ context.Context, msg Message) error {
	m.rec.add("Send")
	m.sent = append(m.sent, msg)
	return nil
}

type flowRig struct {
	f        *Flows
	rec      *flowRecorder
	accounts *flowAccounts
	links    *flowLinks
	sessions *flowSessions
	mail     *flowMail
	locker   *flowLocker
}

const (
	flowPassword = "Correct-horse-battery-9"
)

func newFlowRig(t *testing.T, owned bool) *flowRig {
	t.Helper()
	rec := &flowRecorder{}
	rig := &flowRig{rec: rec,
		accounts: &flowAccounts{rec: rec, byEmail: map[string]*User{}, byID: map[uuid.UUID]*User{}, newID: uuid.New()},
		links:    &flowLinks{rec: rec, redeem: map[[32]byte]uuid.UUID{}},
		sessions: &flowSessions{rec: rec},
		mail:     &flowMail{rec: rec},
		locker:   &flowLocker{},
	}
	rig.accounts.locker = rig.locker
	f, err := NewFlows(FlowsConfig{Accounts: rig.accounts, Links: rig.links, Sessions: rig.sessions, Mail: rig.mail, Locker: rig.locker,
		UsersOwnedByAuth: owned,
		Hash:             func(p string) (string, error) { return "hash-of:" + p, nil },
		Compare:          func(hash, p string) bool { return hash == "hash-of:"+p },
	})
	if err != nil {
		t.Fatal(err)
	}
	rig.f = f
	return rig
}

func (r *flowRig) user(email string, verified bool) *User {
	hash := "hash-of:" + flowPassword
	provider := "local"
	u := &User{ID: uuid.New(), IsActive: true, IsVerified: verified, PasswordHash: &hash, AuthProvider: &provider}
	r.accounts.byEmail[email] = u
	r.accounts.byID[u.ID] = u
	return u
}

func (r *flowRig) grant(secret string, principal uuid.UUID) {
	r.links.redeem[sha256.Sum256([]byte(secret))] = principal
}

func TestFlagOffRefusesEveryFlowBeforeAnyCall(t *testing.T) {
	rig := newFlowRig(t, false)
	ctx := context.Background()
	calls := map[string]error{}
	_, calls["register"] = rig.f.Register(ctx, RegisterRequest{Email: "a@example.com", Password: flowPassword})
	_, calls["verify"] = rig.f.Verify(ctx, "secret")
	calls["resend"] = rig.f.Resend(ctx, "a@example.com")
	calls["forgot"] = rig.f.Forgot(ctx, "a@example.com")
	_, calls["reset"] = rig.f.Reset(ctx, "secret", flowPassword)
	_, calls["change"] = rig.f.Change(ctx, ChangeRequest{PrincipalID: uuid.New(), Current: "x", New: flowPassword})
	for name, err := range calls {
		if !errors.Is(err, identity.ErrSignupNotAvailable) {
			t.Errorf("%s: error = %v, want ErrSignupNotAvailable", name, err)
		}
	}
	if got := rig.rec.names(); len(got) != 0 {
		t.Errorf("calls with the switch off = %v, want none", got)
	}
}

func TestRegisterCreatesThePrincipalAndSendsOneVerificationLink(t *testing.T) {
	rig := newFlowRig(t, true)
	out, err := rig.f.Register(context.Background(), RegisterRequest{Email: "  New@Example.COM ", Password: flowPassword})
	if err != nil || out.Kind != FlowOK || out.PrincipalID != rig.accounts.newID {
		t.Fatalf("Register = %+v, %v", out, err)
	}
	if want := []string{"ByEmail", "ProvisionLocal", "Issue", "Send"}; !reflect.DeepEqual(rig.rec.names(), want) {
		t.Fatalf("calls = %v, want %v", rig.rec.names(), want)
	}
	p := rig.accounts.provided[0]
	if p.Email != "new@example.com" || p.PasswordHash != "hash-of:"+flowPassword || strings.Contains(p.PasswordHash, "  ") {
		t.Errorf("provisioned %+v", p)
	}
	iss := rig.links.issued[0]
	if iss.Purpose != PurposeEmailVerification || iss.TTL != VerificationTTL || iss.PrincipalID != rig.accounts.newID {
		t.Errorf("issued %+v", iss)
	}
	msg := rig.mail.sent[0]
	if !msg.Deliver || msg.To != "new@example.com" || sha256.Sum256([]byte(msg.Secret.Reveal())) != iss.TokenHash {
		t.Errorf("message does not carry the secret whose hash was stored: %+v", msg)
	}
}

func TestRegisterRefusals(t *testing.T) {
	t.Run("taken e-mail is refused before any write", func(t *testing.T) {
		rig := newFlowRig(t, true)
		rig.user("taken@example.com", true)
		out, err := rig.f.Register(context.Background(), RegisterRequest{Email: "Taken@Example.com", Password: flowPassword})
		if err != nil || out.Kind != FlowEmailTaken {
			t.Fatalf("Register = %+v, %v", out, err)
		}
		if want := []string{"ByEmail"}; !reflect.DeepEqual(rig.rec.names(), want) {
			t.Errorf("calls = %v, want %v", rig.rec.names(), want)
		}
	})
	t.Run("race on the unique e-mail is the same refusal", func(t *testing.T) {
		rig := newFlowRig(t, true)
		rig.accounts.taken = true
		out, err := rig.f.Register(context.Background(), RegisterRequest{Email: "x@example.com", Password: flowPassword})
		if err != nil || out.Kind != FlowEmailTaken {
			t.Fatalf("Register = %+v, %v", out, err)
		}
		if len(rig.links.issued) != 0 || len(rig.mail.sent) != 0 {
			t.Error("a link was issued for a refused registration")
		}
	})
	t.Run("password policy comes first and writes nothing", func(t *testing.T) {
		rig := newFlowRig(t, true)
		out, err := rig.f.Register(context.Background(), RegisterRequest{Email: "x@example.com", Password: "short"})
		if err != nil || out.Kind != FlowPolicy || len(out.Violations) == 0 {
			t.Fatalf("Register = %+v, %v", out, err)
		}
		if got := rig.rec.names(); len(got) != 0 {
			t.Errorf("calls = %v, want none", got)
		}
	})
	t.Run("over 72 bytes is a policy refusal, not an error (deliberate divergence from the corpus row)", func(t *testing.T) {
		rig := newFlowRig(t, true)
		out, err := rig.f.Register(context.Background(), RegisterRequest{Email: "x@example.com", Password: "Aa1-" + strings.Repeat("x", 80)})
		if err != nil || out.Kind != FlowPolicy || !reflect.DeepEqual(out.Violations, []string{ViolationTooLong}) {
			t.Fatalf("Register = %+v, %v", out, err)
		}
	})
}

func TestVerifyConsumesOnceAndMarksVerified(t *testing.T) {
	rig := newFlowRig(t, true)
	u := rig.user("v@example.com", false)
	rig.grant("good-secret", u.ID)
	out, err := rig.f.Verify(context.Background(), "good-secret")
	if err != nil || out.Kind != FlowOK || out.PrincipalID != u.ID {
		t.Fatalf("Verify = %+v, %v", out, err)
	}
	if want := []string{"Redeem", "SetEmailVerified"}; !reflect.DeepEqual(rig.rec.names(), want) {
		t.Fatalf("calls = %v, want %v", rig.rec.names(), want)
	}
	if r := rig.links.redeemed[0]; r.Purpose != PurposeEmailVerification || r.TokenHash != sha256.Sum256([]byte("good-secret")) {
		t.Errorf("redeemed %+v", r)
	}
	again, err := rig.f.Verify(context.Background(), "good-secret")
	if err != nil || again.Kind != FlowInvalidToken || len(rig.accounts.verified) != 1 {
		t.Errorf("replay = %+v, %v, verified writes %d", again, err, len(rig.accounts.verified))
	}
}

func TestVerifyGarbageWritesNothing(t *testing.T) {
	rig := newFlowRig(t, true)
	out, err := rig.f.Verify(context.Background(), "garbage")
	if err != nil || out.Kind != FlowInvalidToken || len(rig.accounts.verified) != 0 {
		t.Fatalf("Verify = %+v, %v", out, err)
	}
}

// Resend and Forgot make the same calls for an unknown address, an already
// verified one and an eligible one; only the decoy flags differ.
func TestResendAndForgotMakeOneCallSequenceForEveryAddress(t *testing.T) {
	type run func(*flowRig, string) error
	flows := map[string]struct {
		do      run
		purpose Purpose
		ttl     time.Duration
	}{
		"resend": {func(r *flowRig, e string) error { return r.f.Resend(context.Background(), e) }, PurposeEmailVerification, VerificationTTL},
		"forgot": {func(r *flowRig, e string) error { return r.f.Forgot(context.Background(), e) }, PurposePasswordReset, ResetTTL},
	}
	for name, flow := range flows {
		t.Run(name, func(t *testing.T) {
			var sequences [][]string
			for _, address := range []string{"nobody@example.com", "verified@example.com", "pending@example.com"} {
				rig := newFlowRig(t, true)
				rig.user("verified@example.com", true)
				rig.user("pending@example.com", false)
				if err := flow.do(rig, " "+strings.ToUpper(address)); err != nil {
					t.Fatalf("%s: %v", address, err)
				}
				sequences = append(sequences, rig.rec.names())
				iss, msg := rig.links.issued[0], rig.mail.sent[0]
				real := address == "pending@example.com" || (name == "forgot" && address == "verified@example.com")
				if (iss.PrincipalID != uuid.Nil) != real || msg.Deliver != real {
					t.Errorf("%s: principal set %v, deliver %v, want both %v", address, iss.PrincipalID != uuid.Nil, msg.Deliver, real)
				}
				if iss.Purpose != flow.purpose || iss.TTL != flow.ttl || iss.TokenHash == ([32]byte{}) {
					t.Errorf("%s: issued %+v", address, iss)
				}
				if sha256.Sum256([]byte(msg.Secret.Reveal())) != iss.TokenHash {
					t.Errorf("%s: mailed secret does not match the stored hash", address)
				}
			}
			for i := 1; i < len(sequences); i++ {
				if !reflect.DeepEqual(sequences[0], sequences[i]) {
					t.Errorf("call sequences differ: %v vs %v", sequences[0], sequences[i])
				}
			}
		})
	}
}

func TestResetSetsPasswordRevokesEverySessionAndIsSingleUse(t *testing.T) {
	rig := newFlowRig(t, true)
	u := rig.user("r@example.com", true)
	rig.grant("reset-secret", u.ID)
	out, err := rig.f.Reset(context.Background(), "reset-secret", "A-new-pass-77")
	if err != nil || out.Kind != FlowOK {
		t.Fatalf("Reset = %+v, %v", out, err)
	}
	if want := []string{"Redeem", "SetPasswordHash", "RevokeAll"}; !reflect.DeepEqual(rig.rec.names(), want) {
		t.Fatalf("calls = %v, want %v", rig.rec.names(), want)
	}
	if rig.links.redeemed[0].Purpose != PurposePasswordReset || rig.accounts.hashes[0] != "hash-of:A-new-pass-77" || rig.sessions.revoked[0] != u.ID {
		t.Errorf("redeem %+v hashes %v revoked %v", rig.links.redeemed[0], rig.accounts.hashes, rig.sessions.revoked)
	}
	again, err := rig.f.Reset(context.Background(), "reset-secret", "Another-pass-88")
	if err != nil || again.Kind != FlowInvalidToken || len(rig.accounts.hashes) != 1 {
		t.Errorf("replay = %+v, %v, hash writes %d", again, err, len(rig.accounts.hashes))
	}
}

func TestResetRefusesAnOverlongPasswordBeforeBurningTheCredential(t *testing.T) {
	rig := newFlowRig(t, true)
	u := rig.user("r@example.com", true)
	rig.grant("reset-secret", u.ID)
	out, err := rig.f.Reset(context.Background(), "reset-secret", strings.Repeat("x", 80))
	if err != nil || out.Kind != FlowPolicy || !reflect.DeepEqual(out.Violations, []string{ViolationTooLong}) {
		t.Fatalf("Reset = %+v, %v", out, err)
	}
	if got := rig.rec.names(); len(got) != 0 {
		t.Errorf("calls = %v, want none (the credential must stay usable)", got)
	}
	if _, ok := rig.links.redeem[sha256.Sum256([]byte("reset-secret"))]; !ok {
		t.Error("credential was consumed")
	}
}

func TestChangePassword(t *testing.T) {
	type tc struct {
		name   string
		mutate func(*flowRig, *User, *ChangeRequest)
		want   FlowKind
		calls  []string
	}
	cases := []tc{
		{"success revokes all, then issues the caller a new session", nil, FlowOK,
			[]string{"ByID", "SetPasswordHash", "RevokeAll", "Create"}},
		{"impersonated session", func(_ *flowRig, _ *User, r *ChangeRequest) { r.Impersonated = true }, FlowNotAllowed, nil},
		{"inactive user", func(_ *flowRig, u *User, _ *ChangeRequest) { u.IsActive = false }, FlowNotAllowed, []string{"ByID"}},
		{"unknown user", func(rig *flowRig, u *User, _ *ChangeRequest) { delete(rig.accounts.byID, u.ID) }, FlowNotAllowed, []string{"ByID"}},
		{"externally managed account", func(_ *flowRig, u *User, _ *ChangeRequest) {
			p := "github"
			u.AuthProvider = &p
		}, FlowManagedAccount, []string{"ByID"}},
		{"account without a password", func(_ *flowRig, u *User, _ *ChangeRequest) { u.PasswordHash = nil }, FlowManagedAccount, []string{"ByID"}},
		{"wrong current password", func(_ *flowRig, _ *User, r *ChangeRequest) { r.Current = "nope" }, FlowCurrentRejected, []string{"ByID"}},
		{"new password fails the policy", func(_ *flowRig, _ *User, r *ChangeRequest) { r.New = "short" }, FlowPolicy, nil},
		{"new password over 72 bytes", func(_ *flowRig, _ *User, r *ChangeRequest) { r.New = strings.Repeat("x", 80) }, FlowPolicy, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rig := newFlowRig(t, true)
			u := rig.user("c@example.com", true)
			req := ChangeRequest{PrincipalID: u.ID, Current: flowPassword, New: "Brand-new-pass-42"}
			if c.mutate != nil {
				c.mutate(rig, u, &req)
			}
			out, err := rig.f.Change(context.Background(), req)
			if err != nil || out.Kind != c.want {
				t.Fatalf("Change = %+v, %v; want %s", out, err, c.want)
			}
			if got := rig.rec.names(); !reflect.DeepEqual(got, c.calls) && !(len(got) == 0 && len(c.calls) == 0) {
				t.Errorf("calls = %v, want %v", got, c.calls)
			}
			if c.want == FlowOK && (out.Issued == nil || rig.accounts.hashes[0] != "hash-of:Brand-new-pass-42") {
				t.Errorf("outcome %+v hashes %v", out, rig.accounts.hashes)
			}
			if c.want != FlowOK && out.Issued != nil {
				t.Error("a refused change issued a session")
			}
		})
	}
}

var _ = contracts.NewSecretString

// A write that fails after the consume leaves the link spent: the error
// surfaces, nothing is retried, and a replay is refused. The user asks again.
func TestFailedWriteAfterTheConsumeLeavesTheLinkSpent(t *testing.T) {
	boom := errors.New("store down")
	t.Run("reset", func(t *testing.T) {
		rig := newFlowRig(t, true)
		u := rig.user("r@example.com", true)
		rig.grant("s", u.ID)
		rig.accounts.writeErr = boom
		if _, err := rig.f.Reset(context.Background(), "s", "A-new-pass-77"); !errors.Is(err, boom) {
			t.Fatalf("Reset error = %v, want the store error", err)
		}
		if len(rig.sessions.revoked) != 0 {
			t.Error("sessions were revoked although the password did not change")
		}
		rig.accounts.writeErr = nil
		if out, err := rig.f.Reset(context.Background(), "s", "A-new-pass-77"); err != nil || out.Kind != FlowInvalidToken {
			t.Errorf("replay = %+v, %v; want the spent link refused", out, err)
		}
	})
	t.Run("verify", func(t *testing.T) {
		rig := newFlowRig(t, true)
		u := rig.user("v@example.com", false)
		rig.grant("s", u.ID)
		rig.accounts.writeErr = boom
		if _, err := rig.f.Verify(context.Background(), "s"); !errors.Is(err, boom) {
			t.Fatalf("Verify error = %v, want the store error", err)
		}
		rig.accounts.writeErr = nil
		if out, err := rig.f.Verify(context.Background(), "s"); err != nil || out.Kind != FlowInvalidToken {
			t.Errorf("replay = %+v, %v; want the spent link refused", out, err)
		}
	})
}

type failingMail struct{ err error }

func (m failingMail) Send(context.Context, Message) error { return m.err }

func TestMailFailureLogsAFixedClassNotTheErrorText(t *testing.T) {
	cases := map[string]error{
		"provider_error": errors.New("550 5.1.1 <holder@example.com> mailbox unavailable"),
		"refused":        fmt.Errorf("dial smtp for holder@example.com: %w", syscall.ECONNREFUSED),
		"timeout":        fmt.Errorf("send to holder@example.com: %w", context.DeadlineExceeded),
	}
	for class, cause := range cases {
		t.Run(class, func(t *testing.T) {
			var buf bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
			rig := newFlowRig(t, true)
			f, err := NewFlows(FlowsConfig{Accounts: rig.accounts, Links: rig.links, Sessions: rig.sessions, Mail: failingMail{cause}, Locker: rig.locker, UsersOwnedByAuth: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Forgot(context.Background(), "holder@example.com"); err != nil {
				t.Fatalf("a mail failure must not fail the flow: %v", err)
			}
			logged := buf.String()
			if !strings.Contains(logged, "error.class=") {
				t.Errorf("log %q lacks the error class (%s)", logged, class)
			}
			if strings.Contains(logged, "holder") || strings.Contains(logged, "example.com") {
				t.Errorf("the log line holds the address: %q", logged)
			}
		})
	}
}

// The password write and the session cleanup run under the principal lock.
func TestPasswordFlowsRunUnderThePrincipalLock(t *testing.T) {
	rig := newFlowRig(t, true)
	u := rig.user("l@example.com", true)
	rig.grant("s", u.ID)
	if out, err := rig.f.Reset(context.Background(), "s", "A-new-pass-77"); err != nil || out.Kind != FlowOK {
		t.Fatalf("Reset = %+v, %v", out, err)
	}
	if _, err := rig.f.Change(context.Background(), ChangeRequest{PrincipalID: u.ID, Current: "A-new-pass-77", New: "Brand-new-pass-42"}); err != nil {
		t.Fatal(err)
	}
	if len(rig.locker.locked) != 2 || rig.locker.locked[0] != u.ID || rig.locker.locked[1] != u.ID {
		t.Errorf("locks = %v, want two for the principal", rig.locker.locked)
	}
	if rig.accounts.outside != 0 {
		t.Errorf("%d password writes ran outside the lock", rig.accounts.outside)
	}
}

// The password change is the security event; ending older sessions is cleanup
// that is retried and reported, never a failed flow.
func TestRevokeFailureAfterAPasswordChangeIsRetriedAndReported(t *testing.T) {
	boom := errors.New("sessions down")
	t.Run("reset", func(t *testing.T) {
		rig := newFlowRig(t, true)
		u := rig.user("r@example.com", true)
		rig.grant("s", u.ID)
		rig.sessions.revokeErr = boom
		out, err := rig.f.Reset(context.Background(), "s", "A-new-pass-77")
		if err != nil || out.Kind != FlowOK {
			t.Fatalf("Reset = %+v, %v; want success", out, err)
		}
		if !errors.Is(out.RevokeErr, ErrSessionsNotRevoked) || !errors.Is(out.RevokeErr, boom) {
			t.Errorf("RevokeErr = %v", out.RevokeErr)
		}
		if rig.sessions.attempts != 3 || len(rig.accounts.hashes) != 1 {
			t.Errorf("revoke attempts %d, password writes %d; want 3 and 1", rig.sessions.attempts, len(rig.accounts.hashes))
		}
	})
	t.Run("change still issues the caller a session", func(t *testing.T) {
		rig := newFlowRig(t, true)
		u := rig.user("c@example.com", true)
		rig.sessions.revokeErr = boom
		out, err := rig.f.Change(context.Background(), ChangeRequest{PrincipalID: u.ID, Current: flowPassword, New: "Brand-new-pass-42"})
		if err != nil || out.Kind != FlowOK || out.Issued == nil || !errors.Is(out.RevokeErr, ErrSessionsNotRevoked) {
			t.Fatalf("Change = %+v, %v", out, err)
		}
		if rig.sessions.attempts != 3 || len(rig.sessions.created) != 1 {
			t.Errorf("revoke attempts %d, sessions created %d; want 3 and 1", rig.sessions.attempts, len(rig.sessions.created))
		}
	})
}
