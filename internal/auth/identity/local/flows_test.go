package local

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
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
	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/callseq"
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
	rec       *flowRecorder
	byEmail   map[string]*User
	byID      map[uuid.UUID]*User
	taken     bool
	provided  []ProvisionLocalRequest
	verified  []uuid.UUID
	hashes    []string
	ended     []uuid.UUID
	created   []uuid.UUID
	links     *flowLinks
	afterPeek func()
	newID     uuid.UUID
	writeErr  error
	locker    *flowLocker
	outside   int
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

func (a *flowAccounts) PeekResetLink(_ context.Context, hash [32]byte) (uuid.UUID, bool, error) {
	a.rec.add("PeekResetLink")
	id, ok := a.links.redeem[hash]
	// The port's contract: a link of an account that is gone or of the service provider reads as not ok.
	if user := a.byID[id]; ok && (user == nil || identity.IsServiceProvider(user.AuthProvider)) {
		return uuid.Nil, false, nil
	}
	if ok && a.afterPeek != nil {
		a.afterPeek()
	}
	return id, ok, nil
}

func (a *flowAccounts) SetPasswordEndingSessions(_ context.Context, req PasswordEnd) (PasswordEndResult, error) {
	a.rec.add("SetPasswordEndingSessions")
	if a.locker != nil && !a.locker.active {
		a.outside++
	}
	if a.writeErr != nil {
		return PasswordEndResult{}, a.writeErr
	}
	if req.ResetLink != nil {
		if _, ok := a.links.redeem[*req.ResetLink]; !ok {
			return PasswordEndResult{}, ErrResetLinkUnusable
		}
		delete(a.links.redeem, *req.ResetLink)
	}
	a.hashes = append(a.hashes, req.Hash)
	a.ended = append(a.ended, req.ID)
	var res PasswordEndResult
	if req.NewSession != nil {
		a.created = append(a.created, req.ID)
		res.Issued = &session.Issued{}
	}
	return res, nil
}

type flowLinks struct {
	rec      *flowRecorder
	issued   []IssueRequest
	redeemed []RedeemRequest
	redeem   map[[32]byte]uuid.UUID
}

func (l *flowLinks) IssueBound(_ context.Context, req IssueRequest) error {
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
	rec     *flowRecorder
	created []uuid.UUID
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
	rig.accounts.links = rig.links
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
	if err != nil || out.Kind != FlowOK {
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
	t.Run("taken e-mail is answered like a new one and writes nothing", func(t *testing.T) {
		rig := newFlowRig(t, true)
		rig.user("taken@example.com", true)
		out, err := rig.f.Register(context.Background(), RegisterRequest{Email: "Taken@Example.com", Password: flowPassword})
		if err != nil || out.Kind != FlowOK {
			t.Fatalf("Register = %+v, %v", out, err)
		}
		if want := []string{"ByEmail", "Send"}; !reflect.DeepEqual(rig.rec.names(), want) {
			t.Errorf("calls = %v, want %v", rig.rec.names(), want)
		}
		if len(rig.links.issued) != 0 || len(rig.mail.sent) != 1 || rig.mail.sent[0].Purpose != PurposeAccountExists ||
			rig.mail.sent[0].To != "taken@example.com" || rig.mail.sent[0].Secret.Reveal() != "" {
			t.Errorf("want one notice to the owner and no credential: %+v", rig.mail.sent)
		}
	})
	t.Run("race on the unique e-mail is the same answer", func(t *testing.T) {
		rig := newFlowRig(t, true)
		rig.accounts.taken = true
		out, err := rig.f.Register(context.Background(), RegisterRequest{Email: "x@example.com", Password: flowPassword})
		if err != nil || out.Kind != FlowOK {
			t.Fatalf("Register = %+v, %v", out, err)
		}
		if len(rig.links.issued) != 0 || len(rig.mail.sent) != 1 || rig.mail.sent[0].Purpose != PurposeAccountExists {
			t.Errorf("want one notice and no credential: %+v", rig.mail.sent)
		}
	})
	t.Run("an address with a refused form writes nothing and mails nothing", func(t *testing.T) {
		for _, email := range []string{"a\ufffdb@example.com", "a\x00b@example.com", "a\tb@example.com"} {
			rig := newFlowRig(t, true)
			_, err := rig.f.Register(context.Background(), RegisterRequest{Email: email, Password: flowPassword})
			if !errors.Is(err, identity.ErrInvalidEmail) {
				t.Fatalf("Register(%q) err = %v, want ErrInvalidEmail", email, err)
			}
			if got := rig.rec.names(); len(got) != 0 || len(rig.mail.sent) != 0 {
				t.Errorf("Register(%q): calls = %v, mails %d, want none", email, got, len(rig.mail.sent))
			}
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

// A refused reset link (unknown, of a service-provider account, of an account that is gone) makes the one call
// PeekResetLink and answers FlowInvalidToken, the same steps as an unknown link.
func TestResetMakesOneCallSequenceForEveryRefusedLink(t *testing.T) {
	sequences := map[string][]string{}
	for _, name := range []string{"unknown", "service-provider", "gone"} {
		rig := newFlowRig(t, true)
		switch name {
		case "service-provider":
			provider := "service"
			u := &User{ID: uuid.New(), IsActive: true, IsVerified: true, AuthProvider: &provider}
			rig.accounts.byID[u.ID] = u
			rig.grant("refused-secret", u.ID)
		case "gone":
			rig.grant("refused-secret", uuid.New())
		}
		out, err := rig.f.Reset(context.Background(), "refused-secret", "A-new-pass-77")
		if err != nil || out.Kind != FlowInvalidToken {
			t.Fatalf("%s: Reset = %+v, %v", name, out, err)
		}
		if len(rig.accounts.hashes) != 0 {
			t.Errorf("%s: a password was written", name)
		}
		sequences[name] = rig.rec.names()
	}
	if want := []string{"PeekResetLink"}; !reflect.DeepEqual(sequences["unknown"], want) {
		t.Fatalf("unknown link calls = %v, want %v", sequences["unknown"], want)
	}
	callseq.Same(t, "refused reset links", sequences)
}

func TestResetSetsPasswordRevokesEverySessionAndIsSingleUse(t *testing.T) {
	rig := newFlowRig(t, true)
	u := rig.user("r@example.com", true)
	rig.grant("reset-secret", u.ID)
	out, err := rig.f.Reset(context.Background(), "reset-secret", "A-new-pass-77")
	if err != nil || out.Kind != FlowOK {
		t.Fatalf("Reset = %+v, %v", out, err)
	}
	if want := []string{"PeekResetLink", "SetPasswordEndingSessions"}; !reflect.DeepEqual(rig.rec.names(), want) {
		t.Fatalf("calls = %v, want %v", rig.rec.names(), want)
	}
	if rig.accounts.hashes[0] != "hash-of:A-new-pass-77" || rig.accounts.ended[0] != u.ID {
		t.Errorf("hashes %v ended %v", rig.accounts.hashes, rig.accounts.ended)
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
	out, err := rig.f.Reset(context.Background(), "reset-secret", "Aa1-"+strings.Repeat("x", 76))
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
		{"success ends every credential and issues the caller a new session in the password write", nil, FlowOK,
			[]string{"ByID", "SetPasswordEndingSessions"}},
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

// A verification write that fails after the consume leaves the link spent: the
// error surfaces, nothing is retried, and a replay is refused. A reset's link
// is consumed in the password write, so its failed write leaves it usable.
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
		if len(rig.accounts.ended) != 0 {
			t.Error("credentials were ended although the password did not change")
		}
		rig.accounts.writeErr = nil
		if out, err := rig.f.Reset(context.Background(), "s", "A-new-pass-77"); err != nil || out.Kind != FlowOK {
			t.Errorf("retry = %+v, %v; want the link usable after the failed write", out, err)
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

// Ending the older credentials is part of the password write: when it fails
// the flow fails, the caller gets no session and the password is unchanged.
func TestAFailedPasswordWriteFailsTheFlowAndIssuesNoSession(t *testing.T) {
	boom := errors.New("sessions down")
	t.Run("change", func(t *testing.T) {
		rig := newFlowRig(t, true)
		u := rig.user("c@example.com", true)
		rig.accounts.writeErr = boom
		out, err := rig.f.Change(context.Background(), ChangeRequest{PrincipalID: u.ID, Current: flowPassword, New: "Brand-new-pass-42"})
		if !errors.Is(err, boom) || out.Issued != nil {
			t.Fatalf("Change = %+v, %v; want the store error and no session", out, err)
		}
		if len(rig.sessions.created) != 0 || len(rig.accounts.ended) != 0 {
			t.Errorf("sessions created %d, credentials ended %d; want none", len(rig.sessions.created), len(rig.accounts.ended))
		}
	})
}

// Every flow that sets a password refuses a short, an overlong and a
// byte-overlong one before it consumes a link or writes anything.
func TestPasswordBoundsAreRefusedBeforeAnyConsumeInEveryFlow(t *testing.T) {
	bounds := map[string]string{
		"7 characters":   "Aa1-xyz",
		"11 characters":  "Aa1-xyzabcd",
		"129 characters": "Aa1-" + strings.Repeat("x", 125),
		"73 bytes":       "Aa1-" + strings.Repeat("x", 69),
	}
	flows := map[string]func(*flowRig, string) (FlowOutcome, error){
		"register": func(rig *flowRig, pw string) (FlowOutcome, error) {
			return rig.f.Register(context.Background(), RegisterRequest{Email: "p@example.com", Password: pw})
		},
		"reset": func(rig *flowRig, pw string) (FlowOutcome, error) {
			u := rig.user("p@example.com", true)
			rig.grant("bounds-secret", u.ID)
			return rig.f.Reset(context.Background(), "bounds-secret", pw)
		},
		"change": func(rig *flowRig, pw string) (FlowOutcome, error) {
			u := rig.user("p@example.com", true)
			return rig.f.Change(context.Background(), ChangeRequest{PrincipalID: u.ID, Current: flowPassword, New: pw})
		},
	}
	for flow, run := range flows {
		for name, pw := range bounds {
			t.Run(flow+"/"+name, func(t *testing.T) {
				rig := newFlowRig(t, true)
				out, err := run(rig, pw)
				if err != nil || out.Kind != FlowPolicy || len(out.Violations) == 0 {
					t.Fatalf("%s = %+v, %v; want a policy refusal", flow, out, err)
				}
				for _, call := range rig.rec.names() {
					if call == "PeekResetLink" || call == "ProvisionLocal" || call == "SetPasswordEndingSessions" {
						t.Errorf("%s made the call %s before refusing", flow, call)
					}
				}
				if flow == "reset" {
					if _, ok := rig.links.redeem[sha256.Sum256([]byte("bounds-secret"))]; !ok {
						t.Error("the link was consumed")
					}
				}
			})
		}
	}
}

// Reset applies the same password policy as register and change: eleven
// characters are refused and keep the link usable, twelve are accepted, and
// bcrypt's 72 bytes are the largest accepted size.
func TestResetUsesSharedPasswordPolicy(t *testing.T) {
	cases := []struct {
		name, password string
		want           FlowKind
	}{
		{"11 characters", "Aa1-xyzabcd", FlowPolicy},
		{"12 characters", "Aa1-xyzabcde", FlowOK},
		{"no digit", "Abcdefghijkl", FlowPolicy},
		{"72 bytes", "Aa1-" + strings.Repeat("x", 68), FlowOK},
		{"73 bytes", "Aa1-" + strings.Repeat("x", 69), FlowPolicy},
		{"129 characters", "Aa1-" + strings.Repeat("x", 125), FlowPolicy},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newFlowRig(t, true)
			u := rig.user("r@example.com", true)
			rig.grant("reset-secret", u.ID)
			out, err := rig.f.Reset(context.Background(), "reset-secret", tc.password)
			if err != nil || out.Kind != tc.want {
				t.Fatalf("Reset = %+v, %v; want kind %s", out, err, tc.want)
			}
			if tc.want == FlowPolicy {
				if _, ok := rig.links.redeem[sha256.Sum256([]byte("reset-secret"))]; !ok {
					t.Error("a refused password consumed the link")
				}
			}
		})
	}
}

func TestVerifyAndResetRedeemOnlyTheirOwnPurpose(t *testing.T) {
	rig := newFlowRig(t, true)
	u := rig.user("p@example.com", false)
	rig.grant("verify-secret", u.ID)
	rig.grant("reset-secret", u.ID)
	if _, err := rig.f.Verify(context.Background(), "verify-secret"); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.f.Reset(context.Background(), "reset-secret", "A-new-pass-77"); err != nil {
		t.Fatal(err)
	}
	got := []Purpose{}
	for _, r := range rig.links.redeemed {
		got = append(got, r.Purpose)
	}
	if want := []Purpose{PurposeEmailVerification}; !reflect.DeepEqual(got, want) {
		t.Fatalf("redeemed purposes = %v, want %v", got, want)
	}
	if _, ok := rig.links.redeem[sha256.Sum256([]byte("reset-secret"))]; ok {
		t.Error("the reset link was not consumed by the password write")
	}
}

func TestRegisterOutcomeIsTheSameForNewAndExistingAddress(t *testing.T) {
	fresh := newFlowRig(t, true)
	newOut, err := fresh.f.Register(context.Background(), RegisterRequest{Email: "new@example.com", Password: flowPassword})
	if err != nil {
		t.Fatal(err)
	}
	taken := newFlowRig(t, true)
	taken.user("taken@example.com", true)
	oldOut, err := taken.f.Register(context.Background(), RegisterRequest{Email: "taken@example.com", Password: flowPassword})
	if err != nil {
		t.Fatal(err)
	}
	newBytes, _ := json.Marshal(newOut)
	oldBytes, _ := json.Marshal(oldOut)
	if !bytes.Equal(newBytes, oldBytes) || !reflect.DeepEqual(newOut, oldOut) || newOut.PrincipalID != uuid.Nil {
		t.Errorf("outcomes differ or carry an id: new %+v, existing %+v", newOut, oldOut)
	}
}

func TestResetWhoseLinkWasUsedFirstChangesNothing(t *testing.T) {
	rig := newFlowRig(t, true)
	u := rig.user("r@example.com", true)
	rig.grant("reset-secret", u.ID)
	digest := sha256.Sum256([]byte("reset-secret"))
	rig.accounts.afterPeek = func() { delete(rig.links.redeem, digest) }
	out, err := rig.f.Reset(context.Background(), "reset-secret", "A-new-pass-77")
	if err != nil || out.Kind != FlowInvalidToken || len(rig.accounts.hashes) != 0 {
		t.Fatalf("Reset = %+v, %v, hash writes %d; want the invalid-token answer and no write", out, err, len(rig.accounts.hashes))
	}
}

func TestAFailedResetWriteLeavesTheLinkUsable(t *testing.T) {
	rig := newFlowRig(t, true)
	u := rig.user("r@example.com", true)
	rig.grant("reset-secret", u.ID)
	rig.accounts.writeErr = errors.New("store down")
	if _, err := rig.f.Reset(context.Background(), "reset-secret", "A-new-pass-77"); err == nil {
		t.Fatal("Reset succeeded although the write failed")
	}
	if _, ok := rig.links.redeem[sha256.Sum256([]byte("reset-secret"))]; !ok {
		t.Fatal("the failed write burned the link")
	}
	rig.accounts.writeErr = nil
	if out, err := rig.f.Reset(context.Background(), "reset-secret", "A-new-pass-77"); err != nil || out.Kind != FlowOK {
		t.Fatalf("Reset after the failure = %+v, %v", out, err)
	}
}

func TestAChangeIssuesItsSessionInTheSameWrite(t *testing.T) {
	rig := newFlowRig(t, true)
	u := rig.user("c@example.com", true)
	rig.accounts.writeErr = errors.New("store down")
	out, err := rig.f.Change(context.Background(), ChangeRequest{PrincipalID: u.ID, Current: flowPassword, New: "Brand-new-pass-42"})
	if err == nil || out.Issued != nil || len(rig.accounts.created) != 0 {
		t.Fatalf("Change = %+v, %v, sessions %d; want an error and no session", out, err, len(rig.accounts.created))
	}
	rig.accounts.writeErr = nil
	out, err = rig.f.Change(context.Background(), ChangeRequest{PrincipalID: u.ID, Current: flowPassword, New: "Brand-new-pass-42"})
	if err != nil || out.Issued == nil || len(rig.accounts.created) != 1 || len(rig.sessions.created) != 0 {
		t.Fatalf("Change = %+v, %v, write-created %d, separate Create %d; want the session made inside the write", out, err, len(rig.accounts.created), len(rig.sessions.created))
	}
}

func TestForgotAndResetTreatAServiceProviderAccountAsUnknown(t *testing.T) {
	rig := newFlowRig(t, true)
	u := rig.user("svc@example.com", true)
	service := "service"
	u.AuthProvider = &service
	if err := rig.f.Forgot(context.Background(), "svc@example.com"); err != nil {
		t.Fatal(err)
	}
	iss, msg := rig.links.issued[len(rig.links.issued)-1], rig.mail.sent[len(rig.mail.sent)-1]
	if iss.PrincipalID != uuid.Nil || msg.Deliver {
		t.Errorf("Forgot for a service-provider account: principal set %v, deliver %v; want the unknown-address decoy", iss.PrincipalID != uuid.Nil, msg.Deliver)
	}
	rig.grant("svc-secret", u.ID)
	out, err := rig.f.Reset(context.Background(), "svc-secret", "A-new-pass-77")
	if err != nil || out.Kind != FlowInvalidToken {
		t.Fatalf("Reset with a service-provider account's link = %+v, %v; want %s", out, err, FlowInvalidToken)
	}
	if len(rig.accounts.hashes) != 0 {
		t.Errorf("a password was set: %v", rig.accounts.hashes)
	}
}

func nameOf(m Message) string {
	if m.FullName == nil {
		return "<nil>"
	}
	return *m.FullName
}

func reissueFlows() map[string]func(*flowRig, string) error {
	return map[string]func(*flowRig, string) error{
		"resend": func(r *flowRig, e string) error { return r.f.Resend(context.Background(), e) },
		"forgot": func(r *flowRig, e string) error { return r.f.Forgot(context.Background(), e) },
	}
}

func TestResendCarriesTheDisplayNameToTheMail(t *testing.T) {
	rig := newFlowRig(t, true)
	name := "Pat Example"
	rig.user("pending@example.com", false).FullName = &name
	if err := reissueFlows()["resend"](rig, "pending@example.com"); err != nil {
		t.Fatal(err)
	}
	if got := nameOf(rig.mail.sent[0]); got != name {
		t.Errorf("resend mail name = %q, want %q", got, name)
	}
}

func TestForgotCarriesTheDisplayNameToTheMail(t *testing.T) {
	rig := newFlowRig(t, true)
	name := "Pat Example"
	rig.user("pat@example.com", true).FullName = &name
	if err := reissueFlows()["forgot"](rig, "pat@example.com"); err != nil {
		t.Fatal(err)
	}
	if got := nameOf(rig.mail.sent[0]); got != name {
		t.Errorf("forgot mail name = %q, want %q", got, name)
	}
}

func TestResendAndForgotCarryANonASCIIDisplayName(t *testing.T) {
	name := "Zoë Müller 山田"
	for flow, do := range reissueFlows() {
		rig := newFlowRig(t, true)
		rig.user("zoe@example.com", false).FullName = &name
		if err := do(rig, "zoe@example.com"); err != nil {
			t.Fatal(err)
		}
		if got := nameOf(rig.mail.sent[0]); got != name {
			t.Errorf("%s: mail name = %q, want %q", flow, got, name)
		}
	}
}

func TestResendAndForgotWithNoDisplayNameKeepTheAddressGreeting(t *testing.T) {
	empty := ""
	for flow, do := range reissueFlows() {
		for label, fullName := range map[string]*string{"nil": nil, "empty": &empty} {
			rig := newFlowRig(t, true)
			rig.user("none@example.com", false).FullName = fullName
			if err := do(rig, "none@example.com"); err != nil {
				t.Fatal(err)
			}
			if got := rig.mail.sent[0].FullName; got != nil && *got != "" {
				t.Errorf("%s/%s: mail name = %q, want none", flow, label, *got)
			}
		}
	}
}

func TestResendAndForgotDecoysCarryNoName(t *testing.T) {
	name := "Pat Example"
	for flow, do := range reissueFlows() {
		rig := newFlowRig(t, true)
		rig.user("nobody-else@example.com", true).FullName = &name
		if flow == "resend" {
			rig.user("verified@example.com", true).FullName = &name
		} else {
			provider := edgetokenmint.ServiceAuthProvider
			rig.user("verified@example.com", true).FullName, rig.accounts.byEmail["verified@example.com"].AuthProvider = &name, &provider
		}
		for _, address := range []string{"unknown@example.com", "verified@example.com"} {
			if err := do(rig, address); err != nil {
				t.Fatal(err)
			}
		}
		for i, msg := range rig.mail.sent {
			if msg.Deliver || msg.FullName != nil {
				t.Errorf("%s: decoy %d deliver=%v name=%q", flow, i, msg.Deliver, nameOf(msg))
			}
		}
	}
}
