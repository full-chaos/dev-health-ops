package authapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/callseq"
)

type recLoginUsers struct {
	*fakeLoginUsers
	rec *callseq.Recorder
}

func (r recLoginUsers) ByEmail(ctx context.Context, email string) (*local.User, error) {
	r.rec.Add("ByEmail")
	return r.fakeLoginUsers.ByEmail(ctx, email)
}

func (r recLoginUsers) Project(ctx context.Context, id uuid.UUID) error {
	r.rec.Add("LoginProject")
	return r.fakeLoginUsers.Project(ctx, id)
}

type recAttempts struct {
	*fakeAttempts
	rec *callseq.Recorder
}

func (r recAttempts) Check(ctx context.Context, id []byte) (bool, time.Duration, error) {
	r.rec.Add("Check")
	return r.fakeAttempts.Check(ctx, id)
}

func (r recAttempts) RecordFailure(ctx context.Context, id []byte) error {
	r.rec.Add("RecordFailure")
	return r.fakeAttempts.RecordFailure(ctx, id)
}

type recUsers struct {
	fakeUsers
	rec *callseq.Recorder
}

func (r recUsers) Memberships(ctx context.Context, id uuid.UUID) ([]Membership, error) {
	r.rec.Add("Memberships")
	return r.fakeUsers.Memberships(ctx, id)
}

type recProjector struct {
	*fakeProjector
	rec *callseq.Recorder
}

func (r recProjector) Project(ctx context.Context, id uuid.UUID) error {
	r.rec.Add("Project")
	return r.fakeProjector.Project(ctx, id)
}

func (r recProjector) ProjectOrganization(ctx context.Context, id uuid.UUID) error {
	r.rec.Add("ProjectOrganization")
	return r.fakeProjector.ProjectOrganization(ctx, id)
}

// loginStepsRig is a compat login fixture whose store, projector and audit calls are recorded in order.
type loginStepsRig struct {
	*compatFixture
	rec    *callseq.Recorder
	events []audit.AuditEvent
}

func newLoginStepsRig(t *testing.T) *loginStepsRig {
	t.Helper()
	f := newCompatFixture(t)
	hash := "hash:" + compatPassword
	// Every known account has the memberships of the base user, so only the refusal kind differs.
	shared := f.users.memberships[f.principal]
	for _, c := range []struct {
		email  string
		id     uuid.UUID
		active bool
		hash   *string
	}{{"erin@example.com", uuid.New(), true, nil}, {"dave@example.com", uuid.New(), false, &hash}} {
		f.loginUsers.byEmail[c.email] = &local.User{ID: c.id, IsActive: c.active, IsVerified: true, PasswordHash: c.hash}
		f.users.memberships[c.id] = shared
	}
	r := &loginStepsRig{compatFixture: f, rec: &callseq.Recorder{}}
	routes, err := CompatRoutes(CompatDeps{
		Sessions: f.sessions, Users: recUsers{f.users, r.rec}, Issuer: f.issuer,
		Logger:    slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Projector: recProjector{f.projector, r.rec},
		AppendAudit: func(_ context.Context, event audit.AuditEvent) error {
			r.rec.Add("AppendAudit")
			r.events = append(r.events, event)
			return nil
		},
		ServiceCredentials: f.creds,
		Login: &LoginConfig{
			Users: recLoginUsers{f.loginUsers, r.rec}, Attempts: recAttempts{f.attempts, r.rec},
			LockoutKey: contracts.NewSecretString(testLockoutKey),
			Compare:    func(hash, password string) bool { return hash == "hash:"+password },
		},
		Limits: mustLimits(t, f.counters),
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range MarkResponseModels(routes) {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(flaggedHandler(route)))
	}
	f.server = mux
	return r
}

// loginSteps runs one refused login on a fresh rig and returns its calls, its audit row and the id the store
// holds for the address (uuid.Nil for an unknown address).
func loginSteps(t *testing.T, email, password string, requestedOrg, locked bool) ([]string, audit.AuditEvent, uuid.UUID) {
	t.Helper()
	r := newLoginStepsRig(t)
	r.attempts.locked, r.attempts.remaining = locked, time.Minute
	org := ""
	if requestedOrg {
		org = r.orgD.String()
	}
	r.do(http.MethodPost, PatternCompatLogin, loginBody(email, password, org), nil)
	if len(r.events) != 1 {
		t.Fatalf("%s: %d audit rows, want 1", email, len(r.events))
	}
	var user uuid.UUID
	if u := r.loginUsers.byEmail[email]; u != nil {
		user = u.ID
	}
	return r.rec.Names(), r.events[0], user
}

// A refused sign-in of a known account takes the same steps whatever the refusal (no password, wrong password,
// disabled, locked); against an unknown account it adds exactly the principal lookups the audit row needs. The
// audit row names the principal on purpose (go-api parity, every known refusal).
func TestCompatLoginRefusalsTakeOneStepSequence(t *testing.T) {
	for _, requested := range []bool{false, true} {
		t.Run(fmt.Sprintf("requested org %v", requested), func(t *testing.T) {
			known := map[string]struct {
				email, password string
				locked          bool
			}{
				"no password":    {"erin@example.com", compatPassword, false},
				"wrong password": {compatEmail, "wrong", false},
				"disabled":       {"dave@example.com", compatPassword, false},
				"locked":         {compatEmail, compatPassword, true},
			}
			sequences := map[string][]string{}
			var noPassword audit.AuditEvent
			for name, c := range known {
				seq, event, user := loginSteps(t, c.email, c.password, requested, c.locked)
				sequences[name] = seq
				subject := "<none>"
				if event.SubjectPrincipalID != nil {
					subject = *event.SubjectPrincipalID
				}
				if subject != user.String() {
					t.Errorf("%s: the audit row names %s, want the account %s", name, subject, user)
				}
				if name == "no password" {
					noPassword = event
				}
			}
			callseq.Same(t, "known-account refusals", sequences)

			unknown, event, _ := loginSteps(t, "nobody@example.com", "x", requested, false)
			if event.SubjectPrincipalID != nil {
				t.Errorf("unknown account: the audit row names a principal")
			}
			if event.Attributes["reason_code"] != noPassword.Attributes["reason_code"] {
				t.Errorf("reason codes differ: unknown %v, no password %v", event.Attributes["reason_code"], noPassword.Attributes["reason_code"])
			}
			// The requested organization is one no account is a member of, so with or without it a known
			// account adds the same steps over an unknown one: its memberships, its first organization and itself.
			extra := []string{"Memberships", "ProjectOrganization", "Project"}
			callseq.Delta(t, "no password vs unknown", unknown, sequences["no password"], extra)
		})
	}
}
