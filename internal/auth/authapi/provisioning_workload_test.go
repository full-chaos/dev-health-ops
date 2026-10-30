package authapi

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
)

// provWorkloadFixture is the provisioning routes with the go-api workload path
// on: the same SessionIssueWorkload the session issue route is given (one
// subject, AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT). w mints tokens and counts
// key-set reads.
type provWorkloadFixture struct {
	*provFixture
	w *workloadIssueFixture
}

func newProvWorkloadFixture(t *testing.T) *provWorkloadFixture {
	t.Helper()
	w := newWorkloadIssueFixture(t)
	return &provWorkloadFixture{provFixture: newProvFixtureFor(t, nil, w.workload), w: w}
}

// callerOf is the caller the route handed to its verb.
func callerOf(f *provFixture, op string) audit.Caller {
	switch op {
	case opMembershipAdd, opMembershipRole, opMembershipRemove:
		return f.members.change.Actor.Caller
	case opMembershipTransfer:
		return f.members.transfer.Actor.Caller
	case opInviteCreate:
		return f.invites.create.Actor.Caller
	case opInviteAccept:
		return f.invites.accept.Actor.Caller
	case opOrgDeprovision:
		return f.deprovision.in.Actor.Caller
	case opOrgReactivate:
		return f.reactivate.actor.Caller
	}
	f.t.Fatalf("no caller for %s", op)
	return audit.Caller{}
}

// Every provisioning route accepts the go-api workload token (audience
// auth-service, subject the configured go-api principal), with no service
// credential check and no compat count, and hands the verb the workload
// principal and the token's jti as the caller.
func TestProvisioningRoutesAcceptTheGoAPIWorkloadToken(t *testing.T) {
	f := newProvWorkloadFixture(t)
	want := uuidOfWire(t, f.w.subject)
	for _, c := range routeCases() {
		header, jti := f.w.mint("workload+jwt", nil)
		response := f.post(c.path, encode(c.body(f.provFixture, f.super)), header)
		if response.Code != http.StatusOK {
			t.Fatalf("%s with the workload token: %d %s", c.op, response.Code, response.Body)
		}
		if got, _ := f.uses(c.op); got != 0 {
			t.Fatalf("%s: compat uses = %d after a workload call, want 0", c.op, got)
		}
		caller := callerOf(f.provFixture, c.op)
		if caller.Workload == nil || *caller.Workload != want || caller.JTI != jti || caller.CredentialID != "" {
			t.Fatalf("%s: caller = %+v, want workload %s and jti %s", c.op, caller, want, jti)
		}
	}
	if len(f.creds.scopes) != 0 {
		t.Fatalf("a workload call reached the service credential check %d times", len(f.creds.scopes))
	}
}

// The session issuer credential keeps working beside it: the verb gets the
// credential row id as the caller and the compat counter counts the use.
func TestProvisioningServiceCredentialCallerIsTheCredentialRow(t *testing.T) {
	f := newProvWorkloadFixture(t)
	for _, c := range routeCases() {
		if response := f.post(c.path, encode(c.body(f.provFixture, f.super)), nil); response.Code != http.StatusOK {
			t.Fatalf("%s with the service credential: %d %s", c.op, response.Code, response.Body)
		}
		caller := callerOf(f.provFixture, c.op)
		if caller.CredentialID != "cred-1" || caller.Workload != nil || caller.JTI != "" {
			t.Fatalf("%s: caller = %+v, want the credential row", c.op, caller)
		}
		if got, _ := f.uses(c.op); got != 1 {
			t.Fatalf("%s: compat uses = %d, want 1", c.op, got)
		}
	}
	if f.w.keys.reads != 0 {
		t.Fatalf("a service credential call read the key set %d times", f.w.keys.reads)
	}
}

// Every other credential class, a wrong audience, subject or typ and malformed
// or oversized values get the same 401 body as a refused session issuer
// credential, before any credential check, users read, verb or key-set read.
func TestProvisioningRoutesRefuseWrongCredentialsBeforeAnyRead(t *testing.T) {
	golden := func() string {
		f := newProvWorkloadFixture(t)
		f.creds.refusal = servicecred.RefusedRevoked
		c := routeCases()[0]
		return refusalWithoutPerRequestFields(t, f.post(c.path, encode(c.body(f.provFixture, f.super)), nil).Body.Bytes())
	}()
	for name, build := range issuePreKeyRefusals() {
		for _, c := range routeCases() {
			f := newProvWorkloadFixture(t)
			response := f.post(c.path, encode(c.body(f.provFixture, f.super)), build(f.w))
			expectRefusal(t, response, http.StatusUnauthorized, reasonInvalidCredentials)
			if len(f.creds.scopes) != 0 || f.storeCalls() != 0 || f.users.reads != 0 || f.w.keys.reads != 0 {
				t.Fatalf("%s %s: read before the verdict (checks %d, verbs %d, users %d, key set %d)", name, c.op,
					len(f.creds.scopes), f.storeCalls(), f.users.reads, f.w.keys.reads)
			}
			if got := refusalWithoutPerRequestFields(t, response.Body.Bytes()); got != golden {
				t.Fatalf("%s %s: refusal body %s differs from %s", name, c.op, got, golden)
			}
		}
	}
}

// A token that passes the peek but fails the verifier (expired, not yet valid,
// wrong issuer, lifetime over the ceiling, tampered) is the same 401 with no
// credential check, users read or verb.
func TestProvisioningRoutesRefuseATokenTheVerifierRefuses(t *testing.T) {
	for name, build := range issueVerifierRefusals() {
		for _, c := range routeCases() {
			f := newProvWorkloadFixture(t)
			expectRefusal(t, f.post(c.path, encode(c.body(f.provFixture, f.super)), build(f.w)), http.StatusUnauthorized, reasonInvalidCredentials)
			if len(f.creds.scopes) != 0 || f.storeCalls() != 0 || f.users.reads != 0 {
				t.Fatalf("%s %s: a refused token reached a store", name, c.op)
			}
		}
	}
}

// With no workload path (the subject variable unset), a workload token is
// refused with no read and the session issuer credential is unchanged.
func TestProvisioningRefusesTheWorkloadTokenWhenThePathIsOff(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	f := newProvFixture(t)
	for _, c := range routeCases() {
		header, _ := w.mint("workload+jwt", nil)
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.super)), header), http.StatusUnauthorized, reasonInvalidCredentials)
		if len(f.creds.scopes) != 0 || f.storeCalls() != 0 || f.users.reads != 0 || w.keys.reads != 0 {
			t.Fatalf("%s: a workload token with the path off reached a store", c.op)
		}
	}
	for _, c := range routeCases() {
		if response := f.post(c.path, encode(c.body(f, f.super)), nil); response.Code != http.StatusOK {
			t.Fatalf("%s with the service credential: %d", c.op, response.Code)
		}
	}
}

// A key set that cannot be read is a 503, never a 401 and never a pass.
func TestProvisioningWorkloadKeySetOutageIs503(t *testing.T) {
	f := newProvWorkloadFixture(t)
	f.w.keys.err = errors.New("custody down")
	for _, c := range routeCases() {
		header, _ := f.w.mint("workload+jwt", nil)
		expectRefusal(t, f.post(c.path, encode(c.body(f.provFixture, f.super)), header), http.StatusServiceUnavailable, reasonDependency)
	}
	if f.storeCalls() != 0 || f.users.reads != 0 || len(f.creds.scopes) != 0 {
		t.Fatal("a key-set outage reached a store")
	}
}

// No token text, jti or subject reaches the log of a workload call, accepted
// or refused; a refusal logs its reason.
func TestProvisioningWorkloadLogsCarryNoTokenText(t *testing.T) {
	f := newProvWorkloadFixture(t)
	c := routeCases()[0]
	accepted, jti := f.w.mint("workload+jwt", nil)
	f.post(c.path, encode(c.body(f.provFixture, f.super)), accepted)
	wrong := issuePreKeyRefusals()["wrong subject"](f.w)
	f.post(c.path, encode(c.body(f.provFixture, f.super)), wrong)
	logs := f.logs.String()
	for _, header := range []map[string]string{accepted, wrong} {
		token := strings.TrimPrefix(header["Authorization"], "Bearer ")
		for _, part := range strings.Split(token, ".") {
			if strings.Contains(logs, part) {
				t.Fatalf("a token segment reached the log")
			}
		}
	}
	if strings.Contains(logs, jti) || strings.Contains(logs, f.w.subject) {
		t.Fatal("the jti or the subject reached the log")
	}
	if !strings.Contains(logs, `"refusal":"subject"`) {
		t.Fatalf("the refusal reason is not logged: %s", logs)
	}
}
