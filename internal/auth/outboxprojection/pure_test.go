package outboxprojection

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
)

const sampleOrg = "7f1c0d2e-0000-4000-8000-0000000000a1"

func pushIn(e contracts.ExternalPushLifecycleEvent) Input {
	return Input{Domain: DomainExternalPush, Event: e}
}

func acrIn() Input {
	e := sampleEvent()
	e.AggregateID = "cred_abcdef"
	return Input{Domain: DomainAcr, Event: e, AcrClass: "browser", OwnerSubject: "sub-1", BodyDigest: [32]byte{1, 2, 3}, Actor: uuid.MustParse("0a1b2c3d-0000-4000-8000-0000000000c1")}
}

func sampleEvent() contracts.ExternalPushLifecycleEvent {
	at := contracts.Timestamp{Time: time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)}
	return contracts.ExternalPushLifecycleEvent{
		AggregateType: contracts.ExternalPushCredentialAggregate, AggregateID: "0a1b2c3d-0000-4000-8000-000000000001",
		EventType: contracts.ExternalPushCreated, CreatedAt: at,
		Payload: contracts.ExternalPushLifecyclePayload{
			Revision: 1, OrganizationID: "org_" + sampleOrg, Binding: contracts.ExternalPushBindingSource, SourceID: "src-1",
			Change: contracts.ExternalPushLifecycleChange{Fingerprint: "0123456789abcdef", Scopes: []string{"ingest:write"}},
		},
	}
}

// A retry outcome is the only class a publisher may wait on; a refused parent,
// a conflict and a malformed event are final, and an unfenced timeline is 503.
func TestEveryOutcomeHasOneClassAndOnlyAGapAndAPendingParentRetry(t *testing.T) {
	want := map[Outcome]Class{
		pushlifecycle.Applied: ClassDone, pushlifecycle.Duplicate: ClassDone, pushlifecycle.Stale: ClassDone,
		pushlifecycle.Gap: ClassRetry, pushlifecycle.ParentPending: ClassRetry,
		pushlifecycle.RefusedTerminal: ClassRefused, pushlifecycle.RefusedWiden: ClassRefused,
		pushlifecycle.RefusedTransition: ClassRefused, pushlifecycle.RefusedParent: ClassRefused,
		OutcomeConflict: ClassConflict, OutcomeMalformed: ClassRefused, OutcomeOrganizationUnknown: ClassRefused,
		OutcomeUnfenced: ClassUnfenced, OutcomeStateChanged: ClassRetry,
	}
	for _, core := range pushlifecycle.Outcomes {
		if _, ok := want[core]; !ok {
			t.Errorf("core outcome %s has no row in the class table", core)
		}
	}
	for outcome, class := range want {
		if got := ClassOf(outcome); got != class {
			t.Errorf("ClassOf(%s) = %s, want %s", outcome, got, class)
		}
	}
	if len(Outcomes) != len(want) {
		t.Errorf("Outcomes lists %d values, the class table %d", len(Outcomes), len(want))
	}
	if ClassOf(Outcome("nonsense")) != ClassRefused {
		t.Error("an unknown outcome must fail closed as refused")
	}
}

func TestDigestIsStableAndNamesTheWholeBody(t *testing.T) {
	a, b := sampleEvent(), sampleEvent()
	if Digest(pushIn(a)) != Digest(pushIn(b)) {
		t.Fatal("the same event digests differently")
	}
	for name, edit := range map[string]func(*contracts.ExternalPushLifecycleEvent){
		"scopes":      func(e *contracts.ExternalPushLifecycleEvent) { e.Payload.Change.Scopes = []string{"ingest:read"} },
		"fingerprint": func(e *contracts.ExternalPushLifecycleEvent) { e.Payload.Change.Fingerprint = "fedcba9876543210" },
		"created_at":  func(e *contracts.ExternalPushLifecycleEvent) { e.CreatedAt.Time = e.CreatedAt.Time.Add(time.Second) },
		"source":      func(e *contracts.ExternalPushLifecycleEvent) { e.Payload.SourceID = "src-2" },
		"event type":  func(e *contracts.ExternalPushLifecycleEvent) { e.EventType = contracts.ExternalPushRevoked },
		"revision":    func(e *contracts.ExternalPushLifecycleEvent) { e.Payload.Revision = 2 },
	} {
		c := sampleEvent()
		edit(&c)
		if Digest(pushIn(c)) == Digest(pushIn(a)) {
			t.Errorf("a changed %s leaves the digest unchanged", name)
		}
	}
}

func TestVerdictComparesTheStoredDigest(t *testing.T) {
	d := Digest(pushIn(sampleEvent()))
	other := d
	other[0] ^= 1
	if Compare(nil, d) != VerdictNew || Compare(d[:], d) != VerdictSame || Compare(other[:], d) != VerdictDifferent {
		t.Fatal("Compare does not tell new, same and different apart")
	}
	if Compare(d[:16], d) != VerdictDifferent {
		t.Fatal("a stored prefix of the digest must read as different, never as same")
	}
	if Compare([]byte{1, 2}, d) != VerdictDifferent {
		t.Fatal("a stored digest of the wrong length must read as different, never as same")
	}
}

func TestRevokeReasonMapsIntoTheClosedList(t *testing.T) {
	for in, want := range map[string]string{
		"": "domain_revoked", "compromised": "compromised", "owner_request": "owner_request",
		"expired": "domain_revoked", "anything else": "domain_revoked", "Compromised": "domain_revoked",
	} {
		if got := MapRevokeReason(in); got != want {
			t.Errorf("MapRevokeReason(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNarrowOnlyEverShortensAParent(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	active := pushlifecycle.State{Status: pushlifecycle.StatusActive, ExpiresAt: "2027-01-01T00:00:00.000000Z"}
	if got := Narrow(active, nil, nil); !reflect.DeepEqual(got, active) {
		t.Errorf("no bound changed the state: %+v", got)
	}
	revoked := Narrow(active, &t0, nil)
	if revoked.Status != pushlifecycle.StatusRevoked || !revoked.TerminalAt.Equal(t0) {
		t.Errorf("a revoked registry row must read as a revoked parent at its time: %+v", revoked)
	}
	earlier := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	if got := Narrow(active, nil, &earlier); got.ExpiresAt != "2026-12-01T00:00:00.000000Z" {
		t.Errorf("an earlier registry expiry must win, got %q", got.ExpiresAt)
	}
	later := time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := Narrow(active, nil, &later); got.ExpiresAt != active.ExpiresAt {
		t.Errorf("a later registry expiry must never extend, got %q", got.ExpiresAt)
	}
	terminal := pushlifecycle.State{Status: pushlifecycle.StatusExpired, TerminalAt: t0}
	if got := Narrow(terminal, &earlier, nil); !got.TerminalAt.Equal(t0) || got.Status != pushlifecycle.StatusExpired {
		t.Errorf("an already terminal state must not be rewritten: %+v", got)
	}
}

func TestInputCheckRefusesWhatCannotBeKeyed(t *testing.T) {
	ok := pushIn(sampleEvent())
	if err := ok.Check(); err != nil {
		t.Fatalf("a valid push input: %v", err)
	}
	if err := acrIn().Check(); err != nil {
		t.Fatalf("a valid acr input: %v", err)
	}
	cases := map[string]func(*Input){
		"no source id":                func(i *Input) { i.Event.Payload.SourceID = "" },
		"no organization":             func(i *Input) { i.Event.Payload.OrganizationID = "" },
		"organization not org_":       func(i *Input) { i.Event.Payload.OrganizationID = "org_not-a-uuid" },
		"organization with no prefix": func(i *Input) { i.Event.Payload.OrganizationID = sampleOrg },
		"no aggregate id":             func(i *Input) { i.Event.AggregateID = "" },
		"id with a slash":             func(i *Input) { i.Event.AggregateID = "a/b" },
		"revision zero":               func(i *Input) { i.Event.Payload.Revision = 0 },
		"unknown domain":              func(i *Input) { i.Domain = "stripe" },
		"push with an acr class":      func(i *Input) { i.AcrClass = "browser" },
		"push with an owner":          func(i *Input) { i.OwnerSubject = "sub-1" },
	}
	for name, edit := range cases {
		in := ok
		edit(&in)
		if err := in.Check(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	acrCases := map[string]func(*Input){
		"acr with no class at revision 1":    func(i *Input) { i.AcrClass = "" },
		"acr with a registry class value":    func(i *Input) { i.AcrClass = "acr_client_credential" },
		"acr with the browser refresh class": func(i *Input) { i.AcrClass = "acr_browser_refresh_token" },
		"acr with a seventh class":           func(i *Input) { i.AcrClass = "service" },
		"acr with no body digest":            func(i *Input) { i.BodyDigest = [32]byte{} },
		"acr with no actor":                  func(i *Input) { i.Actor = uuid.Nil },
		"acr later revision, a bad class":    func(i *Input) { i.Event.Payload.Revision, i.AcrClass = 2, "service" },
	}
	for name, edit := range acrCases {
		in := acrIn()
		edit(&in)
		if err := in.Check(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	for _, class := range []string{"browser", "oauth_user", "native_device", "org_bearer", "operator", "workload"} {
		in := acrIn()
		in.AcrClass = class
		if err := in.Check(); err != nil {
			t.Errorf("wire class %s: %v", class, err)
		}
	}
	// The contract carries the class and the owner at revision 1 only: a revoke has neither and must land.
	revoke := acrIn()
	revoke.Event.Payload.Revision, revoke.AcrClass, revoke.OwnerSubject = 2, "", ""
	if err := revoke.Check(); err != nil {
		t.Errorf("an acr event at revision 2 with no class or owner: %v", err)
	}
	if err := ok.Check(); err != nil || ok.Actor != uuid.Nil {
		t.Errorf("a push input needs no actor: %v", err)
	}
}

// The registry class of every acr credential is acr_client_credential; the wire class is kept as projected data.
func TestRegistryClassIsOneValuePerDomain(t *testing.T) {
	if got := RegistryClass(pushIn(sampleEvent())); got != "external_push_ingest_token" {
		t.Errorf("push class %q", got)
	}
	for _, wire := range []string{"browser", "oauth_user", "native_device", "org_bearer", "operator", "workload"} {
		in := acrIn()
		in.AcrClass = wire
		if got := RegistryClass(in); got != "acr_client_credential" {
			t.Errorf("acr wire class %s maps to %q, want acr_client_credential", wire, got)
		}
	}
}

// An acr event is keyed by the SHA-256 of the posted bytes: the mapped push event drops fields (the owner, the
// class), so a digest of it would call a same-revision delivery with another owner a duplicate.
func TestAcrDigestIsTheBodyDigestAndNotTheMappedEvent(t *testing.T) {
	a, b := acrIn(), acrIn()
	b.OwnerSubject, b.AcrClass = "another-owner", "operator"
	if Digest(a) != Digest(b) {
		t.Fatal("the mapped fields changed an acr digest that rests on the posted bytes alone")
	}
	c := acrIn()
	c.BodyDigest = [32]byte{9}
	if Digest(a) == Digest(c) {
		t.Fatal("a different posted body leaves the acr digest unchanged")
	}
	push := pushIn(sampleEvent())
	push.BodyDigest = [32]byte{7}
	if Digest(push) != Digest(pushIn(sampleEvent())) {
		t.Fatal("a push digest must not depend on BodyDigest")
	}
	if Digest(a) == Digest(pushIn(a.Event)) {
		t.Fatal("an acr and a push digest of one event share a value")
	}
}

// Only a decision that refused the event is a denied audit row.
func TestAuditOutcomeIsDeniedOnlyForARefusingDecision(t *testing.T) {
	for _, o := range []Outcome{pushlifecycle.RefusedTerminal, pushlifecycle.RefusedWiden, pushlifecycle.RefusedTransition, pushlifecycle.RefusedParent} {
		if AuditOutcome(o) != audit.OutcomeDenied {
			t.Errorf("%s: audit outcome %s, want denied", o, AuditOutcome(o))
		}
	}
	if AuditOutcome(pushlifecycle.Applied) != audit.OutcomeAllowed {
		t.Error("an applied decision is an allowed audit row")
	}
}

// The audit outcome is fixed before Apply decides, so a refusal is written by a second try. If the second try decides
// something other than a refusal the state moved under us: there is no third try, nothing is written, and the answer
// is a retry.
func TestASecondTryThatDecidesOtherThanRefusedIsARetryAndNeverAThirdTry(t *testing.T) {
	calls := []bool{}
	script := func(answers ...error) func(bool) (Result, error) {
		calls = calls[:0]
		return func(denied bool) (Result, error) {
			calls = append(calls, denied)
			err := answers[len(calls)-1]
			return Result{Outcome: pushlifecycle.Applied, Class: ClassDone}, err
		}
	}
	res, err := runTries(script(errOutcomeMismatch, errOutcomeMismatch))
	if err != nil || res.Outcome != OutcomeStateChanged || res.Class != ClassRetry {
		t.Fatalf("two mismatches: %+v, %v; want a state_changed retry", res, err)
	}
	if len(calls) != 2 || calls[0] || !calls[1] {
		t.Fatalf("tries %v, want exactly two (allowed, then denied) and no third", calls)
	}
	if res, err := runTries(script(errOutcomeMismatch, nil)); err != nil || res.Outcome != pushlifecycle.Applied || len(calls) != 2 {
		t.Fatalf("a mismatch then a match: %+v, %v, %d tries", res, err, len(calls))
	}
	if res, err := runTries(script(nil)); err != nil || res.Outcome != pushlifecycle.Applied || len(calls) != 1 {
		t.Fatalf("a first match: %+v, %v, %d tries", res, err, len(calls))
	}
	boom := errors.New("boom")
	if _, err := runTries(script(boom)); !errors.Is(err, boom) || len(calls) != 1 {
		t.Fatalf("an error must end the call at once: %v, %d tries", err, len(calls))
	}
}

// For acr the fingerprint is derived, not read: the first 8 bytes of the SHA-256 of the aggregate id, in hex
// (acp answers-11196 Q4), so no mapper can pass a value that two credentials share.
func TestAcrFingerprintIsTheFirstEightBytesOfTheSHA256OfTheAggregateID(t *testing.T) {
	sum := sha256.Sum256([]byte("cred_abcdef"))
	if got, want := AcrFingerprint("cred_abcdef"), hex.EncodeToString(sum[:8]); got != want {
		t.Fatalf("AcrFingerprint = %q, want %q", got, want)
	}
	if AcrFingerprint("cred_abcdef") == AcrFingerprint("cred_abcdeg") || len(AcrFingerprint("x")) != 16 {
		t.Fatal("the derived fingerprint is not 16 hex characters, one per id")
	}
}

// The binding rules of Input.Check. A source-bound event names a
// source; an organization-bound event has none (there is no placeholder source); an unknown or absent binding is refused.
func TestInputCheckAppliesTheBindingRules(t *testing.T) {
	source := pushIn(sampleEvent())
	if err := source.Check(); err != nil {
		t.Fatalf("a source-bound push input: %v", err)
	}
	org := pushIn(sampleEvent())
	org.Event.Payload.Binding, org.Event.Payload.SourceID = contracts.ExternalPushBindingOrganization, ""
	if err := org.Check(); err != nil {
		t.Fatalf("an organization-bound push input with no source: %v", err)
	}
	for name, edit := range map[string]func(*Input){
		"source binding with no source id":   func(i *Input) { i.Event.Payload.SourceID = "" },
		"organization binding with a source": func(i *Input) { i.Event.Payload.Binding = contracts.ExternalPushBindingOrganization },
		"unknown binding":                    func(i *Input) { i.Event.Payload.Binding = "team" },
		"no binding":                         func(i *Input) { i.Event.Payload.Binding = "" },
	} {
		in := source
		edit(&in)
		if err := in.Check(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	// An acr event is always source-bound (source acr): the engine derives its binding, so a mapper that leaves it empty is
	// accepted, and one with no source id is refused.
	acr := acrIn()
	acr.Event.Payload.Binding = ""
	if err := acr.Check(); err != nil {
		t.Errorf("an acr input with no binding (derived): %v", err)
	}
	acr.Event.Payload.SourceID = ""
	if err := acr.Check(); err == nil {
		t.Error("an acr input with no source id was accepted")
	}
}
