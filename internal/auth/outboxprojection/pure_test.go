package outboxprojection

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
)

func sampleEvent() contracts.ExternalPushLifecycleEvent {
	at := contracts.Timestamp{Time: time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)}
	return contracts.ExternalPushLifecycleEvent{
		AggregateType: contracts.ExternalPushCredentialAggregate, AggregateID: "0a1b2c3d-0000-4000-8000-000000000001",
		EventType: contracts.ExternalPushCreated, CreatedAt: at,
		Payload: contracts.ExternalPushLifecyclePayload{
			Revision: 1, OrganizationID: "org_" + uuid.NewString(), SourceID: "src-1",
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
		OutcomeUnfenced: ClassUnfenced,
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
	if Digest(a) != Digest(b) {
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
		if Digest(c) == Digest(a) {
			t.Errorf("a changed %s leaves the digest unchanged", name)
		}
	}
}

func TestVerdictComparesTheStoredDigest(t *testing.T) {
	d := Digest(sampleEvent())
	other := d
	other[0] ^= 1
	if Compare(nil, d) != VerdictNew || Compare(d[:], d) != VerdictSame || Compare(other[:], d) != VerdictDifferent {
		t.Fatal("Compare does not tell new, same and different apart")
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
	if got := Narrow(active, nil, nil); got != active {
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
	ok := Input{Domain: DomainExternalPush, Event: sampleEvent()}
	if err := ok.Check(); err != nil {
		t.Fatalf("a valid push input: %v", err)
	}
	acr := Input{Domain: DomainAcr, Event: sampleEvent(), AcrClass: "acr_client_credential", OwnerSubject: "sub-1"}
	acr.Event.AggregateID = "cred_abcdef"
	if err := acr.Check(); err != nil {
		t.Fatalf("a valid acr input: %v", err)
	}
	cases := map[string]func(*Input){
		"no source id":           func(i *Input) { i.Event.Payload.SourceID = "" },
		"no organization":        func(i *Input) { i.Event.Payload.OrganizationID = "" },
		"organization not org_":  func(i *Input) { i.Event.Payload.OrganizationID = "org_not-a-uuid" },
		"no aggregate id":        func(i *Input) { i.Event.AggregateID = "" },
		"id with a slash":        func(i *Input) { i.Event.AggregateID = "a/b" },
		"revision zero":          func(i *Input) { i.Event.Payload.Revision = 0 },
		"unknown domain":         func(i *Input) { i.Domain = "stripe" },
		"acr with no class":      func(i *Input) { i.Domain, i.AcrClass = DomainAcr, "" },
		"acr with a wrong class": func(i *Input) { i.Domain, i.AcrClass = DomainAcr, "workload_token" },
		"push with an acr class": func(i *Input) { i.AcrClass = "acr_client_credential" },
	}
	for name, edit := range cases {
		in := ok
		edit(&in)
		if err := in.Check(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	browser := acr
	browser.AcrClass = "acr_browser_refresh_token"
	if err := browser.Check(); err != nil {
		t.Errorf("the browser refresh class: %v", err)
	}
}
