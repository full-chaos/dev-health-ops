// Package outboxprojection is the shared engine that projects one external
// credential lifecycle event into auth storage: a principal per (organization,
// source), a credential_registry row, and the projected state. The External
// Push reader and the acr endpoint both call Engine.Project with an event in
// hand; neither re-implements a rule. pushlifecycle.Apply decides, this package
// stores. It never validates a credential and holds no secret.
package outboxprojection

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
)

// Domain is the registry's domain_owner of the credentials a caller projects.
type Domain string

const (
	DomainExternalPush Domain = "external_push"
	DomainAcr          Domain = "acr"
)

// Outcome is pushlifecycle's closed outcome plus the engine's own; every value
// is safe as a metric label.
type Outcome = pushlifecycle.Outcome

const (
	// OutcomeConflict: the same revision arrived with another body. Never
	// applied, never dropped silently: counted, and final.
	OutcomeConflict Outcome = "conflict"
	// OutcomeMalformed: the event cannot be keyed (no source id, a bad id).
	OutcomeMalformed Outcome = "malformed"
	// OutcomeOrganizationUnknown: the organization is in neither projection.
	OutcomeOrganizationUnknown Outcome = "organization_unknown"
	// OutcomeUnfenced: the database is on a timeline with no restore-fence record.
	OutcomeUnfenced Outcome = "unfenced"
)

// Outcomes is every Outcome the engine can return, for label sets and tests.
var Outcomes = append(append([]Outcome(nil), pushlifecycle.Outcomes...), OutcomeConflict, OutcomeMalformed, OutcomeOrganizationUnknown, OutcomeUnfenced)

// Class is what a caller does with an outcome.
type Class string

const (
	// ClassDone: nothing more to do (applied, duplicate, stale): HTTP 204.
	ClassDone Class = "done"
	// ClassRetry: hold the aggregate and try later (gap, parent pending): 409 projection_retry.
	ClassRetry Class = "retry"
	// ClassConflict: final (409 projection_conflict).
	ClassConflict Class = "conflict"
	// ClassRefused: final, a retry cannot change it (409 projection_refused).
	ClassRefused Class = "refused"
	// ClassUnfenced: stop and retry later (503).
	ClassUnfenced Class = "unfenced"
)

// ClassOf maps an outcome to its class. An unknown outcome fails closed as refused.
func ClassOf(o Outcome) Class {
	switch o {
	case pushlifecycle.Applied, pushlifecycle.Duplicate, pushlifecycle.Stale:
		return ClassDone
	case pushlifecycle.Gap, pushlifecycle.ParentPending:
		return ClassRetry
	case OutcomeConflict:
		return ClassConflict
	case OutcomeUnfenced:
		return ClassUnfenced
	default:
		return ClassRefused
	}
}

// Input is one event in hand. Event is the event mapped onto the External Push
// lifecycle shape (an acr event is mapped by its caller); Domain says whose
// credential it is.
type Input struct {
	Domain Domain
	Event  contracts.ExternalPushLifecycleEvent
	// AcrClass is the registry credential class of an acr credential
	// (acr_client_credential or acr_browser_refresh_token); empty for push.
	AcrClass string
	// OwnerSubject is the acr credential's owner subject, kept as projected data.
	OwnerSubject string
	// RevokeReason is the reason a revoked event carries, if it carries one.
	RevokeReason string
}

var (
	referencePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	sourcePattern    = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	acrClasses       = map[string]bool{"acr_client_credential": true, "acr_browser_refresh_token": true}
)

// ErrMalformed names why an input cannot be keyed.
var ErrMalformed = errors.New("outboxprojection: malformed input")

// Check refuses an input the store cannot key. An event with no source_id is
// malformed until organization-wide credentials exist (CHAOS-12358).
func (in Input) Check() error {
	e := in.Event
	switch {
	case in.Domain != DomainExternalPush && in.Domain != DomainAcr:
		return fmt.Errorf("%w: unknown domain", ErrMalformed)
	case in.Domain == DomainAcr && !acrClasses[in.AcrClass]:
		return fmt.Errorf("%w: an acr credential needs one of the two acr classes", ErrMalformed)
	case in.Domain == DomainExternalPush && (in.AcrClass != "" || in.OwnerSubject != ""):
		return fmt.Errorf("%w: an acr class on a push credential", ErrMalformed)
	case !referencePattern.MatchString(e.AggregateID):
		return fmt.Errorf("%w: aggregate id", ErrMalformed)
	case e.Payload.Revision < 1:
		return fmt.Errorf("%w: revision", ErrMalformed)
	case !sourcePattern.MatchString(e.Payload.SourceID):
		return fmt.Errorf("%w: source id", ErrMalformed)
	}
	if _, err := ParseOrganization(e.Payload.OrganizationID); err != nil {
		return err
	}
	return nil
}

// ParseOrganization reads the wire organization id (org_ and a UUID).
func ParseOrganization(wire string) (uuid.UUID, error) {
	id, err := uuid.Parse(strings.TrimPrefix(wire, "org_"))
	if err != nil || !strings.HasPrefix(wire, "org_") || id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: organization id", ErrMalformed)
	}
	return id, nil
}

// Digest is the SHA-256 of the whole event body, under a version prefix: a
// same-revision delivery with another body has another digest.
func Digest(e contracts.ExternalPushLifecycleEvent) [32]byte {
	raw, err := json.Marshal(e)
	if err != nil {
		raw = []byte(fmt.Sprintf("unmarshalable:%v", err))
	}
	return sha256.Sum256(append([]byte("credential-projection/v1\n"), raw...))
}

// Verdict compares a stored digest with an incoming one.
type Verdict int

const (
	VerdictNew Verdict = iota
	VerdictSame
	VerdictDifferent
)

// Compare returns VerdictNew for no stored digest. A stored value of the wrong
// length is Different, never Same.
func Compare(stored []byte, incoming [32]byte) Verdict {
	switch {
	case stored == nil:
		return VerdictNew
	case bytes.Equal(stored, incoming[:]):
		return VerdictSame
	default:
		return VerdictDifferent
	}
}

// MapRevokeReason maps a revoke reason an event carries into the registry's
// closed list; an event that names none, or a value outside the list, is
// domain_revoked.
func MapRevokeReason(reason string) string {
	switch reason {
	case "compromised", "owner_request":
		return reason
	default:
		return "domain_revoked"
	}
}

const rfc3339Micro = "2006-01-02T15:04:05.000000Z"

// Narrow applies what the registry row holds to a projected state: a revoked
// registry row reads as a revoked credential at its time, and an earlier
// registry expiry wins. It never widens: the restore fence narrows the
// registry, and a projection that lags must not read a credential live that
// the registry no longer holds live.
func Narrow(state pushlifecycle.State, revokedAt, expiresAt *time.Time) pushlifecycle.State {
	if state.Status.Terminal() {
		return state
	}
	if revokedAt != nil && state.Status == pushlifecycle.StatusActive {
		state.Status = pushlifecycle.StatusRevoked
		state.TerminalAt = revokedAt.UTC()
		return state
	}
	if expiresAt != nil {
		registry := expiresAt.UTC()
		stored, err := time.Parse(time.RFC3339Nano, state.ExpiresAt)
		if state.ExpiresAt == "" || err != nil || registry.Before(stored) {
			state.ExpiresAt = registry.Format(rfc3339Micro)
		}
	}
	return state
}
