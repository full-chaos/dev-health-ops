// Package repoidentityevent is the one shape of a repository_identity.observed event: the producer (provider sync)
// checks a row with it before it appends, and the auth outbox writer (audit) refuses with it. It is a leaf with no
// logging or database dependency, so the provider sync's dependency closure stays free of both.
package repoidentityevent

import (
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// The shapes of migration 0055 (repository_identities) and of the append function.
var (
	providerShape = regexp.MustCompile(`^[a-z]{1,16}$`)
	idShape       = regexp.MustCompile(`^[0-9]{1,20}$`)
	pathShape     = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]*$`)
)

// Event is one repository a provider sync observed: its organization, the provider and the provider's stable id, the
// live path (a canonical repository key) and the instant the sync fetched it.
type Event struct {
	OrganizationID uuid.UUID
	Provider       string
	ProviderRepoID string
	Path           string
	ObservedAt     time.Time
}

// AggregateID is the outbox aggregate id of the repository: "<organization>/<provider>/<provider id>".
func (e Event) AggregateID() string {
	return e.OrganizationID.String() + "/" + e.Provider + "/" + e.ProviderRepoID
}

// Validate reports why the append function would refuse the event, or nil.
func (e Event) Validate() error {
	switch {
	case e.OrganizationID == uuid.Nil:
		return errors.New("organization id is required")
	case !providerShape.MatchString(e.Provider):
		return errors.New("provider is not a provider name")
	case !idShape.MatchString(e.ProviderRepoID):
		return errors.New("provider repository id is not a number")
	case len(e.Path) > 255 || !pathShape.MatchString(e.Path):
		return errors.New("path is not a canonical repository key")
	case e.ObservedAt.IsZero():
		return errors.New("observed_at is required")
	}
	return nil
}
