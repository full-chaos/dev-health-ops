package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

var (
	// ErrOrganizationMember refuses an onboarding of a principal that holds a
	// membership.
	ErrOrganizationMember = errors.New("identity: the principal already has a membership")
	// ErrAccountInactive refuses an onboarding of a disabled account.
	ErrAccountInactive = errors.New("identity: the account is not active")
	// ErrOrganizationDenied is the refusal of CreateOrganizationRequest.Authorize.
	ErrOrganizationDenied = errors.New("identity: the actor may not create an organization")
)

// OnboardOrganizationRequest creates Principal's first organization.
type OnboardOrganizationRequest struct {
	Principal       uuid.UUID
	Name            string
	RequestID       *string
	Caller          audit.Caller
	Description     *string
	RequestMetadata []byte
}

// CreateOrganizationRequest creates an organization for a platform actor.
// Owner nil creates it with no member. Authorize decides under the locks.
type CreateOrganizationRequest struct {
	Actor       uuid.UUID
	RequestID   *string
	Caller      audit.Caller
	Slug, Name  string
	Description *string
	Settings    []byte
	Tier        string
	ManagedBy   string
	Owner       *uuid.UUID
	Authorize   func(ctx context.Context, tx audit.TxOps) error
}

// BornOrganization is an organization a verb created.
type BornOrganization struct {
	ID           uuid.UUID
	MembershipID *uuid.UUID
	Slug         string
}

// OnboardOrganization is not built.
func (s *Store) OnboardOrganization(context.Context, OnboardOrganizationRequest) (*BornOrganization, error) {
	return nil, errors.ErrUnsupported
}

// CreateOrganization is not built.
func (s *Store) CreateOrganization(context.Context, CreateOrganizationRequest) (*BornOrganization, error) {
	return nil, errors.ErrUnsupported
}

type organizationBirth struct {
	ID, MembershipID uuid.UUID
	Slug, Name       string
	Description      *string
	Settings         []byte
	Tier, ManagedBy  string
	Owner            *uuid.UUID
}

func newOrganizationBirth(slug, name string, owner *uuid.UUID) organizationBirth {
	return organizationBirth{ID: uuid.New(), MembershipID: uuid.New(), Slug: slug, Name: name, Settings: []byte("{}"),
		Tier: "community", ManagedBy: "stripe", Owner: owner}
}

func (s *Store) insertOrganizationBirth(context.Context, audit.TxOps, organizationBirth) error {
	return errors.ErrUnsupported
}
