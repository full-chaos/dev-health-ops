package saml

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// PGStore is the PostgreSQL FlowStore, AssertionUses, ProviderReader and
// Memberships. Auth tables live in Schema; go-api tables are read from the
// public schema with read-only grants.
type PGStore struct {
	pool *pgxpool.Pool
	q    string
	// usersOwned is the users-cutover switch: with it, a membership in the
	// auth schema counts as well as one in public.memberships.
	usersOwned bool
}

// NewPGStore binds the store to pool and the auth schema.
func NewPGStore(pool *pgxpool.Pool, schema authschema.ValidatedIdentifier) (*PGStore, error) {
	if pool == nil || schema.String() == "" {
		return nil, errors.New("saml: PGStore needs a pool and a schema")
	}
	return &PGStore{pool: pool, q: authschema.Quote(schema)}, nil
}

// WithUsersOwned returns the store reading memberships from both schemas
// (the users-cutover mode); without it only public.memberships is read.
func (s *PGStore) WithUsersOwned(owned bool) *PGStore {
	copied := *s
	copied.usersOwned = owned
	return &copied
}

func hashOf(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

const saveFlowSQL = `INSERT INTO %s.sso_flow_states (protocol, provider_id, flow_hash, binding_hash, expires_at)
VALUES ('saml', $1, $2, $3, $4)`

// SaveFlow stores the request id and browser binding as hashes.
func (s *PGStore) SaveFlow(ctx context.Context, f Flow) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(saveFlowSQL, s.q), f.ProviderID, hashOf(f.RequestID), f.BindingHash[:], f.ExpiresAt.UTC())
	return err
}

const consumeFlowSQL = `UPDATE %s.sso_flow_states SET consumed_at = $5
WHERE protocol = 'saml' AND flow_hash = $1 AND provider_id = $2 AND binding_hash = $3
  AND consumed_at IS NULL AND expires_at > $4`

// ConsumeFlow consumes the flow once; a second call affects no row.
func (s *PGStore) ConsumeFlow(ctx context.Context, providerID uuid.UUID, requestID string, bindingHash [32]byte, now time.Time) error {
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(consumeFlowSQL, s.q), hashOf(requestID), providerID, bindingHash[:], now.UTC(), now.UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var consumed bool
	err = s.pool.QueryRow(ctx, fmt.Sprintf(consumedFlowSQL, s.q), hashOf(requestID), providerID, bindingHash[:]).Scan(&consumed)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !consumed) {
		return ErrFlowNotFound
	}
	if err != nil {
		return err
	}
	return ErrFlowConsumed
}

const consumedFlowSQL = `SELECT consumed_at IS NOT NULL FROM %s.sso_flow_states
WHERE protocol = 'saml' AND flow_hash = $1 AND provider_id = $2 AND binding_hash = $3`

const legacyReplaySQL = `SELECT EXISTS (SELECT 1 FROM public.saml_assertion_replays WHERE provider_id = $1 AND assertion_id = $2)`

const useAssertionSQL = `INSERT INTO %s.saml_assertion_uses (issuer, assertion_id_hash, expires_at)
VALUES ($1, $2, $3) ON CONFLICT (issuer, assertion_id_hash) DO NOTHING`

// UseAssertion records the assertion id until expiresAt plus the clock skew.
// An id already in the go-api replay table is refused too.
func (s *PGStore) UseAssertion(ctx context.Context, providerID uuid.UUID, issuer, assertionID string, expiresAt time.Time) error {
	if assertionID == "" {
		return ErrAssertionReplayed
	}
	var usedByGoAPI bool
	if err := s.pool.QueryRow(ctx, legacyReplaySQL, providerID, assertionID).Scan(&usedByGoAPI); err != nil {
		return err
	}
	if usedByGoAPI {
		return ErrAssertionReplayed
	}
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(useAssertionSQL, s.q), issuer, hashOf(assertionID), expiresAt.Add(MaxClockSkew).UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrAssertionReplayed
	}
	return nil
}

const providerSQL = `SELECT id, org_id, protocol, status, allow_idp_initiated, auto_provision_users,
  config::text, allowed_domains::text
FROM public.sso_providers WHERE id = $1`

// Provider reads one sso_providers row.
func (s *PGStore) Provider(ctx context.Context, id uuid.UUID) (Provider, error) {
	var (
		p              Provider
		config         string
		allowedDomains *string
	)
	err := s.pool.QueryRow(ctx, providerSQL, id).Scan(&p.ID, &p.OrgID, &p.Protocol, &p.Status,
		&p.AllowIdPInitiated, &p.AutoProvision, &config, &allowedDomains)
	if errors.Is(err, pgx.ErrNoRows) {
		return Provider{}, ErrProviderNotFound
	}
	if err != nil {
		return Provider{}, err
	}
	if p.Config, err = ParseConfig([]byte(config)); err != nil {
		return Provider{}, err
	}
	if err := ValidateACSOverride(p.Config, p.ID); err != nil {
		return Provider{}, err
	}
	if allowedDomains != nil {
		if p.AllowedDomains, err = ParseAllowedDomains([]byte(*allowedDomains)); err != nil {
			return Provider{}, err
		}
	}
	return p, nil
}

const memberSQL = `SELECT EXISTS (SELECT 1 FROM public.memberships WHERE user_id = $1 AND org_id = $2)`

// Member reports whether the principal is a member of the organization.
func (s *PGStore) Member(ctx context.Context, principalID, orgID uuid.UUID) (bool, error) {
	var ok bool
	if err := s.pool.QueryRow(ctx, memberSQL, principalID, orgID).Scan(&ok); err != nil || ok || !s.usersOwned {
		return ok, err
	}
	err := s.pool.QueryRow(ctx, fmt.Sprintf(authMemberSQL, s.q), principalID, orgID).Scan(&ok)
	return ok, err
}

const authMemberSQL = `SELECT EXISTS (SELECT 1 FROM %s.memberships WHERE principal_id = $1 AND organization_id = $2 AND status = 'active')`
