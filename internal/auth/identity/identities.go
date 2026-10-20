package identity

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

var (
	// ErrIdentityConflict reports a link of a key another principal holds.
	ErrIdentityConflict = errors.New("identity: the external identity is linked to another principal")
	// ErrAlreadyLinked reports a link of a key the principal already holds.
	ErrAlreadyLinked = errors.New("identity: the external identity is already linked to this principal")
	// ErrNotFound reports an unlink of a key that is not linked.
	ErrNotFound = errors.New("identity: external identity not linked")
	// ErrNotOwner reports an unlink of a key another principal holds.
	ErrNotOwner = errors.New("identity: the external identity belongs to another principal")
	// ErrLastSignInMethod reports an unlink that would leave the principal
	// with no external identity and no password.
	ErrLastSignInMethod = errors.New("identity: the external identity is the last sign-in method")
	// ErrProviderAlreadyLinked reports a link of a key whose issuer and
	// provider the principal already holds with another subject: one identity
	// per provider per principal.
	ErrProviderAlreadyLinked = errors.New("identity: the principal already has an identity at this provider")
	// ErrPrincipalUnavailable reports a link to a principal that does not
	// exist or is disabled.
	ErrPrincipalUnavailable = errors.New("identity: principal missing or disabled")
	// ErrSignupNotAvailable reports a ProvisionFederated before the users
	// cutover (Config.UsersOwnedByAuth off): go-api still owns user creation.
	ErrSignupNotAvailable = errors.New("identity: sign-up through auth-service is not available")
	// ErrIdentityTaken reports a provisioning whose key is already linked.
	ErrIdentityTaken = errors.New("identity: the external identity is already linked")
	// ErrEmailTaken reports a provisioning whose email another principal
	// holds. Nothing is linked to that principal.
	ErrEmailTaken = errors.New("identity: the email belongs to an existing principal")
	// ErrInvalidLegacyMatch reports a LegacyMatch that does not fit the key's
	// provider.
	ErrInvalidLegacyMatch = errors.New("identity: legacy match does not fit the provider")
)

const externalIdentityKeyIndex = "external_identities_issuer_provider_subject_key"

// errProjectedConcurrently ends a legacy projection whose identity row a
// concurrent call already wrote for the same principal; it is success.
var errProjectedConcurrently = errors.New("identity: projected concurrently")

// LegacyMatch lets IdentityBySubject find a user go-api provisioned before the
// users cutover, by the provider subject go-api stored in public.users
// (auth_provider, auth_provider_id). It carries no email.
//
// Social key: AuthProvider is the key's provider name and MembershipOrg is nil.
// SSO key: AuthProvider is the public.users.auth_provider value of that login
// (oidc, saml, or the OAuth provider type), and MembershipOrg is the org of the
// key's sso_providers row; the user must be a member of that org.
//
// Subject, when set, is the raw value matched against
// public.users.auth_provider_id, for a login whose key subject is not what
// go-api stored (SAML's composite subject). The identity is still linked under
// the key's subject. Empty means the key's subject.
type LegacyMatch struct {
	AuthProvider  string
	MembershipOrg *uuid.UUID
	Subject       string
}

// Identity is one linked external identity.
type Identity struct {
	ID        uuid.UUID
	Issuer    string
	Provider  Provider
	Subject   string
	CreatedAt time.Time
}

// IdentityBySubject returns the principal linked to (issuer, provider,
// subject). It matches the full key and nothing else. Before the users
// cutover, a miss with legacy set falls back to the provider subject go-api
// stored in public.users: exactly one matching user is projected and the key
// is linked to it under audit.Commit (reason identity_projected). found is
// false when nothing matches; a disabled principal is found with DisabledAt
// set.
func (s *Store) IdentityBySubject(ctx context.Context, issuer string, provider Provider, subject string, legacy *LegacyMatch) (Principal, bool, error) {
	key := ExternalKey{Issuer: issuer, Provider: provider, Subject: subject}
	if err := key.Validate(); err != nil {
		return Principal{}, false, err
	}
	p, found, err := s.principalByKey(ctx, key)
	if err != nil || found || legacy == nil || s.ownsUsers {
		return p, found, err
	}
	userID, found, err := s.legacyUser(ctx, key, *legacy)
	if err != nil || !found {
		return Principal{}, false, err
	}
	if err := s.Project(ctx, userID); err != nil {
		return Principal{}, false, err
	}
	if err := s.projectIdentity(ctx, key, userID); err != nil {
		return Principal{}, false, err
	}
	return s.principalByKey(ctx, key)
}

func (s *Store) principalByKey(ctx context.Context, key ExternalKey) (Principal, bool, error) {
	var p Principal
	err := s.pool.QueryRow(ctx, `SELECT p.id, p.disabled_at FROM `+s.q+`.external_identities e
JOIN `+s.q+`.principals p ON p.id = e.principal_id
WHERE e.issuer = $1 AND e.provider = $2 AND e.subject = $3`, key.Issuer, string(key.Provider), key.Subject).Scan(&p.ID, &p.DisabledAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, false, nil
	}
	if err != nil {
		return Principal{}, false, s.storeErr(ctx, "read identity by subject", err)
	}
	p.IsActive = p.DisabledAt == nil
	return p, true, nil
}

// legacyUser finds the one public.users row go-api provisioned for key.
func (s *Store) legacyUser(ctx context.Context, key ExternalKey, legacy LegacyMatch) (uuid.UUID, bool, error) {
	var rows pgx.Rows
	var err error
	stored := key.Subject
	if legacy.Subject != "" {
		stored = legacy.Subject
	}
	switch {
	case key.Provider.IsSocial():
		if legacy.AuthProvider != string(key.Provider) || legacy.MembershipOrg != nil {
			return uuid.Nil, false, ErrInvalidLegacyMatch
		}
		rows, err = s.pool.Query(ctx, `SELECT id FROM public.users WHERE auth_provider = $1 AND auth_provider_id = $2`,
			legacy.AuthProvider, stored)
	default:
		if legacy.AuthProvider == "" || legacy.MembershipOrg == nil {
			return uuid.Nil, false, ErrInvalidLegacyMatch
		}
		rows, err = s.pool.Query(ctx, `SELECT u.id FROM public.users u
WHERE u.auth_provider = $1 AND u.auth_provider_id = $2
  AND EXISTS (SELECT 1 FROM public.sso_providers sp WHERE sp.id = $3::uuid AND sp.org_id = $4)
  AND EXISTS (SELECT 1 FROM public.memberships m WHERE m.user_id = u.id AND m.org_id = $4)`,
			legacy.AuthProvider, stored, string(key.Provider), *legacy.MembershipOrg)
	}
	if err != nil {
		return uuid.Nil, false, s.storeErr(ctx, "read legacy user", err)
	}
	defer rows.Close()
	var found []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return uuid.Nil, false, s.storeErr(ctx, "scan legacy user", err)
		}
		found = append(found, id)
	}
	if err := rows.Err(); err != nil {
		return uuid.Nil, false, s.storeErr(ctx, "read legacy user", err)
	}
	switch len(found) {
	case 0:
		return uuid.Nil, false, nil
	case 1:
		return found[0], true, nil
	}
	return uuid.Nil, false, ErrMultipleUsers
}

// projectIdentity links key to the projected legacy user.
func (s *Store) projectIdentity(ctx context.Context, key ExternalKey, principal uuid.UUID) (err error) {
	defer func() { s.countLink(ctx, "project", err) }()
	linkID := uuid.New()
	target, subject := linkID.String(), principal.String()
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			return s.insertIdentity(ctx, tx, linkID, key, principal, errProjectedConcurrently)
		},
		Audit:  identityAudit("auth.identity.projected", "identity_projected", key, target, subject, Actor{}),
		Events: []audit.OutboxEvent{identityEvent("external_identity.linked", key, target, subject)},
	})
	if errors.Is(err, errProjectedConcurrently) {
		return nil
	}
	if err != nil {
		return s.domainErr(ctx, "project external identity", err)
	}
	s.logger.InfoContext(ctx, "identity: legacy external identity projected",
		"principal_id", subject, "external_identity_id", target, "provider", string(key.Provider))
	return nil
}

// insertIdentity inserts the key for principal. A key the principal already
// holds returns same; a key another principal holds is ErrIdentityConflict.
func (s *Store) insertIdentity(ctx context.Context, tx audit.TxOps, id uuid.UUID, key ExternalKey, principal uuid.UUID, same error) error {
	var inserted uuid.UUID
	err := tx.QueryRow(ctx, `INSERT INTO `+s.q+`.external_identities (id, principal_id, issuer, provider, subject)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (issuer, provider, subject) DO NOTHING
RETURNING id`, id, principal, key.Issuer, string(key.Provider), key.Subject).Scan(&inserted)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	var holder uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT principal_id FROM `+s.q+`.external_identities
WHERE issuer = $1 AND provider = $2 AND subject = $3`, key.Issuer, string(key.Provider), key.Subject).Scan(&holder); err != nil {
		return err
	}
	if holder == principal {
		return same
	}
	return ErrIdentityConflict
}

// ProvisionRequest is a new federated user. A just-in-time sign-up has no
// signed-in actor: pass Actor: SelfSignUp(requestID), and the audit row names
// the new principal as its own actor.
type ProvisionRequest struct {
	Key      ExternalKey
	Email    string
	FullName string
	Actor    Actor
}

// ProvisionFederated creates a user principal, its auth.users row and its
// external identity in one audit.Commit (reason principal_provisioned). It
// writes the auth schema only and is available only after the users cutover
// (ErrSignupNotAvailable before). A key already linked is ErrIdentityTaken; an
// email another principal holds is ErrEmailTaken, and nothing is linked to that
// principal.
func (s *Store) ProvisionFederated(ctx context.Context, req ProvisionRequest) (_ uuid.UUID, err error) {
	defer func() { s.countLink(ctx, "provision", err) }()
	if !s.ownsUsers {
		return uuid.Nil, ErrSignupNotAvailable
	}
	if err := req.Key.Validate(); err != nil {
		return uuid.Nil, err
	}
	emailLower, err := NormalizeEmail(req.Email)
	if err != nil {
		return uuid.Nil, err
	}
	principal, linkID := uuid.New(), uuid.New()
	subject, target := principal.String(), linkID.String()
	auditEvent := identityAudit("auth.principal.provisioned", "principal_provisioned", req.Key, target, subject, req.Actor)
	auditEvent.Attributes["target_kind"], auditEvent.Attributes["target_id"] = "principal", subject
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.principals (id, kind, display_name) VALUES ($1, 'user', $2)`,
				principal, req.FullName); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.users (principal_id, email, email_lower, email_verified_at)
VALUES ($1, $2, $3, clock_timestamp())`, principal, req.Email, emailLower); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.external_identities (id, principal_id, issuer, provider, subject)
VALUES ($1, $2, $3, $4, $5)`, linkID, principal, req.Key.Issuer, string(req.Key.Provider), req.Key.Subject)
			return err
		},
		Audit: auditEvent,
		Events: []audit.OutboxEvent{
			principalEvent(subject, "principal.created", true), userEvent(subject, "user.created"),
			userEvent(subject, "user.email_verified"), identityEvent("external_identity.linked", req.Key, target, subject),
		},
	})
	switch {
	case err == nil:
	case isEmailCollision(err):
		return uuid.Nil, ErrEmailTaken
	case isUniqueViolation(err, externalIdentityKeyIndex):
		return uuid.Nil, ErrIdentityTaken
	default:
		return uuid.Nil, s.storeErr(ctx, "provision federated user", err)
	}
	s.logger.InfoContext(ctx, "identity: federated principal provisioned",
		"principal_id", subject, "external_identity_id", target, "provider", string(req.Key.Provider))
	return principal, nil
}

// Actor names who asked for a change, for the audit row. The zero Actor
// records no actor.
type Actor struct {
	PrincipalID *uuid.UUID
	RequestID   *string
	self        bool
}

// SelfSignUp is the actor of a self-service (just-in-time) sign-up or link,
// where no one is signed in: the audit row names the subject principal (for
// ProvisionFederated, the principal it creates) as its own actor. That id is a
// UUID of an existing principal, as audit actor_principal_id requires.
func SelfSignUp(requestID *string) Actor { return Actor{RequestID: requestID, self: true} }

// LinkIdentity links key to principal, with its audit row and outbox event in
// one transaction. A key another principal holds is ErrIdentityConflict and is
// never re-pointed; a key the principal already holds is ErrAlreadyLinked; a
// key whose issuer and provider the principal holds with another subject is
// ErrProviderAlreadyLinked. The principal row is locked for the transaction,
// so concurrent links of one principal are checked one after the other. It
// returns the new external identity id.
func (s *Store) LinkIdentity(ctx context.Context, key ExternalKey, principal uuid.UUID, actor Actor) (_ uuid.UUID, err error) {
	defer func() { s.countLink(ctx, "link", err) }()
	if err := key.Validate(); err != nil {
		return uuid.Nil, err
	}
	linkID := uuid.New()
	target, subject := linkID.String(), principal.String()
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if err := s.requireLivePrincipal(ctx, tx, principal); err != nil {
				return err
			}
			if err := s.oneIdentityPerProvider(ctx, tx, key, principal); err != nil {
				return err
			}
			return s.insertIdentity(ctx, tx, linkID, key, principal, ErrAlreadyLinked)
		},
		Audit:  identityAudit("auth.identity.linked", "identity_linked", key, target, subject, actor),
		Events: []audit.OutboxEvent{identityEvent("external_identity.linked", key, target, subject)},
	})
	if err != nil {
		return uuid.Nil, s.domainErr(ctx, "link external identity", err)
	}
	s.logger.InfoContext(ctx, "identity: external identity linked",
		"principal_id", subject, "external_identity_id", target, "provider", string(key.Provider))
	return linkID, nil
}

// UnlinkIdentity removes the link of key from principal, audited. A key that
// is not linked is ErrNotFound; a key another principal holds is ErrNotOwner
// and is left alone; the principal's last external identity, when it has no
// password, is ErrLastSignInMethod.
func (s *Store) UnlinkIdentity(ctx context.Context, key ExternalKey, principal uuid.UUID, actor Actor) (err error) {
	defer func() { s.countLink(ctx, "unlink", err) }()
	if err := key.Validate(); err != nil {
		return err
	}
	var target, holder uuid.UUID
	err = s.pool.QueryRow(ctx, `SELECT id, principal_id FROM `+s.q+`.external_identities
WHERE issuer = $1 AND provider = $2 AND subject = $3`, key.Issuer, string(key.Provider), key.Subject).Scan(&target, &holder)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return s.storeErr(ctx, "read external identity", err)
	}
	if holder != principal {
		return ErrNotOwner
	}
	targetID, subject := target.String(), principal.String()
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			// The principal row lock serializes every unlink of one
			// principal, so two unlinks cannot each see the other's identity
			// as the remaining sign-in method.
			var locked uuid.UUID
			if err := tx.QueryRow(ctx, `SELECT id FROM `+s.q+`.principals WHERE id = $1 FOR UPDATE`, principal).Scan(&locked); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return err
			}
			var remaining int
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM `+s.q+`.external_identities WHERE principal_id = $1 AND id <> $2`,
				principal, target).Scan(&remaining); err != nil {
				return err
			}
			if remaining == 0 {
				hasPassword, err := s.hasPassword(ctx, tx.QueryRow, principal)
				if err != nil {
					return err
				}
				if !hasPassword {
					return ErrLastSignInMethod
				}
			}
			tag, err := tx.Exec(ctx, `DELETE FROM `+s.q+`.external_identities WHERE id = $1 AND principal_id = $2`, target, principal)
			if err == nil && tag.RowsAffected() == 0 {
				return ErrNotFound
			}
			return err
		},
		Audit:  identityAudit("auth.identity.unlinked", "identity_unlinked", key, targetID, subject, actor),
		Events: []audit.OutboxEvent{identityEvent("external_identity.unlinked", key, targetID, subject)},
	})
	if err != nil {
		return s.domainErr(ctx, "unlink external identity", err)
	}
	s.logger.InfoContext(ctx, "identity: external identity unlinked",
		"principal_id", subject, "external_identity_id", targetID, "provider", string(key.Provider))
	return nil
}

// ListIdentities returns the principal's external identities, oldest first.
func (s *Store) ListIdentities(ctx context.Context, principal uuid.UUID) ([]Identity, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, issuer, provider, subject, created_at FROM `+s.q+`.external_identities
WHERE principal_id = $1 ORDER BY created_at, id`, principal)
	if err != nil {
		return nil, s.storeErr(ctx, "list external identities", err)
	}
	defer rows.Close()
	var out []Identity
	for rows.Next() {
		var i Identity
		var provider string
		if err := rows.Scan(&i.ID, &i.Issuer, &provider, &i.Subject, &i.CreatedAt); err != nil {
			return nil, s.storeErr(ctx, "scan external identity", err)
		}
		i.Provider = Provider(provider)
		out = append(out, i)
	}
	if err := rows.Err(); err != nil {
		return nil, s.storeErr(ctx, "list external identities", err)
	}
	return out, nil
}

// HasPassword reports whether the principal has a local password: read live
// from public.users before the users cutover, from auth.users after it.
func (s *Store) HasPassword(ctx context.Context, principal uuid.UUID) (bool, error) {
	has, err := s.hasPassword(ctx, s.pool.QueryRow, principal)
	if err != nil {
		return false, s.storeErr(ctx, "read password presence", err)
	}
	return has, nil
}

func (s *Store) hasPassword(ctx context.Context, queryRow func(context.Context, string, ...any) pgx.Row, principal uuid.UUID) (bool, error) {
	sql := `SELECT password_hash IS NOT NULL FROM public.users WHERE id = $1`
	if s.ownsUsers {
		sql = `SELECT password_hash IS NOT NULL FROM ` + s.q + `.users WHERE principal_id = $1`
	}
	var has bool
	err := queryRow(ctx, sql, principal).Scan(&has)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return has, err
}

// oneIdentityPerProvider locks the principal row and refuses a second subject
// at the key's issuer and provider.
func (s *Store) oneIdentityPerProvider(ctx context.Context, tx audit.TxOps, key ExternalKey, principal uuid.UUID) error {
	var locked uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM `+s.q+`.principals WHERE id = $1 FOR UPDATE`, principal).Scan(&locked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrPrincipalUnavailable
		}
		return err
	}
	var others int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM `+s.q+`.external_identities
WHERE principal_id = $1 AND issuer = $2 AND provider = $3 AND subject <> $4`,
		principal, key.Issuer, string(key.Provider), key.Subject).Scan(&others); err != nil {
		return err
	}
	if others > 0 {
		return ErrProviderAlreadyLinked
	}
	return nil
}

func (s *Store) requireLivePrincipal(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error {
	var disabled bool
	err := tx.QueryRow(ctx, `SELECT disabled_at IS NOT NULL FROM `+s.q+`.principals WHERE id = $1 FOR SHARE`, principal).Scan(&disabled)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && disabled) {
		return ErrPrincipalUnavailable
	}
	return err
}

// identityAudit is the audit row of an identity change. The provider
// attribute is a closed vocabulary, so only a social provider name goes there;
// an SSO provider row id is carried by the outbox event instead.
func identityAudit(eventType, reason string, key ExternalKey, target, subject string, actor Actor) audit.AuditEvent {
	attributes := map[string]any{"reason_code": reason, "target_kind": "external_identity", "target_id": target}
	if key.Provider.IsSocial() {
		attributes["provider"] = string(key.Provider)
	}
	event := audit.AuditEvent{
		EventType: eventType, Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject, RequestID: actor.RequestID,
		Attributes: attributes,
	}
	switch {
	case actor.self:
		a := subject
		event.ActorPrincipalID = &a
	case actor.PrincipalID != nil:
		a := actor.PrincipalID.String()
		event.ActorPrincipalID = &a
	}
	return event
}

// identityEvent is an external identity event. It names the principal the
// identity belongs to (subject), never the actor. The wire carries a
// provider's short name only: a social provider's name; an SSO provider is a
// provider row id, which the provider member cannot carry, so it is omitted.
func identityEvent(eventType string, key ExternalKey, target, subject string) audit.OutboxEvent {
	change := map[string]any{"principal_id": provisioning.WireID("principal", subject)}
	if key.Provider.IsSocial() {
		change["provider"] = string(key.Provider)
	}
	return audit.OutboxEvent{
		AggregateType: "external_identity", AggregateID: provisioning.WireID("external_identity", target),
		EventType: eventType, Change: change,
	}
}

func (s *Store) countLink(ctx context.Context, action string, err error) {
	outcome := "ok"
	switch {
	case err == nil:
	case errors.Is(err, ErrIdentityConflict):
		outcome = "conflict"
	case errors.Is(err, ErrAlreadyLinked):
		outcome = "already_linked"
	case errors.Is(err, ErrProviderAlreadyLinked):
		outcome = "provider_already_linked"
	case errors.Is(err, ErrNotFound):
		outcome = "not_found"
	case errors.Is(err, ErrNotOwner):
		outcome = "not_owner"
	case errors.Is(err, ErrLastSignInMethod):
		outcome = "last_sign_in_method"
	case errors.Is(err, ErrPrincipalUnavailable):
		outcome = "principal_unavailable"
	case errors.Is(err, ErrSignupNotAvailable):
		outcome = "signup_not_available"
	case errors.Is(err, ErrIdentityTaken):
		outcome = "identity_taken"
	case errors.Is(err, ErrEmailTaken):
		outcome = "email_taken"
	case errors.Is(err, ErrUserNotFound):
		outcome = "user_not_found"
	case errors.Is(err, ErrInvalidExternalKey), errors.Is(err, ErrInvalidEmail), errors.Is(err, ErrInvalidPasswordHash):
		outcome = "invalid_input"
	default:
		outcome = "error"
	}
	s.links.Add(ctx, 1, metric.WithAttributes(attribute.String("action", action), attribute.String("outcome", outcome)))
}

// domainErr keeps the domain refusals Apply returned (audit.Commit wraps them
// with %w) and wraps everything else as ErrStore.
func (s *Store) domainErr(ctx context.Context, op string, err error) error {
	for _, domain := range []error{ErrIdentityConflict, ErrAlreadyLinked, ErrProviderAlreadyLinked, ErrPrincipalUnavailable,
		ErrNotFound, ErrNotOwner, ErrLastSignInMethod} {
		if errors.Is(err, domain) {
			return domain
		}
	}
	return s.storeErr(ctx, op, err)
}

func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == constraint
}
