// Package oauthgrant is the durable state of the OAuth authorization server
// (ACP-ADR-13, CHAOS-12398): one authorization request and its single-use code,
// and the grant an approval records. It has no route. The rows are read and
// written only through this store.
//
// A grant has its own session row (session.InsertClientSession). A browser
// client's session is linked to the web session and ends with it; an MCP client's
// session is not. Approve commits the decision, the session, the grant, the code
// and their audit rows in one transaction.
//
// Only hashes of the code and of the anti-CSRF token are stored. The code, the
// token and the client's state are never logged and never audited.
package oauthgrant

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	// RequestLifetime is how long an authorization request can be decided.
	RequestLifetime = 10 * time.Minute
	// CodeLifetime is an authorization code's life. An approval near the end of its
	// request gives a code that outlives the request, so the cleanup reads both.
	CodeLifetime = 60 * time.Second

	// sweepBatch is the most expired rows one new request deletes. auth-service has
	// no background loop, so the table is bounded by traffic: it cannot grow idle.
	sweepBatch = 100

	// ClassBrowser and ClassMCP are the client classes. A class comes from the
	// trusted client registry, never from client-supplied metadata (ADR-13
	// decision 7).
	ClassBrowser = "browser"
	ClassMCP     = "mcp"

	bindingWebSession    = "web_session"
	bindingClientSession = "client_session"

	decisionApproved = "approved"
	decisionDenied   = "denied"

	codePrefix  = "fcoac_"
	csrfPrefix  = "fcocs_"
	secretBytes = 32

	// The audit event types. No row carries a code, a token or the client's state.
	EventConsentApproved     = "oauth.consent.approved"
	EventConsentDenied       = "oauth.consent.denied"
	EventClientSessionCreate = "oauth.client_session.created"
)

var (
	// ErrStore is a database failure: the caller answers with a dependency error.
	ErrStore = errors.New("oauthgrant: store failure")
	// ErrActorUnavailable is an actor read that failed: fail-closed.
	ErrActorUnavailable = errors.New("oauthgrant: the actor state could not be read")

	// The refusals of a request.
	ErrRequestNotFound = errors.New("oauthgrant: no such authorization request")
	ErrRequestExpired  = errors.New("oauthgrant: the authorization request has expired")
	ErrRequestBound    = errors.New("oauthgrant: the authorization request is already bound")
	ErrRequestNotBound = errors.New("oauthgrant: the authorization request is not bound to a session")
	ErrOtherSession    = errors.New("oauthgrant: the authorization request belongs to another session")
	ErrBadCSRF         = errors.New("oauthgrant: the anti-CSRF token does not match")
	ErrAlreadyDecided  = errors.New("oauthgrant: the authorization request was already decided")
	ErrNoOrganization  = errors.New("oauthgrant: the web session has no organization")
	ErrActorRefused    = errors.New("oauthgrant: the principal is on the actor side of an impersonation")
	// ErrCodeRefused is every refusal of a code, with no reason: unknown, spent,
	// expired, or bound to another client, redirect, resource or challenge.
	ErrCodeRefused = errors.New("oauthgrant: the authorization code is refused")
	// ErrInvalidRequest is an input the schema would refuse: a caller bug.
	ErrInvalidRequest = errors.New("oauthgrant: invalid authorization request")
)

// refusals are the expected ends of a decision: audit.Commit rolls back and logs
// them at WARN.
var refusals = []error{
	ErrRequestNotFound, ErrRequestExpired, ErrRequestNotBound, ErrOtherSession, ErrBadCSRF, ErrAlreadyDecided, ErrNoOrganization,
	session.ErrWebSessionNotLive, session.ErrWebSessionDelegated, session.ErrWebSessionNoOrganization,
}

// Actors is the one actor check (acp, CHAOS-12251 Q4).
type Actors interface {
	ImpersonatingActor(ctx context.Context, principal, session uuid.UUID) (bool, error)
}

// Config builds a Store. Every field but Logger is required.
type Config struct {
	Pool     *pgxpool.Pool
	Schema   authschema.ValidatedIdentifier
	Sessions *session.Store
	Actors   Actors
	Logger   *slog.Logger
}

// Store reads and writes the two tables.
type Store struct {
	pool     *pgxpool.Pool
	schema   authschema.ValidatedIdentifier
	q        string
	sessions *session.Store
	actors   Actors
	logger   *slog.Logger
}

// New builds a Store.
func New(cfg Config) (*Store, error) {
	switch {
	case cfg.Pool == nil:
		return nil, errors.New("oauthgrant: a pool is required")
	case cfg.Schema == (authschema.ValidatedIdentifier{}):
		return nil, errors.New("oauthgrant: a schema is required")
	case cfg.Sessions == nil:
		return nil, errors.New("oauthgrant: a session store is required")
	case cfg.Actors == nil:
		return nil, errors.New("oauthgrant: an actor check is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Store{pool: cfg.Pool, schema: cfg.Schema, q: authschema.Quote(cfg.Schema), sessions: cfg.Sessions, actors: cfg.Actors, logger: logger}, nil
}

// RequestInput is a validated authorization request. The route has already checked
// it against the client registry.
type RequestInput struct {
	ClientID, ClientClass, RedirectURI, Resource, Audience, Scope string
	// State is the client's own value, echoed to it. It is never logged.
	State         string
	CodeChallenge string
}

// CreateRequest writes one authorization request. The same statement deletes at
// most sweepBatch expired rows: a row goes only when the request and its code have
// both expired, so a code issued near the end of its request survives until it can
// no longer be used.
func (s *Store) CreateRequest(ctx context.Context, in RequestInput) (uuid.UUID, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("%w: %w", ErrStore, err)
	}
	if in.ClientClass != ClassBrowser && in.ClientClass != ClassMCP {
		return uuid.UUID{}, fmt.Errorf("%w: client class", ErrInvalidRequest)
	}
	_, err = s.pool.Exec(ctx, `
		WITH swept AS (
			DELETE FROM `+s.q+`.oauth_authorization_requests
			WHERE id IN (
				SELECT id FROM `+s.q+`.oauth_authorization_requests
				WHERE expires_at < clock_timestamp()
				  AND (code_expires_at IS NULL OR code_expires_at < clock_timestamp())
				ORDER BY expires_at LIMIT $1
				FOR UPDATE SKIP LOCKED))
		INSERT INTO `+s.q+`.oauth_authorization_requests
			(id, client_id, client_class, redirect_uri, resource, audience, scope, state,
			 code_challenge, code_challenge_method, expires_at)
		VALUES ($2, $3, $4, $5, $6, $7, $8, $9, $10, 'S256',
			clock_timestamp() + make_interval(secs => $11::double precision))`,
		sweepBatch, id, in.ClientID, in.ClientClass, in.RedirectURI, in.Resource, in.Audience, in.Scope, in.State,
		in.CodeChallenge, RequestLifetime.Seconds())
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("%w: creating an authorization request: %w", ErrStore, err)
	}
	return id, nil
}

// DescribeInput binds a request to the web session of the logged-in user.
type DescribeInput struct {
	RequestID, WebSessionID, PrincipalID uuid.UUID
	// OrganizationID is nil for an org-less web session: Approve refuses that.
	OrganizationID *uuid.UUID
}

// Described is what the consent page shows, and the anti-CSRF token, returned once.
type Described struct {
	ClientID, ClientClass, Resource, Audience, Scope string
	CSRF                                             contracts.SecretString
}

// Describe binds the request to the session, once. The binding is write-once, so a
// second describe is refused, also by the same session: only the token's hash is
// stored, so it cannot be returned twice.
func (s *Store) Describe(ctx context.Context, in DescribeInput) (Described, error) {
	csrf, err := newSecret(csrfPrefix)
	if err != nil {
		return Described{}, fmt.Errorf("%w: %w", ErrStore, err)
	}
	out := Described{CSRF: csrf}
	err = s.pool.QueryRow(ctx, `
		UPDATE `+s.q+`.oauth_authorization_requests
		SET web_session_id = $2, principal_id = $3, organization_id = $4, csrf_hash = $5
		WHERE id = $1 AND web_session_id IS NULL AND expires_at > clock_timestamp()
		RETURNING client_id, client_class, resource, audience, scope`,
		in.RequestID, in.WebSessionID, in.PrincipalID, in.OrganizationID, hashSecret(csrf)).
		Scan(&out.ClientID, &out.ClientClass, &out.Resource, &out.Audience, &out.Scope)
	if err == nil {
		return out, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Described{}, fmt.Errorf("%w: binding an authorization request: %w", ErrStore, err)
	}
	var bound, expired bool
	err = s.pool.QueryRow(ctx, `
		SELECT web_session_id IS NOT NULL, expires_at <= clock_timestamp()
		FROM `+s.q+`.oauth_authorization_requests WHERE id = $1`, in.RequestID).Scan(&bound, &expired)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Described{}, ErrRequestNotFound
	case err != nil:
		return Described{}, fmt.Errorf("%w: reading an authorization request: %w", ErrStore, err)
	case bound:
		return Described{}, ErrRequestBound
	case expired:
		return Described{}, ErrRequestExpired
	}
	return Described{}, fmt.Errorf("%w: the binding matched no row and the row reads as bindable", ErrStore)
}

// Decision is a consent submit, bound to the request, the session and the token.
type Decision struct {
	RequestID, WebSessionID, PrincipalID uuid.UUID
	CSRF                                 contracts.SecretString
}

// Approved is the answer of an approval. Code is returned once.
type Approved struct {
	Code               contracts.SecretString
	RedirectURI, State string
	GrantID, SessionID uuid.UUID
}

// Denied is the answer of a denial, for the redirect.
type Denied struct {
	RedirectURI, State string
}

// row is one request as a decision reads it.
type row struct {
	clientID, class, redirectURI, resource, audience, scope, state string
	webSession, principal, organization                            *uuid.UUID
	csrfHash                                                       []byte
	decision                                                       *string
	expired                                                        bool
}

func (s *Store) read(ctx context.Context, db interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, id uuid.UUID, suffix string) (row, error) {
	var r row
	err := db.QueryRow(ctx, `
		SELECT client_id, client_class, redirect_uri, resource, audience, scope, state,
		       web_session_id, principal_id, organization_id, csrf_hash, decision, expires_at <= clock_timestamp()
		FROM `+s.q+`.oauth_authorization_requests WHERE id = $1`+suffix, id).
		Scan(&r.clientID, &r.class, &r.redirectURI, &r.resource, &r.audience, &r.scope, &r.state,
			&r.webSession, &r.principal, &r.organization, &r.csrfHash, &r.decision, &r.expired)
	if errors.Is(err, pgx.ErrNoRows) {
		return row{}, ErrRequestNotFound
	}
	return r, err
}

// check is the whole binding of a decision. A caller that does not hold the
// binding learns nothing of the request's state, so the state is read last.
func (r row) check(in Decision) error {
	switch {
	case r.webSession == nil:
		return ErrRequestNotBound
	case *r.webSession != in.WebSessionID, r.principal == nil, *r.principal != in.PrincipalID:
		return ErrOtherSession
	case subtle.ConstantTimeCompare(r.csrfHash, hashSecret(in.CSRF)) != 1:
		return ErrBadCSRF
	case r.decision != nil:
		return ErrAlreadyDecided
	case r.expired:
		return ErrRequestExpired
	}
	return nil
}

// Approve records the consent: the decision, the client's session, the grant, the
// code and the audit rows commit together or not at all. Approving twice gives one
// session and one grant: the second call is ErrAlreadyDecided, under the row lock.
// An org-less web session is refused for both classes, and so is a principal on
// the actor side of an impersonation, and a failed actor read.
func (s *Store) Approve(ctx context.Context, in Decision) (Approved, error) {
	// The request's fields are immutable once bound, so this read names the audit
	// rows; the checks are repeated under the lock.
	before, err := s.read(ctx, s.pool, in.RequestID, "")
	if err != nil {
		return Approved{}, s.storeError(err, "reading an authorization request")
	}
	if err := before.check(in); err != nil {
		return Approved{}, err
	}
	if before.organization == nil {
		return Approved{}, ErrNoOrganization
	}
	impersonating, err := s.actors.ImpersonatingActor(ctx, in.PrincipalID, in.WebSessionID)
	if err != nil {
		s.logger.WarnContext(ctx, "OAuth approval refused: the actor state could not be read", "principal_id", in.PrincipalID.String())
		return Approved{}, ErrActorUnavailable
	}
	if impersonating {
		s.logger.WarnContext(ctx, "OAuth approval refused: actor side of an impersonation", "principal_id", in.PrincipalID.String())
		return Approved{}, ErrActorRefused
	}
	grantID, sessionID, code, err := newApproval()
	if err != nil {
		return Approved{}, fmt.Errorf("%w: %w", ErrStore, err)
	}
	linked := before.class == ClassBrowser
	binding := bindingClientSession
	if linked {
		binding = bindingWebSession
	}
	event := s.event(ctx, in.PrincipalID, before, sessionID)
	created := event
	created.EventType = EventClientSessionCreate
	var out Approved
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			r, err := s.read(ctx, tx, in.RequestID, " FOR UPDATE")
			if err != nil {
				return err
			}
			if err := r.check(in); err != nil {
				return err
			}
			record, err := s.sessions.InsertClientSession(ctx, tx, session.ClientSessionInput{ID: sessionID, WebSession: in.WebSessionID, Linked: linked})
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO `+s.q+`.oauth_grants
					(id, principal_id, organization_id, client_id, client_class, resource, audience, scope,
					 session_id, binding, consented_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, clock_timestamp())`,
				grantID, in.PrincipalID, record.OrganizationID, r.clientID, r.class, r.resource, r.audience, r.scope,
				sessionID, binding); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				UPDATE `+s.q+`.oauth_authorization_requests
				SET decision = $2, grant_id = $3, code_hash = $4,
				    code_expires_at = clock_timestamp() + make_interval(secs => $5::double precision)
				WHERE id = $1`,
				in.RequestID, decisionApproved, grantID, hashSecret(code), CodeLifetime.Seconds()); err != nil {
				return err
			}
			out = Approved{Code: code, RedirectURI: r.redirectURI, State: r.state, GrantID: grantID, SessionID: sessionID}
			return nil
		},
		Audit:     withType(event, EventConsentApproved),
		Also:      []audit.AuditEvent{created},
		OffStream: audit.OffStreamSessionLifecycle,
		Refusals:  refusals,
	})
	if err != nil {
		return Approved{}, s.decisionError(err, "approving")
	}
	return out, nil
}

// Deny records the refusal, once, with its audit row.
func (s *Store) Deny(ctx context.Context, in Decision) (Denied, error) {
	before, err := s.read(ctx, s.pool, in.RequestID, "")
	if err != nil {
		return Denied{}, s.storeError(err, "reading an authorization request")
	}
	if err := before.check(in); err != nil {
		return Denied{}, err
	}
	event := s.event(ctx, in.PrincipalID, before, in.WebSessionID)
	event.EventType, event.Outcome = EventConsentDenied, audit.OutcomeDenied
	event.Attributes["reason_code"] = "denied_policy"
	var out Denied
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			r, err := s.read(ctx, tx, in.RequestID, " FOR UPDATE")
			if err != nil {
				return err
			}
			if err := r.check(in); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.oauth_authorization_requests SET decision = $2 WHERE id = $1`,
				in.RequestID, decisionDenied); err != nil {
				return err
			}
			out = Denied{RedirectURI: r.redirectURI, State: r.state}
			return nil
		},
		Audit:     event,
		OffStream: audit.OffStreamSessionLifecycle,
		Refusals:  refusals,
	})
	if err != nil {
		return Denied{}, s.decisionError(err, "denying")
	}
	return out, nil
}

// event is the audit row of one decision, with no code, token or state.
func (s *Store) event(ctx context.Context, principal uuid.UUID, r row, target uuid.UUID) audit.AuditEvent {
	principalText := principal.String()
	var org *string
	if r.organization != nil {
		text := r.organization.String()
		org = &text
	}
	return audit.AuditEvent{
		Outcome: audit.OutcomeAllowed, ActorPrincipalID: &principalText, SubjectPrincipalID: &principalText,
		OrganizationID: org, RequestID: requestid.Audit(ctx),
		Attributes: map[string]any{
			"reason_code": "accepted", "target_kind": "session", "target_id": target.String(),
			"client_class": r.class, "audience": r.audience,
		},
	}
}

func withType(event audit.AuditEvent, eventType string) audit.AuditEvent {
	event.EventType = eventType
	return event
}

func (s *Store) storeError(err error, doing string) error {
	if errors.Is(err, ErrRequestNotFound) {
		return err
	}
	return fmt.Errorf("%w: %s: %w", ErrStore, doing, err)
}

func (s *Store) decisionError(err error, doing string) error {
	for _, refusal := range refusals {
		if errors.Is(err, refusal) {
			return refusal
		}
	}
	return fmt.Errorf("%w: %s an authorization request: %w", ErrStore, doing, err)
}

// ConsumeInput is a code and everything it is bound to (ADR-13 decision 6). The
// token endpoint computes CodeChallenge from the verifier it was given.
type ConsumeInput struct {
	Code                                           contracts.SecretString
	ClientID, RedirectURI, Resource, CodeChallenge string
}

// Consumed is what a spent code gives the token endpoint.
type Consumed struct {
	GrantID, PrincipalID, OrganizationID uuid.UUID
	Audience, Scope                      string
}

// ConsumeCode spends a code once, atomically: of any number of calls in parallel,
// one gets the row. Every refusal is ErrCodeRefused, with no reason.
func (s *Store) ConsumeCode(ctx context.Context, in ConsumeInput) (Consumed, error) {
	var out Consumed
	err := s.pool.QueryRow(ctx, `
		UPDATE `+s.q+`.oauth_authorization_requests
		SET consumed_at = clock_timestamp()
		WHERE code_hash = $1 AND consumed_at IS NULL AND code_expires_at > clock_timestamp()
		  AND client_id = $2 AND redirect_uri = $3 AND resource = $4 AND code_challenge = $5
		RETURNING grant_id, principal_id, organization_id, audience, scope`,
		hashSecret(in.Code), in.ClientID, in.RedirectURI, in.Resource, in.CodeChallenge).
		Scan(&out.GrantID, &out.PrincipalID, &out.OrganizationID, &out.Audience, &out.Scope)
	if errors.Is(err, pgx.ErrNoRows) {
		return Consumed{}, ErrCodeRefused
	}
	if err != nil {
		return Consumed{}, fmt.Errorf("%w: consuming a code: %w", ErrStore, err)
	}
	return out, nil
}

func newApproval() (grant, sessionID uuid.UUID, code contracts.SecretString, err error) {
	if grant, err = uuid.NewRandom(); err != nil {
		return
	}
	if sessionID, err = uuid.NewRandom(); err != nil {
		return
	}
	code, err = newSecret(codePrefix)
	return
}

func newSecret(prefix string) (contracts.SecretString, error) {
	raw := make([]byte, secretBytes)
	if _, err := rand.Read(raw); err != nil {
		return contracts.SecretString{}, err
	}
	return contracts.NewSecretString(prefix + base64.RawURLEncoding.EncodeToString(raw)), nil
}

func hashSecret(secret contracts.SecretString) []byte {
	sum := sha256.Sum256([]byte(secret.Reveal()))
	return sum[:]
}
