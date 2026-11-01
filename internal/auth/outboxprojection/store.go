package outboxprojection

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
)

// stored is one credential_projection row with the registry bounds it joins.
type stored struct {
	state       pushlifecycle.State
	principal   uuid.UUID
	registry    uuid.UUID
	maxSeen     int64
	revokedAt   *time.Time
	registryExp *time.Time
}

// apply runs inside audit.Commit's transaction. It writes nothing for a
// duplicate, a stale delivery, a conflict or a retry outcome, which Commit
// reports as ErrNoStateChange.
// errOutcomeMismatch: the audit row was built for the other outcome (allowed or denied); Project retries with the right one.
var errOutcomeMismatch = errors.New("outboxprojection: the audit outcome does not match the decision")

func (e *Engine) apply(ctx context.Context, tx audit.TxOps, in Input, organization uuid.UUID, attrs map[string]any, denied bool) (Result, error) {
	event := in.Event
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"credential_projection:"+string(in.Domain)+":"+event.AggregateID); err != nil {
		return Result{}, err
	}
	current, found, err := e.load(ctx, tx, in.Domain, event.AggregateID, organization, true)
	if err != nil {
		return Result{}, err
	}
	revision := int64(event.Payload.Revision)
	digest := Digest(in)

	var storedDigest []byte
	err = tx.QueryRow(ctx, `SELECT digest FROM `+e.q+`.credential_projection_digest
		WHERE domain_owner = $1 AND external_reference = $2 AND revision = $3`, string(in.Domain), event.AggregateID, revision).Scan(&storedDigest)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Result{}, err
	}
	state := current.state
	switch Compare(storedDigest, digest) {
	case VerdictDifferent:
		return Result{Outcome: OutcomeConflict, State: state}, nil
	case VerdictSame:
		outcome := pushlifecycle.Duplicate
		if revision < state.Revision {
			outcome = pushlifecycle.Stale
		}
		return Result{Outcome: outcome, State: state}, nil
	}

	var parent *pushlifecycle.State
	if event.EventType == contracts.ExternalPushRotated && event.Payload.Change.RotationParentID != "" {
		p, ok, err := e.load(ctx, tx, in.Domain, event.Payload.Change.RotationParentID, organization, false)
		if err != nil {
			return Result{}, err
		}
		if ok {
			narrowed := Narrow(p.state, p.revokedAt, p.registryExp)
			parent = &narrowed
		}
	}
	if found {
		state = Narrow(state, current.revokedAt, current.registryExp)
	}
	next, outcome := pushlifecycle.Apply(state, event, parent)
	switch {
	case outcome.Retry():
		return Result{Outcome: outcome, State: current.state}, nil
	case outcome == pushlifecycle.Duplicate || outcome == pushlifecycle.Stale:
		return Result{Outcome: outcome, State: current.state}, nil
	case outcome != pushlifecycle.Applied:
		if !denied {
			return Result{}, errOutcomeMismatch
		}
		attrs["reason_code"] = "denied_policy"
		if err := e.insertDigest(ctx, tx, in, revision, digest, outcome); err != nil {
			return Result{}, err
		}
		return Result{Outcome: outcome, State: current.state}, nil
	}

	if denied {
		return Result{}, errOutcomeMismatch
	}
	principal, err := e.ensureSource(ctx, tx, organization, event.Payload.SourceID)
	if err != nil {
		return Result{}, err
	}
	registry := current.registry
	switch event.EventType {
	case contracts.ExternalPushCreated, contracts.ExternalPushRotated:
		registry = uuid.New()
		err = e.insertRegistry(ctx, tx, in, registry, principal, next)
		attrs["reason_code"] = map[string]string{contracts.ExternalPushCreated: "credential_issued", contracts.ExternalPushRotated: "credential_rotated"}[event.EventType]
	case contracts.ExternalPushScoped:
		err = e.narrowScopes(ctx, tx, registry, next.Scopes)
		attrs["reason_code"] = "credential_rotated"
	case contracts.ExternalPushRevoked:
		reason := MapRevokeReason(in.RevokeReason)
		err = e.revokeRegistry(ctx, tx, registry, next.TerminalAt, reason)
		attrs["reason_code"] = reason
	case contracts.ExternalPushExpired:
		err = e.expireRegistry(ctx, tx, registry, next.ExpiresAt)
		attrs["reason_code"] = "expired"
	}
	if err != nil {
		return Result{}, err
	}
	principalID := principal
	if found {
		principalID = current.principal
	}
	if err := e.upsertProjection(ctx, tx, in, organization, next, principalID, registry); err != nil {
		return Result{}, err
	}
	if err := e.insertDigest(ctx, tx, in, revision, digest, pushlifecycle.Applied); err != nil {
		return Result{}, err
	}
	attrs["new_status"] = string(next.Status)
	attrs["principal_id"] = principalID.String()
	return Result{Outcome: pushlifecycle.Applied, State: next}, nil
}

// load reads one projection row of the organization with the registry bounds.
// lock takes the row FOR UPDATE.
func (e *Engine) load(ctx context.Context, tx audit.TxOps, domain Domain, reference string, organization uuid.UUID, lock bool) (stored, bool, error) {
	query := `SELECT p.revision, p.max_seen_revision, p.status, p.fingerprint, p.scopes, p.rotation_parent_reference, p.expires_at, p.terminal_at,
		p.principal_id, p.registry_id, p.source_id, r.revoked_at, r.expires_at
		FROM ` + e.q + `.credential_projection p JOIN ` + e.q + `.credential_registry r ON r.id = p.registry_id
		WHERE p.domain_owner = $1 AND p.external_reference = $2 AND p.organization_id = $3`
	if lock {
		query += ` FOR UPDATE OF p`
	}
	var (
		s                  stored
		status, source     string
		parentRef, expires *string
		terminal           *time.Time
	)
	err := tx.QueryRow(ctx, query, string(domain), reference, organization).Scan(&s.state.Revision, &s.maxSeen, &status, &s.state.Fingerprint,
		&s.state.Scopes, &parentRef, &expires, &terminal, &s.principal, &s.registry, &source, &s.revokedAt, &s.registryExp)
	if errors.Is(err, pgx.ErrNoRows) {
		return stored{}, false, nil
	}
	if err != nil {
		return stored{}, false, err
	}
	s.state.AggregateID = reference
	s.state.OrganizationID = "org_" + organization.String()
	s.state.SourceID = source
	s.state.Status = pushlifecycle.Status(status)
	if parentRef != nil {
		s.state.RotationParentID = *parentRef
	}
	if expires != nil {
		s.state.ExpiresAt = *expires
	}
	if terminal != nil {
		s.state.TerminalAt = terminal.UTC()
	}
	return s, true, nil
}

// ensureSource returns the external_source principal of (organization, source),
// creating it and its sources row on the first event of the pair.
func (e *Engine) ensureSource(ctx context.Context, tx audit.TxOps, organization uuid.UUID, source string) (uuid.UUID, error) {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "push_source:"+organization.String()+":"+source); err != nil {
		return uuid.Nil, err
	}
	var id uuid.UUID
	err := tx.QueryRow(ctx, `SELECT principal_id FROM `+e.q+`.push_sources WHERE organization_id = $1 AND source_id = $2`, organization, source).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, err
	}
	id = uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO `+e.q+`.principals (id, kind, display_name) VALUES ($1, 'external_source', $2)`, id, source); err != nil {
		return uuid.Nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+e.q+`.push_sources (organization_id, source_id, principal_id) VALUES ($1, $2, $3)`, organization, source, id); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func expiryTime(text string) (*time.Time, error) {
	if text == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return nil, fmt.Errorf("outboxprojection: stored expiry %q: %w", text, err)
	}
	return &t, nil
}

func (e *Engine) insertRegistry(ctx context.Context, tx audit.TxOps, in Input, id, principal uuid.UUID, next pushlifecycle.State) error {
	expires, err := expiryTime(next.ExpiresAt)
	if err != nil {
		return err
	}
	class := RegistryClass(in)
	// The registry row of a domain-owned credential holds the public fingerprint
	// and no verifier (0026); rotation_parent_id stays NULL, the lineage is in
	// credential_projection (0007 refuses a revoked parent that the core accepts).
	_, err = tx.Exec(ctx, `INSERT INTO `+e.q+`.credential_registry
		(id, principal_id, credential_class, secret_hash, hash_algorithm, fingerprint, scopes, expires_at, domain_owner, external_reference)
		VALUES ($1, $2, $3, decode($4, 'hex'), 'fingerprint', $4, $5::text[]::`+e.q+`.credential_scope[], $6, $7, $8)`,
		id, principal, class, next.Fingerprint, next.Scopes, expires, string(in.Domain), next.AggregateID)
	return err
}

func (e *Engine) narrowScopes(ctx context.Context, tx audit.TxOps, registry uuid.UUID, scopes []string) error {
	_, err := tx.Exec(ctx, `UPDATE `+e.q+`.credential_registry
		SET scopes = ARRAY(SELECT s FROM unnest(scopes) AS s WHERE s::text = ANY($2::text[])), updated_at = clock_timestamp()
		WHERE id = $1`, registry, scopes)
	return err
}

func (e *Engine) revokeRegistry(ctx context.Context, tx audit.TxOps, registry uuid.UUID, at time.Time, reason string) error {
	_, err := tx.Exec(ctx, `UPDATE `+e.q+`.credential_registry
		SET revoked_at = COALESCE(revoked_at, $2), revoked_reason = COALESCE(revoked_reason, $3), updated_at = clock_timestamp()
		WHERE id = $1`, registry, at, reason)
	return err
}

func (e *Engine) expireRegistry(ctx context.Context, tx audit.TxOps, registry uuid.UUID, expiresAt string) error {
	expires, err := expiryTime(expiresAt)
	if err != nil || expires == nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE `+e.q+`.credential_registry
		SET expires_at = LEAST(COALESCE(expires_at, $2), $2), updated_at = clock_timestamp()
		WHERE id = $1`, registry, *expires)
	return err
}

func (e *Engine) upsertProjection(ctx context.Context, tx audit.TxOps, in Input, organization uuid.UUID, next pushlifecycle.State, principal, registry uuid.UUID) error {
	var terminal *time.Time
	if !next.TerminalAt.IsZero() {
		t := next.TerminalAt.UTC()
		terminal = &t
	}
	var parent, expires, class, owner *string
	if next.Scopes == nil {
		next.Scopes = []string{}
	}
	if next.RotationParentID != "" {
		parent = &next.RotationParentID
	}
	if next.ExpiresAt != "" {
		expires = &next.ExpiresAt
	}
	if in.Domain == DomainAcr {
		if in.AcrClass != "" {
			class = &in.AcrClass
		}
		if in.OwnerSubject != "" {
			owner = &in.OwnerSubject
		}
	}
	_, err := tx.Exec(ctx, `INSERT INTO `+e.q+`.credential_projection AS p
		(domain_owner, external_reference, organization_id, source_id, revision, max_seen_revision, status, fingerprint, scopes,
		 rotation_parent_reference, expires_at, terminal_at, principal_id, registry_id, acr_class, owner_subject)
		VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		ON CONFLICT (domain_owner, external_reference) DO UPDATE SET
		  revision = EXCLUDED.revision, max_seen_revision = GREATEST(p.max_seen_revision, EXCLUDED.revision),
		  status = EXCLUDED.status, scopes = EXCLUDED.scopes, expires_at = EXCLUDED.expires_at,
		  terminal_at = EXCLUDED.terminal_at, updated_at = clock_timestamp()`,
		string(in.Domain), next.AggregateID, organization, next.SourceID, next.Revision, string(next.Status), next.Fingerprint, next.Scopes,
		parent, expires, terminal, principal, registry, class, owner)
	return err
}

func (e *Engine) insertDigest(ctx context.Context, tx audit.TxOps, in Input, revision int64, digest [32]byte, outcome pushlifecycle.Outcome) error {
	_, err := tx.Exec(ctx, `INSERT INTO `+e.q+`.credential_projection_digest (domain_owner, external_reference, revision, digest, outcome)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, string(in.Domain), in.Event.AggregateID, revision, digest[:], string(outcome))
	return err
}
