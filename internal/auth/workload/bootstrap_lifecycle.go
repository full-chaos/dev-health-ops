package workload

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// Bounds of the bootstrap lifecycle verbs. Every value has an expiry; a
// rotation keeps at most two values live, the old one for at most the overlap.
const (
	BootstrapExpiryDefault  = 30 * 24 * time.Hour
	BootstrapExpiryCeiling  = 90 * 24 * time.Hour
	BootstrapOverlapCeiling = time.Hour
	bootstrapValueBytes     = 32
)

// Revoke reasons: credential_registry.revoked_reason's closed vocabulary (0011).
const (
	BootstrapReasonCompromised  = "compromised"
	BootstrapReasonOwnerRequest = "owner_request"
)

// MintBootstrap returns a new bootstrap value: the prefix and 32 bytes from
// crypto/rand as unpadded base64url, the one form the dev-health-go client
// accepts.
func MintBootstrap() (string, error) {
	raw := make([]byte, bootstrapValueBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", errors.New("workload: the system random source failed")
	}
	return BootstrapPrefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

// CanonicalBootstrap reports whether value is a value MintBootstrap can
// return: the shape, and a strict decode to exactly 32 bytes (no padding, no
// stray bits in the last character).
func CanonicalBootstrap(value string) bool {
	if !ValidBootstrapShape(value) {
		return false
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(strings.TrimPrefix(value, BootstrapPrefix))
	return err == nil && len(raw) == bootstrapValueBytes
}

// BootstrapResult is what a lifecycle verb did. It holds ids and times only.
type BootstrapResult struct {
	CredentialID    uuid.UUID
	ExpiresAt       time.Time
	ParentID        uuid.UUID
	ParentExpiresAt time.Time
	Revoked         int
}

// BootstrapCredential is one live bootstrap credential of a workload.
type BootstrapCredential struct {
	ID        uuid.UUID
	ExpiresAt *time.Time
	ParentID  *uuid.UUID
}

// bootstrapRefusals are the errors a verb returns as they are; any other
// failure becomes a storeFault.
var bootstrapRefusals = []error{
	ErrNotFound, ErrAudienceInvalid, ErrBootstrapLive, ErrBootstrapRotating, ErrBootstrapNone, ErrBootstrapNotFound,
}

// storeFault is a store failure reduced to its SQLSTATE: a pgx error's
// message and detail can quote row values (a digest), so neither is kept.
type storeFault struct{ code string }

var sqlStateShape = regexp.MustCompile(`^[0-9A-Z]{5}$`)

func (e storeFault) Error() string {
	if e.code == "" {
		return ErrStore.Error()
	}
	return ErrStore.Error() + " (sqlstate " + e.code + ")"
}

func (e storeFault) Is(target error) bool { return target == ErrStore }

// SQLState lets logging.ErrorAttr report the code.
func (e storeFault) SQLState() string { return e.code }

func fault(err error) error {
	var f storeFault
	if errors.As(err, &f) {
		return f
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) && sqlStateShape.MatchString(pg.Code) {
		return storeFault{code: pg.Code}
	}
	return storeFault{}
}

const liveBootstrap = `credential_class = '` + BootstrapClass + `' AND revoked_at IS NULL
AND (expires_at IS NULL OR expires_at > clock_timestamp())`

func credentialWire(id uuid.UUID) string { return "crd_" + id.String() }

func bootstrapChange(statusValue string, id uuid.UUID) map[string]any {
	c := map[string]any{"status": statusValue}
	if id != uuid.Nil {
		c["credential_id"] = credentialWire(id)
	}
	return c
}

func (s *Store) bootstrapAudit(eventType, reason string, workloadID, credentialID uuid.UUID, a Actor, fields []string, prev, next string) audit.AuditEvent {
	ev := s.auditFor(eventType, reason, workloadID, a, fields, prev, next)
	ev.Attributes["credential_class"] = BootstrapClass
	if credentialID != uuid.Nil {
		ev.Attributes["credential_id"] = credentialID.String()
	}
	return ev
}

func (s *Store) runBootstrap(ctx context.Context, v verb, m audit.Mutation) (err error) {
	defer func() {
		s.record(ctx, v, err)
		if errors.Is(err, audit.ErrNoStateChange) {
			err = nil
		}
	}()
	err = s.commit(ctx, m)
	if err == nil || errors.Is(err, audit.ErrNoStateChange) {
		return err
	}
	for _, known := range bootstrapRefusals {
		if errors.Is(err, known) {
			return known
		}
	}
	f := fault(err)
	s.logger.ErrorContext(ctx, "workload: bootstrap store failure", "verb", string(v), logging.ErrorAttr(f))
	return f
}

func (s *Store) bootstrapLookup(ctx context.Context, v verb, name string) (*Info, error) {
	info, err := s.Lookup(ctx, name)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			err = fault(err)
		}
		s.record(ctx, v, err)
		return nil, err
	}
	return info, nil
}

func (s *Store) refuse(ctx context.Context, v verb, err error) error {
	s.record(ctx, v, err)
	return err
}

func subset(sub, of []string) bool {
	in := make(map[string]bool, len(of))
	for _, x := range of {
		in[x] = true
	}
	for _, x := range sub {
		if !in[x] {
			return false
		}
	}
	return true
}

func validExpiry(d time.Duration) bool { return d > 0 && d <= BootstrapExpiryCeiling }

// CreateBootstrap registers value as the workload's bootstrap credential,
// expiring after expiresIn (at most 90 d), for audiences (the workload's
// audiences when empty; else a subset of them). value must be canonical
// (MintBootstrap's); only its digest is stored. A workload with a live
// bootstrap credential is refused: rotate it instead.
func (s *Store) CreateBootstrap(ctx context.Context, name, value string, audiences []string, expiresIn time.Duration, a Actor) (BootstrapResult, error) {
	switch {
	case !CanonicalBootstrap(value):
		return BootstrapResult{}, s.refuse(ctx, verbBootstrapCreate, ErrBootstrapValue)
	case !validExpiry(expiresIn):
		return BootstrapResult{}, s.refuse(ctx, verbBootstrapCreate, ErrBootstrapExpiry)
	case ValidateAudiences(audiences) != nil:
		return BootstrapResult{}, s.refuse(ctx, verbBootstrapCreate, ErrAudienceInvalid)
	}
	info, err := s.bootstrapLookup(ctx, verbBootstrapCreate, name)
	if err != nil {
		return BootstrapResult{}, err
	}
	digest := BootstrapDigest(value)
	res := BootstrapResult{CredentialID: uuid.New()}
	err = s.runBootstrap(ctx, verbBootstrapCreate, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			own, _, _, err := s.lockWorkload(ctx, tx, info.PrincipalID)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					return err
				}
				return fault(err)
			}
			grant := audiences
			if len(grant) == 0 {
				grant = own
			}
			if len(grant) == 0 || !subset(grant, own) {
				return ErrAudienceInvalid
			}
			var live int
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM `+s.q+`.credential_registry WHERE principal_id = $1 AND `+liveBootstrap,
				info.PrincipalID).Scan(&live); err != nil {
				return fault(err)
			}
			if live > 0 {
				return ErrBootstrapLive
			}
			if err := tx.QueryRow(ctx, `INSERT INTO `+s.q+`.credential_registry
(id, principal_id, credential_class, display_prefix, secret_hash, hash_algorithm, expires_at, audiences, fingerprint)
VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp() + make_interval(secs => $7), $8::text[], $9) RETURNING expires_at`,
				res.CredentialID, info.PrincipalID, BootstrapClass, BootstrapPrefix, digest[:], BootstrapHashAlgorithm,
				expiresIn.Seconds(), grant, hex.EncodeToString(digest[:8])).Scan(&res.ExpiresAt); err != nil {
				return fault(err)
			}
			return nil
		},
		Audit: s.bootstrapAudit("auth.workload.bootstrap_created", "credential_issued", info.PrincipalID, res.CredentialID, a,
			[]string{"expires_at", "audiences"}, "", "active"),
		Events: []audit.OutboxEvent{{AggregateType: "workload", AggregateID: wire(info.PrincipalID), EventType: "workload.bootstrap_changed",
			Change: bootstrapChange("active", res.CredentialID)}},
	})
	if err != nil {
		return BootstrapResult{}, err
	}
	return res, nil
}

// RotateBootstrap registers value as the successor of the workload's one live
// bootstrap credential. The successor keeps the parent's audiences and expires
// after expiresIn; the parent's expiry only ever moves down, to at most
// overlap (0..1 h) from now. A workload whose rotation is still in its overlap
// (two live values) is refused.
func (s *Store) RotateBootstrap(ctx context.Context, name, value string, overlap, expiresIn time.Duration, a Actor) (BootstrapResult, error) {
	switch {
	case !CanonicalBootstrap(value):
		return BootstrapResult{}, s.refuse(ctx, verbBootstrapRotate, ErrBootstrapValue)
	case !validExpiry(expiresIn):
		return BootstrapResult{}, s.refuse(ctx, verbBootstrapRotate, ErrBootstrapExpiry)
	case overlap < 0 || overlap > BootstrapOverlapCeiling:
		return BootstrapResult{}, s.refuse(ctx, verbBootstrapRotate, ErrBootstrapOverlap)
	}
	info, err := s.bootstrapLookup(ctx, verbBootstrapRotate, name)
	if err != nil {
		return BootstrapResult{}, err
	}
	digest := BootstrapDigest(value)
	res := BootstrapResult{CredentialID: uuid.New()}
	err = s.runBootstrap(ctx, verbBootstrapRotate, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, _, _, err := s.lockWorkload(ctx, tx, info.PrincipalID); err != nil {
				if errors.Is(err, ErrNotFound) {
					return err
				}
				return fault(err)
			}
			rows, err := tx.Query(ctx, `SELECT id FROM `+s.q+`.credential_registry WHERE principal_id = $1 AND `+liveBootstrap+`
FOR UPDATE`, info.PrincipalID)
			if err != nil {
				return fault(err)
			}
			var live []uuid.UUID
			for rows.Next() {
				var id uuid.UUID
				if err := rows.Scan(&id); err != nil {
					rows.Close()
					return fault(err)
				}
				live = append(live, id)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return fault(err)
			}
			switch len(live) {
			case 0:
				return ErrBootstrapNone
			case 1:
			default:
				return ErrBootstrapRotating
			}
			res.ParentID = live[0]
			if err := tx.QueryRow(ctx, `INSERT INTO `+s.q+`.credential_registry
(id, principal_id, credential_class, display_prefix, secret_hash, hash_algorithm, expires_at, audiences, fingerprint, rotation_parent_id)
SELECT $1, principal_id, credential_class, $2, $3, $4, clock_timestamp() + make_interval(secs => $5), audiences, $6, id
FROM `+s.q+`.credential_registry WHERE id = $7 RETURNING expires_at`,
				res.CredentialID, BootstrapPrefix, digest[:], BootstrapHashAlgorithm, expiresIn.Seconds(),
				hex.EncodeToString(digest[:8]), res.ParentID).Scan(&res.ExpiresAt); err != nil {
				return fault(err)
			}
			if err := tx.QueryRow(ctx, `UPDATE `+s.q+`.credential_registry
SET expires_at = LEAST(expires_at, clock_timestamp() + make_interval(secs => $2)), updated_at = clock_timestamp()
WHERE id = $1 RETURNING expires_at`, res.ParentID, overlap.Seconds()).Scan(&res.ParentExpiresAt); err != nil {
				return fault(err)
			}
			return nil
		},
		Audit: s.bootstrapAudit("auth.workload.bootstrap_rotated", "credential_rotated", info.PrincipalID, res.CredentialID, a,
			[]string{"expires_at"}, "", "active"),
		Events: []audit.OutboxEvent{{AggregateType: "workload", AggregateID: wire(info.PrincipalID), EventType: "workload.bootstrap_changed",
			Change: bootstrapChange("active", res.CredentialID)}},
	})
	if err != nil {
		return BootstrapResult{}, err
	}
	return res, nil
}

// RevokeBootstrap revokes one bootstrap credential with reason (compromised
// or owner_request). Revocation is terminal; a repeat writes nothing.
func (s *Store) RevokeBootstrap(ctx context.Context, credentialID uuid.UUID, reason string, a Actor) error {
	if reason != BootstrapReasonCompromised && reason != BootstrapReasonOwnerRequest {
		return s.refuse(ctx, verbBootstrapRevoke, ErrBootstrapReason)
	}
	var principal uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT c.principal_id FROM `+s.q+`.credential_registry c
JOIN `+s.q+`.workloads w ON w.principal_id = c.principal_id
WHERE c.id = $1 AND c.credential_class = $2`, credentialID, BootstrapClass).Scan(&principal)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return s.refuse(ctx, verbBootstrapRevoke, ErrBootstrapNotFound)
	case err != nil:
		f := fault(err)
		s.logger.ErrorContext(ctx, "workload: bootstrap read failure", logging.ErrorAttr(f))
		return s.refuse(ctx, verbBootstrapRevoke, f)
	}
	code := "credential_revoked"
	if reason == BootstrapReasonCompromised {
		code = "compromised"
	}
	return s.runBootstrap(ctx, verbBootstrapRevoke, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, _, _, err := s.lockWorkload(ctx, tx, principal); err != nil {
				if errors.Is(err, ErrNotFound) {
					return ErrBootstrapNotFound
				}
				return fault(err)
			}
			var revoked bool
			if err := tx.QueryRow(ctx, `SELECT revoked_at IS NOT NULL FROM `+s.q+`.credential_registry
WHERE id = $1 AND credential_class = $2 FOR UPDATE`, credentialID, BootstrapClass).Scan(&revoked); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrBootstrapNotFound
				}
				return fault(err)
			}
			if revoked {
				return nil
			}
			if _, err := tx.Exec(ctx, `UPDATE `+s.q+`.credential_registry
SET revoked_at = clock_timestamp(), revoked_reason = $2, updated_at = clock_timestamp() WHERE id = $1`, credentialID, reason); err != nil {
				return fault(err)
			}
			return nil
		},
		Audit: s.bootstrapAudit("auth.workload.bootstrap_revoked", code, principal, credentialID, a, []string{"revoked_at"}, "", "revoked"),
		Events: []audit.OutboxEvent{{AggregateType: "workload", AggregateID: wire(principal), EventType: "workload.bootstrap_changed",
			Change: bootstrapChange("revoked", credentialID)}},
	})
}

// DisableBootstrap is the emergency stop of the workload's bootstrap path: it
// revokes every live bootstrap credential of the workload (reason
// compromised) in one transaction, so the exchange refuses them at once.
// Workload tokens issued before it stay valid to their expiry, at most 300 s.
// The workload itself and its Kubernetes bindings are not touched. With no
// live value it writes nothing.
func (s *Store) DisableBootstrap(ctx context.Context, name string, a Actor) (BootstrapResult, error) {
	info, err := s.bootstrapLookup(ctx, verbBootstrapDisable, name)
	if err != nil {
		return BootstrapResult{}, err
	}
	var res BootstrapResult
	err = s.runBootstrap(ctx, verbBootstrapDisable, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, _, _, err := s.lockWorkload(ctx, tx, info.PrincipalID); err != nil {
				if errors.Is(err, ErrNotFound) {
					return err
				}
				return fault(err)
			}
			tag, err := tx.Exec(ctx, `UPDATE `+s.q+`.credential_registry
SET revoked_at = clock_timestamp(), revoked_reason = $2, updated_at = clock_timestamp()
WHERE principal_id = $1 AND `+liveBootstrap, info.PrincipalID, BootstrapReasonCompromised)
			if err != nil {
				return fault(err)
			}
			res.Revoked = int(tag.RowsAffected())
			return nil
		},
		Audit: s.bootstrapAudit("auth.workload.bootstrap_disabled", "compromised", info.PrincipalID, uuid.Nil, a,
			[]string{"revoked_at"}, "", "revoked"),
		Events: []audit.OutboxEvent{{AggregateType: "workload", AggregateID: wire(info.PrincipalID), EventType: "workload.bootstrap_changed",
			Change: bootstrapChange("revoked", uuid.Nil)}},
	})
	if err != nil {
		return BootstrapResult{}, err
	}
	return res, nil
}

// LiveBootstrap lists the workload's live bootstrap credentials, oldest
// first. It writes nothing.
func (s *Store) LiveBootstrap(ctx context.Context, name string) ([]BootstrapCredential, error) {
	info, err := s.Lookup(ctx, name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, err
		}
		return nil, fault(err)
	}
	rows, err := s.pool.Query(ctx, `SELECT id, expires_at, rotation_parent_id FROM `+s.q+`.credential_registry
WHERE principal_id = $1 AND `+liveBootstrap+` ORDER BY created_at, id`, info.PrincipalID)
	if err != nil {
		return nil, fault(err)
	}
	defer rows.Close()
	var out []BootstrapCredential
	for rows.Next() {
		var c BootstrapCredential
		if err := rows.Scan(&c.ID, &c.ExpiresAt, &c.ParentID); err != nil {
			return nil, fault(err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fault(err)
	}
	return out, nil
}

// BootstrapStatus is the derived status of one bootstrap credential: active,
// expired or revoked. It writes nothing.
func (s *Store) BootstrapStatus(ctx context.Context, credentialID uuid.UUID) (string, error) {
	var statusValue string
	err := s.pool.QueryRow(ctx, `SELECT CASE WHEN revoked_at IS NOT NULL THEN 'revoked'
WHEN expires_at IS NOT NULL AND expires_at <= clock_timestamp() THEN 'expired' ELSE 'active' END
FROM `+s.q+`.credential_registry WHERE id = $1 AND credential_class = $2`, credentialID, BootstrapClass).Scan(&statusValue)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrBootstrapNotFound
	}
	if err != nil {
		return "", fault(err)
	}
	return statusValue, nil
}
