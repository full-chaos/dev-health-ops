package serviceaccount

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// The service-account key: the prefix and 32 random bytes. The registry keeps
// the SHA-256 digest of the whole value; the value leaves once, in IssuedKey.
const (
	KeyClass         = "service_account_key"
	KeyPrefix        = "fcsak_"
	KeyHashAlgorithm = "sha256"

	KeyExpiryDefault  = 90 * 24 * time.Hour
	KeyExpiryCeiling  = 365 * 24 * time.Hour
	KeyOverlapCeiling = 24 * time.Hour

	keyValueBytes = 32
)

// Revoke reasons: credential_registry.revoked_reason's closed vocabulary (0011).
const (
	KeyReasonCompromised  = "compromised"
	KeyReasonOwnerRequest = "owner_request"
)

const (
	verbIssueKey  verb = "issue_key"
	verbRotateKey verb = "rotate_key"
	verbRevokeKey verb = "revoke_key"
)

// Refusal reasons of ResolveKey, for the log and the metric only.
const (
	keyReasonUnknown           = "unknown"
	keyReasonWrongClass        = "wrong_class"
	keyReasonNotPlatform       = "not_platform"
	keyReasonWrongKind         = "wrong_kind"
	keyReasonRevoked           = "revoked"
	keyReasonExpired           = "expired"
	keyReasonAccountDisabled   = "account_disabled"
	keyReasonPrincipalDisabled = "principal_disabled"
	keyReasonOrgSuspended      = "org_suspended"
)

var keyPattern = regexp.MustCompile(`^fcsak_[A-Za-z0-9_-]{43}$`)

// ValidKeyShape reports whether value has the key's shape. A value outside it
// is refused before any store read.
func ValidKeyShape(value string) bool { return keyPattern.MatchString(value) }

// KeyDigest is the digest the registry stores for value: SHA-256 of the whole
// value, prefix included.
func KeyDigest(value string) [sha256.Size]byte { return sha256.Sum256([]byte(value)) }

// MintKey returns a new key value.
func MintKey() (string, error) {
	raw := make([]byte, keyValueBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", errors.New("serviceaccount: the system random source failed")
	}
	return KeyPrefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

// CanonicalKey reports whether value is a value MintKey can return: the shape,
// and a strict decode to exactly 32 bytes.
func CanonicalKey(value string) bool {
	if !ValidKeyShape(value) {
		return false
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(strings.TrimPrefix(value, KeyPrefix))
	return err == nil && len(raw) == keyValueBytes
}

// Key is one registry row of the class. It holds no secret and no digest.
type Key struct {
	ID        uuid.UUID
	Principal uuid.UUID
	ExpiresAt time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
	ParentID  *uuid.UUID

	Fingerprint   string
	RevokedAt     *time.Time
	RevokedReason *string
	CreatedBy     *uuid.UUID
}

// IssuedKey is the answer of IssueKey and RotateKey, and the one place a key
// value exists outside the caller of ResolveKey. ParentExpiresAt is set by a
// rotation.
type IssuedKey struct {
	Secret          contracts.SecretString
	Key             Key
	ParentExpiresAt time.Time
}

// ResolvedKey is a live key. Audiences and Scopes are never nil. Empty means no
// scope, never all: HasAudience and HasScope answer false on it.
type ResolvedKey struct {
	CredentialID uuid.UUID
	Principal    uuid.UUID
	OrgID        uuid.UUID
	ExpiresAt    time.Time
	Revision     int64
	Audiences    []string
	Scopes       []string
}

// HasAudience reports whether the key holds audience.
func (k ResolvedKey) HasAudience(audience string) bool { return slices.Contains(k.Audiences, audience) }

// HasScope reports whether the key holds scope.
func (k ResolvedKey) HasScope(scope string) bool { return slices.Contains(k.Scopes, scope) }

// KeyRefusal is a ResolveKey refusal. Reason is for the log and the metric.
type KeyRefusal struct{ Reason string }

func (e *KeyRefusal) Error() string { return "serviceaccount: key refused: " + e.Reason }

// keyFault is a store failure reduced to its SQLSTATE: a pgx error's message and
// detail can quote row values (a digest), so neither is kept.
type keyFault struct{ code string }

var keySQLState = regexp.MustCompile(`^[0-9A-Z]{5}$`)

func (e keyFault) Error() string {
	if e.code == "" {
		return ErrStore.Error()
	}
	return ErrStore.Error() + " (sqlstate " + e.code + ")"
}

func (e keyFault) Is(target error) bool { return target == ErrStore }

// SQLState lets logging.ErrorAttr report the code.
func (e keyFault) SQLState() string { return e.code }

func faultOf(err error) error {
	var f keyFault
	if errors.As(err, &f) {
		return f
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) && keySQLState.MatchString(pg.Code) {
		return keyFault{code: pg.Code}
	}
	return keyFault{}
}

// keyState is what issue and rotate decide on, read under the verb's locks.
type keyState struct {
	accountActive bool
	principalOff  bool
	orgActive     bool
	owner         ownerState
	live          int
}

func (st keyState) refusal() error {
	switch {
	case !st.accountActive, st.principalOff:
		return ErrAccountDisabled
	case !st.orgActive:
		return ErrOrgNotActive
	}
	return st.owner.refusal()
}

func decideIssue(st keyState) error {
	if err := st.refusal(); err != nil {
		return err
	}
	if st.live > 0 {
		return ErrKeyLive
	}
	return nil
}

func decideRotate(st keyState) error {
	if err := st.refusal(); err != nil {
		return err
	}
	switch st.live {
	case 0:
		return ErrKeyNone
	case 1:
		return nil
	}
	return ErrKeyRotating
}

func validKeyExpiry(d time.Duration) bool { return d > 0 && d <= KeyExpiryCeiling }

func validKeyOverlap(d time.Duration) bool { return d >= 0 && d <= KeyOverlapCeiling }

const liveKey = `credential_class = '` + KeyClass + `' AND revoked_at IS NULL AND expires_at > clock_timestamp()`

// keyColumns are the columns of a Key, in scanKey's order.
const keyColumns = `id, principal_id, expires_at, created_at, updated_at, rotation_parent_id, fingerprint, revoked_at, revoked_reason, created_by`

func scanKey(row pgx.Row) (Key, error) {
	var k Key
	err := row.Scan(&k.ID, &k.Principal, &k.ExpiresAt, &k.CreatedAt, &k.UpdatedAt, &k.ParentID, &k.Fingerprint, &k.RevokedAt, &k.RevokedReason, &k.CreatedBy)
	return k, err
}

func keyWire(id uuid.UUID) string { return "crd_" + id.String() }

func keyAudit(eventType, reason string, id, org uuid.UUID, owner *uuid.UUID, credential uuid.UUID, a Actor, field, next string) audit.AuditEvent {
	ev := auditFor(eventType, reason, id, org, owner, a, []string{field}, "", next)
	ev.Attributes["credential_class"] = KeyClass
	ev.Attributes["credential_id"] = credential.String()
	return ev
}

func keyEvents(id, credential uuid.UUID, status string) []audit.OutboxEvent {
	return []audit.OutboxEvent{{AggregateType: "service_account", AggregateID: wire(id), EventType: "service_account.key_changed",
		Change: map[string]any{"status": status, "credential_id": keyWire(credential)}}}
}

// keyApply keeps a store error out of Commit's log: only a refusal or a keyFault
// leaves Apply.
func keyApply(apply func(ctx context.Context, tx audit.TxOps) error) func(ctx context.Context, tx audit.TxOps) error {
	return func(ctx context.Context, tx audit.TxOps) error {
		err := apply(ctx, tx)
		if err == nil {
			return nil
		}
		for _, known := range keyRefusals {
			if errors.Is(err, known) {
				return known
			}
		}
		return faultOf(err)
	}
}

func (s *Store) runKey(ctx context.Context, v verb, m audit.Mutation) (err error) {
	defer func() {
		s.record(ctx, v, err)
		if errors.Is(err, audit.ErrNoStateChange) {
			err = nil
		}
	}()
	m.Refusals = keyRefusals
	err = s.commit(ctx, m)
	if err == nil || errors.Is(err, audit.ErrNoStateChange) {
		return err
	}
	for _, known := range keyRefusals {
		if errors.Is(err, known) {
			return known
		}
	}
	f := faultOf(err)
	s.logger.ErrorContext(ctx, "serviceaccount: key store failure", "verb", string(v), logging.ErrorAttr(f))
	return f
}

func (s *Store) refuseKey(ctx context.Context, v verb, err error) error {
	s.record(ctx, v, err)
	return err
}

// lockKeyState takes the locks of issue and rotate in Enable's order and reads
// the state they decide on. It returns the live key ids, oldest first.
func (s *Store) lockKeyState(ctx context.Context, tx audit.TxOps, id uuid.UUID, info *Info, a Actor) (keyState, []uuid.UUID, error) {
	var st keyState
	if err := s.lockSet(ctx, tx, &id, info.Owner, a.PrincipalID); err != nil {
		return st, nil, err
	}
	var err error
	if st.orgActive, err = s.lockOrg(ctx, tx, info.OrgID); err != nil {
		return st, nil, err
	}
	if st.owner, err = s.lockOwner(ctx, tx, info.OrgID, info.Owner); err != nil {
		return st, nil, err
	}
	row, err := s.lockAccount(ctx, tx, id)
	if err != nil {
		return st, nil, err
	}
	if row.org != info.OrgID || row.owner != info.Owner {
		return st, nil, ErrStateChanged
	}
	st.accountActive = row.active
	if err := tx.QueryRow(ctx, `SELECT disabled_at IS NOT NULL FROM `+s.q+`.principals WHERE id = $1`, id).Scan(&st.principalOff); err != nil {
		return st, nil, err
	}
	if err := a.authorize(ctx, tx, Locked{OrgID: row.org, Owner: row.owner}); err != nil {
		return st, nil, err
	}
	rows, err := tx.Query(ctx, `SELECT id FROM `+s.q+`.credential_registry WHERE principal_id = $1 AND `+liveKey+`
ORDER BY created_at, id FOR UPDATE`, id)
	if err != nil {
		return st, nil, err
	}
	defer rows.Close()
	var live []uuid.UUID
	for rows.Next() {
		var key uuid.UUID
		if err := rows.Scan(&key); err != nil {
			return st, nil, err
		}
		live = append(live, key)
	}
	if err := rows.Err(); err != nil {
		return st, nil, err
	}
	st.live = len(live)
	return st, live, nil
}

// keyInputs runs the checks issue and rotate share before any transaction, and
// mints the value.
func (s *Store) keyInputs(ctx context.Context, v verb, id uuid.UUID, expiresIn time.Duration) (*Info, string, error) {
	if !s.ownsUsers {
		return nil, "", s.refuseNotOwned(ctx, v)
	}
	if !validKeyExpiry(expiresIn) {
		return nil, "", s.refuseKey(ctx, v, ErrKeyExpiry)
	}
	info, err := s.read(ctx, id)
	if err != nil {
		return nil, "", s.refuseKey(ctx, v, err)
	}
	value, err := s.mint()
	if err != nil {
		return nil, "", s.refuseKey(ctx, v, fmt.Errorf("%w: %w", ErrStore, err))
	}
	return info, value, nil
}

// IssueKey issues the first key of an account: expiry required, at most 365 d.
// An account with a live key is refused (rotate it), and so is a disabled
// account, a suspended organization or a lost owner.
func (s *Store) IssueKey(ctx context.Context, id uuid.UUID, expiresIn time.Duration, a Actor) (IssuedKey, error) {
	info, value, err := s.keyInputs(ctx, verbIssueKey, id, expiresIn)
	if err != nil {
		return IssuedKey{}, err
	}
	digest := KeyDigest(value)
	res := IssuedKey{Key: Key{ID: uuid.New(), Principal: id}}
	owner := info.Owner
	err = s.runKey(ctx, verbIssueKey, audit.Mutation{
		Apply: keyApply(func(ctx context.Context, tx audit.TxOps) error {
			st, _, err := s.lockKeyState(ctx, tx, id, info, a)
			if err != nil {
				return err
			}
			if err := decideIssue(st); err != nil {
				return err
			}
			res.Key, err = scanKey(tx.QueryRow(ctx, `INSERT INTO `+s.q+`.credential_registry
(id, principal_id, credential_class, display_prefix, secret_hash, hash_algorithm, expires_at, fingerprint, created_by)
VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp() + make_interval(secs => $7), $8, $9)
RETURNING `+keyColumns, res.Key.ID, id, KeyClass, KeyPrefix, digest[:], KeyHashAlgorithm,
				expiresIn.Seconds(), hex.EncodeToString(digest[:8]), a.PrincipalID))
			return err
		}),
		Audit:  keyAudit("auth.service_account.key_issued", "credential_issued", id, info.OrgID, &owner, res.Key.ID, a, "expires_at", "active"),
		Events: keyEvents(id, res.Key.ID, "active"),
	})
	if err != nil {
		return IssuedKey{}, err
	}
	res.Secret = contracts.NewSecretString(value)
	return res, nil
}

// RotateKey issues the successor of the account's one live key. The successor
// keeps the parent's audiences and scopes and expires after expiresIn; the
// parent's expiry only ever moves down, to at most overlap (0..24 h) from now.
// An account whose rotation is still in its overlap is refused.
func (s *Store) RotateKey(ctx context.Context, id uuid.UUID, overlap, expiresIn time.Duration, a Actor) (IssuedKey, error) {
	if !validKeyOverlap(overlap) {
		return IssuedKey{}, s.refuseKey(ctx, verbRotateKey, ErrKeyOverlap)
	}
	info, value, err := s.keyInputs(ctx, verbRotateKey, id, expiresIn)
	if err != nil {
		return IssuedKey{}, err
	}
	digest := KeyDigest(value)
	res := IssuedKey{Key: Key{ID: uuid.New(), Principal: id}}
	owner := info.Owner
	err = s.runKey(ctx, verbRotateKey, audit.Mutation{
		Apply: keyApply(func(ctx context.Context, tx audit.TxOps) error {
			st, live, err := s.lockKeyState(ctx, tx, id, info, a)
			if err != nil {
				return err
			}
			if err := decideRotate(st); err != nil {
				return err
			}
			parent := live[0]
			res.Key, err = scanKey(tx.QueryRow(ctx, `INSERT INTO `+s.q+`.credential_registry
(id, principal_id, credential_class, display_prefix, secret_hash, hash_algorithm, expires_at, audiences, scopes, fingerprint, rotation_parent_id, created_by)
SELECT $1, principal_id, credential_class, $2, $3, $4, clock_timestamp() + make_interval(secs => $5), audiences, scopes, $6, id, $7
FROM `+s.q+`.credential_registry WHERE id = $8 RETURNING `+keyColumns,
				res.Key.ID, KeyPrefix, digest[:], KeyHashAlgorithm, expiresIn.Seconds(), hex.EncodeToString(digest[:8]), a.PrincipalID, parent))
			if err != nil {
				return err
			}
			return tx.QueryRow(ctx, `UPDATE `+s.q+`.credential_registry
SET expires_at = LEAST(expires_at, clock_timestamp() + make_interval(secs => $2)), updated_at = clock_timestamp()
WHERE id = $1 RETURNING expires_at`, parent, overlap.Seconds()).Scan(&res.ParentExpiresAt)
		}),
		Audit:  keyAudit("auth.service_account.key_rotated", "credential_rotated", id, info.OrgID, &owner, res.Key.ID, a, "expires_at", "active"),
		Events: keyEvents(id, res.Key.ID, "active"),
	})
	if err != nil {
		return IssuedKey{}, err
	}
	res.Secret = contracts.NewSecretString(value)
	return res, nil
}

// RevokeKey revokes one key with reason (compromised or owner_request). It is
// the kill path: it takes Disable's locks, no users lock, and works on a disabled
// account, in a suspended organization and with a lost owner. Revocation is
// terminal; a repeat writes nothing. An owner that moved before the locks reads
// again once.
func (s *Store) RevokeKey(ctx context.Context, credentialID uuid.UUID, reason string, a Actor) error {
	if reason != KeyReasonCompromised && reason != KeyReasonOwnerRequest {
		return s.refuseKey(ctx, verbRevokeKey, ErrKeyReason)
	}
	err := s.revokeKeyOnce(ctx, credentialID, reason, a)
	if errors.Is(err, ErrStateChanged) {
		err = s.revokeKeyOnce(ctx, credentialID, reason, a)
	}
	return err
}

func (s *Store) revokeKeyOnce(ctx context.Context, credentialID uuid.UUID, reason string, a Actor) error {
	var id uuid.UUID
	err := s.pool.QueryRow(ctx, `SELECT principal_id FROM `+s.q+`.credential_registry WHERE id = $1 AND credential_class = $2`, credentialID, KeyClass).Scan(&id)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return s.refuseKey(ctx, verbRevokeKey, ErrKeyNotFound)
	case err != nil:
		f := faultOf(err)
		s.logger.ErrorContext(ctx, "serviceaccount: key read failure", logging.ErrorAttr(f))
		return s.refuseKey(ctx, verbRevokeKey, f)
	}
	info, err := s.read(ctx, id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			err = ErrKeyNotFound
		}
		return s.refuseKey(ctx, verbRevokeKey, err)
	}
	owner := info.Owner
	code := "credential_revoked"
	if reason == KeyReasonCompromised {
		code = "compromised"
	}
	return s.runKey(ctx, verbRevokeKey, audit.Mutation{
		Apply: keyApply(func(ctx context.Context, tx audit.TxOps) error {
			if err := platformrole.LockWritesShared(ctx, tx); err != nil {
				return err
			}
			var kind string
			err := tx.QueryRow(ctx, `SELECT kind FROM `+s.q+`.principals WHERE id = $1 FOR UPDATE`, id).Scan(&kind)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrKeyNotFound
			}
			if err != nil {
				return err
			}
			if kind != "service_account" {
				return ErrNotAccount
			}
			row, err := s.lockAccount(ctx, tx, id)
			if err != nil {
				return err
			}
			if row.org != info.OrgID || row.owner != info.Owner {
				return ErrStateChanged
			}
			if err := a.authorize(ctx, tx, Locked{OrgID: row.org, Owner: row.owner}); err != nil {
				return err
			}
			var revoked bool
			if err := tx.QueryRow(ctx, `SELECT revoked_at IS NOT NULL FROM `+s.q+`.credential_registry
WHERE id = $1 AND credential_class = $2 FOR UPDATE`, credentialID, KeyClass).Scan(&revoked); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrKeyNotFound
				}
				return err
			}
			if revoked {
				return nil
			}
			_, err = tx.Exec(ctx, `UPDATE `+s.q+`.credential_registry
SET revoked_at = clock_timestamp(), revoked_reason = $2, updated_at = clock_timestamp() WHERE id = $1`, credentialID, reason)
			return err
		}),
		Audit:  keyAudit("auth.service_account.key_revoked", code, id, info.OrgID, &owner, credentialID, a, "revoked_at", "revoked"),
		Events: keyEvents(id, credentialID, "revoked"),
	})
}

// ListKeys lists the account's live keys, oldest first: two at most, the
// rotated one and its successor. It writes nothing.
func (s *Store) ListKeys(ctx context.Context, id uuid.UUID) ([]Key, error) {
	if _, err := s.read(ctx, id); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT `+keyColumns+`
FROM `+s.q+`.credential_registry WHERE principal_id = $1 AND `+liveKey+` ORDER BY created_at, id`, id)
	if err != nil {
		return nil, faultOf(err)
	}
	defer rows.Close()
	var out []Key
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, faultOf(err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, faultOf(err)
	}
	return out, nil
}

// KeyByID reads one key in any state. The id is the caller's, so the class is
// part of the read: a credential of another class is not found.
func (s *Store) KeyByID(ctx context.Context, id uuid.UUID) (*Key, error) {
	k, err := scanKey(s.pool.QueryRow(ctx, `SELECT `+keyColumns+`
FROM `+s.q+`.credential_registry WHERE id = $1 AND credential_class = $2`, id, KeyClass))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, faultOf(err)
	}
	return &k, nil
}

// keyRow is the one row ResolveKey reads.
type keyRow struct {
	id, principal, org uuid.UUID
	class, domainOwner string
	revoked            bool
	expiresAt          time.Time
	audiences, scopes  []string
	kind               string
	accountDisabled    bool
	principalDisabled  bool
	orgSuspended       bool
	revision           int64
}

// decide is the whole rule for a read row. It answers the first refusal in a
// fixed order, and "" for a live key.
func (row keyRow) decide(now time.Time) (ResolvedKey, string) {
	switch {
	case row.class != KeyClass:
		return ResolvedKey{}, keyReasonWrongClass
	case row.domainOwner != "platform":
		return ResolvedKey{}, keyReasonNotPlatform
	case row.kind != "service_account":
		return ResolvedKey{}, keyReasonWrongKind
	case row.revoked:
		return ResolvedKey{}, keyReasonRevoked
	case !now.Before(row.expiresAt):
		return ResolvedKey{}, keyReasonExpired
	case row.accountDisabled:
		return ResolvedKey{}, keyReasonAccountDisabled
	case row.principalDisabled:
		return ResolvedKey{}, keyReasonPrincipalDisabled
	case row.orgSuspended:
		return ResolvedKey{}, keyReasonOrgSuspended
	}
	key := ResolvedKey{CredentialID: row.id, Principal: row.principal, OrgID: row.org, ExpiresAt: row.expiresAt,
		Revision: row.revision, Audiences: row.audiences, Scopes: row.scopes}
	if key.Audiences == nil {
		key.Audiences = []string{}
	}
	if key.Scopes == nil {
		key.Scopes = []string{}
	}
	return key, ""
}

// ResolveKey maps the digest of a presented key to its live credential. It reads
// the current rows on every call, caches nothing and writes nothing. A refusal is
// a *KeyRefusal; a store failure wraps ErrStore.
func (s *Store) ResolveKey(ctx context.Context, digest [sha256.Size]byte, now time.Time) (ResolvedKey, error) {
	var row keyRow
	err := s.pool.QueryRow(ctx, `SELECT c.id, c.credential_class, c.domain_owner, c.revoked_at IS NOT NULL, c.expires_at,
c.audiences::text[], c.scopes::text[], c.principal_id, sa.organization_id, p.kind, sa.status <> 'active', p.disabled_at IS NOT NULL,
o.disabled_at IS NOT NULL, COALESCE(r.revision, 0)
FROM `+s.q+`.credential_registry c
JOIN `+s.q+`.service_accounts sa ON sa.principal_id = c.principal_id
JOIN `+s.q+`.principals p ON p.id = c.principal_id
JOIN `+s.q+`.organizations o ON o.id = sa.organization_id
LEFT JOIN `+s.q+`.aggregate_revisions r ON r.aggregate_type = 'service_account' AND r.aggregate_id = 'prn_' || c.principal_id::text
WHERE c.secret_hash = $1`, digest[:]).Scan(&row.id, &row.class, &row.domainOwner, &row.revoked, &row.expiresAt, &row.audiences, &row.scopes,
		&row.principal, &row.org, &row.kind, &row.accountDisabled, &row.principalDisabled, &row.orgSuspended, &row.revision)
	reason := ""
	key := ResolvedKey{}
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		reason = keyReasonUnknown
	case err != nil:
		f := faultOf(err)
		s.keyResolves.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "error")))
		s.logger.ErrorContext(ctx, "serviceaccount: key resolve store failure", logging.ErrorAttr(f))
		return ResolvedKey{}, f
	default:
		key, reason = row.decide(now)
	}
	if reason != "" {
		s.keyResolves.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "refused")))
		s.logger.InfoContext(ctx, "serviceaccount: key resolve refused", "reason", reason)
		return ResolvedKey{}, &KeyRefusal{Reason: reason}
	}
	s.keyResolves.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "ok")))
	return key, nil
}
