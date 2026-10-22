package keyrotation

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// ErrStateChanged: the key states changed between the plan and the locked
// write. Nothing was written; run the verb again.
var ErrStateChanged = errors.New("signing key states changed while the verb ran; nothing was written")

// ErrBadRow: a signing_keys row this package cannot read (its public key is
// not an Ed25519 JWK x).
var ErrBadRow = errors.New("signing_keys row unreadable")

// Reader reads the durable key states for the runtime (SELECT only).
type Reader struct {
	pool *pgxpool.Pool
	q    string
}

// NewReader returns a reader of schema's signing_keys over pool.
func NewReader(pool *pgxpool.Pool, schema authschema.ValidatedIdentifier) *Reader {
	return &Reader{pool: pool, q: authschema.Quote(schema)}
}

var _ token.KeyStateSource = (*Reader)(nil)

// KeyStates returns every key's state and public key.
func (r *Reader) KeyStates(ctx context.Context) ([]token.KeyStateRow, error) {
	rows, err := r.pool.Query(ctx, `SELECT kid, status, public_key_jwk->>'x' FROM `+r.q+`.signing_keys ORDER BY kid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []token.KeyStateRow
	for rows.Next() {
		var kid, state string
		var x *string
		if err := rows.Scan(&kid, &state, &x); err != nil {
			return nil, err
		}
		public, err := publicKeyOf(x)
		if err != nil {
			return nil, err
		}
		out = append(out, token.KeyStateRow{KeyID: kid, State: token.State(state), PublicKey: public})
	}
	return out, rows.Err()
}

// Denylist returns the compromised keys: their kids and public keys are
// refused everywhere.
func (r *Reader) Denylist(ctx context.Context) ([]token.KeyStateRow, error) {
	rows, err := r.KeyStates(ctx)
	if err != nil {
		return nil, err
	}
	var denied []token.KeyStateRow
	for _, row := range rows {
		if row.State == token.StateCompromised {
			denied = append(denied, row)
		}
	}
	return denied, nil
}

func publicKeyOf(x *string) (ed25519.PublicKey, error) {
	if x == nil {
		return nil, ErrBadRow
	}
	raw, err := base64.RawURLEncoding.DecodeString(*x)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil, ErrBadRow
	}
	return ed25519.PublicKey(raw), nil
}

func publicKeyJWK(public ed25519.PublicKey) (string, error) {
	encoded, err := json.Marshal(map[string]string{
		"kty": "OKP", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(public),
	})
	return string(encoded), err
}

// ErrRuntimeRole: the operator pool connects as the runtime role. Key states
// are written by the migration role only.
var ErrRuntimeRole = errors.New("the operator connects as the runtime role; key states are written by the migration role only")

// CheckNotRuntimeRole refuses a pool that connects as runtimeRole.
func CheckNotRuntimeRole(ctx context.Context, pool *pgxpool.Pool, runtimeRole string) error {
	var user string
	if err := pool.QueryRow(ctx, `SELECT current_user`).Scan(&user); err != nil {
		return err
	}
	if user == runtimeRole {
		return ErrRuntimeRole
	}
	return nil
}

// Operator runs the verbs. Its pool connects as the migration role: the
// runtime role holds SELECT only on signing_keys.
type Operator struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	// Now overrides the database clock (tests).
	Now func() time.Time
}

// Result is what a verb did (Apply) or would do (Plan).
type Result struct {
	Moves []Move
	// Sessions is how many live sessions a compromise revokes.
	Sessions int
	// FloorRemaining is how long until a refused floor is reached.
	FloorRemaining time.Duration
}

type rowSet interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// querier is a pool outside a transaction (Plan, List) or audit.Commit's
// transaction (Apply).
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (rowSet, error)
}

type poolQuerier struct{ pool *pgxpool.Pool }

func (p poolQuerier) Query(ctx context.Context, sql string, args ...any) (rowSet, error) {
	return p.pool.Query(ctx, sql, args...)
}

type txQuerier struct{ tx audit.TxOps }

func (t txQuerier) Query(ctx context.Context, sql string, args ...any) (rowSet, error) {
	return t.tx.Query(ctx, sql, args...)
}

func (o *Operator) q() string { return authschema.Quote(o.Schema) }

func (o *Operator) now(ctx context.Context, db querier) (time.Time, error) {
	if o.Now != nil {
		return o.Now(), nil
	}
	rows, err := db.Query(ctx, `SELECT clock_timestamp()`)
	if err != nil {
		return time.Time{}, err
	}
	defer rows.Close()
	var now time.Time
	if !rows.Next() {
		return time.Time{}, errors.Join(errors.New("no clock row"), rows.Err())
	}
	if err := rows.Scan(&now); err != nil {
		return time.Time{}, err
	}
	return now, rows.Err()
}

func (o *Operator) rows(ctx context.Context, db querier, lock bool) ([]Row, error) {
	sql := `SELECT kid, status, public_key_jwk->>'x', state_since FROM ` + o.q() + `.signing_keys ORDER BY kid`
	if lock {
		sql += ` FOR UPDATE`
	}
	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var row Row
		var state string
		var x *string
		if err := rows.Scan(&row.KeyID, &state, &x, &row.StateSince); err != nil {
			return nil, err
		}
		if row.PublicKey, err = publicKeyOf(x); err != nil {
			return nil, err
		}
		row.State = token.State(state)
		out = append(out, row)
	}
	return out, rows.Err()
}

// List returns every key's durable state.
func (o *Operator) List(ctx context.Context) ([]Row, error) {
	return o.rows(ctx, poolQuerier{o.Pool}, false)
}

// Plan decides request against the current states and counts the sessions a
// compromise would revoke. It writes nothing.
func (o *Operator) Plan(ctx context.Context, request Request) (Result, error) {
	db := poolQuerier{o.Pool}
	now, err := o.now(ctx, db)
	if err != nil {
		return Result{}, err
	}
	rows, err := o.rows(ctx, db, false)
	if err != nil {
		return Result{}, err
	}
	moves, err := Decide(request, rows, now)
	if err != nil {
		return Result{FloorRemaining: floorRemaining(request, rows, now)}, err
	}
	result := Result{Moves: moves}
	if request.Verb == VerbCompromise {
		ids, err := o.sessionsToRevoke(ctx, db, request.KeyID, now, false)
		if err != nil {
			return Result{}, err
		}
		result.Sessions = len(ids)
	}
	return result, nil
}

func floorRemaining(request Request, rows []Row, now time.Time) time.Duration {
	for _, row := range rows {
		if row.KeyID != request.KeyID {
			continue
		}
		switch {
		case request.Verb == VerbActivate && row.State == token.StatePending:
			return PendingFloor - now.Sub(row.StateSince)
		case request.Verb == VerbRetire && row.State == token.StateRetiring:
			return RetiringFloor - now.Sub(row.StateSince)
		}
	}
	return 0
}

// Apply plans request, then writes it in one audit.Commit transaction that
// holds every signing_keys row: the moves, a compromise's session
// revocations and one audit row. A refusal is recorded as a denied audit row
// and returned as a *Refusal; nothing else is written.
func (o *Operator) Apply(ctx context.Context, request Request) (Result, error) {
	planned, err := o.Plan(ctx, request)
	var refusal *Refusal
	if errors.As(err, &refusal) {
		if appendErr := audit.Append(ctx, o.Pool, o.Schema, refusedEvent(request, refusal.Reason)); appendErr != nil {
			return planned, errors.Join(err, appendErr)
		}
		return planned, err
	}
	if err != nil {
		return Result{}, err
	}
	result := Result{Moves: planned.Moves}
	err = audit.Commit(ctx, o.Pool, o.Schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('dev_health.signing_keys'))`); err != nil {
				return err
			}
			db := txQuerier{tx}
			now, err := o.now(ctx, db)
			if err != nil {
				return err
			}
			rows, err := o.rows(ctx, db, true)
			if err != nil {
				return err
			}
			moves, err := Decide(request, rows, now)
			if err != nil {
				return err
			}
			if !sameMoves(moves, planned.Moves) {
				return ErrStateChanged
			}
			if request.Verb == VerbCompromise {
				ids, err := o.sessionsToRevoke(ctx, db, request.KeyID, now, true)
				if err != nil {
					return err
				}
				if err := o.revokeSessions(ctx, tx, ids); err != nil {
					return err
				}
				result.Sessions = len(ids)
			}
			for _, move := range moves {
				if err := o.write(ctx, tx, move, now); err != nil {
					return err
				}
			}
			return nil
		},
		Audit:     appliedEvent(request, planned.Moves),
		OffStream: audit.OffStreamSigningKeyLifecycle,
	})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func sameMoves(a, b []Move) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].KeyID != b[i].KeyID || a[i].From != b[i].From || a[i].To != b[i].To || !a[i].PublicKey.Equal(b[i].PublicKey) {
			return false
		}
	}
	return true
}

func (o *Operator) write(ctx context.Context, tx audit.TxOps, move Move, now time.Time) error {
	if move.From == "" {
		jwk, err := publicKeyJWK(move.PublicKey)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO `+o.q()+`.signing_keys
			    (kid, algorithm, public_key_jwk, custody_ref, custody_kind, status, not_before, state_since,
			     activated_at, deactivated_at, revoked_at)
			VALUES ($1::text, 'EdDSA', $2::jsonb, $1::text, 'file', $3::text, $4::timestamptz, $4::timestamptz,
			        CASE WHEN $3::text = 'pending' THEN NULL ELSE '-infinity'::timestamptz END,
			        CASE WHEN $3::text IN ('retiring', 'retired', 'compromised') THEN $4::timestamptz END,
			        CASE WHEN $3::text = 'compromised' THEN $4::timestamptz END)`,
			move.KeyID, jwk, string(move.To), now)
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE `+o.q()+`.signing_keys
		SET status = $3::text, state_since = $4::timestamptz, updated_at = $4::timestamptz,
		    activated_at = CASE WHEN $3::text = 'active' THEN COALESCE(activated_at, $4::timestamptz) ELSE activated_at END,
		    deactivated_at = CASE WHEN $3::text = 'active' THEN NULL
		                          WHEN $2::text = 'active' THEN $4::timestamptz ELSE deactivated_at END,
		    revoked_at = CASE WHEN $3::text = 'compromised' THEN $4::timestamptz ELSE revoked_at END
		WHERE kid = $1::text AND status = $2::text`,
		move.KeyID, string(move.From), string(move.To), now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrStateChanged
	}
	return nil
}

// sessionsToRevoke returns the live sessions that minted an access token
// while kid was active and that token can still be accepted: a session row or
// a refresh credential issued inside [activated_at, deactivated_at or now] and
// after now - RevokeWindow. A key that never signed revokes nothing.
func (o *Operator) sessionsToRevoke(ctx context.Context, db querier, kid string, now time.Time, lock bool) ([]string, error) {
	sql := `
		SELECT s.id::text
		FROM ` + o.q() + `.sessions s
		JOIN ` + o.q() + `.signing_keys k ON k.kid = $1::text AND k.activated_at IS NOT NULL
		WHERE s.revoked_at IS NULL AND (
		    (s.issued_at >= k.activated_at AND s.issued_at <= COALESCE(k.deactivated_at, $2::timestamptz)
		     AND s.issued_at > $3::timestamptz)
		    OR EXISTS (
		        SELECT 1 FROM ` + o.q() + `.refresh_credentials r
		        WHERE r.session_id = s.id
		          AND r.issued_at >= k.activated_at AND r.issued_at <= COALESCE(k.deactivated_at, $2::timestamptz)
		          AND r.issued_at > $3::timestamptz))
		ORDER BY s.id`
	if lock {
		sql += ` FOR UPDATE OF s`
	}
	rows, err := db.Query(ctx, sql, kid, now, now.Add(-RevokeWindow))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (o *Operator) revokeSessions(ctx context.Context, tx audit.TxOps, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE `+o.q()+`.refresh_credentials
		SET revoked_at = clock_timestamp(), revoked_reason = 'compromised', updated_at = clock_timestamp()
		WHERE session_id = ANY($1::uuid[]) AND revoked_at IS NULL`, ids); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		UPDATE `+o.q()+`.sessions
		SET revoked_at = clock_timestamp(), revoked_reason = 'compromised', updated_at = clock_timestamp()
		WHERE id = ANY($1::uuid[]) AND revoked_at IS NULL`, ids)
	return err
}

var eventNames = map[Verb]string{
	VerbAdopt: "adopted", VerbRegister: "registered", VerbActivate: "activated",
	VerbRetire: "retired", VerbRollback: "rolled_back", VerbCompromise: "compromised",
}

// appliedEvent is the one audit row of a verb: the key it names (adopt: the
// active key), its previous and new state, and the other key it moved.
func appliedEvent(request Request, moves []Move) audit.AuditEvent {
	attributes := map[string]any{"target_kind": "signing_key"}
	primary := request.KeyID
	if request.Verb == VerbAdopt {
		for _, move := range moves {
			if move.To == token.StateActive {
				primary = move.KeyID
			}
		}
	}
	for _, move := range moves {
		if move.KeyID != primary {
			attributes["related_key_id"] = move.KeyID
			continue
		}
		attributes["key_id"] = move.KeyID
		attributes["new_status"] = string(move.To)
		if move.From != "" {
			attributes["previous_status"] = string(move.From)
		}
	}
	if request.Verb == VerbAdopt {
		delete(attributes, "related_key_id")
	}
	if request.Verb == VerbCompromise {
		attributes["reason_code"] = "compromised"
	}
	return audit.AuditEvent{
		EventType:  "auth.signing_key." + eventNames[request.Verb],
		Outcome:    audit.OutcomeAllowed,
		Attributes: attributes,
	}
}

func refusedEvent(request Request, reason Reason) audit.AuditEvent {
	attributes := map[string]any{"target_kind": "signing_key", "rotation_refusal": string(reason)}
	if _, known := eventNames[request.Verb]; known {
		attributes["rotation_verb"] = string(request.Verb)
	}
	if token.ValidKeyID(request.KeyID) {
		attributes["key_id"] = request.KeyID
	}
	return audit.AuditEvent{EventType: audit.EventSigningKeyRefused, Outcome: audit.OutcomeDenied, Attributes: attributes}
}

// String renders a move for an operator: kid and states only.
func (m Move) String() string {
	from := string(m.From)
	if from == "" {
		from = "(new)"
	}
	return fmt.Sprintf("%s: %s -> %s", m.KeyID, from, m.To)
}
