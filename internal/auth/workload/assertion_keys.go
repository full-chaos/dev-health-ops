package workload

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// AssertionKeyStatus is where a signing key is in its life. A key signs and
// verifies while active, only verifies while retiring (until retire_after) and
// verifies nothing once retired. Retired is terminal.
type AssertionKeyStatus string

const (
	AssertionKeyActive   AssertionKeyStatus = "active"
	AssertionKeyRetiring AssertionKeyStatus = "retiring"
	AssertionKeyRetired  AssertionKeyStatus = "retired"
)

// AssertionOverlapCeiling bounds how long the outgoing key verifies after the
// incoming one is rotated in: a retiring key's retire_after is at most the
// active key's rotated_at plus this.
const AssertionOverlapCeiling = 24 * time.Hour

const (
	maxAssertionIssuerBytes = 512
	maxAssertionKIDBytes    = 256
	verbSetAssertionKey     = verb("set_assertion_key")
)

// AssertionKeySpec is the public metadata of one signing key of a workload in
// this deployment. It names a key and never holds one.
type AssertionKeySpec struct {
	Issuer string
	KID    string
	Status AssertionKeyStatus
	// RotatedAt is when the key was put in service. It never changes.
	RotatedAt time.Time
	// RetireAfter is when a retiring key stops verifying. Present exactly while
	// the key is retiring.
	RetireAfter time.Time
	// RetirePreviousAfter is only for a new active key while another is active:
	// the active one becomes retiring with retire_after = RotatedAt + this, in
	// the same transaction. At most AssertionOverlapCeiling.
	RetirePreviousAfter time.Duration
}

// AssertionKey is a stored key row, for the operator list.
type AssertionKey struct {
	ID          uuid.UUID
	Workload    string
	Issuer      string
	KID         string
	Status      AssertionKeyStatus
	RotatedAt   time.Time
	RetireAfter *time.Time
}

// isAssertionKeyRefusal is a rule of the assertion key verbs refusing a change,
// not a store failure.
func isAssertionKeyRefusal(err error) bool {
	for _, refusal := range []error{ErrAssertionKeyInvalid, ErrAssertionKeyActiveExists, ErrAssertionKeyTerminal,
		ErrAssertionKeyTransition, ErrAssertionKeyOverlap, ErrAssertionKeyNoActive, ErrAssertionKeyLastActive, ErrAssertionKeyWorkloadDisabled} {
		if errors.Is(err, refusal) {
			return true
		}
	}
	return false
}

func assertionInvalid(why string) error { return fmt.Errorf("%w: %s", ErrAssertionKeyInvalid, why) }

func isLoopbackHost(host string) bool {
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	return false
}

func printableASCII(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] < 0x21 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

// ValidateAssertionKey refuses a spec the verb would not write. Its errors name
// the field and never carry the input.
func ValidateAssertionKey(spec AssertionKeySpec) error {
	if len(spec.Issuer) > maxAssertionIssuerBytes || !printableASCII(spec.Issuer) || spec.Issuer == "" {
		return assertionInvalid("the issuer is empty, too long or not printable ASCII")
	}
	issuer, err := url.Parse(spec.Issuer)
	if err != nil || issuer.Host == "" || issuer.User != nil || issuer.RawQuery != "" || issuer.ForceQuery || issuer.Fragment != "" ||
		strings.Contains(spec.Issuer, "#") {
		return assertionInvalid("the issuer is not a plain URL (no userinfo, query or fragment)")
	}
	switch issuer.Scheme {
	case "https":
	case "http":
		if !isLoopbackHost(issuer.Hostname()) {
			return assertionInvalid("the issuer is plain http outside a loopback host")
		}
	default:
		return assertionInvalid("the issuer scheme is not https")
	}
	if len(spec.KID) == 0 || len(spec.KID) > maxAssertionKIDBytes || !printableASCII(spec.KID) {
		return assertionInvalid("the kid is empty, longer than 256 bytes or not printable non-space ASCII")
	}
	if credshape.AnyCredentialShaped(spec.Issuer, spec.KID) {
		return assertionInvalid("the issuer or the kid has the shape of a credential or a key")
	}
	switch spec.Status {
	case AssertionKeyActive, AssertionKeyRetiring, AssertionKeyRetired:
	default:
		return assertionInvalid("the status is not active, retiring or retired")
	}
	if spec.RotatedAt.IsZero() {
		return assertionInvalid("rotated_at is required")
	}
	if spec.Status == AssertionKeyRetiring {
		if spec.RetireAfter.IsZero() || !spec.RetireAfter.After(spec.RotatedAt) {
			return assertionInvalid("a retiring key needs retire_after after rotated_at")
		}
	} else if !spec.RetireAfter.IsZero() {
		return assertionInvalid("retire_after belongs to a retiring key only")
	}
	if spec.RetirePreviousAfter != 0 {
		if spec.Status != AssertionKeyActive {
			return assertionInvalid("retire-previous belongs to a new active key only")
		}
		if spec.RetirePreviousAfter < 0 || spec.RetirePreviousAfter > AssertionOverlapCeiling {
			return assertionInvalid("retire-previous is outside (0, 24h]")
		}
	}
	return nil
}

type assertionRow struct {
	Status      AssertionKeyStatus
	RotatedAt   time.Time
	RetireAfter time.Time
}

type planAction int

const (
	planNoop planAction = iota
	planInsert
	planUpdate
)

type assertionPlan struct {
	Action planAction
	// DemotePreviousUntil, when set, moves the currently active key to retiring
	// with this retire_after, before the insert.
	DemotePreviousUntil time.Time
}

func sameInstant(a, b time.Time) bool {
	return a.Truncate(time.Microsecond).Equal(b.Truncate(time.Microsecond))
}

// planAssertionKey decides what a validated spec does to the stored state.
// existing is the row with the spec's (issuer, kid), if any; active is the
// workload's active key, if any (it may be existing).
func planAssertionKey(spec AssertionKeySpec, existing, active *assertionRow) (assertionPlan, error) {
	activeOther := active
	if existing != nil && active != nil && existing.Status == AssertionKeyActive {
		activeOther = nil
	}
	if existing == nil {
		switch spec.Status {
		case AssertionKeyRetired:
			return assertionPlan{}, assertionInvalid("a key is registered active or retiring, never retired")
		case AssertionKeyRetiring:
			if activeOther == nil {
				return assertionPlan{}, ErrAssertionKeyNoActive
			}
			if spec.RetireAfter.After(activeOther.RotatedAt.Add(AssertionOverlapCeiling)) {
				return assertionPlan{}, ErrAssertionKeyOverlap
			}
			return assertionPlan{Action: planInsert}, nil
		}
		if activeOther == nil {
			if spec.RetirePreviousAfter != 0 {
				return assertionPlan{}, assertionInvalid("retire-previous names no active key to retire")
			}
			return assertionPlan{Action: planInsert}, nil
		}
		if spec.RetirePreviousAfter == 0 {
			return assertionPlan{}, ErrAssertionKeyActiveExists
		}
		if !spec.RotatedAt.After(activeOther.RotatedAt) {
			return assertionPlan{}, ErrAssertionKeyTransition
		}
		return assertionPlan{Action: planInsert, DemotePreviousUntil: spec.RotatedAt.Add(spec.RetirePreviousAfter)}, nil
	}
	if spec.RetirePreviousAfter != 0 {
		return assertionPlan{}, assertionInvalid("retire-previous belongs to a new active key only")
	}
	if existing.Status == AssertionKeyRetired {
		if spec.Status == AssertionKeyRetired && sameInstant(spec.RotatedAt, existing.RotatedAt) {
			return assertionPlan{Action: planNoop}, nil
		}
		return assertionPlan{}, ErrAssertionKeyTerminal
	}
	if !sameInstant(spec.RotatedAt, existing.RotatedAt) {
		return assertionPlan{}, ErrAssertionKeyTransition
	}
	switch spec.Status {
	case AssertionKeyActive:
		if existing.Status == AssertionKeyActive {
			return assertionPlan{Action: planNoop}, nil
		}
		return assertionPlan{}, ErrAssertionKeyTransition
	case AssertionKeyRetired:
		if existing.Status == AssertionKeyActive {
			return assertionPlan{}, ErrAssertionKeyLastActive
		}
		return assertionPlan{Action: planUpdate}, nil
	}
	if activeOther == nil {
		return assertionPlan{}, ErrAssertionKeyNoActive
	}
	if spec.RetireAfter.After(activeOther.RotatedAt.Add(AssertionOverlapCeiling)) {
		return assertionPlan{}, ErrAssertionKeyOverlap
	}
	if existing.Status == AssertionKeyRetiring && sameInstant(spec.RetireAfter, existing.RetireAfter) {
		return assertionPlan{Action: planNoop}, nil
	}
	return assertionPlan{Action: planUpdate}, nil
}

// SetAssertionKey records or moves one signing key of a workload. It validates
// every rule before it writes, commits through audit.Commit with one audit row
// (no provisioning event: nothing reads one), and returns the key's row id. A repeat that
// changes nothing writes nothing.
func (s *Store) SetAssertionKey(ctx context.Context, name string, spec AssertionKeySpec, a Actor) (uuid.UUID, error) {
	spec.RotatedAt = spec.RotatedAt.UTC().Truncate(time.Microsecond)
	spec.RetireAfter = spec.RetireAfter.UTC().Truncate(time.Microsecond)
	if err := ValidateAssertionKey(spec); err != nil {
		s.record(ctx, verbSetAssertionKey, err)
		return uuid.Nil, err
	}
	info, err := s.Lookup(ctx, name)
	if err != nil {
		s.record(ctx, verbSetAssertionKey, err)
		return uuid.Nil, err
	}
	// The audit row names the key, so its id is read before the transaction and
	// re-checked under the row locks: a change in between is a transition error.
	id := uuid.New()
	var knownID uuid.UUID
	switch err := s.pool.QueryRow(ctx, `SELECT id FROM `+s.q+`.workload_assertion_keys
WHERE workload_principal_id = $1 AND issuer = $2 AND kid = $3`, info.PrincipalID, spec.Issuer, spec.KID).Scan(&knownID); {
	case err == nil:
		id = knownID
	case !errors.Is(err, pgx.ErrNoRows):
		s.logger.ErrorContext(ctx, "workload: read failure", logging.ErrorAttr(err))
		err = fmt.Errorf("%w: read: %w", ErrStore, err)
		s.record(ctx, verbSetAssertionKey, err)
		return uuid.Nil, err
	}
	err = s.run(ctx, verbSetAssertionKey, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, _, disabled, err := s.lockWorkload(ctx, tx, info.PrincipalID)
			if err != nil {
				return err
			}
			if disabled {
				return ErrAssertionKeyWorkloadDisabled
			}
			existing, existingID, err := s.readAssertionRow(ctx, tx, info.PrincipalID, &spec)
			if err != nil {
				return err
			}
			active, activeID, err := s.readAssertionRow(ctx, tx, info.PrincipalID, nil)
			if err != nil {
				return err
			}
			plan, err := planAssertionKey(spec, existing, active)
			if err != nil {
				return err
			}
			if (existing != nil) != (knownID != uuid.Nil) || existing != nil && existingID != knownID {
				return ErrAssertionKeyTransition
			}
			switch plan.Action {
			case planNoop:
				return nil
			case planUpdate:
				var retire *time.Time
				if spec.Status == AssertionKeyRetiring {
					retire = &spec.RetireAfter
				}
				_, err = tx.Exec(ctx, `UPDATE `+s.q+`.workload_assertion_keys SET status = $2, retire_after = $3, updated_at = clock_timestamp()
WHERE id = $1`, existingID, string(spec.Status), retire)
				return err
			}
			if !plan.DemotePreviousUntil.IsZero() {
				if _, err = tx.Exec(ctx, `UPDATE `+s.q+`.workload_assertion_keys SET status = 'retiring', retire_after = $2, updated_at = clock_timestamp()
WHERE id = $1`, activeID, plan.DemotePreviousUntil.UTC().Truncate(time.Microsecond)); err != nil {
					return err
				}
			}
			var retire *time.Time
			if spec.Status == AssertionKeyRetiring {
				retire = &spec.RetireAfter
			}
			_, err = tx.Exec(ctx, `INSERT INTO `+s.q+`.workload_assertion_keys
(id, workload_principal_id, issuer, kid, status, rotated_at, retire_after) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				id, info.PrincipalID, spec.Issuer, spec.KID, string(spec.Status), spec.RotatedAt, retire)
			return err
		},
		Audit:     s.assertionKeyAudit(info.PrincipalID, id, spec.Status, a),
		OffStream: audit.OffStreamWorkloadRegistry,
	})
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func (s *Store) assertionKeyAudit(workloadID, keyID uuid.UUID, status AssertionKeyStatus, a Actor) audit.AuditEvent {
	ev := s.auditFor("auth.workload.assertion_key_set", "", workloadID, a, []string{"assertion_key"}, "", string(status))
	ev.Attributes["assertion_key_id"] = keyID.String()
	return ev
}

// readAssertionRow reads and locks the workload's key with the spec's identity
// (when spec is set) or its active key (when it is nil).
func (s *Store) readAssertionRow(ctx context.Context, tx audit.TxOps, workloadID uuid.UUID, spec *AssertionKeySpec) (*assertionRow, uuid.UUID, error) {
	var (
		id     uuid.UUID
		status string
		rot    time.Time
		retire *time.Time
		err    error
	)
	if spec != nil {
		err = tx.QueryRow(ctx, `SELECT id, status, rotated_at, retire_after FROM `+s.q+`.workload_assertion_keys
WHERE workload_principal_id = $1 AND issuer = $2 AND kid = $3 FOR UPDATE`, workloadID, spec.Issuer, spec.KID).Scan(&id, &status, &rot, &retire)
	} else {
		err = tx.QueryRow(ctx, `SELECT id, status, rotated_at, retire_after FROM `+s.q+`.workload_assertion_keys
WHERE workload_principal_id = $1 AND status = 'active' FOR UPDATE`, workloadID).Scan(&id, &status, &rot, &retire)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, uuid.Nil, nil
	}
	if err != nil {
		return nil, uuid.Nil, err
	}
	row := &assertionRow{Status: AssertionKeyStatus(status), RotatedAt: rot}
	if retire != nil {
		row.RetireAfter = *retire
	}
	return row, id, nil
}

// ListAssertionKeys returns the key metadata of one workload, or of every
// workload when name is empty. Metadata only: there is no key to return.
func (s *Store) ListAssertionKeys(ctx context.Context, name string) ([]AssertionKey, error) {
	query := `SELECT k.id, w.name, k.issuer, k.kid, k.status, k.rotated_at, k.retire_after
FROM ` + s.q + `.workload_assertion_keys k JOIN ` + s.q + `.workloads w ON w.principal_id = k.workload_principal_id`
	args := []any{}
	if name != "" {
		if err := ValidateName(name); err != nil {
			return nil, err
		}
		query += ` WHERE w.name = $1`
		args = append(args, name)
	}
	rows, err := s.pool.Query(ctx, query+` ORDER BY w.name, k.status, k.rotated_at DESC, k.kid`, args...)
	if err != nil {
		s.logger.ErrorContext(ctx, "workload: read failure", logging.ErrorAttr(err))
		return nil, fmt.Errorf("%w: read: %w", ErrStore, err)
	}
	defer rows.Close()
	var out []AssertionKey
	for rows.Next() {
		var key AssertionKey
		var status string
		if err := rows.Scan(&key.ID, &key.Workload, &key.Issuer, &key.KID, &status, &key.RotatedAt, &key.RetireAfter); err != nil {
			return nil, fmt.Errorf("%w: read: %w", ErrStore, err)
		}
		key.Status = AssertionKeyStatus(status)
		out = append(out, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: read: %w", ErrStore, err)
	}
	return out, nil
}
