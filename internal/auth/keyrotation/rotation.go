// Package keyrotation is the signing-key rotation state machine (ACP-ADR-02
// §6): the durable key states in the auth schema's signing_keys table, the
// operator verbs that move them, and the runtime reader that custody applies.
//
// Planned rotation and emergency compromise are separate verbs:
//
//   - adopt: an empty table takes custody's keys with custody's states (once).
//   - register: a custodied key -> pending (published, not signing).
//   - activate: pending -> active once it has been published for the JWKS
//     cache TTL (publish before issue); the active key -> retiring.
//   - retire: retiring -> retired once access TTL + clock skew + cache TTL have
//     passed (retire after expiry).
//   - rollback: retiring -> active (the active key -> retiring), or
//     retired -> pending (published again before it may sign).
//   - compromise: any state -> compromised at once, optionally promoting a
//     ready pending successor. Compromised is terminal: no verb, rollback or
//     custody manifest takes a key out of it (the table's trigger refuses it
//     too), and its public key is denied under every kid.
package keyrotation

import (
	"crypto/ed25519"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// PendingFloor is how long a key is published before it may sign.
const PendingFloor = token.JWKSCacheTTL

// RetiringFloor is how long a key that stopped signing stays published: every
// token it signed has expired (access TTL + skew) and every cached key set that
// held it has expired (cache TTL).
const RetiringFloor = contracts.AccessTokenLifetimeCeiling + contracts.MaxClockSkew + token.JWKSCacheTTL

// RevokeWindow is how long after its mint an access token may still be
// accepted: a compromise revokes the sessions that minted inside it.
const RevokeWindow = contracts.AccessTokenLifetimeCeiling + contracts.MaxClockSkew

// Verb is one operator verb.
type Verb string

const (
	VerbAdopt      Verb = "adopt"
	VerbRegister   Verb = "register"
	VerbActivate   Verb = "activate"
	VerbRetire     Verb = "retire"
	VerbRollback   Verb = "rollback"
	VerbCompromise Verb = "compromise"
)

// Verbs is every verb that moves a state.
var Verbs = []Verb{VerbAdopt, VerbRegister, VerbActivate, VerbRetire, VerbRollback, VerbCompromise}

// Reason is a bounded refusal reason.
type Reason string

const (
	ReasonTableNotEmpty       Reason = "table_not_empty"
	ReasonTableEmpty          Reason = "table_empty"
	ReasonKeyUnknown          Reason = "key_unknown"
	ReasonKeyNotInCustody     Reason = "key_not_in_custody"
	ReasonKeyRegistered       Reason = "key_registered"
	ReasonCustodyNotOneActive Reason = "custody_not_one_active"
	ReasonWrongState          Reason = "wrong_state"
	ReasonFloorNotReached     Reason = "floor_not_reached"
	ReasonCompromisedTerminal Reason = "compromised_terminal"
	ReasonSuccessorIneligible Reason = "successor_ineligible"
	ReasonUnknownVerb         Reason = "unknown_verb"
)

// ReasonStateChanged: the key states changed between the plan and the locked
// write (ErrStateChanged). Decide never returns it; Apply records it.
const ReasonStateChanged Reason = "state_changed"

// Reasons is every refusal reason Decide returns.
var Reasons = []Reason{
	ReasonTableNotEmpty, ReasonTableEmpty, ReasonKeyUnknown, ReasonKeyNotInCustody, ReasonKeyRegistered,
	ReasonCustodyNotOneActive, ReasonWrongState, ReasonFloorNotReached, ReasonCompromisedTerminal,
	ReasonSuccessorIneligible, ReasonUnknownVerb,
}

// Refusal is a verb the state machine refuses. Nothing is written.
type Refusal struct {
	Reason Reason
}

func (r *Refusal) Error() string { return "signing key rotation refused: " + string(r.Reason) }

func refuse(reason Reason) error { return &Refusal{Reason: reason} }

// Row is one key's durable state.
type Row struct {
	KeyID      string
	State      token.State
	PublicKey  ed25519.PublicKey
	StateSince time.Time
}

// Request is one verb. KeyID names the key it acts on (empty for adopt).
// Successor is compromise's optional key to activate. Custody is custody's
// keys, needed by adopt and register.
type Request struct {
	Verb      Verb
	KeyID     string
	Successor string
	Custody   []token.Key
}

// Move is one key's state change. From is empty for a key the verb inserts.
type Move struct {
	KeyID     string
	From      token.State
	To        token.State
	PublicKey ed25519.PublicKey
}

// Decide returns the moves request makes on rows at now, in the order they
// must be written (a key leaves active before another enters it), or a
// *Refusal.
func Decide(request Request, rows []Row, now time.Time) ([]Move, error) {
	byKid := make(map[string]Row, len(rows))
	var active *Row
	for i, row := range rows {
		byKid[row.KeyID] = row
		if row.State == token.StateActive {
			active = &rows[i]
		}
	}
	if request.Verb == VerbAdopt {
		return adopt(request.Custody, rows)
	}
	if request.Verb == VerbRegister {
		return register(request, rows)
	}
	switch request.Verb {
	case VerbActivate, VerbRetire, VerbRollback, VerbCompromise:
	default:
		return nil, refuse(ReasonUnknownVerb)
	}
	if len(rows) == 0 {
		return nil, refuse(ReasonTableEmpty)
	}
	key, ok := byKid[request.KeyID]
	if !ok {
		return nil, refuse(ReasonKeyUnknown)
	}
	if key.State == token.StateCompromised {
		return nil, refuse(ReasonCompromisedTerminal)
	}
	if request.Successor != "" && request.Verb != VerbCompromise {
		return nil, refuse(ReasonSuccessorIneligible)
	}
	switch request.Verb {
	case VerbActivate:
		if key.State != token.StatePending {
			return nil, refuse(ReasonWrongState)
		}
		if now.Sub(key.StateSince) < PendingFloor {
			return nil, refuse(ReasonFloorNotReached)
		}
		return promote(active, key), nil
	case VerbRetire:
		if key.State != token.StateRetiring {
			return nil, refuse(ReasonWrongState)
		}
		if now.Sub(key.StateSince) < RetiringFloor {
			return nil, refuse(ReasonFloorNotReached)
		}
		return []Move{{KeyID: key.KeyID, From: token.StateRetiring, To: token.StateRetired}}, nil
	case VerbRollback:
		switch key.State {
		case token.StateRetiring:
			return promote(active, key), nil
		case token.StateRetired:
			return []Move{{KeyID: key.KeyID, From: token.StateRetired, To: token.StatePending}}, nil
		}
		return nil, refuse(ReasonWrongState)
	default:
		return compromise(request, key, active, byKid, now)
	}
}

// promote makes key active, the active key (if any) retiring first.
func promote(active *Row, key Row) []Move {
	var moves []Move
	if active != nil {
		moves = append(moves, Move{KeyID: active.KeyID, From: token.StateActive, To: token.StateRetiring})
	}
	return append(moves, Move{KeyID: key.KeyID, From: key.State, To: token.StateActive})
}

func compromise(request Request, key Row, active *Row, byKid map[string]Row, now time.Time) ([]Move, error) {
	moves := []Move{{KeyID: key.KeyID, From: key.State, To: token.StateCompromised}}
	if request.Successor == "" {
		return moves, nil
	}
	successor, ok := byKid[request.Successor]
	if !ok || successor.KeyID == key.KeyID || successor.State != token.StatePending ||
		now.Sub(successor.StateSince) < PendingFloor ||
		(active != nil && active.KeyID != key.KeyID) {
		return nil, refuse(ReasonSuccessorIneligible)
	}
	return append(moves, Move{KeyID: successor.KeyID, From: token.StatePending, To: token.StateActive}), nil
}

// adopt takes custody's keys and states into an empty table: exactly one
// active key, so the first read of the table keeps the signer custody had.
func adopt(custody []token.Key, rows []Row) ([]Move, error) {
	if len(rows) != 0 {
		return nil, refuse(ReasonTableNotEmpty)
	}
	active := 0
	moves := make([]Move, 0, len(custody))
	for _, key := range custody {
		if key.State == token.StateActive {
			active++
		}
		moves = append(moves, Move{KeyID: key.KeyID, To: key.State, PublicKey: key.PublicKey})
	}
	if active != 1 {
		return nil, refuse(ReasonCustodyNotOneActive)
	}
	return moves, nil
}

// register adds one custodied key as pending. A kid or public key the table
// already holds, in any state, is refused: a kid is never reused and a
// compromised public key never returns under a new kid.
func register(request Request, rows []Row) ([]Move, error) {
	if len(rows) == 0 {
		return nil, refuse(ReasonTableEmpty)
	}
	if request.Successor != "" {
		return nil, refuse(ReasonSuccessorIneligible)
	}
	var key *token.Key
	for i := range request.Custody {
		if request.Custody[i].KeyID == request.KeyID {
			key = &request.Custody[i]
		}
	}
	if key == nil {
		return nil, refuse(ReasonKeyNotInCustody)
	}
	if key.State == token.StateCompromised {
		return nil, refuse(ReasonWrongState)
	}
	for _, row := range rows {
		if row.KeyID == key.KeyID || row.PublicKey.Equal(key.PublicKey) {
			return nil, refuse(ReasonKeyRegistered)
		}
	}
	return []Move{{KeyID: key.KeyID, To: token.StatePending, PublicKey: key.PublicKey}}, nil
}
