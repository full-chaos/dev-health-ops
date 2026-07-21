// Package joboutbox implements the generic Python-to-River transactional bridge.
package joboutbox

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidConfiguration = errors.New("invalid worker outbox configuration")
	ErrUnavailable          = errors.New("worker outbox database unavailable")
	ErrLeaseLost            = errors.New("worker outbox claim is no longer owned")
	ErrContractRejected     = errors.New("worker outbox contract rejected")
	ErrPolicyRejected       = errors.New("worker outbox policy rejected")
	ErrRiverInsert          = errors.New("worker outbox River insert failed")
	errInjectedCrash        = errors.New("injected worker outbox crash")
)

const (
	statusPending   = "pending"
	statusClaimed   = "claimed"
	statusDelivered = "delivered"
	statusDead      = "dead"
)

var (
	uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	hashPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// Row is a bounded snapshot. Args may only be decoded through jobcontract.
type Row struct {
	ID              string
	DedupeKey       string
	JobKind         string
	ContractVersion int
	Args            json.RawMessage
	PayloadHash     string
	Queue           string
	Priority        int
	MaxAttempts     int
	ScheduledAt     time.Time
	Status          string
	ClaimToken      string
	ClaimedAt       *time.Time
	ClaimExpiresAt  *time.Time
	AttemptCount    int
	FirstAttemptAt  *time.Time
	LastAttemptAt   *time.Time
	NextAttemptAt   time.Time
	LastErrorCode   *string
	LastErrorDetail *string
	LastErrorAt     *time.Time
	RiverJobID      *int64
	DeliveredAt     *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Claim is the opaque capability returned by ClaimDue.
type Claim struct {
	Row
}

// PolicyRegistry supplies immutable checked-in routing policy.
type PolicyRegistry interface {
	Descriptor(kind string) (jobruntime.Descriptor, bool)
}

type insertFunc func(context.Context, pgx.Tx, Row) (int64, error)

type repositoryFaults struct {
	beforeInsert func() error
	afterInsert  func() error
	beforeMark   func() error
	afterMark    func() error
	afterCommit  func() error
}

type failureKind int

const (
	failureContract failureKind = iota + 1
	failurePolicy
	failureRiver
)

func failureEvidence(kind failureKind) (code, detail string, terminal bool) {
	switch kind {
	case failureContract:
		return "contract_rejected", "stored job contract was rejected", true
	case failurePolicy:
		return "policy_rejected", "stored job policy was rejected", true
	default:
		return "river_insert_failed", "queue insertion failed", false
	}
}
