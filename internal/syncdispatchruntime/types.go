// Package syncdispatchruntime contains dormant, typed River transport
// primitives for the frozen sync-dispatch v1 contract. It has no command,
// route, or worker registration side effects.
package syncdispatchruntime

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/riverqueue/river"
)

const ContractVersionV1 = 1

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

var (
	ErrInvalidClaim     = errors.New("invalid sync dispatch transport claim")
	ErrInvalidReference = errors.New("invalid sync dispatch domain reference")
)

// Claim is the non-sensitive projection of a live transport claim. In
// particular, it deliberately excludes the claim token: that lease secret is
// only for the reconciler's same-transaction terminal write and must never be
// serialized into River arguments.
type Claim struct {
	OutboxID        string
	Kind            string
	RouteGeneration int64
}

// DomainReference is the authoritative, tenant-scoped domain identity looked
// up by a caller that owns the semantic database boundary. Queue arguments do
// not replace the sync run as the source of truth.
type DomainReference struct {
	OrganizationID string
	SyncRunID      string
}

// Args is the exact versioned River argument shape shared by all four frozen
// dispatch kinds. Concrete kind types prevent an accidental kind/argument
// mismatch at the publisher boundary.
type Args interface {
	river.JobArgs
	ContractVersion() int
	OutboxID() string
	OrganizationID() string
	SyncRunID() string
	RouteGeneration() int64
	valid() error
}

// TransportArgs is the common v1 encoded argument envelope. It has no
// arbitrary payload field and no claim token, provider credential, or raw
// domain record.
type TransportArgs struct {
	Version         int    `json:"contract_version"`
	OrgID           string `json:"organization_id"`
	RunID           string `json:"sync_run_id"`
	DispatchOutbox  string `json:"outbox_id" river:"unique"`
	RouteGeneration int64  `json:"route_generation"`
}

func (args TransportArgs) ContractVersion() int   { return args.Version }
func (args TransportArgs) OutboxID() string       { return args.DispatchOutbox }
func (args TransportArgs) OrganizationID() string { return args.OrgID }
func (args TransportArgs) SyncRunID() string      { return args.RunID }
func (args TransportArgs) Generation() int64      { return args.RouteGeneration }

func (args TransportArgs) valid() error {
	if args.Version != ContractVersionV1 || !uuidPattern.MatchString(args.OrgID) ||
		!uuidPattern.MatchString(args.RunID) || !uuidPattern.MatchString(args.DispatchOutbox) ||
		args.RouteGeneration < 1 {
		return ErrInvalidReference
	}
	return nil
}

// DispatchSyncRunArgs is the exact v1 River argument type for dispatch_sync_run.
type DispatchSyncRunArgs struct{ TransportArgs }

func (DispatchSyncRunArgs) Kind() string                { return syncdispatchcontract.KindDispatchSyncRun }
func (args DispatchSyncRunArgs) RouteGeneration() int64 { return args.Generation() }
func (args DispatchSyncRunArgs) valid() error           { return args.TransportArgs.valid() }

// FinalizeSyncRunArgs is the exact v1 River argument type for finalize_sync_run.
type FinalizeSyncRunArgs struct{ TransportArgs }

func (FinalizeSyncRunArgs) Kind() string                { return syncdispatchcontract.KindFinalizeSyncRun }
func (args FinalizeSyncRunArgs) RouteGeneration() int64 { return args.Generation() }
func (args FinalizeSyncRunArgs) valid() error           { return args.TransportArgs.valid() }

// PostSyncArgs is the exact v1 argument type for guarded at-least-once
// post_sync delivery. Creating it does not publish a message or execute an
// external effect.
type PostSyncArgs struct{ TransportArgs }

func (PostSyncArgs) Kind() string                { return syncdispatchcontract.KindPostSync }
func (args PostSyncArgs) RouteGeneration() int64 { return args.Generation() }
func (args PostSyncArgs) valid() error           { return args.TransportArgs.valid() }

// ReferenceDiscoveryArgs is the exact v1 River argument type for
// reference_discovery.
type ReferenceDiscoveryArgs struct{ TransportArgs }

func (ReferenceDiscoveryArgs) Kind() string                { return syncdispatchcontract.KindReferenceDiscovery }
func (args ReferenceDiscoveryArgs) RouteGeneration() int64 { return args.Generation() }
func (args ReferenceDiscoveryArgs) valid() error           { return args.TransportArgs.valid() }

// Convert strictly projects a live claim plus authoritative domain reference
// into one concrete v1 argument type. It never reads or logs payload data.
func Convert(claim Claim, reference DomainReference) (Args, error) {
	if !uuidPattern.MatchString(claim.OutboxID) || claim.RouteGeneration < 1 {
		return nil, ErrInvalidClaim
	}
	base := TransportArgs{
		Version:         ContractVersionV1,
		OrgID:           reference.OrganizationID,
		RunID:           reference.SyncRunID,
		DispatchOutbox:  claim.OutboxID,
		RouteGeneration: claim.RouteGeneration,
	}
	if err := base.valid(); err != nil {
		return nil, err
	}
	switch claim.Kind {
	case syncdispatchcontract.KindDispatchSyncRun:
		return DispatchSyncRunArgs{TransportArgs: base}, nil
	case syncdispatchcontract.KindFinalizeSyncRun:
		return FinalizeSyncRunArgs{TransportArgs: base}, nil
	case syncdispatchcontract.KindPostSync:
		return PostSyncArgs{TransportArgs: base}, nil
	case syncdispatchcontract.KindReferenceDiscovery:
		return ReferenceDiscoveryArgs{TransportArgs: base}, nil
	default:
		return nil, fmt.Errorf("%w: unknown kind", ErrInvalidClaim)
	}
}
