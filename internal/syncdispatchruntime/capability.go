package syncdispatchruntime

import (
	"context"
	"errors"
	"reflect"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
)

var ErrCapabilityUnavailable = errors.New("sync dispatch runtime capability is unavailable")

// The four typed handler interfaces intentionally contain no generic fallback.
// A future composition must provide a concrete implementation for each kind
// before it can advertise executable River readiness.
type DispatchSyncRunHandler interface {
	Work(context.Context, DispatchSyncRunArgs) error
}

type FinalizeSyncRunHandler interface {
	Work(context.Context, FinalizeSyncRunArgs) error
}

type PostSyncHandler interface {
	Work(context.Context, PostSyncArgs) error
}

type ReferenceDiscoveryHandler interface {
	Work(context.Context, ReferenceDiscoveryArgs) error
}

type DispatchSyncRunHandlerFunc func(context.Context, DispatchSyncRunArgs) error

func (function DispatchSyncRunHandlerFunc) Work(ctx context.Context, args DispatchSyncRunArgs) error {
	return function(ctx, args)
}

type FinalizeSyncRunHandlerFunc func(context.Context, FinalizeSyncRunArgs) error

func (function FinalizeSyncRunHandlerFunc) Work(ctx context.Context, args FinalizeSyncRunArgs) error {
	return function(ctx, args)
}

type PostSyncHandlerFunc func(context.Context, PostSyncArgs) error

func (function PostSyncHandlerFunc) Work(ctx context.Context, args PostSyncArgs) error {
	return function(ctx, args)
}

type ReferenceDiscoveryHandlerFunc func(context.Context, ReferenceDiscoveryArgs) error

func (function ReferenceDiscoveryHandlerFunc) Work(ctx context.Context, args ReferenceDiscoveryArgs) error {
	return function(ctx, args)
}

// PostSyncHandoff remains separate because post_sync is mark-before and its
// external effect must happen after the terminal transaction commits. It is
// deliberately not implemented by this package.
type PostSyncHandoff interface {
	Handoff(context.Context, PostSyncArgs) error
}

type Handlers struct {
	DispatchSyncRun    DispatchSyncRunHandler
	FinalizeSyncRun    FinalizeSyncRunHandler
	PostSync           PostSyncHandler
	ReferenceDiscovery ReferenceDiscoveryHandler
	PostSyncHandoff    PostSyncHandoff
}

// CapabilityDescriptor is payload-free composition evidence. It records only
// retained, typed registrations; it is not a route-activation or cross-process
// executable-readiness claim.
type CapabilityDescriptor struct {
	Kind              string
	Transport         string
	ContractVersion   int
	HandlerRegistered bool
	PublisherBound    bool
	HandoffBound      bool
}

// Capabilities is an immutable in-process composition artifact. It does not
// alter a route and intentionally exposes no syncroute projection: a
// cross-process cutover needs separately proven control-plane composition.
type Capabilities struct {
	descriptors []CapabilityDescriptor
	quiescer    *GenerationTracker
	handlers    Handlers
	postSync    PostSyncHandoff
}

func NewCapabilities(publisher *Publisher, handlers Handlers, quiescer *GenerationTracker) (*Capabilities, error) {
	if !publisher.valid() || !quiescer.valid() || !present(handlers.DispatchSyncRun) ||
		!present(handlers.FinalizeSyncRun) || !present(handlers.PostSync) ||
		!present(handlers.ReferenceDiscovery) || !present(handlers.PostSyncHandoff) {
		return nil, ErrCapabilityUnavailable
	}
	descriptors := []CapabilityDescriptor{
		{Kind: syncdispatchcontract.KindDispatchSyncRun, Transport: syncdispatchcontract.RouteRiver, ContractVersion: ContractVersionV1, HandlerRegistered: true, PublisherBound: true},
		{Kind: syncdispatchcontract.KindFinalizeSyncRun, Transport: syncdispatchcontract.RouteRiver, ContractVersion: ContractVersionV1, HandlerRegistered: true, PublisherBound: true},
		{Kind: syncdispatchcontract.KindPostSync, Transport: syncdispatchcontract.RouteRiver, ContractVersion: ContractVersionV1, HandlerRegistered: true, HandoffBound: true},
		{Kind: syncdispatchcontract.KindReferenceDiscovery, Transport: syncdispatchcontract.RouteRiver, ContractVersion: ContractVersionV1, HandlerRegistered: true, PublisherBound: true},
	}
	sort.Slice(descriptors, func(left, right int) bool { return descriptors[left].Kind < descriptors[right].Kind })
	return &Capabilities{
		descriptors: descriptors,
		quiescer:    quiescer,
		handlers:    handlers,
		postSync:    handlers.PostSyncHandoff,
	}, nil
}

func present(value any) bool {
	if value == nil {
		return false
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return !reflected.IsNil()
	default:
		return true
	}
}

func (capabilities *Capabilities) Descriptors() []CapabilityDescriptor {
	if capabilities == nil {
		return nil
	}
	return append([]CapabilityDescriptor(nil), capabilities.descriptors...)
}

// DispatchSyncRun invokes the concrete, retained registration for its exact
// argument type. Composition must bind this typed seam into a River worker
// before it may use the descriptor as executable readiness evidence.
func (capabilities *Capabilities) DispatchSyncRun(ctx context.Context, args DispatchSyncRunArgs) error {
	if capabilities == nil || !present(capabilities.handlers.DispatchSyncRun) || ctx == nil || args.valid() != nil {
		return ErrCapabilityUnavailable
	}
	return capabilities.handlers.DispatchSyncRun.Work(ctx, args)
}

func (capabilities *Capabilities) FinalizeSyncRun(ctx context.Context, args FinalizeSyncRunArgs) error {
	if capabilities == nil || !present(capabilities.handlers.FinalizeSyncRun) || ctx == nil || args.valid() != nil {
		return ErrCapabilityUnavailable
	}
	return capabilities.handlers.FinalizeSyncRun.Work(ctx, args)
}

func (capabilities *Capabilities) PostSync(ctx context.Context, args PostSyncArgs) error {
	if capabilities == nil || !present(capabilities.handlers.PostSync) || ctx == nil || args.valid() != nil {
		return ErrCapabilityUnavailable
	}
	return capabilities.handlers.PostSync.Work(ctx, args)
}

func (capabilities *Capabilities) ReferenceDiscovery(ctx context.Context, args ReferenceDiscoveryArgs) error {
	if capabilities == nil || !present(capabilities.handlers.ReferenceDiscovery) || ctx == nil || args.valid() != nil {
		return ErrCapabilityUnavailable
	}
	return capabilities.handlers.ReferenceDiscovery.Work(ctx, args)
}

// HandoffPostSync runs the supplied concrete external handoff inside the
// tracker-managed pre-publish window. It is intentionally separate from
// Publisher.Publish: callers may invoke it only after the mark-before terminal
// transaction committed. The tracker covers only handoffs issued through this
// process; it is not a syncroute capability or a cross-process cutover barrier.
func (capabilities *Capabilities) HandoffPostSync(ctx context.Context, args PostSyncArgs) error {
	if capabilities == nil || capabilities.quiescer == nil || !present(capabilities.postSync) || ctx == nil || args.valid() != nil {
		return ErrCapabilityUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	leave, err := capabilities.quiescer.EnterLocalHandoff(args.RouteGeneration())
	if err != nil {
		return err
	}
	defer leave()
	return capabilities.postSync.Handoff(ctx, args)
}
