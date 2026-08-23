package providersync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// ErrLinearWorkItemSinkIncomplete names a provider-local construction gap.
// A six-fact or ten-derived partial sink must never be handed to the one
// sixteen-effect family committer.
var ErrLinearWorkItemSinkIncomplete = errors.New(
	"linear work-item clickhouse sink is missing destination adapters",
)

// LinearWorkItemFamilyRouteHandler composes the existing authoritative Linear
// collector with the existing ten-destination derivation boundary. It owns no
// registry, alias reconciliation, activation, or worker construction.
//
// Direct retains the reference team/sprint inputs used by the raw collector.
// A caller may therefore inject a separately collected reference catalog
// without adding its five prerequisite effects to this work-item manifest.
type LinearWorkItemFamilyRouteHandler struct {
	Direct  LinearWorkItemsRouteHandler
	Derived *LinearWorkItemDeriver
}

func (handler LinearWorkItemFamilyRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	// Construction defects are rejected before the raw collector can perform
	// provider I/O. Production can install a deriver only through its governed
	// config-loading constructor; package tests retain the private seams needed
	// to prove derivation failure isolation.
	if handler.Derived == nil || handler.Derived.Source == nil ||
		handler.Derived.engine == nil ||
		!linearWorkItemsFlag(handler.Direct.FetchComments) ||
		!linearWorkItemsFlag(handler.Direct.FetchHistory) ||
		!linearWorkItemsFlag(handler.Direct.FetchCycles) {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	raw, err := handler.Direct.Collect(
		ctx, claim, credential, client, normalizedAt,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if raw.Watermark == nil || claim.BeforeAt == nil ||
		!raw.Watermark.Equal(claim.BeforeAt.UTC()) {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	if err := raw.validate(CompleteRouteDescriptor{
		Destinations: linearWorkItemEffectDestinations,
	}); err != nil {
		return CompleteRouteBatch{}, err
	}
	for _, effect := range raw.Effects {
		if effect.Recovery != EffectReadbackRequired {
			return CompleteRouteBatch{}, ErrInvalidConfiguration
		}
	}
	typed, err := decodeLinearWorkItemsRouteBatch(raw)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	rows := linearWorkItemRows{
		WorkItems: typed.WorkItems, StatusTransitions: typed.StatusTransitions,
		Dependencies: typed.Dependencies, ReopenEvents: typed.ReopenEvents,
		Interactions: typed.Interactions, Sprints: typed.Sprints,
	}
	derived, err := handler.Derived.Derive(ctx, claim, rows, normalizedAt)
	if err != nil {
		// No raw effects or watermark escape when the governed derived family
		// is unavailable. Collection has no persistence side effect, so the
		// caller has nothing it can partially commit.
		return CompleteRouteBatch{}, err
	}
	derivedEffects, err := buildLinearWorkItemDerivedEffectsFromMap(derived)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	effects := make([]EffectBatch, 0, len(raw.Effects)+len(derivedEffects))
	effects = append(effects, raw.Effects...)
	effects = append(effects, derivedEffects...)
	sortEffectBatches(effects)
	raw.Effects = effects
	if err := raw.validate(CompleteRouteDescriptor{
		Destinations: workItemRouteDestinations(),
	}); err != nil {
		return CompleteRouteBatch{}, err
	}
	raw.Result = attachWorkItemTeamInheritanceObservation(raw.Result, handler.Derived)
	return raw, nil
}

// LinearWorkItemFamilyClickHouseEffects is the one complete sink/readback
// boundary accepted by the family committer. Raw and Derived remain visible
// for construction diagnostics and provider-local tests, while every
// EffectSink/EffectReadback call first verifies both halves are complete.
type LinearWorkItemFamilyClickHouseEffects struct {
	Raw     LinearWorkItemClickHouseEffects
	Derived LinearWorkItemDerivedClickHouseEffects
}

func NewLinearWorkItemFamilyClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
) (LinearWorkItemFamilyClickHouseEffects, error) {
	if conn == nil || lease == nil {
		return LinearWorkItemFamilyClickHouseEffects{}, ErrInvalidConfiguration
	}
	derived, err := NewLinearWorkItemDerivedClickHouseEffects(conn, lease)
	if err != nil {
		return LinearWorkItemFamilyClickHouseEffects{}, err
	}
	sink := LinearWorkItemFamilyClickHouseEffects{
		Raw: LinearWorkItemClickHouseEffects{
			Lease:             lease,
			WorkItems:         LinearWorkItemsClickHouseAdapter{Conn: conn},
			StatusTransitions: LinearWorkItemTransitionsClickHouseAdapter{Conn: conn},
			Dependencies: LinearWorkItemDependenciesClickHouseAdapter{
				Delegate: GitHubWorkItemDependenciesClickHouseAdapter{Conn: conn},
			},
			ReopenEvents: LinearWorkItemReopenEventsClickHouseAdapter{
				Delegate: GitHubWorkItemReopenEventsClickHouseAdapter{Conn: conn},
			},
			Interactions: LinearWorkItemInteractionsClickHouseAdapter{
				Delegate: GitHubWorkItemInteractionsClickHouseAdapter{Conn: conn},
			},
			Sprints: LinearSprintsClickHouseAdapter{
				Delegate: GitHubSprintsClickHouseAdapter{Conn: conn},
			},
		},
		Derived: derived,
	}
	if missing := sink.MissingDestinations(); len(missing) > 0 {
		return sink, fmt.Errorf(
			"%w: %s", ErrLinearWorkItemSinkIncomplete, strings.Join(missing, ", "),
		)
	}
	return sink, nil
}

func (sink LinearWorkItemFamilyClickHouseEffects) MissingDestinations() []string {
	rawMissing := make(map[string]struct{})
	for _, destination := range sink.Raw.MissingDestinations() {
		rawMissing[destination] = struct{}{}
	}
	derivedMissing := make(map[string]struct{})
	for _, destination := range sink.Derived.MissingDestinations() {
		derivedMissing[destination] = struct{}{}
	}
	rawReady := sink.Raw.Lease != nil
	derivedReady := sink.Derived.Lease != nil
	missing := make([]string, 0, len(workItemRouteDestinations()))
	for _, destination := range workItemRouteDestinations() {
		switch {
		case linearWorkItemDestination(destination):
			_, adapterMissing := rawMissing[destination]
			if !rawReady || adapterMissing {
				missing = append(missing, destination)
			}
		case linearWorkItemDerivedDestination(destination):
			_, adapterMissing := derivedMissing[destination]
			if !derivedReady || adapterMissing {
				missing = append(missing, destination)
			}
		default:
			missing = append(missing, destination)
		}
	}
	return missing
}

func (sink LinearWorkItemFamilyClickHouseEffects) complete() bool {
	return len(sink.MissingDestinations()) == 0
}

func (sink LinearWorkItemFamilyClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	if ctx == nil || !sink.complete() {
		return ErrInvalidConfiguration
	}
	switch {
	case linearWorkItemDestination(effect.Destination):
		return sink.Raw.WriteEffect(ctx, claim, effect)
	case linearWorkItemDerivedDestination(effect.Destination):
		return sink.Derived.WriteEffect(ctx, claim, effect)
	default:
		return ErrInvalidConfiguration
	}
}

func (sink LinearWorkItemFamilyClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	if ctx == nil || !sink.complete() {
		return EffectConflict, ErrInvalidConfiguration
	}
	switch {
	case linearWorkItemDestination(effect.Destination):
		return sink.Raw.InspectEffect(ctx, claim, effect)
	case linearWorkItemDerivedDestination(effect.Destination):
		return sink.Derived.InspectEffect(ctx, claim, effect)
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

var _ CompleteRouteHandler = LinearWorkItemFamilyRouteHandler{}
var _ EffectSink = LinearWorkItemFamilyClickHouseEffects{}
var _ EffectReadback = LinearWorkItemFamilyClickHouseEffects{}
