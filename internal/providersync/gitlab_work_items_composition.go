package providersync

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// ErrGitLabWorkItemSinkIncomplete names the provider-local construction
// failure that would otherwise let a six- or ten-destination partial sink be
// handed to EffectCommitter for a sixteen-effect route batch.
var ErrGitLabWorkItemSinkIncomplete = errors.New(
	"gitlab work-item clickhouse sink is missing destination adapters",
)

// GitLabWorkItemFamilyClickHouseEffects composes the six raw-fact adapters
// with the ten derived adapters behind the one EffectSink/EffectReadback pair
// EffectCommitter accepts. It registers or activates no route.
type GitLabWorkItemFamilyClickHouseEffects struct {
	Raw     GitLabWorkItemClickHouseEffects
	Derived GitLabWorkItemDerivedClickHouseEffects
}

func NewGitLabWorkItemFamilyClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
) (GitLabWorkItemFamilyClickHouseEffects, error) {
	if conn == nil || lease == nil {
		return GitLabWorkItemFamilyClickHouseEffects{}, ErrInvalidConfiguration
	}
	derived, err := NewGitLabWorkItemDerivedClickHouseEffects(conn, lease)
	if err != nil {
		return GitLabWorkItemFamilyClickHouseEffects{}, err
	}
	sink := GitLabWorkItemFamilyClickHouseEffects{
		Raw:     NewGitLabWorkItemClickHouseEffects(conn, lease),
		Derived: derived,
	}
	if missing := sink.MissingDestinations(); len(missing) > 0 {
		return sink, fmt.Errorf(
			"%w: %s", ErrGitLabWorkItemSinkIncomplete, strings.Join(missing, ", "),
		)
	}
	return sink, nil
}

func (sink GitLabWorkItemFamilyClickHouseEffects) MissingDestinations() []string {
	rawMissing := make(map[string]struct{})
	for _, destination := range sink.Raw.MissingDestinations() {
		rawMissing[destination] = struct{}{}
	}
	derivedMissing := make(map[string]struct{})
	for _, destination := range sink.Derived.MissingDestinations() {
		derivedMissing[destination] = struct{}{}
	}
	missing := make([]string, 0, len(rawMissing)+len(derivedMissing))
	for _, destination := range workItemRouteDestinations() {
		switch {
		case gitLabWorkItemDestination(destination):
			if _, absent := rawMissing[destination]; absent {
				missing = append(missing, destination)
			}
		case gitlabWorkItemDerivedDestination(destination):
			if _, absent := derivedMissing[destination]; absent {
				missing = append(missing, destination)
			}
		default:
			missing = append(missing, destination)
		}
	}
	return missing
}

func (sink GitLabWorkItemFamilyClickHouseEffects) complete() bool {
	return len(sink.MissingDestinations()) == 0
}

func (sink GitLabWorkItemFamilyClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	if ctx == nil || !sink.complete() {
		return ErrInvalidConfiguration
	}
	switch {
	case gitLabWorkItemDestination(effect.Destination):
		return sink.Raw.WriteEffect(ctx, claim, effect)
	case gitlabWorkItemDerivedDestination(effect.Destination):
		return sink.Derived.WriteEffect(ctx, claim, effect)
	default:
		return ErrInvalidConfiguration
	}
}

func (sink GitLabWorkItemFamilyClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	if ctx == nil || !sink.complete() {
		return EffectConflict, ErrInvalidConfiguration
	}
	switch {
	case gitLabWorkItemDestination(effect.Destination):
		return sink.Raw.InspectEffect(ctx, claim, effect)
	case gitlabWorkItemDerivedDestination(effect.Destination):
		return sink.Derived.InspectEffect(ctx, claim, effect)
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

var _ EffectSink = GitLabWorkItemFamilyClickHouseEffects{}
var _ EffectReadback = GitLabWorkItemFamilyClickHouseEffects{}
