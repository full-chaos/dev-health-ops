package joboutbox

import (
	"context"
	"errors"
	"sort"
	"time"
)

const maxRelayPolicyKinds = 1000

type RelayConfig struct {
	LeaseDuration    time.Duration
	MaxRelayAttempts int
	BaseBackoff      time.Duration
	MaximumBackoff   time.Duration
}

func DefaultRelayConfig() RelayConfig {
	return RelayConfig{
		LeaseDuration:    30 * time.Second,
		MaxRelayAttempts: 10,
		BaseBackoff:      5 * time.Second,
		MaximumBackoff:   5 * time.Minute,
	}
}

func (config RelayConfig) validate() error {
	if config.LeaseDuration < time.Second || config.LeaseDuration > 15*time.Minute ||
		config.MaxRelayAttempts < 1 || config.MaxRelayAttempts > 100 ||
		config.BaseBackoff < time.Second || config.MaximumBackoff < config.BaseBackoff ||
		config.MaximumBackoff > time.Hour {
		return ErrInvalidConfiguration
	}
	return nil
}

type StepResult struct {
	Claimed   int
	Deferred  int
	Delivered int
	Retried   int
	Dead      int
	LeaseLost int
}

// Relay is a single bounded reconciliation step. Process lifecycle and polling
// are intentionally left to the Phase 2 reconciler binary.
type Relay struct {
	repository    *Repository
	inserter      *RiverInserter
	config        RelayConfig
	deferredKinds []string
	routes        RouteResolver
}

type RouteResolver interface {
	DeferredKinds(context.Context) ([]string, error)
	Resolve(context.Context, string) (string, error)
}

func NewRelay(repository *Repository, inserter *RiverInserter, config RelayConfig) (*Relay, error) {
	if repository == nil || inserter == nil || config.validate() != nil {
		return nil, ErrInvalidConfiguration
	}
	registry, ok := inserter.registry.(RelayPolicyRegistry)
	if !ok {
		return nil, ErrInvalidConfiguration
	}
	deferredKinds, err := deferredRelayKinds(registry)
	if err != nil {
		return nil, err
	}
	return &Relay{
		repository:    repository,
		inserter:      inserter,
		config:        config,
		deferredKinds: deferredKinds,
	}, nil
}

func NewRelayWithRoutes(
	repository *Repository,
	inserter *RiverInserter,
	routes RouteResolver,
	config RelayConfig,
) (*Relay, error) {
	if routes == nil {
		return nil, ErrInvalidConfiguration
	}
	relay, err := NewRelay(repository, inserter, config)
	if err != nil {
		return nil, err
	}
	relay.routes = routes
	return relay, nil
}

func deferredRelayKinds(registry RelayPolicyRegistry) ([]string, error) {
	if registry == nil {
		return nil, ErrInvalidConfiguration
	}
	descriptors := registry.Descriptors()
	if len(descriptors) < 1 || len(descriptors) > maxRelayPolicyKinds {
		return nil, ErrInvalidConfiguration
	}
	seen := make(map[string]struct{}, len(descriptors))
	deferred := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		if descriptor.Kind == "" {
			return nil, ErrInvalidConfiguration
		}
		if _, duplicate := seen[descriptor.Kind]; duplicate {
			return nil, ErrInvalidConfiguration
		}
		seen[descriptor.Kind] = struct{}{}
		resolved, ok := registry.Descriptor(descriptor.Kind)
		if !ok || resolved.Kind != descriptor.Kind || resolved.Route != descriptor.Route {
			return nil, ErrInvalidConfiguration
		}
		switch descriptor.Route {
		case "celery":
			deferred = append(deferred, descriptor.Kind)
		case "shadow", "river_canary", "river":
		default:
			return nil, ErrInvalidConfiguration
		}
	}
	sort.Strings(deferred)
	return deferred, nil
}

func (relay *Relay) Step(ctx context.Context, now time.Time, limit int) (StepResult, error) {
	if relay == nil || now.IsZero() {
		return StepResult{}, ErrInvalidConfiguration
	}
	deferred := relay.deferredKinds
	if relay.routes != nil {
		var err error
		deferred, err = relay.routes.DeferredKinds(ctx)
		if err != nil {
			return StepResult{}, ErrUnavailable
		}
		if len(deferred) > maxRelayPolicyKinds {
			return StepResult{}, ErrInvalidConfiguration
		}
		sort.Strings(deferred)
	}
	claims, err := relay.repository.claimDueExcept(ctx, now, limit, relay.config.LeaseDuration, deferred)
	if err != nil {
		return StepResult{}, err
	}
	result := StepResult{Claimed: len(claims)}
	for _, claim := range claims {
		if relay.routes != nil {
			transport, routeErr := relay.routes.Resolve(ctx, claim.JobKind)
			if routeErr != nil {
				return result, ErrUnavailable
			}
			if transport == "celery" {
				if releaseErr := relay.repository.releaseClaim(ctx, claim, now); releaseErr != nil {
					return result, releaseErr
				}
				result.Deferred++
				continue
			}
		}
		_, dispatchErr := relay.repository.Dispatch(ctx, claim, now, relay.inserter.Insert)
		switch {
		case dispatchErr == nil:
			result.Delivered++
		case errors.Is(dispatchErr, errInjectedCrash):
			return result, errInjectedCrash
		case errors.Is(dispatchErr, ErrLeaseLost):
			result.LeaseLost++
		case errors.Is(dispatchErr, ErrContractRejected):
			recorded, recordErr := relay.recordOutcome(ctx, claim, now, failureContract, &result)
			if recordErr != nil {
				return result, recordErr
			}
			if recorded {
				result.Dead++
			}
		case errors.Is(dispatchErr, ErrPolicyRejected):
			recorded, recordErr := relay.recordOutcome(ctx, claim, now, failurePolicy, &result)
			if recordErr != nil {
				return result, recordErr
			}
			if recorded {
				result.Dead++
			}
		default:
			recorded, recordErr := relay.recordOutcome(ctx, claim, now, failureRiver, &result)
			if recordErr != nil {
				return result, recordErr
			}
			if recorded {
				if claim.AttemptCount >= relay.config.MaxRelayAttempts {
					result.Dead++
				} else {
					result.Retried++
				}
			}
		}
	}
	return result, nil
}

// recordOutcome distinguishes an expected stale-lease race from a persistence
// outage. Only a proven lost lease is counted and tolerated; an unavailable
// database is fatal so the lifecycle loop closes readiness instead of
// reporting a successful reconciliation step.
func (relay *Relay) recordOutcome(
	ctx context.Context,
	claim Claim,
	now time.Time,
	kind failureKind,
	result *StepResult,
) (bool, error) {
	return classifyRecordOutcome(relay.record(ctx, claim, now, kind), result)
}

func classifyRecordOutcome(err error, result *StepResult) (bool, error) {
	if err == nil {
		return true, nil
	}
	if errors.Is(err, ErrLeaseLost) {
		result.LeaseLost++
		return false, nil
	}
	return false, err
}

func (relay *Relay) record(ctx context.Context, claim Claim, now time.Time, kind failureKind) error {
	return relay.repository.recordFailure(
		ctx,
		claim,
		now,
		kind,
		relay.config.MaxRelayAttempts,
		now.Add(relay.backoff(claim.AttemptCount)),
	)
}

func (relay *Relay) backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exponent := attempt - 1
	if exponent > 10 {
		exponent = 10
	}
	value := relay.config.BaseBackoff * time.Duration(1<<exponent)
	if value > relay.config.MaximumBackoff {
		return relay.config.MaximumBackoff
	}
	return value
}
