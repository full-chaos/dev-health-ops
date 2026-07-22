package joboutbox

import (
	"context"
	"errors"
	"time"
)

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
	Delivered int
	Retried   int
	Dead      int
	LeaseLost int
}

// Relay is a single bounded reconciliation step. Process lifecycle and polling
// are intentionally left to the Phase 2 reconciler binary.
type Relay struct {
	repository *Repository
	inserter   *RiverInserter
	config     RelayConfig
}

func NewRelay(repository *Repository, inserter *RiverInserter, config RelayConfig) (*Relay, error) {
	if repository == nil || inserter == nil || config.validate() != nil {
		return nil, ErrInvalidConfiguration
	}
	return &Relay{repository: repository, inserter: inserter, config: config}, nil
}

func (relay *Relay) Step(ctx context.Context, now time.Time, limit int) (StepResult, error) {
	if relay == nil || now.IsZero() {
		return StepResult{}, ErrInvalidConfiguration
	}
	claims, err := relay.repository.ClaimDue(ctx, now, limit, relay.config.LeaseDuration)
	if err != nil {
		return StepResult{}, err
	}
	result := StepResult{Claimed: len(claims)}
	for _, claim := range claims {
		_, dispatchErr := relay.repository.Dispatch(ctx, claim, now, relay.inserter.Insert)
		switch {
		case dispatchErr == nil:
			result.Delivered++
		case errors.Is(dispatchErr, errInjectedCrash):
			return result, errInjectedCrash
		case errors.Is(dispatchErr, ErrLeaseLost):
			result.LeaseLost++
		case errors.Is(dispatchErr, ErrContractRejected):
			if relay.record(ctx, claim, now, failureContract) == nil {
				result.Dead++
			} else {
				result.LeaseLost++
			}
		case errors.Is(dispatchErr, ErrPolicyRejected):
			if relay.record(ctx, claim, now, failurePolicy) == nil {
				result.Dead++
			} else {
				result.LeaseLost++
			}
		default:
			if relay.record(ctx, claim, now, failureRiver) == nil {
				if claim.AttemptCount >= relay.config.MaxRelayAttempts {
					result.Dead++
				} else {
					result.Retried++
				}
			} else {
				result.LeaseLost++
			}
		}
	}
	return result, nil
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
