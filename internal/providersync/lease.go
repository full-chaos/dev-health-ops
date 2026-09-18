package providersync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

var (
	// ErrInvalidConfiguration is aliased to providerfoundation's copy (not
	// redefined locally) so internal/teamattribution -- which cannot import
	// providersync back -- can return the SAME sentinel identity; see
	// providerfoundation/types.go's doc comment on ErrInvalidConfiguration.
	ErrInvalidConfiguration             = providerfoundation.ErrInvalidConfiguration
	ErrUnitNotClaimable                 = errors.New("provider sync unit is not claimable")
	ErrLeaseLost                        = errors.New("provider sync unit lease is lost")
	ErrCompatibilityRequired            = errors.New("provider sync dataset requires Python compatibility execution")
	ErrGenerationJournalConflict        = errors.New("provider sync generation journal conflicts with persisted state")
	ErrGenerationBlockAmbiguous         = errors.New("provider sync generation block requires readback reconciliation")
	ErrGenerationRecoveryUnsafe         = errors.New("provider sync generation recovery payload is outside the bounded contract")
	ErrEffectLedgerConflict             = errors.New("provider sync effect ledger conflicts with persisted state")
	ErrEffectLedgerNotFound             = errors.New("provider sync effect ledger is not present")
	ErrPreparedRouteSnapshotNotFound    = errors.New("provider sync prepared route snapshot is not present")
	ErrPreparedRouteSnapshotRunTerminal = errors.New(
		"provider sync prepared route snapshot belongs to a run that already finished",
	)
	ErrEffectRecoveryAmbiguous = errors.New("provider sync effect recovery requires exact reconciliation")
	// ErrEffectRecoveryUnsafe is aliased to providerfoundation.ErrRecoveryUnsafe
	// for the same cross-package identity reason as ErrInvalidConfiguration
	// above.
	ErrEffectRecoveryUnsafe = providerfoundation.ErrRecoveryUnsafe
	// ErrPreparedSnapshotManifestMismatch is a persisted snapshot that is
	// authentically this claim's but describes a destination set the route no
	// longer emits -- a document written before a manifest change. It is
	// deliberately NOT ErrEffectLedgerConflict: that error means "do not trust
	// this document", and the caller answers it by refusing, whereas this one
	// means "this document is stale" and is answered by discarding it and
	// replaying the route.
	ErrPreparedSnapshotManifestMismatch = errors.New("provider sync prepared snapshot describes a superseded destination manifest")
	// ErrProviderDatasetUnavailable means the provider account cannot expose a
	// specific dataset even though the credential itself is valid. Retrying
	// cannot add an account-level product ability, and treating the response as
	// an empty snapshot would incorrectly tombstone previously collected rows.
	ErrProviderDatasetUnavailable  = errors.New("provider sync dataset is unavailable for this account")
	ErrShadowMismatch              = errors.New("provider sync native shadow differs from Python compatibility output")
	ErrRepositoryIdentityAmbiguous = errors.New(
		"provider sync repository identity cannot be proven identical to the Python derivation",
	)
	// ErrPaginationCapExceeded means a paginated fetch hit its page cap
	// before reaching the end of the provider's result set (codex H2). A
	// capped fetch must never be reported as a successful, complete unit: an
	// unbounded Python collector would have kept paging, so a capped Go
	// fetch that still returns success and still advances the watermark
	// silently and permanently loses every record past the cap -- no later
	// incremental run recovers them, because the window has already moved
	// past where they were.
	ErrPaginationCapExceeded = errors.New(
		"provider sync paginated fetch hit its page cap before completion",
	)
)

// ProviderDatasetUnavailableCategory records an account-level provider
// capability limitation. It is distinct from authentication and exhaustion:
// retrying the same valid credential cannot make the dataset available.
// Exported so internal/jobs/providerunit (which already imports this
// package to classify ErrProviderDatasetUnavailable) and the mark/clear
// writes in repository_postgres.go share one definition instead of two
// literals that could silently drift apart.
const ProviderDatasetUnavailableCategory = "provider_dataset_unavailable"

type Unit struct {
	ID                    string
	SyncRunID             string
	OrgID                 string
	IntegrationID         string
	SourceID              string
	SourceExternalID      string
	SourceName            string
	Provider              string
	Dataset               string
	CostClass             CostClass
	Mode                  string
	SinceAt               *time.Time
	BeforeAt              *time.Time
	ProcessorFlags        map[string]bool
	DatasetOptions        map[string]any
	Result                map[string]any
	SourceMetadata        map[string]any
	IntegrationConfig     map[string]any
	CredentialID          string
	CredentialFingerprint string
	AuthSource            string
}

func (unit Unit) Validate() error {
	for _, value := range []string{unit.ID, unit.SyncRunID, unit.IntegrationID, unit.SourceID} {
		if _, err := uuid.Parse(value); err != nil {
			return ErrInvalidConfiguration
		}
	}
	if strings.TrimSpace(unit.OrgID) == "" || strings.TrimSpace(unit.SourceExternalID) == "" {
		return ErrInvalidConfiguration
	}
	capability, ok := Capability(unit.Provider, unit.Dataset)
	if !ok || capability.CostClass != unit.CostClass {
		return ErrInvalidConfiguration
	}
	switch unit.Mode {
	case "incremental", "backfill", "full_resync":
	default:
		return ErrInvalidConfiguration
	}
	if unit.SinceAt != nil && unit.BeforeAt != nil && unit.SinceAt.After(*unit.BeforeAt) {
		return ErrInvalidConfiguration
	}
	if unit.AuthSource == "environment" {
		// Go execution never hydrates credentials through process-global state.
		// This is where D18's "environment target/token fallback is not part of
		// the Go route" is actually enforced for every provider, not just
		// Projects v2: a unit whose credentials would come from process state
		// never reaches a collector at all.
		return ErrInvalidConfiguration
	}
	// A resolved credential id is REQUIRED, and this is the only clause that
	// enforces it: uuid.Parse rejects "" as readily as it rejects garbage. The
	// separate `unit.CredentialID == ""` test that used to sit above was the
	// twin of one removed from the Projects v2 fetcher for the same reason --
	// it could never decide on its own, so its mutation was unkillable and it
	// read as coverage while proving nothing.
	if _, err := uuid.Parse(unit.CredentialID); err != nil {
		return ErrInvalidConfiguration
	}
	return nil
}

func (unit Unit) TenantScope() providerfoundation.TenantScope {
	return providerfoundation.TenantScope{
		OrgID:         unit.OrgID,
		Provider:      unit.Provider,
		IntegrationID: unit.IntegrationID,
		CredentialID:  unit.CredentialID,
	}
}

type Claim struct {
	Unit
	Owner          string
	Attempt        int
	LeaseExpiresAt time.Time
	Recovered      bool
}

func (claim Claim) Validate() error {
	if err := claim.Unit.Validate(); err != nil {
		return err
	}
	if _, err := uuid.Parse(claim.Owner); err != nil || claim.Attempt < 1 || claim.LeaseExpiresAt.IsZero() {
		return ErrInvalidConfiguration
	}
	return nil
}

// GenerationKey is stable across expired-lease recovery attempts. Every
// concrete sink must use it as its idempotency generation so a worker killed
// during sink acknowledgement cannot create a second ClickHouse generation.
func (claim Claim) GenerationKey() string { return "sync-unit:" + claim.ID }

type ClaimRequest struct {
	UnitID               string
	OrgID                string
	Owner                string
	Now                  time.Time
	LeaseDuration        time.Duration
	AllowExpiredRecovery bool
}

func (request ClaimRequest) validate() error {
	if _, err := uuid.Parse(request.UnitID); err != nil {
		return ErrInvalidConfiguration
	}
	if _, err := uuid.Parse(request.Owner); err != nil {
		return ErrInvalidConfiguration
	}
	if request.Now.IsZero() || request.LeaseDuration < time.Second || request.LeaseDuration > 15*time.Minute {
		return ErrInvalidConfiguration
	}
	return nil
}

type LeaseRepository interface {
	Claim(context.Context, ClaimRequest) (Claim, error)
	Assert(context.Context, Claim, time.Time) error
	Renew(context.Context, Claim, time.Time, time.Time) error
}

// LeaseSession is both the heartbeat owner and the LeaseGuard passed through
// credential resolution, HTTP requests, and sink writes.
//
// Every field is read-only while Run executes: the work function reads
// session.Claim concurrently with the heartbeat goroutine, so Heartbeat never
// writes it. Claim.LeaseExpiresAt stays the expiry granted at claim time; the
// renewed expiry lives only in the repository row that Renew and Assert check.
type LeaseSession struct {
	Repository    LeaseRepository
	Claim         Claim
	LeaseDuration time.Duration
	Deadline      time.Time
	Now           func() time.Time
}

func (session *LeaseSession) valid() bool {
	return session != nil && session.Repository != nil && session.Claim.Validate() == nil &&
		session.LeaseDuration >= time.Second && session.LeaseDuration <= 15*time.Minute &&
		!session.Deadline.IsZero() && !session.Deadline.Before(session.Claim.LeaseExpiresAt)
}

func (session *LeaseSession) now() time.Time {
	if session.Now != nil {
		return session.Now().UTC()
	}
	return time.Now().UTC()
}

func (session *LeaseSession) Assert(ctx context.Context) error {
	if !session.valid() || ctx == nil {
		return providerfoundation.ErrLeaseLost
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := session.now()
	if !now.Before(session.Deadline) {
		return providerfoundation.ErrLeaseLost
	}
	if err := session.Repository.Assert(ctx, session.Claim, now); err != nil {
		return providerfoundation.ErrLeaseLost
	}
	return nil
}

func (session *LeaseSession) Heartbeat(ctx context.Context) error {
	_, err := session.renew(ctx)
	return err
}

func (session *LeaseSession) renew(ctx context.Context) (time.Time, error) {
	if !session.valid() || ctx == nil {
		return time.Time{}, ErrLeaseLost
	}
	now := session.now()
	if !now.Before(session.Deadline) {
		return time.Time{}, ErrLeaseLost
	}
	expiresAt := now.Add(session.LeaseDuration)
	if expiresAt.After(session.Deadline) {
		expiresAt = session.Deadline
	}
	if !expiresAt.After(now) {
		return time.Time{}, ErrLeaseLost
	}
	if err := session.Repository.Renew(ctx, session.Claim, now, expiresAt); err != nil {
		return time.Time{}, ErrLeaseLost
	}
	return expiresAt, nil
}

// errLeaseSessionHeartbeatLost cancels the work context when a renewal fails
// while work runs. It wraps both lease-loss sentinels because work code
// classifies a lost lease by either the provider foundation's or this
// package's.
var errLeaseSessionHeartbeatLost = fmt.Errorf(
	"heartbeat could not renew the claim: %w: %w", ErrLeaseLost, providerfoundation.ErrLeaseLost,
)

// Run starts a heartbeat loop and cooperatively cancels work on lease loss.
// It cannot forcibly stop a provider call that ignores context; clients in the
// provider foundation all bind requests and retry waits to this context.
//
// The outcome follows the repository's answer to every renewal the heartbeat
// started while work ran, never the order in which goroutines notice it.
// Renewals run on the caller's context bounded by the last granted expiry, not
// on the work context, so the work returning cannot make a renewal fail:
//   - a renewal is refused, fails in the store, or cannot start because the
//     deadline is reached: the work context is cancelled with a cause matching
//     ErrLeaseLost and providerfoundation.ErrLeaseLost, and Run returns
//     ErrLeaseLost whatever the work returns, even when the work returned
//     while that renewal was in flight;
//   - a renewal still in flight when the work returns is awaited; if the
//     store grants it, Run returns the work's error, including nil;
//   - a renewal that outlives the last granted expiry is a lost lease;
//   - a renewal that fails because the caller's context ended is not a lost
//     lease: Run returns the work's error, and the work context carries the
//     caller's cause.
func (session *LeaseSession) Run(
	ctx context.Context,
	interval time.Duration,
	work func(context.Context, providerfoundation.LeaseGuard) error,
) error {
	// A heartbeat at or near the lease duration can arrive after the lease has
	// already expired under scheduler or network jitter. Requiring at least
	// two renewal opportunities per lease keeps expiry from becoming the
	// worker's normal cancellation mechanism.
	if !session.valid() || ctx == nil || interval <= 0 ||
		interval > session.LeaseDuration/2 || work == nil {
		return ErrInvalidConfiguration
	}
	workContext, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	// Written only by the heartbeat goroutine and read only after it exits.
	leaseLost := false
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		grantedUntil := session.Claim.LeaseExpiresAt
		for {
			select {
			case <-workContext.Done():
				return
			case <-ticker.C:
			}
			renewContext, cancelRenew := context.WithTimeout(ctx, grantedUntil.Sub(session.now()))
			expiresAt, err := session.renew(renewContext)
			cancelRenew()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				leaseLost = true
				cancel(errLeaseSessionHeartbeatLost)
				return
			}
			grantedUntil = expiresAt
		}
	}()
	workErr := work(workContext, session)
	cancel(nil)
	<-heartbeatDone
	if leaseLost {
		return ErrLeaseLost
	}
	return workErr
}

var _ providerfoundation.LeaseGuard = (*LeaseSession)(nil)
