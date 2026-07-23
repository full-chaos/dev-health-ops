package providersync

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

var (
	ErrInvalidConfiguration = errors.New("provider sync configuration is invalid")
	ErrUnitNotClaimable     = errors.New("provider sync unit is not claimable")
	ErrLeaseLost            = errors.New("provider sync unit lease is lost")
)

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
	if unit.AuthSource == "environment" || unit.CredentialID == "" {
		// Go execution never hydrates credentials through process-global state.
		return ErrInvalidConfiguration
	}
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
	if !session.valid() || ctx == nil {
		return ErrLeaseLost
	}
	now := session.now()
	if !now.Before(session.Deadline) {
		return ErrLeaseLost
	}
	expiresAt := now.Add(session.LeaseDuration)
	if expiresAt.After(session.Deadline) {
		expiresAt = session.Deadline
	}
	if !expiresAt.After(now) {
		return ErrLeaseLost
	}
	if err := session.Repository.Renew(ctx, session.Claim, now, expiresAt); err != nil {
		return ErrLeaseLost
	}
	session.Claim.LeaseExpiresAt = expiresAt
	return nil
}

// Run starts a heartbeat loop and cooperatively cancels work on lease loss.
// It cannot forcibly stop a provider call that ignores context; clients in the
// provider foundation all bind requests and retry waits to this context.
func (session *LeaseSession) Run(
	ctx context.Context,
	interval time.Duration,
	work func(context.Context, providerfoundation.LeaseGuard) error,
) error {
	if !session.valid() || ctx == nil || interval <= 0 || work == nil {
		return ErrInvalidConfiguration
	}
	workContext, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatResult := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-workContext.Done():
				heartbeatResult <- nil
				return
			case <-ticker.C:
				if err := session.Heartbeat(workContext); err != nil {
					cancel()
					heartbeatResult <- err
					return
				}
			}
		}
	}()
	workErr := work(workContext, session)
	cancel()
	heartbeatErr := <-heartbeatResult
	if heartbeatErr != nil {
		return ErrLeaseLost
	}
	return workErr
}

var _ providerfoundation.LeaseGuard = (*LeaseSession)(nil)
