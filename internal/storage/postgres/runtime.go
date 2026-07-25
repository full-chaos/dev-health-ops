package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrDomainDatabaseRequired        = errors.New("domain PostgreSQL configuration is required")
	ErrQueueControlRequired          = errors.New("WORKER_DATABASE_URI is required for queue control")
	ErrQueueControlTransactionMode   = errors.New("transaction-mode PgBouncer cannot be used for River queue control")
	ErrQueueControlSessionUnverified = errors.New("session-mode queue control is unavailable until its compatibility matrix passes")
	ErrRuntimeRolesNotSeparated      = errors.New("queue-control, coordinator, and domain PostgreSQL roles must be distinct")
	ErrRuntimeRoleConfiguration      = errors.New("runtime PostgreSQL role configuration is invalid")
	// ErrCoordinatorDatabaseRequired is deliberately its own sentinel rather
	// than a reuse of ErrDomainDatabaseRequired: a coordinator binary that
	// silently fell back to the domain pool would re-introduce CHAOS-3113
	// (the domain role holds SELECT+INSERT on worker_job_outbox, and
	// jobroute's LOCK TABLE ... IN SHARE ROW EXCLUSIVE MODE requires UPDATE),
	// so a missing coordinator DSN must fail closed and be attributable.
	ErrCoordinatorDatabaseRequired = errors.New("COORDINATOR_DATABASE_URI is required for coordinator-role work")
	ErrCoordinatorTransactionMode  = errors.New("transaction-mode PgBouncer cannot be used for coordinator control-plane access")
)

// RuntimeConfig describes the PostgreSQL trust boundaries used by River
// processes. Domain traffic may traverse transaction-mode PgBouncer. Queue
// control must use the bounded direct endpoint proved by the compatibility
// matrix; session mode remains fail-closed until equivalent evidence exists.
//
// The coordinator boundary is the third role of the CHAOS-3033 Option B split
// and is OPT-IN per binary via RequireCoordinator: binaries that do only domain
// work (dev-health-worker, dev-health-stream-runner) must not be forced to
// carry a coordinator DSN, while binaries that do coordinator work must fail
// closed without one rather than fall back to the domain pool. Like queue
// control — and unlike domain traffic — the coordinator connection is modeled
// as a DIRECT, server-counted connection, never PgBouncer-pooled; see
// deploymentcontract.BudgetSummary.DirectCoordinatorConnections.
type RuntimeConfig struct {
	DomainURI               string
	QueueControlURI         string
	CoordinatorURI          string
	DomainRole              string
	QueueRole               string
	CoordinatorRole         string
	RiverSchema             string
	QueueControlMode        config.QueueControlMode
	DomainTransactionPooler bool
	DomainMaxConns          int32
	QueueMaxConns           int32
	CoordinatorMaxConns     int32
	// RequireCoordinator opts this process into the coordinator boundary. When
	// false every Coordinator* field is ignored and RuntimePools.Coordinator is
	// nil; when true the coordinator DSN, role, and bounds are validated with
	// the same strictness as the other two and the pool is always opened.
	RequireCoordinator bool
}

func DefaultRuntimeConfig(domainURI, queueControlURI, domainRole, queueRole string) RuntimeConfig {
	return RuntimeConfig{
		DomainURI:        domainURI,
		QueueControlURI:  queueControlURI,
		DomainRole:       domainRole,
		QueueRole:        queueRole,
		RiverSchema:      "river",
		QueueControlMode: config.QueueControlDirect,
		DomainMaxConns:   4,
		QueueMaxConns:    2,
	}
}

func RuntimeConfigFromPlatform(configValue config.Config) RuntimeConfig {
	return RuntimeConfig{
		DomainURI:               configValue.DomainDatabaseURI.Reveal(),
		QueueControlURI:         configValue.QueueDatabaseURI.Reveal(),
		CoordinatorURI:          configValue.CoordinatorDatabaseURI.Reveal(),
		DomainRole:              configValue.DomainDatabaseRole,
		QueueRole:               configValue.QueueDatabaseRole,
		CoordinatorRole:         configValue.CoordinatorDatabaseRole,
		RiverSchema:             configValue.RiverDatabaseSchema,
		QueueControlMode:        configValue.QueueDatabaseMode,
		DomainTransactionPooler: configValue.DomainTransactionPooler,
		DomainMaxConns:          configValue.DomainDatabaseMaxConns,
		QueueMaxConns:           configValue.QueueDatabaseMaxConns,
		CoordinatorMaxConns:     configValue.CoordinatorDatabaseMaxConns,
	}
}

// WithCoordinator opts the process into the coordinator boundary. Coordinator
// binaries call this explicitly so that the requirement is visible at the call
// site and so a domain-only binary can never acquire a coordinator pool by
// merely having the environment variables present.
func (c RuntimeConfig) WithCoordinator() RuntimeConfig {
	c.RequireCoordinator = true
	return c
}

func (c RuntimeConfig) Validate() error {
	if c.DomainURI == "" {
		return ErrDomainDatabaseRequired
	}
	if c.QueueControlURI == "" {
		return ErrQueueControlRequired
	}
	if !validRuntimeIdentifier(c.DomainRole) || !validRuntimeIdentifier(c.QueueRole) ||
		!validRuntimeIdentifier(c.RiverSchema) {
		return ErrRuntimeRoleConfiguration
	}
	if c.DomainRole == c.QueueRole {
		return ErrRuntimeRolesNotSeparated
	}
	if c.RequireCoordinator {
		if c.CoordinatorURI == "" {
			return ErrCoordinatorDatabaseRequired
		}
		if !validRuntimeIdentifier(c.CoordinatorRole) {
			return ErrRuntimeRoleConfiguration
		}
		// All three roles must be pairwise distinct: two of them sharing a
		// login would collapse the split back into one privilege set while
		// every readiness check still passed against its own posture.
		if c.CoordinatorRole == c.DomainRole || c.CoordinatorRole == c.QueueRole {
			return ErrRuntimeRolesNotSeparated
		}
	}
	switch c.QueueControlMode {
	case config.QueueControlDirect:
	case config.QueueControlTransaction:
		return ErrQueueControlTransactionMode
	case config.QueueControlSession:
		return ErrQueueControlSessionUnverified
	default:
		return ErrInvalidConfig
	}
	if c.DomainMaxConns < 1 || c.DomainMaxConns > 16 || c.QueueMaxConns < 1 || c.QueueMaxConns > 4 {
		return ErrInvalidConfig
	}
	// 1..4 mirrors deploymentcontract's own bound on CoordinatorMaxConnections
	// (validateProcess / validateOperator): the coordinator connection is a
	// direct, server-counted connection, so its ceiling is the queue-control
	// ceiling, not the PgBouncer-pooled domain ceiling of 16.
	if c.RequireCoordinator && (c.CoordinatorMaxConns < 1 || c.CoordinatorMaxConns > 4) {
		return ErrInvalidConfig
	}
	domainConfig, err := parseConfig(c.DomainURI)
	if err != nil {
		return ErrInvalidConfig
	}
	queueConfig, err := parseConfig(c.QueueControlURI)
	if err != nil {
		return ErrInvalidConfig
	}
	if domainConfig.ConnConfig.User == "" || queueConfig.ConnConfig.User == "" ||
		domainConfig.ConnConfig.User != c.DomainRole || queueConfig.ConnConfig.User != c.QueueRole {
		return ErrRuntimeRoleConfiguration
	}
	if domainConfig.ConnConfig.User == queueConfig.ConnConfig.User {
		return ErrRuntimeRolesNotSeparated
	}
	if c.DomainTransactionPooler && sameEndpoint(domainConfig, queueConfig) {
		return ErrQueueControlTransactionMode
	}
	if c.RequireCoordinator {
		coordinatorConfig, err := parseConfig(c.CoordinatorURI)
		if err != nil {
			return ErrInvalidConfig
		}
		if coordinatorConfig.ConnConfig.User == "" ||
			coordinatorConfig.ConnConfig.User != c.CoordinatorRole {
			return ErrRuntimeRoleConfiguration
		}
		if coordinatorConfig.ConnConfig.User == domainConfig.ConnConfig.User ||
			coordinatorConfig.ConnConfig.User == queueConfig.ConnConfig.User {
			return ErrRuntimeRolesNotSeparated
		}
		// Same reasoning as queue control: the coordinator pool holds
		// SHARE ROW EXCLUSIVE table locks and FOR UPDATE row locks across
		// statements within one transaction, which a transaction-mode pooler
		// may hand to a different server session. If the domain endpoint is
		// PgBouncer in transaction mode and the coordinator DSN points at that
		// same endpoint, the coordinator is not actually direct.
		if c.DomainTransactionPooler && sameEndpoint(domainConfig, coordinatorConfig) {
			return ErrCoordinatorTransactionMode
		}
	}
	return nil
}

func (c RuntimeConfig) SafeAttributes() map[string]any {
	attributes := map[string]any{
		"domain_configured":             c.DomainURI != "",
		"queue_control_configured":      c.QueueControlURI != "",
		"domain_database_role":          c.DomainRole,
		"queue_database_role":           c.QueueRole,
		"river_database_schema":         c.RiverSchema,
		"queue_control_mode":            c.QueueControlMode,
		"domain_transaction_pooler":     c.DomainTransactionPooler,
		"domain_max_connections":        c.DomainMaxConns,
		"queue_control_max_connections": c.QueueMaxConns,
		"coordinator_required":          c.RequireCoordinator,
		"total_max_connections":         c.DomainMaxConns + c.QueueMaxConns,
	}
	// Only reported when the boundary is actually in use: emitting a role name
	// and a zero connection count for every domain-only worker would suggest a
	// coordinator pool exists where none does.
	if c.RequireCoordinator {
		attributes["coordinator_configured"] = c.CoordinatorURI != ""
		attributes["coordinator_database_role"] = c.CoordinatorRole
		attributes["coordinator_max_connections"] = c.CoordinatorMaxConns
		attributes["total_max_connections"] = c.DomainMaxConns + c.QueueMaxConns + c.CoordinatorMaxConns
	}
	return attributes
}

func validRuntimeIdentifier(value string) bool {
	if value == "" || len(value) > 63 {
		return false
	}
	for index, char := range value {
		if (char >= 'a' && char <= 'z') || char == '_' || (index > 0 && char >= '0' && char <= '9') {
			continue
		}
		return false
	}
	return true
}

func sameEndpoint(left, right *pgxpool.Config) bool {
	return left.ConnConfig.Host == right.ConnConfig.Host &&
		left.ConnConfig.Port == right.ConnConfig.Port &&
		left.ConnConfig.Database == right.ConnConfig.Database
}

// RuntimePools owns separate bounded pools for domain, River queue-control,
// and (when the process opted in via RuntimeConfig.RequireCoordinator)
// coordinator control-plane traffic. Creating these pools never applies schema
// migrations.
//
// Coordinator is nil for domain-only processes. Callers that need it must use
// CoordinatorPool, which fails closed rather than returning the domain pool:
// there is deliberately NO fallback, because a coordinator call site silently
// running on the domain role is exactly the CHAOS-3113 defect.
type RuntimePools struct {
	Domain       *pgxpool.Pool
	QueueControl *pgxpool.Pool
	Coordinator  *pgxpool.Pool
}

// CoordinatorPool returns the coordinator pool or ErrUnavailable. It exists so
// that a coordinator call site cannot compile-and-silently-degrade onto the
// domain pool: the error is unmissable at wiring time.
func (p *RuntimePools) CoordinatorPool() (*pgxpool.Pool, error) {
	if p == nil || p.Coordinator == nil {
		return nil, ErrUnavailable
	}
	return p.Coordinator, nil
}

func OpenRuntimePools(ctx context.Context, runtimeConfig RuntimeConfig) (*RuntimePools, error) {
	pools, err := NewRuntimePools(ctx, runtimeConfig)
	if err != nil {
		return nil, err
	}
	if err := pools.Ping(ctx); err != nil {
		pools.Close()
		return nil, err
	}
	return pools, nil
}

// NewRuntimePools constructs both pools without applying migrations or
// requiring the endpoints to be reachable yet. Readiness calls Ping.
func NewRuntimePools(ctx context.Context, runtimeConfig RuntimeConfig) (*RuntimePools, error) {
	if err := runtimeConfig.Validate(); err != nil {
		return nil, err
	}

	domainConfig := DefaultConfig(runtimeConfig.DomainURI)
	domainConfig.MaxConns = runtimeConfig.DomainMaxConns
	domainPool, err := New(ctx, domainConfig)
	if err != nil {
		return nil, fmt.Errorf("open domain pool: %w", err)
	}

	queueConfig := DefaultConfig(runtimeConfig.QueueControlURI)
	queueConfig.MaxConns = runtimeConfig.QueueMaxConns
	queuePool, err := New(ctx, queueConfig)
	if err != nil {
		domainPool.Close()
		return nil, fmt.Errorf("open queue-control pool: %w", err)
	}

	pools := &RuntimePools{Domain: domainPool, QueueControl: queuePool}
	if runtimeConfig.RequireCoordinator {
		coordinatorConfig := DefaultConfig(runtimeConfig.CoordinatorURI)
		coordinatorConfig.MaxConns = runtimeConfig.CoordinatorMaxConns
		coordinatorPool, err := New(ctx, coordinatorConfig)
		if err != nil {
			queuePool.Close()
			domainPool.Close()
			return nil, fmt.Errorf("open coordinator pool: %w", err)
		}
		pools.Coordinator = coordinatorPool
	}
	return pools, nil
}

func (p *RuntimePools) Ping(ctx context.Context) error {
	if p == nil || p.Domain == nil || p.QueueControl == nil {
		return ErrUnavailable
	}
	if err := p.Domain.Ping(ctx); err != nil {
		return ErrUnavailable
	}
	if err := p.QueueControl.Ping(ctx); err != nil {
		return ErrUnavailable
	}
	// A nil coordinator pool is legitimate for a domain-only process, so it is
	// not pinged; a non-nil one is part of readiness exactly like the others.
	if p.Coordinator != nil {
		if err := p.Coordinator.Ping(ctx); err != nil {
			return ErrUnavailable
		}
	}
	return nil
}

func (p *RuntimePools) Close() {
	if p == nil {
		return
	}
	if p.Coordinator != nil {
		p.Coordinator.Close()
	}
	if p.QueueControl != nil {
		p.QueueControl.Close()
	}
	if p.Domain != nil {
		p.Domain.Close()
	}
}
