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
	ErrRuntimeRolesNotSeparated      = errors.New("queue-control and domain PostgreSQL roles must be distinct")
	ErrRuntimeRoleConfiguration      = errors.New("runtime PostgreSQL role configuration is invalid")
)

// RuntimeConfig describes the two PostgreSQL trust boundaries used by River
// processes. Domain traffic may traverse transaction-mode PgBouncer. Queue
// control must use the bounded direct endpoint proved by the compatibility
// matrix; session mode remains fail-closed until equivalent evidence exists.
type RuntimeConfig struct {
	DomainURI               string
	QueueControlURI         string
	DomainRole              string
	QueueRole               string
	RiverSchema             string
	QueueControlMode        config.QueueControlMode
	DomainTransactionPooler bool
	DomainMaxConns          int32
	QueueMaxConns           int32
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
		DomainRole:              configValue.DomainDatabaseRole,
		QueueRole:               configValue.QueueDatabaseRole,
		RiverSchema:             configValue.RiverDatabaseSchema,
		QueueControlMode:        configValue.QueueDatabaseMode,
		DomainTransactionPooler: configValue.DomainTransactionPooler,
		DomainMaxConns:          configValue.DomainDatabaseMaxConns,
		QueueMaxConns:           configValue.QueueDatabaseMaxConns,
	}
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
	return nil
}

func (c RuntimeConfig) SafeAttributes() map[string]any {
	return map[string]any{
		"domain_configured":             c.DomainURI != "",
		"queue_control_configured":      c.QueueControlURI != "",
		"domain_database_role":          c.DomainRole,
		"queue_database_role":           c.QueueRole,
		"river_database_schema":         c.RiverSchema,
		"queue_control_mode":            c.QueueControlMode,
		"domain_transaction_pooler":     c.DomainTransactionPooler,
		"domain_max_connections":        c.DomainMaxConns,
		"queue_control_max_connections": c.QueueMaxConns,
		"total_max_connections":         c.DomainMaxConns + c.QueueMaxConns,
	}
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

// RuntimePools owns separate bounded pools for domain and River queue-control
// traffic. Creating these pools never applies schema migrations.
type RuntimePools struct {
	Domain       *pgxpool.Pool
	QueueControl *pgxpool.Pool
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

	return &RuntimePools{Domain: domainPool, QueueControl: queuePool}, nil
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
	return nil
}

func (p *RuntimePools) Close() {
	if p == nil {
		return
	}
	if p.QueueControl != nil {
		p.QueueControl.Close()
	}
	if p.Domain != nil {
		p.Domain.Close()
	}
}
