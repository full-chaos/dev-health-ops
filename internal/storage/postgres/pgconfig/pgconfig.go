// Package pgconfig adapts the platform config to postgres.RuntimeConfig. It
// lives apart from internal/storage/postgres so that package, which the auth
// binaries link, does not import internal/platform/config and through it the
// ClickHouse driver.
package pgconfig

import (
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// RuntimeConfig builds the postgres runtime config from the platform config.
func RuntimeConfig(configValue config.Config) postgres.RuntimeConfig {
	return postgres.RuntimeConfig{
		DomainURI:               configValue.DomainDatabaseURI.Reveal(),
		QueueControlURI:         configValue.QueueDatabaseURI.Reveal(),
		CoordinatorURI:          configValue.CoordinatorDatabaseURI.Reveal(),
		DomainRole:              configValue.DomainDatabaseRole,
		QueueRole:               configValue.QueueDatabaseRole,
		CoordinatorRole:         configValue.CoordinatorDatabaseRole,
		RiverSchema:             configValue.RiverDatabaseSchema,
		QueueControlMode:        configValue.QueueDatabaseMode,
		CoordinatorMode:         configValue.CoordinatorDatabaseMode,
		DomainTransactionPooler: configValue.DomainTransactionPooler,
		DomainMaxConns:          configValue.DomainDatabaseMaxConns,
		QueueMaxConns:           configValue.QueueDatabaseMaxConns,
		CoordinatorMaxConns:     configValue.CoordinatorDatabaseMaxConns,
	}
}
