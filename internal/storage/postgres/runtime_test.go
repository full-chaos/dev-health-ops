package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
)

func TestRuntimeConfigRequiresDirectSeparatedPools(t *testing.T) {
	t.Parallel()

	valid := DefaultRuntimeConfig(
		"postgres://domain_role:domain-secret@pgbouncer.internal/app",
		"postgres://queue_role:queue-secret@postgres.internal/app",
		"domain_role",
		"queue_role",
	)
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config failed: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*RuntimeConfig)
		want   error
	}{
		{name: "domain missing", mutate: func(c *RuntimeConfig) { c.DomainURI = "" }, want: ErrDomainDatabaseRequired},
		{name: "queue missing", mutate: func(c *RuntimeConfig) { c.QueueControlURI = "" }, want: ErrQueueControlRequired},
		{name: "transaction queue", mutate: func(c *RuntimeConfig) { c.QueueControlMode = platformconfig.QueueControlTransaction }, want: ErrQueueControlTransactionMode},
		{name: "unverified session queue", mutate: func(c *RuntimeConfig) { c.QueueControlMode = platformconfig.QueueControlSession }, want: ErrQueueControlSessionUnverified},
		{name: "shared configured role", mutate: func(c *RuntimeConfig) { c.QueueRole = "domain_role" }, want: ErrRuntimeRolesNotSeparated},
		{name: "domain DSN role mismatch", mutate: func(c *RuntimeConfig) { c.DomainRole = "other_role" }, want: ErrRuntimeRoleConfiguration},
		{name: "queue DSN role mismatch", mutate: func(c *RuntimeConfig) { c.QueueControlURI = "postgres://other_role:other@postgres.internal/app" }, want: ErrRuntimeRoleConfiguration},
		{name: "invalid role name", mutate: func(c *RuntimeConfig) { c.QueueRole = "Queue-Bad" }, want: ErrRuntimeRoleConfiguration},
		{name: "same transaction endpoint", mutate: func(c *RuntimeConfig) {
			c.DomainTransactionPooler = true
			c.QueueControlURI = "postgres://queue_role:other@pgbouncer.internal/app"
		}, want: ErrQueueControlTransactionMode},
		{name: "queue over budget", mutate: func(c *RuntimeConfig) { c.QueueMaxConns = 5 }, want: ErrInvalidConfig},
		{name: "domain over budget", mutate: func(c *RuntimeConfig) { c.DomainMaxConns = 17 }, want: ErrInvalidConfig},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := valid
			test.mutate(&candidate)
			if err := candidate.Validate(); !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestRuntimeConfigSafeSurfaceContainsOnlyBudgets(t *testing.T) {
	t.Parallel()

	const domainSecret = "domain-never-log"
	const queueSecret = "queue-never-log"
	cfg := DefaultRuntimeConfig(
		"postgres://domain_role:"+domainSecret+"@pgbouncer.internal/app",
		"postgres://queue_role:"+queueSecret+"@postgres.internal/app",
		"domain_role",
		"queue_role",
	)
	surface := fmt.Sprint(cfg.SafeAttributes(), cfg.Validate())
	for _, forbidden := range []string{domainSecret, queueSecret, cfg.DomainURI, cfg.QueueControlURI} {
		if strings.Contains(surface, forbidden) {
			t.Fatalf("safe surface exposed %q: %s", forbidden, surface)
		}
	}
	for _, expected := range []string{"domain_database_role:domain_role", "queue_database_role:queue_role", "river_database_schema:river", "domain_max_connections:4", "queue_control_max_connections:2", "total_max_connections:6"} {
		if !strings.Contains(surface, expected) {
			t.Fatalf("safe surface missing %q: %s", expected, surface)
		}
	}
}
