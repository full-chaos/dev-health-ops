package workerservice

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// repositoryIdentityOutbox is the provider sync repository identity producer of
// WORKER_REPO_IDENTITY_OUTBOX: nil when off; on, the auth schema of
// AUTH_DATABASE_SCHEMA (default "auth") whose append function the worker domain
// login may execute. A malformed flag or schema name is an error.
func repositoryIdentityOutbox(lookup func(string) (string, bool)) (*providersync.RepositoryIdentityOutbox, error) {
	on, err := providersync.ParseRepositoryIdentityOutbox(lookup)
	if err != nil || !on {
		return nil, err
	}
	raw, _ := lookup(envAuthSchema)
	name := strings.TrimSpace(raw)
	if name == "" {
		name = "auth"
	}
	schema, err := authschema.NewValidatedIdentifier(name)
	if err != nil {
		return nil, err
	}
	return &providersync.RepositoryIdentityOutbox{Schema: schema}, nil
}
