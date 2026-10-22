package deploymentcontract

import (
	"slices"
	"strings"
)

// classDSNSecrets is the closed table of database DSN secrets each process
// class may carry, keyed by the manifest runtime ("river", "control",
// "stream") or by "operator" and "migration". It is the one source for the
// manifest validator and for the rendered chart checks (AllowedDSNSecrets).
var classDSNSecrets = map[string][]string{
	"river":     {"POSTGRES_URI", "WORKER_DATABASE_URI"},
	"control":   {"COORDINATOR_DATABASE_URI", "POSTGRES_URI", "WORKER_DATABASE_URI"},
	"stream":    {"POSTGRES_URI"},
	"operator":  {"COORDINATOR_DATABASE_URI", "POSTGRES_URI", "WORKER_DATABASE_URI"},
	"migration": {"MIGRATION_DATABASE_URI"},
}

// componentDSNSecrets maps the prefix of a DSN's component-form variables to
// the DSN secret they assemble, so a class carries its login whichever form
// the deployment uses.
var componentDSNSecrets = map[string]string{
	"DEV_HEALTH_PG_DOMAIN_":      "POSTGRES_URI",
	"DEV_HEALTH_PG_QUEUE_":       "WORKER_DATABASE_URI",
	"DEV_HEALTH_PG_COORDINATOR_": "COORDINATOR_DATABASE_URI",
	"DEV_HEALTH_PG_API_":         "API_DATABASE_URI",
}

// AllowedDSNSecrets returns the database DSN secret names a class may carry,
// sorted, or nil for a class that is not in the table.
func AllowedDSNSecrets(class string) []string {
	return slices.Clone(classDSNSecrets[class])
}

// DatabaseDSNSecret names the database DSN secret an environment variable
// belongs to, and reports whether it belongs to one. A "_URI" name (optionally
// "_FILE") that mentions DATABASE or POSTGRES is its own secret, so a new
// connection secret falls under the table until it is added to it; a
// component-form variable belongs to the secret it assembles.
func DatabaseDSNSecret(name string) (string, bool) {
	for prefix, secret := range componentDSNSecrets {
		if strings.HasPrefix(name, prefix) {
			return secret, true
		}
	}
	base := strings.TrimSuffix(name, "_FILE")
	if strings.HasSuffix(base, "_URI") && (strings.Contains(base, "DATABASE") || strings.Contains(base, "POSTGRES")) {
		return base, true
	}
	return "", false
}

// ForeignDSNSecrets returns the names in env that belong to a database DSN
// secret outside the class's allowed set.
func ForeignDSNSecrets(class string, env []string) []string {
	allowed := classDSNSecrets[class]
	var foreign []string
	for _, name := range env {
		if secret, ok := DatabaseDSNSecret(name); ok && !slices.Contains(allowed, secret) {
			foreign = append(foreign, name)
		}
	}
	return foreign
}
