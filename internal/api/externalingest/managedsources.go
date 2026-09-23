package externalingest

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// ErrOwnershipResolutionUnavailable is
// OperationalOwnershipResolutionUnavailableError: an enabled managed source
// under an active integration needs a host this process cannot read.
var ErrOwnershipResolutionUnavailable = errors.New("operational ownership resolution is unavailable")

// OwnershipResolutionUnavailableMessage is ownership.py
// OWNERSHIP_RESOLUTION_UNAVAILABLE_MESSAGE.
const OwnershipResolutionUnavailableMessage = "Ownership could not be determined because a linked managed credential is " +
	"unreadable or does not declare an instance host. Repair the managed " +
	"integration before enabling customer push."

// ErrInvalidOperationalInstance is InvalidOperationalProviderInstanceError,
// which the Python callers do not handle (an unhandled 500).
var ErrInvalidOperationalInstance = errors.New("invalid operational provider instance")

// ManagedSourceMatch is one find_matching_managed_sources result: the
// integration_sources row and whether its integration is active.
type ManagedSourceMatch struct {
	ID                uuid.UUID
	Enabled           bool
	IntegrationActive bool
}

// RowsQueryer is the query surface FindMatchingManagedSources needs; a pool
// and a pgx.Tx both satisfy it.
type RowsQueryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// FindMatchingManagedSources is external_ingest/ownership.py
// find_matching_managed_sources: every managed integration_sources row of
// the org whose provider (lower-cased) is system and whose instance
// predicate matches, in the query's row order, with its integration's
// is_active. "custom" has no managed equivalent.
//
// For an operational github/gitlab source whose integration names no host
// in its config, Python reads the host from the integration's credential
// (credential_id set; see credentialHost) or from GITHUB_URL/GITLAB_URL (no
// credential; getenv). When that host is not resolved, a non-default
// instance under an enabled source and active integration is
// ErrOwnershipResolutionUnavailable; otherwise the default host is
// compared.
//
// An instance or managed host that normalized_operational_provider_instance
// refuses is ErrInvalidOperationalInstance, raised where Python raises it.
func FindMatchingManagedSources(
	ctx context.Context, q RowsQueryer, getenv func(string) (string, bool), orgID, system, instance, entityFamily string,
) ([]ManagedSourceMatch, error) {
	if system == "custom" {
		return nil, nil
	}
	rows, err := q.Query(ctx, `
		SELECT source.id, source.external_id, source.full_name, source.name, source.metadata,
		       source.is_enabled, integration.is_active, integration.config, integration.credential_id
		FROM integration_sources AS source
		JOIN integrations AS integration ON source.integration_id = integration.id
		WHERE source.org_id = $1 AND lower(source.provider) = $2
	`, orgID, system)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		match        ManagedSourceMatch
		source       integrationSource
		config       map[string]any
		credentialID *uuid.UUID
	}
	var candidates []candidate
	for rows.Next() {
		var (
			c                        candidate
			externalID, fullName     *string
			name                     *string
			metadataJSON, configJSON []byte
		)
		if err := rows.Scan(&c.match.ID, &externalID, &fullName, &name, &metadataJSON,
			&c.match.Enabled, &c.match.IntegrationActive, &configJSON, &c.credentialID); err != nil {
			rows.Close()
			return nil, err
		}
		c.source = integrationSource{
			ExternalID: deref(externalID), FullName: deref(fullName), Name: deref(name),
			Metadata: decodeMetadata(metadataJSON), Enabled: c.match.Enabled, Active: c.match.IntegrationActive,
		}
		c.config = decodeMetadata(configJSON)
		candidates = append(candidates, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	operational := (entityFamily == "operational" || entityFamily == "operational_incident") &&
		(system == "github" || system == "gitlab")
	defaultHost := "github.com"
	if system == "gitlab" {
		defaultHost = "gitlab.com"
	}
	var matches []ManagedSourceMatch
	for _, c := range candidates {
		if !operational {
			if matchesInstance(system, instance, c.source, entityFamily, c.config) {
				matches = append(matches, c.match)
			}
			continue
		}
		managedHost := defaultHost
		configured, hasConfigured := configuredHost(c.config, system)
		switch {
		case hasConfigured:
			managedHost = configured
		case c.credentialID != nil:
			base, resolved, err := credentialHost(ctx, q, orgID, system, *c.credentialID)
			if err != nil {
				return nil, err
			}
			if !resolved {
				instanceHost, ok := OperationalProviderInstance(system, instance)
				if !ok {
					return nil, ErrInvalidOperationalInstance
				}
				defaultNormalized, _ := OperationalProviderInstance(system, defaultHost)
				if instanceHost != defaultNormalized && c.match.Enabled && c.match.IntegrationActive {
					return nil, ErrOwnershipResolutionUnavailable
				}
			} else {
				managedHost = base
			}
		default:
			base, set := getenv(environmentBaseURLVariable(system))
			if set && pythonparity.Strip(base) != "" {
				if _, ok := OperationalProviderInstance(system, base); !ok {
					if c.match.Enabled && c.match.IntegrationActive {
						return nil, ErrOwnershipResolutionUnavailable
					}
				} else {
					managedHost = base
				}
			}
		}
		if pythonparity.Strip(instance) == "" {
			continue
		}
		left, ok := OperationalProviderInstance(system, instance)
		if !ok {
			return nil, ErrInvalidOperationalInstance
		}
		right, ok := OperationalProviderInstance(system, managedHost)
		if !ok {
			return nil, ErrInvalidOperationalInstance
		}
		if left == right {
			matches = append(matches, c.match)
		}
	}
	return matches, nil
}

// configuredHost is `config.get(f"{system}_instance_url") or
// config.get(f"{system}_url")`, kept only when it is a str that is not
// blank: the first key wins whenever its value is truthy, even when that
// value is not a str.
func configuredHost(config map[string]any, system string) (string, bool) {
	value := config[system+"_instance_url"]
	if !truthy(value) {
		value = config[system+"_url"]
	}
	text, ok := value.(string)
	if !ok || pythonparity.Strip(text) == "" {
		return "", false
	}
	return text, true
}

// truthy is Python's bool() of a JSON-decoded value.
func truthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case float64:
		return typed != 0
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	default:
		return true
	}
}

// environmentBaseURLVariable is credentials/resolver.py PROVIDER_ENV_VARS'
// base_url name for github and gitlab.
func environmentBaseURLVariable(system string) string {
	if system == "gitlab" {
		return "GITLAB_URL"
	}
	return "GITHUB_URL"
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// credentialHost is ownership.py _credential_host_resolution for a
// credential whose encrypted payload is absent or does not decrypt
// (credentials None): the org's integration_credentials row must exist and
// name the same provider (case-folded), and the first non-blank str of its
// config's "<system>_url", "url" and "base_url" is the host when
// normalized_operational_provider_instance accepts it; anything else is
// unresolved. This process holds no decryption key, so a payload that
// Python decrypts is treated as absent too (named limit: Python then reads
// the decrypted values' "<system>_url"/"url"/"base_url" first, and refuses
// a decrypted value that is not a mapping).
func credentialHost(ctx context.Context, q RowsQueryer, orgID, system string, credentialID uuid.UUID) (string, bool, error) {
	rows, err := q.Query(ctx, `SELECT provider, config FROM integration_credentials WHERE org_id = $1 AND id = $2`,
		orgID, credentialID)
	if err != nil {
		return "", false, err
	}
	var (
		provider   string
		configJSON []byte
		found      bool
	)
	for rows.Next() {
		if err := rows.Scan(&provider, &configJSON); err != nil {
			rows.Close()
			return "", false, err
		}
		found = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	if !found || pythonparity.Fold(provider) != system {
		return "", false, nil
	}
	config := decodeMetadata(configJSON)
	for _, key := range []string{system + "_url", "url", "base_url"} {
		candidate, ok := config[key].(string)
		if !ok || pythonparity.Strip(candidate) == "" {
			continue
		}
		if _, valid := OperationalProviderInstance(system, candidate); valid {
			return candidate, true, nil
		}
	}
	return "", false, nil
}
