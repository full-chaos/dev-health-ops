package externalingest

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// effectiveMode mirrors ownership.py's EffectiveMode.
type effectiveMode string

const (
	modeFullchaosSync effectiveMode = "fullchaos_sync"
	modeCustomerPush  effectiveMode = "customer_push"
	modeDisabled      effectiveMode = "disabled"
	modeUnclaimed     effectiveMode = "unclaimed"
)

// integrationSource is the subset of integrations/integration_sources this
// package needs to answer "does a managed sync actively own this instance".
type integrationSource struct {
	ExternalID string
	FullName   string
	Name       string
	// MetadataRaw is the source.metadata JSONB, decoded where it is read: Python
	// reads `(source.metadata_ or {}).get(...)` only in the gitlab and linear
	// branches, so a metadata value that is not an object raises only there.
	MetadataRaw []byte
	Enabled     bool
	Active      bool
}

// matchesInstance is ownership.py's matches_instance, statement for statement:
// every str method is Python's (strip and lower over code points), the
// per-provider branches read the source's metadata and the integration's config
// only where Python reads them, and a read that raises in Python is the typed
// error here. credentialBaseURL is find_matching_managed_sources' value for an
// operational github/gitlab row ("" = None): the linked credential's or the
// environment's host, used only when the integration config names none.
func matchesInstance(system, instance string, source integrationSource, entityFamily string, integrationConfig pyConfig, credentialBaseURL string) (bool, error) {
	inst := pythonparity.Lower(pythonparity.Strip(instance))
	if inst == "" {
		return false, nil
	}
	if (entityFamily == "operational" || entityFamily == "operational_incident") && (system == "github" || system == "gitlab") {
		defaultHost := "github.com"
		if system == "gitlab" {
			defaultHost = "gitlab.com"
		}
		managedHost := defaultHost
		if configured, named := configuredHost(integrationConfig, system); named {
			managedHost = configured
		} else if credentialBaseURL != "" {
			managedHost = credentialBaseURL
		}
		left, ok := OperationalProviderInstance(system, instance)
		if !ok {
			return false, ErrInvalidOperationalInstance
		}
		right, ok := OperationalProviderInstance(system, managedHost)
		if !ok {
			return false, ErrInvalidOperationalInstance
		}
		return left == right, nil
	}
	switch system {
	case "github", "jira":
		return candidateSet(source.ExternalID, source.FullName)[inst], nil
	case "gitlab":
		metadata, err := decodePyConfig(source.MetadataRaw)
		if err != nil {
			return false, err
		}
		pathWithNamespace, _ := metadata.str("path_with_namespace")
		return candidateSet(source.FullName, pathWithNamespace, source.ExternalID)[inst], nil
	case "linear":
		orgWide, err := linearIsOrgWidePlaceholder(source)
		if err != nil {
			return false, err
		}
		if orgWide {
			return true, nil
		}
		return candidateSet(source.ExternalID, source.FullName, source.Name)[inst], nil
	default:
		return false, nil
	}
}

func linearIsOrgWidePlaceholder(source integrationSource) (bool, error) {
	metadata, err := decodePyConfig(source.MetadataRaw)
	if err != nil {
		return false, err
	}
	if metadata.isTrue("org_wide_placeholder") {
		return true, nil
	}
	return pythonparity.Lower(pythonparity.Strip(source.ExternalID)) == "linear", nil
}

func candidateSet(values ...string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, v := range values {
		if trimmed := pythonparity.Lower(pythonparity.Strip(v)); trimmed != "" {
			set[trimmed] = true
		}
	}
	return set
}

// findActiveManagedOwner ports ownership.py's find_active_managed_owner:
// whether a managed integration_sources row (enabled, under an active
// integration) actively owns (org, system, instance), through the one
// find_matching_managed_sources port shared with customer-push registration
// (FindMatchingManagedSources: the host an operational github/gitlab source
// is compared on comes from its integration's config, its credential or the
// environment).
func findActiveManagedOwner(ctx context.Context, d Deps, orgID, system, instance, entityFamily string) (bool, error) {
	matches, err := FindMatchingManagedSources(ctx, d.Pool, d.getenv(), d.Cipher, orgID, system, instance, entityFamily)
	if err != nil {
		return false, err
	}
	for _, match := range matches {
		if match.Enabled && match.IntegrationActive {
			return true, nil
		}
	}
	return false, nil
}

// resolveEffectiveMode ports ownership.py's resolve_effective_mode
// precedence exactly (an explicit external_ingest_sources row wins unless a
// managed source actively owns the same instance; with no explicit row, an
// active managed owner implies fullchaos_sync; otherwise unclaimed).
func resolveEffectiveMode(ctx context.Context, d Deps, orgID, system, instance, entityFamily string) (effectiveMode, error) {
	var (
		enabled bool
		mode    string
	)
	err := d.Pool.QueryRow(ctx, `
		SELECT enabled, mode FROM external_ingest_sources
		WHERE org_id = $1 AND system = $2 AND instance = $3 AND entity_family = $4
	`, orgID, system, instance, entityFamily).Scan(&enabled, &mode)
	switch {
	case err == pgx.ErrNoRows:
		owned, ownerErr := findActiveManagedOwner(ctx, d, orgID, system, instance, entityFamily)
		if ownerErr != nil {
			return "", ownerErr
		}
		if owned {
			return modeFullchaosSync, nil
		}
		return modeUnclaimed, nil
	case err != nil:
		return "", err
	}

	if !enabled || mode == "disabled" {
		return modeDisabled, nil
	}
	if mode == "fullchaos_sync" {
		return modeFullchaosSync, nil
	}
	owned, err := findActiveManagedOwner(ctx, d, orgID, system, instance, entityFamily)
	if err != nil {
		return "", err
	}
	if owned {
		return modeFullchaosSync, nil
	}
	return modeCustomerPush, nil
}

func ownershipError(mode effectiveMode, system, instance string) *ingestError {
	switch mode {
	case modeUnclaimed:
		return newIngestError(403, "source_not_registered",
			"No ingest source is registered for system='"+system+"' instance='"+instance+
				"' in this organization. Register it under /org/admin/integrations before pushing.")
	case modeDisabled:
		return newIngestError(403, "source_disabled",
			"Ingest source '"+system+":"+instance+"' is disabled for this organization.")
	default:
		return newIngestError(403, "source_owned_by_fullchaos_sync",
			"Source '"+system+":"+instance+"' is currently managed by FullChaos-hosted sync. "+
				"Disable managed sync for this source before pushing customer data, or contact support.")
	}
}
