package externalingest

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
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
	Metadata   map[string]any
	Enabled    bool
	Active     bool
}

// matchesInstance ports ownership.py's matches_instance. For the operational
// github/gitlab branch it covers the explicitly-configured-host and
// default-host cases (integrationConfig's github_instance_url/
// gitlab_instance_url key, or github.com/gitlab.com when absent) but not
// the narrower sub-case of a self-hosted host known only via a DECRYPTED
// managed credential (Python's credential_base_url fallback,
// ownership.py's _credential_host_resolution): that needs a credential-
// decryption dependency this area does not carry (see deps.go's package
// doc). A managed integration with no explicit config key and a self-hosted
// credential-derived host is therefore still unmatched here -- fail-safe in
// the same direction as before (an explicit customer_push row is not
// overridden), just a narrower gap than "every operational github/gitlab
// push", which is what this function covered before.
func matchesInstance(system, instance string, source integrationSource, entityFamily string, integrationConfig pyConfig) bool {
	inst := strings.ToLower(strings.TrimSpace(instance))
	if inst == "" {
		return false
	}
	if entityFamily == operationalEntityFamily && (system == "github" || system == "gitlab") {
		defaultHost := "github.com"
		if system == "gitlab" {
			defaultHost = "gitlab.com"
		}
		managedHost := defaultHost
		if configured, ok := integrationConfig.str(system + "_instance_url"); ok && strings.TrimSpace(configured) != "" {
			managedHost = configured
		} else if configured, ok := integrationConfig.str(system + "_url"); ok && strings.TrimSpace(configured) != "" {
			managedHost = configured
		}
		return normalizedOperationalHost(system, instance) == normalizedOperationalHost(system, managedHost)
	}
	switch system {
	case "github", "jira":
		return candidateSet(source.ExternalID, source.FullName)[inst]
	case "gitlab":
		pathWithNamespace, _ := source.Metadata["path_with_namespace"].(string)
		return candidateSet(source.FullName, pathWithNamespace, source.ExternalID)[inst]
	case "linear":
		if linearIsOrgWidePlaceholder(source) {
			return true
		}
		return candidateSet(source.ExternalID, source.FullName, source.Name)[inst]
	default:
		return false
	}
}

// normalizedOperationalHost is a simplified port of models/
// operational_identity.py's normalized_operational_provider_instance for
// github/gitlab: lowercase hostname, api.github.com aliased to github.com,
// scheme and default port (443/80) stripped. It does not reproduce every
// edge case Python's version rejects (malformed labels, bracketed IPv6,
// non-default schemes) -- those inputs compare unequal here rather than
// being refused outright, which is safe for a host EQUALITY comparison:
// an unparseable value on either side simply fails to match, never
// falsely matches.
func normalizedOperationalHost(system, raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	withScheme := trimmed
	if !strings.Contains(trimmed, "://") {
		withScheme = "//" + trimmed
	}
	parsed, err := url.Parse(withScheme)
	if err != nil || parsed.Hostname() == "" {
		return strings.ToLower(trimmed)
	}
	host := strings.ToLower(parsed.Hostname())
	if system == "github" && (host == "api.github.com" || host == "github.com") {
		return "github.com"
	}
	port := parsed.Port()
	scheme := strings.ToLower(parsed.Scheme)
	if scheme == "" {
		scheme = "https"
	}
	defaultPort := map[string]string{"https": "443", "http": "80"}[scheme]
	if port != "" && port != defaultPort {
		return host + ":" + port
	}
	return host
}

func linearIsOrgWidePlaceholder(source integrationSource) bool {
	if orgWide, ok := source.Metadata["org_wide_placeholder"].(bool); ok && orgWide {
		return true
	}
	return strings.ToLower(strings.TrimSpace(source.ExternalID)) == "linear"
}

func candidateSet(values ...string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, v := range values {
		if trimmed := strings.ToLower(strings.TrimSpace(v)); trimmed != "" {
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

func decodeMetadata(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return m
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
