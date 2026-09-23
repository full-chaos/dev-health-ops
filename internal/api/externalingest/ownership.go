package externalingest

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

// matchesInstance ports ownership.py's matches_instance for the
// general (non-"operational" github/gitlab) case: a pure string comparison
// against the managed source's own identifiers. It does NOT port the
// self-hosted-instance credential-host branch (system in {github,gitlab}
// AND entity_family == "operational"): that branch decrypts a linked
// integration credential to resolve a configured host, a dependency this
// area does not yet carry (see doc.go). Scoped narrowly: it applies only
// when neither side's system/entityFamily combination is that branch, which
// is every push kind this ticket ports except operational github/gitlab
// records -- those fall back to "no match", the fail-safe direction (a
// customer_push registration for that narrow case is never silently
// overridden by an unseen managed source, matching CC5's own precedence:
// an explicit external_ingest_sources row already wins unless a managed
// owner is found).
func matchesInstance(system, instance string, source integrationSource, entityFamily string) bool {
	inst := strings.ToLower(strings.TrimSpace(instance))
	if inst == "" {
		return false
	}
	if entityFamily == operationalEntityFamily && (system == "github" || system == "gitlab") {
		return false // named limit: see doc comment
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
// the managed integration_sources row (if any) that actively owns
// (org, system, instance) -- enabled row under an active integration.
func findActiveManagedOwner(ctx context.Context, pool *pgxpool.Pool, orgID, system, instance, entityFamily string) (bool, error) {
	if system == "custom" {
		return false, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT source.external_id, source.full_name, source.name, source.metadata,
		       source.is_enabled, integration.is_active
		FROM integration_sources AS source
		JOIN integrations AS integration ON integration.id = source.integration_id
		WHERE source.org_id = $1 AND lower(source.provider) = $2
	`, orgID, system)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			externalID, fullName, name string
			metadataJSON               []byte
			enabled, active            bool
		)
		if err := rows.Scan(&externalID, &fullName, &name, &metadataJSON, &enabled, &active); err != nil {
			return false, err
		}
		if !enabled || !active {
			continue
		}
		source := integrationSource{
			ExternalID: externalID, FullName: fullName, Name: name,
			Metadata: decodeMetadata(metadataJSON), Enabled: enabled, Active: active,
		}
		if matchesInstance(system, instance, source, entityFamily) {
			return true, nil
		}
	}
	return false, rows.Err()
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
func resolveEffectiveMode(ctx context.Context, pool *pgxpool.Pool, orgID, system, instance, entityFamily string) (effectiveMode, error) {
	var (
		enabled bool
		mode    string
	)
	err := pool.QueryRow(ctx, `
		SELECT enabled, mode FROM external_ingest_sources
		WHERE org_id = $1 AND system = $2 AND instance = $3 AND entity_family = $4
	`, orgID, system, instance, entityFamily).Scan(&enabled, &mode)
	switch {
	case err == pgx.ErrNoRows:
		owned, ownerErr := findActiveManagedOwner(ctx, pool, orgID, system, instance, entityFamily)
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
	owned, err := findActiveManagedOwner(ctx, pool, orgID, system, instance, entityFamily)
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
