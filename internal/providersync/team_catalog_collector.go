package providersync

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TeamCatalogSelections mirrors CHAOS-4323's three independent org-level
// reference-catalog toggles (sync_configurations.sync_options:
// auto_import_teams / auto_import_projects / auto_import_members). A false
// selection means that surface must not be collected or written for this
// provider, by any collector -- native or Python bridge.
type TeamCatalogSelections struct {
	Teams    bool
	Projects bool
	Members  bool
}

// Any reports whether at least one surface is selected. A caller with no
// selections enabled must skip this provider entirely (native collector and
// bridge alike) rather than calling a collector that would write nothing.
func (selections TeamCatalogSelections) Any() bool {
	return selections.Teams || selections.Projects || selections.Members
}

// TeamCatalogReference is the claim-free identifier set a native team/member/
// project-catalog collector needs (CHAOS-4431 ruling, team-lead 2026-08-28,
// option (c)). Team/project/member reference discovery runs once per sync
// run per provider -- it is never a claimed provider-unit, so there is
// deliberately no Claim and no lease here.
type TeamCatalogReference struct {
	OrgID         string
	SyncRunID     string
	IntegrationID string
	SourceID      string
	// SyncOptions is the run's own canonical sync_configurations.sync_options
	// (team-lead ruling, 2026-08-28): provider-specific config a collector
	// needs beyond the resolved credential -- e.g. GitLab's group_path
	// (credential.Config carries only auth material, never scope). Collectors
	// that need nothing beyond the credential (Linear today) simply ignore
	// it. Never nil when populated by a real resolver; may be nil in tests.
	SyncOptions map[string]any
	// Strict mirrors which Python call shape this collection run is
	// standing in for: true from the reference-discovery seam
	// (run_team_autoimport_strict -- propagates failures, no per-surface
	// selection gate upstream of the collector) and false from the
	// post-sync seam (run_team_autoimport -- the caller, not the collector,
	// degrades a failure to a non-fatal zero result; see
	// cmd/dev-health-worker/team_catalog_clients.go's teamCatalogAutoimportBridge).
	// A collector MAY use it to decide per-item soft-fail vs hard-fail
	// internally; none do yet (CHAOS-4431's Linear collector always
	// propagates, which already matches strict semantics, and the
	// non-strict safety net lives at the bridge boundary instead).
	Strict bool
}

func (ref TeamCatalogReference) validate() error {
	if ref.OrgID == "" || ref.SyncRunID == "" {
		return ErrInvalidConfiguration
	}
	return nil
}

// TeamCatalogResult reports rows written per destination table plus an
// outcome label, the shape the telemetry layer (rows_written per table +
// outcome: native|bridge|skipped_selection) reports directly.
type TeamCatalogResult struct {
	TeamsWritten       int
	MembersWritten     int
	MembershipsWritten int
	ProjectsWritten    int
	OwnershipWritten   int
	// TeamKeys is the native_team_key set actually written to `teams` this
	// call (empty when Teams was not selected). It exists so a caller can
	// hand it to the existing ClickHouse readback verifier
	// (ReferenceReadbackVerifier/MissingTeamKeys) the same way the Python
	// bridge path's summary["reference_team_keys"] already does -- CHAOS-4431
	// keeps that verification for native providers too, not just bridge ones.
	TeamKeys []string
}

// TeamCatalogCollector is the shared, provider-neutral seam every native
// team/member/project-catalog collector implements (CHAOS-4431 activates
// Linear as the first; CHAOS-4434/CHAOS-4432 add GitHub/GitLab against this
// same interface). A caller resolves credentials/HTTP client exactly as
// cmd/dev-health-worker/provider_sync.go already does for claimed
// provider-units; this seam itself carries no lease and no Claim.
type TeamCatalogCollector interface {
	CollectTeamCatalog(
		ctx context.Context,
		ref TeamCatalogReference,
		credential providerfoundation.Credential,
		client *providerfoundation.HTTPClient,
		selections TeamCatalogSelections,
		normalizedAt time.Time,
	) (TeamCatalogResult, error)
}
