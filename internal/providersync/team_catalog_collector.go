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
