package providersync

import (
	"context"
	"log/slog"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The reasons an ownership close is skipped. Each one is a DegradedLeg reason
// on the run's result and a value of the ownership_close_skipped WARN line.
const (
	OwnershipCloseSkippedListingIncomplete = "listing_incomplete"
	OwnershipCloseSkippedScopeShared       = "scope_shared"
	OwnershipCloseSkippedCensusUnavailable = "scope_census_unavailable"
	OwnershipCloseSkippedCensusFailed      = "scope_census_failed"
	// OwnershipCloseSkippedNoTeamListed: no team listing returned at all.
	OwnershipCloseSkippedNoTeamListed = "no_team_listed"

	ownershipCloseLeg = "ownership_close"
)

// ownershipListingProvesEnd reports whether one team's listing may close that
// team's open ownership rows: the walker stopped on the provider's own
// end-of-list signal and no bound stopped the walk first.
func ownershipListingProvesEnd(pages providerfoundation.PageCollection) bool {
	return pages.EndProven && !pages.PageBudgetExhausted && !pages.ItemCapReached
}

// OwnershipScopeCensus counts the active integrations of a provider in an org,
// other than the run's own. A failed read is an error, never zero.
type OwnershipScopeCensus interface {
	CountActiveSiblingIntegrations(ctx context.Context, orgID, provider, integrationID string) (int, error)
}

type ownershipCloseRequest struct {
	ref      TeamCatalogReference
	provider string
	// listed is every team whose listing returned; unproven is the part of it
	// whose end the provider did not confirm.
	listed, unproven []string
}

type ownershipCloseDecision struct {
	// read is every listed team: its open rows give the first-seen valid_from.
	read []string
	// closable is the part of read whose open rows a snapshot may close.
	closable []string
	legs     []DegradedLeg
	// proven counts the listed teams whose listing proved its end.
	proven int
	// skipped holds every reason the gate gave up a close for.
	skipped map[string]bool
}

// decideOwnershipClose is the one gate in front of every provider_access
// ownership close (GitHub and GitLab). A close runs only on a listing proven
// complete, and only when the org has no other active integration of the same
// provider. Rows carry no integration key, and two integrations' configured
// scopes cannot be compared safely (a subgroup, a numeric id, another spelling
// of one path), so any other integration of the provider may list the same
// teams: its rows stay open. Every skip is one WARN line and a DegradedLeg on
// the result, never silent.
func decideOwnershipClose(ctx context.Context, census OwnershipScopeCensus, request ownershipCloseRequest) ownershipCloseDecision {
	decision := ownershipCloseDecision{read: request.listed, skipped: map[string]bool{}}
	if len(request.listed) == 0 {
		return decision
	}
	unproven := make(map[string]bool, len(request.unproven))
	for _, teamID := range request.unproven {
		unproven[teamID] = true
	}
	var reasons []string
	skip := func(reason, detail string) {
		reasons = append(reasons, reason)
		decision.skipped[reason] = true
		decision.legs = append(decision.legs, DegradedLeg{
			Dataset: "team_ownership", Leg: ownershipCloseLeg, Outcome: "skipped", Reason: reason, Detail: detail,
		})
	}
	for _, teamID := range request.listed {
		if !unproven[teamID] {
			decision.closable = append(decision.closable, teamID)
		}
	}
	decision.proven = len(decision.closable)
	unprovenListed := len(request.listed) - len(decision.closable)
	if unprovenListed > 0 {
		skip(OwnershipCloseSkippedListingIncomplete, strconv.Itoa(unprovenListed)+" team listings without a confirmed end")
	}
	var siblingsSharing int
	censusError := ""
	if len(decision.closable) > 0 {
		switch {
		case census == nil || strings.TrimSpace(request.ref.IntegrationID) == "":
			skip(OwnershipCloseSkippedCensusUnavailable, "no integration census for this run")
			decision.closable = nil
		default:
			siblings, err := census.CountActiveSiblingIntegrations(ctx, request.ref.OrgID, request.provider, request.ref.IntegrationID)
			if err != nil {
				censusError = err.Error()
				skip(OwnershipCloseSkippedCensusFailed, censusError)
				decision.closable = nil
				break
			}
			if siblings != 0 {
				siblingsSharing = siblings
				skip(OwnershipCloseSkippedScopeShared, strconv.Itoa(siblings)+" other active integrations of this provider in the org")
				decision.closable = nil
			}
		}
	}
	if len(reasons) > 0 {
		slog.Default().WarnContext(ctx, "ownership_close_skipped",
			"org_id", request.ref.OrgID, "provider", request.provider, "reasons", strings.Join(reasons, ","),
			"teams_listed", len(request.listed), "teams_unproven", unprovenListed,
			"teams_closable", len(decision.closable), "integrations_sharing_scope", siblingsSharing,
			"error", censusError)
	}
	return decision
}

// snapshot is the grant kind of the closable teams (kind makes it from the
// closable set) with the proof of this gate: a team listing returned, at least
// one listing proved its end, and the scope census allowed the close. A team
// outside closable is of no kind, so its open rows never close.
func (decision ownershipCloseDecision) snapshot(kind func(closable []string) SnapshotKind[OwnershipSnapshotRow]) KindSnapshot[OwnershipSnapshotRow] {
	return kind(decision.closable).Snapshot(ProveSnapshot(
		SnapshotTerm{Holds: len(decision.read) > 0, Reason: OwnershipCloseSkippedNoTeamListed},
		SnapshotTerm{Holds: len(decision.read) == 0 || decision.proven > 0, Reason: OwnershipCloseSkippedListingIncomplete},
		SnapshotTerm{Holds: !decision.skipped[OwnershipCloseSkippedCensusUnavailable], Reason: OwnershipCloseSkippedCensusUnavailable},
		SnapshotTerm{Holds: !decision.skipped[OwnershipCloseSkippedCensusFailed], Reason: OwnershipCloseSkippedCensusFailed},
		SnapshotTerm{Holds: !decision.skipped[OwnershipCloseSkippedScopeShared], Reason: OwnershipCloseSkippedScopeShared},
	))
}
