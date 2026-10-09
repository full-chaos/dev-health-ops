// Package changefailure holds the one definition of change failure rate
// (CHAOS-8981): the share of deployments linked to an incident, measured over
// a view's window and subject.
//
// The writer (the repo_user_commit daily family) counts each repository and
// day with CountDay and stores the counts in repo_change_failure_daily. Every
// reader sums those counts over the view (one repository, a team's owned
// repositories, or the organization) and applies Rate in Go or WindowRateSQL
// in ClickHouse. Both encode the same rule:
//
//   - no stored row in the view                -> no value, no state
//   - no deployments in the view               -> no value, not applicable
//   - no incident tied to the view's subject   -> no value, unknown
//   - else (failed native + failed heuristic) / deployments; 0 is a measured 0
//
// Evaluate returns the value together with its state, and every reader serves
// both: a reader that kept only the value would show unknown, not applicable
// and "never counted" as the same empty answer.
//
// Missing is not healthy: a repository that deploys but has no incident
// evidence must not read 0%.
//
// A deployment is failed when at least one deployment-incident link names it.
// A deployment with a native link counts as native; one with only heuristic
// links counts as heuristic, so a reader can name the lowest tier that
// contributes (LowestTier). An incident ties to a repository directly (a
// service-to-repository mapping row) or, only when it has no direct row at all,
// through the repository of its linked deployment.
package changefailure

import (
	"sort"

	"github.com/google/uuid"
)

// Link tiers, the deployment-incident edge `source` values.
const (
	TierNative    = "native"
	TierHeuristic = "heuristic"
)

// State says why a view has a rate or not. Unknown, not applicable and a
// measured 0 are three different answers, and a reader must be able to show
// which one it has (CHAOS-8981).
type State string

const (
	// StateMeasured: deployments and incident evidence. The value may be 0.
	StateMeasured State = "measured"
	// StateNotApplicable: stored counts, and no deployment in the view.
	StateNotApplicable State = "not_applicable_no_deployments"
	// StateUnknown: deployments exist, but no incident ties to the view's
	// subject in its window.
	StateUnknown State = "unknown_no_incident_evidence"
	// StateNoStoredCounts: the view holds no stored row at all, so nothing
	// was counted for it: the days were never computed, or no repository of
	// the subject had a deployment or an incident. It is the empty string: a
	// reader serves it as a null state.
	StateNoStoredCounts State = ""
)

// Outcome is the answer for a view: the rate when it is measured, the state
// that says why it is or is not, and the weakest link tier that contributes a
// failed deployment to a measured rate ("" when none failed).
type Outcome struct {
	Value    *float64
	State    State
	LinkTier string
}

// StateOrNil is the state as a reader serves it: nil for StateNoStoredCounts.
func (o Outcome) StateOrNil() *string {
	if o.State == StateNoStoredCounts {
		return nil
	}
	state := string(o.State)
	return &state
}

// LinkTierOrNil is the link tier as a reader serves it: nil when there is
// none.
func (o Outcome) LinkTierOrNil() *string {
	if o.LinkTier == "" {
		return nil
	}
	tier := o.LinkTier
	return &tier
}

// View is the summed counts of a view (a window and a subject) and the number
// of stored rows behind the sum.
type View struct {
	Counts
	StoredRows uint64
}

// Evaluate is the one rule every reader applies to a view. A view with no
// stored row has no state; any other view has the Rate of its counts.
func Evaluate(v View) Outcome {
	if v.StoredRows == 0 {
		return Outcome{State: StateNoStoredCounts}
	}
	return Rate(v.Counts)
}

// Deployment is one deployment of a repository on the counted day.
type Deployment struct {
	RepoID       uuid.UUID
	DeploymentID string
}

// IncidentTie ties one incident to one repository. ViaDeployment is false for
// a direct tie (a service-to-repository mapping row) and true when the only
// path to the repository is the incident's linked deployment.
type IncidentTie struct {
	RepoID        uuid.UUID
	IncidentID    string
	ViaDeployment bool
}

// Link is one deployment-incident link. RepoID is the repository of the
// deployment. Source is the link tier; any value other than TierNative counts
// as heuristic, so an unexpected tier is never presented as native.
type Link struct {
	RepoID       uuid.UUID
	DeploymentID string
	IncidentID   string
	Source       string
}

// Counts are the stored inputs of one repository and day, or their sum over
// a view.
type Counts struct {
	Deployments            uint64
	FailedNative           uint64
	FailedHeuristic        uint64
	IncidentsDirect        uint64
	IncidentsViaDeployment uint64
}

// Add returns the sum of two counts.
func (c Counts) Add(o Counts) Counts {
	return Counts{
		Deployments:            c.Deployments + o.Deployments,
		FailedNative:           c.FailedNative + o.FailedNative,
		FailedHeuristic:        c.FailedHeuristic + o.FailedHeuristic,
		IncidentsDirect:        c.IncidentsDirect + o.IncidentsDirect,
		IncidentsViaDeployment: c.IncidentsViaDeployment + o.IncidentsViaDeployment,
	}
}

// Empty reports a repository and day with nothing to store: no deployment and
// no incident. Such a day gets no row (never a zero-filled one).
func (c Counts) Empty() bool {
	return c.Deployments == 0 && c.IncidentsDirect == 0 && c.IncidentsViaDeployment == 0
}

// Rate applies the rule of this package to the counts of stored rows.
func Rate(c Counts) Outcome {
	if c.Deployments == 0 {
		return Outcome{State: StateNotApplicable}
	}
	if c.IncidentsDirect+c.IncidentsViaDeployment == 0 {
		return Outcome{State: StateUnknown}
	}
	value := float64(c.FailedNative+c.FailedHeuristic) / float64(c.Deployments)
	return Outcome{Value: &value, State: StateMeasured, LinkTier: LowestTier(c)}
}

// LowestTier names the lowest link tier that contributes a failed deployment:
// TierHeuristic when any heuristic-only deployment counts, TierNative when only
// native ones do, "" when no deployment failed.
func LowestTier(c Counts) string {
	switch {
	case c.FailedHeuristic > 0:
		return TierHeuristic
	case c.FailedNative > 0:
		return TierNative
	default:
		return ""
	}
}

// CountDay counts one day per repository.
//
// deployments are the day's deployments; ties are the incidents that started
// on the day with their repository ties; links are the deployment-incident
// links of those incidents. A via-deployment tie of an incident that has a
// direct tie to any repository is ignored. A link counts only when its
// incident ties to some repository and its deployment is one of the day's
// deployments of that repository, so a link can never make a deployment failed
// on a day it was not deployed. A repository with nothing to count is absent
// from the result.
func CountDay(deployments []Deployment, ties []IncidentTie, links []Link) map[uuid.UUID]Counts {
	type deploymentKey struct {
		repoID       uuid.UUID
		deploymentID string
	}
	deployed := make(map[deploymentKey]struct{}, len(deployments))
	for _, d := range deployments {
		if d.DeploymentID == "" {
			continue
		}
		deployed[deploymentKey{d.RepoID, d.DeploymentID}] = struct{}{}
	}

	directIncidents := make(map[string]struct{})
	for _, tie := range ties {
		if !tie.ViaDeployment && tie.IncidentID != "" {
			directIncidents[tie.IncidentID] = struct{}{}
		}
	}
	type tieKey struct {
		repoID     uuid.UUID
		incidentID string
	}
	direct := make(map[tieKey]struct{})
	via := make(map[tieKey]struct{})
	tied := make(map[string]struct{})
	for _, tie := range ties {
		if tie.IncidentID == "" {
			continue
		}
		key := tieKey{tie.RepoID, tie.IncidentID}
		if !tie.ViaDeployment {
			direct[key] = struct{}{}
			tied[tie.IncidentID] = struct{}{}
			continue
		}
		if _, hasDirect := directIncidents[tie.IncidentID]; hasDirect {
			continue
		}
		via[key] = struct{}{}
		tied[tie.IncidentID] = struct{}{}
	}

	// failed tier per deployment: native wins over heuristic.
	failed := make(map[deploymentKey]string)
	for _, link := range links {
		key := deploymentKey{link.RepoID, link.DeploymentID}
		if _, ok := deployed[key]; !ok {
			continue
		}
		if _, ok := tied[link.IncidentID]; !ok {
			continue
		}
		tier := TierHeuristic
		if link.Source == TierNative {
			tier = TierNative
		}
		if failed[key] != TierNative {
			failed[key] = tier
		}
	}

	out := make(map[uuid.UUID]Counts)
	bump := func(repoID uuid.UUID, apply func(*Counts)) {
		c := out[repoID]
		apply(&c)
		out[repoID] = c
	}
	for key := range deployed {
		bump(key.repoID, func(c *Counts) { c.Deployments++ })
	}
	for key, tier := range failed {
		if tier == TierNative {
			bump(key.repoID, func(c *Counts) { c.FailedNative++ })
		} else {
			bump(key.repoID, func(c *Counts) { c.FailedHeuristic++ })
		}
	}
	for key := range direct {
		bump(key.repoID, func(c *Counts) { c.IncidentsDirect++ })
	}
	for key := range via {
		bump(key.repoID, func(c *Counts) { c.IncidentsViaDeployment++ })
	}
	return out
}

// SortedRepoIDs returns the keys of a CountDay result in string order, for a
// deterministic write order.
func SortedRepoIDs(counts map[uuid.UUID]Counts) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(counts))
	for id := range counts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	return ids
}
