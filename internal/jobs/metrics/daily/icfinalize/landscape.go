package icfinalize

import "sort"

// LandscapeRecord is one row of ic_landscape_rolling_30d. There is one per
// (identity, map), because map_name is part of that table's sorting key
// (org_id, repo_id, team_id, map_name, as_of_day, identity_id) — so the three
// maps are three rows, not three columns.
type LandscapeRecord struct {
	IdentityID string
	TeamID     string
	MapName    string
	XRaw       float64
	YRaw       float64
	XNorm      float64
	YNorm      float64
	Churn      float64
	Delivery   float64
	CycleP50   float64
	WIPMax     float64
}

// mapNames is the fixed set compute_ic_landscape_rolling emits, in the order
// its `vectors` dict declares them.
var mapNames = [3]string{"churn_throughput", "cycle_throughput", "wip_throughput"}

// ComputeLandscape ports compute_ic_landscape_rolling (compute_ic.py:189).
//
// Normalization is PER TEAM: each identity's x and y are ranked against the
// vector of that team's members only. A one-member team therefore ranks 0.5 on
// every axis by construction — the same value an empty vector yields, so the
// two are indistinguishable in the output.
//
// team_map resolves a missing team_id: an identity with a blank team_id and a
// non-"unknown" identity falls back to team_map, then to "unassigned"; a blank
// or "unknown" identity keeps a blank team. That branch is the reference's
// (compute_ic.py:215-221) and is replicated rather than simplified.
//
// DETERMINISM NOTE. The Python groups with `by_team.setdefault(...)`, so its
// team order follows the INPUT order, and its per-team member order likewise.
// Go map iteration is randomised, so this sorts team ids and preserves input
// order within a team. That is a deliberate divergence in ORDER only: the
// reference has no stable order to reproduce (its input arrives from a
// ClickHouse GROUP BY, which is itself unordered), so a canonical order is the
// honest choice and the values are order-invariant. Recorded because "the port
// sorts and the reference does not" is exactly the kind of difference that
// looks like a bug to the next reader.
func ComputeLandscape(stats []RollingStat, teamMap map[string]string) []LandscapeRecord {
	type enriched struct {
		stat   RollingStat
		teamID string
		axes   [3][2]float64
	}

	byTeam := map[string][]enriched{}
	for _, stat := range stats {
		teamID := stat.TeamID
		if teamID == "" {
			if stat.IdentityID != "" && stat.IdentityID != "unknown" {
				if mapped, ok := teamMap[stat.IdentityID]; ok {
					teamID = mapped
				} else {
					teamID = "unassigned"
				}
			}
		}
		churnXY, cycleXY, wipXY := LandscapeAxes(
			stat.ChurnLOC30d, stat.DeliveryUnits30, stat.CycleP5030dHrs, stat.WIPMax30d,
		)
		byTeam[teamID] = append(byTeam[teamID], enriched{
			stat: stat, teamID: teamID, axes: [3][2]float64{churnXY, cycleXY, wipXY},
		})
	}

	teamIDs := make([]string, 0, len(byTeam))
	for teamID := range byTeam {
		teamIDs = append(teamIDs, teamID)
	}
	sort.Strings(teamIDs)

	var records []LandscapeRecord
	for _, teamID := range teamIDs {
		members := byTeam[teamID]
		// One x/y vector per map, over this team's members only.
		var xVectors, yVectors [3][]float64
		for _, member := range members {
			for i := range mapNames {
				xVectors[i] = append(xVectors[i], member.axes[i][0])
				yVectors[i] = append(yVectors[i], member.axes[i][1])
			}
		}
		for _, member := range members {
			for i, mapName := range mapNames {
				x, y := member.axes[i][0], member.axes[i][1]
				records = append(records, LandscapeRecord{
					IdentityID: member.stat.IdentityID,
					TeamID:     teamID,
					MapName:    mapName,
					XRaw:       x,
					YRaw:       y,
					XNorm:      PercentileRank(xVectors[i], x),
					YNorm:      PercentileRank(yVectors[i], y),
					Churn:      member.stat.ChurnLOC30d,
					Delivery:   member.stat.DeliveryUnits30,
					CycleP50:   member.stat.CycleP5030dHrs,
					WIPMax:     member.stat.WIPMax30d,
				})
			}
		}
	}
	return records
}
