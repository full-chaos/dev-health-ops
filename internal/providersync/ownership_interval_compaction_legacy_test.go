package providersync

import (
	"sort"
	"strings"
	"time"
)

// The 9556 planner and differential (f850e8f9), kept verbatim as the oracle of
// TestCompactionPlannerEqualsThe9556Planner. Only the names carry a "legacy"
// prefix. They are quadratic in the rows of a group: never call them at prod
// scale outside the timing evidence.

// legacyPlanOwnershipCompaction finds the runs and the droppable tombstones of the
// compacted source. It reads rows and writes nothing.
func legacyPlanOwnershipCompaction(spec ownershipCompactionSpec, rows []ownershipIntervalRow) ownershipCompactionPlan {
	plan := ownershipCompactionPlan{byGroup: map[string][]ownershipIntervalRow{}, compacted: spec}
	byKey := map[string][]ownershipIntervalRow{}
	for _, row := range rows {
		plan.byGroup[row.groupOf()] = append(plan.byGroup[row.groupOf()], row)
		if row.Source != spec.source {
			continue
		}
		byKey[row.keyOf()] = append(byKey[row.keyOf()], row)
		plan.org.RowsBefore++
		switch {
		case row.isOpen():
			plan.org.OpenBefore++
		case row.isTombstone():
			plan.org.TombstonesBefore++
		case row.isCloseBeforeOpen():
			plan.org.CloseBeforeOpenBefore++
		default:
			plan.org.ClosedBefore++
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		keyRows := byKey[key]
		sort.Slice(keyRows, func(i, j int) bool { return keyRows[i].ValidFrom.Before(keyRows[j].ValidFrom) })
		plan.legacyPlanKey(keyRows)
	}
	return plan
}

func (plan *ownershipCompactionPlan) legacyPlanKey(keyRows []ownershipIntervalRow) {
	var live []ownershipIntervalRow
	var tombstones []ownershipIntervalRow
	var minLive, maxEnd *time.Time
	var maxLiveStamp time.Time
	hasOpen := false
	for _, row := range keyRows {
		if row.isTombstone() {
			tombstones = append(tombstones, row)
			continue
		}
		if minLive == nil {
			from := row.ValidFrom
			minLive = &from
		}
		if row.UpdatedAt.After(maxLiveStamp) {
			maxLiveStamp = row.UpdatedAt
		}
		if row.isOpen() {
			hasOpen = true
		} else if maxEnd == nil || row.ValidTo.After(*maxEnd) {
			end := *row.ValidTo
			maxEnd = &end
		}
		if row.isCloseBeforeOpen() {
			continue
		}
		live = append(live, row)
	}
	// A tombstone covers no instant, and is dropped unless a reader still
	// keys on it:
	//   - it is the earliest row of its key: team_id_carry reads
	//     min(valid_from) of a key as the "since when" of the fact, and the
	//     acr edge reads it as first_valid_from;
	//   - it is the LATEST row of its key by the acr ownershipFactOrder (no
	//     open row, and no other row ends strictly after it): the edge takes
	//     latest_valid_to and the latest row's columns from it, so dropping it
	//     would change both;
	//   - it holds the key's newest updated_at (more than 1 ms above every other
	//     row; the 1 ms is the stamp a half-finished run of this tool leaves on
	//     its own tombstones): the acr edge pages on max(updated_at) of the key
	//     and the work-item scope check reads it, so a key whose maximum FELL
	//     could slip behind a cursor that has not read it yet. A merged row
	//     takes the newest updated_at of its run, so merging never lowers it.
	//     DO NOT WEAKEN THIS CLAUSE: a falling max(updated_at) is SILENT for
	//     the acr edges (the group is simply not read again, nothing fails), so
	//     this clause and TestCompactOwnershipIntervalsNeverLowersAKeysNewestStamp
	//     are the only protection a consumer has.
	for _, tombstone := range tombstones {
		plan.org.TombstonesFound++
		switch {
		case minLive == nil || !minLive.Before(tombstone.ValidFrom):
			plan.org.TombstonesKeptAnchor++
		case !hasOpen && (maxEnd == nil || !maxEnd.After(tombstone.ValidFrom)):
			plan.org.TombstonesKeptLatest++
		case tombstone.UpdatedAt.After(maxLiveStamp.Add(time.Millisecond)):
			plan.org.TombstonesKeptStamp++
		default:
			plan.drops = append(plan.drops, tombstone)
			plan.org.TombstonesDropped++
		}
	}
	var run []ownershipIntervalRow
	flush := func() {
		if len(run) >= 2 {
			plan.legacyConsider(run)
		}
		run = nil
	}
	for index, row := range live {
		if index > 0 {
			previous := live[index-1]
			switch {
			case previous.ValidTo == nil || row.ValidFrom.Before(*previous.ValidTo):
				plan.org.Overlaps++
			case row.ValidFrom.After(*previous.ValidTo):
				plan.org.Gaps++
			}
		}
		if row.isOpen() {
			flush()
			continue
		}
		if len(run) > 0 {
			last := run[len(run)-1]
			if last.ValidTo.Equal(row.ValidFrom) && last.shape() == row.shape() {
				run = append(run, row)
				continue
			}
			flush()
		}
		run = append(run, row)
	}
	flush()
}

// consider accepts a run when the winner of its (provider, natural, team)
// group is the same at every instant of the run before and after the merge.
// The winner is what the cascade reads: among the rows valid at T of every
// source, the newest by (updated_at, valid_from). Two sources on one key can
// disagree on it; the run then stays as it is.
func (plan *ownershipCompactionPlan) legacyConsider(run []ownershipIntervalRow) {
	plan.org.RunsFound++
	merged := mergedRow(run)
	first := run[0]
	groupRows := plan.byGroup[first.groupOf()]
	var pre, post []ownershipIntervalRow
	inRun := map[string]bool{}
	for _, row := range run {
		inRun[row.sortKey()] = true
	}
	for _, row := range groupRows {
		if row.TeamID != first.TeamID {
			continue
		}
		pre = append(pre, row)
		if inRun[row.sortKey()] {
			continue
		}
		post = append(post, row)
	}
	post = append(post, merged)
	from, to := first.ValidFrom, *run[len(run)-1].ValidTo
	if _, mismatches := legacyCompareOwnerAnswers(pre, post, &from, &to); mismatches > 0 {
		plan.org.RunsKeptMultiSource++
		return
	}
	plan.runs = append(plan.runs, compactionRun{rows: run, merged: merged})
	plan.org.RunsMerged++
	plan.org.RowsMerged += len(run) - 1
}

// final is the state the rows end in: merged rows in, inner rows and dropped
// legacyAnswerAt is what a reader that asks "who owns this at T" sees for one group:
// the multiset of (team, source, shape) valid at T, and per team the winner.
func legacyAnswerAt(rows []ownershipIntervalRow, at time.Time) string {
	var valid []string
	winners := map[string]ownershipIntervalRow{}
	for _, row := range rows {
		if !row.validAt(at) {
			continue
		}
		valid = append(valid, row.TeamID+"\x01"+row.Source+"\x01"+row.shape())
		best, ok := winners[row.TeamID]
		if !ok || row.UpdatedAt.After(best.UpdatedAt) || (row.UpdatedAt.Equal(best.UpdatedAt) && row.ValidFrom.After(best.ValidFrom)) {
			winners[row.TeamID] = row
		}
	}
	sort.Strings(valid)
	teams := make([]string, 0, len(winners))
	for team := range winners {
		teams = append(teams, team)
	}
	sort.Strings(teams)
	var out strings.Builder
	out.WriteString(strings.Join(valid, "\x02"))
	out.WriteString("\x03")
	for _, team := range teams {
		out.WriteString(team + "\x01" + winners[team].shape() + "\x02")
	}
	return out.String()
}

// legacyInstantsOf lists every instant worth asking about for two row sets: each
// boundary, one millisecond before it, the midpoint between neighbours, and the
// instants just outside. Limited to [from, to] when given.
func legacyInstantsOf(from, to *time.Time, sets ...[]ownershipIntervalRow) []time.Time {
	seen := map[int64]bool{}
	add := func(t time.Time) { seen[t.UnixMilli()] = true }
	for _, rows := range sets {
		for _, row := range rows {
			add(row.ValidFrom)
			add(row.ValidFrom.Add(-time.Millisecond))
			if row.ValidTo != nil {
				add(*row.ValidTo)
				add(row.ValidTo.Add(-time.Millisecond))
				add(row.ValidTo.Add(time.Millisecond))
			}
		}
	}
	millis := make([]int64, 0, len(seen))
	for m := range seen {
		millis = append(millis, m)
	}
	sort.Slice(millis, func(i, j int) bool { return millis[i] < millis[j] })
	var out []time.Time
	for index, m := range millis {
		out = append(out, time.UnixMilli(m).UTC())
		if index+1 < len(millis) {
			out = append(out, time.UnixMilli(m+(millis[index+1]-m)/2).UTC())
		}
	}
	if len(millis) > 0 {
		out = append(out, time.UnixMilli(millis[len(millis)-1]).UTC().Add(24*time.Hour*365*100))
	}
	if from == nil && to == nil {
		return out
	}
	limited := out[:0:0]
	for _, at := range out {
		if from != nil && at.Before(from.Add(-time.Millisecond)) {
			continue
		}
		if to != nil && at.After(to.Add(time.Millisecond)) {
			continue
		}
		limited = append(limited, at)
	}
	return limited
}

func legacyCompareOwnerAnswers(pre, post []ownershipIntervalRow, from, to *time.Time) (instants, mismatches int) {
	for _, at := range legacyInstantsOf(from, to, pre, post) {
		instants++
		if legacyAnswerAt(pre, at) != legacyAnswerAt(post, at) {
			mismatches++
		}
	}
	return instants, mismatches
}

// legacyOwnershipDifferential asks the owner-at-T question of every group that the
// change touches, for both row sets, at every boundary and midpoint.
func legacyOwnershipDifferential(pre, post []ownershipIntervalRow) OwnershipCompactionDifferential {
	preGroups, postGroups := map[string][]ownershipIntervalRow{}, map[string][]ownershipIntervalRow{}
	for _, row := range pre {
		preGroups[row.groupOf()] = append(preGroups[row.groupOf()], row)
	}
	for _, row := range post {
		postGroups[row.groupOf()] = append(postGroups[row.groupOf()], row)
	}
	groups := map[string]bool{}
	for group := range preGroups {
		groups[group] = true
	}
	for group := range postGroups {
		groups[group] = true
	}
	result := OwnershipCompactionDifferential{GroupsTotal: len(groups)}
	names := make([]string, 0, len(groups))
	for group := range groups {
		names = append(names, group)
	}
	sort.Strings(names)
	for _, group := range names {
		if sameRowSet(preGroups[group], postGroups[group]) {
			continue
		}
		result.GroupsChecked++
		instants, mismatches := legacyCompareOwnerAnswers(preGroups[group], postGroups[group], nil, nil)
		result.InstantsChecked += instants
		result.Mismatches += mismatches
	}
	return result
}
