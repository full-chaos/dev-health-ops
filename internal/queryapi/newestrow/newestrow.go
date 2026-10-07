// Package newestrow builds the "each team's newest day" predicate shared by the
// capacity and throughput forecast backlog reads (CHAOS-8498).
package newestrow

import "fmt"

// PerTeamPredicate keeps only the newest row day of each (provider, work scope,
// team) key inside the scope, the key every reader groups on.
// A single day over the whole team set drops a team whose newest day is older
// (a daily run writes one batch per repository partition, and a team with no
// event and no open work gets no row for a day). The same scope predicate is
// spliced into the subquery so each team's newest day is found WITHIN the scope.
// from is the table, with FINAL when the caller's reader uses it.
// ifNull keeps a NULL team_id in the match: tuple IN never matches NULL.
func PerTeamPredicate(from, where string) string {
	return fmt.Sprintf(`(ifNull(provider, ''), ifNull(work_scope_id, ''), ifNull(team_id, ''), day) IN (
                SELECT ifNull(provider, ''), ifNull(work_scope_id, ''), ifNull(team_id, ''), max(day) FROM %s WHERE %s GROUP BY ifNull(provider, ''), ifNull(work_scope_id, ''), ifNull(team_id, '')
            )`, from, where)
}
