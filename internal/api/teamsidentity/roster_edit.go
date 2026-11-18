package teamsidentity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sort"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
)

// ErrTeamChangedConcurrently is a roster edit whose read-back did not find it
// stored after rosterRounds writes: another writer of the team kept winning.
// The routes answer it with 409 and a retryable reason code.
var ErrTeamChangedConcurrently = errors.New("team changed concurrently, retry")

// rosterRounds bounds the writes of one roster edit.
const rosterRounds = 3

// rosterReadBackQuery reads the stored roster of one team after a roster
// edit: teams FINAL, the version a reader sees.
const rosterReadBackQuery = "SELECT manual_members FROM teams FINAL WHERE org_id = {org_id:String} AND id = {team_id:String}"

// rosterEdit is one admin roster edit: a full set, members added or members
// removed. apply makes the roster to write from the stored one; held says
// whether a stored roster still holds the edit.
type rosterEdit struct {
	set    *[]string
	add    []string
	remove map[string]bool
}

func (e rosterEdit) apply(current []string) []string {
	switch {
	case e.set != nil:
		return sortedUnique(*e.set)
	case e.remove != nil:
		return sortedUnique(filterOut(current, e.remove))
	}
	return sortedUnique(current, e.add)
}

func (e rosterEdit) held(stored []string) bool {
	if e.set != nil {
		return slices.Equal(sortedUnique(stored), sortedUnique(*e.set))
	}
	has := map[string]bool{}
	for _, member := range stored {
		has[member] = true
	}
	if e.remove != nil {
		for member := range e.remove {
			if has[member] {
				return false
			}
		}
		return true
	}
	for _, member := range e.add {
		if member != "" && !has[member] {
			return false
		}
	}
	return true
}

// rosterHeld reads the stored roster back after a roster edit and says
// whether it holds the edit.
func (s Store) rosterHeld(ctx context.Context, orgID, teamID string, edit rosterEdit) ([]string, bool, error) {
	var stored []string
	err := s.Conn.QueryRow(ctx, rosterReadBackQuery, clickhouse.Named("org_id", orgID), clickhouse.Named("team_id", teamID)).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read back the team roster: %w", err)
	}
	return stored, edit.held(stored), nil
}

// rosterWrite says how a team write writes manual_members.
type rosterWrite struct {
	rule        teamprovider.Roster
	add, remove []string
}

// row is the roster write of an edit: a full set is stated; added or removed
// members are applied inside the insert statement to the stored roster, so a
// roster edit committed after this write's own read is not lost.
func (e rosterEdit) row() rosterWrite {
	switch {
	case e.set != nil:
		return rosterWrite{rule: teamprovider.RosterStated}
	case e.remove != nil:
		remove := make([]string, 0, len(e.remove))
		for member := range e.remove {
			remove = append(remove, member)
		}
		sort.Strings(remove)
		return rosterWrite{rule: teamprovider.RosterDelta, remove: remove}
	}
	return rosterWrite{rule: teamprovider.RosterDelta, add: sortedUnique(e.add)}
}

// teamChangedConcurrentlyReason is the retryable reason code of a roster edit
// refused after rosterRounds writes.
const teamChangedConcurrentlyReason = "team_changed_concurrently"

// routeFailureLevel logs a refused roster edit at Warn: it is a conflict the
// caller retries, not a fault.
func routeFailureLevel(err error) slog.Level {
	if errors.Is(err, ErrTeamChangedConcurrently) {
		return slog.LevelWarn
	}
	return slog.LevelError
}

// writeInternalUnlessRefused answers a refused roster edit with 409
// {"detail":{"message":"team changed concurrently, retry","reason_code":"team_changed_concurrently"}}
// and every other route failure with the generic 500.
func writeInternalUnlessRefused(w http.ResponseWriter, err error) {
	if !errors.Is(err, ErrTeamChangedConcurrently) {
		policy.WriteInternal(w)
		return
	}
	detail := policy.ErrorDetail(ErrTeamChangedConcurrently.Error())
	detail.Set("reason_code", teamChangedConcurrentlyReason)
	policy.WriteDetail(w, http.StatusConflict, detail, nil)
}
