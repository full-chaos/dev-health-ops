package teamsidentity

import (
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
)

// ErrTeamChangedConcurrently is an admin team write whose read-back did not
// find it stored after rosterRounds writes (another writer of the team kept
// winning), or whose team was deleted meanwhile (the delete wins). The routes
// answer it with 409 and a retryable reason code.
var ErrTeamChangedConcurrently = errors.New("team changed concurrently, retry")

// rosterRounds bounds the writes of one admin team write.
const rosterRounds = 3

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

// teamChangedConcurrentlyReason is the retryable reason code of an admin team
// write refused (ErrTeamChangedConcurrently).
const teamChangedConcurrentlyReason = "team_changed_concurrently"

// routeFailureLevel logs a refused admin team write at Warn: it is a conflict the
// caller retries, not a fault.
func routeFailureLevel(err error) slog.Level {
	if errors.Is(err, ErrTeamChangedConcurrently) {
		return slog.LevelWarn
	}
	return slog.LevelError
}

// writeInternalUnlessRefused answers a refused admin team write with 409
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
