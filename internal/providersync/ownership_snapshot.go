package providersync

import (
	"sort"
	"strings"
	"time"
)

// OwnershipSnapshotRow is one team_project_ownership fact as the snapshot
// rule reads it: the columns that name the fact (team, project, source) and
// the valid_from it is stored under.
type OwnershipSnapshotRow struct {
	TeamID, Source string
	ProjectID      ProjectID
	ValidFrom      time.Time
}

// SnapshotTerm is one condition the end of a snapshot depends on, and the
// reason a close is given up when it does not hold. A term with no reason is
// a term that does not hold.
type SnapshotTerm struct {
	Holds  bool
	Reason string
}

// SnapshotProof says whether the walk behind ONE fact kind reached a stated
// end. It is made only by ProveSnapshot, from at least one named term. The
// zero value is not proven, so a snapshot whose proof nobody stated closes
// nothing.
type SnapshotProof struct {
	stated  bool
	missing []string
}

// snapshotProofNotStated is the reason of a proof nobody made.
const snapshotProofNotStated = "snapshot_end_not_stated"

// ProveSnapshot makes the proof of one fact kind from its terms. The proof
// holds when every term holds; otherwise it carries the reason of every term
// that does not.
func ProveSnapshot(first SnapshotTerm, rest ...SnapshotTerm) SnapshotProof {
	proof := SnapshotProof{stated: true}
	for _, term := range append([]SnapshotTerm{first}, rest...) {
		reason := strings.TrimSpace(term.Reason)
		switch {
		case reason == "":
			proof.missing = append(proof.missing, "snapshot_term_without_reason")
		case !term.Holds:
			proof.missing = append(proof.missing, reason)
		}
	}
	return proof
}

// Proven reports whether every term of the proof holds.
func (proof SnapshotProof) Proven() bool { return proof.stated && len(proof.missing) == 0 }

// Missing is the reason of every term that does not hold, in term order.
func (proof SnapshotProof) Missing() []string {
	if !proof.stated {
		return []string{snapshotProofNotStated}
	}
	return append([]string(nil), proof.missing...)
}

// EmptyAnswer is what a proven snapshot of a fact kind that holds no row of
// the kind means. Each kind states it where the kind is made.
type EmptyAnswer int

const (
	// EmptyClosesNothing: an answer with no row of the kind is more often an
	// access change than a real empty state, so it closes no row of the kind.
	// It is the zero value.
	EmptyClosesNothing EmptyAnswer = iota
	// EmptyIsAnAnswer: the kind is read per team, each read proves its own
	// end, and a team with no row is a real answer that closes the team's rows.
	EmptyIsAnAnswer
)

// SnapshotEmptyAnswer is the reason of a proven kind whose answer holds no
// row of the kind, when the kind's empty answer closes nothing.
const SnapshotEmptyAnswer = "empty_answer"

// SnapshotKind is one fact kind of one writer: the rows it names (holds), and
// what an empty answer of the kind means. A writer whose table holds more than
// one kind of fact gives one kind for each. The fields are unexported: a kind
// is made by NewSnapshotKind, at the places SnapshotKindCensus names.
type SnapshotKind[R any] struct {
	name  string
	empty EmptyAnswer
	holds func(R) bool
}

// NewSnapshotKind makes a fact kind. name labels the kind's log line and
// metric; holds says whether a row (fresh or open) is of the kind.
func NewSnapshotKind[R any](name string, empty EmptyAnswer, holds func(R) bool) SnapshotKind[R] {
	return SnapshotKind[R]{name: name, empty: empty, holds: holds}
}

// Name is the kind's label.
func (kind SnapshotKind[R]) Name() string { return kind.name }

// Snapshot states what one run proved for the kind.
func (kind SnapshotKind[R]) Snapshot(proof SnapshotProof) KindSnapshot[R] {
	return KindSnapshot[R]{kind: kind, proof: proof}
}

// KindSnapshot is one kind and the proof of its walk: the only form in which
// PlanSnapshot accepts what a run found. The rule counts the rows of the kind
// itself, so a caller cannot state a count, pass a list of another kind as
// this kind's answer, or leave out the proof.
type KindSnapshot[R any] struct {
	kind  SnapshotKind[R]
	proof SnapshotProof
}

// SnapshotRetraction names one open row to close: its index in the `open`
// argument and the valid_to to close it with.
type SnapshotRetraction struct {
	Open     int
	ClosedAt time.Time
}

// OwnershipSnapshotRetraction is the retraction of an ownership snapshot.
type OwnershipSnapshotRetraction = SnapshotRetraction

// SnapshotKindOutcome is what the rule decided for one kind.
type SnapshotKindOutcome struct {
	Kind string
	// Fresh and Open count the rows of the kind in the run and in the table.
	Fresh, Open int
	// Closed counts the open rows of the kind the plan closes.
	Closed int
	// Abandoned is why the kind closes nothing: the proof's missing terms,
	// or SnapshotEmptyAnswer. Empty when the kind may close.
	Abandoned []string
}

// ClosedNothingThatWasHeld reports whether the abandoned close kept an open
// row of the kind: an empty answer over an empty table protected nothing.
func (outcome SnapshotKindOutcome) ClosedNothingThatWasHeld() bool {
	if len(outcome.Abandoned) == 0 {
		return false
	}
	if len(outcome.Abandoned) == 1 && outcome.Abandoned[0] == SnapshotEmptyAnswer {
		return outcome.Open > 0
	}
	return true
}

// SnapshotPlan is what one write does to the open rows of its writer.
type SnapshotPlan struct {
	// ValidFrom[i] is the valid_from to write fresh[i] with.
	ValidFrom []time.Time
	// Retract is the open rows to write again with valid_to set.
	Retract []SnapshotRetraction
	// Kinds is the outcome of every kind, in argument order.
	Kinds []SnapshotKindOutcome
	// OpenOfNoKind counts the open rows that no kind holds: never closed.
	OpenOfNoKind int
}

// OwnershipSnapshotPlan is the plan of an ownership snapshot.
type OwnershipSnapshotPlan = SnapshotPlan

// Abandoned is the outcome of every kind whose close was given up and that
// kept an open row, or whose proof did not hold.
func (plan SnapshotPlan) Abandoned() []SnapshotKindOutcome {
	var out []SnapshotKindOutcome
	for _, outcome := range plan.Kinds {
		if outcome.ClosedNothingThatWasHeld() {
			out = append(out, outcome)
		}
	}
	return out
}

func ownershipSnapshotKey(row OwnershipSnapshotRow) string {
	return row.TeamID + "\x00" + row.ProjectID.String() + "\x00" + row.Source
}

// PlanOwnershipSnapshot is the snapshot rule of team_project_ownership (and of
// team_repo_ownership, a repository full name standing in for the project).
// Every writer whose run is a full snapshot of what it owns goes through it:
// the Jira, GitHub, GitLab and Linear catalogs and the Atlassian Teams writer
// (TestJiraOwnershipWriterCensus keeps that a named set). It is PlanSnapshot
// keyed by (team, project, source).
func PlanOwnershipSnapshot(fresh, open []OwnershipSnapshotRow, at time.Time, kinds ...KindSnapshot[OwnershipSnapshotRow]) OwnershipSnapshotPlan {
	return PlanSnapshot(fresh, open, ownershipSnapshotKey,
		func(row OwnershipSnapshotRow) time.Time { return row.ValidFrom }, at, kinds...)
}

// PlanSnapshot is the ONE rule that decides which open rows of a writer a
// run closes. fresh is what this run found; open is what the table holds open
// for this writer, and only for this writer: the caller's read is the scope.
// key names a fact; validFrom is a row's stored valid_from.
//
// A fresh row whose fact is already open takes the EARLIEST open valid_from.
// valid_from is a key column, so the write replaces that row; a new stamp at
// each run would add one more open row for the same fact. This applies to
// every fresh row, whatever its kind and whatever the proofs say: it adds no
// row and closes none.
//
// An open row is closed only through its kind. The kind is the one kind that
// holds the row; an open row that no kind holds, or that more than one kind
// holds, is never closed. A kind closes its open rows that the run does not
// hold (a fact the snapshot no longer has, an id form no writer produces any
// more, a later duplicate of a fact it does have) only when:
//
//   - its proof holds: every read of the kind's walk reached a stated end; and
//   - the run holds at least one row of THE KIND, unless the kind declares an
//     empty answer to be an answer (EmptyIsAnAnswer). A row of another kind
//     never makes a kind not empty: the rule counts per kind, never the list.
//
// Otherwise the kind closes nothing, and its outcome says why. A retraction is
// the row's own key written again with valid_to set, never a delete; valid_to
// is never before the row's valid_from.
func PlanSnapshot[R any](
	fresh, open []R, key func(R) string, validFrom func(R) time.Time, at time.Time, kinds ...KindSnapshot[R],
) SnapshotPlan {
	plan := SnapshotPlan{ValidFrom: make([]time.Time, len(fresh)), Kinds: make([]SnapshotKindOutcome, len(kinds))}
	firstSeen := map[string]time.Time{}
	for _, row := range open {
		fact := key(row)
		if seen, ok := firstSeen[fact]; !ok || validFrom(row).Before(seen) {
			firstSeen[fact] = validFrom(row)
		}
	}
	current := map[string]bool{}
	for index, row := range fresh {
		fact := key(row)
		current[fact] = true
		plan.ValidFrom[index] = validFrom(row)
		if seen, ok := firstSeen[fact]; ok && seen.Before(validFrom(row)) {
			plan.ValidFrom[index] = seen
		}
		for k, snapshot := range kinds {
			if snapshot.kind.holds != nil && snapshot.kind.holds(row) {
				plan.Kinds[k].Fresh++
			}
		}
	}
	for k, snapshot := range kinds {
		outcome := &plan.Kinds[k]
		outcome.Kind = snapshot.kind.name
		if snapshot.kind.holds == nil || strings.TrimSpace(snapshot.kind.name) == "" {
			outcome.Abandoned = append(outcome.Abandoned, "snapshot_kind_not_made")
			continue
		}
		if !snapshot.proof.Proven() {
			outcome.Abandoned = append(outcome.Abandoned, snapshot.proof.Missing()...)
		}
		if outcome.Fresh == 0 && snapshot.kind.empty != EmptyIsAnAnswer {
			outcome.Abandoned = append(outcome.Abandoned, SnapshotEmptyAnswer)
		}
	}
	for index, row := range open {
		owner := -1
		for k, snapshot := range kinds {
			if snapshot.kind.holds != nil && snapshot.kind.holds(row) {
				if owner >= 0 {
					owner = -2
					break
				}
				owner = k
			}
		}
		if owner < 0 {
			plan.OpenOfNoKind++
			continue
		}
		outcome := &plan.Kinds[owner]
		outcome.Open++
		fact := key(row)
		if current[fact] && validFrom(row).Equal(firstSeen[fact]) {
			continue
		}
		if len(outcome.Abandoned) > 0 {
			continue
		}
		closedAt := at
		if closedAt.Before(validFrom(row)) {
			closedAt = validFrom(row)
		}
		plan.Retract = append(plan.Retract, SnapshotRetraction{Open: index, ClosedAt: closedAt})
		outcome.Closed++
	}
	return plan
}

// SnapshotReasons is every distinct reason of the abandoned kinds, sorted.
func (plan SnapshotPlan) SnapshotReasons() []string {
	seen := map[string]bool{}
	var out []string
	for _, outcome := range plan.Abandoned() {
		for _, reason := range outcome.Abandoned {
			if !seen[reason] {
				seen[reason] = true
				out = append(out, reason)
			}
		}
	}
	sort.Strings(out)
	return out
}
