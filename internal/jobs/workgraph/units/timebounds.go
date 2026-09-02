package units

import "time"

// TimeBounds is the Go equivalent of evidence.TimeBounds.
type TimeBounds struct {
	Start time.Time
	End   time.Time
}

// NodeTimes carries the timestamps compute_time_bounds reads for one node.
//
// # WHY TYPED FIELDS AND NOT map[string]any
//
// Python's _node_time_bounds pulls these out of a dict and pushes each through
// _ensure_utc, which accepts `datetime | str | None`. It is tempting to mirror
// that shape in Go with `map[string]any` and a parser.
//
// That would be wrong, and measurably so. Every one of these values originates
// in a ClickHouse DateTime64 column, so the Python driver hands _ensure_utc a
// `datetime` and its string branch is never taken on this path. Go's driver
// hands chquery a time.Time. Stringifying a time.Time in order to re-parse it
// would introduce a formatting round trip that exists in neither plane, and
// would need an ISO grammar (basic format, lowercase t, arbitrary separators,
// no-colon offsets, 24:00 rollover, microsecond truncation) that nothing here
// can actually produce.
//
// So the port models only what remains once the type is known: the UTC
// normalisation, which lives in chquery.normalizeTimestamp, and the
// absent-value fallbacks below.
//
// A nil pointer is Python's None. The pointer fields are exactly the columns
// declared Nullable -- completed_at, merged_at, closed_at -- and the value
// fields are exactly the NOT NULL ones, so the shape is not a convention but a
// restatement of the schema.
//
// # THE TYPES ARE THE GUARD, NOT JUST THE MODEL
//
// The reasoning above says a string branch "is never taken on this path". That
// is prose, and prose stops being true silently. What actually prevents an ISO
// parse from being reintroduced is that these fields are time.Time: changing any
// of them to a string breaks every .Before, .After and .UTC() call site at
// COMPILE time, so the reintroduction cannot pass review by accident.
//
// Recorded because the pin was arrived at for an unrelated reason -- typed
// fields were chosen to avoid modelling an ISO grammar neither plane can
// produce -- and a future reader refactoring for a third reason would find
// nothing telling them the type is load-bearing. Safe-by-accident degrades to
// unsafe the moment someone has a reason to change it.
//
// The shape is lane-4752-go's on CHAOS-4819: a containment argument closes a
// question, and then nothing can observe the question reopening.
type NodeTimes struct {
	// Issue: created_at (NOT NULL), completed_at, updated_at (NOT NULL).
	// PR: created_at (NOT NULL), merged_at, closed_at.
	// Commit: author_when (NOT NULL), committer_when (NOT NULL).
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt *time.Time
	MergedAt    *time.Time
	ClosedAt    *time.Time
	AuthorWhen  time.Time
	// Present reports whether the node was found in its map at all. Python's
	// `map.get(node_id, {})` yields an empty dict for a missing node, and every
	// field lookup on it then returns None -- so a dangling node contributes
	// nothing rather than contributing a zero time.
	Present bool
}

// nodeTimeBounds ports evidence._node_time_bounds.
//
// The per-type fallback chains are NOT interchangeable and the ordering is
// load-bearing:
//
//	issue   end = completed_at, else updated_at, else start
//	pr      end = merged_at,    else closed_at,  else start
//	commit  start = end = author_when
//
// An unknown node type yields nothing at all -- not a zero time -- which is why
// this returns explicit ok flags rather than zero values a caller might mistake
// for real timestamps.
func nodeTimeBounds(nodeType string, times NodeTimes) (start, end time.Time, hasStart, hasEnd bool) {
	if !times.Present {
		// Python: `map.get(node_id, {})` then `.get(field)` -> None for every
		// field. A node in the component but missing from its map contributes
		// neither a start nor an end.
		return time.Time{}, time.Time{}, false, false
	}

	switch nodeType {
	case "issue":
		start, hasStart = times.CreatedAt, true
		if times.CompletedAt != nil {
			end, hasEnd = *times.CompletedAt, true
		} else {
			end, hasEnd = times.UpdatedAt, true
		}
	case "pr":
		start, hasStart = times.CreatedAt, true
		switch {
		case times.MergedAt != nil:
			end, hasEnd = *times.MergedAt, true
		case times.ClosedAt != nil:
			end, hasEnd = *times.ClosedAt, true
		}
	case "commit":
		// Python: `when = author_when or committer_when`, then returns
		// (when, when). author_when is NOT NULL in git_commits, so the
		// committer_when fallback is unreachable through this path -- a
		// datetime is never falsy, and the column cannot be None.
		start, end = times.AuthorWhen, times.AuthorWhen
		hasStart, hasEnd = true, true
	default:
		// Python returns (None, None) for any other node type. Neither a start
		// nor an end, rather than a zero time.
		return time.Time{}, time.Time{}, false, false
	}

	// Python: `return start, end or start`. When no end was found, the start
	// stands in as the end -- so a node always contributes either both bounds
	// or neither, never a start alone.
	if !hasEnd && hasStart {
		end, hasEnd = start, true
	}

	// _node_time_bounds pushes EVERY field through _ensure_utc, so the values
	// it returns are already on UTC. Doing the same here keeps the comparison
	// and the returned bounds in one zone.
	//
	// The comparison itself would be correct without this -- Go's Before/After
	// compare instants regardless of location -- but the RETURNED bounds would
	// carry whatever zone they arrived in, and those values are written to
	// stored columns. .UTC() rather than a wall-clock rebuild, for the reason
	// in chquery.normalizeTimestamp: converting preserves the instant.
	if hasStart {
		start = start.UTC()
	}
	if hasEnd {
		end = end.UTC()
	}
	return start, end, hasStart, hasEnd
}

// ComputeTimeBounds ports evidence.compute_time_bounds.
//
// Returns ok=false for Python's None, which happens when NO node yielded a
// start or NO node yielded an end. Note the `and`, not `or`: Python guards with
// `if not starts or not ends`, so a single node with both bounds is enough and
// a hundred nodes with starts but no ends yields nothing.
//
// Given the fallback in nodeTimeBounds -- an absent end becomes the start --
// the two lists are always the same length in practice, so the guard is
// belt-and-braces on the Python side too. Confirmed by mutation: replacing the
// || with && passes the entire suite, because haveStart and haveEnd are never
// observed to differ. It is reproduced rather than simplified, since the day a
// node type is added that yields a start without an end, the two spellings stop
// agreeing -- and that day the || is the one matching Python.
func ComputeTimeBounds(nodes []NodeKey, times map[NodeKey]NodeTimes) (TimeBounds, bool) {
	var (
		earliest, latest   time.Time
		haveStart, haveEnd bool
	)

	for _, node := range nodes {
		start, end, hasStart, hasEnd := nodeTimeBounds(node.Type, times[node])
		if hasStart {
			// min() over the starts, by INSTANT -- Before/After are
			// zone-independent, matching Python's comparison of aware
			// datetimes.
			//
			// Note this comparison is NOT what protects against the
			// wall-clock defect: nodeTimeBounds has already normalised to
			// UTC, so by here the two are equivalent (confirmed by mutation --
			// swapping in a wall-clock comparison passes the whole suite).
			// What protects it is that normalisation, and removing it fails 15
			// subtests. Stated because a reader could otherwise take this line
			// as the guard and safely remove the one that actually is.
			if !haveStart || start.Before(earliest) {
				earliest, haveStart = start, true
			}
		}
		if hasEnd {
			if !haveEnd || end.After(latest) {
				latest, haveEnd = end, true
			}
		}
	}

	if !haveStart || !haveEnd {
		return TimeBounds{}, false
	}
	return TimeBounds{Start: earliest, End: latest}, true
}
