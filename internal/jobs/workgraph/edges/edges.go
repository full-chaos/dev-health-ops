// Package edges is the Go port of the work-graph EDGE producer (CHAOS-4766):
// the half of work_graph/builder.py that turns provider rows into
// work_graph_edges, work_graph_issue_pr and work_graph_pr_commit.
//
// # RELATIONSHIP TO units
//
// internal/jobs/workgraph/units (CHAOS-4441) is the CONSUMER of what this
// package writes: it groups work_graph_edges into work units and hashes their
// node sets. This package is the PRODUCER. They meet at one column, and that
// column is the reason this package exists at all -- see the confidence
// contract in confidence.go.
//
// # FIDELITY CONTRACT: THREE ENUMERATED DIVERGENCES
//
// This is a bit-exact port of the current Python behaviour except where the
// Divergences list below says otherwise. That list is the single place they are
// enumerated, and TestEveryDivergenceIsImplemented asserts each entry is real,
// so a divergence that is REMOVED cannot sit in the list stale.
//
// This replaced a sentence claiming "exactly ONE enumerated divergence" while a
// second file separately claimed to hold "THE ONE deliberate divergence". Both
// were true when written; three of the divergences arrived afterwards and
// neither sentence was revisited.
//
// # WHAT THIS CONTRACT DOES AND DOES NOT ENFORCE
//
// Stated plainly because the previous version read as more enforced than it
// was: the golden test fails on any delta from the frozen Python output that
// the exception list does not name -- but the golden can only exercise
// divergence 1. It contains no malformed PR id and is a scoped run, so 2 and 3
// are outside its reach and rest on their own targeted tests.
//
// The test below proves every LISTED divergence is real. It cannot prove no
// UNLISTED one exists; claiming otherwise would repeat the failure it replaces.
//
// Proven by tests/fixtures/workgraph_issue_edges_python_golden.json -- 6,531
// dependency rows and the 3,548 edges, 1 projection watermark and 5 mutations
// the deployed Python producer derived from them, on real synced data.
package edges

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

// Node and edge type tokens. These are the `.value` of Python's NodeType and
// EdgeType (str, Enum) members and appear verbatim in the edge_id hash input,
// so they are a wire contract, not an internal naming choice.
const (
	NodeTypeIssue = "issue"
	NodeTypePR    = "pr"

	EdgeTypeBlocks        = "blocks"
	EdgeTypeRelates       = "relates"
	EdgeTypeDuplicates    = "duplicates"
	EdgeTypeIsBlockedBy   = "is_blocked_by"
	EdgeTypeIsRelatedTo   = "is_related_to"
	EdgeTypeIsDuplicateOf = "is_duplicate_of"
	EdgeTypeParentOf      = "parent_of"
	EdgeTypeChildOf       = "child_of"
	EdgeTypeImplements    = "implements"
	EdgeTypeReferences    = "references"
	EdgeTypeContains      = "contains"

	ProvenanceNative       = "native"
	ProvenanceExplicitText = "explicit_text"
	ProvenanceHeuristic    = "heuristic"
)

// Row is one work_graph_edges row.
//
// The column set and its types are read from the LIVE schema, not from
// migrations/clickhouse/014_work_graph.sql -- that file predates two changes
// that matter: org_id was added to the ORDER BY (so it IS part of the
// ReplacingMergeTree dedup identity), and `day` was added alongside event_ts.
//
//	ReplacingMergeTree(last_synced)
//	ORDER BY (org_id, source_type, source_id, edge_type, target_type, target_id)
type Row struct {
	EdgeID     string
	SourceType string
	SourceID   string
	TargetType string
	TargetID   string
	EdgeType   string
	Provenance string
	// Confidence is written into a Float32 column. It is float32 here, not
	// float64, so the quantisation cannot be forgotten at a call site -- see
	// confidence.go.
	Confidence float32
	Evidence   string
	// DiscoveredAt and LastSynced are the BUILD's clock, not the row's:
	// _build_issue_issue_edges stamps both from the builder's construction time
	// for every edge in the run. EventTs is per-row. Reversing this changes
	// every ReplacingMergeTree version and silently re-orders dedup winners.
	DiscoveredAt time.Time
	LastSynced   time.Time
	EventTs      time.Time
	// Day must equal EventTs truncated to a UTC date. The column has a DEFAULT,
	// but an explicit INSERT supplies the value, so the invariant is ours to
	// hold; it is 11275-for-11275 true on the proof org today.
	Day   time.Time
	OrgID string
}

// EdgeID is the deterministic edge identity, byte-identical to Python's
// work_graph/ids.py::generate_edge_id.
//
// The canonical string is source, edge type and target with a colon inside each
// endpoint and a pipe between the three parts:
//
//	"{source_type}:{source_id}|{edge_type}|{target_type}:{target_id}"
//
// hashed with SHA-256 and rendered lowercase hex. Field order and both
// separators are load-bearing -- this id is the ClickHouse dedup identity's
// twin and the key the cleanup step deletes by, so a divergence orphans rows
// rather than failing loudly.
//
// The concatenation does not escape its delimiters, so the mapping is not
// injective in general: ("a", "b|c", "d", ...) and ("a", "b", "c|d", ...) hash
// alike. That is a property of the scheme being ported, deliberately preserved,
// and bounded by the two type positions being a closed enum with no ':' or '|'
// in any member -- see TestEdgeIDIsAmbiguousAcrossDelimiters, which pins both
// the ambiguity and the bound.
func EdgeID(sourceType, sourceID, edgeType, targetType, targetID string) string {
	canonical := sourceType + ":" + sourceID + "|" + edgeType + "|" + targetType + ":" + targetID
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

// DayFor returns the value of the `day` column for an event timestamp.
func DayFor(eventTs time.Time) time.Time {
	utc := eventTs.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}

// Divergence is one place this port knowingly differs from the Python it
// replaces.
type Divergence struct {
	// Name is what the divergence is, in one phrase.
	Name string
	// Authority is the ruling or ticket that permits it. A divergence without
	// one is a defect.
	Authority string
	// GoldenCanSee reports whether the frozen oracle can detect a regression in
	// it. Only variant-C can; recording that stops the contract reading as more
	// enforced than it is.
	GoldenCanSee bool
	// implemented reports whether the code still does this. Its purpose is to
	// fail when a divergence is removed and the list is not updated.
	implemented func() bool
}

// Divergences is the complete enumerated list. See the fidelity contract above.
var Divergences = []Divergence{
	{
		Name:         "variant-C confidence: the associative family ranks strictly below delivery",
		Authority:    "CHAOS-4752 / CHAOS-4758",
		GoldenCanSee: true,
		implemented:  func() bool { return len(AssociativeConfidenceExceptions) > 0 },
	},
	{
		Name:         "malformed_pr_id where Python raises an uncaught ValueError and aborts the org's build",
		Authority:    "CHAOS-4811, team-lead ruling",
		GoldenCanSee: false,
		implemented: func() bool {
			_, err := ParsePRDependencySource("ghpr:o/r#\u00b2")
			return errors.Is(err, ErrMalformedPRID)
		},
	},
	{
		Name:         "refusing an unscoped run, making audit gates 14/15/21/23/29/32 unreachable rather than replicated",
		Authority:    "CHAOS-4441 RequireOrganizationScope",
		GoldenCanSee: false,
		implemented:  func() bool { return requireEdgeScope("") != nil },
	},
}
