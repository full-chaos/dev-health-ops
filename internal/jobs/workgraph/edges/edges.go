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
// # FIDELITY CONTRACT, AND THE ONE DELIBERATE EXCEPTION
//
// This is a bit-exact port of the current Python behaviour, with exactly ONE
// enumerated divergence: the CHAOS-4752/CHAOS-4758 variant-C confidence policy
// (confidence.go). Python writes every dependency-derived issue<->issue edge at
// 1.0, which makes an associative tracker link indistinguishable from a PR's
// delivery link and drives the oversized-component split into deleting nodes;
// this port ranks the associative family strictly below the delivery tier.
//
// That exception is DATA, not a comment: AssociativeConfidenceExceptions is
// enumerated in confidence.go and the golden test fails on any divergence from
// the frozen Python output that the list does not name. Nothing else about the
// port is allowed to differ.
//
// Proven by tests/fixtures/workgraph_issue_edges_python_golden.json -- 6,531
// dependency rows and the 3,548 edges, 1 projection watermark and 5 mutations
// the deployed Python producer derived from them, on real synced data.
package edges

import (
	"crypto/sha256"
	"encoding/hex"
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
