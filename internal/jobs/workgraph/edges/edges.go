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
// # FIDELITY CONTRACT: FOUR ENUMERATED DIVERGENCES
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
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"time"

	"github.com/google/uuid"
)

// Node and edge type tokens. These are the `.value` of Python's NodeType and
// EdgeType (str, Enum) members and appear verbatim in the edge_id hash input,
// so they are a wire contract, not an internal naming choice. The full set
// mirrors work_graph/models.py's NodeType/EdgeType enums (not just the subset
// CHAOS-4766 needed) so CHAOS-4924's operational/fast-path/heuristic callers
// share one definition instead of each re-declaring their own strings.
const (
	NodeTypeIssue                 = "issue"
	NodeTypePR                    = "pr"
	NodeTypeCommit                = "commit"
	NodeTypeFile                  = "file"
	NodeTypeRelease               = "release"
	NodeTypeFeatureFlag           = "feature_flag"
	NodeTypeAIWorkflowRun         = "ai_workflow_run"
	NodeTypeDiff                  = "diff"
	NodeTypeReviewOutcome         = "review_outcome"
	NodeTypeDeployment            = "deployment"
	NodeTypeIncident              = "incident"
	NodeTypeOperationalService    = "operational_service"
	NodeTypeOperationalAlert      = "operational_alert"
	NodeTypeIncidentTimelineEvent = "incident_timeline_event"
	NodeTypeIncidentResponder     = "incident_responder"
	NodeTypeEscalationPolicy      = "escalation_policy"
	NodeTypeRepository            = "repository"
	NodeTypeUser                  = "user"
	NodeTypeTeam                  = "team"

	EdgeTypeBlocks           = "blocks"
	EdgeTypeRelates          = "relates"
	EdgeTypeDuplicates       = "duplicates"
	EdgeTypeIsBlockedBy      = "is_blocked_by"
	EdgeTypeIsRelatedTo      = "is_related_to"
	EdgeTypeIsDuplicateOf    = "is_duplicate_of"
	EdgeTypeParentOf         = "parent_of"
	EdgeTypeChildOf          = "child_of"
	EdgeTypeImplements       = "implements"
	EdgeTypeReferences       = "references"
	EdgeTypeContains         = "contains"
	EdgeTypeFixes            = "fixes"
	EdgeTypeTouches          = "touches"
	EdgeTypeIntroducedBy     = "introduced_by"
	EdgeTypeConfigChangedBy  = "config_changed_by"
	EdgeTypeGuards           = "guards"
	EdgeTypeImpacts          = "impacts"
	EdgeTypeHasAIWorkflow    = "has_ai_workflow"
	EdgeTypeGenerates        = "generates"
	EdgeTypeHasReviewOutcome = "has_review_outcome"
	EdgeTypeDeploys          = "deploys"
	EdgeTypeLinkedIncident   = "linked_incident"
	EdgeTypeMapsToRepository = "maps_to_repository"
	EdgeTypeHasIncident      = "has_incident"
	EdgeTypeHasAlert         = "has_alert"
	EdgeTypeHasTimelineEvent = "has_timeline_event"
	EdgeTypeHasResponder     = "has_responder"
	EdgeTypeAssignedTo       = "assigned_to"
	EdgeTypeEscalatesWith    = "escalates_with"
	EdgeTypeRemediatedBy     = "remediated_by"

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
	// RepoID and Provider are Nullable(UUID)/Nullable(String) in the live
	// schema (014_work_graph.sql). CHAOS-4766's issue-issue edges never set
	// either in Python, so they were absent from WriteEdges until CHAOS-4924's
	// operational/repo-scoped edges needed them -- nil means SQL NULL, not "".
	RepoID   *uuid.UUID
	Provider *string
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
		// Probes the BEHAVIOUR, not the exception list. `len(list) > 0` was the
		// first version and it proves nothing: neutering DependencyConfidence to
		// return DeliveryConfidence always, while leaving the list populated,
		// left this probe passing. A probe that a removed divergence survives is
		// not a probe.
		implemented: func() bool {
			return DependencyConfidence("relates") != DeliveryConfidence &&
				DependencyConfidence("relates") == AssociativeConfidence
		},
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
	{
		// The honest one: this divergence was INTRODUCED by the fix for a
		// previous divergence. Replacing `strings.ToLower` with
		// `cases.Lower(language.Und)` corrected context-sensitive final sigma
		// and moved the oracle from Go's stdlib Unicode table to x/text's --
		// which is Unicode 17 where the deployed interpreter is UCD 16.
		//
		// 28 code points differ, all Unicode 17 additions CPython treats as
		// unassigned and therefore leaves alone. Pinned exactly by
		// TestEveryRuneLowercasesLikeLivePython, which compares EVERY code point
		// rather than a derived subset.
		//
		// WHY THIS CANNOT CHANGE AN ANSWER. Two arguments, and the second is the
		// one to rely on:
		//
		// Input-side: relationship types are ASCII by provider spec, and all 11
		// distinct values in the frozen corpus are ASCII. That is an assumption
		// about upstream data and it is ENFORCED NOWHERE -- a provider emitting
		// a non-ASCII type silently invalidates it.
		//
		// Structural: the only consumers of the folded value are membership
		// tests against `blockerTypes` and `dependencyTypeMap`, whose keys are
		// pure ASCII. Lowercasing a non-ASCII rune yields a non-ASCII rune, so
		// a string containing one cannot become a member under EITHER plane --
		// membership is identical whatever arrives, and the divergence cannot
		// reach an outcome. Verified across every skew rune in every position
		// against every blocker key: 0 strings where membership differs
		// (lane-4441, delta review of this change).
		//
		// The structural argument holds under a data change that breaks the
		// input-side one, which is why both are recorded and why this order.
		Name:         "case-folding 28 Unicode 17 code points the deployed CPython leaves unchanged",
		Authority:    "CHAOS-4766 re-cert P3a, team-lead ruling; full derivation tracked as CHAOS-4869",
		GoldenCanSee: false,
		implemented: func() bool {
			// U+A7CE is one of the 28: x/text lowers it, CPython 16 does not.
			return pythonparity.Lower("\uA7CE") != "\uA7CE"
		},
	},
}
