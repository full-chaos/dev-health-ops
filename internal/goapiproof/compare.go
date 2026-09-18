// Package goapiproof implements CHAOS-5425's `prove` verb: it executes
// every registered Go-API operation against a DEPLOYED query-api build
// through the real edge path, compares the two planes' complete
// observable responses under CHAOS-4381's signed parity rules, and
// records an immutable `deployed_executed` receipt in `go_api_proof_run`
// per operation.
//
// Why a Go port of a Python comparator that already exists. The Python
// `compare_responses` (src/dev_health_ops/api/graphql/go_api_comparator.py)
// is the in-process dual-run comparator for the Python edge's own
// harnesses. `prove` is an operator command in the Go cutover's
// no-new-Python regime, so it cannot import it. This is a deliberate
// second implementation of one signed rule set, and the risk that buys
// is drift between the two -- so every rule below cites the Python
// function it mirrors by name, and the rule number from CHAOS-4381 it
// implements. A rule change lands in both or in neither.
//
// Parity rule 5 makes positional list ordering the default and every
// relaxation a per-operation exception carrying a written reason and a
// ticket. That escape (`OrderInsensitiveList`, this file's port of
// Python's `tie_ordering="relaxed"` / `relaxed_list_path`) was originally
// left unported: none of the fifteen registered operations had a use for
// it, and porting unused relaxation machinery ships a loosening its first
// user would face no review for. CHAOS-5546 is that first user:
// `investmentFull`'s sankey nodes/edges query is a plain ClickHouse
// `UNION ALL` with no outer ORDER BY, and the SAME compiled SQL run 4
// times in a row against the live engine came back in 3 different
// orderings -- a positional comparison of that list reports engine
// nondeterminism as a Go-vs-Python defect, on BOTH planes, forever.
// `OrderInsensitiveList` pairs elements by a declared key instead of
// position; every OTHER list in every OTHER operation stays positional
// by default, exactly as before.
package goapiproof

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Terminal states from src/dev_health_ops/models/go_api_registry.py's
// TERMINAL_STATES. Only the subset this comparator can itself produce is
// named here; the runner adds the transport-level ones (auth_rejected,
// timeout, proof_failed, ...) from its own observations.
const (
	TerminalStateMatch       = "match"
	TerminalStateMismatch    = "mismatch"
	TerminalStateUnsupported = "unsupported"
)

// Finding kinds. Only "mismatch" blocks a MATCH verdict; parity rule 1
// makes an error MESSAGE difference at the same (path, code) identity a
// recorded observation rather than a blocking one, because the message is
// prose and the code is the contract.
const (
	FindingMismatch          = "mismatch"
	FindingErrorMessageDrift = "error_message_drift"
	FindingWatermarkDrift    = "watermark_drift"
	FindingWatermarkMissing  = "watermark_missing"
)

// floatTolerance is CHAOS-4381 parity rule 3's Tier-B tolerance: 1e-9,
// applied as max(abs, rel) exactly as the Python comparator's
// _compare_number does.
const floatTolerance = 1e-9

// Snapshot is one plane's complete observable GraphQL response body --
// the Python comparator's ResponseSnapshot.
//
// DataPresent distinguishes `"data": null` (key present, value null: a
// nullable root field that errored) from the `data` key being absent
// entirely (a request that never reached execution, e.g. a validation
// error). Both collapse to a nil Data, and parity rule 2's
// null-vs-omission rule applies at the envelope's top level too, so the
// two must stay distinguishable.
//
// Watermark is parity rule 4's high-water value this side observed; an
// empty string means this side reported none.
type Snapshot struct {
	Data        any
	DataPresent bool
	Errors      []map[string]any
	Watermark   string
	// TrailingBytes reports that the body carried more bytes after its
	// first JSON value. The decoder silently drops them, so a comparison
	// over the decoded value is not a comparison over what was served --
	// the admission gate refuses rather than compare.
	TrailingBytes bool
	// BodyBytes is the raw response body's length, set by DecodeSnapshot
	// from the bytes it decoded. CHAOS-5661's bodySizeDisagreement reads
	// it directly: a gross size mismatch between the two legs is a
	// structural signal independent of (and checked before) any decoded
	// value, and it has to come from the BYTES, since a value comparison
	// never sees how large the body that produced it was.
	BodyBytes int
}

// Finding is one comparator observation.
type Finding struct {
	Kind   string `json:"kind"`
	Path   string `json:"path"`
	Detail string `json:"detail"`
	// Shape says WHAT differs, and decides whether a baseline-defect
	// citation may cover it: only a LEAF difference can be (see
	// leafDifference). Empty on findings the comparator did not classify
	// (transport-level ones the runner adds), and an empty shape is never
	// coverable.
	Shape string `json:"shape,omitempty"`
}

// Finding shapes. A citation (BaselineDefect) declares that the Python
// baseline produces wrong VALUES under a path. It covers a difference only
// where both sides are LEAVES -- a scalar or null -- at the differing path:
// a scalar against another scalar, a scalar against null, null against a
// scalar. It never covers a STRUCTURAL difference -- a list of another
// length, a container (object or list) against null or a scalar, an object
// against a list, a key or element present on one side only -- whatever the
// cited path.
//
// Before shapes existed a cited path covered EVERY finding
// beneath it, and compareList reports a LENGTH difference at the list's own
// path. hotspots cites the whole `data.hotspots.rows` subtree, so a Go build
// returning `[]`, `null`, or rows of empty objects was a fully-cited mismatch
// with outside=0 -- primary enablement proof. None of those is the cited
// defect. A NULL leaf is: row selection (hotspots' defect) surfaces as a
// null in one plane and a value in the other, in both directions.
const (
	// ShapeValue (leaf): two scalars of one JSON type (string, number or
	// bool), both finite, that differ.
	ShapeValue = "value"
	// ShapeNull (leaf): null on one side, a finite scalar on the other.
	ShapeNull = "null"
	// ShapeScalarType (leaf): two finite scalars of different JSON types,
	// e.g. a string where the other plane has a number.
	ShapeScalarType = "scalar_type"
	// ShapeStructure (structural): a container (object or list) against
	// anything of another kind -- null, a scalar, or the other container.
	ShapeStructure = "structure"
	// ShapeLength (structural): two lists of different lengths.
	ShapeLength = "length"
	// ShapePresence (structural): a key, an element (by key), an error or
	// `data` itself present on one side only.
	ShapePresence = "presence"
	// ShapeEmptyResult (structural): a leaf difference under
	// a cited path whose CANDIDATE subtree has no non-null leaf while the
	// baseline's has at least one -- an empty result in disguise (every
	// field null, the right shape). Assigned by classifyBaselineDefects in
	// place of the leaf shape the comparator gave.
	ShapeEmptyResult = "empty_result"
	// ShapeNonFinite: NaN or Infinity on either side. Never covered: a
	// non-finite value is forbidden in Go output (null per field at the
	// write boundary), so one reaching a response is a Go defect no Python
	// baseline defect can explain; parity rule 3 also reports it even when
	// both sides carry the same literal.
	ShapeNonFinite = "non_finite"
)

// leafDifference reports whether a finding's shape is one a citation may
// cover: both sides were a scalar or null at the differing path.
func leafDifference(shape string) bool {
	return shape == ShapeValue || shape == ShapeNull || shape == ShapeScalarType
}

// jsonKind names a decoded JSON value's type for the leaf/structure split.
func jsonKind(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case bool:
		return "bool"
	case string:
		return "string"
	}
	if _, isNumber := asFloat(value); isNumber {
		return "number"
	}
	return fmt.Sprintf("%T", value)
}

func isContainerKind(kind string) bool {
	return kind == "object" || kind == "array"
}

// nonFinite reports a decoded number that is NaN or +/-Inf.
func nonFinite(value any) bool {
	f, isNumber := asFloat(value)
	return isNumber && (math.IsNaN(f) || math.IsInf(f, 0))
}

// Result is the comparator's verdict plus every observation behind it.
type Result struct {
	TerminalState string    `json:"terminal_state"`
	Findings      []Finding `json:"findings"`
	// CoveredByShape and OutsideByShape count the mismatch findings a
	// citation covered and did not cover, by Shape: the
	// receipt, the go-api-prove line and `enable` show WHAT was counted,
	// so "one value differed" and "Go returned no rows" are not the same
	// line. Their sums are the covered count and the outside count.
	CoveredByShape map[string]int `json:"covered_by_shape,omitempty"`
	OutsideByShape map[string]int `json:"outside_by_shape,omitempty"`
	// UnusedExclusions names every VolatileFields entry that matched no
	// path in this comparison. An exclusion that excuses nothing is
	// either stale or misspelled, and a misspelled exclusion silently
	// excuses NOTHING while reading as though it excuses something --
	// the caller fails the run on a non-empty value here. Same discipline
	// as internal/testsupport/oraclecompare: "exclusions that need a
	// written reason and must actually match something".
	UnusedExclusions []string `json:"unused_exclusions,omitempty"`

	// UnusedTierB names every FloatTierB entry that matched no compared
	// numeric field. Same rule, same reason as UnusedExclusions: a Tier-B
	// declaration that relaxes nothing reads as a relaxation that is
	// there, and the first person to trust it is trusting nothing.
	UnusedTierB []string `json:"unused_tier_b,omitempty"`

	// UndeclaredNumericLeaves names every numeric leaf path reached
	// while opts.NumericLeavesDeclared was set that opts.FloatTierB,
	// opts.FloatExactLeaves and opts.IntegerLeaves ALL leave undeclared.
	// The per-entry numeric-leaf declaration is opt-out for tolerance (a
	// float leaf is Tier-B by default) but never optional for TYPE: every
	// numeric leaf an entry with the marker set actually
	// reaches must be named as float or integer once, or this run cannot
	// stand -- an entry that leaves one leaf unclassified is exactly the
	// gap that let a real Tier-A/Tier-B mismatch on this route go
	// unnoticed before. The comparison itself still runs Tier A for an
	// undeclared leaf (the same default a route with no marker at all
	// gets), so this field is the ONLY signal a corpus author sees.
	UndeclaredNumericLeaves []string `json:"undeclared_numeric_leaves,omitempty"`

	// BaselineDefectsMatched names the tickets whose declared paths cover
	// at least one of this comparison's differences.
	BaselineDefectsMatched []string `json:"baseline_defect,omitempty"`

	// StaleBaselineDefects names declared baseline-defect entries that
	// cover NO difference here -- the defect was fixed, or the paths are
	// wrong. Either way the entry must go, so the caller fails the run.
	StaleBaselineDefects []string `json:"stale_baseline_defects,omitempty"`

	// LiveBaselineDefectsUnexplained names SHAPED entries whose own cited
	// Paths carried at least one real leaf-level difference this run --
	// the mechanism was LIVE, not absent -- but whose shape admitted NONE
	// of them. This is deliberately never folded into
	// IdleIntermittentBaselineDefects, Intermittent or not: "idle" means
	// the citation's own Paths carried nothing to explain, which is a
	// property of the request (a merge happened, a population moved on);
	// this means they carried something and the shape's own rules failed
	// to account for it, which is a property of the SHAPE, and staying
	// silent about that difference is exactly the failure mode this
	// package's four fixed checks-that-cannot-fail were. The caller fails
	// the run on this exactly as it does on a stale declaration.
	LiveBaselineDefectsUnexplained []string `json:"live_baseline_defects_unexplained,omitempty"`

	// IdleIntermittentBaselineDefects names intermittent entries whose
	// own cited Paths carried NO difference here at all -- the true idle
	// case: the mechanism is absent, there is nothing to explain. Unlike
	// a stale entry (or a live-but-unexplained one, above) this does not
	// fail the run: the defect is expected to be absent while the
	// baseline's source table is merged. It is recorded so an idle
	// declaration stays visible instead of silent.
	IdleIntermittentBaselineDefects []string `json:"idle_intermittent_baseline_defects,omitempty"`

	// DifferencesOutsideBaselineDefect counts the mismatch findings NOT
	// covered by any declared baseline defect. It is serialised even when
	// zero: "every difference is a known Python defect" and "there were no
	// differences" are different facts, and an omitted zero hides which
	// one happened.
	DifferencesOutsideBaselineDefect int `json:"differences_outside_baseline_defect"`

	// UnusedOrderInsensitiveLists names every OrderInsensitiveLists entry
	// whose Path matched no list in this comparison. Same discipline as
	// UnusedExclusions/UnusedTierB: a relaxation that relaxes nothing is
	// either stale or misspelled, and the caller fails the run on it.
	UnusedOrderInsensitiveLists []string `json:"unused_order_insensitive_lists,omitempty"`

	// OrderInsensitiveListRefusals names every declared list whose
	// comparison hit an element missing one of its declared KeyFields --
	// the declaration does not describe the data it was pointed at, so
	// the pairing it promises cannot be built. The caller fails the run
	// on this exactly as it does on a stale declaration.
	OrderInsensitiveListRefusals []string `json:"order_insensitive_list_refusals,omitempty"`

	// StructuralRefusal is CHAOS-5661's precondition, checked BEFORE any
	// value in `data` is compared: see structuralAgreementFailure,
	// bodySizeDisagreement and vacuousEmptyLegs. Non-empty means Compare
	// refused outright -- every other field above stays zero-valued,
	// because no value comparison ran at all. One of the run.go
	// RefusalLegsDoNotOverlap / RefusalVacuousEmptyLegs constants.
	//
	// This exists because a path-joining comparator that only ever
	// visits paths present on BOTH legs reports zero differences when the
	// legs share none -- JOB 7 step 7 run 2 measured exactly that:
	// flowMatrix's TEAM/REPO variants are Go name-keyed, Python
	// UUID-keyed, with ZERO ids in common, and the run refused only
	// because the declared CHAOS-5426 baseline defect matched nothing --
	// a refusal that reads as "the ticket citation is stale" when the
	// real fact is "these two legs are not describing the same data at
	// all". Checked ahead of the stale-baseline-defect guard so that
	// refusal text is never emitted for THIS cause again.
	StructuralRefusal string `json:"structural_refusal,omitempty"`
	// StructuralDetail explains StructuralRefusal: the key or id sets
	// that failed to overlap, or the body-size ratio, so a reader does
	// not have to reproduce the run to see why.
	StructuralDetail string `json:"structural_detail,omitempty"`

	// StochasticLeafCitation is set when Options.StochasticLeaves applied
	// and described the response: the covered leaves were NOT compared by
	// value, so the terminal state is mismatch and never match, and a
	// receipt carries this string in its baseline_defect array. Empty
	// otherwise.
	StochasticLeafCitation string `json:"stochastic_leaf_class,omitempty"`

	// StochasticLeafRefusals names every way Options.StochasticLeaves did
	// not describe this comparison: an invalid declaration, a covered path
	// that crosses a list or ends on a container, or a path neither plane
	// carries. The caller fails the run on a non-empty value.
	StochasticLeafRefusals []string `json:"stochastic_leaf_refusals,omitempty"`
}

// IsMatch reports whether the verdict is a clean match.
func (r Result) IsMatch() bool { return r.TerminalState == TerminalStateMatch }

// Options carries the per-operation parity configuration CHAOS-4381
// requires to be declared, never inferred.
type Options struct {
	// FloatTierB names Tier-B float fields by dotted, index-free path
	// (e.g. "data.hotspots.edges.score") mapped to the WRITTEN REASON
	// that field tolerates a difference. Every field not named here is
	// Tier A (exact) by default -- parity rule 3.
	//
	// The reason this table exists at all, measured under CHAOS-5451:
	// ClickHouse merges partial aggregate states in thread-completion
	// order, so avg/sum/stddevPop over Float64 differ by 1-2 ULP run to
	// run on BOTH planes -- 20 runs of stddevPop over 5M rows produced 9
	// distinct values, and max_threads=1 produced 1. That is engine
	// nondeterminism, not a Go-vs-Python defect, and comparing such a
	// field Tier A (as the enablement harness did) manufactures a
	// mismatch. It is equally not a licence to relax any field that
	// happens to differ: an entry here names a field whose value is a
	// merged floating-point aggregate, and nothing else.
	//
	// For an entry with NumericLeavesDeclared set, FloatTierB is the
	// default for every leaf whose DECLARED TYPE is float -- a float leaf
	// compares Tier B unless it also appears in FloatExactLeaves.
	// FloatTierB/FloatExactLeaves
	// TOGETHER are the entry's float-leaf declaration; IntegerLeaves is
	// its integer-leaf declaration. A route with NumericLeavesDeclared
	// unset keeps today's plain opt-in behaviour, unchanged: FloatTierB
	// is read exactly as this comment's first paragraph says, and
	// FloatExactLeaves/IntegerLeaves are never consulted.
	//
	// A key here (same for FloatExactLeaves/IntegerLeaves) may name ONE
	// segment with a trailing "+" (e.g. "data.root.children+.value") --
	// the ONE repeat-marker form these three tables accept, for a
	// RECURSIVE tree of UNBOUNDED depth a fixed dotted path cannot
	// enumerate (flame/aggregated's own code_hotspots mode, a real
	// repository file path split on "/"). It matches a tiered leaf path
	// whose corresponding position holds one or MORE consecutive
	// occurrences of the base segment name ("children"), never zero --
	// a depth-0 leaf (no repeated segment at all) needs its own,
	// separate, plain entry. Picked because a real JSON key this
	// package's own leaf paths are built from (a Go struct's json tag or
	// a Python field name) never contains "+". validateNumericLeaves
	// rejects a "+" anywhere but as the sole trailing character of a
	// non-terminal segment. See matchRepeatDeclaration's own doc comment
	// for the matching rule itself.
	FloatTierB map[string]string

	// FloatExactLeaves names float-DECLARED leaves (dotted, index-free
	// path, same form as FloatTierB) that this entry opts back OUT of
	// Tier-B tolerance despite their float provenance, mapped to the
	// WRITTEN REASON exact comparison is still correct for that leaf.
	// Only consulted when NumericLeavesDeclared is set. A leaf here
	// still counts as declared-float for UndeclaredNumericLeaves; it
	// simply does not get the tolerance FloatTierB would otherwise give
	// it. Never used to relax an integer leaf -- that is IntegerLeaves'
	// own, permanently-exact, declaration.
	FloatExactLeaves map[string]string

	// IntegerLeaves names leaves (same dotted, index-free path form)
	// whose DECLARED TYPE is integer -- a bare count or id, never a
	// merged ClickHouse floating-point aggregate, whatever its WIRE type
	// happens to be (a count cast toFloat64(...) purely so the driver can
	// scan it is still declared integer here; see FloatTierB/
	// FloatExactLeaves for the converse). Mapped to the WRITTEN REASON
	// the leaf is integer-valued. Comparison is Tier A (exact) either
	// way -- this map exists so NumericLeavesDeclared's own enforcement
	// can tell "declared integer" apart from "never reached a
	// declaration at all". Only consulted when NumericLeavesDeclared is
	// set.
	IntegerLeaves map[string]string

	// NumericLeavesDeclared marks this entry as having named EVERY
	// numeric leaf its own response shape can reach, as float
	// (FloatTierB/FloatExactLeaves) or integer (IntegerLeaves) -- the
	// per-route marker. A numeric leaf this comparison
	// actually reaches that appears in none of the three is recorded
	// into Result.UndeclaredNumericLeaves and fails the run: the
	// guarantee this marker buys is that every float leaf on a marked
	// route is named, none inferred, which is what makes the opt-out
	// default (FloatTierB's own doc comment) safe to apply. A route
	// without this marker is unaffected: FloatTierB stays a plain opt-in
	// and no leaf is ever required to appear anywhere.
	NumericLeavesDeclared bool

	// BaselineDefects declares differences that are known PYTHON defects
	// with Go correct. See BaselineDefect.
	BaselineDefects []BaselineDefect

	// VolatileFields names fields excluded from comparison entirely, by
	// the same dotted, index-free path form. Reserved for values that are
	// freshly generated per request and therefore cannot agree across two
	// calls -- capacityForecast's forecastId/computedAt are the measured
	// instance (CHAOS-5425 comment: identical request, Go
	// forecastId=33fb9f32..., Python 78296c67..., timestamps ~350ms
	// apart). This is NOT a general "ignore a difference I do not like"
	// knob: each entry carries a written reason in exclusions.go, and an
	// entry matching nothing fails the run via UnusedExclusions.
	VolatileFields map[string]string

	// EnvelopeKeys excuses named keys from the null-vs-omission check at
	// the TOP LEVEL of `data` only -- parity rule 2's stated exception for
	// transport-envelope keys one framework omits when empty. Never
	// applied to a nested field of the same name.
	EnvelopeKeys map[string]bool

	// RequireWatermark marks the operation as watermark-bearing: a
	// MISSING watermark on either side is then `unsupported`, not a
	// silent comparison without one -- parity rule 4.
	RequireWatermark bool

	// OrderInsensitiveLists declares parity rule 5's per-operation escape:
	// a named list is compared by KEY, not position, because the
	// underlying data source has no order guarantee. See
	// OrderInsensitiveList.
	OrderInsensitiveLists []OrderInsensitiveList

	// StochasticLeaves names leaves whose values are drawn per request and
	// are therefore checked for type, order and date offset instead of
	// compared across planes. nil for every operation that does not draw
	// its answer. See StochasticLeafClass.
	StochasticLeaves *StochasticLeafClass
}

// OrderInsensitiveList declares that a list at Path must be compared by
// KeyFields rather than position -- CHAOS-5546's finding for
// investmentFull's sankey nodes/edges: the underlying ClickHouse
// `UNION ALL` has no ORDER BY, so the SAME query returns different
// element orders run to run, on both planes, and a positional comparison
// reports that engine artefact as a Go-vs-Python defect forever.
//
// This changes HOW two lists are compared, never whether every element
// still is: elements are paired by KeyFields and every other field is
// still compared value-for-value within the pair (parity rules 1-4 all
// still apply inside a paired element); a key present on only one side is
// still a finding (a real missing/extra element, not a reordering); a
// length difference is still visible as an implied missing/extra key.
// The two vacuity guards mirror BaselineDefect's "an entry that excuses
// nothing fails the run": a declaration matching NO list in this
// comparison, or an element that does not carry every one of KeyFields at
// all, is a REFUSAL (Result.UnusedOrderInsensitiveLists /
// Result.OrderInsensitiveListRefusals), never a silent no-op and never a
// promotion to match.
type OrderInsensitiveList struct {
	// Path is the dotted, index-free path to the LIST ITSELF (the same
	// form FloatTierB/VolatileFields/BaselineDefect.Paths use), e.g.
	// "data.analytics.sankey.nodes".
	Path string
	// KeyFields names the element field(s) that together uniquely
	// identify an element for pairing, e.g. []string{"id"} for sankey
	// nodes or []string{"source", "target"} for sankey edges.
	KeyFields []string
	// Reason states, in words, why this list's order carries no parity
	// signal.
	Reason string
	// Ticket is the issue that owns the relaxation, e.g. "CHAOS-5546".
	Ticket string
}

// BaselineDefect declares that a named set of field paths differs
// because the BASELINE (Python) is wrong and the candidate (Go) is right.
//
// It changes what a receipt SAYS, never what it is. Measured instances:
// CHAOS-5448 (Python omits FINAL on work_item_cycle_times and so counts
// superseded row versions) and CHAOS-5450 (Python's list path emits a
// naive timestamp). Both are real divergences, and a proof run must keep
// recording them as such.
//
// So the rules are deliberately narrow:
//
//   - the terminal state stays `mismatch`, unchanged. A declared baseline
//     defect NEVER converts a mismatch into a match and never promotes an
//     operation. Anything else would let "we know why" become "it passed".
//   - the receipt additionally records which tickets covered the
//     differences, and an explicit count of the differences covered by
//     NONE of them -- so "every difference is a known Python defect" is a
//     readable, checkable claim rather than a footnote in a chat message.
//   - an entry covering no difference in this comparison FAILS the run.
//     The defect was fixed, or the paths are wrong; a stale exemption that
//     silently exempts nothing is the failure mode this whole file is
//     built against. The one exception is an entry marked Intermittent
//     with a stated IntermittentReason: a defect that exists only while
//     the baseline's source table holds unmerged row versions is recorded
//     as idle instead of stale when it covers nothing. Its coverage rules
//     and the mismatch terminal state are unchanged.
type BaselineDefect struct {
	// Ticket is the issue that owns the defect, e.g. "CHAOS-5448".
	Ticket string
	// Reason states, in words, what the baseline gets wrong.
	Reason string
	// Paths are dotted, index-free field paths (the same form
	// FloatTierB and VolatileFields use). A path also reaches everything
	// beneath it, so naming a subtree root reaches its fields -- but only
	// a LEAF difference (scalar or null on both sides) is covered. A
	// length, presence or structure difference beneath a cited path is
	// outside every citation (see leafDifference).
	Paths []string
	// Intermittent marks a defect that is visible only while the
	// baseline's source table still holds unmerged row versions. A
	// comparison taken after the merge shows no difference under Paths,
	// and for this entry alone that is expected rather than stale. It
	// relaxes the stale rule and nothing else.
	Intermittent bool
	// IntermittentReason states why the defect comes and goes. It is
	// mandatory when Intermittent is set, and forbidden when it is not.
	IntermittentReason string

	// RepoFanoutShape, when set, replaces this defect's blanket "any leaf
	// difference under Paths is covered" rule with a shape-specific
	// admission built from the two DECODED response bodies -- see
	// RepoFanoutShape's own doc comment (repofanout.go). nil is the
	// default, unchanged blanket behaviour every other declared defect
	// still uses.
	RepoFanoutShape *RepoFanoutShape

	// CoverageShiftShape, when set, replaces this defect's blanket "any
	// leaf difference under Paths is covered" rule with a shape-specific
	// admission built from the whole comparison's mismatch paths and the
	// two DECODED response bodies -- see CoverageShiftShape's own doc
	// comment (coverageshift.go). nil is the default, unchanged blanket
	// behaviour every other declared defect still uses. A defect never
	// sets both this and RepoFanoutShape.
	CoverageShiftShape *CoverageShiftShape

	// WorkGraphEdgeDedupShape, when set, replaces this defect's blanket
	// "any leaf difference under Paths is covered" rule with a
	// shape-specific admission built from the two DECODED edge lists --
	// see WorkGraphEdgeDedupShape's own doc comment (workgraphedgedup.go).
	// nil is the default, unchanged blanket behaviour every other
	// declared defect still uses. A defect never sets more than one shape
	// field.
	WorkGraphEdgeDedupShape *WorkGraphEdgeDedupShape

	// SupersessionSkewShape, when set, replaces this defect's blanket
	// "any leaf difference under Paths is covered" rule with a
	// direction-only, whole-comparison admission built from the whole
	// comparison's mismatch paths and the two DECODED coverage leaves --
	// see SupersessionSkewShape's own doc comment (supersessionskew.go).
	// nil is the default, unchanged blanket behaviour every other
	// declared defect still uses. A defect never sets more than one shape
	// field.
	SupersessionSkewShape *SupersessionSkewShape

	// SankeyRepoFanoutShape, when set, replaces this defect's blanket
	// "any leaf difference under Paths is covered" rule with a per-edge
	// integer-multiplier admission built from the two DECODED sankey
	// nodes/links subtrees -- see SankeyRepoFanoutShape's own doc comment
	// (sankeyrepofanout.go). nil is the default, unchanged blanket
	// behaviour every other declared defect still uses. A defect never
	// sets more than one shape field.
	SankeyRepoFanoutShape *SankeyRepoFanoutShape

	// KeyedDirectionShape, when set, replaces this defect's blanket "any
	// leaf difference under Paths is covered" rule with a per-key,
	// direction-only admission built from the two DECODED lists at a
	// declared path -- see KeyedDirectionShape's own doc comment
	// (keyeddirection.go). nil is the default, unchanged blanket
	// behaviour every other declared defect still uses. A defect never
	// sets more than one shape field.
	KeyedDirectionShape *KeyedDirectionShape

	// ConservationShape, when set, replaces this defect's blanket "any
	// leaf difference under Paths is covered" rule with a
	// direction-agnostic, whole-comparison total-conserved admission
	// built from the whole comparison's mismatch paths and the two
	// DECODED lists at a declared path -- see ConservationShape's own
	// doc comment (sankeyconservation.go). nil is the default, unchanged
	// blanket behaviour every other declared defect still uses. A defect
	// never sets more than one shape field.
	ConservationShape *ConservationShape

	// DictKeyDirectionShape, when set, replaces this defect's blanket
	// "any leaf difference under Paths is covered" rule with a per-key,
	// direction-only admission built from the two DECODED JSON OBJECTS at
	// a declared path -- see DictKeyDirectionShape's own doc comment
	// (dictkeydirection.go). nil is the default, unchanged blanket
	// behaviour every other declared defect still uses. A defect never
	// sets more than one shape field.
	DictKeyDirectionShape *DictKeyDirectionShape

	// ScalarDirectionShape, when set, replaces this defect's blanket "any
	// leaf difference under Paths is covered" rule with a direction-only
	// admission on ONE named scalar leaf, optionally whole-comparison
	// gated -- see ScalarDirectionShape's own doc comment
	// (scalardirection.go). nil is the default, unchanged blanket
	// behaviour every other declared defect still uses. A defect never
	// sets more than one shape field.
	ScalarDirectionShape *ScalarDirectionShape

	// TeamCoverageIdentityShape, when set, replaces this defect's blanket
	// "any leaf difference under Paths is covered" rule with a per-leg
	// arithmetic identity between a team_coverage leaf and its OWN
	// response's team-group sankey nodes, admitted only once every
	// team-group node difference in this comparison is already covered
	// by another declared defect -- see TeamCoverageIdentityShape's own
	// doc comment (teamcoverageidentity.go). Unlike every other shape in
	// this file, it is evaluated in a SECOND pass, after every other
	// defect's own `covered` decision is final (classifyBaselineDefects
	// enforces this). nil is the default, unchanged blanket behaviour
	// every other declared defect still uses. A defect never sets more
	// than one shape field.
	TeamCoverageIdentityShape *TeamCoverageIdentityShape

	// LimitDisplacementShape, when set, replaces this defect's blanket
	// "any leaf difference under Paths is covered" rule with a per-key
	// admission of a LIMIT-boundary presence swap -- a row entering one
	// plane's list and another leaving it, both explained by the SAME
	// already-covered value-multiplying defect -- see
	// LimitDisplacementShape's own doc comment (limitdisplacement.go).
	// Like TeamCoverageIdentityShape, it is evaluated in a SECOND pass,
	// after every other defect's own `covered` decision is final. nil is
	// the default, unchanged blanket behaviour every other declared
	// defect still uses. A defect never sets more than one shape field.
	LimitDisplacementShape *LimitDisplacementShape

	// BoundaryTieShape, when set, replaces this defect's blanket "any
	// leaf difference under Paths is covered" rule with a per-key
	// admission of a presence swap caused by a genuinely UNORDERED tie at
	// a LIMIT-bounded list's own cutoff -- unlike LimitDisplacementShape,
	// there is no value-inflating root cause or direction: the swap is
	// admitted purely because every displaced key, on both sides, shares
	// the SAME tie value -- see BoundaryTieShape's own doc comment
	// (boundarytie.go). It reads nothing from any other defect's own
	// `covered` state, so unlike TeamCoverageIdentityShape/
	// LimitDisplacementShape/HotspotListBoundaryShape/TeamRepoSubsetShape
	// it is evaluated in the FIRST pass, the same as every other
	// self-contained shape in this file. nil is the default, unchanged
	// blanket behaviour every other declared defect still uses. A defect
	// never sets more than one shape field.
	BoundaryTieShape *BoundaryTieShape

	// HotspotListBoundaryShape, when set, replaces this defect's blanket
	// "any leaf difference under Paths is covered" rule with a
	// LIMIT-boundary admission over a reconstructed file list AND its
	// own repo->directory parent-sum consequence -- see
	// HotspotListBoundaryShape's own doc comment
	// (hotspotlistboundary.go). Its own repository multiplier is read
	// from another defect's already-covered findings, so like
	// TeamCoverageIdentityShape and LimitDisplacementShape it is
	// evaluated in a SECOND pass, after every other defect's own
	// `covered` decision is final. nil is the default, unchanged
	// blanket behaviour every other declared defect still uses. A
	// defect never sets more than one shape field.
	HotspotListBoundaryShape *HotspotListBoundaryShape

	// TeamRepoSubsetShape, when set, replaces this defect's blanket "any
	// leaf difference under Paths is covered" rule with a bounded-subset
	// admission built from the two DECODED lists at a declared path -- see
	// TeamRepoSubsetShape's own doc comment (teamreposubset.go). Unlike
	// every other shape in this file except LimitDisplacementShape, it can
	// admit a STRUCTURAL finding (ShapeLength or a one-directional
	// ShapePresence), never only a leaf one -- see the gate in
	// classifyBaselineDefects. Its own EqualLeaves check on a matched
	// element also reads another defect's already-covered findings (an
	// element whose own leaf difference a sibling shape already explains,
	// e.g. sankeyRepoDedupParity's own SankeyRepoFanoutShape, never
	// invalidates the SUBSET claim for the rest of the list), so like
	// TeamCoverageIdentityShape, LimitDisplacementShape and
	// HotspotListBoundaryShape it is evaluated in a SECOND pass, after
	// every other defect's own `covered` decision is final. nil is the
	// default, unchanged blanket behaviour every other declared defect
	// still uses. A defect never sets more than one shape field.
	TeamRepoSubsetShape *TeamRepoSubsetShape

	// ZeroValueEmptyListShape, when set, replaces this defect's blanket "any
	// leaf difference under Paths is covered" rule with a value-collapse-
	// consequence admission of a ShapePresence finding at ONE declared list
	// path -- see ZeroValueEmptyListShape's own doc comment
	// (zerovalueemptylist.go). Like TeamRepoSubsetShape, it can admit a
	// STRUCTURAL finding (a one-directional ShapePresence), never only a
	// leaf one -- see the gate in classifyBaselineDefects. nil is the
	// default, unchanged blanket behaviour every other declared defect
	// still uses. A defect never sets more than one shape field.
	ZeroValueEmptyListShape *ZeroValueEmptyListShape

	// HeatmapCellBoundaryShape, when set, replaces this defect's blanket
	// "any leaf difference under Paths is covered" rule with a
	// LIMIT-boundary admission over a reconstructed heatmap file list AND
	// its own axis-reordering consequence -- see HeatmapCellBoundaryShape's
	// own doc comment (heatmapcellboundary.go). Like LimitDisplacementShape
	// and HotspotListBoundaryShape, it can admit a STRUCTURAL finding (a
	// ShapePresence cell), never only a leaf one -- see the gate in
	// classifyBaselineDefects. nil is the default, unchanged blanket
	// behaviour every other declared defect still uses. A defect never
	// sets more than one shape field.
	HeatmapCellBoundaryShape *HeatmapCellBoundaryShape

	// DuplicateCollapseLengthShape, when set, replaces this defect's
	// blanket "any leaf difference under Paths is covered" rule with a
	// whole-list admission of a ShapeLength finding, built from the two
	// DECODED lists -- see DuplicateCollapseLengthShape's own doc comment
	// (dedupcollapselength.go). It can admit a STRUCTURAL finding (a
	// ShapeLength on the list itself), never a leaf one -- see the gate
	// in classifyBaselineDefects. nil is the default, unchanged blanket
	// behaviour every other declared defect still uses. A defect never
	// sets more than one shape field.
	DuplicateCollapseLengthShape *DuplicateCollapseLengthShape
}

// validateBaselineDefects refuses a declaration that claims the
// intermittent exemption without saying why, or gives a reason for an
// exemption it does not claim.
func validateBaselineDefects(defects []BaselineDefect) error {
	for _, defect := range defects {
		hasReason := strings.TrimSpace(defect.IntermittentReason) != ""
		if defect.Intermittent && !hasReason {
			return fmt.Errorf("goapiproof: baseline defect %s is marked intermittent with no IntermittentReason -- an exemption from the stale rule must say why the defect comes and goes", defect.Ticket)
		}
		if !defect.Intermittent && hasReason {
			return fmt.Errorf("goapiproof: baseline defect %s gives an IntermittentReason but is not marked intermittent -- the reason describes an exemption the entry does not claim", defect.Ticket)
		}
	}
	return nil
}

// validateNumericLeaves catches a contradictory declaration before it
// ever reaches a live comparison: a path named in IntegerLeaves cannot
// also appear in FloatTierB or FloatExactLeaves -- a declared type is a
// property of the leaf's domain, never a runtime observation, and that
// rule means a path is float or integer, never both. FloatTierB and
// FloatExactLeaves overlapping is expected and fine (see
// Options.FloatExactLeaves' own doc comment): together they ARE the
// entry's float declaration, one naming the tolerant leaves and the
// other the ones opted back to exact. It also catches a malformed
// repeat-segment marker (validateRepeatMarker) in any of the three
// tables' own keys, before that key ever reaches matchRepeatDeclaration.
func validateNumericLeaves(opts Options) error {
	for _, table := range []map[string]string{opts.FloatTierB, opts.FloatExactLeaves, opts.IntegerLeaves} {
		for path := range table {
			if err := validateRepeatMarker(path); err != nil {
				return err
			}
		}
	}
	for path := range opts.IntegerLeaves {
		if _, ok := opts.FloatTierB[path]; ok {
			return fmt.Errorf("goapiproof: %q is declared in both IntegerLeaves and FloatTierB -- a leaf is float or integer, never both", path)
		}
		if _, ok := opts.FloatExactLeaves[path]; ok {
			return fmt.Errorf("goapiproof: %q is declared in both IntegerLeaves and FloatExactLeaves -- a leaf is float or integer, never both", path)
		}
	}
	for path := range opts.FloatExactLeaves {
		if _, ok := opts.FloatTierB[path]; !ok {
			return fmt.Errorf("goapiproof: %q is declared in FloatExactLeaves but not FloatTierB -- FloatExactLeaves opts a float-declared leaf back to exact, it does not declare a leaf float on its own", path)
		}
	}
	return nil
}

// validateRepeatMarker rejects a malformed repeat-segment marker in a
// declared numeric-leaf path (FloatTierB/FloatExactLeaves/IntegerLeaves'
// own doc comment): the ONE accepted form is a whole dotted segment
// ending in exactly one trailing "+", with at least one character
// before it, and it can never be the path's own LAST segment -- a
// repeat segment names a recursive STRUCTURAL node (e.g. "children+"),
// never the leaf itself, so at least one segment (the leaf's own field
// name) must follow it. A "+" anywhere else -- mid-segment, doubled, or
// with an empty base -- is rejected rather than silently matching
// nothing or matching something a corpus author did not intend.
func validateRepeatMarker(path string) error {
	segments := strings.Split(path, ".")
	for i, segment := range segments {
		if !strings.Contains(segment, "+") {
			continue
		}
		base, isRepeat := strings.CutSuffix(segment, "+")
		if !isRepeat || base == "" || strings.Contains(base, "+") {
			return fmt.Errorf("goapiproof: %q has a malformed repeat marker in segment %q -- the one accepted form is a whole segment ending in exactly one trailing plus sign with a non-empty base name (e.g. children+)", path, segment)
		}
		if i == len(segments)-1 {
			return fmt.Errorf("goapiproof: %q marks its own last segment %q as repeating -- a repeat segment names a recursive structural node, never the leaf itself, so at least one segment must follow it", path, segment)
		}
	}
	return nil
}

// tracker records which declared entries actually matched something, so
// a declaration that matched nothing can be reported rather than sitting
// in the table reading as coverage.
type tracker struct {
	volatile         map[string]bool
	tierB            map[string]bool
	orderInsensitive map[string]bool
	// orderInsensitiveKeyMissing collects one message per declared list
	// that hit an element missing a declared key field. A slice, not a
	// set: every occurrence is worth reporting, since it names which
	// side and which list.
	orderInsensitiveKeyMissing []string
	// undeclaredNumeric collects every numeric leaf path reached, under
	// an entry with NumericLeavesDeclared set, that FloatTierB,
	// FloatExactLeaves and IntegerLeaves all leave unnamed -- see
	// Result.UndeclaredNumericLeaves.
	undeclaredNumeric map[string]bool
}

// Compare compares a baseline (Python) and candidate (Go) response under
// CHAOS-4381's parity rules. It mirrors the Python comparator's
// compare_responses, including its ordering: watermark handling runs
// FIRST and short-circuits, because a watermark delta is ClickHouse
// eventual consistency, never Go-vs-Python evidence, and must be allowed
// neither to produce nor to hide a mismatch verdict.
func Compare(baseline, candidate Snapshot, opts Options) Result {
	if opts.RequireWatermark && (baseline.Watermark == "" || candidate.Watermark == "") {
		return Result{
			TerminalState: TerminalStateUnsupported,
			Findings: []Finding{{
				Kind: FindingWatermarkMissing,
				Path: "$.watermark",
				Detail: fmt.Sprintf("required watermark missing: baseline=%q candidate=%q",
					baseline.Watermark, candidate.Watermark),
			}},
		}
	}
	if baseline.Watermark != "" && candidate.Watermark != "" && baseline.Watermark != candidate.Watermark {
		return Result{
			TerminalState: TerminalStateUnsupported,
			Findings: []Finding{{
				Kind: FindingWatermarkDrift,
				Path: "$.watermark",
				Detail: fmt.Sprintf("baseline watermark %q != candidate watermark %q",
					baseline.Watermark, candidate.Watermark),
			}},
		}
	}

	// CHAOS-5661: judged BEFORE anything below, and over the RAW decoded
	// bodies -- never the view VolatileFields/exclusions would leave.
	// Skipping a whole collection because it is declared volatile is
	// exactly how a comparator can visit zero overlapping paths and
	// report zero differences (see compareDict's `excluded` check);
	// structure has to be judged on what the response actually carries,
	// not on what the value comparator was told to ignore.
	if baseline.DataPresent && candidate.DataPresent {
		if vacuousEmptyLegs(baseline.Data, candidate.Data, opts) {
			return Result{
				StructuralRefusal: RefusalVacuousEmptyLegs,
				StructuralDetail:  "both legs resolved to zero non-null leaves under data: an empty result on both sides is not evidence the two planes agree, because nothing was actually compared",
			}
		}
		if reason, detail := structuralAgreementFailure(baseline.Data, candidate.Data, "$.data", opts); reason != "" {
			return Result{StructuralRefusal: reason, StructuralDetail: detail}
		}
		// A declared structural shape's own valid plan against these two
		// decoded bodies defers this size-ratio refusal to the ordinary
		// comparison below, which then judges the difference through that
		// shape's own admission rule (classifyBaselineDefects) -- see
		// bodySizeGateDeferred's own doc comment.
		if reason, detail := bodySizeDisagreement(baseline.BodyBytes, candidate.BodyBytes); reason != "" && !bodySizeGateDeferred(opts, baseline.Data, candidate.Data) {
			return Result{StructuralRefusal: reason, StructuralDetail: detail}
		}
	}

	track := &tracker{volatile: map[string]bool{}, tierB: map[string]bool{}, orderInsensitive: map[string]bool{}, undeclaredNumeric: map[string]bool{}}
	findings := compareErrors(baseline.Errors, candidate.Errors, "$.errors")

	switch {
	case baseline.DataPresent != candidate.DataPresent:
		detail := "present in baseline, absent in candidate"
		if !baseline.DataPresent {
			detail = "present in candidate, absent in baseline"
		}
		findings = append(findings, Finding{Kind: FindingMismatch, Path: "$.data", Detail: detail, Shape: ShapePresence})
	case baseline.DataPresent && candidate.DataPresent:
		findings = append(findings, compareJSON(baseline.Data, candidate.Data, "$.data", opts, opts.EnvelopeKeys, track)...)
	}
	// Neither side present: two responses that never reached execution.
	// Absence on both sides is not itself a finding.

	var baselineData, candidateData any
	if baseline.DataPresent {
		baselineData = baseline.Data
	}
	if candidate.DataPresent {
		candidateData = candidate.Data
	}

	// Applied before the terminal state is decided: it turns drawn-value
	// differences on the named leaves into observations and adds its own
	// per-plane check findings as mismatches.
	var stochasticCovered int
	var stochasticRefusals []string
	if opts.StochasticLeaves != nil {
		if err := validateStochasticLeafClass(opts); err != nil {
			stochasticRefusals = []string{err.Error()}
		} else {
			findings, stochasticCovered, stochasticRefusals = applyStochasticLeafClass(opts.StochasticLeaves, baselineData, candidateData, findings)
		}
	}

	terminal := TerminalStateMatch
	for _, f := range findings {
		if f.Kind == FindingMismatch {
			terminal = TerminalStateMismatch
			break
		}
	}
	// A class that applied left its leaves uncompared, so equality over the
	// whole response was not established: the verdict is mismatch even when
	// the draws happened to agree, and the citation says why.
	stochasticApplied := opts.StochasticLeaves != nil && len(stochasticRefusals) == 0
	if stochasticApplied {
		terminal = TerminalStateMismatch
	}

	result := Result{TerminalState: terminal, Findings: findings}
	result.UnusedExclusions = unmatched(opts.VolatileFields, track.volatile)
	result.UnusedTierB = unmatched(opts.FloatTierB, track.tierB)
	result.UndeclaredNumericLeaves = sortedSet(track.undeclaredNumeric)
	classifyBaselineDefects(&result, opts.BaselineDefects, baselineData, candidateData)
	result.StochasticLeafRefusals = stochasticRefusals
	if stochasticApplied {
		result.StochasticLeafCitation = opts.StochasticLeaves.Citation()
		if stochasticCovered > 0 {
			result.CoveredByShape[ShapeStochasticLeaf] = stochasticCovered
		}
	}

	declaredOrderInsensitive := make(map[string]string, len(opts.OrderInsensitiveLists))
	for _, decl := range opts.OrderInsensitiveLists {
		declaredOrderInsensitive[decl.Path] = decl.Ticket
	}
	result.UnusedOrderInsensitiveLists = unmatched(declaredOrderInsensitive, track.orderInsensitive)
	sort.Strings(track.orderInsensitiveKeyMissing)
	result.OrderInsensitiveListRefusals = track.orderInsensitiveKeyMissing
	return result
}

// findOrderInsensitiveList looks up a declared OrderInsensitiveList by
// its list-level path (the tiered form compareList's own `path` argument
// reduces to, e.g. "data.analytics.sankey.nodes" -- never an indexed
// element path).
func findOrderInsensitiveList(opts Options, path string) (OrderInsensitiveList, bool) {
	for _, decl := range opts.OrderInsensitiveLists {
		if decl.Path == path {
			return decl, true
		}
	}
	return OrderInsensitiveList{}, false
}

// unmatched lists declared table keys that nothing marked as used.
func unmatched(declared map[string]string, used map[string]bool) []string {
	var out []string
	for path := range declared {
		if !used[path] {
			out = append(out, path)
		}
	}
	sort.Strings(out)
	return out
}

// classifyBaselineDefects attributes each mismatch finding to a declared
// baseline defect, if any, and counts the ones nothing covers.
//
// It never touches result.TerminalState. That is the whole contract: a
// declared, understood, ticketed Python defect is still a divergence, and
// the receipt still says mismatch.
func classifyBaselineDefects(result *Result, defects []BaselineDefect, baselineData, candidateData any) {
	// Under a cited path, a candidate with NO non-null leaf
	// while the baseline has at least one is an empty result in disguise.
	// Its leaf differences are relabelled structural before anything is
	// covered, so "Go returned every field null" can never pass as the
	// cited defect. One non-null leaf anywhere under the path keeps the
	// rule leaf-by-leaf (hotspots' JOB 5 receipt: 7 null of 391 leaves).
	for _, defect := range defects {
		for _, cited := range defect.Paths {
			if nonNullLeaves(candidateData, citedSegments(cited)) > 0 || nonNullLeaves(baselineData, citedSegments(cited)) == 0 {
				continue
			}
			for i := range result.Findings {
				finding := &result.Findings[i]
				path := tieredPath(finding.Path)
				if finding.Kind == FindingMismatch && leafDifference(finding.Shape) && (path == cited || strings.HasPrefix(path, cited+".")) {
					finding.Shape = ShapeEmptyResult
				}
			}
		}
	}

	var mismatches, shapes []string
	var findingRefs []int
	for i, finding := range result.Findings {
		if finding.Kind == FindingMismatch {
			mismatches = append(mismatches, tieredPath(finding.Path))
			shapes = append(shapes, finding.Shape)
			findingRefs = append(findingRefs, i)
		}
	}

	covered := make([]bool, len(mismatches))
	// repoMultiplierCovered mirrors `covered`, but is set ONLY for a
	// finding SankeyRepoFanoutShape or HotspotListBoundaryShape itself
	// admitted -- the two shapes whose own admission is grounded in a
	// verified repository fan-out multiplier. TeamRepoSubsetShape's own
	// second-pass composition (teamreposubset.go) reads THIS slice, never
	// the generic `covered`: a blanket (unshaped) citation, or a
	// directional shape with no magnitude bound (e.g. KeyedDirectionShape,
	// used by heatmap's own team-scope declarations), can cover a finding
	// of ANY size in either direction, and validating a subset plan on
	// that alone would let an unrelated, unbounded undercount silently
	// clear the whole list's subset claim -- a hole confirmed live by an
	// independent review. Only a verified, integer repository multiplier
	// is narrow enough a claim to compose with TeamRepoSubsetShape's own.
	repoMultiplierCovered := make([]bool, len(mismatches))
	// perDefect records what each defect touched, for a SECOND pass below
	// once `covered` is FINAL across every defect -- a shaped defect's own
	// citation can legitimately share Paths with a SIBLING declaration
	// that explains the same finding through a different mechanism
	// (CoverageShiftShape's own doc comment: the repos-join fan-out cites
	// the same two coverage leaves), and that sibling can run before or
	// after this one in `defects`. Deciding "live but unexplained" from
	// `covered` while it is still partially built would flag a citation
	// as unexplained purely because ITS sibling had not been processed
	// yet, or wrongly clear it because a defect processed later covers
	// the same finding for an unrelated reason -- either way the wrong
	// entry gets refused. So every defect's plans run first, `covered` is
	// finished, and only THEN is each defect judged against the final
	// state.
	type perDefect struct {
		hit    bool
		shaped bool
		// touched names the leaf-finding indices this SHAPED defect's own
		// Paths covered (defectCovers matched, leafDifference true),
		// admitted or not -- the set "live but unexplained" is drawn from.
		touched []int
	}
	verdicts := make([]perDefect, len(defects))
	// evaluateDefect runs the shared per-defect admission logic. Every
	// existing shape is a pure function of the two decoded bodies (plus,
	// for a whole-comparison shape, this run's own mismatch paths) and
	// never needs to see what any OTHER defect decided -- so ordering
	// among them has never mattered. TeamCoverageIdentityShape,
	// LimitDisplacementShape, HotspotListBoundaryShape and
	// TeamRepoSubsetShape are the exceptions: each one's own admission is
	// DEFINED in terms of `covered` from every OTHER defect, so
	// evaluateDefect is called for them ONLY in a second pass below, once
	// every ordinary defect has already run and `covered` is final for
	// everything else in this comparison.
	evaluateDefect := func(d int, defect BaselineDefect) {
		hit := false
		// Built once per defect, not per finding: a shape's plan (its
		// per-repo multiplier map and aggregate totals, or its
		// whole-comparison coverage-shift verdict) is a property of this
		// ONE comparison, and every finding under the defect's Paths is
		// judged against the SAME plan.
		var repoPlan *repoFanoutPlan
		if defect.RepoFanoutShape != nil {
			repoPlan = buildRepoFanoutPlan(defect.RepoFanoutShape, baselineData, candidateData)
		}
		var covPlan *coverageShiftPlan
		if defect.CoverageShiftShape != nil {
			covPlan = buildCoverageShiftPlan(defect.CoverageShiftShape, baselineData, candidateData, mismatches)
		}
		var dedupPlan *workGraphEdgeDedupPlan
		if defect.WorkGraphEdgeDedupShape != nil {
			dedupPlan = buildWorkGraphEdgeDedupPlan(defect.WorkGraphEdgeDedupShape, baselineData, candidateData)
		}
		var skewPlan *supersessionSkewPlan
		if defect.SupersessionSkewShape != nil {
			skewPlan = buildSupersessionSkewPlan(defect.SupersessionSkewShape, baselineData, candidateData, mismatches)
		}
		var sankeyFanoutPlan *sankeyRepoFanoutPlan
		if defect.SankeyRepoFanoutShape != nil {
			sankeyFanoutPlan = buildSankeyRepoFanoutPlan(defect.SankeyRepoFanoutShape, baselineData, candidateData)
		}
		var keyedDirPlan *keyedDirectionPlan
		if defect.KeyedDirectionShape != nil {
			keyedDirPlan = buildKeyedDirectionPlan(defect.KeyedDirectionShape, baselineData, candidateData)
		}
		var conservePlan *conservationPlan
		if defect.ConservationShape != nil {
			conservePlan = buildConservationPlan(defect.ConservationShape, baselineData, candidateData, mismatches)
		}
		var dictDirPlan *dictKeyDirectionPlan
		if defect.DictKeyDirectionShape != nil {
			dictDirPlan = buildDictKeyDirectionPlan(defect.DictKeyDirectionShape, baselineData, candidateData)
		}
		var scalarDirPlan *scalarDirectionPlan
		if defect.ScalarDirectionShape != nil {
			scalarDirPlan = buildScalarDirectionPlan(defect.ScalarDirectionShape, baselineData, candidateData, mismatches)
		}
		var identityPlan *teamCoverageIdentityPlan
		if defect.TeamCoverageIdentityShape != nil {
			identityPlan = buildTeamCoverageIdentityPlan(defect.TeamCoverageIdentityShape, baselineData, candidateData, mismatches, findingRefs, result.Findings, covered)
		}
		var displacePlan *limitDisplacementPlan
		if defect.LimitDisplacementShape != nil {
			displacePlan = buildLimitDisplacementPlan(defect.LimitDisplacementShape, baselineData, candidateData, mismatches, findingRefs, result.Findings, covered)
		}
		var tiePlan *boundaryTiePlan
		if defect.BoundaryTieShape != nil {
			tiePlan = buildBoundaryTiePlan(defect.BoundaryTieShape, baselineData, candidateData)
		}
		var hotspotBoundaryPlan *hotspotListBoundaryPlan
		if defect.HotspotListBoundaryShape != nil {
			hotspotBoundaryPlan = buildHotspotListBoundaryPlan(defect.HotspotListBoundaryShape, baselineData, candidateData, mismatches, findingRefs, result.Findings, covered)
		}
		var subsetPlan *teamRepoSubsetPlan
		if defect.TeamRepoSubsetShape != nil {
			subsetPlan = buildTeamRepoSubsetPlan(defect.TeamRepoSubsetShape, baselineData, candidateData, mismatches, findingRefs, result.Findings, repoMultiplierCovered)
		}
		var zeroValueEmptyListPlan *zeroValueEmptyListPlan
		if defect.ZeroValueEmptyListShape != nil {
			zeroValueEmptyListPlan = buildZeroValueEmptyListPlan(defect.ZeroValueEmptyListShape, baselineData, candidateData)
		}
		var heatmapCellPlan *heatmapCellBoundaryPlan
		if defect.HeatmapCellBoundaryShape != nil {
			heatmapCellPlan = buildHeatmapCellBoundaryPlan(defect.HeatmapCellBoundaryShape, baselineData, candidateData)
		}
		var dupLenPlan *duplicateCollapseLengthPlan
		if defect.DuplicateCollapseLengthShape != nil {
			dupLenPlan = buildDuplicateCollapseLengthPlan(defect.DuplicateCollapseLengthShape, baselineData, candidateData)
		}
		// A SHAPED defect's citation is LIVE only when its shape actually
		// admits something. A blanket (unshaped) citation stays live from
		// path proximity alone -- any difference under Paths, covered or
		// not, keeps the entry from reading as stale (see
		// TestCompareIntermittentBaselineDefectCoversOnlyLeafDifferences).
		// A shape exists specifically to tell a real instance of its
		// mechanism apart from an unrelated difference that merely shares
		// the same cited path -- CoverageShiftShape's own reason: the
		// repos-join fan-out (CHAOS-4773) cites the SAME two coverage
		// leaves, so path proximity alone cannot tell the two mechanisms
		// apart, and a shaped defect that hit on path alone would still
		// double-report alongside the shape that actually explains the
		// difference.
		shaped := repoPlan != nil || covPlan != nil || dedupPlan != nil || skewPlan != nil || sankeyFanoutPlan != nil || keyedDirPlan != nil || conservePlan != nil || dictDirPlan != nil || scalarDirPlan != nil || identityPlan != nil || displacePlan != nil || hotspotBoundaryPlan != nil || subsetPlan != nil || zeroValueEmptyListPlan != nil || tiePlan != nil || heatmapCellPlan != nil || dupLenPlan != nil
		var touched []int
		for i, path := range mismatches {
			if !defectCovers(defect, path) {
				continue
			}
			if !shaped {
				// The citation is LIVE -- there is a difference under its
				// path, so it is not stale -- but it COVERS only a leaf
				// difference. A length / presence / structure finding
				// under a cited subtree stays outside every citation.
				hit = true
			}
			// LimitDisplacementShape, HotspotListBoundaryShape and
			// TeamRepoSubsetShape are the only shapes that admit a
			// STRUCTURAL finding UNCONDITIONALLY -- a row entering or
			// leaving a limit-bounded list, or a candidate list narrower
			// than its baseline, is a presence or length difference by
			// construction, never a leaf one, and each shape's own doc
			// comment states exactly what makes its own admission safe.
			// DictKeyDirectionShape joins them, but ONLY for one declared
			// defect at a time: a ShapePresence finding reaches such a
			// declaration's own admits() when, and only when, that ONE
			// declaration set AdmitBaselineOnlyKeys (dictkeydirection.go's
			// own doc comment on that field) -- unlike the other three,
			// this is not "the shape is declared at all", so the
			// condition below reads defect.DictKeyDirectionShape's own
			// field directly rather than testing dictDirPlan != nil (a
			// declaration with the field left false must see EXACTLY the
			// gate behaviour it saw before that field existed). No other
			// shape ever reaches past leafDifference. This gate is
			// deliberately named BY SHAPE FIELD, never by "some shape is
			// set": widening it to let ANY shaped defect's structural
			// finding reach its own admits() would change nothing
			// observable today, because every OTHER shape's own admits()
			// dispatches on a leaf value path (KeyedDirectionShape,
			// ScalarDirectionShape, ...) and simply returns false for a
			// ShapeLength/ShapePresence finding it was never written to
			// recognise -- but naming the gate by field keeps that safety
			// a property of THIS switch, not an incidental fact about
			// every other shape's own dispatch that a future shape's
			// admits() could quietly stop upholding.
			// TestGate_OtherShapesNeverAdmitAStructuralFinding pins the
			// observable behaviour either phrasing produces today.
			if !leafDifference(shapes[i]) &&
				!(displacePlan != nil && shapes[i] == ShapePresence) &&
				!(hotspotBoundaryPlan != nil && shapes[i] == ShapePresence) &&
				!(subsetPlan != nil && (shapes[i] == ShapePresence || shapes[i] == ShapeLength)) &&
				!(dictDirPlan != nil && defect.DictKeyDirectionShape.AdmitBaselineOnlyKeys && shapes[i] == ShapePresence) &&
				!(zeroValueEmptyListPlan != nil && shapes[i] == ShapePresence) &&
				!(tiePlan != nil && shapes[i] == ShapePresence) &&
				!(heatmapCellPlan != nil && shapes[i] == ShapePresence) &&
				!(dupLenPlan != nil && shapes[i] == ShapeLength) {
				continue
			}
			if shaped {
				touched = append(touched, i)
			}
			admitted := !shaped
			switch {
			case repoPlan != nil:
				admitted = repoPlan.admits(result.Findings[findingRefs[i]])
			case covPlan != nil:
				admitted = covPlan.admits(result.Findings[findingRefs[i]])
			case dedupPlan != nil:
				admitted = dedupPlan.admits(result.Findings[findingRefs[i]])
				// The dedup shape's verdict is per id, not a
				// whole-comparison boolean, so a NOT-admitted finding
				// here can sit right beside an admitted one at the very
				// next index. Name the excluded id directly in the
				// finding's own Detail -- a reader of the report can see
				// which id was not covered and why without cross-
				// referencing the raw bodies.
				if !admitted {
					if id, ok := dedupPlan.uncoveredEdgeID(result.Findings[findingRefs[i]]); ok {
						result.Findings[findingRefs[i]].Detail += dedupPlan.refusalDetail(id)
					}
				}
			case skewPlan != nil:
				admitted = skewPlan.admits(result.Findings[findingRefs[i]])
			case sankeyFanoutPlan != nil:
				admitted = sankeyFanoutPlan.admits(result.Findings[findingRefs[i]])
			case keyedDirPlan != nil:
				admitted = keyedDirPlan.admits(result.Findings[findingRefs[i]])
			case conservePlan != nil:
				admitted = conservePlan.admits(result.Findings[findingRefs[i]])
			case dictDirPlan != nil:
				admitted = dictDirPlan.admits(result.Findings[findingRefs[i]])
			case scalarDirPlan != nil:
				admitted = scalarDirPlan.admits(result.Findings[findingRefs[i]])
			case identityPlan != nil:
				admitted = identityPlan.admits(result.Findings[findingRefs[i]])
			case displacePlan != nil:
				admitted = displacePlan.admits(result.Findings[findingRefs[i]])
			case hotspotBoundaryPlan != nil:
				admitted = hotspotBoundaryPlan.admits(result.Findings[findingRefs[i]])
			case subsetPlan != nil:
				admitted = subsetPlan.admits(result.Findings[findingRefs[i]])
			case zeroValueEmptyListPlan != nil:
				admitted = zeroValueEmptyListPlan.admits(result.Findings[findingRefs[i]])
			case tiePlan != nil:
				admitted = tiePlan.admits(result.Findings[findingRefs[i]])
			case heatmapCellPlan != nil:
				admitted = heatmapCellPlan.admits(result.Findings[findingRefs[i]])
			case dupLenPlan != nil:
				admitted = dupLenPlan.admits(result.Findings[findingRefs[i]])
			}
			if admitted {
				covered[i] = true
				hit = true
				if sankeyFanoutPlan != nil || hotspotBoundaryPlan != nil {
					repoMultiplierCovered[i] = true
				}
			}
		}
		verdicts[d] = perDefect{hit: hit, shaped: shaped, touched: touched}
	}
	for d, defect := range defects {
		if defect.TeamCoverageIdentityShape != nil || defect.LimitDisplacementShape != nil || defect.HotspotListBoundaryShape != nil || defect.TeamRepoSubsetShape != nil {
			continue
		}
		evaluateDefect(d, defect)
	}
	for d, defect := range defects {
		if defect.TeamCoverageIdentityShape == nil && defect.LimitDisplacementShape == nil && defect.HotspotListBoundaryShape == nil && defect.TeamRepoSubsetShape == nil {
			continue
		}
		evaluateDefect(d, defect)
	}

	var matched, stale, idle, liveUnexplainedDefects []string
	for d, defect := range defects {
		v := verdicts[d]
		// liveUnexplained is decided against the FINAL `covered`, now that
		// every defect (including a sibling sharing this one's Paths) has
		// run: it means this shaped defect's own cited Paths carried at
		// least one leaf-level finding this run (the mechanism was LIVE,
		// not absent) that NOBODY -- this defect's own shape or any
		// sibling's -- ended up covering.
		liveUnexplained := false
		if v.shaped && !v.hit {
			for _, i := range v.touched {
				if !covered[i] {
					liveUnexplained = true
					break
				}
			}
		}
		switch {
		case v.hit:
			matched = append(matched, defect.Ticket)
		case liveUnexplained:
			// Refused ALWAYS, Intermittent or not: the Intermittent
			// exemption below is for a citation whose Paths carried
			// nothing at all this run, which is a different, honest state
			// (the source table happened to be merged) from "carried
			// something and nothing explained it" -- a declaration that
			// explains nothing while live is either stale, wrong, or
			// masking an unrelated difference by accident, and none of
			// those may pass quietly.
			liveUnexplainedDefects = append(liveUnexplainedDefects, defect.Ticket)
		case defect.Intermittent && validateBaselineDefects([]BaselineDefect{defect}) == nil:
			// Absent while the baseline's source table is merged: expected,
			// so recorded as idle and never as stale.
			idle = append(idle, defect.Ticket)
		default:
			stale = append(stale, defect.Ticket)
		}
	}
	sort.Strings(matched)
	sort.Strings(stale)
	sort.Strings(idle)
	sort.Strings(liveUnexplainedDefects)

	outside := 0
	coveredByShape, outsideByShape := map[string]int{}, map[string]int{}
	for i, isCovered := range covered {
		if !isCovered {
			outside++
			outsideByShape[shapeLabel(shapes[i])]++
			continue
		}
		coveredByShape[shapeLabel(shapes[i])]++
	}

	result.CoveredByShape = coveredByShape
	result.OutsideByShape = outsideByShape
	result.BaselineDefectsMatched = matched
	result.StaleBaselineDefects = stale
	result.LiveBaselineDefectsUnexplained = liveUnexplainedDefects
	result.IdleIntermittentBaselineDefects = idle
	result.DifferencesOutsideBaselineDefect = outside
}

// FormatShapeCounts renders per-shape counts as `covered[null=7 value=384]
// outside[length=1]`, keys sorted, one form for every surface that prints
// them (go-api-prove's line, the tests; `enable` renders the same JSON).
func FormatShapeCounts(covered, outside map[string]int) string {
	render := func(counts map[string]int) string {
		keys := make([]string, 0, len(counts))
		for key := range counts {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, fmt.Sprintf("%s=%d", key, counts[key]))
		}
		return strings.Join(parts, " ")
	}
	return "covered[" + render(covered) + "] outside[" + render(outside) + "]"
}

// shapeLabel names a finding's shape for the per-shape counts; a finding
// the comparator did not classify is counted, never dropped.
func shapeLabel(shape string) string {
	if shape == "" {
		return "unclassified"
	}
	return shape
}

// citedSegments splits a cited path ("data.hotspots.rows") into the keys
// under the response's `data` value. A path not under `data` has no
// segments there, so nothing under it is counted. The root itself
// ("data", naming the whole payload) is the empty segment list, not "not
// under data" -- conflating the two is F1 (CHAOS-5484 opus-r9): the empty
// list is what tells nonNullLeaves to walk everything under the value, and
// nil is what tells it there is nothing to walk, so a bare "data" citation
// silently disabled the empty-result addendum for its entire payload.
func citedSegments(cited string) []string {
	if cited == "data" {
		return []string{}
	}
	rest, ok := strings.CutPrefix(cited, "data.")
	if !ok {
		return nil
	}
	return strings.Split(rest, ".")
}

// nonNullLeaves counts the non-null scalar leaves at and under an
// index-free path: keys are followed, every list element is visited.
func nonNullLeaves(value any, segments []string) int {
	if segments == nil {
		return 0
	}
	switch typed := value.(type) {
	case map[string]any:
		if len(segments) == 0 {
			total := 0
			for _, child := range typed {
				total += nonNullLeaves(child, segments)
			}
			return total
		}
		child, present := typed[segments[0]]
		if !present {
			return 0
		}
		return nonNullLeaves(child, segments[1:])
	case []any:
		total := 0
		for _, element := range typed {
			total += nonNullLeaves(element, segments)
		}
		return total
	case nil:
		return 0
	default:
		if len(segments) == 0 {
			return 1
		}
		return 0
	}
}

// defectCovers reports whether a difference path lies under a declared
// path: itself or anything beneath it, so a subtree root can be named once
// instead of every leaf under it. Whether the difference is then COVERED
// also depends on its shape -- see classifyBaselineDefects.
func defectCovers(defect BaselineDefect, path string) bool {
	for _, cited := range defect.Paths {
		if path == cited || strings.HasPrefix(path, cited+".") {
			return true
		}
	}
	return false
}

// --- Error comparison (parity rule 1) ---------------------------------

// errorIdentity is parity rule 1's error identity: path plus
// extensions.code. The MESSAGE is deliberately not part of it -- two
// planes phrasing the same failure differently is drift to record, not a
// different error.
func errorIdentity(err map[string]any) [2]string {
	segments := []string{}
	if raw, ok := err["path"]; ok {
		if list, ok := raw.([]any); ok {
			for _, seg := range list {
				segments = append(segments, fmt.Sprintf("%v", seg))
			}
		}
	}
	code := ""
	if raw, ok := err["extensions"]; ok {
		if ext, ok := raw.(map[string]any); ok {
			if value, ok := ext["code"]; ok {
				code = fmt.Sprintf("%v", value)
			}
		}
	}
	return [2]string{strings.Join(segments, "."), code}
}

func compareErrors(baseline, candidate []map[string]any, pathPrefix string) []Finding {
	byIdentity := func(errs []map[string]any) map[[2]string]map[string]any {
		out := make(map[[2]string]map[string]any, len(errs))
		for _, err := range errs {
			out[errorIdentity(err)] = err
		}
		return out
	}
	baselineByIdentity := byIdentity(baseline)
	candidateByIdentity := byIdentity(candidate)

	findings := []Finding{}
	identityPath := func(id [2]string) string {
		return fmt.Sprintf("%s[%q,%q]", pathPrefix, id[0], id[1])
	}

	var missing, extra, shared [][2]string
	for id := range baselineByIdentity {
		if _, ok := candidateByIdentity[id]; ok {
			shared = append(shared, id)
		} else {
			missing = append(missing, id)
		}
	}
	for id := range candidateByIdentity {
		if _, ok := baselineByIdentity[id]; !ok {
			extra = append(extra, id)
		}
	}
	sortIdentities(missing)
	sortIdentities(extra)
	sortIdentities(shared)

	for _, id := range missing {
		findings = append(findings, Finding{
			Kind:   FindingMismatch,
			Path:   identityPath(id),
			Detail: "error present in baseline, missing from candidate",
			Shape:  ShapePresence,
		})
	}
	for _, id := range extra {
		findings = append(findings, Finding{
			Kind:   FindingMismatch,
			Path:   identityPath(id),
			Detail: "error present in candidate, missing from baseline",
			Shape:  ShapePresence,
		})
	}
	for _, id := range shared {
		baseMessage := fmt.Sprintf("%v", baselineByIdentity[id]["message"])
		candMessage := fmt.Sprintf("%v", candidateByIdentity[id]["message"])
		if baseMessage != candMessage {
			findings = append(findings, Finding{
				Kind:   FindingErrorMessageDrift,
				Path:   identityPath(id) + ".message",
				Detail: fmt.Sprintf("%q != %q", baseMessage, candMessage),
			})
		}
	}
	return findings
}

func sortIdentities(ids [][2]string) {
	sort.Slice(ids, func(i, j int) bool {
		if ids[i][0] != ids[j][0] {
			return ids[i][0] < ids[j][0]
		}
		return ids[i][1] < ids[j][1]
	})
}

// --- Structural JSON comparison (parity rules 2, 3, 5) -----------------

// listIndexSuffix matches a bracketed list-index suffix a comparator path
// segment can carry ("[0]", "[42]") -- ALWAYS a plain ordinal, never a
// raw data value (compareListByKey's pairing key is deliberately kept
// OUT of the path string entirely -- see its own doc comment -- so a key
// value containing "." or "]" can never reach here and corrupt this
// stripping or the "." split below it). Stripped from the WHOLE path
// BEFORE splitting on "." (not per already-split segment): harmless for
// today's digit-only content, but keeps this function correct even if a
// future bracket form ever carries a "." inside it again.
var listIndexSuffix = regexp.MustCompile(`\[[^\[\]]*\]`)

// tieredPath normalises a concrete comparator path ("$.data.edges[0].score")
// to the dotted, index-free form callers declare Tier-B and volatile
// fields with ("data.edges.score").
func tieredPath(path string) string {
	path = listIndexSuffix.ReplaceAllString(path, "")
	segments := strings.Split(path, ".")
	if len(segments) > 0 && segments[0] == "$" {
		segments = segments[1:]
	}
	return strings.Join(segments, ".")
}

// excluded reports whether path is a declared volatile field, recording
// the hit so an exclusion that never matches can be reported as unused.
func excluded(path string, opts Options, track *tracker) bool {
	key := tieredPath(path)
	if _, ok := opts.VolatileFields[key]; !ok {
		return false
	}
	track.volatile[key] = true
	return true
}

func compareJSON(baseline, candidate any, path string, opts Options, envelopeKeys map[string]bool, track *tracker) []Finding {
	baselineMap, baselineIsMap := baseline.(map[string]any)
	candidateMap, candidateIsMap := candidate.(map[string]any)
	if baselineIsMap && candidateIsMap {
		return compareDict(baselineMap, candidateMap, path, opts, envelopeKeys, track)
	}

	baselineList, baselineIsList := baseline.([]any)
	candidateList, candidateIsList := candidate.([]any)
	if baselineIsList && candidateIsList {
		return compareList(baselineList, candidateList, path, opts, track)
	}

	// Different JSON kinds. A container on either side is STRUCTURAL and
	// no citation covers it; two leaves (scalar or null) are a leaf
	// difference a citation may cover; a non-finite number is never
	// covered. Detail text
	// unchanged.
	if baselineKind, candidateKind := jsonKind(baseline), jsonKind(candidate); baselineKind != candidateKind {
		shape := ShapeScalarType
		switch {
		case isContainerKind(baselineKind) || isContainerKind(candidateKind):
			shape = ShapeStructure
		case nonFinite(baseline) || nonFinite(candidate):
			shape = ShapeNonFinite
		case baselineKind == "null" || candidateKind == "null":
			shape = ShapeNull
		}
		return []Finding{{Kind: FindingMismatch, Path: path, Detail: fmt.Sprintf("%v != %v", baseline, candidate), Shape: shape}}
	}

	baselineBool, baselineIsBool := baseline.(bool)
	candidateBool, candidateIsBool := candidate.(bool)
	if baselineIsBool || candidateIsBool {
		if !baselineIsBool || !candidateIsBool || baselineBool != candidateBool {
			return []Finding{{Kind: FindingMismatch, Path: path, Detail: fmt.Sprintf("%v != %v", baseline, candidate), Shape: ShapeValue}}
		}
		return nil
	}

	baselineNumber, baselineIsNumber := asFloat(baseline)
	candidateNumber, candidateIsNumber := asFloat(candidate)
	if baselineIsNumber && candidateIsNumber {
		// DELIBERATE DIVERGENCE from the Python comparator, in the
		// STRICTER direction. _compare_number does `float(baseline) !=
		// float(candidate)`, so two DIFFERENT int64s beyond float64's
		// 2^53 precision (9007199254740992 vs 9007199254740993) collapse
		// to the same float and compare EQUAL -- a Tier-A "exact"
		// comparison that is not exact. A proof instrument that cannot
		// distinguish two different integers cannot certify parity over
		// them, so integers are compared as integers here. Floats, and
		// any mixed int/float pair, still go through the float path, so
		// a formatting-only difference (1 vs 1.0) does not become a false
		// mismatch. Filed against the Python side as a divergence to
		// re-mirror, not silently forked.
		//
		// Tier-B fields are excluded from this path: a field declared as
		// tolerance-compared must stay tolerance-compared even when both
		// sides happen to be whole numbers, or the declaration would mean
		// different things depending on the data.
		//
		// tolerant is decided ONCE here, by numericLeafTolerant (which
		// also performs the undeclared-leaf bookkeeping): this
		// is the only place every numeric leaf is guaranteed to pass
		// through, since the int64 fast path below returns without ever
		// reaching compareNumber when both sides parse as clean integers.
		tolerant := numericLeafTolerant(tieredPath(path), opts, track)
		if !tolerant {
			if baselineInt, ok := asInt64(baseline); ok {
				if candidateInt, ok := asInt64(candidate); ok {
					if baselineInt != candidateInt {
						return []Finding{{
							Kind:   FindingMismatch,
							Path:   path,
							Detail: fmt.Sprintf("%d != %d (Tier A, exact integer)", baselineInt, candidateInt),
							Shape:  ShapeValue,
						}}
					}
					return nil
				}
			}
		}
		return compareNumber(baselineNumber, candidateNumber, path, opts, track, tolerant)
	}

	if !sameScalar(baseline, candidate) {
		return []Finding{{Kind: FindingMismatch, Path: path, Detail: fmt.Sprintf("%v != %v", baseline, candidate), Shape: ShapeValue}}
	}
	return nil
}

// asFloat accepts every JSON number shape this package can be handed:
// json.Number (the decoder configured by DecodeSnapshot, which preserves
// the literal so an integer is never silently widened before comparison),
// and float64/int for values built in Go by a test or caller.
//
// A json.Number that does not parse as a float is NOT a number: it is
// returned as not-a-number so the scalar branch reports a mismatch rather
// than this function inventing a zero.
func asFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := strconv.ParseFloat(typed.String(), 64)
		if err != nil {
			// An OUT-OF-RANGE literal (1e400) is still a number, and
			// ParseFloat has already returned the correct ±Inf for it --
			// returning "not a number" here would drop it into the scalar
			// branch, where two identical overflowing literals compare
			// EQUAL and parity rule 3's "non-finite always mismatches"
			// would silently never fire. Only a SYNTAX error means this
			// value is not a number.
			if errors.Is(err, strconv.ErrRange) {
				return parsed, true
			}
			return 0, false
		}
		return parsed, true
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	default:
		return 0, false
	}
}

// asInt64 reports whether value is an INTEGER JSON number, and its exact
// value. A float literal ("1.0", "1e3") is deliberately not an integer
// here: json.Number.Int64 rejects it, which is what keeps a
// formatting-only difference on the float path instead of this exact one.
func asInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		return parsed, true
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	default:
		return 0, false
	}
}

// sameScalar compares two non-map, non-list, non-bool, non-number values.
// nil equals only nil (parity rule 2: null is a value, never a wildcard).
func sameScalar(baseline, candidate any) bool {
	if baseline == nil || candidate == nil {
		return baseline == nil && candidate == nil
	}
	baselineString, baselineIsString := baseline.(string)
	candidateString, candidateIsString := candidate.(string)
	if baselineIsString && candidateIsString {
		return baselineString == candidateString
	}
	return fmt.Sprintf("%T:%v", baseline, baseline) == fmt.Sprintf("%T:%v", candidate, candidate)
}

// matchRepeatingSegments reports whether actual's dot-segments satisfy
// declared's own segments under the ONE repeat-marker form
// (FloatTierB's own doc comment): a declared segment ending in "+"
// consumes one or MORE consecutive actual segments equal to its own
// base name (zero is never accepted -- a depth-0 leaf needs its own,
// separate, plain entry); every other declared segment must equal the
// actual segment at that exact position; the two segment lists must
// consume each other completely start to finish, with nothing left
// over on either side.
func matchRepeatingSegments(declared, actual []string) bool {
	ai := 0
	for _, segment := range declared {
		base, isRepeat := strings.CutSuffix(segment, "+")
		if isRepeat {
			start := ai
			for ai < len(actual) && actual[ai] == base {
				ai++
			}
			if ai == start {
				return false
			}
			continue
		}
		if ai >= len(actual) || actual[ai] != segment {
			return false
		}
		ai++
	}
	return ai == len(actual)
}

// matchRepeatDeclaration scans declared's own keys for one containing a
// repeat-marker segment that matches path under matchRepeatingSegments,
// returning that key. Only ever consulted AFTER an exact-key lookup on
// the same table already missed -- the repeat form is a fallback for
// the one recursive, unbounded-depth tree shape a literal dotted path
// cannot enumerate (code_hotspots' own real-file-path tree,
// flame/aggregated), never a replacement for the plain map lookup every
// other leaf still uses, and never itself consulted when path contains
// no "." at all (a repeat-marked declaration always has a structural
// segment plus the leaf's own name that follows it, so a bare,
// dot-free path can never match one -- validateRepeatMarker's own
// last-segment rule guarantees every declared key has this shape).
func matchRepeatDeclaration(path string, declared map[string]string) (string, bool) {
	if len(declared) == 0 || !strings.Contains(path, ".") {
		return "", false
	}
	actual := strings.Split(path, ".")
	for key := range declared {
		if !strings.Contains(key, "+") {
			continue
		}
		if matchRepeatingSegments(strings.Split(key, "."), actual) {
			return key, true
		}
	}
	return "", false
}

// classifyDeclaredLeaf looks up path across FloatTierB/FloatExactLeaves/
// IntegerLeaves, first by EXACT key match and then, only on a miss, by
// the ONE repeat-segment form (matchRepeatDeclaration) -- the fallback a
// recursive tree needs, playing the same role for THAT shape that
// numericLeafTolerant's own ancestor climb plays for a
// dynamically-keyed map. ok reports whether path is declared at all;
// tolerant reports its comparison mode when it is; matchedKey is the
// declared key that actually matched -- path itself for an exact hit,
// the repeat-marked pattern for a repeat hit -- the string
// Result.UnusedTierB tracks usage against.
func classifyDeclaredLeaf(path string, opts Options) (tolerant, ok bool, matchedKey string) {
	_, inTierB := opts.FloatTierB[path]
	_, inExact := opts.FloatExactLeaves[path]
	_, inInt := opts.IntegerLeaves[path]
	if inTierB || inExact || inInt {
		return inTierB && !inExact, true, path
	}
	if key, matched := matchRepeatDeclaration(path, opts.FloatExactLeaves); matched {
		return false, true, key
	}
	if key, matched := matchRepeatDeclaration(path, opts.FloatTierB); matched {
		return true, true, key
	}
	if key, matched := matchRepeatDeclaration(path, opts.IntegerLeaves); matched {
		return false, true, key
	}
	return false, false, ""
}

// parentPath returns key's immediate dotted parent ("data.a.b" ->
// "data.a", true), or ("", false) when key carries no "." to split on.
func parentPath(key string) (string, bool) {
	idx := strings.LastIndex(key, ".")
	if idx < 0 {
		return "", false
	}
	return key[:idx], true
}

// numericLeafTolerant reports whether the numeric leaf at the given
// tiered path compares Tier B (tolerant) or Tier A (exact), and performs
// the undeclared-leaf and Tier-B-usage bookkeeping as a side
// effect. This is the ONE place FloatTierB/FloatExactLeaves/IntegerLeaves
// are read; compareValue's integer fast path and compareNumber both go
// through it rather than each re-deriving the same lookup, which would
// let the two disagree on a leaf the fast path handles without ever
// calling compareNumber.
//
// Tried in order: the leaf's OWN exact path (which classifyDeclaredLeaf
// itself also tries the repeat-segment form for, see its own doc
// comment -- the fallback a RECURSIVE tree of unbounded depth needs),
// then (only if that finds nothing) each ANCESTOR path in turn, climbing
// one "." segment at a time until a declared one is found or none
// remain -- the fallback a dynamically-keyed JSON object needs. A Go
// map[string]<number> with
// data-dependent keys (theme_distribution, keyed by an org's own theme
// names) produces a leaf path compareDict builds from the LITERAL key
// ("data.theme_distribution.engineering"), which no per-leaf declaration
// can name in advance -- unlike a LIST, where tieredPath already strips
// the "[N]" index so one declaration covers every element. Declaring the
// map's own path ("data.theme_distribution") once, with no child
// segment, covers every value beneath it the same way.
//
// The climb cannot stop at one level: a map key can itself legitimately
// contain a "." (subcategory_distribution's own keys are compound,
// "<theme>.<subcategory>" -- response.go's own row.Subcategory filter
// requires strings.Contains(row.Subcategory, ".")), and once baseline
// and candidate key and path are both flattened into one dotted string
// there is no way to tell a "." that separates JSON structure from a "."
// inside a literal key -- the same ambiguity OrderInsensitiveList's own
// pairing key deliberately keeps OUT of the path string entirely (see
// listIndexSuffix's own doc comment). Climbing until a declared ancestor
// is found is what actually matches "declare the map once" for every key
// shape this corpus can produce, a one-dot subcategory or a plain,
// dot-free theme alike, rather than silently undershooting whichever one
// has more dots than assumed. track.tierB is marked at whichever path
// actually matched (the leaf's own, or an ancestor) -- the same path a
// corpus author wrote, so UnusedTierB reads against what was declared,
// never a data-dependent child no declaration could have named.
//
// Tolerant iff the matched declaration is in FloatTierB and NOT also in
// FloatExactLeaves -- FloatExactLeaves opts a float-declared leaf back
// to exact without reclassifying it as integer (see Options.
// FloatExactLeaves' own doc comment). When NumericLeavesDeclared is
// unset, FloatExactLeaves/IntegerLeaves and the parent-path fallback are
// never consulted, no bookkeeping happens, and FloatTierB is read as a
// plain exact-match opt-in -- a route with no marker keeps today's
// behaviour, unchanged.
func numericLeafTolerant(key string, opts Options, track *tracker) bool {
	if !opts.NumericLeavesDeclared {
		_, inTierB := opts.FloatTierB[key]
		if inTierB {
			track.tierB[key] = true
		}
		return inTierB
	}
	tolerant, ok, matched := classifyDeclaredLeaf(key, opts)
	if !ok {
		matched = key
	}
	// Climb ancestors -- NOT stopping at one level -- because a
	// dynamically-keyed map's own key can itself legitimately contain a
	// "." (subcategory_distribution's keys are compound,
	// "<theme>.<subcategory>" -- response.go's own row.Subcategory
	// filter requires strings.Contains(row.Subcategory, ".")), and this
	// function has no way to tell a "." that separates JSON structure
	// from a "." inside a literal key once both are flattened into one
	// dotted string -- the same ambiguity OrderInsensitiveList's own
	// pairing key deliberately keeps OUT of the path string entirely
	// (see listIndexSuffix's own doc comment). One climb only handled
	// theme_distribution (a plain, dot-free key); it silently
	// undershot subcategory_distribution, whose compound keys need TWO
	// climbs for a one-dot subcategory and more for further-nested
	// taxonomy. Climbing until a declared ancestor is found, or none
	// remain, is what actually matches "declare the map once" for every
	// key shape this corpus can produce, not only the simplest one.
	for candidate, hasParent := key, true; !ok && hasParent; {
		candidate, hasParent = parentPath(candidate)
		if !hasParent {
			break
		}
		if candidateTolerant, candidateOK, candidateMatched := classifyDeclaredLeaf(candidate, opts); candidateOK {
			tolerant, ok, matched = candidateTolerant, true, candidateMatched
		}
	}
	if !ok {
		track.undeclaredNumeric[key] = true
		return false
	}
	if tolerant {
		track.tierB[matched] = true
	}
	return tolerant
}

func compareNumber(baseline, candidate float64, path string, opts Options, track *tracker, tolerant bool) []Finding {
	if math.IsNaN(baseline) || math.IsNaN(candidate) || math.IsInf(baseline, 0) || math.IsInf(candidate, 0) {
		// Parity rule 3: NaN/Infinity ALWAYS mismatches, never
		// tolerance-compared -- including when both sides agree. inf==inf
		// is true in IEEE 754, but a non-finite value reaching a client at
		// all is itself the defect this rule exists to catch.
		return []Finding{{
			Kind:   FindingMismatch,
			Path:   path,
			Detail: fmt.Sprintf("non-finite value: %v vs %v (NaN/Infinity always mismatches)", baseline, candidate),
			Shape:  ShapeNonFinite,
		}}
	}

	if !tolerant {
		if baseline != candidate {
			return []Finding{{
				Kind:   FindingMismatch,
				Path:   path,
				Detail: fmt.Sprintf("%v != %v (Tier A, exact)", baseline, candidate),
				Shape:  ShapeValue,
			}}
		}
		return nil
	}
	// track.tierB is already marked -- by numericLeafTolerant, the one
	// place that decides tolerant, at whichever path (the leaf's own, or
	// a dynamically-keyed map's parent) the declaration actually matched.
	// Marked as soon as the field is COMPARED, not only when its
	// tolerance is exercised: the declaration's claim is "this field
	// exists and is a merged aggregate", and a field that compares equal
	// every run still satisfies it.

	tolerance := math.Max(floatTolerance, floatTolerance*math.Max(math.Abs(baseline), math.Abs(candidate)))
	if math.Abs(baseline-candidate) > tolerance {
		return []Finding{{
			Kind:   FindingMismatch,
			Path:   path,
			Detail: fmt.Sprintf("%v != %v (Tier B, tolerance %v)", baseline, candidate, tolerance),
			Shape:  ShapeValue,
		}}
	}
	return nil
}

// compareDict applies envelopeKeys to THIS dict's own key-presence check
// only, then drops it: every nested dict, at any depth, compares with no
// envelope exclusions. A field named "extensions" nested inside `data` is
// ordinary business data, not a transport-envelope key.
func compareDict(baseline, candidate map[string]any, path string, opts Options, envelopeKeys map[string]bool, track *tracker) []Finding {
	keys := make([]string, 0, len(baseline)+len(candidate))
	seen := make(map[string]bool, len(baseline)+len(candidate))
	for key := range baseline {
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	for key := range candidate {
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	var findings []Finding
	for _, key := range keys {
		childPath := path + "." + key
		if excluded(childPath, opts, track) {
			continue
		}
		_, inBaseline := baseline[key]
		_, inCandidate := candidate[key]
		if inBaseline != inCandidate {
			if envelopeKeys[key] {
				continue
			}
			detail := "present in baseline, absent in candidate"
			if !inBaseline {
				detail = "present in candidate, absent in baseline"
			}
			findings = append(findings, Finding{Kind: FindingMismatch, Path: childPath, Detail: detail, Shape: ShapePresence})
			continue
		}
		findings = append(findings, compareJSON(baseline[key], candidate[key], childPath, opts, nil, track)...)
	}
	return findings
}

// compareList compares positionally -- parity rule 5's default -- unless
// this list's path is declared as an OrderInsensitiveList, in which case
// it defers to compareListByKey entirely. A length difference is reported
// alone: element-wise findings past that point would be a cascade of
// noise about an offset, not independent defects.
func compareList(baseline, candidate []any, path string, opts Options, track *tracker) []Finding {
	if decl, ok := findOrderInsensitiveList(opts, tieredPath(path)); ok {
		track.orderInsensitive[decl.Path] = true
		return compareListByKey(baseline, candidate, path, decl, opts, track)
	}
	if len(baseline) != len(candidate) {
		return []Finding{{
			Kind:   FindingMismatch,
			Path:   path,
			Detail: fmt.Sprintf("length %d != %d", len(baseline), len(candidate)),
			Shape:  ShapeLength,
		}}
	}
	var findings []Finding
	for i := range baseline {
		childPath := fmt.Sprintf("%s[%d]", path, i)
		if excluded(childPath, opts, track) {
			continue
		}
		findings = append(findings, compareJSON(baseline[i], candidate[i], childPath, opts, nil, track)...)
	}
	return findings
}

// orderInsensitiveKey builds the pairing key for one element under decl:
// the declared KeyFields' values, joined so distinct field-value tuples
// cannot collide (0x1f, ASCII unit separator, cannot appear in a
// %v-formatted JSON scalar). ok is false when the element is not an
// object, or is missing ANY declared key field -- the caller REFUSES the
// whole comparison in that case rather than guessing a pairing, exactly
// as BaselineDefect's vacuity guard refuses a stale declaration.
func orderInsensitiveKey(element any, keyFields []string) (string, bool) {
	object, isObject := element.(map[string]any)
	if !isObject {
		return "", false
	}
	parts := make([]string, len(keyFields))
	for i, field := range keyFields {
		value, ok := object[field]
		if !ok {
			return "", false
		}
		parts[i] = fmt.Sprintf("%v", value)
	}
	return strings.Join(parts, "\x1f"), true
}

// compareListByKey is OrderInsensitiveList's comparator: elements are
// paired by decl.KeyFields instead of position, and every paired element
// is still compared field-by-field via compareJSON -- parity rules 1-4
// all still apply inside a pair. A key present on only one side is a
// genuine missing/extra-element finding, not a reordering, so it is
// reported per-key rather than collapsed into one length mismatch.
//
// If ANY element on EITHER side does not carry every declared key field,
// OR two elements on the SAME side share the same key, the declaration
// does not describe this data: the whole list is refused (tracked, never
// silently downgraded to a finding) rather than guessing a partial
// pairing that could hide a real divergence behind a comparison nobody
// actually asked for. CHAOS-5546 finding: a naive `byKey[key] =
// element` silently let a LATER duplicate overwrite an EARLIER one,
// which can make a materially different candidate compare as a clean
// match (construction: baseline [{id:a,v:1},{id:a,v:2}], candidate
// [{id:a,v:999},{id:a,v:2}] -- both collapse to key "a" -> {v:2} and
// compare equal, discarding the v:1/v:999 divergence entirely). A
// duplicate key means KeyFields does not uniquely identify elements in
// THIS data, which is exactly the same "declaration doesn't describe
// this data" failure the missing-key-field guard already covers.
func compareListByKey(baseline, candidate []any, path string, decl OrderInsensitiveList, opts Options, track *tracker) []Finding {
	index := func(elements []any, side string) (map[string]any, bool) {
		byKey := make(map[string]any, len(elements))
		for _, element := range elements {
			key, ok := orderInsensitiveKey(element, decl.KeyFields)
			if !ok {
				track.orderInsensitiveKeyMissing = append(track.orderInsensitiveKeyMissing, fmt.Sprintf(
					"order-insensitive list %q (ticket %s): a %s element at %s is missing one of the declared key field(s) %v",
					decl.Path, decl.Ticket, side, path, decl.KeyFields))
				return nil, false
			}
			if _, duplicate := byKey[key]; duplicate {
				track.orderInsensitiveKeyMissing = append(track.orderInsensitiveKeyMissing, fmt.Sprintf(
					"order-insensitive list %q (ticket %s): the %s side has two elements at %s sharing key %q -- KeyFields %v does not uniquely identify elements in this data",
					decl.Path, decl.Ticket, side, path, key, decl.KeyFields))
				return nil, false
			}
			byKey[key] = element
		}
		return byKey, true
	}

	baselineByKey, baselineOK := index(baseline, "baseline")
	candidateByKey, candidateOK := index(candidate, "candidate")
	if !baselineOK || !candidateOK {
		return nil
	}

	keys := make([]string, 0, len(baselineByKey)+len(candidateByKey))
	seen := make(map[string]bool, len(baselineByKey)+len(candidateByKey))
	for key := range baselineByKey {
		seen[key] = true
		keys = append(keys, key)
	}
	for key := range candidateByKey {
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	var findings []Finding
	for i, key := range keys {
		// elementPath uses an OPAQUE ORDINAL (the key's position in the
		// sorted, deduplicated key list), never the raw key value.
		// CHAOS-5546 finding: embedding the key literally
		// (`[key="..."]`) let a key value containing `]` defeat
		// tieredPath's bracket-stripping regex, which stops at the FIRST
		// `]` it finds -- an embedded `]` inside the key closes the
		// match early and leaves the real closing bracket unstripped,
		// corrupting the FloatTierB/VolatileFields lookup for every
		// field nested inside that element. A plain digit index matches
		// the SAME `[N]` shape ordinary positional list elements already
		// use, so it needs no special-casing in tieredPath at all. The
		// human-readable key still appears in every finding's Detail.
		elementPath := fmt.Sprintf("%s[%d]", path, i)
		baselineElement, inBaseline := baselineByKey[key]
		candidateElement, inCandidate := candidateByKey[key]
		if inBaseline != inCandidate {
			detail := fmt.Sprintf("key %q present in baseline, absent in candidate", key)
			if !inBaseline {
				detail = fmt.Sprintf("key %q present in candidate, absent in baseline", key)
			}
			findings = append(findings, Finding{Kind: FindingMismatch, Path: elementPath, Detail: detail, Shape: ShapePresence})
			continue
		}
		for _, finding := range compareJSON(baselineElement, candidateElement, elementPath, opts, nil, track) {
			finding.Detail = fmt.Sprintf("[key=%q] %s", key, finding.Detail)
			findings = append(findings, finding)
		}
	}
	return findings
}
