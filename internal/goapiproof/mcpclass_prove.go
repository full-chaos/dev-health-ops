package goapiproof

// CHAOS-7214: the per-root-field proof receipt of the MCP caller class
// (design r5 D.8 "Proof": "a proof per ROOT FIELD with a generated set of
// selection shapes").
//
// WHAT A CLASS RECEIPT IS. One go_api_proof_run row per root field, keyed
// (schema_digest, class document digest, "mcp:<root>", candidate build),
// stage deployed_executed, measurement_route proof. It is derived from the
// SAME measurements `dho goapi prove` makes -- the registered documents whose
// response root is that field, with the proof corpus's variables and parity
// declarations (operations.go), executed once on the Go build through the MCP
// class's proof route (POST /query/proof-mcp, a measurement-only switch over
// the same handler) and once on the Python edge, compared by the same
// admission and comparator. The documents ARE the generated shapes: every
// selection a registered operation makes under that root, with its real
// variables. Nothing here is a second comparator.
//
// THE RULE. A root's receipt is a `match` only when
//
//   - at least one shape was executed and admitted (a root with nothing
//     measured writes NOTHING: a measurement that did not happen is not a
//     result), and
//   - every executed shape matched, with the serving build bound per
//     response and no difference outside a declaration, and
//   - nothing failed unexplained.
//
// A shape the MCP class refuses BY POLICY (HTTP 400/403/422 from the
// listener's own gate while Python served it: a person selector, a depth or
// complexity limit, a ClickHouse budget) cannot be a served shape, so it is
// EXCLUDED, never silently: it is counted and named on the receipt. Every
// other non-executed shape (a 404 root_field_not_enabled, a 5xx, a transport
// failure, an admission refusal) is a failure and blocks the match. A cited
// baseline-defect mismatch is NOT admitted for the class: the class has no
// declared Python defects of its own.
//
// WHAT THE RECEIPT DOES NOT PROVE: the 8092 listener's header parsing and
// NetworkPolicy boundary (the proof route is on the internal listener); those
// are CHAOS-7215's acceptance probe. And it is not design D.8's "oracle O4":
// that oracle (expected values from a path that is not the Go resolver,
// against acr facts) is run on the acr side; this receipt is the two-plane
// comparison of the SAME document on the Go and Python planes.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

// MCPClassProvenance is the class receipt's own account of what it rests on.
type MCPClassProvenance struct {
	Root     string `json:"root"`
	Executed int    `json:"shapes_executed"`
	Matched  int    `json:"shapes_matched"`
	// Excluded names every shape left out and why, "operation[:variant]=reason".
	Excluded []string `json:"shapes_excluded,omitempty"`
	// Failed names every shape that blocked the match.
	Failed []string `json:"shapes_failed,omitempty"`
}

// MCPClassVerdict is one root's result of a class proof run.
type MCPClassVerdict struct {
	Root          string
	Operation     string
	Executed      int
	Matched       int
	Excluded      []string
	Failed        []string
	TerminalState string
	// Written is whether a receipt was built for the root (false: nothing was
	// measured).
	Written bool
}

// MCPClassSourceOperations maps each requested class operation ("mcp:<root>")
// to the registered QUERY operations whose response root is that field, sorted.
// A root with no source operation is an error: nothing could be measured for it.
func MCPClassSourceOperations(classOperations []string, documents map[string]string) (map[string][]string, error) {
	wanted := map[string]bool{}
	for _, operation := range classOperations {
		if !mcpclass.AllowedOperation(operation) {
			return nil, fmt.Errorf("goapiproof: %s is not an allowlisted MCP class operation", operation)
		}
		root, _ := mcpclass.Root(operation)
		wanted[root] = true
	}
	out := map[string][]string{}
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil || !wanted[spec.ResponseRoot] {
			continue
		}
		text, registered := documents[operation]
		if !registered {
			continue
		}
		if kind, kindErr := goapidigest.DocumentKind(text); kindErr != nil || kind != goapidigest.KindQuery {
			continue
		}
		out[mcpclass.Operation(spec.ResponseRoot)] = append(out[mcpclass.Operation(spec.ResponseRoot)], operation)
	}
	var missing []string
	for _, operation := range classOperations {
		if len(out[operation]) == 0 {
			missing = append(missing, operation)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("goapiproof: no registered query document has these MCP roots as its response root, so there is nothing to measure for them: %v", missing)
	}
	for operation := range out {
		sort.Strings(out[operation])
	}
	return out, nil
}

// policyRefusalStatuses are the listener's own refusals of a shape it will
// never serve: 400 (a limit or validation), 403 (person scope, org, a root or
// claim) and 422 (a ClickHouse read budget). 404 is deliberately absent: it is
// root_field_not_enabled, and a root that cannot be reached is a failure.
var policyRefusalStatuses = map[int]string{
	http.StatusBadRequest:          "listener_limit_or_validation",
	http.StatusForbidden:           "listener_policy",
	http.StatusUnprocessableEntity: "listener_read_budget",
}

// classifyMCPOutcome says what one measured shape is to its root's proof:
// "executed", an exclusion reason, or a failure.
func classifyMCPOutcome(o Outcome) (state, reason string) {
	switch {
	case o.Executed:
		return "executed", ""
	case o.KnownRefusal != nil:
		return "excluded", "known_refusal"
	case o.RefusalReason == RefusalNeedsInstanceID:
		return "excluded", "needs_instance_identifier"
	case o.RefusalReason == RefusalNonSuccessStatus && o.Candidate != nil && o.Baseline != nil:
		if why, ok := policyRefusalStatuses[o.Candidate.StatusCode]; ok &&
			o.Baseline.StatusCode >= http.StatusOK && o.Baseline.StatusCode < http.StatusMultipleChoices {
			return "excluded", why
		}
	}
	return "failed", o.RefusalReason
}

// MCPClassReceipts turns a finished run into one receipt per root. outcomes is
// what Run returned for exactly the source operations of rootSources; the run's
// sealed measurements are the only evidence read. Receipts are returned, not
// written. A root with nothing executed gets a verdict and NO receipt.
func (r *Runner) MCPClassReceipts(outcomes []Outcome, rootSources map[string][]string, observedAt time.Time) ([]Receipt, []MCPClassVerdict, error) {
	if len(outcomes) != len(r.sealed) {
		return nil, nil, errors.New("goapiproof: the outcomes are not the ones this run sealed")
	}
	rootOf := map[string]string{}
	for classOperation, operations := range rootSources {
		for _, operation := range operations {
			rootOf[operation] = classOperation
		}
	}
	type bucket struct {
		verdict MCPClassVerdict
		sealed  []sealedOutcome
		bound   bool
	}
	buckets := map[string]*bucket{}
	for classOperation := range rootSources {
		root, _ := mcpclass.Root(classOperation)
		buckets[classOperation] = &bucket{verdict: MCPClassVerdict{Root: root, Operation: classOperation}, bound: true}
	}
	for index, outcome := range outcomes {
		classOperation, ok := rootOf[outcome.Operation]
		if !ok {
			return nil, nil, fmt.Errorf("goapiproof: outcome for %s belongs to no requested MCP root", outcome.Operation)
		}
		b := buckets[classOperation]
		label := outcome.Operation
		if outcome.Variant != "" {
			label += ":" + outcome.Variant
		}
		sealed := r.sealed[index]
		state, reason := classifyMCPOutcome(outcome)
		switch state {
		case "executed":
			b.verdict.Executed++
			b.sealed = append(b.sealed, sealed)
			if sealed.terminalState == TerminalStateMatch && sealed.admitted && sealed.executed &&
				sealed.differencesOutsideBaselineDefect == 0 && sealed.edgeBinding == EdgeBuildPresent &&
				sealed.route == RouteProof {
				b.verdict.Matched++
			} else {
				b.verdict.Failed = append(b.verdict.Failed, label+"="+sealed.terminalState)
			}
			if sealed.edgeBinding != EdgeBuildPresent {
				b.bound = false
			}
		case "excluded":
			b.verdict.Excluded = append(b.verdict.Excluded, label+"="+reason)
		default:
			b.verdict.Failed = append(b.verdict.Failed, label+"="+reason)
		}
	}

	var receipts []Receipt
	var verdicts []MCPClassVerdict
	for _, classOperation := range sortedKeysOf(buckets) {
		b := buckets[classOperation]
		v := &b.verdict
		sort.Strings(v.Excluded)
		sort.Strings(v.Failed)
		switch {
		case v.Executed == 0:
			v.TerminalState = ""
		case len(v.Failed) == 0:
			// Every executed shape matched (a shape that did not is in Failed), none
			// failed, and every excluded one is named on the receipt.
			v.TerminalState = TerminalStateMatch
		case anyTerminal(b.sealed, TerminalStateMismatch):
			v.TerminalState = TerminalStateMismatch
		default:
			v.TerminalState = "proof_failed"
		}
		verdicts = append(verdicts, *v)
		if v.Executed == 0 {
			continue
		}
		first := b.sealed[0]
		shapes := make([]string, 0, len(b.sealed))
		outside := 0
		for _, s := range b.sealed {
			label := s.operation
			if s.variant != "" {
				label += ":" + s.variant
			}
			shapes = append(shapes, label)
			outside += s.differencesOutsideBaselineDefect
		}
		sort.Strings(shapes)
		identity, err := RequestIdentity(first.orgID, r.authFor(first.principal), map[string]any{"class": classOperation, "shapes": shapes})
		if err != nil {
			return nil, nil, err
		}
		binding := EdgeBuildPresent
		if !b.bound {
			binding = EdgeBuildAbsent
		}
		provenance, err := json.Marshal(ReceiptProvenance{
			Operator:         r.Config.ReviewEvidence,
			MeasurementRoute: RouteProof,
			EdgeBuildBinding: binding,
			MCPClass:         &MCPClassProvenance{Root: v.Root, Executed: v.Executed, Matched: v.Matched, Excluded: v.Excluded, Failed: v.Failed},
		})
		if err != nil {
			return nil, nil, fmt.Errorf("goapiproof: encode the class receipt's provenance: %w", err)
		}
		v.Written = true
		verdicts[len(verdicts)-1] = *v
		receipts = append(receipts, Receipt{
			SchemaDigest:                     first.schemaDigest,
			DocumentDigest:                   mcpclass.DocumentDigest(),
			SelectedOperation:                classOperation,
			CandidateBuild:                   first.candidateBuild,
			RequestIdentity:                  identity,
			Stage:                            Stage,
			TerminalState:                    v.TerminalState,
			OrgID:                            first.orgID,
			ReviewEvidence:                   string(provenance),
			RecordedBy:                       r.Config.RecordedBy,
			ObservedAt:                       observedAt,
			BaselineResponseRef:              first.baselineRef,
			CandidateResponseRef:             first.candidateRef,
			MeasurementRoute:                 RouteProof,
			BuildBinding:                     binding,
			DifferencesOutsideBaselineDefect: outside,
		})
	}
	return receipts, verdicts, nil
}

func anyTerminal(sealed []sealedOutcome, state string) bool {
	for _, s := range sealed {
		if s.terminalState == state {
			return true
		}
	}
	return false
}

func sortedKeysOf[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// FormatMCPClassVerdict is the one report line per root.
func FormatMCPClassVerdict(v MCPClassVerdict) string {
	state := v.TerminalState
	if state == "" {
		state = "NOT_MEASURED"
	}
	return fmt.Sprintf("%s state=%s executed=%d matched=%d excluded=%d failed=%d receipt=%t%s",
		v.Operation, state, v.Executed, v.Matched, len(v.Excluded), len(v.Failed), v.Written,
		classDetail(v))
}

func classDetail(v MCPClassVerdict) string {
	var parts []string
	if len(v.Excluded) > 0 {
		parts = append(parts, " excluded=["+strings.Join(v.Excluded, ",")+"]")
	}
	if len(v.Failed) > 0 {
		parts = append(parts, " failed=["+strings.Join(v.Failed, ",")+"]")
	}
	return strings.Join(parts, "")
}
