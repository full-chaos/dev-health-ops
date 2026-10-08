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
// listener's own gate, with a typed reason from the closed policy set, while the
// reference served it: a person selector, a size or cost limit, a ClickHouse budget)
// cannot be a served shape, so it is EXCLUDED, never silently: it is counted and
// named on the receipt with its reason. A refusal for any other reason (an identity
// or document-validity one) blocks the root. Every
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
	"context"
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
	Root string `json:"root"`
	// Reference says what the MCP pipeline's answers were compared with:
	// "python_edge" (the Python app's /graphql) or "go_document_route"
	// (query-api's own /graphql for the registered document, CHAOS-7442).
	Reference string `json:"reference"`
	Executed  int    `json:"shapes_executed"`
	Matched   int    `json:"shapes_matched"`
	// Stochastic names the shapes proven under the stochastic leaf class (CHAOS-5901): executed, never counted as
	// matched. A root with any is recorded as terminal mismatch with the class citation, never as a match.
	Stochastic []string `json:"shapes_stochastic,omitempty"`
	// Excluded names every shape left out and why, "operation[:variant]=reason".
	Excluded []string `json:"shapes_excluded,omitempty"`
	// BornInGo names the counted shapes whose operation has no Python counterpart and no document receipt (ledger born_in_go): each was measured
	// MCP pipeline against the Go document route, the only comparison that exists for it.
	BornInGo []string `json:"shapes_born_in_go,omitempty"`
	// Failed names every shape that blocked the match.
	Failed []string `json:"shapes_failed,omitempty"`
}

// MCPClassVerdict is one root's result of a class proof run.
type MCPClassVerdict struct {
	Root      string
	Operation string
	Executed  int
	Matched   int
	Excluded  []string
	BornInGo  []string
	Failed    []string
	// Stochastic names the shapes proven under the stochastic leaf class; Citations are the distinct class citations
	// the root receipt carries as its baseline_defect.
	Stochastic    []string
	Citations     []string
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

// policyRefusalStatuses are the HTTP statuses of the listener's own refusals of a shape it
// will never serve: 400 (a limit or validation), 403 (person scope, org, a root or claim)
// and 422 (a ClickHouse read budget). 404 is deliberately absent: it is
// root_field_not_enabled, and a root that cannot be reached is a failure.
var policyRefusalStatuses = map[int]bool{
	http.StatusBadRequest:          true,
	http.StatusForbidden:           true,
	http.StatusUnprocessableEntity: true,
}

// mcpPolicyRefusalReasons is the CLOSED set of listener refusal reasons that mean "this
// shape is outside what the MCP class serves, by policy": a person selector, an input or
// size limit, a cost cap, a read budget. A shape refused for one of them is excluded and
// named. Every other reason blocks the root, notably the identity ones (org_mismatch,
// invalid_org_argument, elevated_claim, no_carrier, authorization_header, invalid_org):
// the proof's org is the header org, so one of those firing is the identity mapping
// diverging from the document route, which is exactly what this proof exists to catch,
// and so are the document-validity reasons (the registered documents validate on both).
var mcpPolicyRefusalReasons = map[string]bool{
	"person_scope": true, "input_limit": true, "unclassified_input": true,
	"depth_limit": true, "alias_limit": true, "complexity_limit": true,
	"bytes_ceiling": true, "rows_ceiling": true, "time_ceiling": true,
}

// mcpRefusalReason reads the listener's typed refusal reason out of a response body
// (errors[0].extensions.reason), "" when the body is not one.
func mcpRefusalReason(body []byte) string {
	var parsed struct {
		Errors []struct {
			Extensions struct {
				Reason string `json:"reason"`
			} `json:"extensions"`
		} `json:"errors"`
	}
	if json.Unmarshal(body, &parsed) != nil || len(parsed.Errors) != 1 {
		return ""
	}
	return parsed.Errors[0].Extensions.Reason
}

// classifyMCPOutcome says what one measured shape is to its root's proof:
// "executed", an exclusion reason, or a failure.
func classifyMCPOutcome(o Outcome) (state, reason string) {
	switch {
	case o.Executed:
		return "executed", ""
	// A known refusal is a corpus tag for a Python-vs-Go divergence (e.g. CHAOS-6108). In doc-route mode both sides are Go, the tag does not
	// apply, and the shape's TRUE reason (below) is what the receipt must name.
	case o.KnownRefusal != nil && o.EdgeMode != EdgeModeDocRoute:
		return "excluded", "known_refusal"
	case o.RefusalReason == RefusalNeedsInstanceID:
		return "excluded", "needs_instance_identifier"
	case o.RefusalReason == RefusalNonSuccessStatus && o.Candidate != nil && o.Baseline != nil:
		if reason := mcpRefusalReason(o.Candidate.Body); policyRefusalStatuses[o.Candidate.StatusCode] && mcpPolicyRefusalReasons[reason] &&
			o.Baseline.StatusCode >= http.StatusOK && o.Baseline.StatusCode < http.StatusMultipleChoices {
			return "excluded", "listener_policy:" + reason
		}
	}
	return "failed", o.RefusalReason
}

// MCPClassReceipts turns a finished run into one receipt per root. outcomes is
// what Run returned for exactly the source operations of rootSources; the run's
// sealed measurements are the only evidence read. Receipts are returned, not
// written. A root with nothing executed gets a verdict and NO receipt.
//
// docBacked is, in the doc-route reference mode, the set of document operations that
// are themselves receipt-backed at this build (DocumentOperationsReceiptBacked): a
// shape counts only if its document operation is, and a shape whose document
// operation is not (a go_only_unproven operation, a named limit) is EXCLUDED and
// named, never counted. It is required in that mode and unused otherwise.
func (r *Runner) MCPClassReceipts(outcomes []Outcome, rootSources map[string][]string, docBacked map[string]bool, observedAt time.Time) ([]Receipt, []MCPClassVerdict, error) {
	if len(outcomes) != len(r.sealed) {
		return nil, nil, errors.New("goapiproof: the outcomes are not the ones this run sealed")
	}
	if r.Config.DocRouteReference && docBacked == nil {
		return nil, nil, errors.New("goapiproof: the doc-route reference mode needs the set of receipt-backed document operations (docBacked) to judge a shape")
	}
	reference := "python_edge"
	if r.Config.DocRouteReference {
		reference = "go_document_route"
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
		// citations are the distinct stochastic-leaf-class citations of the root's stochastic shapes.
		citations []string
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
		// An unbacked document operation's shape is excluded only when it MATCHED or is proven under the stochastic leaf class (both would count): a
		// divergence between the MCP pipeline and the document route blocks the root
		// whether or not the document operation itself is receipt-backed.
		bornGo := r.Config.DocRouteReference && !docBacked[outcome.Operation] && r.GoServed.BornInGo(outcome.Operation)
		if state == "executed" && r.Config.DocRouteReference && !docBacked[outcome.Operation] && !bornGo && (sealedMatches(sealed) || sealedStochasticCitation(sealed) != "") {
			state, reason = "excluded", "doc_operation_not_receipt_backed"
		}
		// A shape refused for measuring nothing (both answers empty: the org holds no data
		// for it) says nothing either way, so for an operation that is not receipt-backed it
		// is excluded too; for a backed one it stays a failure (a data gap must be read).
		if state == "failed" && r.Config.DocRouteReference && !docBacked[outcome.Operation] && outcome.RefusalReason == RefusalVacuousEmptyLegs {
			state, reason = "excluded", "doc_operation_not_receipt_backed"
		}
		switch state {
		case "executed":
			b.verdict.Executed++
			b.sealed = append(b.sealed, sealed)
			if bornGo {
				b.verdict.BornInGo = append(b.verdict.BornInGo, label)
			}
			if sealedMatches(sealed) {
				b.verdict.Matched++
			} else if citation := sealedStochasticCitation(sealed); citation != "" {
				b.verdict.Stochastic = append(b.verdict.Stochastic, label)
				b.citations = appendDistinct(b.citations, citation)
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
		case len(v.Failed) == 0 && len(v.Stochastic) == 0:
			// Every executed shape matched (a shape that did not is in Failed), none
			// failed, and every excluded one is named on the receipt.
			v.TerminalState = TerminalStateMatch
		case len(v.Failed) == 0:
			// Every executed shape matched or is proven under the stochastic leaf class: by that
			// class's own contract the root is NEVER a match. It is recorded as the cited mismatch
			// `enable` admits (terminal mismatch, nothing outside the citation, a named citation).
			v.TerminalState = TerminalStateMismatch
			v.Citations = append([]string(nil), b.citations...)
			sort.Strings(v.Citations)
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
			EdgeMode:         r.edgeMode(),
			MCPClass:         &MCPClassProvenance{Root: v.Root, Reference: reference, Executed: v.Executed, Matched: v.Matched, Stochastic: v.Stochastic, Excluded: v.Excluded, BornInGo: v.BornInGo, Failed: v.Failed},
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
			BaselineDefects:                  v.Citations,
		})
	}
	return receipts, verdicts, nil
}

// sealedMatches is the one definition of "this measurement is a match for the class
// proof": admitted and executed, terminal match, nothing outside a declaration, the
// serving build bound per response, measured through the proof route.
func sealedMatches(s sealedOutcome) bool {
	return s.terminalState == TerminalStateMatch && s.admitted && s.executed &&
		s.differencesOutsideBaselineDefect == 0 && s.edgeBinding == EdgeBuildPresent && s.route == RouteProof
}

// sealedStochasticCitation returns the stochastic-leaf-class citation of a measurement that is PROVEN under
// that class, or "" if it is not: terminal mismatch, nothing outside the citation, admitted and executed, the
// serving build bound, measured through the proof route, the comparator's own verdict is
// ProvenUnderStochasticLeafClass, and a non-blank class citation is on the sealed outcome. Every clause is
// needed: a stochastic root counts only if its ONLY differences are the declared drawn values.
func sealedStochasticCitation(s sealedOutcome) string {
	if s.terminalState != TerminalStateMismatch || !s.admitted || !s.executed || s.differencesOutsideBaselineDefect != 0 ||
		s.edgeBinding != EdgeBuildPresent || s.route != RouteProof || s.provenUnder != ProvenUnderStochasticLeafClass {
		return ""
	}
	for _, citation := range s.baselineDefects {
		if strings.HasPrefix(citation, StochasticLeafCitationPrefix) && strings.TrimSpace(strings.TrimPrefix(citation, StochasticLeafCitationPrefix)) != "" {
			return citation
		}
	}
	return ""
}

func appendDistinct(list []string, value string) []string {
	for _, existing := range list {
		if existing == value {
			return list
		}
	}
	return append(list, value)
}

// edgeMode is the provenance edge_mode of a class receipt: empty for the Python
// reference (the historic default), doc_route for the Go document route.
func (r *Runner) edgeMode() string {
	if r.Config.DocRouteReference {
		return EdgeModeDocRoute
	}
	return ""
}

// DocumentOperationsReceiptBacked reports which document operations hold an
// admissible receipt (match, or the cited go_only arm) for exactly this schema
// digest and candidate build: the same reader `enable` uses for a query
// operation, so "receipt-backed" here means what it means there.
func DocumentOperationsReceiptBacked(ctx context.Context, db Querier, schemaDigest, candidateBuild string, documentDigests map[string]string) (map[string]bool, error) {
	kinds := make(map[string]string, len(documentDigests))
	for operation := range documentDigests {
		kinds[operation] = OperationKindQuery
	}
	found, err := OperationsWithEnablementProofByKind(ctx, db, schemaDigest, candidateBuild, TargetModeCanary, documentDigests, kinds)
	if err != nil {
		return nil, err
	}
	if found == nil {
		found = map[string]bool{}
	}
	return found, nil
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
	proven := ""
	if len(v.Stochastic) > 0 && v.TerminalState == TerminalStateMismatch {
		proven = " proven_under=" + ProvenUnderStochasticLeafClass
	}
	return fmt.Sprintf("%s state=%s%s executed=%d matched=%d excluded=%d failed=%d receipt=%t%s",
		v.Operation, state, proven, v.Executed, v.Matched, len(v.Excluded), len(v.Failed), v.Written,
		classDetail(v))
}

func classDetail(v MCPClassVerdict) string {
	var parts []string
	if len(v.Excluded) > 0 {
		parts = append(parts, " excluded=["+strings.Join(v.Excluded, ",")+"]")
	}
	if len(v.Stochastic) > 0 {
		parts = append(parts, " stochastic=["+strings.Join(v.Stochastic, ",")+"]")
	}
	if len(v.Failed) > 0 {
		parts = append(parts, " failed=["+strings.Join(v.Failed, ",")+"]")
	}
	return strings.Join(parts, "")
}
