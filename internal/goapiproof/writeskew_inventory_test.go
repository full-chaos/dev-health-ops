package goapiproof

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// writeSkewRefusalPlaces pins every refusal constant of the REST prover's
// packages to its place in the bracketed re-read's decision table
// (rows F0a..F6 of the first comparison, columns R1..R8 of the re-read),
// or states why it is not an axis. A new refusal constant fails
// TestEveryRefusalHasAPlaceInTheWriteSkewTable until it is placed here.
var writeSkewRefusalPlaces = map[string]string{
	// First-comparison admission (F0a) and the second read's own
	// admission (R1 transport, R2 admission): the same names, because B2
	// goes through the same leg client and RESTAdmit as B1.
	"RESTRefusalBaselineLegTimedOut":          "F0a / R1",
	"RESTRefusalBaselineLegTransportError":    "F0a / R1",
	"RESTRefusalBaselineLegCutByRunDeadline":  "F0a / R1 (the run ended during the read)",
	"RESTRefusalBaselineLegCutBySignal":       "F0a / R1 (the run ended during the read)",
	"RESTRefusalCandidateLegTimedOut":         "F0a (the re-read never touches the candidate plane)",
	"RESTRefusalCandidateLegTransportError":   "F0a (the re-read never touches the candidate plane)",
	"RESTRefusalCandidateLegCutByRunDeadline": "F0a (the re-read never touches the candidate plane)",
	"RESTRefusalCandidateLegCutBySignal":      "F0a (the re-read never touches the candidate plane)",
	"RESTRefusalUnexpectedStatus":             "F0a / R2",
	"RESTRefusalBodyNotJSON":                  "F0a / R2",
	"RESTRefusalTrailingBytes":                "F0a / R2",
	"RESTRefusalBaselineNotReferencePlane":    "F0a / R2 (B2's plane identity is checked like B1's)",
	"RESTRefusalServedUnderImpersonation":     "F0a / R2 (B2's impersonation stamp is checked like B1's)",
	"RESTRefusalBaselineMayBeRelayed":         "F0a / R2 (B2 on a forwarder endpoint is judged like B1)",
	"RESTRefusalBuildUnbound":                 "F0a only: B2 is admitted alongside the same candidate leg C1, whose build already passed",
	// Structural (F1) and the re-read's own structural refusal (R3).
	"RefusalVacuousEmptyLegs":     "F1 / F1i",
	"RefusalScopeNotReflected":    "not an axis: GraphQL prover only",
	"RefusalLegsDoNotOverlap":     "F1",
	"RESTRefusalRereadStructural": "R3",
	// The re-read's own leaf and declaration refusals.
	"RESTRefusalLeafMovedBetweenReads":     "R4",
	"RESTRefusalRereadLeafAmbiguous":       "R4 (a leaf whose path denotes more than one location is never witnessed)",
	"RESTRefusalRereadDeclarationsInvalid": "R6",
	// Result.Acceptance codes: HARD ones refuse the first comparison on
	// the normal path; soft ones are P-declarations (the run fails at the
	// end); every one of them on the second comparison is R6.
	"RefusalUndeclaredNumericLeaf":          "Acceptance (hard) / R6",
	"RefusalOrderInsensitiveListKeyMissing": "Acceptance (hard) / R6",
	"RefusalStochasticLeafClassUnfit":       "Acceptance (hard) / R6",
	"RefusalStaleTierB":                     "Acceptance (soft, P-declarations) / R6",
	"RefusalLiveBaselineDefectUnexplained":  "Acceptance (soft, P-declarations) / R6",
	"RefusalStaleBaselineDefect":            "Acceptance (soft, P-declarations) / R6",
	"RefusalStaleExclusion":                 "Acceptance (soft, P-declarations) / R6",
	"RefusalStaleOrderInsensitiveList":      "Acceptance (soft, P-declarations) / R6",
	// The bounded-search gate (P-iterate on F2, and on B2/C1 as R7i).
	"RESTRefusalDeclaredIDListUnrecognised": "P-iterate on F2 / F1i / R7i",
	"RESTRefusalNoLegProducedTheDeclaredID": "P-iterate on F2 / R7i",
	// Status-only producer (F0b): no body is compared, no re-read.
	"RESTRefusalCandidateProducerUnresolved": "F0b",
	"RESTRefusalCandidateBodyUndecodable":    "F0b",
	// Refused before any leg is read, or a search-level outcome.
	"RESTRefusalIDBindingUnresolved":         "not an axis: refused before any leg is read",
	"RESTRefusalBoundIDNotAPathLiteral":      "not an axis: refused before any leg is read",
	"RESTRefusalCandidateIterationExhausted": "not an axis: a search-level outcome with no comparison of its own",
	// GraphQL prover only: the REST prover never produces these.
	"RefusalBuildUnbound":               "not an axis: GraphQL prover only",
	"RefusalBuildMismatch":              "not an axis: GraphQL prover only",
	"RefusalDocumentDigestDrift":        "not an axis: GraphQL prover only",
	"RefusalEmptyResponseRoot":          "not an axis: GraphQL prover only",
	"RefusalErroredResponse":            "not an axis: GraphQL prover only",
	"RefusalInvalidBaselineDefect":      "not an axis: GraphQL prover only",
	"RefusalInvalidStochasticLeafClass": "not an axis: GraphQL prover only",
	"RefusalNeedsInstanceID":            "not an axis: GraphQL prover only",
	"RefusalNonFinite":                  "not an axis: GraphQL prover only",
	"RefusalNoPayload":                  "not an axis: GraphQL prover only",
	"RefusalNonSuccessStatus":           "not an axis: GraphQL prover only",
	"RefusalNotRouted":                  "not an axis: GraphQL prover only",
	"RefusalPlaneUnidentified":          "not an axis: GraphQL prover only",
	"RefusalServedUnderImpersonation":   "not an axis: GraphQL prover only",
	"RefusalShadowUnmeasurable":         "not an axis: GraphQL prover only",
	"RefusalTrailingBytes":              "not an axis: GraphQL prover only",
	"RefusalTransport":                  "not an axis: GraphQL prover only",
	"RefusalUndecodableResponse":        "not an axis: GraphQL prover only",
	"RefusalWrongPlane":                 "not an axis: GraphQL prover only",
}

// TestEveryRefusalHasAPlaceInTheWriteSkewTable parses the non-test Go
// files of this package and of the REST prover, collects every constant
// named Refusal* or RESTRefusal*, and requires the placement table above
// to name exactly that set.
func TestEveryRefusalHasAPlaceInTheWriteSkewTable(t *testing.T) {
	name := regexp.MustCompile(`^(REST)?Refusal[A-Z]\w*$`)
	found := map[string]bool{}
	for _, dir := range []string{".", filepath.Join("..", "..", "cmd", "go-api-rest-prove")} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, entry.Name()), nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, decl := range file.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok || gen.Tok != token.CONST {
					continue
				}
				for _, spec := range gen.Specs {
					for _, ident := range spec.(*ast.ValueSpec).Names {
						if name.MatchString(ident.Name) {
							found[ident.Name] = true
						}
					}
				}
			}
		}
	}
	if len(found) == 0 {
		t.Fatal("found no refusal constants: the scan is blind")
	}
	var unplaced, stale []string
	for constant := range found {
		if _, ok := writeSkewRefusalPlaces[constant]; !ok {
			unplaced = append(unplaced, constant)
		}
	}
	for constant := range writeSkewRefusalPlaces {
		if !found[constant] {
			stale = append(stale, constant)
		}
	}
	// Every place names a row or column of the table, or states why the
	// constant is not an axis.
	place := regexp.MustCompile(`\b(F0a|F0b|F1i?|F2|F5|F6|R[1-8]i?|Acceptance|P-iterate)\b|^not an axis: `)
	for constant, where := range writeSkewRefusalPlaces {
		if !place.MatchString(where) {
			t.Errorf("%s is placed at %q, which names no row or column of the table", constant, where)
		}
	}
	sort.Strings(unplaced)
	sort.Strings(stale)
	if len(unplaced) > 0 || len(stale) > 0 {
		t.Fatalf("refusal constants with no place in the write-skew table: %v; places naming no constant: %v", unplaced, stale)
	}
}
