package goapiproof

import (
	"context"
	"strings"
	"testing"
	"time"
)

// wellFormedReceipt is everything Write's pre-database guards accept, so
// each test below can break exactly ONE field and attribute the refusal.
func wellFormedReceipt() Receipt {
	return Receipt{
		SchemaDigest:      "sha256:29d509cd",
		DocumentDigest:    "06ca28a0",
		SelectedOperation: "featureFlags",
		CandidateBuild:    "b18e56fa79cfe20ce0f75df148144b832d92be36",
		RequestIdentity:   "sha256:deadbeef",
		Stage:             "deployed_executed",
		TerminalState:     "match",
		OrgID:             "70d529e0",
		RecordedBy:        "go-api-prove",
		MeasurementRoute:  RouteEdge,
		// Required since CHAOS-5484: Write refuses a receipt that does
		// not say whether the build was bound per response.
		BuildBinding: EdgeBuildPresent,
		ObservedAt:   time.Unix(1757000000, 0).UTC(),
	}
}

// r9 F10a: the empty-candidate-build guard had no killer. Without it the
// INSERT reaches PostgreSQL and the composite FK refuses it there -- with a
// constraint name instead of a sentence, which is the entire reason the
// guard is stated at the call site.
func TestWriteRefusesAReceiptWithNoCandidateBuild(t *testing.T) {
	receipt := wellFormedReceipt()
	receipt.CandidateBuild = ""

	// db is nil on purpose: both guards must fire BEFORE any statement is
	// sent, so a nil Querier proves nothing was executed.
	_, err := Write(context.Background(), nil, receipt)
	if err == nil {
		t.Fatal("a receipt naming no candidate build was accepted: `proven` is keyed on the build, so such a row can never be matched by the enablement predicate")
	}
	if !strings.Contains(err.Error(), "empty candidate build") {
		t.Fatalf("the refusal must name the empty build, got: %v", err)
	}
}

// r9 F10b: the measurement-route guard had no killer either. An empty
// route is the value a caller that never set the field would send, and
// such a row cannot be told apart from served traffic later -- the whole
// reason the column exists (R50).
func TestWriteRefusesAReceiptWithNoMeasurementRoute(t *testing.T) {
	for _, route := range []string{"", "served", "Edge"} {
		receipt := wellFormedReceipt()
		receipt.MeasurementRoute = route

		_, err := Write(context.Background(), nil, receipt)
		if err == nil {
			t.Fatalf("measurement route %q was accepted: a receipt that does not SAY which route produced it cannot be told apart from served traffic", route)
		}
		if !strings.Contains(err.Error(), "measurement route") {
			t.Fatalf("route %q: the refusal must name the route, got: %v", route, err)
		}
	}

	// Control: the two legal routes must still pass the guard and reach
	// the database (which is nil here, so a panic proves they got past).
	for _, route := range []string{RouteEdge, RouteProof} {
		receipt := wellFormedReceipt()
		receipt.MeasurementRoute = route
		func() {
			defer func() { _ = recover() }()
			_, err := Write(context.Background(), nil, receipt)
			if err != nil && strings.Contains(err.Error(), "measurement route") {
				t.Fatalf("route %q is legal but the guard refused it: %v", route, err)
			}
		}()
	}
}

// CHAOS-5484 added a third pre-database guard beside the two above, and it
// arrived without a unit killer -- only the integration suite touched it.
// Same argument as the measurement-route guard one line down from it: a
// receipt that does not say how well it knew which build served it cannot
// be told apart later from one that knew exactly, and `proven` is keyed on
// candidate_build.
func TestWriteRefusesAReceiptWithNoBuildBinding(t *testing.T) {
	for _, binding := range []string{"", "run_level", "per-request", "strong"} {
		receipt := wellFormedReceipt()
		receipt.BuildBinding = binding

		// db is nil on purpose: the guard must fire BEFORE any statement
		// is sent, so a nil Querier proves nothing was executed.
		_, err := Write(context.Background(), nil, receipt)
		if err == nil {
			t.Fatalf("build binding %q was accepted", binding)
		}
		if !strings.Contains(err.Error(), "build binding") {
			t.Fatalf("binding %q: the refusal must name the binding, got: %v", binding, err)
		}
	}

	// "run_level" is first in that list on purpose: it is what an earlier
	// draft of CHAOS-5484 called the weak case, so it is the value a
	// future writer is most likely to reinvent. It was dropped because
	// every row that exists already carries run-level evidence -- R70
	// makes VerifyCandidateBuild a hard refusal -- so the value would
	// distinguish nothing. The DB CHECK says the same thing; this says it
	// before the round trip.

	// Control: both legal bindings pass the guard and reach the database
	// (nil here, so getting past shows in the error, not a refusal).
	for _, binding := range []string{EdgeBuildPresent, EdgeBuildAbsent} {
		receipt := wellFormedReceipt()
		receipt.BuildBinding = binding
		func() {
			defer func() { _ = recover() }()
			_, err := Write(context.Background(), nil, receipt)
			if err != nil && strings.Contains(err.Error(), "build binding") {
				t.Fatalf("binding %q is legal but the guard refused it: %v", binding, err)
			}
		}()
	}
}

// opus r6 (P2-1): the SQL cutset literal was HAND-TYPED beside the
// constant it claimed to be generated from, and one of its escapes, `\v`,
// is the letter v on PostgreSQL 16 and a vertical tab on 17 and 18. The Go
// readers and the Python predicate (which binds the cutset as a parameter)
// then disagreed about "names nothing" on PG16, in both directions.
//
// Two properties, both needed. The literal must DECODE back to exactly
// blankCitationCutset -- otherwise it is a second definition however it
// was produced. And it may contain only the numeric escapes (\xNN,
// \uNNNN, \UNNNNNNNN), whose meaning is a code point on every server
// version, never a named escape a version can add or drop. The decode
// below implements only that grammar, so a named escape fails the
// decode itself rather than being interpreted by this test's guess at
// what some server would do with it.
func TestTheBlankCutsetLiteralIsGeneratedFromTheConstant(t *testing.T) {
	literal := blankCitationSQL()
	if !strings.HasPrefix(literal, "E'") || !strings.HasSuffix(literal, "'") || len(literal) < 3 {
		t.Fatalf("blankCitationSQL() = %q: not an escape-string literal E'...'", literal)
	}
	body := literal[2 : len(literal)-1]

	var decoded strings.Builder
	for len(body) > 0 {
		if body[0] != '\\' {
			t.Fatalf("blankCitationSQL() = %q: a raw character %q is in the literal; every rune must be a numeric escape so no byte depends on how the literal was typed", literal, body[0])
		}
		width := 0
		switch {
		case strings.HasPrefix(body, `\x`):
			width = 2
		case strings.HasPrefix(body, `\u`):
			width = 4
		case strings.HasPrefix(body, `\U`):
			width = 8
		default:
			t.Fatalf("blankCitationSQL() = %q: escape %q is not numeric. A named escape's meaning depends on the server version -- E'\\v' is the letter v on PostgreSQL 16 and 0x0b on 17+ -- so the literal may use only \\xNN, \\uNNNN or \\UNNNNNNNN", literal, body[:min(2, len(body))])
		}
		if len(body) < 2+width {
			t.Fatalf("blankCitationSQL() = %q: truncated escape at %q", literal, body)
		}
		var code rune
		for _, h := range body[2 : 2+width] {
			var nibble rune
			switch {
			case h >= '0' && h <= '9':
				nibble = h - '0'
			case h >= 'a' && h <= 'f':
				nibble = h - 'a' + 10
			case h >= 'A' && h <= 'F':
				nibble = h - 'A' + 10
			default:
				t.Fatalf("blankCitationSQL() = %q: %q is not a hex digit in escape %q", literal, h, body[:2+width])
			}
			code = code<<4 | nibble
		}
		if width == 2 && code >= 0x80 {
			t.Fatalf("blankCitationSQL() = %q: \\x%02x is a raw BYTE above ASCII, which is not a code point in UTF-8; use \\u", literal, code)
		}
		decoded.WriteRune(code)
		body = body[2+width:]
	}

	if decoded.String() != blankCitationCutset {
		t.Fatalf("blankCitationSQL() decodes to %q but blankCitationCutset is %q: the SQL and Go definitions of \"names nothing\" differ, which is the defect class this change exists to close", decoded.String(), blankCitationCutset)
	}
}
