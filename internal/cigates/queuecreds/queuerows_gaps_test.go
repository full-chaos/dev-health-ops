package queuecreds_test

import "testing"

// gapCell is one planted shape and the census verdict it must get. refuse=true is a leak (or an exception
// bypass) the census must report; refuse=false is valid code it must accept.
type gapCell struct {
	name, path, body string
	refuse           bool
}

const (
	pathAdapter  = "internal/jobruntime/adapter.go"
	pathInvest   = "internal/workersctl/trigger_investment.go"
	pathWorkgrph = "internal/workersctl/trigger_workgraph.go"
	pathOutbox   = "internal/joboutbox/x.go"
)

var gapCells = []gapCell{
	// Exceptions follow the value's origin (its declared type), not the expression text.
	{"E1-execute-row-under-the-listed-name", pathAdapter, "func execute(row Row) { envelope := row; slog.Info(\"x\", \"k\", envelope.CorrelationID) }\n", true},
	{"E2-logStart-row-under-the-listed-name", pathAdapter, "func logStart(row Row) { envelope := row; slog.Info(\"x\", \"k\", envelope.CorrelationID) }\n", true},
	{"E3-logFinish-row-in-the-listed-attributes", pathAdapter, "func logFinish(row Row) { attributes := []any{\"c\", row.DedupeKey}; slog.Info(\"x\", attributes...) }\n", true},
	{"E4-startJobSpan-row-under-the-listed-name", pathAdapter, "func startJobSpan(row Row) { envelope := row; attributes := []attribute.KeyValue{attribute.String(\"k\", envelope.CorrelationID)}; _ = oteltrace.WithAttributes(attributes...) }\n", true},
	{"E5-NewAdapter-row-under-the-listed-name", pathAdapter, "func NewAdapter(row Row) { args := row; slog.Info(\"x\", \"k\", args.Kind()) }\n", true},
	{"E6-investment-row-under-the-listed-name", pathInvest, "func writeManualInvestmentTrigger(row Row) { generation := row.DedupeKey; slog.Info(\"x\", \"g\", generation) }\n", true},
	{"E7-workgraph-row-under-the-listed-name", pathWorkgrph, "func writeManualWorkgraphTrigger(row Row) { generation := row.DedupeKey; slog.Info(\"x\", \"g\", generation) }\n", true},
	{"E8-rollback-row-in-the-listed-args", pathWorkgrph, "func rollbackTriggerTx(row Row) { args := []any{row.DedupeKey}; slog.Default().Error(\"x\", args...) }\n", true},
	{"E9-row-assigned-into-a-field-of-an-envelope", pathAdapter, "func execute(row Row) { var envelope jobcontract.Envelope; envelope.CorrelationID = row.DedupeKey; slog.Info(\"x\", \"k\", envelope.CorrelationID) }\n", true},
	{"E10-row-ranged-into-the-listed-args", pathWorkgrph, "func rollbackTriggerTx(rows ...Row) { args := make([]any, 0); for _, r := range rows { args = append(args, r.DedupeKey) }; slog.Default().Error(\"x\", args...) }\n", true},
	{"E11-declared-envelope-reassigned-from-a-row", pathAdapter, "func execute(row Row) { var envelope jobcontract.Envelope; envelope = jobcontract.Envelope{CorrelationID: row.DedupeKey}; slog.Info(\"x\", \"k\", envelope.CorrelationID) }\n", true},
	{"E12-typed-parameter-reassigned-from-a-row", pathAdapter, "func execute(envelope jobcontract.Envelope, row Row) { envelope = jobcontract.Envelope{CorrelationID: row.DedupeKey}; slog.Info(\"x\", \"k\", envelope.CorrelationID) }\n", true},
	{"E13-caller-passes-a-row-built-envelope", pathAdapter, "func execute(envelope jobcontract.Envelope) { slog.Info(\"x\", \"k\", envelope.CorrelationID) }\nfunc run(row Row) { execute(jobcontract.Envelope{CorrelationID: row.DedupeKey}) }\n", true},
	{"E16-helper-returns-a-row-built-envelope", pathAdapter, "func load(row Row) jobcontract.Envelope { return jobcontract.Envelope{CorrelationID: row.DedupeKey} }\nfunc execute(envelope jobcontract.Envelope) { slog.Info(\"x\", \"k\", envelope.CorrelationID) }\nfunc run(row Row) { execute(load(row)) }\n", true},
	{"E17-helper-fills-its-named-result-from-a-row", pathAdapter, "func load(row Row) (envelope jobcontract.Envelope) { envelope.CorrelationID = row.DedupeKey; return }\nfunc execute(envelope jobcontract.Envelope) { slog.Info(\"x\", \"k\", envelope.CorrelationID) }\nfunc run(row Row) { e := load(row); execute(e) }\n", true},
	{"E14-clean-caller-passes-a-jobcontract-decoded-envelope", pathAdapter, "func execute(envelope jobcontract.Envelope) { slog.Info(\"x\", \"k\", envelope.CorrelationID) }\nfunc run(row Row) { envelope, err := jobcontract.Decode(\"k\", row.Args); if err != nil { return }; execute(envelope) }\n", false},
	{"E15-clean-helper-returns-a-jobcontract-decoded-envelope", pathAdapter, "func load(row Row) (jobcontract.Envelope, error) { return jobcontract.Decode(\"k\", row.Args) }\nfunc execute(envelope jobcontract.Envelope) { slog.Info(\"x\", \"k\", envelope.CorrelationID) }\nfunc run(row Row) { envelope, err := load(row); if err != nil { return }; execute(envelope) }\n", false},
	{"E11-clean-declared-envelope-reassigned-from-a-decoded-one", pathAdapter, "func execute(decoded jobcontract.Envelope) { var envelope jobcontract.Envelope; envelope = decoded; slog.Info(\"x\", \"k\", envelope.CorrelationID) }\n", false},
	{"E13-clean-caller-passes-a-decoded-envelope", pathAdapter, "func execute(envelope jobcontract.Envelope) { slog.Info(\"x\", \"k\", envelope.CorrelationID) }\nfunc run(decoded jobcontract.Envelope) { execute(decoded) }\n", false},
	{"E1-clean-decoded-envelope", pathAdapter, "func execute(envelope jobcontract.Envelope) { slog.Info(\"x\", \"k\", envelope.CorrelationID) }\n", false},
	{"E5-clean-typed-args-parameter", pathAdapter, "func NewAdapter[T ContractArgs]() { var args T; slog.Info(\"x\", \"k\", args.Kind()) }\n", false},
	{"E8-clean-attrs-parameter", pathWorkgrph, "func rollbackTriggerTx(attrs ...slog.Attr) { args := make([]any, 0); for _, a := range attrs { args = append(args, a) }; slog.Default().Error(\"x\", args...) }\n", false},

	// A same-package helper that emits its parameter is checked against the caller's value.
	{"H1-helper-logs-its-parameter", pathOutbox, "func logIt(v string) { slog.Info(\"x\", \"k\", v) }\nfunc f(row Row) { logIt(row.DedupeKey) }\n", true},
	{"H2-helper-passes-the-value-on", pathOutbox, "func logIt(v string) { slog.Info(\"x\", \"k\", v) }\nfunc mid(w string) { logIt(w) }\nfunc f(row Row) { mid(row.DedupeKey) }\n", true},
	{"H3-helper-formats-into-an-error-and-returns-it", pathOutbox, "func wrap(v string) error { return fmt.Errorf(\"bad %s\", v) }\nfunc f(row Row) error { return wrap(row.DedupeKey) }\n", true},
	{"H4-variadic-helper-gets-the-value-past-its-last-parameter", pathOutbox, "func logAll(vs ...string) { slog.Info(\"x\", \"k\", vs) }\nfunc f(row Row) { logAll(\"a\", row.DedupeKey) }\n", true},
	{"H5-helper-name-shared-by-two-methods", pathOutbox, "type A struct{}\ntype B struct{}\nfunc (A) emit(v string) { slog.Info(\"x\", \"k\", v) }\nfunc (B) emit(v string) {}\nfunc f(a A, row Row) { a.emit(row.DedupeKey) }\n", true},
	{"H6-shared-name-method-with-an-interface-parameter", pathOutbox, "type A struct{}\ntype B struct{}\nfunc (A) emit(v any) { slog.Info(\"x\", \"k\", v) }\nfunc (B) emit(v string) {}\nfunc f(x Thing, row Row) { var s string = row.DedupeKey; x.emit(s) }\n", true},
	{"H6-clean-shared-name-method-picked-by-the-argument-type", pathOutbox, "type Key string\ntype A struct{}\ntype B struct{}\nfunc (A) emit(v Key) { slog.Info(\"x\", \"k\", v) }\nfunc (B) emit(v string) {}\nfunc f(x Thing, row Row) { var s string = row.DedupeKey; x.emit(s) }\n", false},
	{"H7-helper-returns-its-named-result-with-a-bare-return", pathOutbox, "func load(row Row) (k string) { k = row.DedupeKey; return }\nfunc f(row Row) { e := load(row); slog.Info(\"x\", \"k\", e) }\n", true},
	{"H7-clean-named-result-guarded-before-the-bare-return", pathOutbox, "func load(row Row) (k string) { k = row.DedupeKey; if credshape.AnyCredentialShaped(k) { return \"\" }; return }\nfunc f(row Row) { e := load(row); slog.Info(\"x\", \"k\", e) }\n", false},
	{"H8-shared-name-methods-none-take-the-argument-type", pathOutbox, "type K struct{ V string }\ntype A struct{}\ntype B struct{}\nfunc (A) emit(v fmt.Stringer) { slog.Info(\"x\", \"k\", v) }\nfunc (B) emit(v int) {}\nfunc f(x Thing, row Row) { x.emit(K{V: row.DedupeKey}) }\n", true},
	{"H4-clean-variadic-helper-gets-fixed-values", pathOutbox, "func logAll(vs ...string) { slog.Info(\"x\", \"k\", vs) }\nfunc f() { logAll(\"a\", \"b\") }\n", false},
	{"H1-clean-fixed-argument", pathOutbox, "func logIt(v string) { slog.Info(\"x\", \"k\", v) }\nfunc f() { logIt(\"fixed\") }\n", false},
	{"H1-clean-guarded-at-the-call", pathOutbox, "func logIt(v string) { slog.Info(\"x\", \"k\", v) }\nfunc f(row Row) { if credshape.AnyCredentialShaped(row.DedupeKey) { return }; logIt(row.DedupeKey) }\n", false},
	{"H1-clean-guarded-in-the-helper", pathOutbox, "func logIt(v string) { if credshape.AnyCredentialShaped(v) { return }; slog.Info(\"x\", \"k\", v) }\nfunc f(row Row) { logIt(row.DedupeKey) }\n", false},

	// A copy of the guarded value keeps the guard; a different value drops it.
	{"A1-clean-alias-copy-after-the-guard", pathOutbox, "func f(row Row) { k := row.DedupeKey; if credshape.AnyCredentialShaped(k) { return }; alias := k; slog.Info(\"x\", \"k\", alias) }\n", false},
	{"A2-clean-copy-of-a-copy", pathOutbox, "func f(row Row) { k := row.DedupeKey; if credshape.AnyCredentialShaped(k) { return }; a := k; b := a; slog.Info(\"x\", \"k\", b) }\n", false},
	{"A3-other-value-after-the-guard", pathOutbox, "func f(row, other Row) { k := row.DedupeKey; if credshape.AnyCredentialShaped(k) { return }; alias := other.DedupeKey; slog.Info(\"x\", \"k\", alias) }\n", true},
	{"A4-copy-after-the-source-was-replaced", pathOutbox, "func f(row, other Row) { k := row.DedupeKey; if credshape.AnyCredentialShaped(k) { return }; k = other.DedupeKey; alias := k; slog.Info(\"x\", \"k\", alias) }\n", true},

	// The detector's truth path through a compound condition.
	{"C1-clean-detector-or-other-returns", pathOutbox, "func f(row Row, c bool) { if credshape.AnyCredentialShaped(row.DedupeKey) || c { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n", false},
	{"C2-clean-other-or-detector-returns", pathOutbox, "func f(row Row, c bool) { if c || credshape.AnyCredentialShaped(row.DedupeKey) { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n", false},
	{"C3-clean-sink-on-the-not-detector-and-other-branch", pathOutbox, "func f(row Row, c bool) { if !credshape.AnyCredentialShaped(row.DedupeKey) && c { slog.Info(\"x\", \"k\", row.DedupeKey) } }\n", false},
	{"C4-other-and-detector-returns-skips-the-detector", pathOutbox, "func f(row Row, c bool) { if c && credshape.AnyCredentialShaped(row.DedupeKey) { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n", true},
	{"C5-not-detector-or-other-returns", pathOutbox, "func f(row Row, c bool) { if !credshape.AnyCredentialShaped(row.DedupeKey) || c { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n", true},
	{"C6-sink-on-the-detector-or-other-branch", pathOutbox, "func f(row Row, c bool) { if credshape.AnyCredentialShaped(row.DedupeKey) || c { slog.Info(\"x\", \"k\", row.DedupeKey) } }\n", true},

	// An attribute constructor is a value; the value is checked where it is emitted.
	{"T1-clean-attribute-built-and-not-emitted", pathOutbox, "func f(row Row) { a := attribute.String(\"k\", row.DedupeKey); _ = a }\n", false},
	{"T2-attribute-built-then-set-on-a-span", pathOutbox, "func f(span Span, row Row) { a := attribute.String(\"k\", row.DedupeKey); span.SetAttributes(a) }\n", true},
	{"T3-attribute-built-then-passed-to-with-attributes", pathOutbox, "func f(row Row) { a := attribute.String(\"k\", row.DedupeKey); _ = oteltrace.WithAttributes(a) }\n", true},
	{"T5-attribute-appended-then-set-on-a-span", pathOutbox, "func f(span Span, row Row) { var attrs []attribute.KeyValue; attrs = append(attrs, attribute.String(\"k\", row.DedupeKey)); span.SetAttributes(attrs...) }\n", true},
	{"T4-clean-guarded-attribute-set-on-a-span", pathOutbox, "func f(span Span, row Row) { if credshape.AnyCredentialShaped(row.DedupeKey) { return }; a := attribute.String(\"k\", row.DedupeKey); span.SetAttributes(a) }\n", false},
}

func TestQueueRowCensusGapCells(t *testing.T) {
	for _, c := range gapCells {
		t.Run(c.name, func(t *testing.T) {
			got := plantCensus(t, map[string]string{c.path: c.body})
			if c.refuse && len(got) == 0 {
				t.Errorf("%s: the census did not turn red", c.name)
			}
			if !c.refuse && len(got) != 0 {
				t.Errorf("%s: valid code is reported: %v", c.name, got)
			}
		})
	}
}
