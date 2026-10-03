package routeswitch

// CHAOS-8517: the catalog rule, row state by row state, at the unit tier.
//
// The registry-backed tests of this package need a Postgres container, so before this file nothing a
// plain `go test ./...` runs could see Enabled change its answer. These tests drive the REAL Enabled
// over an in-memory table that answers the two statements Enabled sends (liveModesSQL, anyRowSQL) by
// their meaning, and refuses any other statement. What they cannot show is that Postgres reads those
// two statements the same way: catalog_switch_integration_test.go does that, on a real table.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	liveSchema  = "sha256:live-schema"
	otherSchema = "sha256:another-schema"
	currentDoc  = "sha256:current-document"
	legacyDoc   = "sha256:legacy-document"
	otherDoc    = "sha256:another-document"
)

type routingRow struct{ schemaDigest, documentDigest, operation, mode string }

// fakeRegistry is go_api_routing_state as a slice. Query answers the two statements of this package by
// what they select, so a test states ROWS and never an answer.
type fakeRegistry struct {
	rows []routingRow
	// failOn makes the named statement fail: at the call (queryErr) or while its rows are read (rowsErr).
	failOn   string
	queryErr error
	rowsErr  error
	// statements is every statement Enabled sent, in order.
	statements []string
}

func (f *fakeRegistry) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.statements = append(f.statements, sql)
	if sql == f.failOn && f.queryErr != nil {
		return nil, f.queryErr
	}
	out := &valueRows{}
	if sql == f.failOn {
		out.err = f.rowsErr
	}
	switch sql {
	case liveModesSQL:
		schemaDigest, accepted, operation := args[0].(string), args[1].([]string), args[2].(string)
		for _, row := range f.rows {
			if row.schemaDigest == schemaDigest && contains(accepted, row.documentDigest) && row.operation == operation {
				out.values = append(out.values, row.mode)
			}
		}
	case anyRowSQL:
		operation := args[0].(string)
		for _, row := range f.rows {
			if row.operation == operation {
				out.values = append(out.values, "1")
				break
			}
		}
	default:
		return nil, fmt.Errorf("fakeRegistry: a statement this package does not own: %s", sql)
	}
	return out, nil
}

// valueRows is one text column of values, with an error the read ends on.
type valueRows struct {
	values []string
	next   int
	err    error
}

func (r *valueRows) Next() bool {
	if r.err != nil || r.next >= len(r.values) {
		return false
	}
	r.next++
	return true
}

func (r *valueRows) Scan(dest ...any) error {
	target, ok := dest[0].(*string)
	if !ok || len(dest) != 1 {
		return errors.New("valueRows: one *string destination")
	}
	*target = r.values[r.next-1]
	return nil
}

func (r *valueRows) Err() error                                   { return r.err }
func (r *valueRows) Close()                                       {}
func (r *valueRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *valueRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *valueRows) Values() ([]any, error)                       { return nil, nil }
func (r *valueRows) RawValues() [][]byte                          { return nil }
func (r *valueRows) Conn() *pgx.Conn                              { return nil }

// lazyPool is a pool that never connects: the constructors refuse a nil one, and these tests replace it.
func lazyPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:none@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

var (
	catalogDigests = map[string]string{"hotspots": currentDoc}
	catalogLegacy  = map[string][]string{"hotspots": {legacyDoc}}
)

// over builds the switch a constructor returns and points it at the fake table.
func over(sw *PostgresSwitch, registry *fakeRegistry) *PostgresSwitch {
	sw.pool = registry
	return sw
}

// silence keeps the two signals out of the process log and the meter for tests that do not read them.
func silence(t *testing.T) {
	t.Helper()
	previousMiss, previousServed := recordDigestMiss, recordServedWithoutRow
	recordDigestMiss = func(context.Context, string, string, string) {}
	recordServedWithoutRow = func(context.Context, string, bool) {}
	t.Cleanup(func() { recordDigestMiss, recordServedWithoutRow = previousMiss, previousServed })
}

// TestCatalogSwitchRowStateTable is the row-state table of catalog_switch.go, executed. `before` is the
// answer of the switch this route had (NewPostgresSwitchWithLegacy), `after` the answer of the catalog
// switch over the SAME rows: the two differ in exactly one state.
func TestCatalogSwitchRowStateTable(t *testing.T) {
	silence(t)
	row := func(schemaDigest, documentDigest, mode string) routingRow {
		return routingRow{schemaDigest, documentDigest, "hotspots", mode}
	}
	for _, tc := range []struct {
		name          string
		rows          []routingRow
		before, after bool
	}{
		{"no row at any schema digest", nil, false, true},
		{"only rows of other operations", []routingRow{{liveSchema, currentDoc, "home", "disabled"}, {otherSchema, currentDoc, "mcp:hotspots", "shadow"}}, false, true},
		{"live row canary", []routingRow{row(liveSchema, currentDoc, "canary")}, true, true},
		{"live row primary", []routingRow{row(liveSchema, currentDoc, "primary")}, true, true},
		{"live row canary under a legacy document digest", []routingRow{row(liveSchema, legacyDoc, "canary")}, true, true},
		{"live row shadow", []routingRow{row(liveSchema, currentDoc, "shadow")}, false, false},
		{"live row python", []routingRow{row(liveSchema, currentDoc, "python")}, false, false},
		{"live row disabled", []routingRow{row(liveSchema, currentDoc, "disabled")}, false, false},
		{"live row disabled and a canary row at another schema digest", []routingRow{row(liveSchema, currentDoc, "disabled"), row(otherSchema, currentDoc, "canary")}, false, false},
		{"legacy row disabled, current row canary", []routingRow{row(liveSchema, legacyDoc, "disabled"), row(liveSchema, currentDoc, "canary")}, true, true},
		{"only a canary row at another schema digest", []routingRow{row(otherSchema, currentDoc, "canary")}, false, false},
		{"only a disabled row at another schema digest", []routingRow{row(otherSchema, currentDoc, "disabled")}, false, false},
		{"only a python row at another schema digest", []routingRow{row(otherSchema, currentDoc, "python")}, false, false},
		{"only a shadow row at another schema digest", []routingRow{row(otherSchema, currentDoc, "shadow")}, false, false},
		{"only a canary row under another document digest", []routingRow{row(liveSchema, otherDoc, "canary")}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := lazyPool(t)
			before := over(NewPostgresSwitchWithLegacy(pool, liveSchema, catalogDigests, catalogLegacy), &fakeRegistry{rows: tc.rows})
			if got := before.Enabled("hotspots"); got != tc.before {
				t.Errorf("the switch without the catalog rule answered %v, want %v: the 'before' column is wrong", got, tc.before)
			}
			after := over(NewCatalogSwitchWithLegacy(pool, liveSchema, catalogDigests, catalogLegacy), &fakeRegistry{rows: tc.rows})
			if got := after.Enabled("hotspots"); got != tc.after {
				t.Errorf("the catalog switch answered %v, want %v", got, tc.after)
			}
		})
	}
}

// TestOnlyTheCatalogConstructorServesAnUnroutedOperation: the class-row switch (NewPostgresSwitch over
// the class digests) and both proof switches keep "no row = not reachable". Asked of each constructor
// by what it answers over an empty table, not by reading a field.
func TestOnlyTheCatalogConstructorServesAnUnroutedOperation(t *testing.T) {
	silence(t)
	pool := lazyPool(t)
	classDigests := map[string]string{"mcp:securityAlerts": "sha256:class-digest"}
	for name, tc := range map[string]struct {
		sw        *PostgresSwitch
		operation string
		want      bool
	}{
		"NewPostgresSwitch (the class-row switch)":   {NewPostgresSwitch(pool, liveSchema, classDigests), "mcp:securityAlerts", false},
		"NewPostgresSwitch (a document switch)":      {NewPostgresSwitch(pool, liveSchema, catalogDigests), "hotspots", false},
		"NewPostgresSwitchWithLegacy":                {NewPostgresSwitchWithLegacy(pool, liveSchema, catalogDigests, catalogLegacy), "hotspots", false},
		"NewProofSwitch (the class proof switch)":    {NewProofSwitch(pool, liveSchema, classDigests), "mcp:securityAlerts", false},
		"NewProofSwitchWithLegacy (document proofs)": {NewProofSwitchWithLegacy(pool, liveSchema, catalogDigests, catalogLegacy), "hotspots", false},
		"NewCatalogSwitchWithLegacy":                 {NewCatalogSwitchWithLegacy(pool, liveSchema, catalogDigests, catalogLegacy), "hotspots", true},
	} {
		if got := over(tc.sw, &fakeRegistry{}).Enabled(tc.operation); got != tc.want {
			t.Errorf("%s over an empty table: Enabled(%q) = %v, want %v", name, tc.operation, got, tc.want)
		}
	}
}

// TestCatalogSwitchNeverServesAnOperationItDoesNotRegister: the rule is for registered operations. A
// name outside the registered-document inventory -- an MCP class key included -- is refused before any
// read, on an empty table and on any other.
func TestCatalogSwitchNeverServesAnOperationItDoesNotRegister(t *testing.T) {
	silence(t)
	registry := &fakeRegistry{}
	sw := over(NewCatalogSwitchWithLegacy(lazyPool(t), liveSchema, catalogDigests, catalogLegacy), registry)
	for _, operation := range []string{"notInTheCatalog", "mcp:securityAlerts", "mcp:hotspots", ""} {
		if sw.Enabled(operation) {
			t.Errorf("the catalog switch served %q, which it does not register", operation)
		}
	}
	if len(registry.statements) != 0 {
		t.Errorf("an unregistered operation was looked up: %v", registry.statements)
	}
}

// TestCatalogSwitchFailsClosedWhenTheTableCannotBeRead: neither read failing may serve. A table that
// cannot be read cannot show that no row holds the operation dark.
func TestCatalogSwitchFailsClosedWhenTheTableCannotBeRead(t *testing.T) {
	silence(t)
	unreadable := errors.New("the registry cannot be read")
	for name, registry := range map[string]*fakeRegistry{
		"the live-row read fails at the call":      {failOn: liveModesSQL, queryErr: unreadable},
		"the live-row read fails while it is read": {failOn: liveModesSQL, rowsErr: unreadable},
		"the any-row read fails at the call":       {failOn: anyRowSQL, queryErr: unreadable},
		"the any-row read fails while it is read":  {failOn: anyRowSQL, rowsErr: unreadable},
	} {
		served := false
		recordServedWithoutRow = func(context.Context, string, bool) { served = true }
		if over(NewCatalogSwitchWithLegacy(lazyPool(t), liveSchema, catalogDigests, catalogLegacy), registry).Enabled("hotspots") {
			t.Errorf("%s: the catalog switch served the operation", name)
		}
		if served {
			t.Errorf("%s: the served-with-no-row signal fired for a refusal", name)
		}
	}
}

// TestAnOperationServedByItsRowIsAnsweredByTheSameSingleRead: for a row in a served mode the catalog
// switch sends the statement the old switch sent and nothing else, and emits no signal. This is what
// "production does not change for what it serves today" means at this layer.
func TestAnOperationServedByItsRowIsAnsweredByTheSameSingleRead(t *testing.T) {
	signals := 0
	previousMiss, previousServed := recordDigestMiss, recordServedWithoutRow
	recordDigestMiss = func(context.Context, string, string, string) { signals++ }
	recordServedWithoutRow = func(context.Context, string, bool) { signals++ }
	defer func() { recordDigestMiss, recordServedWithoutRow = previousMiss, previousServed }()

	rows := []routingRow{{liveSchema, currentDoc, "hotspots", "canary"}}
	pool := lazyPool(t)
	before, after := &fakeRegistry{rows: rows}, &fakeRegistry{rows: rows}
	if !over(NewPostgresSwitchWithLegacy(pool, liveSchema, catalogDigests, catalogLegacy), before).Enabled("hotspots") {
		t.Fatal("the old switch did not serve a canary row")
	}
	if !over(NewCatalogSwitchWithLegacy(pool, liveSchema, catalogDigests, catalogLegacy), after).Enabled("hotspots") {
		t.Fatal("the catalog switch did not serve a canary row")
	}
	if len(after.statements) != 1 || after.statements[0] != liveModesSQL || fmt.Sprint(after.statements) != fmt.Sprint(before.statements) {
		t.Errorf("statements for a served row: catalog switch %q, old switch %q, want the one live-row read from both", after.statements, before.statements)
	}
	if signals != 0 {
		t.Errorf("%d signal(s) fired for an operation served by its row, want none", signals)
	}
}

// TestCatalogSwitchSignals: the decision is observable. Served with no row fires its own signal and
// never the digest miss; a row left at another digest keeps the digest miss it always had and never
// fires the served signal.
func TestCatalogSwitchSignals(t *testing.T) {
	type served struct {
		operation string
		first     bool
	}
	var servedCalls []served
	var misses []string
	previousMiss, previousServed := recordDigestMiss, recordServedWithoutRow
	recordDigestMiss = func(_ context.Context, operation, schemaDigest, documentDigest string) {
		misses = append(misses, operation+" "+schemaDigest+" "+documentDigest)
	}
	recordServedWithoutRow = func(_ context.Context, operation string, first bool) {
		servedCalls = append(servedCalls, served{operation, first})
	}
	defer func() { recordDigestMiss, recordServedWithoutRow = previousMiss, previousServed }()

	digests := map[string]string{"hotspots": currentDoc, "home": currentDoc}
	sw := over(NewCatalogSwitchWithLegacy(lazyPool(t), liveSchema, digests, nil),
		&fakeRegistry{rows: []routingRow{{otherSchema, currentDoc, "home", "canary"}}})

	for range 2 {
		if !sw.Enabled("hotspots") {
			t.Fatal("an operation with no row was not served")
		}
	}
	if want := []served{{"hotspots", true}, {"hotspots", false}}; fmt.Sprint(servedCalls) != fmt.Sprint(want) {
		t.Errorf("served-with-no-row signals = %v, want %v (the first decision per operation is marked first)", servedCalls, want)
	}
	if len(misses) != 0 {
		t.Errorf("a served operation fired the digest miss: %v", misses)
	}

	servedCalls = nil
	if sw.Enabled("home") {
		t.Fatal("an operation whose only row is at another schema digest was served")
	}
	if want := []string{"home " + liveSchema + " " + currentDoc}; fmt.Sprint(misses) != fmt.Sprint(want) {
		t.Errorf("digest-miss signals = %v, want %v", misses, want)
	}
	if len(servedCalls) != 0 {
		t.Errorf("a refused operation fired the served-with-no-row signal: %v", servedCalls)
	}
}

// readServedWithoutRow points the package's counter at a meter of its own for the test and returns a
// reader of it: the value of the data point that carries exactly {operation, reason=catalog_no_row},
// -1 when there is none. It does not set the global meter provider: the global one hands an instrument
// built at package init to the FIRST provider a process sets and to no later one, so two tests that
// each set their own would leave the second reading nothing.
func readServedWithoutRow(t *testing.T) func(operation string) int64 {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("routeswitch-test").Int64Counter(servedWithoutRowMetric)
	if err != nil {
		t.Fatal(err)
	}
	previous := servedWithoutRowCounter
	servedWithoutRowCounter = counter
	t.Cleanup(func() { servedWithoutRowCounter = previous })
	return func(operation string) int64 {
		t.Helper()
		var metrics metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &metrics); err != nil {
			t.Fatal(err)
		}
		var value int64 = -1
		for _, scope := range metrics.ScopeMetrics {
			for _, metric := range scope.Metrics {
				sum, ok := metric.Data.(metricdata.Sum[int64])
				if metric.Name != servedWithoutRowMetric || !ok {
					continue
				}
				for _, point := range sum.DataPoints {
					got, _ := point.Attributes.Value(attribute.Key("operation"))
					reason, _ := point.Attributes.Value(attribute.Key("reason"))
					if got.AsString() == operation && reason.AsString() == ReasonCatalogNoRow && point.Attributes.Len() == 2 {
						value = point.Value
					}
				}
			}
		}
		return value
	}
}

// TestServedWithoutRowReachesTheMeterAndTheLog: the real signal, read back through a real meter reader
// and a real JSON log handler. The counter carries the operation and the closed reason value; the log
// line is written once per operation, at INFO, and carries nothing else.
func TestServedWithoutRowReachesTheMeterAndTheLog(t *testing.T) {
	read := readServedWithoutRow(t)

	var logged bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	defer slog.SetDefault(previousLogger)

	sw := over(NewCatalogSwitchWithLegacy(lazyPool(t), liveSchema, catalogDigests, nil), &fakeRegistry{})
	for range 3 {
		if !sw.Enabled("hotspots") {
			t.Fatal("an operation with no row was not served")
		}
	}

	if servedWithoutRowMetric != "devhealth_query_api_routeswitch_served_without_row_total" || ReasonCatalogNoRow != "catalog_no_row" {
		t.Errorf("the metric name or the reason value changed: %q %q (dashboards and the roll readback name both)", servedWithoutRowMetric, ReasonCatalogNoRow)
	}
	if value := read("hotspots"); value != 3 {
		t.Errorf("%s{operation=hotspots,reason=%s} = %d, want 3 (-1 = no such data point)", servedWithoutRowMetric, ReasonCatalogNoRow, value)
	}

	var records []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(logged.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		records = append(records, record)
	}
	if len(records) != 1 {
		t.Fatalf("%d log record(s) for three decisions on one operation, want 1: %s", len(records), logged.String())
	}
	record := records[0]
	delete(record, "time")
	want := map[string]any{
		"level": "INFO", "msg": "routeswitch: operation served with no go_api_routing_state row (catalog rule)",
		"operation": "hotspots", "reason": ReasonCatalogNoRow,
	}
	if fmt.Sprint(record) != fmt.Sprint(want) {
		t.Errorf("log record = %v, want %v", record, want)
	}
}
