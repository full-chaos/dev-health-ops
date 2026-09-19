package providersync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Invariant under test: a PagerDuty sink never sends an INSERT or a readback
// whose column shape differs from the shape of the table it writes; when it
// cannot establish the table's shape it returns an error and writes nothing.

var (
	errProbeScanShapeMatched  = errors.New("probe: scan targets match the selected columns")
	errProbeScanShapeMismatch = errors.New("probe: scan targets differ from the selected columns")
)

// Table shapes the probe connection reports through system.columns.
const (
	probeTableLegacy  = "legacy"
	probeTableCurrent = "current"
	probeTablePartial = "partial"
	probeTableAbsent  = "absent"
	probeTableFailing = "read-error"
)

var probeTableShapes = []string{probeTableLegacy, probeTableCurrent, probeTablePartial, probeTableAbsent, probeTableFailing}

// Env alphabet: "<unset>" means the variable is not present.
var probeEnvValues = []string{"<unset>", "1", "2", "", "3"}

type contractProbeConn struct {
	driver.Conn
	shape      string
	shapes     []string // when set, the n-th system.columns read reports shapes[n] (last one repeats)
	scanRows   bool
	mu         sync.Mutex
	systemRead int
	queries    []string
	inserts    []string
	appends    []int
	sends      int
}

func (conn *contractProbeConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if strings.Contains(query, "system.columns") {
		conn.systemRead++
		shape := conn.shape
		if len(conn.shapes) > 0 {
			shape = conn.shapes[min(conn.systemRead, len(conn.shapes))-1]
		}
		base := []string{"org_id", "provider", "id", "source_version_at"}
		switch shape {
		case probeTableLegacy:
			return &probeNameRows{names: base}, nil
		case probeTableCurrent:
			return &probeNameRows{names: append(base, operationalOrderingColumnNames...)}, nil
		case probeTablePartial:
			return &probeNameRows{names: append(base, "source_revision", "ingest_revision")}, nil
		case probeTableAbsent:
			return &probeNameRows{}, nil
		}
		return nil, errors.New("probe: system.columns unavailable")
	}
	conn.queries = append(conn.queries, query)
	if conn.scanRows && strings.Contains(query, "FROM operational_") {
		return &probeShapeRows{query: query}, nil
	}
	return pagerDutyProbeEmptyRows{}, nil
}

func (conn *contractProbeConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.inserts = append(conn.inserts, query)
	return &probeBatch{conn: conn}, nil
}

type probeBatch struct {
	driver.Batch
	conn *contractProbeConn
}

func (batch *probeBatch) Append(values ...any) error {
	batch.conn.mu.Lock()
	defer batch.conn.mu.Unlock()
	batch.conn.appends = append(batch.conn.appends, len(values))
	return nil
}

func (batch *probeBatch) Send() error {
	batch.conn.mu.Lock()
	defer batch.conn.mu.Unlock()
	batch.conn.sends++
	return nil
}

func (batch *probeBatch) Abort() error { return nil }

type probeNameRows struct {
	pagerDutyProbeEmptyRows
	names []string
	next  int
}

func (rows *probeNameRows) Next() bool {
	rows.next++
	return rows.next <= len(rows.names)
}

func (rows *probeNameRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = rows.names[rows.next-1]
	return nil
}

// probeShapeRows returns one row whose Scan reports whether the number of
// scan targets equals the number of columns the query selects.
type probeShapeRows struct {
	pagerDutyProbeEmptyRows
	query string
	done  bool
}

func (rows *probeShapeRows) Next() bool {
	if rows.done {
		return false
	}
	rows.done = true
	return true
}

func (rows *probeShapeRows) Scan(dest ...any) error {
	if len(dest) != len(probeSelectedColumns(rows.query)) {
		return errProbeScanShapeMismatch
	}
	return errProbeScanShapeMatched
}

type pagerDutyProbeEmptyRows struct{}

func (pagerDutyProbeEmptyRows) Next() bool                       { return false }
func (pagerDutyProbeEmptyRows) Scan(...any) error                { return nil }
func (pagerDutyProbeEmptyRows) ScanStruct(any) error             { return nil }
func (pagerDutyProbeEmptyRows) ColumnTypes() []driver.ColumnType { return nil }
func (pagerDutyProbeEmptyRows) Totals(...any) error              { return nil }
func (pagerDutyProbeEmptyRows) Columns() []string                { return nil }
func (pagerDutyProbeEmptyRows) Close() error                     { return nil }
func (pagerDutyProbeEmptyRows) Err() error                       { return nil }
func (pagerDutyProbeEmptyRows) HasData() bool                    { return false }

func probeSelectedColumns(query string) []string {
	start := strings.Index(query, "SELECT ") + len("SELECT ")
	end := strings.Index(query, " FROM ")
	return strings.Split(query[start:end], ",")
}

func probeInsertColumns(query string) []string {
	start := strings.Index(query, "(") + 1
	end := strings.LastIndex(query, ")")
	return strings.Split(query[start:end], ",")
}

func setProbeEnv(t *testing.T, value string) {
	t.Setenv(operationalOrderingContractEnv, "1")
	if value == "<unset>" {
		if err := os.Unsetenv(operationalOrderingContractEnv); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Setenv(operationalOrderingContractEnv, value)
}

// assertColumnShape checks one column list against the table's contract:
// the four ordering columns all present directly after source_version_at for
// contract 2, none present for contract 1.
func assertColumnShape(t *testing.T, label string, columns []string, current bool) {
	t.Helper()
	present := 0
	for _, column := range columns {
		for _, ordering := range operationalOrderingColumnNames {
			if strings.TrimSpace(column) == ordering {
				present++
			}
		}
	}
	if !current {
		if present != 0 {
			t.Fatalf("%s: legacy table given %d ordering columns: %v", label, present, columns)
		}
		return
	}
	if present != len(operationalOrderingColumnNames) {
		t.Fatalf("%s: current table given %d ordering columns: %v", label, present, columns)
	}
	lead := strings.Split(operationalLegacyLeadColumns, ",")
	for i, column := range lead {
		if columns[i] != column {
			t.Fatalf("%s: column %d = %q want %q", label, i, columns[i], column)
		}
	}
	for i, column := range operationalOrderingColumnNames {
		if columns[len(lead)+i] != column {
			t.Fatalf("%s: column %d = %q want %q", label, len(lead)+i, columns[len(lead)+i], column)
		}
	}
}

// TestEveryPagerDutySinkSendsOnlyTheTableShape enumerates every PagerDuty
// sink x {WriteEffect, InspectEffect} x table shape x env value, through the
// production sink types.
func TestEveryPagerDutySinkSendsOnlyTheTableShape(t *testing.T) {
	cells := 0
	for _, env := range probeEnvValues {
		for _, shape := range probeTableShapes {
			for _, operation := range []string{"write", "inspect", "inspect-scan"} {
				for _, sinkCase := range pagerDutyContractSinkCases(t, "org-acme") {
					label := fmt.Sprintf("env=%q table=%s op=%s sink=%s", env, shape, operation, sinkCase.name)
					setProbeEnv(t, env)
					conn := &contractProbeConn{shape: shape, scanRows: operation == "inspect-scan"}
					sink := sinkCase.build(conn)
					var err error
					switch operation {
					case "write":
						err = sink.WriteEffect(context.Background(), sinkCase.claim, sinkCase.effect)
					default:
						_, err = sink.InspectEffect(context.Background(), sinkCase.claim, sinkCase.effect)
					}
					cells++
					validEnv := env == "<unset>" || env == "1" || env == "2"
					known := shape == probeTableLegacy || shape == probeTableCurrent
					if !validEnv || !known {
						if err == nil {
							t.Fatalf("%s: no error for an unestablished shape", label)
						}
						if !validEnv && !errors.Is(err, ErrInvalidConfiguration) {
							t.Fatalf("%s: error=%v want invalid configuration", label, err)
						}
						if validEnv && !errors.Is(err, ErrOperationalTableContractUnknown) {
							t.Fatalf("%s: error=%v want unknown table contract", label, err)
						}
						if len(conn.inserts) != 0 || len(conn.appends) != 0 || conn.sends != 0 {
							t.Fatalf("%s: wrote inserts=%v appends=%v sends=%d", label, conn.inserts, conn.appends, conn.sends)
						}
						for _, query := range conn.queries {
							if strings.Contains(query, "FROM operational_") {
								t.Fatalf("%s: read the table without a shape: %s", label, query)
							}
						}
						continue
					}
					current := shape == probeTableCurrent
					switch operation {
					case "inspect-scan":
						if !errors.Is(err, errProbeScanShapeMatched) {
							t.Fatalf("%s: error=%v want scan targets matching the selected columns", label, err)
						}
					default:
						if err != nil {
							t.Fatalf("%s: error=%v", label, err)
						}
					}
					if operation == "write" {
						if len(conn.inserts) != 1 || conn.sends != 1 || len(conn.appends) == 0 {
							t.Fatalf("%s: inserts=%v appends=%v sends=%d", label, conn.inserts, conn.appends, conn.sends)
						}
						if !strings.HasPrefix(conn.inserts[0], "INSERT INTO "+sinkCase.table+" (") {
							t.Fatalf("%s: insert %q", label, conn.inserts[0])
						}
						columns := probeInsertColumns(conn.inserts[0])
						assertColumnShape(t, label+" insert", columns, current)
						for _, count := range conn.appends {
							if count != len(columns) {
								t.Fatalf("%s: appended %d values for %d columns", label, count, len(columns))
							}
						}
					} else if len(conn.inserts) != 0 {
						t.Fatalf("%s: readback wrote %v", label, conn.inserts)
					}
					for _, query := range conn.queries {
						if !strings.Contains(query, "FROM operational_") && !strings.Contains(query, "FROM (SELECT") {
							continue
						}
						assertColumnShape(t, label+" select", probeSelectedColumns(query), current)
						if strings.Contains(query, " FINAL ") == current {
							t.Fatalf("%s: FINAL use does not match contract %v: %s", label, current, query)
						}
					}
				}
			}
		}
	}
	if want := len(probeEnvValues) * len(probeTableShapes) * 3 * len(pagerDutyContractSinkCases(t, "org-acme")); cells != want || cells == 0 {
		t.Fatalf("cells=%d want %d", cells, want)
	}
	t.Logf("enumerated %d cells", cells)
}

func TestClassifyOperationalTableColumnsEnumeratesEveryOrderingSubset(t *testing.T) {
	for mask := 0; mask < 1<<len(operationalOrderingColumnNames); mask++ {
		columns := []string{"org_id", "id"}
		count := 0
		for i, column := range operationalOrderingColumnNames {
			if mask&(1<<i) != 0 {
				columns = append(columns, column)
				count++
			}
		}
		contract, err := classifyOperationalTableColumns(columns)
		switch count {
		case 0:
			if err != nil || contract != operationalLegacyContract {
				t.Fatalf("mask=%b contract=%d err=%v", mask, contract, err)
			}
		case len(operationalOrderingColumnNames):
			if err != nil || contract != operationalCurrentContract {
				t.Fatalf("mask=%b contract=%d err=%v", mask, contract, err)
			}
		default:
			if !errors.Is(err, ErrOperationalTableContractUnknown) {
				t.Fatalf("mask=%b contract=%d err=%v", mask, contract, err)
			}
		}
	}
	if _, err := classifyOperationalTableColumns(nil); !errors.Is(err, ErrOperationalTableContractUnknown) {
		t.Fatalf("no columns err=%v", err)
	}
}

func TestOperationalTableContractsCachesOnlySuccessfulReads(t *testing.T) {
	setProbeEnv(t, "<unset>")
	ctx := context.Background()
	conn := &contractProbeConn{shape: probeTableCurrent}
	cache := newOperationalTableContracts()
	for range 3 {
		contract, err := cache.resolve(ctx, conn, "operational_users")
		if err != nil || contract != operationalCurrentContract {
			t.Fatalf("contract=%d err=%v", contract, err)
		}
	}
	if conn.systemRead != 1 {
		t.Fatalf("system reads=%d want 1", conn.systemRead)
	}
	failing := &contractProbeConn{shape: probeTableFailing}
	for range 2 {
		if _, err := cache.resolve(ctx, failing, "operational_teams"); !errors.Is(err, ErrOperationalTableContractUnknown) {
			t.Fatalf("err=%v", err)
		}
	}
	if failing.systemRead != 2 {
		t.Fatalf("failed reads cached: system reads=%d want 2", failing.systemRead)
	}
	var none *operationalTableContracts
	uncached := &contractProbeConn{shape: probeTableLegacy}
	for range 2 {
		if contract, err := none.resolve(ctx, uncached, "operational_users"); err != nil || contract != operationalLegacyContract {
			t.Fatalf("contract=%d err=%v", contract, err)
		}
	}
	if uncached.systemRead != 2 {
		t.Fatalf("nil cache system reads=%d want 2", uncached.systemRead)
	}
	// An invalid env refuses even when the table's shape is cached.
	setProbeEnv(t, "3")
	if _, err := cache.resolve(ctx, conn, "operational_users"); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("cached shape with invalid env err=%v", err)
	}
}

func TestOperationalTableContractMismatchLogsBothValues(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	for _, env := range []string{"<unset>", "1", "2"} {
		for _, shape := range []string{probeTableLegacy, probeTableCurrent} {
			var buffer bytes.Buffer
			slog.SetDefault(slog.New(slog.NewTextHandler(&buffer, nil)))
			setProbeEnv(t, env)
			contract, err := (*operationalTableContracts)(nil).resolve(
				context.Background(), &contractProbeConn{shape: shape}, "operational_users")
			if err != nil {
				t.Fatal(err)
			}
			tableContract := operationalLegacyContract
			if shape == probeTableCurrent {
				tableContract = operationalCurrentContract
			}
			if contract != tableContract {
				t.Fatalf("env=%s table=%s used contract %d", env, shape, contract)
			}
			envContract := operationalCurrentContract
			envValue := env
			if env != "2" {
				envContract = operationalLegacyContract
			}
			if env == "<unset>" {
				envValue = "unset"
			}
			logged := buffer.String()
			if envContract == tableContract {
				if strings.Contains(logged, "operational_ordering_contract_mismatch") {
					t.Fatalf("env=%s table=%s logged a mismatch: %s", env, shape, logged)
				}
				continue
			}
			for _, want := range []string{
				"level=WARN", "operational_ordering_contract_mismatch", "table=operational_users",
				"env_value=" + envValue, fmt.Sprintf("env_contract=%d", envContract),
				fmt.Sprintf("table_contract=%d", tableContract), "used=table",
			} {
				if !strings.Contains(logged, want) {
					t.Fatalf("env=%s table=%s log %q lacks %q", env, shape, logged, want)
				}
			}
		}
	}
}

// TestEveryPagerDutySinkTypeIsInTheContractSweep is a structural pin, not the
// behaviour proof (TestEveryPagerDutySinkSendsOnlyTheTableShape is): it fails
// when a PagerDuty sink type lacks the per-call table-contract cache, an entry
// point does not start a new one, the type is missing from the behaviour
// sweep, or a PrepareBatch/Query/Append/Scan argument is not built from the
// resolved contract.
var resolvedContractArgument = regexp.MustCompile(`\bcontract\b`)

func TestEveryPagerDutySinkTypeIsInTheContractSweep(t *testing.T) {
	files, err := filepath.Glob("pagerduty_*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	declared := map[string]bool{}
	statements, entries := 0, 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.TypeSpec:
				structType, ok := typed.Type.(*ast.StructType)
				if !ok || !strings.HasPrefix(typed.Name.Name, "PagerDuty") ||
					!strings.HasSuffix(typed.Name.Name, "ClickHouseEffects") {
					return true
				}
				declared[typed.Name.Name] = true
				hasField := false
				for _, field := range structType.Fields.List {
					var buffer bytes.Buffer
					_ = printer.Fprint(&buffer, fset, field.Type)
					for _, fieldName := range field.Names {
						if fieldName.Name == "contracts" && buffer.String() == "*operationalTableContracts" {
							hasField = true
						}
					}
				}
				if !hasField {
					t.Errorf("%s has no contracts *operationalTableContracts field", typed.Name.Name)
				}
			case *ast.FuncDecl:
				if typed.Recv == nil || len(typed.Recv.List) != 1 ||
					(typed.Name.Name != "WriteEffect" && typed.Name.Name != "InspectEffect") {
					return true
				}
				var receiver bytes.Buffer
				_ = printer.Fprint(&receiver, fset, typed.Recv.List[0].Type)
				if !strings.HasPrefix(receiver.String(), "PagerDuty") || !strings.HasSuffix(receiver.String(), "ClickHouseEffects") {
					return true
				}
				entries++
				var body []string
				for _, statement := range typed.Body.List {
					var buffer bytes.Buffer
					_ = printer.Fprint(&buffer, fset, statement)
					body = append(body, buffer.String())
				}
				if len(body) == 0 || body[0] != "sink.contracts = newOperationalTableContracts()" {
					t.Errorf("%s.%s does not start with a new per-call table-contract cache: %q",
						receiver.String(), typed.Name.Name, body)
				}
				if typed.Name.Name == "InspectEffect" && strings.Join(body, "\n") != strings.Join([]string{
					"sink.contracts = newOperationalTableContracts()",
					"inspection, err := sink.inspectEffect(ctx, claim, effect)",
					"return sink.contracts.confirmInspection(ctx, sink.Conn, inspection, err)",
				}, "\n") {
					t.Errorf("%s.InspectEffect does not confirm the table shape after its readback: %q",
						receiver.String(), body)
				}
			case *ast.CallExpr:
				selector, ok := typed.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				argument := -1
				switch selector.Sel.Name {
				case "PrepareBatch", "Query":
					argument = 1
				case "Append", "Scan":
					argument = 0
				}
				if argument < 0 || len(typed.Args) <= argument {
					return true
				}
				var buffer bytes.Buffer
				_ = printer.Fprint(&buffer, fset, typed.Args[argument])
				statement := buffer.String()
				var target bytes.Buffer
				_ = printer.Fprint(&target, fset, selector.X)
				// The org's repos catalog is not an operational table.
				if strings.Contains(statement, "FROM repos") || target.String() == "catalogRows" {
					return true
				}
				statements++
				if !resolvedContractArgument.MatchString(statement) {
					t.Errorf("%s: %s statement not built from the resolved contract: %s",
						fset.Position(typed.Pos()), selector.Sel.Name, statement)
				}
			}
			return true
		})
	}
	swept := map[string]bool{}
	for _, sinkCase := range pagerDutyContractSinkCases(t, "org-acme") {
		swept[strings.TrimPrefix(fmt.Sprintf("%T", sinkCase.build(nil)), "providersync.")] = true
	}
	var missing []string
	for name := range declared {
		if !swept[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(declared) == 0 || len(missing) != 0 {
		t.Fatalf("declared=%v missing from sweep=%v", declared, missing)
	}
	if statements == 0 || entries != 2*len(declared) {
		t.Fatalf("statements=%d entry points=%d for %d sink types", statements, entries, len(declared))
	}
	t.Logf("sink types=%d entry points=%d statements=%d", len(declared), entries, statements)
}

// A table whose shape changes between two effect calls on the same sink
// value is read again: the second call sends the new shape. Within one call
// each table is read once.
func TestPagerDutySinkRereadsTheTableShapeOnEveryCall(t *testing.T) {
	setProbeEnv(t, "2")
	for _, sinkCase := range pagerDutyContractSinkCases(t, "org-acme") {
		for _, operation := range []string{"write", "inspect"} {
			conn := &contractProbeConn{shape: probeTableLegacy}
			sink := sinkCase.build(conn)
			run := func() error {
				if operation == "write" {
					return sink.WriteEffect(context.Background(), sinkCase.claim, sinkCase.effect)
				}
				_, err := sink.InspectEffect(context.Background(), sinkCase.claim, sinkCase.effect)
				return err
			}
			label := sinkCase.name + " " + operation
			if err := run(); err != nil {
				t.Fatalf("%s legacy call: %v", label, err)
			}
			// A write reads the table once; a readback reads it once more to
			// confirm the shape did not change during the call.
			perCall := 1
			if operation == "inspect" {
				perCall = 2
			}
			if conn.systemRead != perCall {
				t.Fatalf("%s: system reads in one call=%d want %d", label, conn.systemRead, perCall)
			}
			conn.shape, conn.queries, conn.inserts, conn.appends = probeTableCurrent, nil, nil, nil
			if err := run(); err != nil {
				t.Fatalf("%s current call: %v", label, err)
			}
			if conn.systemRead != 2*perCall {
				t.Fatalf("%s: system reads after second call=%d want %d", label, conn.systemRead, 2*perCall)
			}
			for _, query := range conn.queries {
				if strings.Contains(query, "FROM operational_") || strings.Contains(query, "FROM (SELECT") {
					assertColumnShape(t, label+" select after migration", probeSelectedColumns(query), true)
				}
			}
			if operation == "write" {
				assertColumnShape(t, label+" insert after migration", probeInsertColumns(conn.inserts[0]), true)
			}
		}
	}
}

// A readback never reports absent or exact when a table's shape changed
// between the call's shape read and its SELECT: every sink/destination x both
// directions returns an error and EffectConflict, and writes nothing.
func TestPagerDutyReadbackRefusesAShapeThatChangedDuringTheCall(t *testing.T) {
	setProbeEnv(t, "2")
	cells := 0
	for _, direction := range [][]string{
		{probeTableLegacy, probeTableCurrent},
		{probeTableCurrent, probeTableLegacy},
		{probeTableLegacy, probeTableFailing},
		{probeTableCurrent, probeTableAbsent},
	} {
		for _, sinkCase := range pagerDutyContractSinkCases(t, "org-acme") {
			label := fmt.Sprintf("%s->%s sink=%s", direction[0], direction[1], sinkCase.name)
			conn := &contractProbeConn{shapes: direction}
			inspection, err := sinkCase.build(conn).InspectEffect(context.Background(), sinkCase.claim, sinkCase.effect)
			cells++
			if !errors.Is(err, ErrOperationalTableContractUnknown) || inspection != EffectConflict {
				t.Fatalf("%s: inspection=%s err=%v want conflict + unknown contract", label, inspection, err)
			}
			if len(conn.inserts) != 0 || conn.sends != 0 {
				t.Fatalf("%s: readback wrote %v", label, conn.inserts)
			}
		}
	}
	t.Logf("raced readback cells=%d", cells)
}
