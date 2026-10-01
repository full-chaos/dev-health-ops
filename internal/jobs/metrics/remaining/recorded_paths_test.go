package remaining

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedpaths"
)

// CHAOS-7473: every scalar path of the recorded engine, loader and signals goldens is declared as a
// CLAIM (a change makes a test of this package fail; the mutation run in the pull request that
// added this file backs that) or a NOT-A-CLAIM with its reason.

var engineClaims = []string{
	".cases[].now",
	".cases[].records[].computed_at",
	".cases[].records[].evidence_json",
	".cases[].records[].fired",
	".cases[].records[].org_id",
	".cases[].records[].rationale",
	".cases[].records[].rule_id",
	".cases[].records[].rule_version",
	".cases[].records[].severity",
	".cases[].records[].success_criterion",
	".cases[].records[].team_id",
	".cases[].records[].title",
	".cases[].records[].window_end",
	".cases[].records[].window_start",
	".cases[].snapshot.after_hours_ratio",
	".cases[].snapshot.compounding_risk_score",
	".cases[].snapshot.compounding_risk_severity",
	".cases[].snapshot.cycle_time_by_day[]",
	".cases[].snapshot.hotspot_churn_overlap",
	".cases[].snapshot.hotspot_complexity_delta",
	".cases[].snapshot.review_latency_p75_hours",
	".cases[].snapshot.reviewer_gini",
	".cases[].snapshot.rework_churn_ratio",
	".cases[].snapshot.throughput_by_cycle[]",
	".cases[].snapshot.wip_by_day[]",
	".cases[].window_days",
	".environment.float_repr_style",
	".environment.implementation",
	".environment.python_version",
	".record_counts.fired",
	".record_counts.tombstones",
	".record_counts.total",
}

var engineNotClaims = map[string]string{
	".cases[].name":                     "a case label (subtest/failure-message name only)",
	".purpose":                          "prose describing the file, read by no test",
	".environment.machine_not_compared": "the generating machine, recorded on purpose and not compared (a corpus must not depend on the CPU that made it; see TestFrozenEnvironmentIsPortable)",
}
var loaderClaims = []string{
	".cases[].queries_seen[]",
	".cases[].snapshot.after_hours_ratio",
	".cases[].snapshot.compounding_risk_score",
	".cases[].snapshot.compounding_risk_severity",
	".cases[].snapshot.cycle_time_by_day[]",
	".cases[].snapshot.hotspot_churn_overlap",
	".cases[].snapshot.hotspot_complexity_delta",
	".cases[].snapshot.review_latency_p75_hours",
	".cases[].snapshot.reviewer_gini",
	".cases[].snapshot.rework_churn_ratio",
	".cases[].snapshot.throughput_by_cycle[]",
	".cases[].snapshot.wip_by_day[]",
	".cases[].tables.compounding_risk_daily.columns[]",
	".cases[].tables.compounding_risk_daily.rows[][]",
	".cases[].tables.cycle_time.columns[]",
	".cases[].tables.cycle_time.rows[][]",
	".cases[].tables.file_hotspot_daily.columns[]",
	".cases[].tables.file_hotspot_daily.rows[][]",
	".cases[].tables.latency.columns[]",
	".cases[].tables.latency.rows[][]",
	".cases[].tables.repo_complexity_daily.columns[]",
	".cases[].tables.repo_complexity_daily.rows[][]",
	".cases[].tables.rework.columns[]",
	".cases[].tables.rework.rows[][]",
	".cases[].tables.team_metrics_daily.columns[]",
	".cases[].tables.team_metrics_daily.rows[][]",
	".cases[].tables.user_metrics_daily.columns[]",
	".cases[].tables.user_metrics_daily.rows[][]",
	".cases[].tables.wip_throughput.columns[]",
	".cases[].tables.wip_throughput.rows[][]",
	".environment.float_repr_style",
	".environment.implementation",
	".environment.python_version",
}

var loaderNotClaims = map[string]string{
	".cases[].name":                     "a case label (subtest/failure-message name only)",
	".purpose":                          "prose describing the file, read by no test",
	".environment.machine_not_compared": "the generating machine, recorded on purpose and not compared (a corpus must not depend on the CPU that made it; see TestFrozenEnvironmentIsPortable)",
}
var signalsClaims = []string{
	"._generator",
	"._marker",
	".cases[].gini_hex",
	".cases[].gini_is_none",
	".cases[].slope_hex",
	".cases[].values_hex[]",
	".generating_interpreter.float_repr_style",
	".generating_interpreter.implementation",
	".generating_interpreter.python_version",
	".generating_interpreter.unicode_version",
}

var signalsNotClaims = map[string]string{
	".generating_interpreter.machine": "the generating machine, recorded on purpose and not compared (see TestFrozenEnvironmentIsPortable)",
}

func TestEveryRecordedPathIsDeclared(t *testing.T) {
	for name, entry := range map[string]struct {
		file      string
		claims    []string
		notClaims map[string]string
	}{
		"engine":  {"testdata/recommendations_engine_golden.json", engineClaims, engineNotClaims},
		"loader":  {"testdata/recommendations_loader_golden.json", loaderClaims, loaderNotClaims},
		"signals": {"testdata/recommendations_signals_golden.json", signalsClaims, signalsNotClaims},
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(entry.file)
			if err != nil {
				t.Fatal(err)
			}
			recordedpaths.Check(t, raw, entry.claims, entry.notClaims)
		})
	}
}

type interpreterFacts struct {
	PythonVersion  string `json:"python_version"`
	Implementation string `json:"implementation"`
	FloatReprStyle string `json:"float_repr_style"`
	UnicodeVersion string `json:"unicode_version"`
}

// TestRecordedInterpretersArePortable applies to the engine, loader and signals goldens the
// portability check the rules golden already had (TestFrozenEnvironmentIsPortable): a bare X.Y.Z
// version, CPython, the shortest-round-trip float repr, and (signals only) a bare Unicode
// version. Before this their environment blocks were decoded and never compared.
func TestRecordedInterpretersArePortable(t *testing.T) {
	read := func(file string, into any) {
		t.Helper()
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, into); err != nil {
			t.Fatal(err)
		}
	}
	var engine, loader struct {
		Environment interpreterFacts `json:"environment"`
	}
	var signals struct {
		Interpreter interpreterFacts `json:"generating_interpreter"`
	}
	read("testdata/recommendations_engine_golden.json", &engine)
	read("testdata/recommendations_loader_golden.json", &loader)
	read("testdata/recommendations_signals_golden.json", &signals)
	for name, facts := range map[string]interpreterFacts{
		"engine": engine.Environment, "loader": loader.Environment, "signals": signals.Interpreter,
	} {
		if !portableVersion.MatchString(facts.PythonVersion) {
			t.Errorf("%s: python_version %q is not a bare X.Y.Z version", name, facts.PythonVersion)
		}
		if facts.Implementation != "CPython" {
			t.Errorf("%s: implementation %q, want CPython", name, facts.Implementation)
		}
		if facts.FloatReprStyle != "short" {
			t.Errorf("%s: float_repr_style %q, want short: on a legacy build repr falls back to the platform libc", name, facts.FloatReprStyle)
		}
	}
	if !portableVersion.MatchString(signals.Interpreter.UnicodeVersion) {
		t.Errorf("signals: unicode_version %q is not a bare X.Y.Z version: the corpus must record which Unicode tables produced it", signals.Interpreter.UnicodeVersion)
	}
}

// TestEngineRecordCountsAreTheRecordedRows pins the three totals the engine golden states about
// itself against the rows it holds, so a re-recorded or hand-trimmed file cannot keep stale totals.
func TestEngineRecordCountsAreTheRecordedRows(t *testing.T) {
	document := loadEngineGolden(t)
	var total, fired, tombstones int
	for _, testCase := range document.Cases {
		for _, record := range testCase.Records {
			total++
			if record.Fired {
				fired++
			} else {
				tombstones++
			}
		}
	}
	got := [3]int{document.RecordCounts.Total, document.RecordCounts.Fired, document.RecordCounts.Tombstones}
	if want := [3]int{total, fired, tombstones}; got != want {
		t.Fatalf("recorded record_counts total/fired/tombstones = %v, the rows hold %v", got, want)
	}
}

// TestEngineRecordsCarryTheGeneratorsLiterals pins records[].org_id and records[].rule_version.
//
// WHY THE OLD COMPARE COULD NOT FAIL: TestEvaluateStateMatchesTheReference builds the Go input
// snapshot from records[0] (snapshot.OrgID = testCase.Records[0].OrgID, and the rule version it
// passes to EvaluateState is Records[0].RuleVersion) and then compares the Go record's org_id and
// rule_version with the recorded ones: the golden value goes in and the same value comes out, so
// a changed golden moves both sides and the compare stays green. The generator names case i's
// organisation "org-%04d" and runs every rule at version "1.0.0"; this pins those two literals.
func TestEngineRecordsCarryTheGeneratorsLiterals(t *testing.T) {
	for index, testCase := range loadEngineGolden(t).Cases {
		for _, record := range testCase.Records {
			if want := fmt.Sprintf("org-%04d", index); record.OrgID != want {
				t.Errorf("case %q: record org_id %q, the generator names it %q", testCase.Name, record.OrgID, want)
			}
			if record.RuleVersion != "1.0.0" {
				t.Errorf("case %q: record rule_version %q, the generator runs version 1.0.0", testCase.Name, record.RuleVersion)
			}
		}
	}
}

// loaderQuerySlots is the order in which the reference loader reads its nine tables.
var loaderQuerySlots = []string{
	"wip_throughput", "latency", "user_metrics_daily", "rework", "team_metrics_daily",
	"cycle_time", "repo_complexity_daily", "file_hotspot_daily", "compounding_risk_daily",
}

// TestLoaderQueriesSeenAreTheTablesTheCaseSupplies pins queries_seen: the file does not pin SQL
// text (its header says so), but the list of query slots the reference issued must be the nine
// slots in order, and each case must supply exactly those tables to the Go side.
func TestLoaderQueriesSeenAreTheTablesTheCaseSupplies(t *testing.T) {
	var document struct {
		Cases []struct {
			Name        string                     `json:"name"`
			QueriesSeen []string                   `json:"queries_seen"`
			Tables      map[string]json.RawMessage `json:"tables"`
		} `json:"cases"`
	}
	raw, err := os.ReadFile("testdata/recommendations_loader_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	if len(document.Cases) == 0 {
		t.Fatal("no cases")
	}
	for _, testCase := range document.Cases {
		if !reflect.DeepEqual(testCase.QueriesSeen, loaderQuerySlots) {
			t.Errorf("case %q: queries_seen %v, want %v", testCase.Name, testCase.QueriesSeen, loaderQuerySlots)
		}
		for _, slot := range testCase.QueriesSeen {
			if _, ok := testCase.Tables[slot]; !ok {
				t.Errorf("case %q: the reference read %q but the case supplies no such table", testCase.Name, slot)
			}
		}
		if len(testCase.Tables) != len(testCase.QueriesSeen) {
			t.Errorf("case %q: %d tables supplied, %d queries seen", testCase.Name, len(testCase.Tables), len(testCase.QueriesSeen))
		}
	}
}

// TestSignalsGeneratorIsARealFile pins the signals golden's `_generator`: the path of the script
// that records it must exist, so the file can still be traced to its recorder.
func TestSignalsGeneratorIsARealFile(t *testing.T) {
	raw, err := os.ReadFile("testdata/recommendations_signals_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Generator string `json:"_generator"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	if document.Generator == "" {
		t.Fatal("the signals golden names no generator")
	}
	if _, err := os.Stat(filepath.Join("..", "..", "..", "..", document.Generator)); err != nil {
		t.Fatalf("the recorded generator %q does not exist: %v", document.Generator, err)
	}
}
