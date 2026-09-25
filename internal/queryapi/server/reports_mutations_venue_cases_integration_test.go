//go:build integration

package server

import (
	"fmt"
	"strings"
)

// bs is a backslash. This source never writes a backslash-u escape literally:
// a JSON escape is built as bs + "u0061" so no tool between the author and the
// compiler can decode it.
const bs = "\x5c"

// expectation is what the PYTHON plane must answer for a case: a case that does
// not get it is a malformed case, not a comparison, and would otherwise read as
// two planes agreeing on nothing.
type expectation int

const (
	expectData  expectation = iota // data.<op> is not null and there is no error
	expectNull                     // data.<op> is null and there is no error
	expectError                    // there is an error
	expectAny                      // either: the case only reports what each plane does
)

// jsonValueClasses is the grid of JSON values one JSON-typed input field takes:
// every class the columns store differently under json.loads/json.dumps. It is
// crossed with every JSON field of every mutation that has one.
func jsonValueClasses() []struct{ name, raw string } {
	deep := strings.Repeat(`{"d": `, 300) + `1` + strings.Repeat(`}`, 300)
	return []struct{ name, raw string }{
		{"object", `{"b": 1, "a": 2}`},
		{"unsorted nested", `{"z": 1, "a": {"y": 1, "b": [3, 2, 1]}}`},
		{"repeated key", `{"a": 1, "b": 2, "a": 3}`},
		{"repeated key by escape", `{"a": 1, "` + bs + `u0061": 2}`},
		{"empty object", `{}`},
		{"unicode and dotted capital I", `{"k": "é ü İ ß"}`},
		{"astral literal", "{\"k\": \"\U0001F600\"}"},
		{"astral by escape", `{"k": "` + bs + `ud83d` + bs + `ude00"}`},
		{"lone high surrogate", `{"k": "` + bs + `ud800"}`},
		{"lone low surrogate", `{"k": "` + bs + `udc00"}`},
		{"control and DEL", `{"k": "` + bs + `u0001` + bs + `u007f` + bs + `n` + bs + `t"}`},
		{"solidus escape", `{"k": "a` + bs + `/b"}`},
		{"key with escapes", `{"a` + bs + `u00e9` + bs + `"": 1}`},
		{"int above 2^53", `{"n": 9007199254740993}`},
		{"int far above 2^64", `{"n": 123456789012345678901234567890}`},
		{"negative zero", `{"i": -0, "f": -0.0}`},
		{"floats", `{"f": [1.0, 1e5, 1.5e-7, 0.1, 123456789012345680.0, 1E+2]}`},
		{"null member", `{"k": null}`},
		{"whitespace variety", "{\n\t\"a\" :  1 ,\r\n \"b\":[ 1 , 2 ] }"},
		{"nesting 300", deep},
		{"null", `null`},
		{"array", `[1, {"a": 2}]`},
		{"string", `"text"`},
		{"number", `7`},
		{"boolean", `true`},
	}
}

// oracleCases is the request set both planes answer, in order (the rows the
// early cases write are read by the later ones on both planes alike).
func oracleCases() []oracleCase {
	var cases []oracleCase
	add := func(c oracleCase) {
		if c.org == "" {
			c.org = oracleOrgA
		}
		cases = append(cases, c)
	}
	orgVar := func(org string) string { return fmt.Sprintf("%q", org) }

	// ---- createSavedReport ----
	create := func(name, input string, expect expectation, diff ...string) {
		add(oracleCase{name: "create/" + name, op: "createSavedReport", expect: expect, messageDiffers: strings.Join(diff, ""),
			vars: fmt.Sprintf(`{"orgId": %s, "input": %s}`, orgVar(oracleOrgA), input)})
	}
	create("name only", `{"name": "created one"}`, expectData)
	create("empty name", `{"name": ""}`, expectData)
	create("unicode name and description", `{"name": "Ünï İ 😀", "description": "d é"}`, expectData)
	create("template", `{"name": "template one", "isTemplate": true}`, expectData)
	create("description empty", `{"name": "described", "description": ""}`, expectData)
	create("plan and parameters", `{"name": "created three", "reportPlan": {"b": 1, "a": [1, 2.5]}, "parameters": {"z": null, "y": "é"}}`, expectData)
	for _, class := range jsonValueClasses() {
		create("reportPlan "+class.name, fmt.Sprintf(`{"name": "plan %s", "reportPlan": %s}`, class.name, class.raw), expectData)
		create("parameters "+class.name, fmt.Sprintf(`{"name": "params %s", "parameters": %s}`, class.name, class.raw), expectData)
	}
	create("schedule yearly", `{"name": "sched yearly", "scheduleCron": "0 0 1 1 *"}`, expectData)
	create("schedule zone", `{"name": "sched zone", "scheduleCron": "30 3 15 6 *", "scheduleTimezone": "America/New_York"}`, expectData)
	create("schedule empty zone", `{"name": "sched empty zone", "scheduleCron": "0 0 1 1 *", "scheduleTimezone": ""}`, expectData)
	create("schedule name collides", `{"name": "sched yearly", "scheduleCron": "0 0 1 1 *"}`, expectError, "the duplicate scheduled job name is a database error: message text differs by driver")
	create("cron five fields wrong count", `{"name": "bad cron a", "scheduleCron": "0 0 1 1"}`, expectError)
	create("cron six fields", `{"name": "bad cron b", "scheduleCron": "0 0 1 1 * *"}`, expectError)
	create("cron empty", `{"name": "bad cron c", "scheduleCron": ""}`, expectError)
	create("cron whitespace only", `{"name": "bad cron d", "scheduleCron": "   "}`, expectError)
	create("cron alias", `{"name": "bad cron e", "scheduleCron": "@daily"}`, expectError)
	create("cron five fields invalid range", `{"name": "bad cron f", "scheduleCron": "61 0 1 1 *"}`, expectError, "croniter's own message text is not reproduced")
	create("cron five fields invalid word", `{"name": "bad cron g", "scheduleCron": "a b c d e"}`, expectError, "croniter's own message text is not reproduced")
	create("cron padded", `{"name": "padded cron", "scheduleCron": "  0   0 1 1 *  "}`, expectData)
	create("zone invalid", `{"name": "bad zone a", "scheduleCron": "0 0 1 1 *", "scheduleTimezone": "Mars/Base"}`, expectError)
	create("zone absolute path", `{"name": "bad zone b", "scheduleCron": "0 0 1 1 *", "scheduleTimezone": "/UTC"}`, expectError)
	create("zone dotdot", `{"name": "bad zone c", "scheduleCron": "0 0 1 1 *", "scheduleTimezone": "../UTC"}`, expectError)
	create("zone Local", `{"name": "bad zone d", "scheduleCron": "0 0 1 1 *", "scheduleTimezone": "Local"}`, expectError)
	create("zone lowercase", `{"name": "bad zone e", "scheduleCron": "0 0 1 1 *", "scheduleTimezone": "utc"}`, expectAny)
	create("zone invalid but no cron", `{"name": "zone without cron", "scheduleTimezone": "Mars/Base"}`, expectData)

	// ---- organisation and variable shapes ----
	add(oracleCase{name: "create/other org token", op: "createSavedReport", org: oracleOrgB, expect: expectError,
		vars: fmt.Sprintf(`{"orgId": %s, "input": {"name": "cross"}}`, orgVar(oracleOrgA))})
	add(oracleCase{name: "create/org id padded", op: "createSavedReport", expect: expectError,
		vars: fmt.Sprintf(`{"orgId": " %s", "input": {"name": "padded"}}`, oracleOrgA)})
	add(oracleCase{name: "create/org id empty", op: "createSavedReport", expect: expectError,
		vars: `{"orgId": "", "input": {"name": "empty"}}`})
	add(oracleCase{name: "create/org id null", op: "createSavedReport", expect: expectError, messageDiffers: "GraphQL variable coercion message text",
		vars: `{"orgId": null, "input": {"name": "null"}}`})
	add(oracleCase{name: "create/input missing", op: "createSavedReport", expect: expectError, messageDiffers: "GraphQL variable coercion message text",
		vars: fmt.Sprintf(`{"orgId": %s}`, orgVar(oracleOrgA))})
	add(oracleCase{name: "create/input name null", op: "createSavedReport", expect: expectError, messageDiffers: "GraphQL variable coercion message text",
		vars: fmt.Sprintf(`{"orgId": %s, "input": {"name": null}}`, orgVar(oracleOrgA))})
	add(oracleCase{name: "create/input field unknown", op: "createSavedReport", expect: expectError, messageDiffers: "GraphQL variable coercion message text",
		vars: fmt.Sprintf(`{"orgId": %s, "input": {"name": "x", "nope": 1}}`, orgVar(oracleOrgA))})
	add(oracleCase{name: "create/is template wrong type", op: "createSavedReport", expect: expectError, messageDiffers: "GraphQL variable coercion message text",
		vars: fmt.Sprintf(`{"orgId": %s, "input": {"name": "x", "isTemplate": "yes"}}`, orgVar(oracleOrgA))})
	add(oracleCase{name: "create/repeated variable key", op: "createSavedReport", expect: expectData,
		vars: fmt.Sprintf(`{"orgId": "nope", "orgId": %s, "input": {"name": "dup var"}}`, orgVar(oracleOrgA))})

	// ---- updateSavedReport ----
	update := func(name, report, input string, expect expectation) {
		add(oracleCase{name: "update/" + name, op: "updateSavedReport", expect: expect,
			vars: fmt.Sprintf(`{"orgId": %s, "reportId": %q, "input": %s}`, orgVar(oracleOrgA), report, input)})
	}
	update("empty input", reportPlain, `{}`, expectData)
	update("rename", reportPlain, `{"name": "plain renamed"}`, expectData)
	update("description", reportPlain, `{"description": "new description"}`, expectData)
	update("description empty", reportPlain, `{"description": ""}`, expectData)
	update("description null is a no-op", reportPlain, `{"description": null}`, expectData)
	update("deactivate", reportPlain, `{"isActive": false}`, expectData)
	update("reactivate", reportPlain, `{"isActive": true, "isTemplate": true}`, expectData)
	update("template off", reportPlain, `{"isTemplate": false}`, expectData)
	for _, class := range jsonValueClasses() {
		update("reportPlan "+class.name, reportPlain, fmt.Sprintf(`{"reportPlan": %s}`, class.raw), expectData)
		update("parameters "+class.name, reportPlain, fmt.Sprintf(`{"parameters": %s}`, class.raw), expectData)
	}
	update("plan and parameters and name", reportPlain, `{"name": "combined", "reportPlan": {"q": 1}, "parameters": {"r": 2}}`, expectData)
	update("not found", reportMissing, `{"name": "x"}`, expectNull)
	update("other org report", reportOther, `{"name": "x"}`, expectNull)
	update("malformed id", "not-a-uuid", `{"name": "x"}`, expectError)
	update("inactive report", reportInactive, `{"name": "renamed inactive"}`, expectData)
	// The id spellings uuid.UUID(text) accepts.
	for _, spelling := range reportIDSpellings(reportClone) {
		update("id spelling "+spelling.name, spelling.text, `{"description": "spelled"}`, spelling.expect)
	}

	// The schedule grid runs against reports whose last_run_at is a fixed instant, so
	// the next run is a function of (cron, zone, base) and the same on both planes.
	sched := func(name, report, cron, zone string, expect expectation) {
		input := fmt.Sprintf(`{"scheduleCron": %q`, cron)
		if zone != "-" {
			input += fmt.Sprintf(`, "scheduleTimezone": %q`, zone)
		}
		update("schedule "+name, report, input+`}`, expect)
	}
	sched("create job", reportPlain, "0 9 * * 1", "UTC", expectData)
	sched("update job", reportPlain, "*/15 * * * *", "UTC", expectData)
	sched("update job zone", reportPlain, "0 9 * * *", "Asia/Kolkata", expectData)
	sched("update job default zone", reportPlain, "0 9 * * *", "-", expectData)
	sched("update job empty zone", reportPlain, "5 4 * * *", "", expectData)
	sched("existing job of the seeded report", reportScheduled, "30 7 * * 1-5", "Europe/London", expectData)
	sched("invalid cron keeps the job", reportScheduled, "nope", "UTC", expectError)
	sched("invalid zone keeps the job", reportScheduled, "0 6 * * *", "Mars/Base", expectError)
	sched("leap day", reportDST1, "0 0 29 2 *", "UTC", expectData)
	sched("month names", reportDST2, "0 0 1 JAN,JUN *", "UTC", expectData)
	sched("weekday names", reportDST3, "0 0 * * MON-FRI", "UTC", expectData)
	sched("steps and ranges", reportDST4, "5-59/10 1-5 * * *", "UTC", expectData)
	// DST: the base sits just before the transition, so the next occurrence lands on it.
	sched("spring forward gap", reportDST1, "30 2 * * *", "America/New_York", expectData)
	sched("fall back repeat", reportDST2, "30 1 * * *", "America/New_York", expectData)
	sched("southern spring forward", reportDST3, "30 2 * * *", "Australia/Lord_Howe", expectData)
	sched("europe fall back", reportDST4, "30 1 * * *", "Europe/London", expectData)
	sched("half hour zone", reportDST1, "0 0 * * *", "Asia/Kolkata", expectData)
	sched("day of month and week both set", reportDST2, "0 0 13 * FRI", "UTC", expectData)
	sched("last day of month", reportDST3, "0 0 L * *", "UTC", expectData)
	sched("hour step", reportDST4, "0 */7 * * *", "UTC", expectData)

	// ---- cloneSavedReport ----
	clone := func(name, input string, expect expectation) {
		add(oracleCase{name: "clone/" + name, op: "cloneSavedReport", expect: expect,
			vars: fmt.Sprintf(`{"orgId": %s, "input": %s}`, orgVar(oracleOrgA), input)})
	}
	clone("default name", fmt.Sprintf(`{"sourceReportId": %q}`, reportClone), expectData)
	clone("new name", fmt.Sprintf(`{"sourceReportId": %q, "newName": "cloned as"}`, reportClone), expectData)
	clone("empty new name", fmt.Sprintf(`{"sourceReportId": %q, "newName": ""}`, reportClone), expectData)
	clone("new name null", fmt.Sprintf(`{"sourceReportId": %q, "newName": null}`, reportClone), expectData)
	clone("overrides merge", fmt.Sprintf(`{"sourceReportId": %q, "parameterOverrides": {"c": 3, "a": 9}}`, reportClone), expectData)
	for _, class := range jsonValueClasses() {
		clone("overrides "+class.name, fmt.Sprintf(`{"sourceReportId": %q, "parameterOverrides": %s}`, reportClone, class.raw), expectData)
	}
	clone("source with a schedule", fmt.Sprintf(`{"sourceReportId": %q}`, reportScheduled), expectData)
	clone("source is inactive", fmt.Sprintf(`{"sourceReportId": %q}`, reportInactive), expectData)
	clone("source not found", fmt.Sprintf(`{"sourceReportId": %q}`, reportMissing), expectNull)
	clone("source of another org", fmt.Sprintf(`{"sourceReportId": %q}`, reportOther), expectNull)
	clone("source malformed", `{"sourceReportId": "not-a-uuid"}`, expectError)
	for _, spelling := range reportIDSpellings(reportClone) {
		clone("id spelling "+spelling.name, fmt.Sprintf(`{"sourceReportId": %q}`, spelling.text), spelling.expect)
	}

	// ---- deleteSavedReport ----
	del := func(name, report string, expect expectation) {
		add(oracleCase{name: "delete/" + name, op: "deleteSavedReport", expect: expect,
			vars: fmt.Sprintf(`{"orgId": %s, "reportId": %q}`, orgVar(oracleOrgA), report)})
	}
	del("with a run", reportDelete, expectData)
	del("again", reportDelete, expectData)
	del("not found", reportMissing, expectData)
	del("other org", reportOther, expectData)
	del("malformed", "not-a-uuid", expectError)
	del("report with a schedule", reportScheduled, expectData)

	// ---- triggerReport ----
	trigger := func(name, report string, expect expectation) {
		add(oracleCase{name: "trigger/" + name, op: "triggerReport", expect: expect,
			vars: fmt.Sprintf(`{"orgId": %s, "reportId": %q}`, orgVar(oracleOrgA), report)})
	}
	trigger("active", reportPlain, expectData)
	trigger("active again", reportPlain, expectData)
	trigger("cloned report", reportClone, expectData)
	trigger("inactive", reportInactive, expectNull)
	trigger("not found", reportMissing, expectNull)
	trigger("other org", reportOther, expectNull)
	trigger("malformed", "not-a-uuid", expectNull)
	trigger("deleted report", reportDelete, expectNull)
	for _, spelling := range reportIDSpellings(reportClone) {
		trigger("id spelling "+spelling.name, spelling.text, expectDataOrNull(spelling.expect))
	}
	return cases
}

func expectDataOrNull(e expectation) expectation {
	if e == expectData {
		return expectData
	}
	return expectNull
}

type idSpelling struct {
	name   string
	text   string
	expect expectation
}

// reportIDSpellings are the spellings of one uuid that uuid.UUID(text) does and
// does not accept (ParseReportID reproduces its rules).
func reportIDSpellings(id string) []idSpelling {
	compact := strings.ReplaceAll(id, "-", "")
	return []idSpelling{
		{"uppercase", strings.ToUpper(id), expectData},
		{"urn prefix", "urn:uuid:" + id, expectData},
		{"braces", "{" + id + "}", expectData},
		{"no hyphens", compact, expectData},
		{"hyphens elsewhere", compact[:4] + "-" + compact[4:], expectData},
		{"padded with spaces", " " + id + " ", expectError},
		{"one character short", id[:len(id)-1], expectError},
		{"one character long", id + "0", expectError},
		{"non hex", strings.Replace(id, "a", "g", 1), expectError},
		{"empty", "", expectError},
	}
}

// cronDialectExpressions are five-field expressions on which the two cron
// dialects may part: croniter (Python) against the scheduler's reviewed subset
// (sync.NextOccurrence). Each is created on both planes; the ones in
// cronDialectGoRefuses are the declared divergence, every other one must answer
// alike.
func cronDialectExpressions() []string {
	return []string{
		"\u0660 \u0660 \u0661 \u0661 *", // Arabic-Indic digits
		"R R * * *", "0 0 * * 1#2", "0 0 L * 1", "0 0 1W * *", "0 0 * * L", "0 0 ? * *",
		"0 0 1-31/2 * *", "59 23 31 12 *", "0 0 30 2 *", "*/0 * * * *", "0-0 0 * * *",
		"0 0 * * 7", "0 0 * * 0-7", "0,30 * * * *", "1,1 * * * *", "*/5,7 * * * *",
		"0 0 1 2-4 *", "0 0 1 feb-apr *", "0 0 * * sun,MON", "60 * * * *", "0 24 * * *",
		"0 0 32 * *", "0 0 * 13 *", "-1 * * * *", "1-2-3 * * * *", "0 0 * * *\t",
		"0 0 29 2 *", "*/7 */5 * * *", "0 0 L * *", "0 0 * * mon#1",
	}
}

// cronDialectMessageDiffers are the expressions both dialects refuse; the two
// engines word the refusal differently (croniter's own text against the
// scheduler parser's), so only the refusal, its path, location and data are compared.
var cronDialectMessageDiffers = map[string]bool{
	"0 0 * * L": true, "0 0 30 2 *": true, "*/0 * * * *": true, "60 * * * *": true, "0 24 * * *": true,
	"0 0 32 * *": true, "0 0 * 13 *": true, "-1 * * * *": true, "1-2-3 * * * *": true,
}

func cronDialectCases() []oracleCase {
	var cases []oracleCase
	for i, expression := range cronDialectExpressions() {
		diff := ""
		if cronDialectMessageDiffers[expression] {
			diff = "croniter's own message text is not reproduced"
		}
		cases = append(cases, oracleCase{
			name: fmt.Sprintf("cron dialect %02d %q", i, expression), op: "createSavedReport", org: oracleOrgA, expect: expectAny, messageDiffers: diff,
			vars: fmt.Sprintf(`{"orgId": %q, "input": {"name": "dialect %d", "scheduleCron": %s}}`, oracleOrgA, i, jsonString(expression)),
		})
	}
	return cases
}
