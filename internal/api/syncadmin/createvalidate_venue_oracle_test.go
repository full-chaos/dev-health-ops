package syncadmin

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// createValidationPinnedNow is the instant both planes compute cron
// intervals from: croniter reads time.time() for a missing start, which
// the Python program replaces with this value.
var createValidationPinnedNow = time.Date(2026, 9, 24, 12, 34, 56, 0, time.UTC)

// pythonCreateValidationProgram runs every case through the api's own code:
// croniter.croniter (as create_sync_config calls it, with croniter's
// time() pinned), utils/datetime.validate_timezone_name (with every zone
// available_timezones() lists added to the corpus), and
// providers/team_capabilities' auto-import checks (the malformed values as
// the route's detail renders them, str(value)).
const pythonCreateValidationProgram = `
import json, sys
import croniter.croniter as cronmod
from croniter import croniter as Croniter
from zoneinfo import available_timezones
from dev_health_ops.utils.datetime import validate_timezone_name
from dev_health_ops.providers.team_capabilities import (
    malformed_auto_import_category_values, unsupported_auto_import_categories,
)
payload = json.loads(sys.stdin.read())
pinned = payload["now"]
cronmod.time = lambda: pinned
crons = []
for raw in payload["crons"]:
    expr = json.loads(raw)
    try:
        itr = Croniter(expr)
        first = itr.get_next(float)
        second = itr.get_next(float)
        crons.append({"ok": True, "hours": (second - first) / 3600.0})
    except Exception as exc:
        crons.append({"ok": False, "error": type(exc).__name__ + ": " + str(exc)})
zones = []
for raw in payload["zones"] + [json.dumps(name) for name in sorted(available_timezones())]:
    value = json.loads(raw)
    try:
        validate_timezone_name(value)
        zones.append([raw, "ok"])
    except ValueError as exc:
        zones.append([raw, "ValueError: " + str(exc)])
    except TypeError:
        zones.append([raw, "TypeError"])
imports = []
for case in payload["imports"]:
    options = json.loads(case["options"])
    malformed = malformed_auto_import_category_values(options)
    imports.append({
        "malformed": [[key, str(value)] for key, value in malformed.items()],
        "unsupported": list(unsupported_auto_import_categories(case["provider"], options).items()),
    })
print(json.dumps({"crons": crons, "zones": zones, "imports": imports}))
`

type autoImportCase struct {
	Provider string `json:"provider"`
	Options  string `json:"options"`
}

// createValidationCrons is the cron corpus: every syntax form of the Go
// scheduler's five-field domain (lists, ranges, steps, names, L, W, nth and
// last weekdays, the DOM/DOW union), the forms only croniter accepts (six
// and seven fields, @aliases, R, H, "?", mixed L/W/# with the other day
// field restricted), malformed expressions of every kind, and values that
// are not strings.
func createValidationCrons() []string {
	exprs := []string{
		"* * * * *", "0 * * * *", "*/5 * * * *", "0 */6 * * *", "0 0 * * *", "0 0 * * 1", "0 0 * * 0", "0 0 * * 7",
		"0 0 * * sun", "0 0 * * MON-FRI", "0 0 * * mon,wed,fri", "0 0 1 * *", "0 0 31 * *", "0 0 29 2 *", "0 0 30 2 *",
		"0 0 1 jan *", "0 0 1 JAN,jul *", "15,45 9-17 * * 1-5", "0 0-23/2 * * *", "5-55/10 * * * *", "0 12 1,15 * *",
		"0 0 L * *", "0 0 15W * *", "0 0 LW * *", "0 0 * * 5L", "0 0 * * 1#1", "0 0 * * 1#5", "0 0 1 * 1", "0 0 13 * 5",
		"59 23 31 12 *", "0 0 * 2 *", "0 0 1-7 * 1", "  0 0 * * *  ", "0\t0 * * *", "0 0 * * 1-7", "0 0 * * 6-7",
		"0 0 1/10 * *", "0 0 */2 */3 *", "*/59 * * * *", "*/60 * * * *", "0 */24 * * *", "0 0 */31 * *", "0 0 * */12 *",
		"0 0 * * */7", "0 0 * * 0-6/2", "30 */4 * * *", "1-59/2 * * * *", "0 0 L 2 *", "0 0 L-1 * *",
		// croniter only.
		"0 0 * * * 0", "0 0 * * * */15", "0 0 * * * 0 2027", "@hourly", "@daily", "@weekly", "@monthly", "@yearly",
		"@annually", "@midnight", "R * * * *", "0 R * * *", "H * * * *", "0 0 ? * *", "0 0 * * ?", "0 0 15W * 1",
		"0 0 1 * 1#2", "0 0 L * 5", "0 0 * * 1#1,2",
		// malformed.
		"", " ", "*", "* * * *", "* * * * * * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * 32 * *", "* * * 0 *",
		"* * * 13 *", "* * * * 8", "a * * * *", "* * * * xyz", "*/0 * * * *", "*/-1 * * * *", "5-1 * * * *", "1-2-3 * * * *",
		"1,,2 * * * *", "-1 * * * *", "0 0 32W * *", "0 0 W * *", "0 0 * * 1#6", "0 0 * * 1#0", "0 0 30 2 *", "0 0 31 4 *",
		"0 0 31 2,4 *", "* * * * * *x", "1.5 * * * *", "0x1 * * * *", "٣ * * * *", "0 0 * * mon-", "0 0 * jan-feb-mar *",
		"0 0 L-40 * *", "0 0 LL * *", "0 0 1L * *", "0 0 * * L", "0 0 * * 1W", "\x00 * * * *",
	}
	out := make([]string, 0, len(exprs)+8)
	for _, expr := range exprs {
		encoded, _ := json.Marshal(expr)
		out = append(out, string(encoded))
	}
	return append(out, `5`, `0`, `true`, `false`, `null`, `[]`, `["0 * * * *"]`, `{"a": 1}`)
}

func createValidationZones() []string {
	return []string{
		`""`, `null`, `0`, `false`, `[]`, `{}`, `5`, `true`, `1.5`, `[1]`, `{"a": 1}`,
		`"UTC"`, `"utc"`, `"Local"`, `"posixrules"`, `"zone.tab"`, `"tzdata.zi"`, `"iso3166.tab"`, `"leapseconds"`, `"Factory"`,
		`"."`, `".."`, `"America/./New_York"`, `"America//New_York"`, `"America/New_York/"`, `"/UTC"`, `"../UTC"`,
		`"America/../UTC"`, `"Europe/London\u0000"`, `" UTC"`, `"UTC "`, `"EST5EDT"`, `"US/Pacific"`, `"Etc/GMT+14"`,
		`"Etc/GMT-14"`, `"right/UTC"`, `"posix/UTC"`, `"America"`, `"America/"`, `"Asia/Kolkata"`, `"GMT+0"`, `"Zulu"`,
		`"America/Argentina/Buenos_Aires"`, `"Ámerica/New_York"`, `"a\\b"`, `"SystemV/AST4"`, `"Europe/Kyiv"`, `"Europe/Kiev"`,
	}
}

func createValidationImports() []autoImportCase {
	providers := []string{"github", "gitlab", "jira", "linear", "GitHub", " jira ", "pagerduty", "launchdarkly", "", "İ"}
	options := []string{
		`{}`, `{"auto_import_teams": true}`, `{"auto_import_projects": true}`, `{"auto_import_members": true}`,
		`{"auto_import_teams": true, "auto_import_projects": true, "auto_import_members": true}`,
		`{"auto_import_members": true, "auto_import_projects": true, "auto_import_teams": true}`,
		`{"auto_import_teams": false, "auto_import_projects": false}`, `{"auto_import_projects": "false"}`,
		`{"auto_import_teams": 1, "auto_import_projects": 0, "auto_import_members": null}`,
		`{"auto_import_teams": "true", "auto_import_projects": [], "auto_import_members": {"a": 1}}`,
		`{"auto_import_projects": 1.5, "auto_import_members": "x"}`, `{"auto_import_projects": [1, "a"]}`,
		`{"auto_import_teams": 1e400}`, `{"other": true}`,
	}
	var out []autoImportCase
	for _, provider := range providers {
		for _, option := range options {
			out = append(out, autoImportCase{Provider: provider, Options: option})
		}
	}
	return out
}

// croniterOnlyForm reports whether expr is one of the documented forms
// croniter accepts outside the Go scheduler's domain (the named
// divergence): not five fields, an @alias, R or H, "?", an L/W/# day
// clause with the other day field restricted, or a non-ASCII decimal digit
// (croniter reads fields with Python's int(), which accepts "٣").
func croniterOnlyForm(expr string) bool {
	for _, r := range expr {
		if r > unicode.MaxASCII && unicode.IsDigit(r) {
			return true
		}
	}
	fields := strings.Fields(expr)
	if len(fields) != 5 || strings.HasPrefix(strings.TrimSpace(expr), "@") {
		return true
	}
	lower := strings.ToLower(expr)
	if strings.ContainsAny(lower, "rh?") {
		return true
	}
	dom, dow := fields[2], fields[4]
	return strings.ContainsAny(dom, "LWlw") && dow != "*" || strings.ContainsAny(dow, "#Ll") && dom != "*"
}

// TestCreateValidationVenueOracleMatchesLivePython holds the sync config
// write validation engines to the api's own code:
//
//   - cron: every expression Go accepts, Python accepts with the same
//     interval; every expression Python refuses, Go refuses; an expression
//     only Python accepts must be one of the documented croniter-only forms
//     (the named divergence), and is counted;
//   - timezone: the same outcome (pass, "Invalid timezone: ..." text, or
//     TypeError) for every zone available_timezones() lists and every
//     malformed key and value;
//   - auto-import: the same malformed (key, str(value)) and unsupported
//     (category, reason) lists, in order.
func TestCreateValidationVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the create validation oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	crons, zones, imports := createValidationCrons(), createValidationZones(), createValidationImports()
	input, _ := json.Marshal(map[string]any{"now": float64(createValidationPinnedNow.Unix()), "crons": crons, "zones": zones, "imports": imports})
	command := exec.Command(python, "-c", pythonCreateValidationProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want struct {
		Crons []struct {
			OK    bool    `json:"ok"`
			Hours float64 `json:"hours"`
			Error string  `json:"error"`
		} `json:"crons"`
		Zones   [][2]string `json:"zones"`
		Imports []struct {
			Malformed   [][2]string `json:"malformed"`
			Unsupported [][2]string `json:"unsupported"`
		} `json:"imports"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil || len(want.Crons) != len(crons) || len(want.Imports) != len(imports) {
		t.Fatalf("decode: %v\n%s", err, output)
	}

	accepted, refusedBoth, divergent := 0, 0, 0
	for index, raw := range crons {
		value, err := pyjson.DecodeString(raw)
		if err != nil {
			t.Fatalf("cron %s: %v", raw, err)
		}
		hours, goErr := cronIntervalHours(value, createValidationPinnedNow)
		python := want.Crons[index]
		switch {
		case goErr == nil && !python.OK:
			t.Errorf("cron %s: Go accepts (%vh), Python refuses: %s", raw, hours, python.Error)
		case goErr == nil && hours != python.Hours:
			t.Errorf("cron %s: interval go %vh, python %vh", raw, hours, python.Hours)
		case goErr == nil:
			accepted++
		case !python.OK:
			refusedBoth++
		default:
			text, _ := value.(string)
			if _, isString := value.(string); !isString || !croniterOnlyForm(text) {
				t.Errorf("cron %s: Python accepts (%vh), Go refuses outside the documented croniter-only forms: %v", raw, python.Hours, goErr)
			}
			divergent++
		}
	}
	if accepted == 0 || refusedBoth == 0 || divergent == 0 {
		t.Errorf("cron corpus did not reach every outcome: accepted %d, refused by both %d, croniter-only %d", accepted, refusedBoth, divergent)
	}

	zoneOutcomes := map[string]int{}
	for _, pair := range want.Zones {
		value, err := pyjson.DecodeString(pair[0])
		if err != nil {
			t.Fatalf("zone %s: %v", pair[0], err)
		}
		detail, err := validateTimezoneName(value)
		got := "ok"
		switch {
		case err == errTimezoneNotString:
			got = "TypeError"
		case err != nil:
			got = "error " + err.Error()
		case detail != "":
			got = "ValueError: " + detail
		}
		if got != pair[1] {
			t.Errorf("zone %s: go %q, python %q", pair[0], got, pair[1])
		}
		zoneOutcomes[strings.SplitN(got, ":", 2)[0]]++
	}
	if zoneOutcomes["ok"] < 100 || zoneOutcomes["ValueError"] == 0 || zoneOutcomes["TypeError"] == 0 {
		t.Errorf("zone corpus did not reach every outcome: %v", zoneOutcomes)
	}

	importOutcomes := map[string]int{}
	for index, c := range imports {
		value, err := pyjson.DecodeString(c.Options)
		if err != nil {
			t.Fatalf("options %s: %v", c.Options, err)
		}
		options := value.(*pyjson.Object)
		got := fmt.Sprint(nilIfEmpty(pairs(malformedAutoImportCategoryValues(options))), nilIfEmpty(pairs(unsupportedAutoImportCategories(c.Provider, options))))
		wantText := fmt.Sprint(nilIfEmpty(want.Imports[index].Malformed), nilIfEmpty(want.Imports[index].Unsupported))
		if got != wantText {
			t.Errorf("auto-import %q %s: go %s, python %s", c.Provider, c.Options, got, wantText)
		}
		importOutcomes[fmt.Sprint(len(want.Imports[index].Malformed) > 0, len(want.Imports[index].Unsupported) > 0)]++
	}
	if len(importOutcomes) != 4 {
		t.Errorf("auto-import corpus did not reach every malformed/unsupported combination: %v", importOutcomes)
	}
	t.Logf("cron: %d accepted by both, %d refused by both, %d croniter-only (named divergence); zones: %v; auto-import: %d cases %v",
		accepted, refusedBoth, divergent, zoneOutcomes, len(imports), importOutcomes)
	venueoracle.WriteProof(t)
}

func pairs(object *pyjson.Object) [][2]string {
	var out [][2]string
	for _, key := range object.Keys() {
		value, _ := object.Get(key)
		out = append(out, [2]string{key, value.(string)})
	}
	return out
}

func nilIfEmpty(list [][2]string) [][2]string {
	if len(list) == 0 {
		return nil
	}
	return list
}
