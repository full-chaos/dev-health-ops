package deploycli

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/deploy/inputs"
)

type decision map[string]any

func events(t *testing.T, errOut string) []decision {
	t.Helper()
	var out []decision
	for _, l := range strings.Split(strings.TrimSpace(errOut), "\n") {
		if !strings.Contains(l, inputsDecisionEvent) {
			continue
		}
		var d decision
		if err := json.Unmarshal([]byte(l), &d); err != nil {
			t.Fatalf("event line is not JSON: %v", err)
		}
		out = append(out, d)
	}
	return out
}

func oneEvent(t *testing.T, errOut string) decision {
	t.Helper()
	ev := events(t, errOut)
	if len(ev) != 1 {
		t.Fatalf("want exactly one inputs decision event, got %d:\n%s", len(ev), errOut)
	}
	return ev[0]
}

func lineOf(buf, prefix string) int {
	for i, l := range strings.Split(buf, "\n") {
		if strings.HasPrefix(l, prefix) {
			return i + 1
		}
	}
	return 0
}

func sub(old, new string) string {
	if strings.Count(good, old) != 1 {
		panic("fixture edit: " + old)
	}
	return strings.Replace(good, old, new, 1)
}

const imgRow = "IMGROW=Deployment/dev-health-ops-web .spec.template.spec.containers[0].image 2c87c3613bca 3c61345532cc\n"

type refusalCase struct {
	name     string
	buf      string
	class    string
	key      string
	line     int
	notBuilt bool
}

func refusalCases() []refusalCase {
	n := strings.Count(good, "\n")
	return []refusalCase{
		{"too_large", good + "#" + strings.Repeat("x", 70000) + "\n", inputs.ClassTooLarge, "", 0, false},
		{"nul_byte", good + "#a\x00b\n", inputs.ClassNulByte, "", n + 1, false},
		{"bad_byte", good + "#a\tb\n", inputs.ClassBadByte, "", n + 1, false},
		{"too_many_lines", good + strings.Repeat("#c\n", 300), inputs.ClassTooManyLines, "", 0, false},
		{"not_key_value", good + "lower=1\n", inputs.ClassNotKeyValue, "", n + 1, false},
		{"no_entries", "# only a comment\n", inputs.ClassNoEntries, "", 0, false},
		{"version_first known key", sub("DHO_DEPLOY_INPUTS=1\n", "TARGET=dev-health\n"), inputs.ClassVersionFirst, "TARGET", 1, false},
		{"version_first unknown key", "ZZ_CANARY_KEY=canary_unknown_value\n" + good, inputs.ClassVersionFirst, "", 1, false},
		{"version_mismatch", sub("DHO_DEPLOY_INPUTS=1", "DHO_DEPLOY_INPUTS=2"), inputs.ClassVersionMismatch, "DHO_DEPLOY_INPUTS", 1, false},
		{"kit_file", "PRODROLL_INPUTS=1\n" + good, inputs.ClassKitFile, "PRODROLL_INPUTS", 1, false},
		{"duplicate_key", good + "STEP=42\n", inputs.ClassDuplicateKey, "STEP", n + 1, false},
		{"bad_value", sub("STEP=41", "STEP=xx"), inputs.ClassBadValue, "STEP", lineOf(good, "STEP="), false},
		{"missing_key", strings.Replace(good, "SCRAPE=no\n", "", 1), inputs.ClassMissingKey, "SCRAPE", 0, false},
		{"unknown_key", good + "ZZ_CANARY_KEY=canary_unknown_value\n", inputs.ClassUnknownKey, "", n + 1, false},
		{"repeat_limit", good + strings.Repeat(imgRow, 64), inputs.ClassRepeatLimit, "IMGROW", n + 64, false},
		{"forbidden_key", good + "RENDER_FILE=x\n", inputs.ClassForbiddenKey, "RENDER_FILE", n + 1, false},
		{"kit_target", sub("TARGET=dev-health", "TARGET=acr"), inputs.ClassKitTarget, "TARGET", 2, true},
		{"kit_key", good + "CMDELTA=1\n", inputs.ClassKitKey, "CMDELTA", n + 1, true},
		{"hook_row", sub("Deployment/dev-health-ops-web", "Job/dev-health-ops-migrate"), inputs.ClassHookRow, "IMGROW", lineOf(good, "IMGROW="), true},
		{"auth_row", sub("Deployment/dev-health-ops-web", "Deployment/dev-health-ops-auth-service"), inputs.ClassAuthRow, "IMGROW", lineOf(good, "IMGROW="), true},
		{"rev_rule", sub("ROLLBACK_REV=449", "ROLLBACK_REV=448"), inputs.ClassRevRule, "ROLLBACK_REV", lineOf(good, "ROLLBACK_REV="), false},
		{"sha_rule", sub("SVC_SHA=1111111111111111111111111111111111111111", "SVC_SHA=033140968a4fd14b31cb3606a7fe1ec749f5b8ba"), inputs.ClassShaRule, "SVC_SHA", lineOf(good, "SVC_SHA="), false},
		{"digest_rule", sub("2c87c3613bca 3c61345532cc", "2c87c3613bca 2c87c3613bca"), inputs.ClassDigestRule, "IMGROW", lineOf(good, "IMGROW="), false},
	}
}

func TestInputsEventPass(t *testing.T) {
	for _, c := range []struct {
		verb string
		args []string
	}{
		{"self-check", []string{"deploy", "self-check", "-"}},
		{"roll", []string{"deploy", "roll", "dev-health", "pre", "-"}},
	} {
		_, errOut, _ := exec(t, pinnedHost(t), good, c.args...)
		d := oneEvent(t, errOut)
		want := decision{"event": inputsDecisionEvent, "tool": "dho", "verb": c.verb, "outcome": "PASS", "reason_class": "none",
			"not_built": false, "input_key": "", "input_line": float64(0), "target": "dev-health", "rows": float64(1)}
		for k, v := range want {
			if d[k] != v {
				t.Fatalf("%s: field %s = %v, want %v", c.verb, k, d[k], v)
			}
		}
		checkKeys(t, d)
	}
}

var allowedKeys = map[string]bool{"time": true, "level": true, "msg": true, "event": true, "tool": true, "verb": true, "outcome": true,
	"reason_class": true, "not_built": true, "input_key": true, "input_line": true, "target": true, "rows": true}

func checkKeys(t *testing.T, d decision) {
	t.Helper()
	for k := range d {
		if !allowedKeys[k] {
			t.Fatalf("event holds a field outside the allow list: %s", k)
		}
	}
	if d["level"] != "INFO" || d["msg"] != inputsDecisionEvent {
		t.Fatalf("level/msg: %v %v", d["level"], d["msg"])
	}
}

func TestInputsEventRefusedPerClass(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range refusalCases() {
		seen[c.class] = true
		for _, verb := range []string{"self-check", "roll"} {
			t.Run(c.name+"/"+verb, func(t *testing.T) {
				args := []string{"deploy", "self-check", "-"}
				if verb == "roll" {
					args = []string{"deploy", "roll", "dev-health", "pre", "-"}
				}
				out, errOut, code := exec(t, pinnedHost(t), c.buf, args...)
				if code != cli.ExitRefused || !strings.Contains(out, "INPUTS: REFUSED") {
					t.Fatalf("exit %d:\n%s", code, out)
				}
				d := oneEvent(t, errOut)
				want := decision{"event": inputsDecisionEvent, "tool": "dho", "verb": verb, "outcome": "REFUSED", "reason_class": c.class,
					"not_built": c.notBuilt, "input_key": c.key, "input_line": float64(c.line)}
				for k, v := range want {
					if d[k] != v {
						t.Fatalf("field %s = %v, want %v", k, d[k], v)
					}
				}
				checkKeys(t, d)
				if _, has := d["target"]; has {
					t.Fatal("a refusal event carries no target")
				}
			})
		}
	}
	for _, class := range inputs.Classes {
		if class == inputs.ClassStdinUnreadable || class == inputs.ClassTargetMismatch {
			continue
		}
		if !seen[class] {
			t.Errorf("no cell for reason class %s", class)
		}
	}
}

type failReader struct{}

func (failReader) Read([]byte) (int, error) { return 0, errors.New("canary_read_failure") }

func TestInputsEventStdinUnreadable(t *testing.T) {
	for verb, args := range map[string][]string{"self-check": {"deploy", "self-check", "-"}, "roll": {"deploy", "roll", "dev-health", "pre", "-"}} {
		var out, errOut strings.Builder
		code := cli.Execute(t.Context(), "dho", []cli.Command{command(pinnedHost(t))}, cli.Env{
			Args: args, Stdin: failReader{}, Stdout: &out, Stderr: &errOut,
			Lookup: func(string) (string, bool) { return "", false },
		})
		if code != cli.ExitRefused {
			t.Fatalf("%s: exit %d", verb, code)
		}
		d := oneEvent(t, errOut.String())
		if d["outcome"] != "REFUSED" || d["reason_class"] != inputs.ClassStdinUnreadable || d["verb"] != verb || d["input_line"] != float64(0) || d["input_key"] != "" {
			t.Fatalf("%s: %v", verb, d)
		}
		checkKeys(t, d)
		if strings.Contains(errOut.String(), "canary_read_failure") {
			t.Fatal("the read error text reached the event")
		}
	}
}

func TestInputsEventTargetMismatch(t *testing.T) {
	out, errOut, code := exec(t, pinnedHost(t), good, "deploy", "roll", "acr", "pre", "-")
	if code != cli.ExitRefused || !strings.Contains(out, "TARGET is not the target of the command") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	d := oneEvent(t, errOut)
	if d["outcome"] != "REFUSED" || d["reason_class"] != inputs.ClassTargetMismatch || d["input_key"] != "TARGET" || d["verb"] != "roll" || d["not_built"] != false {
		t.Fatalf("%v", d)
	}
	checkKeys(t, d)
}

func TestInputsEventOnePerRunAndNoneWithoutADecision(t *testing.T) {
	for name, c := range map[string]struct {
		h     host
		stdin string
		args  []string
		want  int
	}{
		"self-check pass":                  {pinnedHost(t), good, []string{"deploy", "self-check", "-"}, 1},
		"self-check refused":               {pinnedHost(t), good + "lower=1\n", []string{"deploy", "self-check", "-"}, 1},
		"self-check unpinned, inputs read": {unpinnedHost(t), good, []string{"deploy", "self-check", "-"}, 1},
		"roll pass":                        {pinnedHost(t), good, []string{"deploy", "roll", "dev-health", "pre", "-"}, 1},
		"roll refused":                     {pinnedHost(t), good + "lower=1\n", []string{"deploy", "roll", "dev-health", "pre", "-"}, 1},
		"roll target mismatch":             {pinnedHost(t), good, []string{"deploy", "roll", "acr", "pre", "-"}, 1},
		"self-check without inputs":        {pinnedHost(t), "", []string{"deploy", "self-check"}, 0},
		"roll unpinned: no parse":          {unpinnedHost(t), good, []string{"deploy", "roll", "dev-health", "pre", "-"}, 0},
		"roll usage error":                 {pinnedHost(t), good, []string{"deploy", "roll", "dev-health", "bogus", "-"}, 0},
		"self-check usage error":           {pinnedHost(t), good, []string{"deploy", "self-check", "x"}, 0},
	} {
		_, errOut, _ := exec(t, c.h, c.stdin, c.args...)
		if got := len(events(t, errOut)); got != c.want {
			t.Errorf("%s: %d events, want %d:\n%s", name, got, c.want, errOut)
		}
	}
}

func TestInputsEventHoldsNoValueBytes(t *testing.T) {
	canaries := []string{"ZZ_CANARY_KEY", "canary_unknown_value", "canarytgt", "Canary_Run", "zzcanary-web", "canary_author"}
	bufs := []string{
		good + "ZZ_CANARY_KEY=canary_unknown_value\n",
		"ZZ_CANARY_KEY=canary_unknown_value\n" + good,
		sub("TARGET=dev-health", "TARGET=canarytgt"),
		sub("RUN_TAG=s41-a", "RUN_TAG=Canary_Run"),
		sub("AUTHOR=fcsup", "AUTHOR=Canary_author"),
		sub("Deployment/dev-health-ops-web", "Job/zzcanary-web-migrate"),
		sub("Deployment/dev-health-ops-web", "Deployment/zzcanary-web-auth-x"),
	}
	for _, c := range refusalCases() {
		bufs = append(bufs, c.buf)
	}
	for i, buf := range bufs {
		for _, args := range [][]string{{"deploy", "self-check", "-"}, {"deploy", "roll", "dev-health", "pre", "-"}} {
			_, errOut, _ := exec(t, pinnedHost(t), buf, args...)
			d := oneEvent(t, errOut)
			checkKeys(t, d)
			for _, c := range canaries {
				if strings.Contains(errOut, c) {
					t.Fatalf("buffer %d: the event holds input bytes %q:\n%s", i, c, errOut)
				}
			}
			if k, _ := d["input_key"].(string); k != "" && !knownToken(k) {
				t.Fatalf("buffer %d: input_key %q is not a fixed key", i, k)
			}
			if s, _ := d["reason_class"].(string); s != "none" && !inClasses(s) {
				t.Fatalf("buffer %d: reason_class %q outside the set", i, s)
			}
		}
	}
}

func inClasses(s string) bool {
	i := sort.SearchStrings(sortedClasses(), s)
	c := sortedClasses()
	return i < len(c) && c[i] == s
}

func sortedClasses() []string {
	c := append([]string(nil), inputs.Classes...)
	sort.Strings(c)
	return c
}

func knownToken(k string) bool {
	if k == strings.ToUpper(k) && !strings.ContainsAny(k, " /=") {
		return !strings.Contains(k, "CANARY")
	}
	return false
}
