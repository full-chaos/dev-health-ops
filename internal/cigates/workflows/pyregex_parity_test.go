package workflows_test

import (
	"regexp"
	"testing"
)

// Planted non-ASCII input for every regex ported from a python guard: U+00A0, "café" and a non-ASCII digit. Each case is
// one the python `re` (Unicode \s \w \d \b) classifies one way and an ASCII Go regexp classifies the other.
func TestPortedRegexesKeepPythonUnicodeSemantics(t *testing.T) {
	const nbsp = " "
	for _, c := range []struct {
		name string
		got  bool
		want bool
	}{
		{"container tool glued to an accented letter is not a word match", containerTool.MatchString("cafédocker pull"), false},
		{"container tool after an accented word is a word match", containerTool.MatchString("café docker pull"), true},
		{"container tool beside a non-breaking space", containerTool.MatchString("run" + nbsp + "docker" + nbsp + "pull"), true},
		{"GOFLAGS glued to an accented letter", tagsViaEnv.MatchString("éGOFLAGS=-tags=x"), false},
		{"go test tags beside a non-breaking space", goTestWithTags.MatchString("go test" + nbsp + "-tags=integration"), true},
		{"go test tags glued to an accented letter", containerLeafPattern.MatchString("go testé -tags=integration"), false},
		{"mirror host after an accented letter is another host", mirrorHost.MatchString("éghcr.io/x"), false},
		{"mirror host after a non-breaking space", mirrorHost.MatchString("x" + nbsp + "ghcr.io/x"), true},
		{"mirror host with an Arabic-Indic port", mirrorHost.MatchString("ghcr.io:٤٤٣/x"), true},
		{"check_go verb after a non-breaking space", checkGoInvocation.MatchString("ci/check_go.sh" + nbsp + "ci"), true},
		{"check_go verb stops at a non-breaking space", checkGoInvocation.FindStringSubmatch("ci/check_go.sh ci" + nbsp + "x")[1] == "ci", true},
		{"runtime stage line with a trailing non-breaking space", regexp.MustCompile(`(?m)^FROM .* AS runtime` + pyS + `*$`).MatchString("FROM x AS runtime" + nbsp + "\nRUN y"), true},
		{"a no-op prepull wrapped in non-breaking spaces is stripped to the prepull", stepKind(yamlMap{"run": nbsp + "bash ci/check_go.sh integration-prepull" + nbsp}) == "prepull", true},
		{"go test glued by a non-breaking space is one word, not a go test command", func() bool { _, err := theGoTestLine("go" + nbsp + "test -timeout 8m ./x"); return err != nil }(), true},
		{"an inner timeout value ends at a non-breaking space", innerTimeoutDefect("go test -timeout 8m"+nbsp+"x ./y", 10) == "", true},
		{"an Arabic-Indic inner timeout is read as its value", innerTimeoutDefect("go test -timeout ٨m ./y", 10) == "", true},
		{"an Arabic-Indic inner timeout over the cap is refused", innerTimeoutDefect("go test -timeout ١٢m ./y", 10) != "", true},
		{"a word search does not match inside an accented word", pyWordBounded("café_run", "run"), false},
		{"a word search matches beside a non-breaking space", pyWordBounded("a"+nbsp+"run"+nbsp+"b", "run"), true},
	} {
		if c.got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, c.got, c.want)
		}
	}
	if n, ok := pyAtoi("٥٧"); !ok || n != 57 {
		t.Errorf("pyAtoi of Arabic-Indic 57 = %d %v", n, ok)
	}
	if _, ok := pyAtoi("5x"); ok {
		t.Error("pyAtoi accepted a non-digit")
	}
	if m := regexp.MustCompile(pyBefore + `timeout (` + pyD + `+)m` + pyAfterAny).FindStringSubmatch("timeout ٥٧m"); m == nil || m[1] != "٥٧" {
		t.Errorf("the pilot timeout regexp does not read a non-ASCII digit run: %v", m)
	}
}
