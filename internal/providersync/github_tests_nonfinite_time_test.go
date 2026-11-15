package providersync

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestGitHubJUnitNonFiniteTimesAreNullAndCounted(t *testing.T) {
	for _, value := range []string{"NaN", "+Inf", "-Inf", "Infinity", "1e999", "-3", "1.25", "abc", ""} {
		t.Run(value, func(t *testing.T) {
			body := []byte(`<testsuites><testsuite name="s" time="` + value + `"><testcase name="c" time="` + value + `"/></testsuite>` +
				`<testsuite name="ok" time="2.5"><testcase name="d" time="2.5"/></testsuite></testsuites>`)
			suites, cases, nonFinite, err := parseJUnitRows(body, "artifact-1", "repo", "run", "org", nil, nil, time.Now(), map[string]int{})
			if err != nil || len(suites) != 2 || len(cases) != 2 {
				t.Fatalf("err=%v suites=%d cases=%d", err, len(suites), len(cases))
			}
			wantNull := value != "-3" && value != "1.25"
			if (suites[0].DurationSeconds == nil) != wantNull || (cases[0].DurationSeconds == nil) != wantNull {
				t.Fatalf("suite=%v case=%v, wantNull=%v", suites[0].DurationSeconds, cases[0].DurationSeconds, wantNull)
			}
			if suites[1].DurationSeconds == nil || *suites[1].DurationSeconds != 2.5 {
				t.Fatalf("other suite time lost: %v", suites[1].DurationSeconds)
			}
			wantCount := 0
			switch value {
			case "NaN", "+Inf", "-Inf", "Infinity", "1e999":
				wantCount = 2
			}
			if nonFinite != wantCount {
				t.Fatalf("nonFinite=%d, want %d (garbage text and finite values are not counted)", nonFinite, wantCount)
			}
		})
	}
}

func TestOptionalFloatNonFiniteClasses(t *testing.T) {
	for value, want := range map[string]bool{"NaN": true, "+Inf": true, "-Inf": true, "Infinity": true, "1e999": true, "-1e999": true, "abc": false, "": false, "-3": false, "1.25": false} {
		parsed, nonFinite := parseOptionalFloat(value)
		if nonFinite != want {
			t.Fatalf("%q nonFinite=%v want %v", value, nonFinite, want)
		}
		if (value == "-3" || value == "1.25") != (parsed != nil) {
			t.Fatalf("%q parsed=%v", value, parsed)
		}
	}
	metrics := providerfoundation.NewMetrics()
	metrics.RecordNonFiniteTestTime("github", "tests", 2)
	var rendered strings.Builder
	if err := metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rendered.String(), nonFiniteCounterName+`{provider="github",dataset="tests"} 2`) {
		t.Fatalf("github counter row missing:\n%s", rendered.String())
	}
}
