package workflows_test

import (
	"encoding/xml"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-13766: every Go test leg of go.yml and go-quality.yml converts its go test -json into a JUnit part and uploads it with
// `if: always()` (a failing leg still reports); the existing fan-in job merges the parts into ONE report per workflow run.
// The part names carry the go-junit-part- prefix (the sync's artifact cap skips it), the merged names do not.

type junitFanIn struct {
	file, partJob, partStep, partUploadStep, partName, fanInJob, mergedName, downloadPattern string
	// testRun, when set, is the run line of the leg's go test step: that step must hand the leg's go test -json directory to the part step.
	testRun string
}

const legJSONDir = "${{ runner.temp }}/go-test-leg-json"

var junitFanIns = []junitFanIn{
	{"go.yml", "go-storage-integration-shard", "Write JUnit XML parts of the shard's go test -json", "Upload the shard's JUnit part",
		"go-junit-part-${{ matrix.target }}-${{ matrix.shard }}", "go-storage-integration", "go-test-junit-integration", "go-junit-part-*", ""},
	{"go-quality.yml", "go-quality-leg", "Write JUnit XML parts of the leg's go test -json", "Upload the leg's JUnit part",
		"go-junit-part-${{ matrix.name }}", "go-quality", "go-test-junit-quality", "go-junit-part-*", ""},
	// CHAOS-14494: the three auth legs of go-auth-service.yml, one fan-in job.
	{"go-auth-service.yml", "auth-test", "Write the leg's JUnit part from its go test -json", "Upload the leg's JUnit part",
		"go-junit-part-auth-test-${{ matrix.leg }}", "auth-junit", "go-test-junit-auth", "go-junit-part-auth-*", "ci/check_go.sh auth test-leg"},
	{"go-auth-service.yml", "auth-race", "Write the leg's JUnit part from its go test -json", "Upload the leg's JUnit part",
		"go-junit-part-auth-race-${{ matrix.leg }}", "auth-junit", "go-test-junit-auth", "go-junit-part-auth-*", "ci/check_go.sh auth race-leg"},
	{"go-auth-service.yml", "auth-race-integration", "Write the leg's JUnit part from its go test -json", "Upload the leg's JUnit part",
		"go-junit-part-auth-race-integration-${{ matrix.leg }}", "auth-junit", "go-test-junit-auth", "go-junit-part-auth-*", "ci/check_go.sh auth race-integration-leg"},
}

func junitReportViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	for _, f := range junitFanIns {
		doc := loadWf(t, root, f.file)
		bestEffort := func(what string, s yamlMap) {
			if !strings.HasPrefix(asString(s["if"]), "always()") {
				out = append(out, f.file+": "+what+" must run with if: always() (a failing leg still reports)")
			}
			if s["continue-on-error"] != true {
				out = append(out, f.file+": "+what+" must be continue-on-error: it never decides the leg")
			}
		}
		partJob := pinJob(doc, f.partJob)
		conv, n := stepNamed(partJob, f.partStep)
		if n != 1 {
			out = append(out, f.file+": the JUnit part step is missing in "+f.partJob)
			continue
		}
		bestEffort("the JUnit part step", conv)
		if f.testRun != "" {
			testSteps := 0
			for _, st := range stepsOf(partJob) {
				if !strings.Contains(asString(st["run"]), f.testRun) {
					continue
				}
				testSteps++
				if asString(asMap(st["env"])["GO_TEST_LEG_JSON_DIR"]) != legJSONDir {
					out = append(out, f.file+": the go test step of "+f.partJob+" must set GO_TEST_LEG_JSON_DIR to "+legJSONDir+" (the leg writes no go test -json otherwise)")
				}
			}
			if testSteps != 1 {
				out = append(out, f.file+": "+f.partJob+" must have exactly one go test step running "+f.testRun)
			}
			if asString(asMap(conv["env"])["LEG_JSON_DIR"]) != legJSONDir {
				out = append(out, f.file+": the JUnit part step of "+f.partJob+" must read LEG_JSON_DIR "+legJSONDir+" (the directory the go test step writes)")
			}
		}
		if !strings.Contains(asString(conv["run"]), "ci/go_junit_part.sh go-junit-part ") {
			out = append(out, f.file+": the JUnit part step does not run ci/go_junit_part.sh into go-junit-part/")
		}
		up, n := stepNamed(partJob, f.partUploadStep)
		if n != 1 {
			out = append(out, f.file+": the JUnit part upload step is missing in "+f.partJob)
			continue
		}
		bestEffort("the JUnit part upload", up)
		with := asMap(up["with"])
		if !pinnedUpload.MatchString(asString(up["uses"])) || asString(with["name"]) != f.partName || strings.TrimSuffix(asString(with["path"]), "/") != "go-junit-part" {
			out = append(out, f.file+": the JUnit part upload must be the pinned upload-artifact of go-junit-part/ named "+f.partName)
		}
		fan := pinJob(doc, f.fanInJob)
		var seen [3]bool
		for _, s := range stepsOf(fan) {
			switch {
			case strings.HasPrefix(asString(s["uses"]), "actions/download-artifact@"):
				seen[0] = asString(asMap(s["with"])["pattern"]) == f.downloadPattern
				bestEffort("the part download", s)
			case strings.Contains(asString(s["run"]), "junit-parts"):
				seen[1] = true
				bestEffort("the merge", s)
			case strings.HasPrefix(asString(s["uses"]), "actions/upload-artifact@") && strings.Contains(asString(asMap(s["with"])["path"]), "junit-merged"):
				seen[2] = asString(asMap(s["with"])["name"]) == f.mergedName && pinnedUpload.MatchString(asString(s["uses"]))
				bestEffort("the merged upload", s)
			}
		}
		if seen != [3]bool{true, true, true} {
			out = append(out, f.file+": "+f.fanInJob+" must download "+f.downloadPattern+", merge, and upload the one artifact "+f.mergedName)
		}
		if strings.HasPrefix(f.mergedName, "go-junit-part-") || strings.HasPrefix(f.mergedName, "go-test-timings-") {
			out = append(out, f.file+": the merged name is excluded by the sync")
		}
	}
	return out
}

func TestEveryGoLegUploadsAJUnitPartAndTheFanInMergesThem(t *testing.T) {
	expectNoViolations(t, junitReportViolations(t, rootOf(t)))
}

func TestJUnitReportGuardPlantedDefectsAreCaught(t *testing.T) {
	root := rootOf(t)
	gy, gq, ga := ".github/workflows/go.yml", ".github/workflows/go-quality.yml", ".github/workflows/go-auth-service.yml"
	for _, c := range []struct {
		name string
		m    mutation
		want string
	}{
		{"the part upload only on success", mutation{gy, "      - name: Upload the shard's JUnit part\n        continue-on-error: true\n        if: always()", "      - name: Upload the shard's JUnit part\n        continue-on-error: true\n        if: success()"}, "if: always()"},
		{"the part step decides the shard", mutation{gy, "      - name: Write JUnit XML parts of the shard's go test -json\n        continue-on-error: true\n", "      - name: Write JUnit XML parts of the shard's go test -json\n"}, "continue-on-error"},
		{"one part name for every shard", mutation{gy, "name: go-junit-part-${{ matrix.target }}-${{ matrix.shard }}", "name: go-junit-part"}, "named go-junit-part-"},
		{"a part name the sync reads as a report", mutation{gq, "name: go-junit-part-${{ matrix.name }}", "name: go-test-junit-part-${{ matrix.name }}"}, "named go-junit-part-"},
		{"the merged report only on success", mutation{gy, "      - name: Upload the merged integration JUnit report\n        if: always()", "      - name: Upload the merged integration JUnit report\n        if: success()"}, "if: always()"},
		{"another merged name", mutation{gq, "name: go-test-junit-quality", "name: go-junit-part-quality"}, "must download go-junit-part-*"},
		{"an auth leg uploads its part only on success", mutation{ga, "      - name: Upload the leg's JUnit part\n        continue-on-error: true\n        if: always()\n        uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7\n        with:\n          name: go-junit-part-auth-race-${{ matrix.leg }}", "      - name: Upload the leg's JUnit part\n        continue-on-error: true\n        if: success()\n        uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7\n        with:\n          name: go-junit-part-auth-race-${{ matrix.leg }}"}, "if: always()"},
		{"an auth leg writes no go test -json", mutation{ga, "        env:\n          GO_TEST_LEG_JSON_DIR: ${{ runner.temp }}/go-test-leg-json\n        run: bash ci/check_go.sh auth race-leg", "        run: bash ci/check_go.sh auth race-leg"}, "GO_TEST_LEG_JSON_DIR"},
		{"an auth part reads another directory", mutation{ga, "          LEG_JSON_DIR: ${{ runner.temp }}/go-test-leg-json\n        run: bash ci/go_junit_part.sh go-junit-part \"${LEG_JSON_DIR}\"\n      - name: Upload the leg's JUnit part\n        continue-on-error: true\n        if: always()\n        uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7\n        with:\n          name: go-junit-part-auth-race-integration", "          LEG_JSON_DIR: ${{ runner.temp }}/elsewhere\n        run: bash ci/go_junit_part.sh go-junit-part \"${LEG_JSON_DIR}\"\n      - name: Upload the leg's JUnit part\n        continue-on-error: true\n        if: always()\n        uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7\n        with:\n          name: go-junit-part-auth-race-integration"}, "LEG_JSON_DIR"},
		{"the auth merged report under a part name", mutation{ga, "name: go-test-junit-auth", "name: go-junit-part-auth-merged"}, "must download go-junit-part-auth-*"},
		{"the auth download misses the parts", mutation{ga, "pattern: go-junit-part-auth-*", "pattern: go-test-timings-*"}, "must download go-junit-part-auth-*"},
		{"the download misses the parts", mutation{gq, "pattern: go-junit-part-*", "pattern: go-test-timings-*"}, "must download go-junit-part-*"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := junitReportViolations(t, plantTree(t, root, []string{gy, gq, ga}, c.m))
			expectViolation(t, got, c.want)
		})
	}
}

const junitPartFailed = `<?xml version="1.0" encoding="UTF-8"?>
<testsuites tests="2" failures="1" errors="0" skipped="0" time="1.5">
	<testsuite name="p1" tests="2" failures="1" time="1.5">
		<testcase name="TestA" classname="p1" time="0.5"></testcase>
		<testcase name="TestB" classname="p1" time="1.0"><failure message="boom"><![CDATA[full output of the failed case]]></failure></testcase>
	</testsuite>
</testsuites>
`

const junitPartSkipped = `<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
	<testsuite name="p2" tests="1"><testcase name="TestC" classname="p2"><skipped message="s"></skipped></testcase></testsuite>
</testsuites>
`

// The merge step of both workflows is executed on two parts, one with a failed case: every testcase and the failure output survive
// and the result is one well-formed <testsuites> document.
func TestMergeKeepsEveryTestcase(t *testing.T) {
	root := rootOf(t)
	for _, f := range junitFanIns {
		var merge string
		for _, s := range stepsOf(pinJob(loadWf(t, root, f.file), f.fanInJob)) {
			if strings.Contains(asString(s["run"]), "junit-parts") {
				merge = asString(s["run"])
				if env := asMap(s["env"]); env["JUNIT_MERGED_NAME"] == nil {
					t.Fatalf("%s: the merge step has no JUNIT_MERGED_NAME", f.file)
				}
			}
		}
		if merge == "" {
			t.Fatalf("%s: no merge step in %s", f.file, f.fanInJob)
		}
		dir := t.TempDir()
		for name, body := range map[string]string{"go-junit-part-a/a.xml": junitPartFailed, "go-junit-part-b/b.xml": junitPartSkipped} {
			p := filepath.Join(dir, "junit-parts", name)
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		cmd := exec.Command("bash", "-eo", "pipefail", "-c", merge)
		cmd.Dir = dir
		cmd.Env = []string{"PATH=/usr/bin:/bin:/usr/local/bin", "JUNIT_MERGED_NAME=merged"}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s: merge step failed: %v\n%s", f.file, err, out)
		}
		b, err := os.ReadFile(filepath.Join(dir, "junit-merged", "merged.xml"))
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Suites []struct {
				Name  string `xml:"name,attr"`
				Cases []struct {
					Name    string `xml:"name,attr"`
					Failure string `xml:"failure"`
				} `xml:"testcase"`
			} `xml:"testsuite"`
		}
		if err := xml.Unmarshal(b, &doc); err != nil {
			t.Fatalf("%s: merged report is not well-formed: %v\n%s", f.file, err, b)
		}
		cases := 0
		failure := ""
		for _, s := range doc.Suites {
			for _, c := range s.Cases {
				cases++
				failure += c.Failure
			}
		}
		if len(doc.Suites) != 2 || cases != 3 || !strings.Contains(failure, "full output of the failed case") {
			t.Fatalf("%s: merged report has %d suites, %d cases, failure %q; want 2, 3 and the failed case's output\n%s", f.file, len(doc.Suites), cases, failure, b)
		}
	}
}
