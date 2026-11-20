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
	file, partJob, partStep, partUploadStep, partName, fanInJob, mergedName string
}

var junitFanIns = []junitFanIn{
	{"go.yml", "go-storage-integration-shard", "Write JUnit XML parts of the shard's go test -json", "Upload the shard's JUnit part",
		"go-junit-part-${{ matrix.target }}-${{ matrix.shard }}", "go-storage-integration", "go-test-junit-integration"},
	{"go-quality.yml", "go-quality-leg", "Write JUnit XML parts of the leg's go test -json", "Upload the leg's JUnit part",
		"go-junit-part-${{ matrix.name }}", "go-quality", "go-test-junit-quality"},
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
				seen[0] = asString(asMap(s["with"])["pattern"]) == "go-junit-part-*"
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
			out = append(out, f.file+": "+f.fanInJob+" must download go-junit-part-*, merge, and upload the one artifact "+f.mergedName)
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
	gy, gq := ".github/workflows/go.yml", ".github/workflows/go-quality.yml"
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
		{"the download misses the parts", mutation{gq, "pattern: go-junit-part-*", "pattern: go-test-timings-*"}, "must download go-junit-part-*"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := junitReportViolations(t, plantTree(t, root, []string{gy, gq}, c.m))
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
