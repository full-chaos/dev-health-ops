package workflows_test

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// A merge queue entry can fail first in any workflow, so every merge_group workflow that is not go.yml carries the same
// cancel-stacked-queue-children job. Its needs is every other job of the workflow: the job is declared last and a job added
// later without the needs edge fails here.

var cancelEveryWorkflows = []string{"go-quality.yml", "go-auth-service.yml", "codeql-analysis.yml", "docker-images.yml"}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func cancelEveryDefects(doc yamlMap) []string {
	jobs := jobsOf(doc)
	job := asMap(jobs[cancelJobName])
	if job == nil {
		return []string{"job " + cancelJobName + " is absent"}
	}
	var out []string
	var want []string
	for name := range jobs {
		if name != cancelJobName {
			want = append(want, name)
		}
	}
	got := pinNeeds(job)
	if fmt.Sprint(sortedCopy(got)) != fmt.Sprint(sortedCopy(want)) {
		out = append(out, fmt.Sprintf("needs is %v, want every other job %v", sortedCopy(got), sortedCopy(want)))
	}
	out = append(out, cancelJobDefectsNeeds(doc, fmt.Sprint(job["needs"]))...)
	for name, j := range jobs {
		if name == cancelJobName {
			continue
		}
		for _, n := range pinNeeds(asMap(j)) {
			if n == cancelJobName {
				out = append(out, name+" needs "+cancelJobName+" (it must not become a required context)")
			}
		}
		if asMap(asMap(j)["permissions"])["actions"] == "write" {
			out = append(out, name+" has actions: write; only "+cancelJobName+" may")
		}
	}
	if asMap(doc["permissions"])["actions"] == "write" {
		out = append(out, "workflow-level permissions carry actions: write")
	}
	return out
}

func TestQueueCancelEveryWorkflowCarriesTheJob(t *testing.T) {
	root := rootOf(t)
	for _, wf := range cancelEveryWorkflows {
		if d := cancelEveryDefects(loadWf(t, root, wf)); len(d) > 0 {
			t.Errorf("%s:\n%s", wf, strings.Join(d, "\n"))
		}
	}
}

func plantCancelJob(t *testing.T, root, wf, old, repl string) yamlMap {
	t.Helper()
	text := readFile(t, filepath.Join(root, ".github", "workflows", wf))
	idx := strings.Index(text, "\n  "+cancelJobName+":\n")
	if idx < 0 {
		t.Fatalf("%s: no %s job to plant a defect in", wf, cancelJobName)
	}
	head, tail := text[:idx], text[idx:]
	if !strings.Contains(tail, old) {
		t.Fatalf("%s: planted defect did not land: %q", wf, old)
	}
	return parseYAML(t, head+strings.Replace(tail, old, repl, 1), wf)
}

func TestQueueCancelEveryWorkflowNegativeControls(t *testing.T) {
	root := rootOf(t)
	dropLastNeed := regexp.MustCompile(`(?m)^    needs: \[(.*), [^,\]]+\]$`)
	plants := []struct {
		name string
		do   func(t *testing.T, wf string) yamlMap
	}{
		{"job missing", func(t *testing.T, wf string) yamlMap {
			return plantCancelJob(t, root, wf, "\n  "+cancelJobName+":\n", "\n  "+cancelJobName+"-x:\n")
		}},
		{"needs missing one job", func(t *testing.T, wf string) yamlMap {
			text := readFile(t, filepath.Join(root, ".github", "workflows", wf))
			idx := strings.Index(text, "\n  "+cancelJobName+":\n")
			if idx < 0 || !dropLastNeed.MatchString(text[idx:]) {
				t.Fatalf("%s: needs line to plant in not found", wf)
			}
			return parseYAML(t, text[:idx]+dropLastNeed.ReplaceAllString(text[idx:], "    needs: [$1]"), wf)
		}},
		{"job added later without the needs edge", func(t *testing.T, wf string) yamlMap {
			text := readFile(t, filepath.Join(root, ".github", "workflows", wf))
			return parseYAML(t, text+"\n  later-job:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo later\n", wf)
		}},
		{"another job needs the cancel job", func(t *testing.T, wf string) yamlMap {
			text := readFile(t, filepath.Join(root, ".github", "workflows", wf))
			return parseYAML(t, text+"\n  later-job:\n    needs: ["+cancelJobName+"]\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo later\n", wf)
		}},
		{"contents widened to write", func(t *testing.T, wf string) yamlMap {
			return plantCancelJob(t, root, wf, "      contents: read\n", "      contents: write\n")
		}},
		{"extra permission", func(t *testing.T, wf string) yamlMap {
			return plantCancelJob(t, root, wf, "      contents: read\n", "      contents: read\n      packages: write\n")
		}},
		{"checkout ref is the merge group sha", func(t *testing.T, wf string) yamlMap {
			return plantCancelJob(t, root, wf, "ref: "+cancelCheckoutRef, "ref: ${{ github.sha }}")
		}},
		{"checkout ref is the merge group head sha", func(t *testing.T, wf string) yamlMap {
			return plantCancelJob(t, root, wf, "ref: "+cancelCheckoutRef, "ref: ${{ github.event.merge_group.head_sha }}")
		}},
		{"persist-credentials true", func(t *testing.T, wf string) yamlMap {
			return plantCancelJob(t, root, wf, "persist-credentials: false", "persist-credentials: true")
		}},
		{"if is only failure()", func(t *testing.T, wf string) yamlMap {
			return plantCancelJob(t, root, wf, "if: "+cancelIf, "if: failure()")
		}},
		{"continue-on-error removed", func(t *testing.T, wf string) yamlMap {
			return plantCancelJob(t, root, wf, "    continue-on-error: true\n", "")
		}},
	}
	for _, wf := range cancelEveryWorkflows {
		for _, p := range plants {
			if len(cancelEveryDefects(p.do(t, wf))) == 0 {
				t.Errorf("%s: planted defect %q was not caught", wf, p.name)
			}
		}
	}
}
