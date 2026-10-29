package workflows_test

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-11795: the token a workflow gets. Every workflow carries a workflow-level permissions mapping that is exactly
// contents: read, or an allowlisted exact map with a reason; a permissions value that is not a mapping (write-all,
// read-all, a list, null) is refused at both levels; a job-level block that grants a write scope is allowlisted by its
// exact map; a registry login needs packages: write; a checkout in a job that can write persists no credentials.

type permRow struct {
	file   string
	want   map[string]string
	reason string
}

var workflowPermAllowlist = []permRow{
	{"arc-runner-image.yml", map[string]string{"contents": "read", "packages": "write"}, "the single build job publishes the runner image to ghcr; the write scope should move to that job"},
	{"auto-release.yml", map[string]string{"id-token": "write", "contents": "write", "attestations": "write"}, "the single release job signs and attests the release; id-token and attestations should move to that job"},
	{"docker-images.yml", map[string]string{"contents": "read", "packages": "write", "id-token": "write"}, "the image jobs build, push and sign; six jobs inherit the grant today, so each needs its own block before it can leave the workflow level"},
	{"freeze-provider-worlds.yml", map[string]string{"contents": "read", "packages": "read"}, "the job pulls images from ghcr"},
	{"go-quality.yml", map[string]string{"contents": "read", "packages": "read"}, "the legs pull images from ghcr; the plan job adds actions: read at job level"},
	{"go-workers-compose-proof.yml", map[string]string{"contents": "read", "packages": "read"}, "the proof pulls images from ghcr"},
	{"go.yml", map[string]string{"contents": "read", "packages": "read"}, "the jobs pull images from ghcr; two jobs carry their own blocks"},
	{"mirror-test-images.yml", map[string]string{"contents": "read", "packages": "write"}, "the mirror job pushes to ghcr; the write scope should move to that job"},
	{"venue-oracles-changed.yml", map[string]string{"contents": "read", "packages": "read"}, "the job pulls images from ghcr"},
	{"venue-oracles-main-alert.yml", map[string]string{}, "narrower than the default: the token is granted at job level only"},
	{"venue-oracles.yml", map[string]string{"contents": "read", "actions": "read", "packages": "read"}, "the legs read run state and pull images from ghcr"},
}

type jobKey struct{ file, job string }

var jobWriteAllowlist = map[jobKey]map[string]string{
	{"auto-release.yml", "tag-and-release"}:      {"contents": "write"},
	{"codeql-analysis.yml", "analyze-docs-only"}: {"actions": "read", "contents": "read", "security-events": "write"},
	{"codeql-analysis.yml", "analyze"}:           {"actions": "read", "contents": "read", "security-events": "write"},
	{"docker-images.yml", "resolve"}:             {"contents": "read", "packages": "write", "pull-requests": "read", "attestations": "read"},
	{"docker-images.yml", "go-merge"}:            {"contents": "read", "packages": "write", "id-token": "write", "attestations": "write"},
	{"docker-images.yml", "go-api-tools-merge"}:  {"contents": "read", "packages": "write", "id-token": "write", "attestations": "write"},
	{"go.yml", "cancel-stacked-queue-children"}:  {"actions": "write", "contents": "read"},
	{"security-scan.yml", "semgrep"}:             {"contents": "read", "security-events": "write"},
	{"security-scan.yml", "gitleaks"}:            {"contents": "read", "security-events": "write"},
	{"venue-oracles-main-alert.yml", "alert"}:    {"contents": "read", "actions": "read", "issues": "write"},
}

var persistAllowlist = map[jobKey]string{
	{"arc-runner-image.yml", "build"}:           "publishing job; follow-up: persist-credentials: false",
	{"auto-release.yml", "tag-and-release"}:     "pushes the release tag with the persisted token; check before changing",
	{"codeql-analysis.yml", "analyze"}:          "follow-up: persist-credentials: false",
	{"docker-images.yml", "changes"}:            "follow-up: persist-credentials: false",
	{"docker-images.yml", "resolve"}:            "follow-up: persist-credentials: false",
	{"docker-images.yml", "go-build"}:           "follow-up: persist-credentials: false",
	{"docker-images.yml", "go-build-dho-arm64"}: "follow-up: persist-credentials: false",
	{"docker-images.yml", "go-api-tools-build"}: "follow-up: persist-credentials: false",
	{"docker-images.yml", "fan-in-latest"}:      "follow-up: persist-credentials: false",
	{"security-scan.yml", "semgrep"}:            "follow-up: persist-credentials: false",
	{"security-scan.yml", "gitleaks"}:           "follow-up: persist-credentials: false",
}

var registryLogin = regexp.MustCompile(`(^|[\s;&|(])(docker|podman|buildah)\s+login(\s|$)`)

// permsOf reports the mapping of a permissions value, whether the key is present and whether it is a mapping.
func permsOf(holder yamlMap) (m map[string]string, present, isMap bool) {
	v, present := holder["permissions"]
	if !present {
		return nil, false, false
	}
	raw, ok := v.(yamlMap)
	if !ok {
		return nil, true, false
	}
	m = map[string]string{}
	for k, x := range raw {
		m[k] = fmt.Sprint(x)
	}
	return m, true, true
}

func hasWrite(m map[string]string) bool {
	for _, v := range m {
		if v == "write" {
			return true
		}
	}
	return false
}

func permissionsDefects(t *testing.T, root string, wfAllow []permRow, jobAllow map[jobKey]map[string]string, persistAllow map[jobKey]string) []string {
	t.Helper()
	var out []string
	allowed := map[string]permRow{}
	for _, r := range wfAllow {
		allowed[r.file] = r
	}
	seenWf := map[string]bool{}
	seenJobWrite := map[jobKey]bool{}
	seenPersist := map[jobKey]bool{}
	for _, name := range workflowNames(t, root) {
		seenWf[name] = true
		doc := loadWf(t, root, name)
		top, present, isMap := permsOf(doc)
		switch {
		case !present:
			out = append(out, name+": no workflow-level permissions block (the token falls back to the repository default)")
		case !isMap:
			out = append(out, fmt.Sprintf("%s: workflow-level permissions is %v, not a mapping", name, doc["permissions"]))
		default:
			if row, ok := allowed[name]; ok {
				if !reflect.DeepEqual(top, row.want) {
					out = append(out, fmt.Sprintf("%s: workflow-level permissions %v differ from the allowlisted %v", name, top, row.want))
				}
			} else if !reflect.DeepEqual(top, map[string]string{"contents": "read"}) {
				out = append(out, fmt.Sprintf("%s: workflow-level permissions %v, want exactly contents: read or an allowlist row with a reason", name, top))
			}
		}
		jobs := jobsOf(doc)
		jobNames := make([]string, 0, len(jobs))
		for j := range jobs {
			jobNames = append(jobNames, j)
		}
		sort.Strings(jobNames)
		for _, j := range jobNames {
			job := asMap(jobs[j])
			key := jobKey{name, j}
			jp, jPresent, jIsMap := permsOf(job)
			if jPresent && !jIsMap {
				out = append(out, fmt.Sprintf("%s job %s: permissions is %v, not a mapping", name, j, job["permissions"]))
			}
			if jPresent && jIsMap && hasWrite(jp) {
				seenJobWrite[key] = true
				if want, ok := jobAllow[key]; !ok {
					out = append(out, fmt.Sprintf("%s job %s: job-level permissions %v grant a write scope that is not allowlisted", name, j, jp))
				} else if !reflect.DeepEqual(jp, want) {
					out = append(out, fmt.Sprintf("%s job %s: job-level permissions %v differ from the allowlisted %v", name, j, jp, want))
				}
			}
			eff, effOK := top, isMap
			if jPresent {
				eff, effOK = jp, jIsMap
			}
			canWrite := effOK && hasWrite(eff)
			for _, s := range stepsOf(job) {
				text := shellCommands(asString(s["run"]))
				uses := strings.ToLower(asString(s["uses"]))
				if (registryLogin.MatchString(text) || strings.HasPrefix(uses, "docker/login-action@")) && !(effOK && eff["packages"] == "write") {
					out = append(out, fmt.Sprintf("%s job %s: a registry login in a job without packages: write", name, j))
				}
				if !strings.HasPrefix(uses, "actions/checkout@") || !canWrite {
					continue
				}
				if pc, ok := asMap(s["with"])["persist-credentials"].(bool); ok && !pc {
					continue
				}
				seenPersist[key] = true
				if _, ok := persistAllow[key]; !ok {
					out = append(out, fmt.Sprintf("%s job %s: checkout in a job that can write must set persist-credentials: false", name, j))
				}
			}
		}
	}
	for _, r := range wfAllow {
		if !seenWf[r.file] {
			out = append(out, r.file+": allowlisted workflow does not exist")
			continue
		}
		if top, _, isMap := permsOf(loadWf(t, root, r.file)); isMap && reflect.DeepEqual(top, map[string]string{"contents": "read"}) {
			out = append(out, r.file+": stale allowlist row, the workflow is exactly contents: read")
		}
		if strings.TrimSpace(r.reason) == "" {
			out = append(out, r.file+": allowlist row without a reason")
		}
	}
	for k := range jobAllow {
		if !seenJobWrite[k] {
			out = append(out, fmt.Sprintf("%s job %s: stale job write allowlist row, the job has no write block", k.file, k.job))
		}
	}
	for k, reason := range persistAllow {
		if !seenPersist[k] {
			out = append(out, fmt.Sprintf("%s job %s: stale persist-credentials allowlist row", k.file, k.job))
		}
		if strings.TrimSpace(reason) == "" {
			out = append(out, fmt.Sprintf("%s job %s: persist-credentials allowlist row without a reason", k.file, k.job))
		}
	}
	sort.Strings(out)
	return out
}

func allWorkflowFiles(t *testing.T, root string) []string {
	var files []string
	for _, n := range workflowNames(t, root) {
		files = append(files, ".github/workflows/"+n)
	}
	return files
}

func TestWorkflowPermissionsGateHoldsOnTheRealTree(t *testing.T) {
	d := permissionsDefects(t, rootOf(t), workflowPermAllowlist, jobWriteAllowlist, persistAllowlist)
	if len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
}

func TestWorkflowPermissionsGateRefusesEachDefectClass(t *testing.T) {
	root := rootOf(t)
	files := allWorkflowFiles(t, root)
	const (
		testWf   = ".github/workflows/test.yml"
		mirror   = ".github/workflows/mirror-test-images.yml"
		topRead  = "permissions:\n  contents: read\n"
		jobRead  = "    permissions:\n      contents: read\n    outputs:\n"
		suiteRun = "      - name: Run the precheck library suite\n"
	)
	for _, c := range []struct {
		name string
		m    mutation
		want string
	}{
		{"scalar write-all at workflow level", mutation{testWf, topRead, "permissions: write-all\n"}, "test.yml: workflow-level permissions is write-all, not a mapping"},
		{"scalar read-all at job level", mutation{testWf, jobRead, "    permissions: read-all\n    outputs:\n"}, "test.yml job changes: permissions is read-all, not a mapping"},
		{"null at workflow level", mutation{testWf, topRead, "permissions:\n"}, "test.yml: workflow-level permissions is <nil>, not a mapping"},
		{"deleted workflow-level block", mutation{testWf, topRead, ""}, "test.yml: no workflow-level permissions block"},
		{"workflow block widened", mutation{testWf, topRead, "permissions:\n  contents: write\n"}, "test.yml: workflow-level permissions map[contents:write], want exactly contents: read"},
		{"job block widened to write", mutation{testWf, jobRead, "    permissions:\n      contents: write\n    outputs:\n"}, "test.yml job changes: job-level permissions map[contents:write] grant a write scope that is not allowlisted"},
		{"registry login in a read-only job", mutation{mirror, suiteRun, "      - name: Login\n        run: docker login ghcr.io\n" + suiteRun}, "mirror-test-images.yml job precheck-lib-test: a registry login in a job without packages: write"},
		{"login action in a read-only job", mutation{mirror, suiteRun, "      - uses: docker/login-action@v4\n" + suiteRun}, "mirror-test-images.yml job precheck-lib-test: a registry login in a job without packages: write"},
	} {
		t.Run(c.name, func(t *testing.T) {
			tmp := plantTree(t, root, files, c.m)
			d := strings.Join(permissionsDefects(t, tmp, workflowPermAllowlist, jobWriteAllowlist, persistAllowlist), "\n")
			if !strings.Contains(d, c.want) {
				t.Errorf("want %q in:\n%s", c.want, d)
			}
		})
	}
	t.Run("the mirror checkout persists credentials", func(t *testing.T) {
		tmp := plantTree(t, root, files, mutation{mirror, "          persist-credentials: false\n", allMarker})
		d := strings.Join(permissionsDefects(t, tmp, workflowPermAllowlist, jobWriteAllowlist, persistAllowlist), "\n")
		if !strings.Contains(d, "mirror-test-images.yml job mirror: checkout in a job that can write must set persist-credentials: false") {
			t.Errorf("not caught:\n%s", d)
		}
	})
	t.Run("a stale or wrong allowlist row", func(t *testing.T) {
		stale := append([]permRow{{"test.yml", map[string]string{"contents": "write"}, "x"}}, workflowPermAllowlist...)
		if d := strings.Join(permissionsDefects(t, root, stale, jobWriteAllowlist, persistAllowlist), "\n"); !strings.Contains(d, "test.yml: stale allowlist row") {
			t.Errorf("stale row not caught:\n%s", d)
		}
		gone := append([]permRow{{"no-such.yml", map[string]string{}, "x"}}, workflowPermAllowlist...)
		if d := strings.Join(permissionsDefects(t, root, gone, jobWriteAllowlist, persistAllowlist), "\n"); !strings.Contains(d, "no-such.yml: allowlisted workflow does not exist") {
			t.Errorf("missing workflow not caught:\n%s", d)
		}
		wrong := append([]permRow(nil), workflowPermAllowlist...)
		wrong[0] = permRow{wrong[0].file, map[string]string{"contents": "read"}, wrong[0].reason}
		if d := strings.Join(permissionsDefects(t, root, wrong, jobWriteAllowlist, persistAllowlist), "\n"); !strings.Contains(d, "differ from the allowlisted") {
			t.Errorf("a map that differs from its row not caught:\n%s", d)
		}
		noReason := append([]permRow(nil), workflowPermAllowlist...)
		noReason[0] = permRow{noReason[0].file, noReason[0].want, " "}
		if d := strings.Join(permissionsDefects(t, root, noReason, jobWriteAllowlist, persistAllowlist), "\n"); !strings.Contains(d, "without a reason") {
			t.Errorf("a row without a reason not caught:\n%s", d)
		}
		jobs := map[jobKey]map[string]string{{"test.yml", "changes"}: {"contents": "write"}}
		for k, v := range jobWriteAllowlist {
			jobs[k] = v
		}
		if d := strings.Join(permissionsDefects(t, root, workflowPermAllowlist, jobs, persistAllowlist), "\n"); !strings.Contains(d, "stale job write allowlist row") {
			t.Errorf("stale job row not caught:\n%s", d)
		}
		pers := map[jobKey]string{{"test.yml", "changes"}: "x"}
		for k, v := range persistAllowlist {
			pers[k] = v
		}
		if d := strings.Join(permissionsDefects(t, root, workflowPermAllowlist, jobWriteAllowlist, pers), "\n"); !strings.Contains(d, "stale persist-credentials allowlist row") {
			t.Errorf("stale persist row not caught:\n%s", d)
		}
	})
}
