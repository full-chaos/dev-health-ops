package workflows_test

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// CHAOS-11795: the token a workflow gets. Every workflow carries a workflow-level permissions mapping that is exactly
// contents: read, or an allowlisted exact map with a reason; a permissions value that is not a mapping (write-all,
// read-all, a list, null) is refused at both levels; the effective map of every job (its block, else the workflow map) is
// exactly contents: read or an allowlisted exact map with a reason; a checkout in a job that can write persists no credentials.

type permRow struct {
	file   string
	want   map[string]string
	reason string
}

var workflowPermAllowlist = []permRow{
	{"freeze-provider-worlds.yml", map[string]string{"contents": "read", "packages": "read"}, "the job pulls images from ghcr"},
	{"go-quality.yml", map[string]string{"contents": "read", "packages": "read"}, "the legs pull images from ghcr; the plan job adds actions: read at job level"},
	{"go-workers-compose-proof.yml", map[string]string{"contents": "read", "packages": "read"}, "the proof pulls images from ghcr"},
	{"go.yml", map[string]string{"contents": "read", "packages": "read"}, "the jobs pull images from ghcr; two jobs carry their own blocks"},
	{"venue-oracles-changed.yml", map[string]string{"contents": "read", "packages": "read"}, "the job pulls images from ghcr"},
	{"venue-oracles-main-alert.yml", map[string]string{}, "narrower than the default: the token is granted at job level only"},
	{"venue-oracles.yml", map[string]string{"contents": "read", "actions": "read", "packages": "read"}, "the legs read run state and pull images from ghcr"},
}

type jobKey struct{ file, job string }

// jobPermRow: the exact effective permissions of a job that are not exactly contents: read, and why. The job name "*" is
// the row of every job of the workflow that has no permissions block of its own: it is valid only while at least one such
// job exists and every one of them inherits exactly this map.
type jobPermRow struct {
	want   map[string]string
	reason string
}

var jobPermAllowlist = map[jobKey]jobPermRow{
	{"arc-runner-image.yml", "build"}:              {map[string]string{"contents": "read", "packages": "write"}, "pushes the runner image to ghcr"},
	{"auto-release.yml", "tag-and-release"}:        {map[string]string{"contents": "write"}, "creates the release tag and the release"},
	{"codeql-analysis.yml", "analyze-docs-only"}:   {map[string]string{"actions": "read", "contents": "read", "security-events": "write"}, "uploads the code scanning result of the docs-only twin"},
	{"codeql-analysis.yml", "analyze"}:             {map[string]string{"actions": "read", "contents": "read", "security-events": "write"}, "uploads the code scanning result"},
	{"docker-images.yml", "resolve"}:               {map[string]string{"contents": "read", "packages": "write", "pull-requests": "read", "attestations": "read"}, "reads the promotion state (pull requests, attestations) and writes the lookup package"},
	{"docker-images.yml", "go-build"}:              {map[string]string{"contents": "read", "packages": "write"}, "pushes the platform image by digest to ghcr"},
	{"docker-images.yml", "go-build-dho-arm64"}:    {map[string]string{"contents": "read", "packages": "write"}, "pushes the arm64 dho image by digest to ghcr"},
	{"docker-images.yml", "go-api-tools-build"}:    {map[string]string{"contents": "read", "packages": "write"}, "pushes the platform go-api-tools images by digest to ghcr"},
	{"docker-images.yml", "fan-in-latest"}:         {map[string]string{"contents": "read", "packages": "write"}, "applies the moving tags in ghcr"},
	{"docker-images.yml", "go-merge"}:              {map[string]string{"contents": "read", "packages": "write", "id-token": "write", "attestations": "write"}, "merges the multi-arch image, signs and attests it"},
	{"docker-images.yml", "go-api-tools-merge"}:    {map[string]string{"contents": "read", "packages": "write", "id-token": "write", "attestations": "write"}, "merges the multi-arch image, signs and attests it"},
	{"go-quality.yml", "go-quality-plan"}:          {map[string]string{"contents": "read", "packages": "read", "actions": "read"}, "lists runs and pulls images to plan the legs"},
	{"go.yml", "go-storage-integration-plan"}:      {map[string]string{"contents": "read", "actions": "read"}, "lists runs for the integration shard plan"},
	{"go.yml", "go-storage-integration"}:           {map[string]string{"actions": "read"}, "reads run state and nothing else"},
	{"go.yml", "cancel-stacked-queue-children"}:    {map[string]string{"actions": "write", "contents": "read"}, "cancels the runs stacked on a red queue entry"},
	{"governance.yml", "governance"}:               {map[string]string{"contents": "read", "pull-requests": "read"}, "reads pull requests for the governance gate"},
	{"live-e2e.yml", "live-e2e-run"}:               {map[string]string{"contents": "read", "packages": "read"}, "pulls images from ghcr"},
	{"live-e2e.yml", "metrics-executed-proof-run"}: {map[string]string{"contents": "read", "packages": "read"}, "pulls images from ghcr"},
	{"mirror-test-images.yml", "precheck"}:         {map[string]string{"contents": "read", "packages": "read"}, "reads the mirror packages, writes nothing"},
	{"mirror-test-images.yml", "mirror"}:           {map[string]string{"contents": "read", "packages": "write"}, "publishes the mirrored images to ghcr"},
	{"mirror-test-images.yml", "dry-run"}:          {map[string]string{"contents": "read", "packages": "read"}, "the read-only dry run of the mirror"},
	{"security-scan.yml", "changes"}:               {map[string]string{"contents": "read", "pull-requests": "read"}, "reads pull requests for the change filter"},
	{"security-scan.yml", "semgrep"}:               {map[string]string{"contents": "read", "security-events": "write"}, "uploads the code scanning result"},
	{"security-scan.yml", "gitleaks"}:              {map[string]string{"contents": "read", "security-events": "write"}, "uploads the code scanning result"},
	{"venue-oracles-main-alert.yml", "alert"}:      {map[string]string{"contents": "read", "actions": "read", "issues": "write"}, "opens the issue for a red main run"},
	{"freeze-provider-worlds.yml", "*"}:            {map[string]string{"contents": "read", "packages": "read"}, "the jobs without a block inherit the workflow-level grant (see its workflow row); each must keep inheriting exactly this map"},
	{"go-quality.yml", "*"}:                        {map[string]string{"contents": "read", "packages": "read"}, "the jobs without a block inherit the workflow-level grant (see its workflow row); each must keep inheriting exactly this map"},
	{"go-workers-compose-proof.yml", "*"}:          {map[string]string{"contents": "read", "packages": "read"}, "the jobs without a block inherit the workflow-level grant (see its workflow row); each must keep inheriting exactly this map"},
	{"go.yml", "*"}:                                {map[string]string{"contents": "read", "packages": "read"}, "the jobs without a block inherit the workflow-level grant (see its workflow row); each must keep inheriting exactly this map"},
	{"venue-oracles-changed.yml", "*"}:             {map[string]string{"contents": "read", "packages": "read"}, "the jobs without a block inherit the workflow-level grant (see its workflow row); each must keep inheriting exactly this map"},
	{"venue-oracles.yml", "*"}:                     {map[string]string{"contents": "read", "actions": "read", "packages": "read"}, "the jobs without a block inherit the workflow-level grant (see its workflow row); each must keep inheriting exactly this map"},
}

// wildcardJobs: for each wildcard row, the exact set of jobs without a block of their own that inherit the workflow map. A new
// inheriting job is not approved by the row; it needs an entry here (or a block and a row of its own), and an entry whose job is
// gone, has its own block or no longer inherits is refused.
var wildcardJobs = map[string][]string{
	"freeze-provider-worlds.yml":   {"freeze"},
	"go-quality.yml":               {"go-quality-leg", "go-quality"},
	"go-workers-compose-proof.yml": {"go-workers-compose-proof"},
	"go.yml":                       {"go-arm64-numeric-parity", "dind-smoke-test", "go-storage-integration-shard", "go-golden-drift", "go-container-build", "go-container-smoke", "go-container-security", "go-container-reproducibility", "go-container-reproducibility-self-hosted", "go-auth-compose-proof", "go-auth-web-proxy-proof", "graphql-wire-parity", "go-queue-gate"},
	"venue-oracles-changed.yml":    {"venue-oracles-changed"},
	"venue-oracles.yml":            {"venue-oracles", "auth-corpus-external"},
}

var persistAllowlist = map[jobKey]string{
	{"auto-release.yml", "tag-and-release"}: "git push origin $TAG at auto-release.yml:59 pushes the release tag with the persisted checkout token",
}

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

func permissionsDefects(t *testing.T, root string, wfAllow []permRow, jobAllow map[jobKey]jobPermRow, inherit map[string][]string, persistAllow map[jobKey]string) []string {
	t.Helper()
	var out []string
	allowed := map[string]permRow{}
	for _, r := range wfAllow {
		allowed[r.file] = r
	}
	seenWf := map[string]bool{}
	seenJob := map[jobKey]bool{}
	inheriting := map[string]map[string]bool{}
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
			eff, effOK := top, isMap
			if jPresent {
				eff, effOK = jp, jIsMap
			}
			if effOK && !reflect.DeepEqual(eff, map[string]string{"contents": "read"}) {
				row, ok := jobAllow[key]
				switch {
				case ok:
					seenJob[key] = true
				case !jPresent:
					if row, ok = jobAllow[jobKey{name, "*"}]; ok {
						seenJob[jobKey{name, "*"}] = true
						if inheriting[name] == nil {
							inheriting[name] = map[string]bool{}
						}
						inheriting[name][j] = true
						listed := false
						for _, l := range inherit[name] {
							listed = listed || l == j
						}
						if !listed {
							out = append(out, fmt.Sprintf("%s job %s: inherits effective permissions %v but is not in the job list of its wildcard row", name, j, eff))
						}
					}
				}
				switch {
				case !ok:
					out = append(out, fmt.Sprintf("%s job %s: effective permissions %v are not exactly contents: read and have no allowlist row", name, j, eff))
				case !reflect.DeepEqual(eff, row.want):
					out = append(out, fmt.Sprintf("%s job %s: effective permissions %v differ from the allowlisted %v", name, j, eff, row.want))
				}
			}
			canWrite := effOK && hasWrite(eff)
			for _, s := range stepsOf(job) {
				uses := strings.ToLower(asString(s["uses"]))
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
	files := make([]string, 0, len(inherit))
	for f := range inherit {
		files = append(files, f)
	}
	sort.Strings(files)
	for _, f := range files {
		if _, ok := jobAllow[jobKey{f, "*"}]; !ok {
			out = append(out, f+": a job list without a wildcard row")
		}
		for _, j := range inherit[f] {
			if !inheriting[f][j] {
				out = append(out, fmt.Sprintf("%s: the wildcard row lists job %s, which has no inheriting job of that name", f, j))
			}
		}
	}
	for k := range jobAllow {
		if k.job == "*" && len(inherit[k.file]) == 0 {
			out = append(out, k.file+": a wildcard row without a job list")
		}
	}
	for k, row := range jobAllow {
		if !seenJob[k] {
			out = append(out, fmt.Sprintf("%s job %s: stale job allowlist row, no job has these effective permissions under this key", k.file, k.job))
		}
		if strings.TrimSpace(row.reason) == "" {
			out = append(out, fmt.Sprintf("%s job %s: job allowlist row without a reason", k.file, k.job))
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
	d := permissionsDefects(t, rootOf(t), workflowPermAllowlist, jobPermAllowlist, wildcardJobs, persistAllowlist)
	if len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
}

func TestWorkflowPermissionsGateRefusesEachDefectClass(t *testing.T) {
	root := rootOf(t)
	files := allWorkflowFiles(t, root)
	const (
		testWf  = ".github/workflows/test.yml"
		mirror  = ".github/workflows/mirror-test-images.yml"
		topRead = "permissions:\n  contents: read\n"
		jobRead = "    permissions:\n      contents: read\n    outputs:\n"
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
		{"workflow block gains a scope", mutation{testWf, topRead, "permissions:\n  contents: read\n  packages: write\n"}, "test.yml: workflow-level permissions map[contents:read packages:write], want exactly contents: read"},
		{"a job adds a read scope", mutation{testWf, jobRead, "    permissions:\n      contents: read\n      packages: read\n    outputs:\n"}, "test.yml job changes: effective permissions map[contents:read packages:read] are not exactly contents: read and have no allowlist row"},
		{"a job without a block inherits a non-read workflow map", mutation{testWf, topRead, "permissions:\n  contents: read\n  packages: read\n"}, "test.yml job test: effective permissions map[contents:read packages:read] are not exactly contents: read and have no allowlist row"},
		{"a new job inherits a workflow-level read grant", mutation{".github/workflows/go-quality.yml", "jobs:\n", "jobs:\n  unreviewed-job:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n"}, "go-quality.yml job unreviewed-job: inherits effective permissions map[contents:read packages:read] but is not in the job list of its wildcard row"},
		{"a listed job is renamed away", mutation{".github/workflows/go-workers-compose-proof.yml", "jobs:\n  go-workers-compose-proof:\n", "jobs:\n  go-workers-compose-proof-renamed:\n"}, "go-workers-compose-proof.yml: the wildcard row lists job go-workers-compose-proof, which has no inheriting job of that name"},
		{"job block widened to write", mutation{testWf, jobRead, "    permissions:\n      contents: write\n    outputs:\n"}, "test.yml job changes: effective permissions map[contents:write] are not exactly contents: read and have no allowlist row"},
	} {
		t.Run(c.name, func(t *testing.T) {
			tmp := plantTree(t, root, files, c.m)
			d := strings.Join(permissionsDefects(t, tmp, workflowPermAllowlist, jobPermAllowlist, wildcardJobs, persistAllowlist), "\n")
			if !strings.Contains(d, c.want) {
				t.Errorf("want %q in:\n%s", c.want, d)
			}
		})
	}
	t.Run("a workflow-level write grant that was moved to its jobs comes back", func(t *testing.T) {
		const wf = ".github/workflows/"
		for _, c := range []struct {
			name string
			m    mutation
			want string
		}{
			{"arc-runner-image", mutation{wf + "arc-runner-image.yml", "permissions:\n  contents: read\n\njobs:\n", "permissions:\n  contents: read\n  packages: write\n\njobs:\n"}, "arc-runner-image.yml: workflow-level permissions map[contents:read packages:write], want exactly contents: read or an allowlist row with a reason"},
			{"auto-release", mutation{wf + "auto-release.yml", "permissions:\n  contents: read\non:\n", "permissions:\n  id-token: write\n  contents: write\n  attestations: write\non:\n"}, "auto-release.yml: workflow-level permissions map[attestations:write contents:write id-token:write], want exactly contents: read or an allowlist row with a reason"},
			{"docker-images", mutation{wf + "docker-images.yml", "permissions:\n  contents: read\n\njobs:\n", "permissions:\n  contents: read\n  packages: write\n  id-token: write\n\njobs:\n"}, "docker-images.yml: workflow-level permissions map[contents:read id-token:write packages:write], want exactly contents: read or an allowlist row with a reason"},
		} {
			tmp := plantTree(t, root, files, c.m)
			if d := strings.Join(permissionsDefects(t, tmp, workflowPermAllowlist, jobPermAllowlist, wildcardJobs, persistAllowlist), "\n"); !strings.Contains(d, c.want) {
				t.Errorf("%s: want %q in:\n%s", c.name, c.want, d)
			}
		}
	})
	t.Run("a checkout that persists credentials comes back on a fixed job", func(t *testing.T) {
		const wf = ".github/workflows/"
		for _, c := range []struct {
			file string
			jobs []string
		}{
			{"arc-runner-image.yml", []string{"build"}},
			{"codeql-analysis.yml", []string{"analyze"}},
			{"docker-images.yml", []string{"resolve", "go-build", "go-build-dho-arm64", "go-api-tools-build", "fan-in-latest"}},
			{"security-scan.yml", []string{"semgrep", "gitleaks"}},
		} {
			tmp := plantTree(t, root, files, mutation{wf + c.file, "          persist-credentials: false\n", allMarker + "          lfs: false\n"})
			d := strings.Join(permissionsDefects(t, tmp, workflowPermAllowlist, jobPermAllowlist, wildcardJobs, persistAllowlist), "\n")
			for _, j := range c.jobs {
				if want := c.file + " job " + j + ": checkout in a job that can write must set persist-credentials: false"; !strings.Contains(d, want) {
					t.Errorf("want %q in:\n%s", want, d)
				}
			}
		}
	})
	t.Run("the mirror checkout persists credentials", func(t *testing.T) {
		tmp := plantTree(t, root, files, mutation{mirror, "          persist-credentials: false\n", allMarker})
		d := strings.Join(permissionsDefects(t, tmp, workflowPermAllowlist, jobPermAllowlist, wildcardJobs, persistAllowlist), "\n")
		if !strings.Contains(d, "mirror-test-images.yml job mirror: checkout in a job that can write must set persist-credentials: false") {
			t.Errorf("not caught:\n%s", d)
		}
	})
	t.Run("a stale or wrong allowlist row", func(t *testing.T) {
		stale := append([]permRow{{"test.yml", map[string]string{"contents": "write"}, "x"}}, workflowPermAllowlist...)
		if d := strings.Join(permissionsDefects(t, root, stale, jobPermAllowlist, wildcardJobs, persistAllowlist), "\n"); !strings.Contains(d, "test.yml: stale allowlist row") {
			t.Errorf("stale row not caught:\n%s", d)
		}
		gone := append([]permRow{{"no-such.yml", map[string]string{}, "x"}}, workflowPermAllowlist...)
		if d := strings.Join(permissionsDefects(t, root, gone, jobPermAllowlist, wildcardJobs, persistAllowlist), "\n"); !strings.Contains(d, "no-such.yml: allowlisted workflow does not exist") {
			t.Errorf("missing workflow not caught:\n%s", d)
		}
		wrong := append([]permRow(nil), workflowPermAllowlist...)
		wrong[0] = permRow{wrong[0].file, map[string]string{"contents": "read"}, wrong[0].reason}
		if d := strings.Join(permissionsDefects(t, root, wrong, jobPermAllowlist, wildcardJobs, persistAllowlist), "\n"); !strings.Contains(d, "differ from the allowlisted") {
			t.Errorf("a map that differs from its row not caught:\n%s", d)
		}
		noReason := append([]permRow(nil), workflowPermAllowlist...)
		noReason[0] = permRow{noReason[0].file, noReason[0].want, " "}
		if d := strings.Join(permissionsDefects(t, root, noReason, jobPermAllowlist, wildcardJobs, persistAllowlist), "\n"); !strings.Contains(d, "without a reason") {
			t.Errorf("a row without a reason not caught:\n%s", d)
		}
		withJob := func(k jobKey, row jobPermRow) map[jobKey]jobPermRow {
			m := map[jobKey]jobPermRow{}
			for kk, v := range jobPermAllowlist {
				m[kk] = v
			}
			m[k] = row
			return m
		}
		for name, c := range map[string]struct {
			rows map[jobKey]jobPermRow
			want string
		}{
			"a job row for a job whose effective map is contents: read": {withJob(jobKey{"test.yml", "changes"}, jobPermRow{map[string]string{"contents": "write"}, "x"}), "stale job allowlist row"},
			"a job row for a job that is gone":                          {withJob(jobKey{"test.yml", "no-such-job"}, jobPermRow{map[string]string{"contents": "write"}, "x"}), "stale job allowlist row"},
			"a wildcard row for a workflow that has no inheriting job":  {withJob(jobKey{"test.yml", "*"}, jobPermRow{map[string]string{"contents": "write"}, "x"}), "stale job allowlist row"},
			"a job row without a reason":                                {withJob(jobKey{"mirror-test-images.yml", "mirror"}, jobPermRow{map[string]string{"contents": "read", "packages": "write"}, " "}), "job allowlist row without a reason"},
			"a wildcard row without a reason":                           {withJob(jobKey{"go.yml", "*"}, jobPermRow{map[string]string{"contents": "read", "packages": "read"}, ""}), "job allowlist row without a reason"},
			"a job row whose map differs from the effective map":        {withJob(jobKey{"mirror-test-images.yml", "mirror"}, jobPermRow{map[string]string{"contents": "read", "packages": "read"}, "x"}), "differ from the allowlisted"},
			"a wildcard row whose map differs from the inherited map":   {withJob(jobKey{"go.yml", "*"}, jobPermRow{map[string]string{"contents": "read"}, "x"}), "differ from the allowlisted"},
		} {
			if d := strings.Join(permissionsDefects(t, root, workflowPermAllowlist, c.rows, wildcardJobs, persistAllowlist), "\n"); !strings.Contains(d, c.want) {
				t.Errorf("%s not caught (want %q):\n%s", name, c.want, d)
			}
		}
		withList := func(f string, jobs []string) map[string][]string {
			m := map[string][]string{}
			for k, v := range wildcardJobs {
				m[k] = v
			}
			if jobs == nil {
				delete(m, f)
			} else {
				m[f] = jobs
			}
			return m
		}
		for name, c := range map[string]struct {
			inherit map[string][]string
			want    string
		}{
			"a listed job that does not exist":            {withList("go.yml", append(append([]string(nil), wildcardJobs["go.yml"]...), "no-such-job")), "go.yml: the wildcard row lists job no-such-job, which has no inheriting job of that name"},
			"a job list for a workflow without a row":     {withList("test.yml", []string{"changes"}), "test.yml: a job list without a wildcard row"},
			"a wildcard row without a job list":           {withList("go.yml", nil), "go.yml: a wildcard row without a job list"},
			"an inheriting job missing from the job list": {withList("go.yml", wildcardJobs["go.yml"][1:]), "is not in the job list of its wildcard row"},
		} {
			if d := strings.Join(permissionsDefects(t, root, workflowPermAllowlist, jobPermAllowlist, c.inherit, persistAllowlist), "\n"); !strings.Contains(d, c.want) {
				t.Errorf("%s not caught (want %q):\n%s", name, c.want, d)
			}
		}
		pers := map[jobKey]string{{"test.yml", "changes"}: "x"}
		for k, v := range persistAllowlist {
			pers[k] = v
		}
		if d := strings.Join(permissionsDefects(t, root, workflowPermAllowlist, jobPermAllowlist, wildcardJobs, pers), "\n"); !strings.Contains(d, "stale persist-credentials allowlist row") {
			t.Errorf("stale persist row not caught:\n%s", d)
		}
	})
}
