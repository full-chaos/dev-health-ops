package affected

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	goWorkflowPath      = ".github/workflows/go.yml"
	goQualityWorkflow   = ".github/workflows/go-quality.yml"
	checkGoPath         = "ci/check_go.sh"
	shardManifestDir    = "ci/go_integration_shards.d/"
	raceWeightsDir      = "ci/go_race_weights.d/"
	integrationJobsNote = "integration job(s) changed: "
)

// Inputs of the plan and of the integration shard runner: a change here can move every shard or every leg.
var (
	runnerFiles    = map[string]bool{"ci/go_relevant_diff.sh": true, shardManifestDir + "_shards.tsv": true}
	runnerPrefixes = []string{"ci/lib/"}
	// Inputs of the go-quality legs and of their package partition: FULL for the go-quality plan (--go-files-full) only.
	qualityRunnerFiles = map[string]bool{
		goQualityWorkflow: true, "ci/go_test_weights.tsv": true, "ci/go_race_solo.tsv": true,
		"ci/race_excluded_tests.tsv": true, "ci/go_race_shard.awk": true, "ci/go_race_missing_rows.awk": true,
		"ci/go_providersync_race_weights.tsv": true, "ci/go_providersync_race_shard.awk": true,
	}
	qualityRunnerPrefixes = []string{raceWeightsDir}
	// Each of these splits the tests of ONE integration package into shards.
	splitTestManifests = map[string]string{
		"ci/go_providersync_test_shards.tsv":    "internal/providersync",
		"ci/go_daily_test_shards.tsv":           "internal/jobs/metrics/daily",
		"ci/go_workerservice_test_shards.tsv":   "internal/workerservice",
		"ci/go_queryapi_server_test_shards.tsv": "internal/queryapi/server",
		"ci/go_remaining_test_shards.tsv":       "internal/jobs/metrics/remaining",
	}
)

type planRules struct {
	root, base string
	keys       map[string]bool
	quality    bool // the go-quality legs' plan: their inputs and their check_go.sh verbs count too

	headWorkflow    *integrationWorkflow
	headWorkflowErr error
	headLoaded      bool
}

// classifyInput decides a changed path that is an input of the plan or of the runners. A non-empty why makes the
// change set FULL; keys are planned packages the path selects directly. The class rules still apply to the path.
func (r *planRules) classifyInput(p string) (why string, keys []string) {
	if runnerFiles[p] || hasAnyPrefix(p, runnerPrefixes) {
		return "runner input", nil
	}
	if r.quality && (qualityRunnerFiles[p] || hasAnyPrefix(p, qualityRunnerPrefixes)) {
		return "go-quality leg input", nil
	}
	if key, ok := splitTestManifests[p]; ok {
		if !r.keys[key] {
			return "split test manifest for a package that is not planned", nil
		}
		return "", []string{key}
	}
	if strings.HasPrefix(p, shardManifestDir) {
		if !strings.HasSuffix(p, ".tsv") || strings.Contains(strings.TrimPrefix(p, shardManifestDir), "/") {
			return "", nil
		}
		key := strings.ReplaceAll(strings.TrimSuffix(path.Base(p), ".tsv"), "__", "/")
		if r.keys[key] {
			return "", []string{key}
		}
		return "", nil
	}
	if p == goWorkflowPath {
		return r.workflowChange(), nil
	}
	if p == checkGoPath {
		return r.checkGoChange(), nil
	}
	if strings.HasPrefix(p, ".github/") {
		wf, err := r.head()
		if err != nil {
			return goWorkflowPath + " unreadable at head: " + err.Error(), nil
		}
		for _, u := range wf.localUses {
			if p == u || strings.HasPrefix(p, u+"/") {
				return "local action of an integration job", nil
			}
		}
	}
	return "", nil
}

func hasAnyPrefix(p string, prefixes []string) bool {
	for _, pre := range prefixes {
		if strings.HasPrefix(p, pre) {
			return true
		}
	}
	return false
}

// baseFile returns the content of p at the merge base of --base and HEAD; ok=false when the file is absent there.
func (r *planRules) baseFile(p string) ([]byte, bool, error) {
	if r.base == "" {
		return nil, false, errors.New("no --base to compare against")
	}
	mb, err := exec.Command("git", "-C", r.root, "merge-base", r.base, "HEAD").Output()
	if err != nil {
		return nil, false, fmt.Errorf("git merge-base %s HEAD: %w", r.base, err)
	}
	spec := strings.TrimSpace(string(mb)) + ":" + p
	if err := exec.Command("git", "-C", r.root, "cat-file", "-e", spec).Run(); err != nil {
		return nil, false, nil
	}
	out, err := exec.Command("git", "-C", r.root, "show", spec).Output()
	if err != nil {
		return nil, false, fmt.Errorf("git show %s: %w", spec, err)
	}
	return out, true, nil
}

func (r *planRules) headFile(p string) ([]byte, error) {
	return os.ReadFile(filepath.Join(r.root, filepath.FromSlash(p)))
}

// integrationWorkflow is what of go.yml decides how the integration shards run.
type integrationWorkflow struct {
	runKeys   map[string]any
	jobs      map[string]any
	verbs     map[string]bool
	localUses []string
}

var (
	checkGoVerb = regexp.MustCompile(`ci/check_go\.sh\s+([A-Za-z0-9_-]+)`)
	planMarker  = regexp.MustCompile(`ci/check_go\.sh\s+integration|plan-verdict`)
)

func parseIntegrationWorkflow(raw []byte) (*integrationWorkflow, error) {
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	all, _ := doc["jobs"].(map[string]any)
	wf := &integrationWorkflow{runKeys: map[string]any{}, jobs: map[string]any{}, verbs: map[string]bool{}}
	for _, k := range []string{"env", "defaults", "permissions"} {
		wf.runKeys[k] = doc[k]
	}
	for name, job := range all {
		if planMarker.MatchString(jobRunText(job)) {
			wf.jobs[name] = job
		}
	}
	if len(wf.jobs) == 0 {
		return nil, errors.New("no job runs the integration plan or shards")
	}
	for grew := true; grew; {
		grew = false
		for name, job := range all {
			if _, in := wf.jobs[name]; in {
				continue
			}
			for _, need := range jobNeeds(job) {
				if _, in := wf.jobs[need]; in {
					wf.jobs[name] = job
					grew = true
					break
				}
			}
		}
	}
	for _, job := range wf.jobs {
		for _, m := range checkGoVerb.FindAllStringSubmatch(jobRunText(job), -1) {
			wf.verbs[m[1]] = true
		}
		jm, _ := job.(map[string]any)
		uses := []any{jm["uses"]}
		steps, _ := jm["steps"].([]any)
		for _, st := range steps {
			if sm, ok := st.(map[string]any); ok {
				uses = append(uses, sm["uses"])
			}
		}
		for _, u := range uses {
			if s, ok := u.(string); ok && strings.HasPrefix(s, "./") {
				wf.localUses = append(wf.localUses, path.Clean(strings.TrimPrefix(s, "./")))
			}
		}
	}
	return wf, nil
}

func jobRunText(job any) string {
	jm, _ := job.(map[string]any)
	steps, _ := jm["steps"].([]any)
	var b strings.Builder
	for _, st := range steps {
		if sm, ok := st.(map[string]any); ok {
			if run, ok := sm["run"].(string); ok {
				b.WriteString(run)
				b.WriteByte('\n')
			}
		}
	}
	return b.String()
}

func jobNeeds(job any) []string {
	jm, _ := job.(map[string]any)
	switch n := jm["needs"].(type) {
	case string:
		return []string{n}
	case []any:
		var out []string
		for _, v := range n {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func (r *planRules) head() (*integrationWorkflow, error) {
	if !r.headLoaded {
		r.headLoaded = true
		raw, err := r.headFile(goWorkflowPath)
		if err == nil {
			r.headWorkflow, err = parseIntegrationWorkflow(raw)
		}
		r.headWorkflowErr = err
	}
	return r.headWorkflow, r.headWorkflowErr
}

// workflowChange is empty when the go.yml change leaves every integration job and the workflow-wide run settings
// as they were at the merge base; otherwise it says what moved.
func (r *planRules) workflowChange() string {
	headWF, err := r.head()
	if err != nil {
		return "unreadable at head: " + err.Error()
	}
	raw, ok, err := r.baseFile(goWorkflowPath)
	if err != nil {
		return err.Error()
	}
	if !ok {
		return "absent at the merge base"
	}
	baseWF, err := parseIntegrationWorkflow(raw)
	if err != nil {
		return "unreadable at the merge base: " + err.Error()
	}
	for _, k := range []string{"env", "defaults", "permissions"} {
		if !reflect.DeepEqual(baseWF.runKeys[k], headWF.runKeys[k]) {
			return "top-level " + k + " changed"
		}
	}
	names := map[string]bool{}
	for n := range baseWF.jobs {
		names[n] = true
	}
	for n := range headWF.jobs {
		names[n] = true
	}
	var moved []string
	for n := range names {
		if !reflect.DeepEqual(baseWF.jobs[n], headWF.jobs[n]) {
			moved = append(moved, n)
		}
	}
	if len(moved) > 0 {
		sort.Strings(moved)
		return integrationJobsNote + strings.Join(moved, ",")
	}
	return ""
}

// checkGoChange is empty when no part of ci/check_go.sh that the integration verbs of go.yml execute changed.
func (r *planRules) checkGoChange() string {
	wf, err := r.head()
	if err != nil {
		return goWorkflowPath + " unreadable at head: " + err.Error()
	}
	headRaw, err := r.headFile(checkGoPath)
	if err != nil {
		return "unreadable at head: " + err.Error()
	}
	baseRaw, ok, err := r.baseFile(checkGoPath)
	if err != nil {
		return err.Error()
	}
	if !ok {
		return "absent at the merge base"
	}
	head, err := parseShellScript(string(headRaw))
	if err != nil {
		return "unparsable at head: " + err.Error()
	}
	base, err := parseShellScript(string(baseRaw))
	if err != nil {
		return "unparsable at the merge base: " + err.Error()
	}
	verbs := map[string]bool{}
	if r.quality {
		if verbs, err = r.qualityVerbs(); err != nil {
			return goQualityWorkflow + " unreadable at head: " + err.Error()
		}
	}
	for v := range wf.verbs {
		verbs[v] = true
	}
	return shellIntegrationChange(base, head, verbs)
}

// qualityVerbs is every check_go.sh verb a job of go-quality.yml runs (ci-leg reaches the unit, race and static
// stages); an absent workflow has none.
func (r *planRules) qualityVerbs() (map[string]bool, error) {
	verbs := map[string]bool{}
	raw, err := r.headFile(goQualityWorkflow)
	if errors.Is(err, fs.ErrNotExist) {
		return verbs, nil
	}
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	jobs, _ := doc["jobs"].(map[string]any)
	for _, job := range jobs {
		for _, m := range checkGoVerb.FindAllStringSubmatch(jobRunText(job), -1) {
			verbs[m[1]] = true
		}
	}
	return verbs, nil
}

type shellScript struct {
	funcs   map[string]string
	assigns map[string]string
	other   []string
	arms    map[string]string
}

var (
	shellFunc     = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\(\)\s*\{\s*$`)
	shellAssign   = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)=(.*)$`)
	shellDispatch = regexp.MustCompile(`^case "\$\{1`)
	shellArm      = regexp.MustCompile(`^  ([^ \t()]+)\)(.*)$`)
	shellWord     = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)
)

func shellCode(line string) bool {
	t := strings.TrimSpace(line)
	return t != "" && !strings.HasPrefix(t, "#")
}

// parseShellScript splits a script into its top-level functions, its column-0 variable assignments, its top-level
// dispatch arms and the rest of its global code. Comment and blank lines are dropped, so editing them changes nothing.
func parseShellScript(src string) (*shellScript, error) {
	s := &shellScript{funcs: map[string]string{}, assigns: map[string]string{}, arms: map[string]string{}}
	lines := strings.Split(src, "\n")
	dispatch, dispatchSeen, arm := false, false, ""
	for i := 0; i < len(lines); i++ {
		l := lines[i]
		if dispatch {
			if l == "esac" {
				dispatch, dispatchSeen, arm = false, true, ""
				continue
			}
			if m := shellArm.FindStringSubmatch(l); m != nil {
				arm = m[1]
				if _, dup := s.arms[arm]; dup {
					return nil, fmt.Errorf("dispatch arm %q repeats", arm)
				}
				s.arms[arm] = strings.TrimSpace(m[2])
				continue
			}
			if !shellCode(l) {
				continue
			}
			if arm == "" {
				return nil, fmt.Errorf("line %d: code before the first dispatch arm", i+1)
			}
			s.arms[arm] += "\n" + l
			continue
		}
		if m := shellFunc.FindStringSubmatch(l); m != nil {
			var body []string
			closed := false
			for i++; i < len(lines); i++ {
				if lines[i] == "}" {
					closed = true
					break
				}
				if shellCode(lines[i]) {
					body = append(body, lines[i])
				}
			}
			if !closed {
				return nil, fmt.Errorf("function %s is not closed", m[1])
			}
			if _, dup := s.funcs[m[1]]; dup {
				return nil, fmt.Errorf("function %s is defined twice", m[1])
			}
			s.funcs[m[1]] = strings.Join(body, "\n")
			continue
		}
		if shellDispatch.MatchString(l) {
			if dispatchSeen {
				return nil, errors.New("more than one top-level dispatch")
			}
			dispatch = true
			continue
		}
		if m := shellAssign.FindStringSubmatch(l); m != nil {
			def := []string{l}
			if strings.TrimSpace(m[2]) == "(" {
				closed := false
				for i++; i < len(lines); i++ {
					if shellCode(lines[i]) {
						def = append(def, lines[i])
					}
					if lines[i] == ")" {
						closed = true
						break
					}
				}
				if !closed {
					return nil, fmt.Errorf("array %s is not closed", m[1])
				}
			}
			s.assigns[m[1]] = strings.Join(def, "\n")
			continue
		}
		if shellCode(l) {
			s.other = append(s.other, l)
		}
	}
	if dispatch {
		return nil, errors.New("the dispatch is not closed")
	}
	if !dispatchSeen {
		return nil, errors.New("no top-level dispatch")
	}
	return s, nil
}

// armRuns reports whether a dispatch arm can take one of the verbs; a glob pattern is assumed to.
func armRuns(arm string, verbs map[string]bool) bool {
	for _, alt := range strings.Split(arm, "|") {
		if verbs[alt] || strings.ContainsAny(alt, "*?[") {
			return true
		}
	}
	return false
}

// shellReach returns the functions the verbs' arms and the global code call, transitively, and the text they run.
func shellReach(s *shellScript, verbs map[string]bool) (map[string]bool, string) {
	var text []string
	for arm, body := range s.arms {
		if armRuns(arm, verbs) {
			text = append(text, body)
		}
	}
	text = append(text, s.other...)
	for _, def := range s.assigns {
		text = append(text, def)
	}
	reach := map[string]bool{}
	todo := shellWord.FindAllString(strings.Join(text, "\n"), -1)
	for len(todo) > 0 {
		w := todo[len(todo)-1]
		todo = todo[:len(todo)-1]
		body, ok := s.funcs[w]
		if !ok || reach[w] {
			continue
		}
		reach[w] = true
		text = append(text, body)
		todo = append(todo, shellWord.FindAllString(body, -1)...)
	}
	return reach, strings.Join(text, "\n")
}

func shellIntegrationChange(base, head *shellScript, verbs map[string]bool) string {
	if len(verbs) == 0 {
		return "go.yml names no check_go.sh verb for its integration jobs"
	}
	if !reflect.DeepEqual(base.other, head.other) {
		return "global code changed"
	}
	for arm := range unionKeys(base.arms, head.arms) {
		if armRuns(arm, verbs) && base.arms[arm] != head.arms[arm] {
			return "dispatch arm " + arm + " changed"
		}
	}
	reachB, _ := shellReach(base, verbs)
	reachH, _ := shellReach(head, verbs)
	var moved []string
	for f := range unionKeys(base.funcs, head.funcs) {
		if (reachB[f] || reachH[f]) && base.funcs[f] != head.funcs[f] {
			moved = append(moved, f)
		}
	}
	if len(moved) > 0 {
		sort.Strings(moved)
		return "integration function(s) changed: " + strings.Join(moved, ",")
	}
	usedB, usedH := shellUsedGlobals(base, verbs), shellUsedGlobals(head, verbs)
	for v := range unionKeys(base.assigns, head.assigns) {
		if (usedB[v] || usedH[v]) && base.assigns[v] != head.assigns[v] {
			moved = append(moved, v)
		}
	}
	if len(moved) > 0 {
		sort.Strings(moved)
		return "global(s) the integration verbs read changed: " + strings.Join(moved, ",")
	}
	return ""
}

// shellUsedGlobals: the column-0 globals named in the reachable text, followed through the definitions of the ones named.
func shellUsedGlobals(s *shellScript, verbs map[string]bool) map[string]bool {
	reach, _ := shellReach(s, verbs)
	var text []string
	for arm, body := range s.arms {
		if armRuns(arm, verbs) {
			text = append(text, body)
		}
	}
	text = append(text, s.other...)
	for f := range reach {
		text = append(text, s.funcs[f])
	}
	used := map[string]bool{}
	todo := shellWord.FindAllString(strings.Join(text, "\n"), -1)
	for len(todo) > 0 {
		w := todo[len(todo)-1]
		todo = todo[:len(todo)-1]
		def, ok := s.assigns[w]
		if !ok || used[w] {
			continue
		}
		used[w] = true
		todo = append(todo, shellWord.FindAllString(strings.TrimPrefix(def, w+"="), -1)...)
	}
	return used
}

func unionKeys(a, b map[string]string) map[string]bool {
	out := map[string]bool{}
	for k := range a {
		out[k] = true
	}
	for k := range b {
		out[k] = true
	}
	return out
}
