package workflows_test

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-11849: a registry login happens only through ONE pinned docker/login-action (registry ghcr.io) or ONE script line
// (`bash ci/lib/ghcr_login.sh`), only in a job whose effective permissions carry packages, and the census below names every
// such job. The shell check is one command-position test used for ci/*.sh and for workflow run: text: split on | || && ;,
// look at the first word of each segment. Quoted separators may refuse a harmless line: the safe direction. The contract is
// bounded to the mistakes of trusted authors, not hostile text.

const (
	loginScript    = "ci/lib/ghcr_login.sh"
	loginScriptRun = "bash " + loginScript
)

type loginCount struct{ action, script int }

var jobLoginCensus = map[jobKey]loginCount{
	{"arc-runner-image.yml", "build"}:                            {1, 0},
	{"docker-images.yml", "resolve"}:                             {1, 0},
	{"docker-images.yml", "go-build"}:                            {1, 1},
	{"docker-images.yml", "go-build-dho-arm64"}:                  {1, 1},
	{"docker-images.yml", "go-merge"}:                            {1, 0},
	{"docker-images.yml", "go-api-tools-build"}:                  {1, 1},
	{"docker-images.yml", "go-api-tools-merge"}:                  {1, 0},
	{"docker-images.yml", "fan-in-latest"}:                       {1, 0},
	{"freeze-provider-worlds.yml", "freeze"}:                     {1, 0},
	{"go-quality.yml", "go-quality-leg"}:                         {1, 1},
	{"go-workers-compose-proof.yml", "go-workers-compose-proof"}: {1, 0},
	{"go.yml", "go-storage-integration-shard"}:                   {1, 1},
	{"go.yml", "go-golden-drift"}:                                {1, 0},
	{"go.yml", "go-auth-compose-proof"}:                          {1, 1},
	{"go.yml", "go-auth-web-proxy-proof"}:                        {1, 1},
	{"mirror-test-images.yml", "mirror"}:                         {1, 0},
	{"venue-oracles-changed.yml", "venue-oracles-changed"}:       {1, 0},
	{"venue-oracles.yml", "venue-oracles"}:                       {1, 0},
}

var (
	loginTools   = setOf("docker", "podman", "buildah", "helm", "oras", "crane", "skopeo")
	loginWrapper = setOf("sudo", "env", "command", "exec")
	loginEnvWord = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*=`)
	loginSplit   = regexp.MustCompile(`\|\||&&|\||;`)
	loginNamed   = regexp.MustCompile(`ghcr_login\.sh`)
	loginPinned  = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

func unquoteWord(w string) string { return strings.Trim(w, `"'`) }

func shellSegments(text string) [][]string {
	text = strings.ReplaceAll(shellCode(text), "\\\n", " ")
	var out [][]string
	for _, line := range strings.Split(text, "\n") {
		for _, seg := range loginSplit.Split(line, -1) {
			var words []string
			for _, w := range strings.Fields(seg) {
				words = append(words, unquoteWord(w))
			}
			if len(words) > 0 {
				out = append(out, words)
			}
		}
	}
	return out
}

func hasLoginWord(words []string) bool {
	tool := false
	for _, w := range words {
		if loginTools[w] {
			tool = true
		}
		if tool && w == "login" {
			return true
		}
	}
	return false
}

// segmentLogin classifies one segment: "login" (a direct login), "indirect" (eval/source/. /sh -c holding a login or the
// login script), or "".
func segmentLogin(words []string) string {
	i := 0
	for i < len(words) && (loginEnvWord.MatchString(words[i]) || loginWrapper[words[i]]) {
		i++
	}
	if i >= len(words) {
		return ""
	}
	first := filepath.Base(words[i])
	rest := words[i+1:]
	if loginTools[first] {
		for _, w := range rest {
			if w == "login" {
				return "login"
			}
		}
		return ""
	}
	indirect := first == "eval" || first == "source" || words[i] == "."
	if first == "bash" || first == "sh" {
		for _, w := range rest {
			indirect = indirect || w == "-c" || (strings.HasPrefix(w, "-") && !strings.HasPrefix(w, "--") && strings.Contains(w, "c"))
		}
	}
	if indirect && (hasLoginWord(words) || loginNamed.MatchString(strings.Join(words, " "))) {
		return "indirect"
	}
	return ""
}

func shellLoginKinds(text string) (kinds []string) {
	for _, seg := range shellSegments(text) {
		if k := segmentLogin(seg); k != "" {
			kinds = append(kinds, k)
		}
	}
	return kinds
}

func ciScripts(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(filepath.Join(root, "ci"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, ".sh") && !strings.HasSuffix(p, "_test.sh") {
			rel, _ := filepath.Rel(root, p)
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func effectivePackages(doc, job yamlMap) string {
	holder := doc
	if _, ok := job["permissions"]; ok {
		holder = job
	}
	m, _, isMap := permsOf(holder)
	if !isMap {
		return ""
	}
	return m["packages"]
}

func registryLoginDefects(t *testing.T, root string, census map[jobKey]loginCount) []string {
	t.Helper()
	var out []string
	scripts := ciScripts(t, root)
	var withLogin []string
	for _, s := range scripts {
		for _, k := range shellLoginKinds(readFile(t, filepath.Join(root, s))) {
			if k == "indirect" {
				out = append(out, fmt.Sprintf("%s: an indirect registry login (eval, source, . or sh -c holding a login or %s)", s, loginScript))
			} else if len(withLogin) == 0 || withLogin[len(withLogin)-1] != s {
				withLogin = append(withLogin, s)
			}
		}
	}
	if len(withLogin) != 1 || withLogin[0] != loginScript {
		out = append(out, fmt.Sprintf("ci scripts with a registry login are %v, want exactly [%s]", withLogin, loginScript))
	}
	seen := map[jobKey]bool{}
	for _, name := range workflowNames(t, root) {
		doc := loadWf(t, root, name)
		jobs := jobsOf(doc)
		jobNames := make([]string, 0, len(jobs))
		for j := range jobs {
			jobNames = append(jobNames, j)
		}
		sort.Strings(jobNames)
		for _, j := range jobNames {
			job := asMap(jobs[j])
			key := jobKey{name, j}
			var got loginCount
			for i, s := range stepsOf(job) {
				where := fmt.Sprintf("%s job %s step %d", name, j, i+1)
				if uses := asString(s["uses"]); strings.HasPrefix(strings.ToLower(uses), dockerLoginAction) {
					got.action++
					if _, ref, _ := strings.Cut(uses, "@"); !loginPinned.MatchString(ref) {
						out = append(out, where+": docker/login-action is not pinned to a full commit sha")
					}
					if reg := withString(s, "registry"); reg != "ghcr.io" {
						out = append(out, fmt.Sprintf("%s: docker/login-action registry is %q, want ghcr.io (an unset registry is Docker Hub)", where, reg))
					}
				}
				run := asString(s["run"])
				if run == "" {
					continue
				}
				switch {
				case strings.TrimSpace(run) == loginScriptRun:
					got.script++
				case loginNamed.MatchString(shellCode(run)):
					out = append(out, where+": run names "+loginScript+" in a form other than exactly `"+loginScriptRun+"`")
				}
				for _, k := range shellLoginKinds(run) {
					out = append(out, fmt.Sprintf("%s: run holds a %s registry login; logins go through docker/login-action or `%s` only", where, k, loginScriptRun))
				}
			}
			if got == (loginCount{}) {
				if _, ok := census[key]; ok {
					seen[key] = true
					out = append(out, fmt.Sprintf("%s job %s: stale login census row, the job has no login step", name, j))
				}
				continue
			}
			if p := effectivePackages(doc, job); p != "read" && p != "write" {
				out = append(out, fmt.Sprintf("%s job %s: logs in to a registry without packages scope (effective packages %q)", name, j, p))
			}
			want, ok := census[key]
			seen[key] = true
			switch {
			case !ok:
				out = append(out, fmt.Sprintf("%s job %s: login steps (action %d, script %d) are not in the login census", name, j, got.action, got.script))
			case want != got:
				out = append(out, fmt.Sprintf("%s job %s: login steps are action %d script %d, census says action %d script %d", name, j, got.action, got.script, want.action, want.script))
			}
		}
	}
	for k := range census {
		if !seen[k] {
			out = append(out, fmt.Sprintf("%s job %s: stale login census row, no such job", k.file, k.job))
		}
	}
	sort.Strings(out)
	return out
}
