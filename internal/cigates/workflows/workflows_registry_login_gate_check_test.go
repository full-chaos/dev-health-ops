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
	loginTools    = setOf("docker", "podman", "buildah", "helm", "oras", "crane", "skopeo")
	loginWrappers = setOf("sudo", "env", "command", "exec", "time", "nice", "nohup")
	loginOptValue = map[string]map[string]bool{"sudo": setOf("-u", "-g"), "env": setOf("-u"), "exec": setOf("-a"), "nice": setOf("-n")}
	loginReserved = setOf("if", "then", "else", "elif", "fi", "while", "until", "do", "done", "!")
	loginShells   = setOf("bash", "sh", "dash", "zsh")
	loginEnvWord  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*=`)
	loginPinned   = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

func isLoginScriptPath(w string) bool { return strings.HasSuffix(w, "ghcr_login.sh") }

// matchClose returns the index just past the close that matches an open at s[i-1]: a ")" for "$(" (nested "$(" and quotes
// counted) or the next backtick. An unclosed substitution returns len(s).
func matchClose(s string, i int, backtick bool) (inner string, next int) {
	depth := 1
	for j := i; j < len(s); j++ {
		switch c := s[j]; {
		case backtick && c == '`':
			return s[i:j], j + 1
		case backtick && c == '\\':
			j++
		case backtick:
		case c == '\'':
			if k := strings.IndexByte(s[j+1:], '\''); k >= 0 {
				j += k + 1
			}
		case c == '(':
			depth++
		case c == ')':
			if depth--; depth == 0 {
				return s[i:j], j + 1
			}
		}
	}
	return s[i:], len(s)
}

// shellSegments splits shell text into the word lists of its commands. Separators inside quotes do not split; $(...) and
// backtick bodies become segments of their own; leading reserved words are stripped from each segment.
func shellSegments(text string) [][]string {
	s := strings.ReplaceAll(shellCode(text), "\\\n", " ")
	var out [][]string
	var words []string
	var cur strings.Builder
	haveWord := false
	flushWord := func() {
		if haveWord {
			words = append(words, cur.String())
		}
		cur.Reset()
		haveWord = false
	}
	flushSeg := func() {
		flushWord()
		for len(words) > 0 && loginReserved[words[0]] {
			words = words[1:]
		}
		if len(words) > 0 {
			out = append(out, words)
		}
		words = nil
	}
	sub := func(i int, backtick bool) int {
		inner, next := matchClose(s, i, backtick)
		out = append(out, shellSegments(inner)...)
		return next
	}
	ownWord := func(i int) bool {
		return cur.Len() == 0 && !haveWord && (i+1 >= len(s) || strings.ContainsRune(" \t\n;", rune(s[i+1])))
	}
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '\\' && i+1 < len(s):
			cur.WriteByte(s[i+1])
			haveWord = true
			i += 2
		case c == '\'':
			haveWord = true
			if k := strings.IndexByte(s[i+1:], '\''); k >= 0 {
				cur.WriteString(s[i+1 : i+1+k])
				i += k + 2
			} else {
				cur.WriteString(s[i+1:])
				i = len(s)
			}
		case c == '"':
			haveWord = true
			i++
			for i < len(s) && s[i] != '"' {
				switch {
				case s[i] == '\\' && i+1 < len(s):
					cur.WriteByte(s[i+1])
					i += 2
				case s[i] == '$' && i+1 < len(s) && s[i+1] == '(':
					i = sub(i+2, false)
				case s[i] == '`':
					i = sub(i+1, true)
				default:
					cur.WriteByte(s[i])
					i++
				}
			}
			i++
		case c == '$' && i+1 < len(s) && s[i+1] == '(':
			haveWord = true
			i = sub(i+2, false)
		case c == '`':
			haveWord = true
			i = sub(i+1, true)
		case c == ' ' || c == '\t':
			flushWord()
			i++
		case c == '\n' || c == ';' || c == '(' || c == ')':
			flushSeg()
			i++
		case c == '&' || c == '|':
			flushSeg()
			i++
			if i < len(s) && s[i] == c {
				i++
			}
		case (c == '{' || c == '}') && ownWord(i):
			flushSeg()
			i++
		default:
			cur.WriteByte(c)
			haveWord = true
			i++
		}
	}
	flushSeg()
	return out
}

// commandWord returns the index of the command word of a segment after env assignments, wrapper words and their options.
func commandWord(words []string) int {
	i := 0
	for i < len(words) {
		w := words[i]
		switch {
		case loginEnvWord.MatchString(w):
			i++
		case loginWrappers[w]:
			i++
			for i < len(words) && strings.HasPrefix(words[i], "-") {
				opt := words[i]
				i++
				if loginOptValue[w][opt] && i < len(words) {
					i++
				}
			}
		default:
			return i
		}
	}
	return i
}

// segmentLogin classifies one segment: "login" (a direct login), "call" (a direct call of the login script), "indirect"
// (eval, source, . or sh -c holding a login or a call of the script), or "".
func segmentLogin(words []string) string {
	i := commandWord(words)
	if i >= len(words) {
		return ""
	}
	cmd := words[i]
	base := filepath.Base(cmd)
	rest := words[i+1:]
	switch {
	case loginTools[base]:
		for _, w := range rest {
			if w == "login" {
				return "login"
			}
		}
	case isLoginScriptPath(cmd):
		return "call"
	case loginShells[base]:
		for j := 0; j < len(rest); j++ {
			w := rest[j]
			if !strings.HasPrefix(w, "-") {
				if isLoginScriptPath(w) {
					return "call"
				}
				break
			}
			if !strings.HasPrefix(w, "--") && strings.Contains(w, "c") && j+1 < len(rest) {
				if len(shellLoginKinds(rest[j+1])) > 0 {
					return "indirect"
				}
				break
			}
		}
	case cmd == "eval":
		if len(shellLoginKinds(strings.Join(rest, " "))) > 0 {
			return "indirect"
		}
	case cmd == "source" || cmd == ".":
		if len(rest) > 0 && isLoginScriptPath(rest[0]) {
			return "indirect"
		}
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
			if k == "call" {
				continue
			}
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
				if strings.TrimSpace(run) == loginScriptRun {
					got.script++
					continue
				}
				for _, k := range shellLoginKinds(run) {
					if k == "call" {
						out = append(out, where+": run calls "+loginScript+" in a form other than exactly `"+loginScriptRun+"`")
						continue
					}
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
