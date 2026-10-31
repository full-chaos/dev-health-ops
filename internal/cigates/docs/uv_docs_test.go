package docs_test

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-10466 (D6134): the docs workflows build the site through uv, with a hash-locked requirements file and no setup-python
// or pip step. `uv run --with-requirements` does not enforce hashes, so the install is its own step with --require-hashes.

var (
	uvDocsWorkflows = []string{
		".github/workflows/docs-cloudflare.yml",
		".github/workflows/docs-guards.yml",
		".github/workflows/docs-health-report.yml",
	}
	uvDocsReqs   = glue("requirements-docs", ".", "txt")
	uvSetupPin   = regexp.MustCompile(`^astral-sh/setup-uv@[0-9a-f]{40}( #.*)?$`)
	uvExactVer   = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
	uvLockHash1  = "--hash=sha256:b80b99a14bd085fcacfa15c9165f651fbb3406e66cc603abf11c5750937c992d"
	uvInstTail   = "--no-deps -r requirements-docs.txt\n"
	uvMkdocsPref = `uv run --no-project --python "$RUNNER_TEMP/docs-venv/bin/python" mkdocs build --strict`
	// The one build line: this prefix, `--config-file mkdocs.yml`, then only the existing tee tail.
	uvBuildLine = regexp.MustCompile(`^uv run --no-project --python "\$RUNNER_TEMP/docs-venv/bin/python" mkdocs build --strict --config-file mkdocs\.yml( 2>&1 \| tee( -a)? \.build/[A-Za-z0-9._-]+\.log)?$`)
	uvVenvLine  = regexp.MustCompile(`^uv venv --python ([0-9]+\.[0-9]+\.[0-9]+) "\$RUNNER_TEMP/docs-venv"$`)
	uvInstLine  = `uv pip install --python "$RUNNER_TEMP/docs-venv/bin/python" --require-hashes --no-deps -r ` + glue("requirements-docs", ".", "txt")
	// The lock grammar: `name==version` then one or more sha256 hashes, nothing else (uv emits no markers here).
	uvLockVer   = `[0-9]+(?:\.[0-9]+)*(?:[a-z]+[0-9]+)?(?:\.post[0-9]+)?`
	uvLockEntry = regexp.MustCompile(`^([A-Za-z0-9][A-Za-z0-9._-]*)==(` + uvLockVer + `)((?: --hash=sha256:[0-9a-f]{64})+)$`)
	uvLockName  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*==` + uvLockVer + `$`)
	uvHashTok   = regexp.MustCompile(`^--hash=sha256:[0-9a-f]{64}$`)
)

func uvLockFindings(d docTree) []string {
	reqs, err := d.read(uvDocsReqs)
	if err != nil {
		return []string{err.Error()}
	}
	var out []string
	entries, material := 0, false
	for _, e := range strings.Split(strings.ReplaceAll(reqs, "\\\n", " "), "\n") {
		e = strings.Join(strings.Fields(e), " ")
		if e == "" || strings.HasPrefix(e, "#") {
			continue
		}
		entries++
		tok := strings.Fields(e)
		if strings.HasPrefix(e, "-") {
			out = append(out, fmt.Sprintf("%s: entry %q carries something other than only name==version and --hash (exact name==version required)", uvDocsReqs, e))
			continue
		}
		if !uvLockName.MatchString(tok[0]) {
			out = append(out, fmt.Sprintf("%s: entry %q is not an exact name==version pin", uvDocsReqs, tok[0]))
			continue
		}
		hashes := 0
		for _, h := range tok[1:] {
			switch {
			case uvHashTok.MatchString(h):
				hashes++
			case strings.HasPrefix(strings.ToLower(h), "--hash="):
				out = append(out, fmt.Sprintf("%s: %s hash %q must be --hash=sha256:<64 lowercase hex>", uvDocsReqs, tok[0], h))
			default:
				out = append(out, fmt.Sprintf("%s: %s carries %q: only name==version and --hash lines are allowed", uvDocsReqs, tok[0], h))
			}
		}
		if hashes == 0 {
			out = append(out, fmt.Sprintf("%s: %s must carry --hash=sha256:<64 lowercase hex>", uvDocsReqs, tok[0]))
		}
		if uvLockEntry.MatchString(e) && tok[0] == "mkdocs-material==9.7.7" {
			material = true
		}
	}
	if entries < 2 {
		out = append(out, uvDocsReqs+" must pin the full transitive set, found "+fmt.Sprint(entries)+" entries")
	}
	if !material {
		out = append(out, uvDocsReqs+" must pin mkdocs-material==9.7.7")
	}
	return out
}

var (
	uvSubcommands = map[string]bool{"pip": true, "venv": true, "tool": true, "add": true, "sync": true, "run": true}
	uvPipWord     = regexp.MustCompile(`^pip[0-9.]*$`)
	uvShellSplit  = strings.NewReplacer(";", " ", "&", " ", "|", " ", "(", " ", ")", " ", "`", " ", "<", " ", ">", " ")
)

// uvWords splits a run line into words at whitespace and shell separators and strips quotes, so a hidden command (after
// `&&`, `;`, `|`) is still a word. The scan is fail closed: an echo of a forbidden command is refused too.
func uvWords(line string) []string {
	var out []string
	for _, w := range strings.Fields(uvShellSplit.Replace(line)) {
		out = append(out, strings.Trim(w, "\"'"))
	}
	return out
}

type uvLine struct {
	step int
	text string
}

func uvDocsGuard(d docTree) []string {
	out := uvLockFindings(d)
	for _, rel := range uvDocsWorkflows {
		wf, err := parseYAML(d, rel)
		if err != nil {
			out = append(out, fmt.Sprintf("%s: %v", rel, err))
			continue
		}
		if raw, err := d.read(rel); err == nil && (strings.Contains(raw, "--no-verify-hashes") || strings.Contains(raw, "UV_NO_VERIFY_HASHES")) {
			out = append(out, rel+" must not carry --no-verify-hashes or UV_NO_VERIFY_HASHES anywhere (run, env, with, comment)")
		}
		mkdocsJobs := 0
		for jobID := range asMap(wf["jobs"]) {
			steps := workflowSteps(wf, jobID)
			uvStepIdx := -1
			var uvStep map[string]any
			var lines []uvLine
			for i, s := range steps {
				uses := asString(s["uses"])
				if strings.HasPrefix(uses, "actions/setup-python@") {
					out = append(out, fmt.Sprintf("%s:%s sets up Python with setup-python: %s", rel, jobID, uses))
				}
				if strings.HasPrefix(uses, "astral-sh/setup-uv@") {
					uvStepIdx, uvStep = i, s
					if !uvSetupPin.MatchString(uses) {
						out = append(out, fmt.Sprintf("%s:%s setup-uv must be pinned to a 40-hex commit: %s", rel, jobID, uses))
					}
				}
				for _, l := range commandLines(asString(s["run"])) {
					lines = append(lines, uvLine{i, l})
				}
			}
			venvAt, instAt, buildAt := []int{}, []int{}, []int{}
			builds := false
			for n, l := range lines {
				w := uvWords(l.text)
				uvSub, mkdocs := false, false
				for i, x := range w {
					next := ""
					if i+1 < len(w) {
						next = w[i+1]
					}
					if x == "uv" && uvSubcommands[next] {
						uvSub = true
					}
					if (uvPipWord.MatchString(filepath.Base(x)) && next == "install" && (i == 0 || w[i-1] != "uv")) || (x == "-m" && next == "pip") || x == "ensurepip" || x == "uvx" {
						out = append(out, fmt.Sprintf("%s:%s runs pip or uvx outside the chain (bare pip install, -m pip, ensurepip, uvx): %q", rel, jobID, l.text))
					}
					if x == "mkdocs" || (x == "-m" && next == "mkdocs") {
						mkdocs = true
					}
				}
				builds = builds || mkdocs
				switch {
				case uvVenvLine.MatchString(l.text) && uvExactVer.MatchString(uvVenvLine.FindStringSubmatch(l.text)[1]):
					venvAt = append(venvAt, n)
				case l.text == uvInstLine:
					instAt = append(instAt, n)
				case uvBuildLine.MatchString(l.text):
					buildAt = append(buildAt, n)
				default:
					if mkdocs {
						out = append(out, fmt.Sprintf("%s:%s must build with `%s --config-file mkdocs.yml` and only the tee tail: %q", rel, jobID, uvMkdocsPref, l.text))
					}
					if uvSub {
						out = append(out, fmt.Sprintf("%s:%s uv line is not part of the chain (only the three chain lines may use uv pip, venv, tool, add, sync or run; venv needs an exact X.Y.Z python): %q", rel, jobID, l.text))
					}
				}
			}
			if !builds {
				continue
			}
			mkdocsJobs++
			for _, c := range []struct {
				name string
				at   []int
				want string
			}{{"venv", venvAt, `uv venv --python <X.Y.Z> "$RUNNER_TEMP/docs-venv"`}, {"install", instAt, uvInstLine}, {"build", buildAt, uvMkdocsPref + " --config-file mkdocs.yml"}} {
				if len(c.at) != 1 {
					out = append(out, fmt.Sprintf("%s:%s needs exactly one %s chain line, found %d: %s", rel, jobID, c.name, len(c.at), c.want))
				}
			}
			if len(venvAt) == 1 && len(instAt) == 1 && len(buildAt) == 1 && !(venvAt[0] < instAt[0] && instAt[0] < buildAt[0]) {
				out = append(out, fmt.Sprintf("%s:%s chain lines are out of order: want venv, then install, then build", rel, jobID))
			}
			if uvStep == nil {
				out = append(out, fmt.Sprintf("%s:%s runs mkdocs without a setup-uv step", rel, jobID))
				continue
			}
			if len(venvAt) == 1 && uvStepIdx > lines[venvAt[0]].step {
				out = append(out, fmt.Sprintf("%s:%s setup-uv must come before the uv venv step", rel, jobID))
			}
			with := asMap(uvStep["with"])
			if !uvExactVer.MatchString(asString(with["version"])) {
				out = append(out, fmt.Sprintf("%s:%s setup-uv version must be an exact X.Y.Z release, got %v", rel, jobID, with["version"]))
			}
			switch v := with["enable-cache"].(type) {
			case bool:
				if v {
					out = append(out, fmt.Sprintf("%s:%s setup-uv must set enable-cache: false", rel, jobID))
				}
			case string:
				if v != "false" {
					out = append(out, fmt.Sprintf("%s:%s setup-uv must set enable-cache: false (got %q)", rel, jobID, v))
				}
			default:
				out = append(out, fmt.Sprintf("%s:%s setup-uv must set enable-cache: false", rel, jobID))
			}
		}
		if mkdocsJobs == 0 {
			out = append(out, rel+" runs no mkdocs build job")
		}
	}
	return out
}

func TestDocsWorkflowsBuildThroughUv(t *testing.T) {
	const cf, gd, hr = ".github/workflows/docs-cloudflare.yml", ".github/workflows/docs-guards.yml", ".github/workflows/docs-health-report.yml"
	assertGuard(t, uvDocsGuard, []plantCase{
		{"setup-python returns", func(t *testing.T, d docTree) docTree {
			return d.edit(t, hr, "      - name: Set up uv\n", "      - uses: actions/setup-python@v6\n      - name: Set up uv\n")
		}, "setup-python"},
		{"a bare pip install returns", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, "          uv pip install ", "          pip install ")
		}, "bare pip install"},
		{"the install loses --require-hashes", func(t *testing.T, d docTree) docTree {
			return d.edit(t, gd, "--require-hashes ", "")
		}, "chain line"},
		{"the install loses --no-deps", func(t *testing.T, d docTree) docTree {
			return d.edit(t, hr, "--no-deps ", "")
		}, "chain line"},
		{"mkdocs runs without uv", func(t *testing.T, d docTree) docTree {
			return d.edit(t, gd, uvMkdocsPref, "python -m mkdocs build --strict")
		}, "must build with"},
		{"mkdocs runs with --with-requirements", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, uvMkdocsPref, "uv run --no-project --with-requirements "+uvDocsReqs+" mkdocs build --strict")
		}, "must build with"},
		{"setup-uv is unpinned", func(t *testing.T, d docTree) docTree {
			return d.edit(t, hr, "astral-sh/setup-uv@1c37ad07a6a961277cf70c0d37d6f313000f5884 # v10.3.0", "astral-sh/setup-uv@v10")
		}, "40-hex"},
		{"setup-uv pins no uv version", func(t *testing.T, d docTree) docTree {
			return d.edit(t, gd, "          version: 0.12.9\n", "")
		}, "exact X.Y.Z"},
		{"setup-uv caches", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, "          enable-cache: false\n", "")
		}, "enable-cache: false"},
		{"the lock gains an unhashed pin", func(t *testing.T, d docTree) docTree {
			return d.appended(t, uvDocsReqs, "\nunhashed==1.0\n")
		}, "carry --hash=sha256:"},
		{"the lock shrinks to one pin", func(t *testing.T, d docTree) docTree {
			return d.added(uvDocsReqs, "mkdocs-material==9.7.7 --hash=sha256:00\n")
		}, "full transitive set"},
		// CHAOS-10466 r1 classes: lock grammar, exact versions, one linked command chain, enable-cache spellings.
		{"every lock pin becomes a wildcard", func(t *testing.T, d docTree) docTree {
			return d.editAll(t, uvDocsReqs, "==", "==9.*")
		}, "exact name==version"},
		{"a lock pin becomes a range", func(t *testing.T, d docTree) docTree {
			return d.edit(t, uvDocsReqs, "\nbabel==2.18.0 \\\n", "\nbabel>=2.18.0 \\\n")
		}, "exact name==version"},
		{"the lock loses mkdocs-material 9.7.7", func(t *testing.T, d docTree) docTree {
			return d.edit(t, uvDocsReqs, "\nmkdocs-material==9.7.7 \\\n", "\nmkdocs-material==9.7.6 \\\n")
		}, "mkdocs-material==9.7.7"},
		{"a lock hash is bogus", func(t *testing.T, d docTree) docTree {
			return d.edit(t, uvDocsReqs, uvLockHash1, "--hash=sha256:bogus")
		}, "64 lowercase hex"},
		{"a lock hash has 63 hex digits", func(t *testing.T, d docTree) docTree {
			return d.edit(t, uvDocsReqs, uvLockHash1, uvLockHash1[:len(uvLockHash1)-1])
		}, "64 lowercase hex"},
		{"a lock hash is uppercase", func(t *testing.T, d docTree) docTree {
			return d.edit(t, uvDocsReqs, uvLockHash1, strings.ToUpper(uvLockHash1[:len("--hash=sha256:")])+strings.ToUpper(uvLockHash1[len("--hash=sha256:"):]))
		}, "64 lowercase hex"},
		{"the lock gains an index url", func(t *testing.T, d docTree) docTree {
			return d.appended(t, uvDocsReqs, "--index-url https://example.invalid/simple\n")
		}, "only name==version and --hash"},
		{"the lock gains a url requirement", func(t *testing.T, d docTree) docTree {
			return d.appended(t, uvDocsReqs, "evil @ https://example.invalid/evil.whl --hash=sha256:"+strings.Repeat("0", 64)+"\n")
		}, "exact name==version"},
		{"the lock gains an include", func(t *testing.T, d docTree) docTree {
			return d.appended(t, uvDocsReqs, "-r other.txt\n")
		}, "exact name==version"},
		{"setup-uv version is latest", func(t *testing.T, d docTree) docTree {
			return d.edit(t, hr, "          version: 0.12.9\n", "          version: latest\n")
		}, "exact X.Y.Z"},
		{"setup-uv version is a range", func(t *testing.T, d docTree) docTree {
			return d.edit(t, gd, "          version: 0.12.9\n", "          version: '>=0.12.9'\n")
		}, "exact X.Y.Z"},
		{"setup-uv version is 0.12", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, "          version: 0.12.9\n", "          version: '0.12'\n")
		}, "exact X.Y.Z"},
		{"the venv python is 3.14", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, "uv venv --python 3.14.7 ", "uv venv --python 3.14 ")
		}, "exact X.Y.Z"},
		{"the install loses --python", func(t *testing.T, d docTree) docTree {
			return d.edit(t, gd, `uv pip install --python "$RUNNER_TEMP/docs-venv/bin/python" --require-hashes`, "uv pip install --require-hashes")
		}, "chain line"},
		{"the venv step is missing", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, "          uv venv --python 3.14.7 \"$RUNNER_TEMP/docs-venv\"\n", "")
		}, "chain line"},
		{"the install reads a .bak file", func(t *testing.T, d docTree) docTree {
			return d.edit(t, gd, "-r requirements-docs.txt", "-r requirements-docs.txt.bak")
		}, "chain line"},
		{"the chain is reordered", func(t *testing.T, d docTree) docTree {
			venv := "          uv venv --python 3.14.7 \"$RUNNER_TEMP/docs-venv\"\n"
			inst := "          uv pip install --python \"$RUNNER_TEMP/docs-venv/bin/python\" --require-hashes --no-deps -r requirements-docs.txt\n"
			return d.edit(t, hr, venv+inst, inst+venv)
		}, "order"},
		{"a second uv pip install is added", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, "--no-deps -r requirements-docs.txt\n", "--no-deps -r requirements-docs.txt\n          uv pip install --python \"$RUNNER_TEMP/docs-venv/bin/python\" requests\n")
		}, "not part of the chain"},
		{"a second uv venv is added", func(t *testing.T, d docTree) docTree {
			return d.edit(t, gd, "--no-deps -r requirements-docs.txt\n", "--no-deps -r requirements-docs.txt\n          uv venv --python 3.14.7 other\n")
		}, "not part of the chain"},
		{"the build line carries an extra flag", func(t *testing.T, d docTree) docTree {
			return d.edit(t, hr, "mkdocs build --strict --config-file mkdocs.yml", "mkdocs build --strict --config-file mkdocs.yml --no-directory-urls")
		}, "chain line"},
		{"enable-cache is true", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, "enable-cache: false", "enable-cache: true")
		}, "enable-cache"},
		{"enable-cache is the string true", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, "enable-cache: false", `enable-cache: "true"`)
		}, "enable-cache"},
		// CHAOS-10466 r2 tightening: a word scan over every run line, fail closed.
		{"an and-list hides a uv pip install", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, uvInstTail, uvInstTail+"          true && uv pip install x\n")
		}, "not part of the chain"},
		{"an and-list hides a uv venv", func(t *testing.T, d docTree) docTree {
			return d.edit(t, gd, uvInstTail, uvInstTail+"          cd . && uv venv y\n")
		}, "not part of the chain"},
		{"the venv pip runs by path", func(t *testing.T, d docTree) docTree {
			return d.edit(t, hr, uvInstTail, uvInstTail+"          \"$RUNNER_TEMP/docs-venv/bin/pip\" install x\n")
		}, "bare pip install"},
		{"python -m pip runs", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, uvInstTail, uvInstTail+"          python -m pip install x\n")
		}, "bare pip install"},
		{"ensurepip runs", func(t *testing.T, d docTree) docTree {
			return d.edit(t, gd, uvInstTail, uvInstTail+"          python -m ensurepip\n")
		}, "ensurepip"},
		{"uvx runs", func(t *testing.T, d docTree) docTree {
			return d.edit(t, hr, uvInstTail, uvInstTail+"          uvx foo\n")
		}, "uvx"},
		{"uv tool install runs", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, uvInstTail, uvInstTail+"          uv tool install foo\n")
		}, "not part of the chain"},
		{"uv add runs", func(t *testing.T, d docTree) docTree {
			return d.edit(t, gd, uvInstTail, uvInstTail+"          uv add foo\n")
		}, "not part of the chain"},
		{"uv sync runs", func(t *testing.T, d docTree) docTree {
			return d.edit(t, hr, uvInstTail, uvInstTail+"          uv sync\n")
		}, "not part of the chain"},
		{"uv run --with runs python", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, uvInstTail, uvInstTail+"          uv run --with x python -c 1\n")
		}, "not part of the chain"},
		{"an echo of a uv pip install is refused (fail closed)", func(t *testing.T, d docTree) docTree {
			return d.edit(t, gd, uvInstTail, uvInstTail+"          echo uv pip install x\n")
		}, "not part of the chain"},
		{"UV_NO_VERIFY_HASHES is set", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, "      - name: Set up uv\n", "      - name: Set up uv\n        env:\n          UV_NO_VERIFY_HASHES: '1'\n")
		}, "NO_VERIFY_HASHES"},
		{"--no-verify-hashes appears in a comment", func(t *testing.T, d docTree) docTree {
			return d.appended(t, hr, "# uv pip install --no-verify-hashes\n")
		}, "no-verify-hashes"},
		{"enable-cache is False", func(t *testing.T, d docTree) docTree {
			return d.edit(t, cf, "enable-cache: false", `enable-cache: "False"`)
		}, "enable-cache"},
	})
}

// A disabled cache has two valid YAML spellings; the guard accepts both.
func TestDocsWorkflowsUvAcceptsQuotedFalse(t *testing.T) {
	d := realTree(t).edit(t, ".github/workflows/docs-cloudflare.yml", "enable-cache: false", `enable-cache: "false"`)
	if got := uvDocsGuard(d); len(got) > 0 {
		t.Fatalf("enable-cache: \"false\" is refused:\n%s", strings.Join(got, "\n"))
	}
}
