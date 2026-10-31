package docs_test

import (
	"fmt"
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
	uvLockHash1  = "--hash=sha256:b80b99a14bd085fcacfa15c9165f651fbb3406e66cc603abf11c5750937c992d"
	uvMkdocsPref = `uv run --no-project --python "$RUNNER_TEMP/docs-venv/bin/python" mkdocs build --strict`
)

func uvDocsGuard(d docTree) []string {
	var out []string
	for _, rel := range uvDocsWorkflows {
		wf, err := parseYAML(d, rel)
		if err != nil {
			out = append(out, fmt.Sprintf("%s: %v", rel, err))
			continue
		}
		mkdocsJobs := 0
		for jobID := range asMap(wf["jobs"]) {
			steps := workflowSteps(wf, jobID)
			var uvStep map[string]any
			installed, builds := false, false
			for _, s := range steps {
				uses := asString(s["uses"])
				if strings.HasPrefix(uses, "actions/setup-python@") {
					out = append(out, fmt.Sprintf("%s:%s sets up Python with setup-python: %s", rel, jobID, uses))
				}
				if strings.HasPrefix(uses, "astral-sh/setup-uv@") {
					uvStep = s
					if !uvSetupPin.MatchString(uses) {
						out = append(out, fmt.Sprintf("%s:%s setup-uv must be pinned to a 40-hex commit: %s", rel, jobID, uses))
					}
				}
				for _, line := range commandLines(asString(s["run"])) {
					f := strings.Fields(line)
					mkSeen := false
					for i, w := range f {
						if (w == "pip" || w == "pip3") && i+1 < len(f) && f[i+1] == "install" && (i == 0 || f[i-1] != "uv") {
							out = append(out, fmt.Sprintf("%s:%s runs a bare pip install: %q", rel, jobID, line))
						}
						if !mkSeen && (w == "mkdocs" || (w == "-m" && i+1 < len(f) && f[i+1] == "mkdocs")) {
							mkSeen = true
							builds = true
							if !strings.HasPrefix(line, uvMkdocsPref) || !strings.Contains(line, "--config-file mkdocs.yml") {
								out = append(out, fmt.Sprintf("%s:%s must build with `%s ...`: %q", rel, jobID, uvMkdocsPref, line))
							}
						}
					}
					if strings.HasPrefix(line, "uv pip install") && strings.Contains(line, "--require-hashes") &&
						strings.Contains(line, "--no-deps") && strings.Contains(line, "-r "+uvDocsReqs) {
						installed = true
					}
				}
			}
			if !builds {
				continue
			}
			mkdocsJobs++
			if uvStep == nil {
				out = append(out, fmt.Sprintf("%s:%s runs mkdocs without a setup-uv step", rel, jobID))
			} else {
				with := asMap(uvStep["with"])
				if asString(with["version"]) == "" {
					out = append(out, fmt.Sprintf("%s:%s setup-uv must pin a uv version", rel, jobID))
				}
				if v, ok := with["enable-cache"].(bool); !ok || v {
					out = append(out, fmt.Sprintf("%s:%s setup-uv must set enable-cache: false", rel, jobID))
				}
			}
			if !installed {
				out = append(out, fmt.Sprintf("%s:%s must run `uv pip install --require-hashes --no-deps -r %s`", rel, jobID, uvDocsReqs))
			}
		}
		if mkdocsJobs == 0 {
			out = append(out, rel+" runs no mkdocs build job")
		}
	}
	reqs, err := d.read(uvDocsReqs)
	if err != nil {
		return append(out, err.Error())
	}
	entries := 0
	for _, e := range strings.Split(strings.ReplaceAll(reqs, "\\\n", " "), "\n") {
		e = strings.TrimSpace(e)
		if e == "" || strings.HasPrefix(e, "#") {
			continue
		}
		entries++
		if !strings.Contains(e, "==") || !strings.Contains(e, "--hash=sha256:") {
			out = append(out, uvDocsReqs+": every requirement must be pinned with == and carry --hash=sha256: (got "+strings.Fields(e)[0]+")")
		}
	}
	if entries < 2 {
		out = append(out, uvDocsReqs+" must pin the full transitive set, found "+fmt.Sprint(entries)+" entries")
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
		}, "--require-hashes"},
		{"the install loses --no-deps", func(t *testing.T, d docTree) docTree {
			return d.edit(t, hr, "--no-deps ", "")
		}, "--no-deps"},
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
		}, "pin a uv version"},
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
