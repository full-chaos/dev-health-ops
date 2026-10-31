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
	})
}
