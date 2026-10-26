// Package docsbuild builds the MkDocs site and prepares the static tree Wrangler uploads. It is the Go port of the former
// scripts/build_docs_cloudflare.py: the same chain, in the same order, with the same flags.
package docsbuild

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/cigates/docsprepare"
	"github.com/full-chaos/dev-health-ops/internal/cigates/docstools"
)

const (
	siteDir          = ".build/docs"
	assetDir         = ".build/docs-cloudflare"
	redirects        = "docs-data/redirects.tsv"
	searchAcceptance = "docs-data/search-acceptance.json"
	usage            = "usage: cigate docs-build --mode {preview,production} [--full-check]"
)

var requiredAssets = []string{"index.html", "404.html", "_headers", "_redirects", "robots.txt", "cloudflare-build-manifest.json"}

// Deps are the side effects of a build: the cigate verbs, the prepare step, the MkDocs call and the revision lookup.
type Deps struct {
	Docs     func(args []string) int
	Prepare  func(args []string) int
	MkDocs   func() error
	Revision func() string
}

// OSDeps runs the verbs in this process and MkDocs as a tool call, `python3 -m mkdocs build --strict --config-file
// mkdocs.yml`, the command the former script ran with its own interpreter. DOCS_PYTHON names another interpreter.
func OSDeps(stdout, stderr io.Writer) Deps {
	return Deps{
		Docs:    func(args []string) int { return docstools.Run(args, stdout, stderr) },
		Prepare: func(args []string) int { return docsprepare.Run(args, stdout, stderr) },
		MkDocs: func() error {
			python := os.Getenv("DOCS_PYTHON")
			if python == "" {
				python = "python3"
			}
			cmd := exec.Command(python, "-m", "mkdocs", "build", "--strict", "--config-file", "mkdocs.yml")
			cmd.Stdout, cmd.Stderr = stdout, stderr
			return cmd.Run()
		},
		Revision: sourceRevision,
	}
}

func sourceRevision() string {
	if sha := strings.TrimSpace(os.Getenv("GITHUB_SHA")); sha != "" {
		return sha
	}
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// Run is the `cigate docs-build` entry point. Exit 1 on a failed step, 2 on a usage error.
func Run(args []string, stdout, stderr io.Writer) int {
	return run(args, OSDeps(stdout, stderr), stdout, stderr)
}

func run(args []string, deps Deps, stdout, stderr io.Writer) int {
	mode, fullCheck, problem := parseArgs(args)
	if problem != "" {
		fmt.Fprintf(stderr, "%s\ndocs-build: error: %s\n", usage, problem)
		return 2
	}
	// Production is never prepared without the full publication gate.
	fullCheck = fullCheck || mode == "production"
	if err := os.MkdirAll(".build", 0o755); err != nil {
		fmt.Fprintf(stderr, "ERROR: %s\n", err)
		return 1
	}
	docs := func(args ...string) bool {
		fmt.Fprintf(stdout, "+ cigate docs %s\n", strings.Join(args, " "))
		return deps.Docs(args) == 0
	}
	fail := func(step string) int {
		fmt.Fprintf(stderr, "ERROR: documentation build step failed: %s\nInstall the documentation environment with: python -m pip install -r requirements-docs.txt\n", step)
		return 1
	}
	if fullCheck && !docs("validate-publication") {
		return fail("validate-publication")
	}
	fmt.Fprintln(stdout, "+ python3 -m mkdocs build --strict --config-file mkdocs.yml")
	if err := deps.MkDocs(); err != nil {
		return fail("mkdocs build: " + err.Error())
	}
	if fullCheck {
		for _, step := range fullCheckSteps() {
			if !docs(step...) {
				return fail(step[0])
			}
		}
	}
	prepare := []string{"--source", siteDir, "--output", assetDir, "--mode", mode, "--redirects", redirects, "--source-revision", deps.Revision()}
	fmt.Fprintf(stdout, "+ cigate docs-prepare %s\n", strings.Join(prepare, " "))
	if deps.Prepare(prepare) != 0 {
		return fail("docs-prepare")
	}
	if err := validateAssetTree(); err != nil {
		fmt.Fprintf(stderr, "ERROR: %s\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "Cloudflare documentation assets are ready at %s (mode=%s, full_check=%t).\n", assetDir, mode, fullCheck)
	return 0
}

// fullCheckSteps are the verbs the full check runs on the built site, in order.
func fullCheckSteps() [][]string {
	return [][]string{
		{"check-built-site-links", "--site-dir", siteDir},
		{"check-docs-search", "--site-dir", siteDir, "--queries", searchAcceptance},
		{"check-docs-accessibility", "--site-dir", siteDir, "--css", "docs/stylesheets/extra.css"},
		{"check-candidate-facts"},
	}
}

func validateAssetTree() error {
	var missing []string
	for _, name := range requiredAssets {
		if info, err := os.Stat(filepath.Join(assetDir, name)); err != nil || !info.Mode().IsRegular() {
			missing = append(missing, filepath.Join(assetDir, name))
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("prepared Cloudflare asset tree is incomplete: %s", strings.Join(missing, ", "))
	}
	return nil
}

func parseArgs(args []string) (mode string, fullCheck bool, problem string) {
	for i := 0; i < len(args); i++ {
		name, value, hasValue := strings.Cut(args[i], "=")
		switch name {
		case "--full-check":
			if hasValue {
				return "", false, "argument --full-check: ignored explicit argument " + value
			}
			fullCheck = true
		case "--mode":
			if !hasValue {
				if i+1 >= len(args) || strings.HasPrefix(args[i+1], "-") {
					return "", false, "argument --mode: expected one argument"
				}
				i++
				value = args[i]
			}
			mode = value
		default:
			return "", false, "unrecognized arguments: " + args[i]
		}
	}
	if mode == "" {
		return "", false, "the following arguments are required: --mode"
	}
	if mode != "preview" && mode != "production" {
		return "", false, fmt.Sprintf("argument --mode: invalid choice: %q (choose from 'preview', 'production')", mode)
	}
	return mode, fullCheck, ""
}
