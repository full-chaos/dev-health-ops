// Package docsbuild builds the MkDocs site and prepares the static tree Wrangler uploads. It is the Go port of the former
// scripts/build_docs_cloudflare.py: the same chain, in the same order, with the same flags.
package docsbuild

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	siteDir          = ".build/docs"
	assetDir         = ".build/docs-cloudflare"
	redirects        = "docs-data/redirects.tsv"
	searchAcceptance = "docs-data/search-acceptance.json"
	usage            = "usage: cigate docs-build --mode {preview,production} [--full-check]"
)

var requiredAssets = []string{"index.html", "404.html", "_headers", "_redirects", "robots.txt", "cloudflare-build-manifest.json"}

// Deps are the side effects of a build: the cigate verbs, the prepare step, the MkDocs call and the revision lookup. Every
// one runs in root, the repository root found from Start, so the build does not depend on the caller's directory.
type Deps struct {
	Start    string
	Docs     func(root string, args []string) int
	Prepare  func(root string, args []string) int
	MkDocs   func(root string) (interpreter string, err error)
	Revision func(root string) string
}

// OSDeps runs the verbs in this process and MkDocs as a tool call, `python3 -m mkdocs build --strict --config-file
// mkdocs.yml`, the command the former script ran with its own interpreter. DOCS_PYTHON names another interpreter.
func OSDeps(stdout, stderr io.Writer) Deps {
	start, _ := os.Getwd()
	// The verbs read and write paths relative to their working directory, and a process-wide chdir would race every other
	// verb in this process, so each runs as this same binary with its own working directory.
	self := func(root string, args []string) int {
		exe, err := os.Executable()
		if err != nil {
			fmt.Fprintf(stderr, "ERROR: %s\n", err)
			return 1
		}
		cmd := exec.Command(exe, args...)
		cmd.Dir, cmd.Stdout, cmd.Stderr = root, stdout, stderr
		if err := cmd.Run(); err != nil {
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				return exit.ExitCode()
			}
			fmt.Fprintf(stderr, "ERROR: %s\n", err)
			return 1
		}
		return 0
	}
	return Deps{
		Start:   start,
		Docs:    func(root string, args []string) int { return self(root, append([]string{"docs"}, args...)) },
		Prepare: func(root string, args []string) int { return self(root, append([]string{"docs-prepare"}, args...)) },
		MkDocs: func(root string) (string, error) {
			python := os.Getenv("DOCS_PYTHON")
			if python == "" {
				python = "python3"
			}
			cmd := exec.Command(python, "-m", "mkdocs", "build", "--strict", "--config-file", "mkdocs.yml")
			cmd.Dir, cmd.Stdout, cmd.Stderr = root, stdout, stderr
			return python, cmd.Run()
		},
		Revision: sourceRevision,
	}
}

// findRoot returns the nearest directory at or above start that holds go.mod and mkdocs.yml, the files the docs build reads
// from the repository root.
func findRoot(start string) (string, error) {
	for dir := start; ; dir = filepath.Dir(dir) {
		if isFile(filepath.Join(dir, "go.mod")) && isFile(filepath.Join(dir, "mkdocs.yml")) {
			return dir, nil
		}
		if filepath.Dir(dir) == dir {
			return "", fmt.Errorf("no repository root (go.mod and mkdocs.yml) at or above %s", start)
		}
	}
}

func isFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

func sourceRevision(root string) string {
	if sha := strings.TrimSpace(os.Getenv("GITHUB_SHA")); sha != "" {
		return sha
	}
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = root
	out, err := cmd.Output()
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
	root, err := findRoot(deps.Start)
	if err != nil {
		fmt.Fprintf(stderr, "ERROR: %s\n", err)
		return 1
	}
	// Production is never prepared without the full publication gate.
	fullCheck = fullCheck || mode == "production"
	if err := os.MkdirAll(filepath.Join(root, ".build"), 0o755); err != nil {
		fmt.Fprintf(stderr, "ERROR: %s\n", err)
		return 1
	}
	docs := func(args ...string) bool {
		fmt.Fprintf(stdout, "+ cigate docs %s\n", strings.Join(args, " "))
		return deps.Docs(root, args) == 0
	}
	fail := func(step string) int {
		fmt.Fprintf(stderr, "ERROR: documentation build step failed: %s\nInstall the documentation environment with: python -m pip install -r requirements-docs.txt\n", step)
		return 1
	}
	if fullCheck && !docs("validate-publication") {
		return fail("validate-publication")
	}
	python, err := deps.MkDocs(root)
	fmt.Fprintf(stdout, "+ %s -m mkdocs build --strict --config-file mkdocs.yml\n", python)
	if err != nil {
		return fail("mkdocs build: " + err.Error())
	}
	if fullCheck {
		for _, step := range fullCheckSteps() {
			if !docs(step...) {
				return fail(step[0])
			}
		}
	}
	prepare := []string{"--source", siteDir, "--output", assetDir, "--mode", mode, "--redirects", redirects, "--source-revision", deps.Revision(root)}
	fmt.Fprintf(stdout, "+ cigate docs-prepare %s\n", strings.Join(prepare, " "))
	if deps.Prepare(root, prepare) != 0 {
		return fail("docs-prepare")
	}
	if err := validateAssetTree(root); err != nil {
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

func validateAssetTree(root string) error {
	var missing []string
	for _, name := range requiredAssets {
		if info, err := os.Stat(filepath.Join(root, assetDir, name)); err != nil || !info.Mode().IsRegular() {
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
