package scripts_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-11561: a script under ci/ or scripts/, or a Go test that drives one, must find its toolchain from PATH or
// `go env GOROOT`, never from an absolute install location. Hosted runners install go through setup-go (under
// /opt/hostedtoolcache), bigboy has it elsewhere, so a hard-coded /usr/local/go/bin/go passes on one host and fails
// on the next (the CHAOS-11439 case).
//
// The toolchain-path class (a line-1 "#!" shebang is never scanned):
//   - a Go toolchain root: /usr/local/go, /usr/lib/go, /usr/lib/golang, /usr/lib/go-<version>, or anything under
//     /opt/hostedtoolcache/;
//   - a host binary path /bin/<tool>, /usr/bin/<tool> or /usr/local/bin/<tool> that starts its token (so
//     "$(go env GOROOT)/bin/gofmt" and "${dir}/bin/x" are not hits). The launchers env, bash, sh and dash are not
//     toolchain paths, and /usr/local/bin/<name> is one only for a host tool name: the other names are binaries
//     baked into our own images (dho, acr-*, auth-service), used inside docker exec and compose.

const toolchainGuardFile = "internal/cigates/scripts/toolchain_paths_test.go"

var (
	toolchainRootPattern = regexp.MustCompile(`/usr/local/go\b|/usr/lib/go(?:lang)?(?:-[0-9.]+)?\b|/opt/hostedtoolcache/`)
	hostBinaryPattern    = regexp.MustCompile(`(?:/usr/local|/usr)?/bin/[A-Za-z0-9_.+-]+`)
	scriptPathPattern    = regexp.MustCompile(`"(?:ci|scripts)"|(?:ci|scripts)/[A-Za-z0-9_./-]+\.(?:sh|bash|py|awk|mjs)\b`)
	scriptExtensions     = []string{".sh", ".bash", ".py", ".awk", ".mjs"}
	launcherNames        = map[string]bool{"env": true, "bash": true, "sh": true, "dash": true}
	hostToolNames        = map[string]bool{
		"go": true, "gofmt": true, "golangci-lint": true, "staticcheck": true, "gitleaks": true, "actionlint": true,
		"shellcheck": true, "jq": true, "yq": true, "git": true, "gh": true, "helm": true, "kubectl": true,
		"docker": true, "python": true, "python3": true, "node": true, "npm": true, "uv": true,
	}
)

type toolchainHit struct {
	file string
	line int
	path string
}

// toolchainPathsIn returns every toolchain-path hit in one file's text.
func toolchainPathsIn(file, text string) []toolchainHit {
	var hits []toolchainHit
	for index, line := range strings.Split(text, "\n") {
		if index == 0 && strings.HasPrefix(line, "#!") {
			continue
		}
		for _, match := range toolchainRootPattern.FindAllString(line, -1) {
			hits = append(hits, toolchainHit{file: file, line: index + 1, path: match})
		}
		for _, span := range hostBinaryPattern.FindAllStringIndex(line, -1) {
			if span[0] > 0 && strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-${})", rune(line[span[0]-1])) {
				continue
			}
			path := line[span[0]:span[1]]
			name := path[strings.LastIndex(path, "/")+1:]
			if launcherNames[name] {
				continue
			}
			if strings.HasPrefix(path, "/usr/local/bin/") && !hostToolNames[name] {
				continue
			}
			hits = append(hits, toolchainHit{file: file, line: index + 1, path: path})
		}
	}
	return hits
}

// toolchainScopeFiles lists, relative to root, every script under ci/ and scripts/ and every Go test that drives one.
func toolchainScopeFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	files := map[string]string{}
	for _, top := range []string{"ci", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !hasScriptExtension(path) {
				return nil
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			files[filepath.ToSlash(rel)] = string(body)
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if rel == toolchainGuardFile {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if scriptPathPattern.Match(body) {
			files[rel] = string(body)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func hasScriptExtension(path string) bool {
	for _, extension := range scriptExtensions {
		if strings.HasSuffix(path, extension) {
			return true
		}
	}
	return false
}

func toolchainHitsUnder(t *testing.T, root string) []toolchainHit {
	t.Helper()
	files := toolchainScopeFiles(t, root)
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	var hits []toolchainHit
	for _, name := range names {
		hits = append(hits, toolchainPathsIn(name, files[name])...)
	}
	return hits
}

// toolchainPathAllowlist names each real hit that stays, keyed by file and path, with the reason it stays.
var toolchainPathAllowlist = []struct {
	file, path, reason string
}{}

func TestNoAbsoluteToolchainPathsInCIScripts(t *testing.T) {
	type key struct{ file, path string }
	allowed := map[key]string{}
	for _, row := range toolchainPathAllowlist {
		if strings.TrimSpace(row.reason) == "" {
			t.Errorf("allowlist row %s %s has no reason", row.file, row.path)
		}
		allowed[key{row.file, row.path}] = row.reason
	}
	used := map[key]bool{}
	for _, hit := range toolchainHitsUnder(t, repoRoot(t)) {
		k := key{hit.file, hit.path}
		if _, ok := allowed[k]; ok {
			used[k] = true
			continue
		}
		t.Errorf("%s:%d names the absolute toolchain path %q: resolve it from PATH or `go env GOROOT`, or allowlist it with a reason", hit.file, hit.line, hit.path)
	}
	for k := range allowed {
		if !used[k] {
			t.Errorf("stale allowlist row %s %s: the file no longer names that path", k.file, k.path)
		}
	}
}

func TestToolchainPathGuardCatchesPlantedPaths(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("ci/plant_build.sh", "#!/usr/bin/env bash\nset -eu\n/usr/local/go/bin/go test ./...\n")
	write("internal/x/drive_test.go", "package x\n// drives ci/plant_build.sh\nconst goBin = \"/opt/hostedtoolcache/go/1.27.4/x64/bin/go\"\n")
	write("scripts/fmt.sh", "#!/bin/bash\n/usr/bin/gofmt -l .\n")
	write("ci/clean.sh", strings.Join([]string{
		"#!/usr/bin/env bash",
		"exec /usr/bin/env -i PATH=/usr/bin:/bin /bin/bash \"$0\"",
		`GOFMT="$(go env GOROOT)/bin/gofmt"`,
		`docker exec box /usr/local/bin/dho healthcheck`,
		`PATH="${go_root}/bin:/usr/bin:/bin"`,
		"",
	}, "\n"))
	write("internal/y/undriven_test.go", "package y\nconst p = \"/usr/local/go/bin/go\"\n")

	got := map[string]bool{}
	for _, hit := range toolchainHitsUnder(t, root) {
		got[hit.file+" "+hit.path] = true
	}
	for _, want := range []string{
		"ci/plant_build.sh /usr/local/go",
		"internal/x/drive_test.go /opt/hostedtoolcache/",
		"scripts/fmt.sh /usr/bin/gofmt",
	} {
		if !got[want] {
			t.Errorf("planted toolchain path not reported: %s (got %v)", want, got)
		}
	}
	for hit := range got {
		if strings.HasPrefix(hit, "ci/clean.sh ") || strings.HasPrefix(hit, "internal/y/") {
			t.Errorf("a launcher, a GOROOT-relative path, an image binary or an undriven test was reported: %s", hit)
		}
	}
}
