// Package moduleroot finds the Go module root and gives test code a source-file lookup that keeps working
// under `go test -trimpath` (CHAOS-8030).
//
// Many tests locate repository files (frozen Python sources, goldens, schema files) from the path of their own
// source file, the way runtime.Caller reports it. With -trimpath the runtime reports that path in module-path
// form (github.com/full-chaos/dev-health-ops/internal/x/x_test.go), which is not a file on disk, so every
// `filepath.Dir(file)/..` lookup built on it fails. Caller has the signature of runtime.Caller and returns the
// absolute path of the source file in both build modes, so a call site changes by its package name only.
package moduleroot

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

var (
	once       sync.Once
	cachedRoot string
	cachedPath string
	cachedErr  error
)

// Root returns the directory of the nearest go.mod at or above the working directory (`go test` runs a test
// in its package directory, so that is the module of the package under test).
func Root() (string, error) {
	load()
	return cachedRoot, cachedErr
}

// Caller is runtime.Caller for test support code: file is the ABSOLUTE source path of the caller at skip frames
// above Caller's caller (skip 0 = the function that called Caller), also under -trimpath.
func Caller(skip int) (pc uintptr, file string, line int, ok bool) {
	pc, file, line, ok = runtime.Caller(skip + 1)
	if !ok {
		return pc, file, line, false
	}
	load()
	return pc, absolute(file, cachedRoot, cachedPath), line, true
}

// absolute maps the module-path form the runtime reports under -trimpath back to a file under root. An absolute
// path, or a path of another module, is returned unchanged.
func absolute(file, root, modulePath string) string {
	if filepath.IsAbs(file) || root == "" || modulePath == "" {
		return file
	}
	slashed := filepath.ToSlash(file)
	if rest, ok := strings.CutPrefix(slashed, modulePath+"/"); ok {
		return filepath.Join(root, filepath.FromSlash(rest))
	}
	return file
}

func load() {
	once.Do(func() {
		directory, err := os.Getwd()
		if err != nil {
			cachedErr = fmt.Errorf("moduleroot: working directory: %w", err)
			return
		}
		cachedRoot, cachedPath, cachedErr = find(directory)
	})
}

// find walks up from directory to the nearest go.mod and returns its directory and module path.
func find(directory string) (root, modulePath string, err error) {
	for {
		goMod := filepath.Join(directory, "go.mod")
		if info, statErr := os.Stat(goMod); statErr == nil && !info.IsDir() {
			path, readErr := readModulePath(goMod)
			if readErr != nil {
				return "", "", readErr
			}
			return directory, path, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", "", errors.New("moduleroot: no go.mod at or above the working directory")
		}
		directory = parent
	}
}

func readModulePath(goMod string) (string, error) {
	file, err := os.Open(goMod)
	if err != nil {
		return "", fmt.Errorf("moduleroot: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if rest, ok := strings.CutPrefix(line, "module "); ok {
			return strings.Trim(strings.TrimSpace(rest), `"`), nil
		}
	}
	return "", fmt.Errorf("moduleroot: %s has no module line", goMod)
}
