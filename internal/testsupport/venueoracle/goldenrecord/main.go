// Command goldenrecord records the golden files of venue oracles that use
// venueoracle.OpenGolden: the Python plane's answers, executed on the pinned
// Python-bearing build, frozen for a Go-only replay.
//
// Recording is three steps and a golden lands only if all of them pass:
//
//  1. RECORD: the named tests run with DHO_VENUE_GOLDEN_UPDATE=1 against the
//     clean checkout DHO_VENUE_GOLDEN_PYTHON_ROOT names; each writes a
//     candidate (<golden>.recording) from Finish, in the test body.
//  2. REPLAY: the same tests run again in a fresh process with
//     DHO_VENUE_GOLDEN_CANDIDATE=1, which replays the candidate frozen.
//  3. PROMOTE: only if both passed, each candidate is renamed onto its golden and
//     its digest is pinned in the package's tests.
//
// A test failure at any point, including a cleanup that fails after Finish,
// deletes the candidates and leaves every golden as it was.
//
// Usage, from the repository root:
//
//	go run ./internal/testsupport/venueoracle/goldenrecord \
//	    -pkg ./internal/apiservice/admin/ -test '^TestX$' -python-root <clean worktree at the pinned build>
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const candidateSuffix = ".recording"

// Config is one recording.
type Config struct {
	// Root is the repository root: go test runs there and Package is relative to it.
	Root string
	// Package is the go package pattern of the tests, e.g. ./internal/apiservice/admin/.
	Package string
	// Test is the -run regular expression selecting the oracles to record.
	Test string
	// PythonRoot is the clean checkout at the pinned build Python runs from.
	PythonRoot string
	// Run executes one go test invocation with the extra environment. The
	// default runs `go test -tags=integration` in Root.
	Run func(cfg Config, env []string) error
}

// Result names what one recording promoted.
type Result struct {
	Promoted []Promotion
}

// Promotion is one golden that landed.
type Promotion struct {
	Path      string
	OldDigest string
	NewDigest string
	// Pinned lists the test files whose pinned digest was updated.
	Pinned []string
}

func main() {
	cfg := Config{}
	flag.StringVar(&cfg.Root, "root", ".", "repository root")
	flag.StringVar(&cfg.Package, "pkg", "", "go package of the oracles, relative to the root")
	flag.StringVar(&cfg.Test, "test", "", "-run regular expression of the oracles to record")
	flag.StringVar(&cfg.PythonRoot, "python-root", "", "clean checkout at the pinned Python-bearing build")
	flag.Parse()
	if cfg.Package == "" || cfg.Test == "" || cfg.PythonRoot == "" {
		fmt.Fprintln(os.Stderr, "goldenrecord: -pkg, -test and -python-root are required")
		os.Exit(2)
	}
	result, err := Record(context.Background(), cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "goldenrecord: nothing was promoted: %v\n", err)
		os.Exit(1)
	}
	for _, promotion := range result.Promoted {
		fmt.Printf("promoted %s\n  sha256 %s (was %s)\n", promotion.Path, promotion.NewDigest, orNone(promotion.OldDigest))
		if len(promotion.Pinned) == 0 {
			fmt.Printf("  no test pins the previous digest: pin %s in the test that opens this golden\n", promotion.NewDigest)
		}
		for _, file := range promotion.Pinned {
			fmt.Printf("  pinned in %s\n", file)
		}
	}
}

func orNone(digest string) string {
	if digest == "" {
		return "none: a new golden"
	}
	return digest
}

// Record runs the three steps and returns what it promoted, or an error with
// every golden untouched and every candidate deleted.
func Record(ctx context.Context, cfg Config) (Result, error) {
	if cfg.Run == nil {
		cfg.Run = goTest
	}
	packageDir := filepath.Join(cfg.Root, cfg.Package)
	if _, err := os.Stat(packageDir); err != nil {
		return Result{}, err
	}
	stale, err := candidates(packageDir)
	if err != nil {
		return Result{}, err
	}
	if len(stale) > 0 {
		return Result{}, fmt.Errorf("candidates from an earlier run are still on disk (%s): delete them, then record again", stale[0])
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		dir, err := os.MkdirTemp("", "goldenrecord-proof-")
		if err != nil {
			return Result{}, err
		}
		defer os.RemoveAll(dir)
		proofDir = dir
	}
	base := []string{"DEV_HEALTH_LIVE_PYTHON_ORACLES=1", "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR=" + proofDir}
	discard := func() { removeAll(packageDir) }

	if err := cfg.Run(cfg, append(append([]string{}, base...), "DHO_VENUE_GOLDEN_UPDATE=1", "DHO_VENUE_GOLDEN_PYTHON_ROOT="+cfg.PythonRoot)); err != nil {
		discard()
		return Result{}, fmt.Errorf("the recording run failed (a golden is only recorded from a run that passed every check, cleanups included): %w", err)
	}
	found, err := candidates(packageDir)
	if err != nil {
		discard()
		return Result{}, err
	}
	if len(found) == 0 {
		return Result{}, errors.New("the recording run passed but wrote no candidate: the selected tests do not use venueoracle.OpenGolden or did not reach Finish")
	}
	if err := cfg.Run(cfg, append(append([]string{}, base...), "DHO_VENUE_GOLDEN_CANDIDATE=1")); err != nil {
		discard()
		return Result{}, fmt.Errorf("the fresh-process replay of the candidates failed: %w", err)
	}

	var result Result
	for _, candidate := range found {
		final := strings.TrimSuffix(candidate, candidateSuffix)
		promotion := Promotion{Path: final}
		if raw, err := os.ReadFile(final); err == nil {
			promotion.OldDigest = digest(raw)
		}
		raw, err := os.ReadFile(candidate)
		if err != nil {
			return result, err
		}
		promotion.NewDigest = digest(raw)
		if err := os.Rename(candidate, final); err != nil {
			return result, err
		}
		pinned, err := pin(packageDir, promotion.OldDigest, promotion.NewDigest)
		if err != nil {
			return result, err
		}
		promotion.Pinned = pinned
		result.Promoted = append(result.Promoted, promotion)
	}
	return result, nil
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// candidates lists the candidate files under dir.
func candidates(dir string) ([]string, error) {
	var found []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, candidateSuffix) {
			found = append(found, path)
		}
		return nil
	})
	return found, err
}

func removeAll(dir string) {
	found, _ := candidates(dir)
	for _, path := range found {
		_ = os.Remove(path)
	}
}

// pin replaces the previous digest of a golden with the new one in the Go test
// files of the package directory and returns the files it changed. A new golden
// has no previous digest: the test names the placeholder "PIN" until the first
// record, and that literal is replaced when exactly one test file holds it.
func pin(dir, oldDigest, newDigest string) ([]string, error) {
	needle := oldDigest
	if needle == "" {
		return nil, nil
	}
	files, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil {
		return nil, err
	}
	var changed []string
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			return changed, err
		}
		if !strings.Contains(string(raw), needle) {
			continue
		}
		updated := strings.ReplaceAll(string(raw), needle, newDigest)
		if err := os.WriteFile(file, []byte(updated), 0o644); err != nil {
			return changed, err
		}
		changed = append(changed, file)
	}
	return changed, nil
}

// goTest is the default runner.
func goTest(cfg Config, env []string) error {
	command := exec.Command("go", "test", "-tags=integration", "-count=1", "-timeout", "120m", "-v", "-run", cfg.Test, cfg.Package)
	command.Dir = cfg.Root
	command.Env = append(os.Environ(), env...)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	return command.Run()
}
