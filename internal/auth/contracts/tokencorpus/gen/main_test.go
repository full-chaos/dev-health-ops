package main

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/tokencorpus"
)

type chain struct {
	state [32]byte
	buf   []byte
}

func (c *chain) Read(p []byte) (int, error) {
	for len(c.buf) < len(p) {
		c.state = sha256.Sum256(c.state[:])
		c.buf = append(c.buf, c.state[:]...)
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

func TestRunWritesACorpusThatVerifiesAndDropsStaleVectors(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "corpus")
	seed := func() *chain { return &chain{state: sha256.Sum256([]byte("gen-test"))} }
	if err := run(root, out, seed()); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(out, "vectors", "invalid-retired.json")
	if err := os.WriteFile(stale, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(root, out, seed()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); err == nil {
		t.Fatal("run left a retired vector behind")
	}
	c, err := tokencorpus.Load(out)
	if err != nil {
		t.Fatal(err)
	}
	if err := tokencorpus.VerifyAll(root, c); err != nil {
		t.Fatal(err)
	}
}

func TestRunRefusesAnUnknownRepoRoot(t *testing.T) {
	if err := run(t.TempDir(), t.TempDir(), nil); err == nil {
		t.Fatal("run wrote a corpus against a root with no contract schemas")
	}
}

func TestRunWorkloadWritesACorpusThatVerifiesAndDropsStaleVectors(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "corpus")
	seed := func() *chain { return &chain{state: sha256.Sum256([]byte("gen-workload-test"))} }
	if err := runWorkload(root, out, seed()); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(out, "vectors", "invalid-retired.json")
	if err := os.WriteFile(stale, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runWorkload(root, out, seed()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); err == nil {
		t.Fatal("runWorkload left a retired vector behind")
	}
	c, err := tokencorpus.Load(out)
	if err != nil {
		t.Fatal(err)
	}
	if c.Manifest.Surface != tokencorpus.SurfaceWorkload {
		t.Fatalf("surface %q", c.Manifest.Surface)
	}
	if err := tokencorpus.VerifyAll(root, c); err != nil {
		t.Fatal(err)
	}
}

func TestRunWorkloadRefusesAnUnknownRepoRoot(t *testing.T) {
	if err := runWorkload(t.TempDir(), t.TempDir(), nil); err == nil {
		t.Fatal("runWorkload wrote a corpus against a root with no contract schemas")
	}
}
