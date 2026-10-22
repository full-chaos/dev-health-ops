// Command gen writes the signed access-token.v1 golden corpus with fresh keys.
// The private keys live in memory only and are discarded.
package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/tokencorpus"
)

func main() {
	wd, err := os.Getwd()
	if err != nil {
		fail(err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		fail(err)
	}
	if err := run(root, filepath.Join(root, tokencorpus.Dir), nil); err != nil {
		fail(err)
	}
}

// run generates a corpus with keys from random (crypto/rand when nil), checks
// that every vector gets its manifest decision, and writes it to out.
func run(root, out string, random io.Reader) error {
	if _, err := contracts.RepoRoot(root); err != nil {
		return err
	}
	corpus, err := tokencorpus.Generate(random)
	if err != nil {
		return err
	}
	if err := tokencorpus.VerifyAll(root, corpus); err != nil {
		return err
	}
	return tokencorpus.Write(out, corpus)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "tokencorpus:", err)
	os.Exit(1)
}
