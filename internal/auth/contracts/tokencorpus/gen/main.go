// Command gen writes the signed access-token.v1 golden corpus with fresh keys.
// The private keys live in memory only and are discarded.
package main

import (
	"fmt"
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
	corpus, err := tokencorpus.Generate(nil)
	if err != nil {
		fail(err)
	}
	if err := tokencorpus.VerifyAll(root, corpus); err != nil {
		fail(err)
	}
	if err := tokencorpus.Write(filepath.Join(root, tokencorpus.Dir), corpus); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "tokencorpus:", err)
	os.Exit(1)
}
