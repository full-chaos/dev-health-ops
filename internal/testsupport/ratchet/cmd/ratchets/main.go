// Command ratchets holds every closed list named in ci/ratchets.tsv to its size on the merge base (see package ratchet).
package main

import (
	"flag"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/ratchet"
)

func main() {
	repo := flag.String("repo", ".", "the repository root")
	base := flag.String("base", os.Getenv("RATCHET_BASE_SHA"), "the commit to compare with (default: RATCHET_BASE_SHA, else the merge base with origin/main)")
	flag.Parse()
	os.Exit(ratchet.Run(*repo, *base, os.Stdout, os.Stderr))
}
