// Command ratchets holds every closed list named in ci/ratchets.tsv to its size on the merge base (see package ratchet).
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/ratchet"
)

func main() {
	repo := flag.String("repo", ".", "the repository root")
	base := flag.String("base", os.Getenv("RATCHET_BASE_SHA"), "the commit to compare with (default: RATCHET_BASE_SHA, else the merge base with origin/main)")
	flag.Parse()
	entries, err := ratchet.ReadManifest(*repo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ratchets: FAIL: %v\n", err)
		os.Exit(1)
	}
	commit, err := ratchet.Resolve(*repo, *base)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ratchets: FAIL: %v\n", err)
		os.Exit(1)
	}
	failed := false
	for _, entry := range entries {
		result, grew, err := ratchet.Check(*repo, commit, entry)
		switch {
		case err != nil:
			fmt.Fprintf(os.Stderr, "ratchets: FAIL: %v\n", err)
			failed = true
		case grew != "":
			fmt.Fprintf(os.Stderr, "ratchets: FAIL: %s\n", grew)
			failed = true
		default:
			fmt.Printf("ratchet %s: base %d, now %d: ok\n", entry.Name, result.BaseCount, result.NowCount)
		}
	}
	if failed {
		os.Exit(1)
	}
	fmt.Printf("ratchets: OK (%d lists, base %s)\n", len(entries), commit[:12])
}
