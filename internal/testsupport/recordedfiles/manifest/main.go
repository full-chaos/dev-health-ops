// Command manifest writes the manifests of the recordedfiles guard: the one
// way a row of a manifest comes, changes or goes.
//
//	go run ./internal/testsupport/recordedfiles/manifest -kind hand-written internal/x/testdata/case.json
//	go run ./internal/testsupport/recordedfiles/manifest internal/x/testdata/golden/TestY.json   (a golden of the record verb)
//	go run ./internal/testsupport/recordedfiles/manifest -sync                                  (after deleting files)
//	go run ./internal/testsupport/recordedfiles/manifest -check                                 (what the guard refuses, nothing written)
//
// A python-recorded row changes or goes only with -recorded-again: the file
// was recorded again from its Python producer, or replaced on purpose.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedfiles"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	flags := flag.NewFlagSet("manifest", flag.ContinueOnError)
	kind := flags.String("kind", "", "the kind of the files: "+strings.Join(recordedfiles.Kinds[:4], ", ")+" (none for a golden of the record verb)")
	recordedAgain := flags.Bool("recorded-again", false, "allow a python-recorded row to change or to go: the file was recorded again, or replaced on purpose")
	sync := flags.Bool("sync", false, "drop the rows of deleted files and write every manifest again")
	check := flags.Bool("check", false, "print what the guard refuses; write nothing")
	dayOne := flags.Bool("day-one", false, "first manifests only: allow -kind unclassified and write the day-one list; give a golden with no stamp of the record verb the kind header-before-stamp")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	repo, err := recordedfiles.RepoRoot(".")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	change := recordedfiles.Change{Kind: *kind, RecordedAgain: *recordedAgain, DayOne: *dayOne}
	var notes []string
	switch {
	case *check:
	case *sync:
		notes, err = recordedfiles.Sync(repo, change)
	case flags.NArg() == 0:
		fmt.Fprintln(os.Stderr, "name the files, or give -sync or -check")
		return 2
	default:
		notes, err = recordedfiles.Set(repo, flags.Args(), change)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	for _, note := range notes {
		fmt.Println("TO DO:", note)
	}
	problems, err := recordedfiles.Problems(repo)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	for _, problem := range problems {
		fmt.Println(problem)
	}
	if remaining, err := recordedfiles.Remaining(repo); err == nil {
		fmt.Println(remaining)
	}
	if len(problems) > 0 {
		fmt.Printf("%d problem(s) left\n", len(problems))
		return 1
	}
	return 0
}
