package pgmigrate

import "fmt"

// HeadVerdict is how the revisions a database records relate to the heads a build brings it to.
type HeadVerdict int

const (
	// HeadsEqual: the database is at this build's heads.
	HeadsEqual HeadVerdict = iota
	// HeadsBehind: the database lacks a revision this build needs.
	HeadsBehind
	// HeadsAhead: the database records a revision this build does not know (a rollback).
	HeadsAhead
)

func (v HeadVerdict) String() string {
	switch v {
	case HeadsEqual:
		return "equal"
	case HeadsBehind:
		return "behind"
	case HeadsAhead:
		return "ahead"
	}
	return "unknown"
}

// BuildHeads is what this build expects of a database: its embedded baseline and chain.
type BuildHeads struct {
	baseline Baseline
	chain    []ChainFile
	known    map[string]bool
}

// LoadBuildHeads reads the embedded baseline, chain and history.
func LoadBuildHeads() (BuildHeads, error) {
	baseline, err := LoadBaseline()
	if err != nil {
		return BuildHeads{}, fmt.Errorf("load the baseline: %w", err)
	}
	chain, err := LoadChain()
	if err != nil {
		return BuildHeads{}, fmt.Errorf("load the chain: %w", err)
	}
	history, err := LoadHistory()
	if err != nil {
		return BuildHeads{}, fmt.Errorf("load the history: %w", err)
	}
	return NewBuildHeads(baseline, chain, KnownRevisions(history, baseline, chain)), nil
}

// NewBuildHeads builds the expectation from its parts.
func NewBuildHeads(baseline Baseline, chain []ChainFile, known map[string]bool) BuildHeads {
	return BuildHeads{baseline: baseline, chain: chain, known: known}
}

// Heads are the sorted head revisions this build brings a database to.
func (b BuildHeads) Heads() []string { return Heads(b.baseline, b.chain) }

// CompareHeads classifies the recorded revisions. A revision this build does not know
// is HeadsAhead before anything else, as Decide does; otherwise the database is
// HeadsBehind when a baseline head is missing or a chain revision after the recorded
// one is still pending. It reads no table and never changes anything.
func (b BuildHeads) CompareHeads(recorded []string) HeadVerdict {
	if len(unknownRevisions(recorded, b.known)) > 0 {
		return HeadsAhead
	}
	has := map[string]bool{}
	for _, revision := range recorded {
		has[revision] = true
	}
	position := -1
	for index, file := range b.chain {
		if has[file.Revision] {
			position = index
		}
	}
	application := applicationHead(b.baseline)
	for _, head := range b.baseline.Heads {
		if head == application && position >= 0 {
			continue
		}
		if !has[head] {
			return HeadsBehind
		}
	}
	if position < len(b.chain)-1 {
		return HeadsBehind
	}
	return HeadsEqual
}
