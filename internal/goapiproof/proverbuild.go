package goapiproof

import (
	"errors"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

// ErrProverBuildSkew reports that a prover binary was not built from the
// candidate's own commit. Every declaration, shape and corpus entry a run
// applies is compiled into the prover, so a skewed prover measures the
// candidate against another commit's rules.
var ErrProverBuildSkew = errors.New("goapiproof: the prover was not built from the candidate build's commit, so the declarations, shapes and corpus it applies are not the candidate's")

// ProverBuild pairs the prover's own build with the candidate build
// /buildinfo named, and records whether a skew between them was allowed.
type ProverBuild struct {
	Prover         string `json:"prover_build"`
	ProverModified bool   `json:"prover_build_modified"`
	Candidate      string `json:"candidate_build"`
	// Skew is true when the prover's own commit is not the candidate's,
	// or does not identify its source at all (unstamped or modified).
	Skew        bool `json:"prover_build_skew"`
	SkewAllowed bool `json:"prover_build_skew_allowed"`
}

// NewProverBuild compares the prover's own build info with the candidate
// build. An unstamped prover (commit empty or version's "unknown"
// default) or one built from a modified tree never matches: its commit
// does not name the source its rules came from.
//
// The modified arm reports what the binary knows: a build stamped from
// VCS (a local `go build` in a work tree) carries vcs.modified, while a
// released image builds with -buildvcs=false and takes its commit from
// -ldflags, so it reports modified=false whatever its build context held.
// For a released prover the commit comparison is the guard that fires;
// the same limit applies to the candidate's own /buildinfo answer
// (FetchBuildIdentity's own modified refusal).
func NewProverBuild(prover version.Info, candidate string, allowSkew bool) ProverBuild {
	commit := strings.TrimSpace(prover.Commit)
	skew := commit == "" || commit == "unknown" || prover.Modified || commit != candidate
	return ProverBuild{
		Prover:         commit,
		ProverModified: prover.Modified,
		Candidate:      candidate,
		Skew:           skew,
		SkewAllowed:    skew && allowSkew,
	}
}

// Line is the stdout line naming both builds.
func (b ProverBuild) Line() string {
	line := fmt.Sprintf("prover_build=%s prover_build_modified=%t candidate_build=%s prover_build_skew=%t", b.Prover, b.ProverModified, b.Candidate, b.Skew)
	if b.SkewAllowed {
		line += " prover_build_skew_allowed=true"
	}
	return line
}

// Check refuses a skewed run unless overrideFlag allowed it.
func (b ProverBuild) Check(overrideFlag string) error {
	if !b.Skew || b.SkewAllowed {
		return nil
	}
	return fmt.Errorf("%w: prover build %q (modified=%t), candidate build %q -- run a prover built from the candidate's commit, or pass %s to measure anyway with the skew recorded in the report", ErrProverBuildSkew, b.Prover, b.ProverModified, b.Candidate, overrideFlag)
}
