package goapiproof

// CHAOS-7214: `status` for the MCP class rows.
//
// A class row is not in the edge catalog, so the per-operation table cannot
// carry it (it would read as UNREGISTERED, which is false: the class has its
// own key). This is its own table: one entry per allowlisted root field, with
// the row's state, and whether the listener would serve it.

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

// MCPClassRootStatus is one allowlisted root field's class-row state.
type MCPClassRootStatus struct {
	Root      string
	Operation string
	// ServedByBinary is whether this binary's SDL has the root as a Query
	// field (the allowlist is the ceiling, the SDL is the fact).
	ServedByBinary bool
	// DigestState is DigestMatch (a row at the live schema digest under the
	// class digest), DigestStale (rows exist only at other schema digests or
	// under another document digest) or DigestMissing (no row at all).
	DigestState           string
	Mode                  string
	CurrentCandidateBuild string
	// Reachable is what the listener's switch answers: a live row in
	// canary or primary. A shadow row is NOT reachable.
	Reachable bool
	// Dark is true for a root with no reachable live row. It is the
	// state a digest move produces when carry did not run, so it is named.
	Dark         bool
	StaleDigests []string
}

// MCPClassStatusRows reports every allowlisted root at liveSchemaDigest.
// served is mcpclass.ServedRoots of this binary's SDL.
func MCPClassStatusRows(ctx context.Context, db Querier, liveSchemaDigest string, served map[string]bool) ([]MCPClassRootStatus, error) {
	if db == nil {
		return nil, errors.New("goapiproof: nil database handle")
	}
	if liveSchemaDigest == "" {
		return nil, errors.New("goapiproof: cannot classify MCP class rows without a live schema digest")
	}
	decisions, err := ReadClassDecisions(ctx, db)
	if err != nil {
		return nil, err
	}
	type classRow struct{ build, mode string }
	live := map[string]classRow{}
	for operation, decision := range decisions {
		live[operation] = classRow{build: decision.Build, mode: decision.Mode}
	}

	out := make([]MCPClassRootStatus, 0, len(mcpclass.SortedRoots()))
	for _, root := range mcpclass.SortedRoots() {
		operation := mcpclass.Operation(root)
		status := MCPClassRootStatus{Root: root, Operation: operation, ServedByBinary: served[root]}
		if row, ok := live[operation]; ok {
			status.DigestState = DigestMatch
			status.Mode = row.mode
			status.CurrentCandidateBuild = row.build
			status.Reachable = row.mode == TargetModeCanary || row.mode == TargetModePrimary
		} else {
			status.DigestState = DigestMissing
		}
		status.Dark = !status.Reachable
		out = append(out, status)
	}
	return out, nil
}
