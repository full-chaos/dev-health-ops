package goapiproof

// CHAOS-7214: `status` for the MCP class rows.
//
// A class row is not in the edge catalog, so the per-operation table cannot
// carry it (it would read as UNREGISTERED, which is false: the class has its
// own key). This is its own table: one entry per allowlisted root field, with
// the row's state at the LIVE schema digest, whether the listener would serve
// it, and whether the exact build it names holds a class proof receipt.

import (
	"context"
	"errors"
	"fmt"
	"sort"

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
	// Proven is whether the build the live row names holds a class receipt.
	Proven bool
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
	rows, err := db.Query(ctx, `
		SELECT schema_digest, document_digest, selected_operation, current_candidate_build, mode
		  FROM public.go_api_routing_state
		 WHERE left(selected_operation, `+fmt.Sprint(len(mcpclass.OperationPrefix))+`) = '`+mcpclass.OperationPrefix+`'`)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read MCP class rows: %w", err)
	}
	type classRow struct{ schemaDigest, documentDigest, operation, build, mode string }
	var all []classRow
	for rows.Next() {
		var row classRow
		if err := rows.Scan(&row.schemaDigest, &row.documentDigest, &row.operation, &row.build, &row.mode); err != nil {
			rows.Close()
			return nil, fmt.Errorf("goapiproof: scan MCP class row: %w", err)
		}
		all = append(all, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("goapiproof: read MCP class rows: %w", err)
	}

	classDigest := mcpclass.DocumentDigest()
	live := map[string]classRow{}
	stale := map[string]map[string]bool{}
	for _, row := range all {
		if row.schemaDigest == liveSchemaDigest && row.documentDigest == classDigest {
			live[row.operation] = row
			continue
		}
		if stale[row.operation] == nil {
			stale[row.operation] = map[string]bool{}
		}
		stale[row.operation][row.schemaDigest] = true
	}

	// Proof is read per (build): a row naming an older build must not borrow a
	// newer build's receipt.
	wantedByBuild := map[string]map[string]string{}
	for operation, row := range live {
		if wantedByBuild[row.build] == nil {
			wantedByBuild[row.build] = map[string]string{}
		}
		wantedByBuild[row.build][operation] = classDigest
	}
	proven := map[string]bool{}
	for build, wanted := range wantedByBuild {
		kinds := make(map[string]string, len(wanted))
		for operation := range wanted {
			kinds[operation] = OperationKindMCPClass
		}
		found, err := OperationsWithEnablementProofByKind(ctx, db, liveSchemaDigest, build, TargetModeCanary, wanted, kinds)
		if err != nil {
			return nil, err
		}
		for operation := range found {
			proven[operation] = true
		}
	}

	out := make([]MCPClassRootStatus, 0, len(mcpclass.SortedRoots()))
	for _, root := range mcpclass.SortedRoots() {
		operation := mcpclass.Operation(root)
		status := MCPClassRootStatus{Root: root, Operation: operation, ServedByBinary: served[root]}
		for digest := range stale[operation] {
			status.StaleDigests = append(status.StaleDigests, digest)
		}
		sort.Strings(status.StaleDigests)
		if row, ok := live[operation]; ok {
			status.DigestState = DigestMatch
			status.Mode = row.mode
			status.CurrentCandidateBuild = row.build
			status.Reachable = row.mode == TargetModeCanary || row.mode == TargetModePrimary
			status.Proven = proven[operation]
		} else if len(status.StaleDigests) > 0 {
			status.DigestState = DigestStale
		} else {
			status.DigestState = DigestMissing
		}
		status.Dark = !status.Reachable
		out = append(out, status)
	}
	return out, nil
}
