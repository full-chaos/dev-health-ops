package syncrunrollup

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// crossPackageDirs lists every package, relative to this file's own
// directory (internal/syncrunrollup), that writes a terminal status to
// public.sync_run_units. CHAOS-4586 (chris: "Not again" / "the fix covers
// the defect CLASS by mechanism, not one package") widened this guard from
// a single-package registry (which would NOT have caught
// internal/syncreconciler/unreclaimable_sweep.go:1144, a different
// package) to this cross-package scan. Adding a FOURTH package that ever
// writes public.sync_run_units.status to a terminal value must add its
// directory here, or its terminal-status writes are invisible to
// TestCrossPackageRollupSeamRegistryIsComplete by construction.
var crossPackageDirs = []string{
	"../providersync",
	"../syncdispatchruntime",
	"../syncreconciler",
}

// crossPackageWriteSite is one line of source that writes a terminal status
// ('failed'/'success') to public.sync_run_units -- i.e. one place in the
// whole codebase that terminalizes a sync unit. snippet is matched by exact
// substring, not line number, so this registry survives unrelated
// line-shifting edits to its file -- only ADDING or REMOVING a
// terminal-status write site should ever require touching this table.
type crossPackageWriteSite struct {
	pkgDir  string // one of crossPackageDirs
	file    string // filename within pkgDir
	snippet string
	// seamCoveredPkgDir/seamCoveredFile/seamCoveredMarker: where THIS site's
	// terminal write is followed, in the same transaction, by a call into
	// syncrunrollup.Bump (the ONE seam). Not always the same file as the
	// write itself: failPlannedUnits/failStaleDispatchingUnits are
	// low-level helpers in syncdispatchruntime/dispatch_denial.go, but the
	// seam call for their hasActive-branch caller lives in denyRun
	// (native_dispatch_sync_run_service.go, same package, different file) --
	// so this is deliberately its OWN triple, not just "does the write's
	// own file contain a Bump call anywhere" (a weaker check that would
	// give dispatch_denial.go a false pass on the strength of a DIFFERENT
	// site's seam call in the same file).
	//
	// Exactly one of (seamCoveredPkgDir+seamCoveredFile+seamCoveredMarker)
	// or exemptReason is set, enforced by
	// TestCrossPackageRollupSeamRegistryCoverageIsHonest. A false
	// seamCovered claim is worse than an honest exemption (AGENTS.md: "an
	// inaccurate coverage claim is worse than an admitted gap"), so
	// exemptReason is REQUIRED, not optional, for anything not wired to
	// the seam.
	seamCoveredPkgDir string
	seamCoveredFile   string
	seamCoveredMarker string
	exemptReason      string
}

// crossPackageRollupSeamRegistry is the CHAOS-4586 registry-level guard:
// every terminal-status write to public.sync_run_units, in EVERY package,
// must appear here, mapped to either its seam call site or a written
// reason it is exempt. Completeness
// (TestCrossPackageRollupSeamRegistryIsComplete) fails loudly if a NEW
// terminal-status write appears anywhere in crossPackageDirs without being
// added here first -- forcing a deliberate decision about its rollup
// story, the same way CHAOS-4586 itself exists because CHAOS-4559 did not
// make that decision for five syncdispatchruntime mechanisms and a sixth
// (unrelated to either ticket) went unnoticed in syncreconciler until this
// registry was generalized to look for it.
var crossPackageRollupSeamRegistry = []crossPackageWriteSite{
	// --- providersync (CHAOS-4559's original fix) ---
	{
		pkgDir:            "../providersync",
		file:              "repository_postgres.go",
		snippet:           `SET status = 'success',`,
		seamCoveredPkgDir: "../providersync",
		seamCoveredFile:   "repository_postgres.go",
		seamCoveredMarker: `bumpSyncRunRollup(ctx, tx, claim.SyncRunID)`,
	},
	{
		pkgDir:            "../providersync",
		file:              "repository_postgres.go",
		snippet:           `SET status = 'failed',`,
		seamCoveredPkgDir: "../providersync",
		seamCoveredFile:   "repository_postgres.go",
		seamCoveredMarker: `bumpSyncRunRollup(ctx, tx, claim.SyncRunID)`,
	},
	// --- syncdispatchruntime (CHAOS-4586) ---
	{
		pkgDir:            "../syncdispatchruntime",
		file:              "budget_chokepoint.go",
		snippet:           `unit.id, syncRunUnitStatusFailed, verdict.errorText, resultJSON, now,`,
		seamCoveredPkgDir: "../syncdispatchruntime",
		seamCoveredFile:   "budget_chokepoint.go",
		seamCoveredMarker: `bumpSyncRunRollup(ctx, tx, unit.syncRunID)`,
	},
	{
		// failPlannedUnits: called from BOTH of denyRun's branches. The
		// hasActive branch (which leaves the run non-terminal -- CHAOS-4586's
		// actual bug) now bumps the seam; the sibling !hasActive branch
		// terminalizes the WHOLE run immediately via its own direct
		// sync_runs write (never sync_run_units, so out of THIS registry's
		// scope by definition), which was already correct before this ticket.
		pkgDir:            "../syncdispatchruntime",
		file:              "dispatch_denial.go",
		snippet:           `syncRunID, syncRunUnitStatusFailed, errorText, now, syncRunUnitStatusPlanned, syncRunUnitStatusRetrying)`,
		seamCoveredPkgDir: "../syncdispatchruntime",
		seamCoveredFile:   "native_dispatch_sync_run_service.go",
		seamCoveredMarker: `recordRollupBump(ctx, rollupPathDenied)`,
	},
	{
		pkgDir:            "../syncdispatchruntime",
		file:              "dispatch_denial.go",
		snippet:           `syncRunID, syncRunUnitStatusFailed, errorText, ` + "`" + `{"error_category":"dispatch_denied"}` + "`" + `, now,`,
		seamCoveredPkgDir: "../syncdispatchruntime",
		seamCoveredFile:   "native_dispatch_sync_run_service.go",
		seamCoveredMarker: `recordRollupBump(ctx, rollupPathDenied)`,
	},
	{
		pkgDir:            "../syncdispatchruntime",
		file:              "dispatch_denial.go",
		snippet:           `unitIDs, syncRunUnitStatusFailed, featureDisabledErrorCategory, reason, resultJSON, now, syncRunUnitStatusDispatching)`,
		seamCoveredPkgDir: "../syncdispatchruntime",
		seamCoveredFile:   "dispatch_denial.go",
		seamCoveredMarker: `recordRollupBump(ctx, rollupPathUnroutable)`,
	},
	{
		pkgDir:            "../syncdispatchruntime",
		file:              "dispatch_denial.go",
		snippet:           `unitIDs, syncRunUnitStatusFailed, invalidProviderFamilyClaimErrorCategory, reason, resultJSON, now, syncRunUnitStatusDispatching)`,
		seamCoveredPkgDir: "../syncdispatchruntime",
		seamCoveredFile:   "dispatch_denial.go",
		seamCoveredMarker: `recordRollupBump(ctx, rollupPathInvalidClaim)`,
	},
	{
		// codex round 10, P1: this entry was WRONGLY exempted as "already
		// correct" on the strength of doing a full COUNT(*) recompute --
		// necessary but not sufficient, since (unlike syncrunrollup.Bump)
		// it did the count via a separate, un-lock-protected query into Go
		// variables, then wrote them later with no compare-and-swap. A
		// concurrent Bump call on the same run committing in between could
		// have its fresh counts silently overwritten. Fixed by locking the
		// run first (syncrunrollup.LockRun) before counting, and now also
		// records the rollup bump under its own path
		// ("feature_disabled") -- the SAME telemetry convention every
		// other seam-covered entry in this registry uses, even though it
		// reaches the recompute via LockRun's protection rather than a
		// direct Bump call (Bump's own SQL doesn't return the running-unit
		// count this function also needs, and this function writes
		// several OTHER sync_runs columns in the same statement as the
		// rollup counters).
		pkgDir:            "../syncdispatchruntime",
		file:              "feature_disabled_termination.go",
		snippet:           `run.id, syncRunUnitStatusFailed, errorText, resultJSON, now); err != nil {`,
		seamCoveredPkgDir: "../syncdispatchruntime",
		seamCoveredFile:   "feature_disabled_termination.go",
		seamCoveredMarker: `recordRollupBump(ctx, rollupPathFeatureDisabled)`,
	},
	{
		pkgDir:            "../syncdispatchruntime",
		file:              "feature_disabled_termination.go",
		snippet:           `lease.unitID, syncRunUnitStatusFailed, errorText, resultJSON, now,`,
		seamCoveredPkgDir: "../syncdispatchruntime",
		seamCoveredFile:   "feature_disabled_termination.go",
		seamCoveredMarker: `recordRollupBump(ctx, rollupPathFeatureDisabled)`,
	},
	{
		pkgDir:            "../syncdispatchruntime",
		file:              "native_reference_discovery.go",
		snippet:           `runID, syncRunUnitStatusFailed, message, resultJSON, now,`,
		seamCoveredPkgDir: "../syncdispatchruntime",
		seamCoveredFile:   "native_reference_discovery.go",
		seamCoveredMarker: `recordRollupBump(ctx, rollupPathReferenceDiscoveryFailed)`,
	},
	{
		// The SAME failNonterminalUnits tx.Exec(...) call as the entry
		// immediately above, split across two source lines -- this is its
		// closing line (`status NOT IN ($6, $7)` args), not a second write.
		pkgDir:            "../syncdispatchruntime",
		file:              "native_reference_discovery.go",
		snippet:           `syncRunUnitStatusSuccess, syncRunUnitStatusFailed); err != nil {`,
		seamCoveredPkgDir: "../syncdispatchruntime",
		seamCoveredFile:   "native_reference_discovery.go",
		seamCoveredMarker: `recordRollupBump(ctx, rollupPathReferenceDiscoveryFailed)`,
	},
	// --- syncreconciler (CHAOS-4586, chris: "the guard must be the thing
	// that would have caught line 1144") ---
	{
		pkgDir:            "../syncreconciler",
		file:              "lease_repair.go",
		snippet:           `SET status = 'failed',`,
		seamCoveredPkgDir: "../syncreconciler",
		seamCoveredFile:   "lease_repair.go",
		seamCoveredMarker: `syncrunrollup.Bump(ctx, tx, candidate.syncRunID)`,
	},
	{
		// terminalizeUnreclaimableSQL and terminalizeTerminalDeliverySQL are
		// two alternative branches of the SAME terminalize() call -- both
		// share this exact SET clause text and both are covered by the ONE
		// Bump call terminalize() makes after either branch's Exec succeeds.
		pkgDir:            "../syncreconciler",
		file:              "unreclaimable_sweep.go",
		snippet:           `SET status = 'failed',`,
		seamCoveredPkgDir: "../syncreconciler",
		seamCoveredFile:   "unreclaimable_sweep.go",
		seamCoveredMarker: `syncrunrollup.Bump(ctx, tx, candidate.syncRunID)`,
	},
}

// terminalStatusLiteralRegexp matches the raw-SQL-literal write idiom used
// by providersync and syncreconciler: `SET status = 'failed'` /
// `SET status = 'success'` embedded directly in a query string.
var terminalStatusLiteralRegexp = regexp.MustCompile(`SET status = '(failed|success)'`)

// terminalStatusIdentifierRegexp matches syncdispatchruntime's own idiom:
// its syncRunUnitStatusFailed/syncRunUnitStatusSuccess Go constants, passed
// as a write argument rather than embedded as a literal.
var terminalStatusIdentifierRegexp = regexp.MustCompile(`\bsyncRunUnitStatus(Failed|Success)\b`)

// terminalStatusComparisonRegexp matches the status identifier ADJACENT to
// == or != (a comparison, e.g. `unit.status != syncRunUnitStatusFailed`),
// not just anywhere-on-the-line -- a plain `strings.Contains(line, "!=")`
// also excludes a legitimate write-argument line whose multi-line
// tx.Exec(...) call happens to close with `); err != nil {`.
var terminalStatusComparisonRegexp = regexp.MustCompile(
	`(==|!=)\s*syncRunUnitStatus(Failed|Success)\b|\bsyncRunUnitStatus(Failed|Success)\s*(==|!=)`,
)

// isPlausibleTerminalStatusWrite is a heuristic, not a SQL/AST parse (the
// brief permits an rg-based guard): a line that slips past this net
// without being registered fails TestCrossPackageRollupSeamRegistryIsComplete
// loudly rather than silently, which is the property that matters -- see
// that test's own doc comment for what to do when it fires.
func isPlausibleTerminalStatusWrite(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "//") {
		return false
	}
	if terminalStatusLiteralRegexp.MatchString(trimmed) {
		return true
	}
	if !terminalStatusIdentifierRegexp.MatchString(trimmed) {
		return false
	}
	// Exclude the two const definitions themselves (syncdispatchruntime's
	// native_finalize_sync_run.go: syncRunUnitStatusFailed = "failed").
	if strings.Contains(trimmed, `= "failed"`) || strings.Contains(trimmed, `= "success"`) {
		return false
	}
	// Exclude a comparison of the status identifier itself (e.g.
	// `unit.status != syncRunUnitStatusFailed`) -- but NOT a line that
	// merely also ends in an unrelated `err != nil` check, which a
	// multi-line tx.Exec(...) call's closing line commonly does (a bare
	// Contains(trimmed, "!=") false-excluded exactly this shape once,
	// caught by this test failing red against feature_disabled_termination.go).
	if terminalStatusComparisonRegexp.MatchString(trimmed) {
		return false
	}
	if strings.HasPrefix(trimmed, "case ") || strings.HasPrefix(trimmed, "func ") {
		return false
	}
	return true
}

// crossPackageSourceFiles lists the non-test .go files directly inside
// pkgDir (no recursion -- every package this registry cares about is a
// flat directory).
func crossPackageSourceFiles(t *testing.T, pkgDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(pkgDir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", pkgDir, err)
	}
	var files []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		files = append(files, name)
	}
	return files
}

// TestCrossPackageRollupSeamRegistryIsComplete fails if a terminal-status
// write to public.sync_run_units exists ANYWHERE in crossPackageDirs that
// crossPackageRollupSeamRegistry does not know about -- CHAOS-4586's
// registry-level guard, generalized across packages after a single-package
// version would have missed internal/syncreconciler's own instance of the
// same defect. If this fails because you added a NEW place that
// terminalizes a sync_run_units row: decide its rollup story (route it
// through syncrunrollup.Bump, or write a reason it is exempt) and add a
// matching entry to crossPackageRollupSeamRegistry in this file. Do not
// delete a registry entry to make this pass; only its target write site
// being genuinely removed from the source justifies that.
func TestCrossPackageRollupSeamRegistryIsComplete(t *testing.T) {
	found := map[string]bool{} // "pkgDir\x00file\x00snippet" -> matched
	var unregistered []string

	for _, pkgDir := range crossPackageDirs {
		for _, file := range crossPackageSourceFiles(t, pkgDir) {
			path := filepath.Join(pkgDir, file)
			handle, err := os.Open(path)
			if err != nil {
				t.Fatalf("open %s: %v", path, err)
			}
			scanner := bufio.NewScanner(handle)
			lineNum := 0
			for scanner.Scan() {
				lineNum++
				line := scanner.Text()
				if !isPlausibleTerminalStatusWrite(line) {
					continue
				}
				trimmed := strings.TrimSpace(line)
				matchedRegistryEntry := false
				for _, site := range crossPackageRollupSeamRegistry {
					if site.pkgDir == pkgDir && site.file == file && strings.Contains(trimmed, site.snippet) {
						found[site.pkgDir+"\x00"+site.file+"\x00"+site.snippet] = true
						matchedRegistryEntry = true
						break
					}
				}
				if !matchedRegistryEntry {
					unregistered = append(unregistered, path+":"+strconv.Itoa(lineNum)+": "+trimmed)
				}
			}
			if err := scanner.Err(); err != nil {
				t.Fatalf("scan %s: %v", path, err)
			}
			handle.Close()
		}
	}

	if len(unregistered) > 0 {
		t.Fatalf("found %d terminal-status write(s) not present in crossPackageRollupSeamRegistry -- "+
			"add an entry (seam-covered or exempt with a reason) before this can pass:\n%s",
			len(unregistered), strings.Join(unregistered, "\n"))
	}
	for _, site := range crossPackageRollupSeamRegistry {
		key := site.pkgDir + "\x00" + site.file + "\x00" + site.snippet
		if !found[key] {
			t.Fatalf("crossPackageRollupSeamRegistry entry for %s/%s (%q) no longer matches any line in "+
				"the source -- either the write site was removed (delete this entry) or its exact text "+
				"changed (update the snippet)", site.pkgDir, site.file, site.snippet)
		}
	}
}

// TestCrossPackageRollupSeamRegistryCoverageIsHonest checks that every
// non-exempt entry actually has its claimed seam call present, and every
// exempt entry carries a non-empty reason -- so this registry itself
// cannot silently drift into claiming coverage a later refactor removed.
func TestCrossPackageRollupSeamRegistryCoverageIsHonest(t *testing.T) {
	fileContents := map[string]string{} // "pkgDir\x00file" -> content
	for _, pkgDir := range crossPackageDirs {
		for _, file := range crossPackageSourceFiles(t, pkgDir) {
			content, err := os.ReadFile(filepath.Join(pkgDir, file))
			if err != nil {
				t.Fatalf("read %s/%s: %v", pkgDir, file, err)
			}
			fileContents[pkgDir+"\x00"+file] = string(content)
		}
	}

	for i, site := range crossPackageRollupSeamRegistry {
		hasSeam := site.seamCoveredPkgDir != "" && site.seamCoveredFile != "" && site.seamCoveredMarker != ""
		if hasSeam == (site.exemptReason != "") {
			t.Fatalf("crossPackageRollupSeamRegistry[%d] (%s/%s) must set EXACTLY ONE of "+
				"(seamCoveredPkgDir+seamCoveredFile+seamCoveredMarker) or exemptReason, never both, never neither",
				i, site.pkgDir, site.file)
		}
		if site.exemptReason != "" {
			continue
		}
		content, ok := fileContents[site.seamCoveredPkgDir+"\x00"+site.seamCoveredFile]
		if !ok {
			t.Fatalf("crossPackageRollupSeamRegistry[%d] (%s/%s) names seamCoveredPkgDir/File %s/%s, "+
				"which is not a source file this test scanned",
				i, site.pkgDir, site.file, site.seamCoveredPkgDir, site.seamCoveredFile)
		}
		if !strings.Contains(content, site.seamCoveredMarker) {
			t.Fatalf("crossPackageRollupSeamRegistry[%d]: %s/%s's terminal write no longer has its seam "+
				"call -- %q not found in %s/%s. A refactor likely dropped the syncrunrollup.Bump call "+
				"CHAOS-4586 added; restore it or update this registry with a written exemption reason.",
				i, site.pkgDir, site.file, site.seamCoveredMarker, site.seamCoveredPkgDir, site.seamCoveredFile)
		}
	}
}
