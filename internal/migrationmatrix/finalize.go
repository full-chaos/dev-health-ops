package migrationmatrix

// THIRD EXECUTOR STATE (CHAOS-5118-class fix, ported from the deleted
// scripts/gen_go_migration_matrix_docs.py by CHAOS-5473): a family's Executor
// verdict in native-families.json says only whether daily.go registers a
// native REPO-scope partition executor for it. run_daily_metrics_finalize
// (job_daily.py) is a separate, always-Python finalize step some families
// used to depend on for their team/finalize scope, independent of the repo
// scope's own native-ness -- a family whose repo scope went native could
// render bare NATIVE while Python still computed part of it.
//
// KNOWN, DELIBERATE SIMPLIFICATION vs. the deleted Python generator: that
// version walked job_daily.py's real AST, resolving local-variable aliases,
// destructured bindings, and for-loop/with-statement targets so a call
// routed through a variable could not evade the naming-convention check
// (round 1-3 codex findings, CHAOS-5118). This Go port does a regex scan of
// the function body's TEXT instead -- it finds every `name(` call token,
// bare or `.attr(`, exactly as the Python AST walker's func.id/func.attr
// did, but it does NOT resolve a call reached only through a local alias.
// This is safe TODAY because run_daily_metrics_finalize's real body (as of
// this port) contains zero calls of that shape at all -- every finalize-scope
// Python write it used to make (team_cognitive_load, compounding_risk_team,
// team_complexity, benchmarking, ic_finalize) has been deleted outright, not
// merely gated -- so load_daily_finalize_compat_families' real output is the
// empty set, byte-identically reproduced here. If a future PR reintroduces a
// finalize-scope Python write reached only through an alias, this scan will
// silently miss it; a direct or attribute call is still caught. Flagged in
// the CHAOS-5473 PR body for team-lead's call per the brief's own escape
// valve (byte-identical output today; a narrower drift-detection guarantee
// than the deleted script going forward).
//
// FINALIZE_CALL_IRREGULAR_FAMILY/FINALIZE_CALL_DORMANT_SKIP_GATED (see
// curated.go) are both empty today for the same reason -- this port keeps
// them as live extension points with the same completeness checks the
// deleted script ran, so the next instance of either class has somewhere to
// go.

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

var callTokenRe = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)\s*\(`)

// extractPythonFunctionBody returns the source text of a top-level `def
// <name>(` / `async def <name>(` function, from its signature line up to
// (excluding) the next non-blank, non-comment, column-0 line -- the next
// top-level statement. Column-0 comment lines are included on purpose: they
// are this function's own trailing documentation in job_daily.py, not a new
// top-level construct.
func extractPythonFunctionBody(src, name string) (string, error) {
	lines := strings.Split(src, "\n")
	def1 := "def " + name + "("
	def2 := "async def " + name + "("
	start := -1
	for i, line := range lines {
		if strings.HasPrefix(line, def1) || strings.HasPrefix(line, def2) {
			start = i
			break
		}
	}
	if start == -1 {
		return "", fmt.Errorf("function %s not found", name)
	}
	end := len(lines)
	for i := start + 1; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		if line[0] != ' ' && line[0] != '\t' {
			end = i
			break
		}
	}
	return strings.Join(lines[start:end], "\n"), nil
}

// loadFinalizeWriteCalls returns every call-like token's name found in the
// function body: a bare `name(` or an attribute `.name(` reads the same way
// Python's ast.Call did (func.id or func.attr) -- see the module comment for
// what this does NOT do (alias resolution).
func loadFinalizeWriteCalls(body string) map[string]bool {
	calls := map[string]bool{}
	for _, m := range callTokenRe.FindAllStringSubmatch(body, -1) {
		calls[m[1]] = true
	}
	return calls
}

var writeForDayRe = regexp.MustCompile(`^_write_(.+)_for_day$`)

// finalizeCallFamily maps one finalize call name to the (namespace, family)
// it writes/computes for, or reports it out of scope (ok=false, err=nil).
// Mirrors the deleted script's _finalize_call_family.
func finalizeCallFamily(callName string, dailyNames, remainingNames map[string]bool) (FinalizeTarget, bool, error) {
	if target, ok := FinalizeCallDormantSkipGated[callName]; ok {
		live := dailyNames
		if target.Namespace == "remaining" {
			live = remainingNames
		}
		if (target.Namespace != "daily" && target.Namespace != "remaining") || !live[target.Family] {
			return FinalizeTarget{}, false, fmt.Errorf(
				"FinalizeCallDormantSkipGated maps %q to (%q, %q), which is not a live %s family name -- "+
					"family renamed/removed, or namespace typo? Update or drop the entry.",
				callName, target.Namespace, target.Family, target.Namespace)
		}
		// Forced dormant: proven skip_families-gated, never a live write.
		return FinalizeTarget{}, false, nil
	}
	if target, ok := FinalizeCallIrregularFamily[callName]; ok {
		live := dailyNames
		if target.Namespace == "remaining" {
			live = remainingNames
		}
		if (target.Namespace != "daily" && target.Namespace != "remaining") || !live[target.Family] {
			return FinalizeTarget{}, false, fmt.Errorf(
				"FinalizeCallIrregularFamily maps %q to (%q, %q), which is not a live %s family name -- "+
					"family renamed/removed, or namespace typo? Update the ledger.",
				callName, target.Namespace, target.Family, target.Namespace)
		}
		if !irregularMappingPlausible(callName, target.Family) {
			return FinalizeTarget{}, false, fmt.Errorf(
				"FinalizeCallIrregularFamily maps %q to %q, but no word of %q appears in the call name -- "+
					"likely a copy/paste or rename error. Re-verify this mapping.", callName, target.Family, target.Family)
		}
		return target, true, nil
	}
	m := writeForDayRe.FindStringSubmatch(callName)
	if m == nil {
		return FinalizeTarget{}, false, nil
	}
	middle := m[1]
	matches := func(names map[string]bool) []string {
		var out []string
		for family := range names {
			if middle == family || strings.HasPrefix(middle, family+"_") {
				out = append(out, family)
			}
		}
		sort.Strings(out)
		return out
	}
	dailyMatches := matches(dailyNames)
	remainingMatches := matches(remainingNames)
	if len(dailyMatches) > 1 {
		return FinalizeTarget{}, false, fmt.Errorf(
			"finalize call %q matches MULTIPLE daily families via the naming convention: %v -- "+
				"ambiguous. Add an explicit FinalizeCallIrregularFamily entry naming the intended one.", callName, dailyMatches)
	}
	if len(remainingMatches) > 1 {
		return FinalizeTarget{}, false, fmt.Errorf(
			"finalize call %q matches MULTIPLE remaining families via the naming convention: %v -- "+
				"ambiguous. Add an explicit FinalizeCallIrregularFamily entry naming the intended one.", callName, remainingMatches)
	}
	var dailyMatch, remainingMatch string
	if len(dailyMatches) == 1 {
		dailyMatch = dailyMatches[0]
	}
	if len(remainingMatches) == 1 {
		remainingMatch = remainingMatches[0]
	}
	if dailyMatch != "" && remainingMatch != "" {
		return FinalizeTarget{}, false, fmt.Errorf(
			"finalize call %q matches family %q via the naming convention in BOTH the daily and remaining "+
				"family sets -- ambiguous namespace. Add an explicit FinalizeCallIrregularFamily entry naming "+
				"which section this call belongs to.", callName, dailyMatch)
	}
	if dailyMatch != "" {
		return FinalizeTarget{Namespace: "daily", Family: dailyMatch}, true, nil
	}
	if remainingMatch != "" {
		return FinalizeTarget{Namespace: "remaining", Family: remainingMatch}, true, nil
	}
	return FinalizeTarget{}, false, fmt.Errorf(
		"finalize call %q matches the `_write_<family>_..._for_day` naming convention but names no live "+
			"daily or remaining family -- a new Python finalize write was added with no matching family/ticket. "+
			"Add the family if it's genuinely new, or map this call in FinalizeCallIrregularFamily if the name "+
			"is just irregular.", callName)
}

func irregularMappingPlausible(callName, family string) bool {
	if strings.HasPrefix(callName, "_write_") && strings.HasSuffix(callName, "_for_day") {
		middle := strings.TrimSuffix(strings.TrimPrefix(callName, "_write_"), "_for_day")
		callTokens := toSet(strings.Split(middle, "_"))
		familyTokens := []string{}
		for _, t := range strings.Split(family, "_") {
			if t != "" {
				familyTokens = append(familyTokens, t)
			}
		}
		if len(familyTokens) == 0 {
			return true
		}
		for _, t := range familyTokens {
			if !callTokens[t] {
				return false
			}
		}
		return true
	}
	var tokens []string
	for _, t := range strings.Split(family, "_") {
		if len(t) >= 2 {
			tokens = append(tokens, t)
		}
	}
	if len(tokens) == 0 {
		return true
	}
	for _, t := range tokens {
		if strings.Contains(callName, t) {
			return true
		}
	}
	return false
}

// assertNoStaleFinalizeLedgerEntries mirrors the other completeness
// direction: every FinalizeCallIrregularFamily/FinalizeCallDormantSkipGated
// entry must name a call actually present in the function body right now.
func assertNoStaleFinalizeLedgerEntries(presentCalls map[string]bool, dailyNames, remainingNames map[string]bool) error {
	for callName, target := range FinalizeCallDormantSkipGated {
		if !presentCalls[callName] {
			return fmt.Errorf("FinalizeCallDormantSkipGated names call %q that no longer appears in "+
				"run_daily_metrics_finalize's body -- renamed or removed? Update or drop the entry.", callName)
		}
		live := dailyNames
		if target.Namespace == "remaining" {
			live = remainingNames
		}
		if (target.Namespace != "daily" && target.Namespace != "remaining") || !live[target.Family] {
			return fmt.Errorf("FinalizeCallDormantSkipGated maps %q to (%q, %q), which is not a live %s family name.",
				callName, target.Namespace, target.Family, target.Namespace)
		}
	}
	for callName, target := range FinalizeCallIrregularFamily {
		if !presentCalls[callName] {
			return fmt.Errorf("FinalizeCallIrregularFamily names call %q that no longer appears in "+
				"run_daily_metrics_finalize's body -- renamed or removed? Update or drop the ledger entry.", callName)
		}
		live := dailyNames
		if target.Namespace == "remaining" {
			live = remainingNames
		}
		if (target.Namespace != "daily" && target.Namespace != "remaining") || !live[target.Family] {
			return fmt.Errorf("FinalizeCallIrregularFamily maps %q to (%q, %q), which is not a live %s family name.",
				callName, target.Namespace, target.Family, target.Namespace)
		}
	}
	return nil
}

const dailyFinalizeFunc = "run_daily_metrics_finalize"

// LoadDailyFinalizeCompatFamilies reads jobDailyPyPath and returns every live
// (namespace, family) with a still-Python finalize-scope write, proven by an
// actual call inside run_daily_metrics_finalize's body. See the module
// comment for the scan's scope and its one known gap vs. the deleted
// Python generator.
func LoadDailyFinalizeCompatFamilies(jobDailyPyPath string, dailyNames, remainingNames []string) (map[FinalizeTarget]bool, error) {
	raw, err := os.ReadFile(jobDailyPyPath) //nolint:gosec // repo-relative path
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", jobDailyPyPath, err)
	}
	body, err := extractPythonFunctionBody(string(raw), dailyFinalizeFunc)
	if err != nil {
		return nil, fmt.Errorf("%s in %s: %w -- function renamed/moved? Update LoadDailyFinalizeCompatFamilies.",
			dailyFinalizeFunc, jobDailyPyPath, err)
	}
	dailySet := toSet(dailyNames)
	remainingSet := toSet(remainingNames)
	presentCalls := loadFinalizeWriteCalls(body)

	if err := assertNoStaleFinalizeLedgerEntries(presentCalls, dailySet, remainingSet); err != nil {
		return nil, err
	}

	compat := map[FinalizeTarget]bool{}
	names := make([]string, 0, len(presentCalls))
	for name := range presentCalls {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, callName := range names {
		target, ok, err := finalizeCallFamily(callName, dailySet, remainingSet)
		if err != nil {
			return nil, err
		}
		if ok {
			compat[target] = true
		}
	}
	return compat, nil
}
