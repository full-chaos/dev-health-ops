package migrationmatrix

// This file replaces scripts/gen_go_migration_matrix_docs.py (CHAOS-5473):
// the four blocks below were the last generated content on
// docs/go-migration-matrix.md still produced by Python. The split mirrors
// the deleted script's own module docstring exactly -- see git history for
// scripts/gen_go_migration_matrix_docs.py if that context is needed again.
//
//  1. Provider sync -- rendered straight from
//     contracts/provider-matrix/v1/matrix.json (CUT-08), no curated ledger.
//  2. Daily metrics families -- families.json's family-name set, cross-checked
//     against DailyCitationLedger; the Executor verdict comes from
//     native-families.json ("daily" + "finalize" sections merged), never from
//     families.json's own "port" field or a curated dict.
//  3. Remaining metrics families -- same shape, RemainingExecutorLedger,
//     native-families.json's "remaining" section.
//  4. Workgraph/investment -- entirely hand-maintained (no families.json
//     equivalent exists); checked against native-families.json's "workgraph"
//     section by _assertLedgerMatchesArtifact.
//
// A THIRD EXECUTOR STATE feeds §2/§3: native-families.json only knows whether
// a family's repo-scope partition executor is native -- it says nothing about
// run_daily_metrics_finalize (job_daily.py), a separate, always-Python
// finalize step some families used to depend on. See finalize.go.

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// readJSONLoose decodes a JSON file into v without rejecting unknown fields
// -- unlike LoadStatusLedger/LoadRender, the files this reads
// (matrix.json, families.json) are owned by other contracts/tests and carry
// many fields this page never renders.
func readJSONLoose(path string, v any) error {
	raw, err := os.ReadFile(path) //nolint:gosec // caller-supplied repo path
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// Marker pairs bounding the four blocks this file renders. Identical strings
// to the ones the deleted Python generator used, so the document needs no
// marker changes.
const (
	ProviderSyncBegin        = "<!-- BEGIN GENERATED PROVIDER SYNC MATRIX -->"
	ProviderSyncEnd          = "<!-- END GENERATED PROVIDER SYNC MATRIX -->"
	DailyMetricsBegin        = "<!-- BEGIN GENERATED DAILY METRICS MATRIX -->"
	DailyMetricsEnd          = "<!-- END GENERATED DAILY METRICS MATRIX -->"
	RemainingMetricsBegin    = "<!-- BEGIN GENERATED REMAINING METRICS MATRIX -->"
	RemainingMetricsEnd      = "<!-- END GENERATED REMAINING METRICS MATRIX -->"
	WorkgraphInvestmentBegin = "<!-- BEGIN GENERATED WORKGRAPH INVESTMENT MATRIX -->"
	WorkgraphInvestmentEnd   = "<!-- END GENERATED WORKGRAPH INVESTMENT MATRIX -->"
)

// ProviderMatrixPair is one row of contracts/provider-matrix/v1/matrix.json's
// "pairs" array. Only the fields this page renders are modeled; the file
// carries many more the provider-matrix contract test owns.
type ProviderMatrixPair struct {
	Provider          string   `json:"provider"`
	Dataset           string   `json:"dataset"`
	GoExecutor        string   `json:"go_executor"`
	RouteDestinations []string `json:"route_destinations"`
	RouteReady        bool     `json:"route_ready"`
	Plannable         bool     `json:"plannable"`
}

// LoadProviderMatrixPairs reads matrix.json's pairs.
func LoadProviderMatrixPairs(path string) ([]ProviderMatrixPair, error) {
	var file struct {
		Pairs []ProviderMatrixPair `json:"pairs"`
	}
	if err := readJSONLoose(path, &file); err != nil {
		return nil, fmt.Errorf("read provider matrix: %w", err)
	}
	return file.Pairs, nil
}

// LoadFamilyNames reads a families.json's ["families"][*]["name"] list. Used
// for both internal/jobs/metrics/daily/families.json and its remaining/
// sibling -- this page only needs the name set, never the other curated
// fields those files also carry (those belong to the Go worker's own
// registration wiring, not to this doc).
func LoadFamilyNames(path string) ([]string, error) {
	var file struct {
		Families []struct {
			Name string `json:"name"`
		} `json:"families"`
	}
	if err := readJSONLoose(path, &file); err != nil {
		return nil, fmt.Errorf("read families: %w", err)
	}
	names := make([]string, 0, len(file.Families))
	for _, f := range file.Families {
		names = append(names, f.Name)
	}
	return names, nil
}

func toSet(items []string) map[string]bool {
	out := make(map[string]bool, len(items))
	for _, item := range items {
		out[item] = true
	}
	return out
}

func sortedSet(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// consistencyGuard is the same completeness check the deleted Python
// generator's `_consistency_guard` ran before every render: refuse rather
// than silently render a page that is missing a family, or still describing
// one that no longer exists.
func consistencyGuard(label string, live, curated map[string]bool, hint string) error {
	var missing, extra []string
	for name := range live {
		if !curated[name] {
			missing = append(missing, name)
		}
	}
	for name := range curated {
		if !live[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 {
		return fmt.Errorf("%s %v exist in the live producer but have no curated row. %s", label, missing, hint)
	}
	if len(extra) > 0 {
		return fmt.Errorf("%s %v have a curated row but no longer exist in the live producer -- remove the stale row (or, if renamed, update it). %s", label, extra, hint)
	}
	return nil
}

func pyBool(b bool) string {
	if b {
		return "True"
	}
	return "False"
}

// RenderProviderSyncBlock renders §1. No curated ledger: every cell comes
// straight from matrix.json, the frozen, CI-verified provider x dataset
// parity contract both internal/providersync and the Python sync worker
// already drift-test against.
func RenderProviderSyncBlock(pairs []ProviderMatrixPair) (string, error) {
	unknown := map[string]bool{}
	for _, p := range pairs {
		if _, ok := GoExecutorTranslation[p.GoExecutor]; !ok {
			unknown[p.GoExecutor] = true
		}
	}
	if len(unknown) > 0 {
		return "", fmt.Errorf("matrix.json has go_executor value(s) %v not in GoExecutorTranslation -- "+
			"add a legend mapping for it before rendering", sortedSet(unknown))
	}

	sorted := append([]ProviderMatrixPair(nil), pairs...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Provider != sorted[j].Provider {
			return sorted[i].Provider < sorted[j].Provider
		}
		return sorted[i].Dataset < sorted[j].Dataset
	})

	var b strings.Builder
	b.WriteString("| Provider | Dataset | Executor | Route destinations (tables written) | Route ready | Plannable |\n")
	b.WriteString("| --- | --- | --- | --- | --- | --- |\n")
	for _, p := range sorted {
		executor := GoExecutorTranslation[p.GoExecutor]
		destinations := "--"
		if len(p.RouteDestinations) > 0 {
			quoted := make([]string, len(p.RouteDestinations))
			for i, d := range p.RouteDestinations {
				quoted[i] = "`" + d + "`"
			}
			destinations = strings.Join(quoted, ", ")
		}
		fmt.Fprintf(&b, "| %s | `%s` | %s | %s | %s | %s |\n",
			p.Provider, p.Dataset, executor, destinations, pyBool(p.RouteReady), pyBool(p.Plannable))
	}
	return b.String(), nil
}

func isCompatExecutor(executor string) bool {
	return strings.Contains(executor, "COMPAT-Python")
}

func dailyFamilyExecutor(name string, artifactDaily map[string]string, finalizeCompat map[FinalizeTarget]bool) string {
	artifactValue, ok := artifactDaily[name]
	if !ok {
		artifactValue = "compat"
	}
	executor := "NATIVE"
	if artifactValue == "compat" {
		executor = "COMPAT-Python"
	}
	if artifactValue == "post_bridge" {
		executor += ", post_bridge"
	}
	if finalizeCompat[FinalizeTarget{Namespace: "daily", Family: name}] && artifactValue != "compat" {
		executor = executor + " (repo) / COMPAT-Python (finalize)"
	}
	return executor
}

func remainingFamilyExecutor(name string, artifactRemaining map[string]string, finalizeCompat map[FinalizeTarget]bool) string {
	executor := "COMPAT-Python"
	if artifactRemaining[name] == "native" {
		executor = "NATIVE"
	}
	if name == "work_item_attribution" {
		executor += " (narrow: staleness backstop only)"
	}
	if finalizeCompat[FinalizeTarget{Namespace: "remaining", Family: name}] && artifactRemaining[name] == "native" {
		executor = executor + " (repo) / COMPAT-Python (finalize)"
	}
	return executor
}

func mergeStringMaps(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

// RenderDailyMetricsBlock renders §2. See the module comment for the split
// between "which family exists" (families.json), "what executes it"
// (native-families.json daily+finalize), and "the citation prose"
// (DailyCitationLedger).
func RenderDailyMetricsBlock(dailyNames []string, artifact *NativeFamilies, finalizeCompat map[FinalizeTarget]bool) (string, error) {
	liveNames := toSet(dailyNames)
	curated := map[string]bool{}
	for name := range DailyCitationLedger {
		curated[name] = true
	}
	if err := consistencyGuard("daily metrics family(ies)", liveNames, curated,
		"Add a DailyCitationLedger row for it."); err != nil {
		return "", err
	}

	// §2 covers every daily-metrics family, partition- AND finalize-scope
	// alike -- merging "finalize" in here fixes the staleness CHAOS-5141
	// found: before the merge, every finalize-scope native family showed
	// COMPAT-Python regardless of its actual port.
	artifactDaily := mergeStringMaps(artifact.Daily, artifact.Finalize)
	unknown := map[string]bool{}
	for name := range artifactDaily {
		if !liveNames[name] {
			unknown[name] = true
		}
	}
	if len(unknown) > 0 {
		return "", fmt.Errorf("native-families.json's \"daily\"/\"finalize\" sections name family(ies) %v "+
			"that no longer exist in internal/jobs/metrics/daily/families.json -- regenerate the artifact "+
			"(UPDATE_NATIVE_FAMILIES_ARTIFACT=1 go test ./cmd/dev-health-worker/... -run TestNativeFamiliesArtifactUpToDate)",
			sortedSet(unknown))
	}

	var b strings.Builder
	b.WriteString("| Family | Executor | Citation | Ticket |\n")
	b.WriteString("| --- | --- | --- | --- |\n")
	for _, name := range sortedSet(liveNames) {
		executor := dailyFamilyExecutor(name, artifactDaily, finalizeCompat)
		row := DailyCitationLedger[name]
		fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", name, executor, row.Citation, row.Ticket)
	}
	return b.String(), nil
}

// RenderRemainingMetricsBlock renders §3.
func RenderRemainingMetricsBlock(remainingNames []string, artifactRemaining map[string]string, finalizeCompat map[FinalizeTarget]bool) (string, error) {
	liveNames := toSet(remainingNames)
	curated := map[string]bool{}
	for name := range RemainingExecutorLedger {
		curated[name] = true
	}
	if err := consistencyGuard("remaining metrics family(ies)", liveNames, curated,
		"Add a RemainingExecutorLedger row for it (citation/ticket text; the Executor verdict comes from "+
			"native-families.json, not this map)."); err != nil {
		return "", err
	}
	artifactNames := map[string]bool{}
	for name := range artifactRemaining {
		artifactNames[name] = true
	}
	if err := consistencyGuard("remaining metrics family(ies) in native-families.json", artifactNames, liveNames,
		"Regenerate the artifact (UPDATE_NATIVE_FAMILIES_ARTIFACT=1 go test ./cmd/dev-health-worker/... "+
			"-run TestNativeFamiliesArtifactUpToDate)."); err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString("| Family | Executor | Citation | Route transport | Ticket |\n")
	b.WriteString("| --- | --- | --- | --- | --- |\n")
	for _, name := range sortedSet(curated) {
		executor := remainingFamilyExecutor(name, artifactRemaining, finalizeCompat)
		row := RemainingExecutorLedger[name]
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", name, executor, row.Citation, row.Route, row.Ticket)
	}
	return b.String(), nil
}

// workgraphLabelToLedgerPrefix mirrors the deleted script's
// _ARTIFACT_LABEL_TO_LEDGER_PREFIX: the artifact is AST-derived from
// addWorkgraphWorker's dispatch switch, so it -- not the curated ledger -- is
// the authority on native-vs-compat.
var workgraphLabelToLedgerPrefix = map[string][]string{
	"native": {"NATIVE"},
	"compat": {"COMPAT-Python", "PYTHON-ONLY"},
}

func assertWorkgraphLedgerMatchesArtifact(artifactWorkgraph map[string]string) error {
	if len(artifactWorkgraph) == 0 {
		return fmt.Errorf("native-families.json has no `workgraph` section -- regenerate it with " +
			"UPDATE_NATIVE_FAMILIES_ARTIFACT=1 go test ./cmd/dev-health-worker/... -run TestNativeFamiliesArtifactUpToDate")
	}
	kinds := make([]string, 0, len(artifactWorkgraph))
	for kind := range artifactWorkgraph {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		label := artifactWorkgraph[kind]
		row, ok := WorkgraphInvestmentLedger[kind]
		if !ok {
			return fmt.Errorf("native-families artifact records kind %q but WorkgraphInvestmentLedger has no entry for it -- add one", kind)
		}
		expected, ok := workgraphLabelToLedgerPrefix[label]
		if !ok {
			return fmt.Errorf("native-families artifact records kind %q with an unrecognized label %q", kind, label)
		}
		matched := false
		for _, prefix := range expected {
			if strings.HasPrefix(row.Executor, prefix) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("kind %q is wired as %q in addWorkgraphWorker (per the native-families artifact) but the "+
				"curated ledger says %q. The wiring is authoritative -- fix the ledger", kind, label, row.Executor)
		}
	}

	// The OTHER direction (CHAOS-5153): a ledger row whose kind is no longer
	// in the artifact is a STALE row describing dead code by omission.
	// recommendations/DORA/cognitive-load rows are cross-references to §2/§3
	// (their own "citation": "see §2"/"see §3"), not workgraph-wiring facts,
	// so they are exempt (no "." in the name).
	names := make([]string, 0, len(WorkgraphInvestmentLedger))
	for name := range WorkgraphInvestmentLedger {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !strings.Contains(name, ".") {
			continue
		}
		if _, ok := artifactWorkgraph[name]; !ok {
			return fmt.Errorf("WorkgraphInvestmentLedger has a row for %q, but the native-families artifact no longer "+
				"records it -- its addWorkgraphWorker case was removed. Delete it from WorkgraphInvestmentLedger", name)
		}
	}
	return nil
}

// RenderWorkgraphInvestmentBlock renders §4.
func RenderWorkgraphInvestmentBlock(artifactWorkgraph map[string]string) (string, error) {
	if err := assertWorkgraphLedgerMatchesArtifact(artifactWorkgraph); err != nil {
		return "", err
	}
	names := make([]string, 0, len(WorkgraphInvestmentLedger))
	for name := range WorkgraphInvestmentLedger {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	b.WriteString("| Kind/area | Executor | Citation | Route transport | Ticket |\n")
	b.WriteString("| --- | --- | --- | --- | --- |\n")
	for _, name := range names {
		row := WorkgraphInvestmentLedger[name]
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", name, row.Executor, row.Citation, row.Route, row.Ticket)
	}
	return b.String(), nil
}
