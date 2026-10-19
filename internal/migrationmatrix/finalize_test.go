package migrationmatrix

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempPy(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "job_daily.py")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write temp job_daily.py: %v", err)
	}
	return path
}

func TestLoadDailyFinalizeCompatFamiliesDetectsARealWriteCall(t *testing.T) {
	py := "def run_daily_metrics_finalize():\n" +
		"    _write_capacity_extra_for_day(day, org_id)\n" +
		"\n" +
		"def other():\n" +
		"    pass\n"
	path := writeTempPy(t, py)
	compat, err := LoadDailyFinalizeCompatFamilies(path, []string{"repo_user_commit"}, []string{"capacity"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := FinalizeTarget{Namespace: "remaining", Family: "capacity"}
	if !compat[want] {
		t.Fatalf("expected %+v in the compat set, got %v", want, compat)
	}
}

func TestLoadDailyFinalizeCompatFamiliesRejectsAnAmbiguousNamespace(t *testing.T) {
	// "work_item_attribution" is a live family in BOTH namespaces -- a call
	// naming it via the plain convention is genuinely ambiguous.
	py := "def run_daily_metrics_finalize():\n" +
		"    _write_work_item_attribution_for_day(day, org_id)\n"
	path := writeTempPy(t, py)
	_, err := LoadDailyFinalizeCompatFamilies(path, []string{"work_item_attribution"}, []string{"work_item_attribution"})
	if err == nil {
		t.Fatal("expected the both-namespaces ambiguity to refuse")
	}
}

func TestLoadDailyFinalizeCompatFamiliesIgnoresPlainProseMentions(t *testing.T) {
	// A comment merely NAMING a call (no trailing paren) must not be treated
	// as a call -- this is the shape job_daily.py's own trailing history
	// comments use pervasively.
	py := "def run_daily_metrics_finalize():\n" +
		"    # see _write_capacity_for_day for history\n" +
		"    logger.info(\"done\")\n"
	path := writeTempPy(t, py)
	compat, err := LoadDailyFinalizeCompatFamilies(path, []string{"repo_user_commit"}, []string{"capacity"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(compat) != 0 {
		t.Fatalf("expected no calls from prose alone, got %v", compat)
	}
}

// TestScanPinsTheDocumentedAliasGapAsMeasuredBehaviour is the team-lead
// condition on accepting the regex text-scan in place of the deleted
// script's full AST alias-resolver (CHAOS-5473 gate TELL): a direct call is
// caught; a call reached only through an import alias or a local-variable
// alias is NOT -- pinned here as measured behaviour, not prose, so a future
// change to the scan's scope has a test to break.
func TestScanPinsTheDocumentedAliasGapAsMeasuredBehaviour(t *testing.T) {
	dailyNames := []string{"repo_user_commit"}
	remainingNames := []string{"capacity"}
	target := FinalizeTarget{Namespace: "remaining", Family: "capacity"}

	t.Run("direct call is caught", func(t *testing.T) {
		py := "def run_daily_metrics_finalize():\n" +
			"    _write_capacity_extra_for_day(day, org_id)\n"
		compat, err := LoadDailyFinalizeCompatFamilies(writeTempPy(t, py), dailyNames, remainingNames)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !compat[target] {
			t.Fatalf("expected a direct call to be caught, got %v", compat)
		}
	})

	t.Run("import-alias call is missed", func(t *testing.T) {
		// `from finalize_writers import _write_capacity_extra_for_day as
		// _aliased_write` then calling `_aliased_write(...)` -- the call
		// TOKEN in the body is "_aliased_write(", which never matches the
		// `_write_..._for_day` naming convention, so this scan (unlike the
		// deleted script's AST walker, which read func.id/func.attr the
		// same way but could not follow an IMPORT alias either -- only a
		// LOCAL one) misses it exactly as prose above claims.
		py := "from finalize_writers import _write_capacity_extra_for_day as _aliased_write\n\n" +
			"def run_daily_metrics_finalize():\n" +
			"    _aliased_write(day, org_id)\n"
		compat, err := LoadDailyFinalizeCompatFamilies(writeTempPy(t, py), dailyNames, remainingNames)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if compat[target] {
			t.Fatal("expected an import-alias call to be missed by this scan (documented gap) -- it was caught instead")
		}
	})

	t.Run("local-variable-alias call is missed", func(t *testing.T) {
		// `writer = _write_capacity_extra_for_day` then `writer(...)` --
		// the deleted script's AST walker resolved exactly this shape
		// (round-1/2 codex findings, CHAOS-5118); this text scan does not.
		py := "def run_daily_metrics_finalize():\n" +
			"    writer = _write_capacity_extra_for_day\n" +
			"    writer(day, org_id)\n"
		compat, err := LoadDailyFinalizeCompatFamilies(writeTempPy(t, py), dailyNames, remainingNames)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if compat[target] {
			t.Fatal("expected a local-variable-alias call to be missed by this scan (documented gap) -- it was caught instead")
		}
	})
}

func TestFinalizeCallFamilyOutOfScopeForGenericInfrastructure(t *testing.T) {
	target, ok, err := finalizeCallFamily("logger.info", map[string]bool{}, map[string]bool{})
	if err != nil || ok {
		t.Fatalf("expected a non-matching call to be out of scope, got target=%v ok=%v err=%v", target, ok, err)
	}
}

func repoRootForTest(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find the module root")
		}
		dir = parent
	}
}
