package exceptionledger_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/exceptionledger"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repository root %s: %v", root, err)
	}
	return root
}

func TestTheRealTreeMatchesTheLedger(t *testing.T) {
	root := repoRoot(t)
	ledger, err := os.ReadFile(filepath.Join(root, exceptionledger.LedgerPath))
	if err != nil {
		t.Fatal(err)
	}
	defects, err := exceptionledger.Check(root, ledger)
	if err != nil {
		t.Fatal(err)
	}
	for _, defect := range defects {
		t.Error(defect)
	}
	rows, _ := exceptionledger.ParseLedger(ledger)
	sites, err := exceptionledger.Discover(root)
	if err != nil {
		t.Fatal(err)
	}
	exact := 0
	for _, site := range sites {
		if site.Exact {
			exact++
		}
	}
	if len(rows) < 25 || len(sites) < 12 || exact < 7 || len(sites) == exact {
		t.Fatalf("vacuous census: %d rows, %d sites (%d exact); the scan or the ledger lost its rows", len(rows), len(sites), exact)
	}
}

const (
	baseProfile = `{"rows":[{"access_token_validation":{"legacy_hs256":true}},{"access_token_validation":{"legacy_hs256":false}},{"impersonation_policy":"unknown","notes":["impersonation_policy: declared gap, tracked in CHAOS-1"]}]}`
	basePDP     = "package pdp\n\nconst (\n\tClassHS256 = \"h\"\n\tClassEnvelope = \"e\"\n)\n\nvar ExemptClasses = []string{ClassHS256}\n"
	baseQuirks  = "package authrows\n\nvar Quirks = []Quirk{{\n\tID: \"q\", Kind: Gap,\n\tTracked: \"CHAOS-2\",\n}}\n"
	baseWriter  = "package writerinventory\n\n// TEMPORARY: the legacy writer row.\nvar A = 1\n"
	baseRefusal = "# id\tgo_api\tauth_service\tkind\treason\n" +
		"r_same\t400\t400\tsame\tok\n" +
		"r_div\t200\t422\tdivergent\tnamed gap\n"
	baseLedger = "# fixture\n" +
		"quirk-q\ttemporary\tinternal/testsupport/authrows/quirks.go\tTracked: \"CHAOS-2\"\tCHAOS-20\topen\tGap quirk\n" +
		"writer-legacy\ttemporary\tinternal/auth/writer.go\t// TEMPORARY: the legacy writer\tCHAOS-21\topen\tlegacy writer\n" +
		"profile-legacy\ttemporary\tcontracts/auth/v1/endpoint-profiles.go-api.json\tcount:legacy_hs256=1\tCHAOS-22\topen\tlegacy rows\n" +
		"profile-gaps\ttemporary\tcontracts/auth/v1/endpoint-profiles.go-api.json\tcount:declared_gap_rows=1\tCHAOS-23\topen\tdeclared gaps\n" +
		"pdp-hs256\ttemporary\tinternal/queryapi/pdp/pdp.go\tExemptClasses:ClassHS256\tCHAOS-24\topen\texempt class\n" +
		"refusal-div\ttemporary\tci/x_refusals.tsv\tdivergent:r_div\tCHAOS-25\topen\tnamed gap\n" +
		"anchor-only\ttemporary\tci/closure.tsv\tgithub.com/x/jwt\tCHAOS-26\topen\tlinked module\n" +
		"permanent-row\tpermanent\tci/closure.tsv\tgithub.com/x/jwt\tD1\t-\tan operator input ruled permanent by D1\n"
)

// fixture is a tree on which the census over baseLedger is clean; each cell plants one defect into a copy.
func fixture() map[string]string {
	files := map[string]string{
		"contracts/auth/v1/endpoint-profiles.go-api.json": baseProfile,
		"contracts/auth/v1/embed.go":                      "package v1\n",
		"internal/queryapi/pdp/pdp.go":                    basePDP,
		"internal/testsupport/authrows/quirks.go":         baseQuirks,
		"internal/auth/writer.go":                         baseWriter,
		"ci/x_refusals.tsv":                               baseRefusal,
		"ci/closure.tsv":                                  "github.com/x/jwt\treason\n",
	}
	for _, scope := range exceptionledger.MarkerScope {
		if strings.HasSuffix(scope, ".go") {
			files[scope] = "package p\n"
			continue
		}
		if _, ok := files[scope+"/a.go"]; !ok {
			files[scope+"/a.go"] = "package p\n"
		}
	}
	return files
}

func run(t *testing.T, files map[string]string, ledger string) []string {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	defects, err := exceptionledger.Check(root, []byte(ledger))
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	return defects
}

func want(t *testing.T, defects []string, substrs ...string) {
	t.Helper()
	if len(defects) == 0 {
		t.Fatalf("no defect; want one naming %q", substrs)
	}
	for _, defect := range defects {
		ok := true
		for _, s := range substrs {
			ok = ok && strings.Contains(defect, s)
		}
		if ok {
			return
		}
	}
	t.Fatalf("no defect names all of %q: %q", substrs, defects)
}

func TestTheFixtureIsClean(t *testing.T) {
	if defects := run(t, fixture(), baseLedger); len(defects) != 0 {
		t.Fatalf("clean fixture has defects: %q", defects)
	}
}

func TestPlantedFailures(t *testing.T) {
	cells := []struct {
		name   string
		plant  func(files map[string]string) string
		substr []string
	}{
		{"P1 a new Gap quirk with no ledger row", func(f map[string]string) string {
			f["internal/testsupport/authrows/quirks.go"] += "\nvar More = []Quirk{{ID: \"n\", Kind: Gap, Tracked: \"CHAOS-9\"}}\n"
			return baseLedger
		}, []string{"unregistered exception", "quirks.go:8", `Tracked: "CHAOS-9"`}},
		{"P2 a row whose owner is closed", func(map[string]string) string {
			return strings.Replace(baseLedger, "CHAOS-21\topen", "CHAOS-21\tclosed", 1)
		}, []string{"writer-legacy", "owner CHAOS-21 is closed"}},
		{"P3 the anchor is gone from the code", func(f map[string]string) string {
			delete(f, "internal/auth/writer.go")
			f["ci/closure.tsv"] = "github.com/y/other\treason\n"
			return baseLedger
		}, []string{"anchor-only", "stale row", "github.com/x/jwt"}},
		{"P4 one more legacy_hs256 row", func(f map[string]string) string {
			f["contracts/auth/v1/endpoint-profiles.go-api.json"] = strings.Replace(baseProfile, `"legacy_hs256":false`, `"legacy_hs256":true`, 1)
			return baseLedger
		}, []string{"unregistered exception", "count:legacy_hs256=2"}},
		{"P5 a govulncheck exception row with no ledger row", func(f map[string]string) string {
			f["ci/auth-vuln-exceptions.tsv"] = "# osv\tmodule\tversion\tticket\treason\nGO-1\tgithub.com/x/y\tv1.0.0\tCHAOS-7\tno fix\n"
			return baseLedger
		}, []string{"unregistered exception", "vuln:GO-1 github.com/x/y v1.0.0"}},
		{"P6 one more declared-gap row", func(f map[string]string) string {
			f["contracts/auth/v1/endpoint-profiles.go-api.json"] = strings.TrimSuffix(baseProfile, `]}`) + `,{"notes":["cache: declared gap"]}]}`
			return baseLedger
		}, []string{"unregistered exception", "count:declared_gap_rows=2"}},
		{"P7 a new TEMPORARY comment in the auth scope", func(f map[string]string) string {
			f["internal/auth/other.go"] = "package auth\n\n// TEMPORARY: a second legacy path.\nvar B = 2\n"
			return baseLedger
		}, []string{"unregistered exception", "internal/auth/other.go:3"}},
		{"P8 a new divergent parity row", func(f map[string]string) string {
			f["ci/x_refusals.tsv"] = baseRefusal + "r_new\t200\t422\tdivergent\tanother gap\n"
			return baseLedger
		}, []string{"unregistered exception", "divergent:r_new"}},
		{"P9 a new member of the PDP exempt list", func(f map[string]string) string {
			f["internal/queryapi/pdp/pdp.go"] = strings.Replace(basePDP, "{ClassHS256}", "{ClassHS256, ClassEnvelope}", 1)
			return baseLedger
		}, []string{"unregistered exception", "ExemptClasses:ClassEnvelope"}},
		{"P10 a temporary row with no owner ticket", func(map[string]string) string {
			return strings.Replace(baseLedger, "CHAOS-26\topen", "NONE\topen", 1)
		}, []string{"anchor-only", "needs an owner ticket"}},
		{"P11 a permanent row with no reason", func(map[string]string) string {
			return strings.Replace(baseLedger, "an operator input ruled permanent by D1", "kept", 1)
		}, []string{"permanent-row", "states why it is permanent"}},
		{"P13 an exact row does not cover a marker line that quotes its key", func(f map[string]string) string {
			f["internal/queryapi/pdp/pdp.go"] = basePDP + "\n// TEMPORARY: ExemptClasses:ClassHS256 stays until the flip.\n"
			return baseLedger
		}, []string{"unregistered exception", "internal/queryapi/pdp/pdp.go:10"}},
		{"P14 an exact row stays stale when its key is only quoted in the file", func(f map[string]string) string {
			f["internal/queryapi/pdp/pdp.go"] = basePDP + "\n// ClassEnvelope left; was ExemptClasses:ClassEnvelope.\n"
			return baseLedger + "pdp-envelope\ttemporary\tinternal/queryapi/pdp/pdp.go\tExemptClasses:ClassEnvelope\tCHAOS-27\topen\tgone\n"
		}, []string{"pdp-envelope", "stale row, no exception"}},
		{"P12 an interim comment in a single-file scope", func(f map[string]string) string {
			f["internal/apiservice/accessverify.go"] = "package apiservice\n\n// An interim pin until the registry lands.\nvar C = 3\n"
			return baseLedger
		}, []string{"unregistered exception", "internal/apiservice/accessverify.go:3"}},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			files := fixture()
			ledger := cell.plant(files)
			want(t, run(t, files, ledger), cell.substr...)
		})
	}
}

func TestAFieldMarkerInATestFileIsAFixtureNotAnException(t *testing.T) {
	files := fixture()
	files["internal/testsupport/authrows/quirks_test.go"] = "package authrows\n\nvar q = Quirk{Kind: Gap, Tracked: \"CHAOS-9\"}\n"
	if defects := run(t, files, baseLedger); len(defects) != 0 {
		t.Fatalf("a test fixture's Tracked field was taken for an exception: %q", defects)
	}
}

func TestAMissingScanInputFailsLoudly(t *testing.T) {
	for _, rel := range []string{"internal/queryapi/pdp/pdp.go", "contracts/auth/v1/endpoint-profiles.go-api.json", "ci/x_refusals.tsv", "internal/auth/writer.go"} {
		t.Run(rel, func(t *testing.T) {
			files := fixture()
			delete(files, rel)
			if rel == "internal/auth/writer.go" {
				delete(files, "internal/auth/a.go")
			}
			root := t.TempDir()
			for name, body := range files {
				path := filepath.Join(root, filepath.FromSlash(name))
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if rel == "internal/auth/writer.go" {
				if err := os.MkdirAll(filepath.Join(root, "internal", "auth"), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := exceptionledger.Check(root, []byte(baseLedger)); err == nil {
				t.Fatalf("no error with %s missing: the census measured nothing and passed", rel)
			}
		})
	}
}

func TestMalformedLedgerRowsAreRefused(t *testing.T) {
	for name, line := range map[string]string{
		"six columns":     "a\ttemporary\tp\tn\tCHAOS-1\topen\n",
		"bad kind":        "a\tlater\tp\tn\tCHAOS-1\topen\tr\n",
		"bad id":          "A_B\ttemporary\tp\tn\tCHAOS-1\topen\tr\n",
		"absolute path":   "a\ttemporary\t/etc/p\tn\tCHAOS-1\topen\tr\n",
		"unknown state":   "a\ttemporary\tp\tn\tCHAOS-1\tdone\tr\n",
		"empty needle":    "a\ttemporary\tp\t \tCHAOS-1\topen\tr\n",
		"duplicate id":    "a\ttemporary\tp\tn\tCHAOS-1\topen\tr\na\ttemporary\tp\tn\tCHAOS-1\topen\tr\n",
		"decision empty":  "a\tdecision\tp\tn\t-\topen\tr\n",
		"temporary ruled": "a\ttemporary\tp\tn\tD1\topen\tr\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, defects := exceptionledger.ParseLedger([]byte(line)); len(defects) == 0 {
				t.Fatalf("no defect for %q", line)
			}
		})
	}
}

func TestAProfileFileWithNoRowsFailsLoudly(t *testing.T) {
	files := fixture()
	files["contracts/auth/v1/endpoint-profiles.query-api.json"] = `{"rows":[]}`
	root := t.TempDir()
	for name, body := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := exceptionledger.Check(root, []byte(baseLedger)); err == nil {
		t.Fatal("a profile file with no rows was counted as zero exceptions")
	}
}
