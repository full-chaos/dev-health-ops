package daily

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func retiredSlotDay(text string) time.Time {
	day, err := time.Parse(time.DateOnly, text)
	if err != nil {
		panic(err)
	}
	return day
}

func TestRetiredSlotRecomputeDaysCoverDThroughDPlus29UpToThrough(t *testing.T) {
	repoA, repoB := "aaaaaaaa-0000-4000-8000-000000000001", "bbbbbbbb-0000-4000-8000-000000000002"
	pairs := []RetiredSlotRepoDay{{repoB, retiredSlotDay("2026-07-01")}, {repoA, retiredSlotDay("2026-07-01")}, {repoA, retiredSlotDay("2026-07-03")}}

	days := RetiredSlotRecomputeDays(pairs, retiredSlotDay("2026-12-31"))
	if first, last := days[0].Day, days[len(days)-1].Day; !first.Equal(retiredSlotDay("2026-07-01")) || !last.Equal(retiredSlotDay("2026-08-01")) {
		t.Fatalf("days %s .. %s, want 2026-07-01 (D) .. 2026-08-01 (the later D + 29)", first.Format(time.DateOnly), last.Format(time.DateOnly))
	}
	byDay := map[string][]RepositoryID{}
	for _, day := range days {
		byDay[day.Day.Format(time.DateOnly)] = day.RepoIDs
	}
	for day, want := range map[string][]RepositoryID{
		"2026-07-01": {RepositoryID(repoA), RepositoryID(repoB)},
		"2026-07-30": {RepositoryID(repoA), RepositoryID(repoB)}, // D + 29 of 07-01
		"2026-07-31": {RepositoryID(repoA)},                      // D + 30 of 07-01 is out; D + 28 of 07-03
		"2026-08-01": {RepositoryID(repoA)},
		"2026-08-02": nil,
	} {
		if got := byDay[day]; !reflect.DeepEqual(got, want) {
			t.Errorf("%s: repos %v, want %v", day, got, want)
		}
	}
	if got, want := RetiredSlotRepoDayCount(days), 30+32; got != want {
		t.Errorf("repo-days %d, want %d (repo B 30 days, repo A 07-01 .. 08-01)", got, want)
	}

	capped := RetiredSlotRecomputeDays(pairs, retiredSlotDay("2026-07-02"))
	if got := RetiredSlotRepoDayCount(capped); got != 4 {
		t.Errorf("through 07-02: %d repo-days %v, want 4 (07-01 and 07-02 of both repos; 07-03 is after through)", got, capped)
	}
	if got := RetiredSlotRecomputeDays(nil, retiredSlotDay("2026-07-02")); len(got) != 0 {
		t.Errorf("no pairs: %v, want no day", got)
	}
}

// retiredSlotFormSQL renders FORM-14544-SEL-1 S1 from RetiredSlotRepoDaysSQL: the prod read form takes the verb's own text and
// only swaps each parameter for its literal, adds the cost gate (FORM-TEMPLATE-3) and the org prefix, and counts.
func retiredSlotFormSQL(t *testing.T, orgPrefix, from, to string, rowsCap, bytesCap, rowCap int64) string {
	t.Helper()
	query := RetiredSlotRepoDaysSQL
	swap := func(old, replacement string, times int) {
		t.Helper()
		if got := strings.Count(query, old); got != times {
			t.Fatalf("RetiredSlotRepoDaysSQL holds %q %d times, want %d: re-check the FORM render", old, got, times)
		}
		query = strings.ReplaceAll(query, old, replacement)
	}
	swap("org_id = {org_id:String}", "startsWith(org_id, '"+orgPrefix+"') AND cost_gate = 0", 2)
	swap("{day_from:Date}", "toDate('"+from+"')", 1)
	swap("{day_to:Date}", "toDate('"+to+"')", 1)
	swap("(empty({repo_ids:Array(String)}) OR has({repo_ids:Array(String)}, toString(repo_id)))", "1", 1)
	swap("{row_cap:UInt32}", formatInt(rowCap), 1)
	return `WITH (SELECT throwIf(
  (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > ` + formatInt(rowsCap) + `
  OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('test_suite_results')) > ` + formatInt(bytesCap) + `,
  'STOP-COST-GATE')) AS cost_gate
SELECT count() AS repo_days, uniqExact(repo) AS repos, toString(min(day)) AS first_day, toString(max(day)) AS last_day
FROM (
` + query + `
)
`
}

func formatInt(value int64) string { return strconv.FormatInt(value, 10) }

// The FORM text is the verb's statement: a change to RetiredSlotRepoDaysSQL changes this hash, so the FORM is rendered and read
// again. FORM14544_OUT=<path> writes the rendered text for the card.
func TestRetiredSlotFormRendersFromTheVerbConstant(t *testing.T) {
	form := retiredSlotFormSQL(t, "c6a38355", "2026-11-17", "2026-11-23", 6_000_000, 4_000_000_000, 201)
	sum := sha256.Sum256([]byte(form))
	got := hex.EncodeToString(sum[:])[:12]
	if out := os.Getenv("FORM14544_OUT"); out != "" {
		if err := os.WriteFile(out, []byte(form), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	const pinned = "5f2ed667d21d"
	if got != pinned {
		t.Fatalf("FORM-14544-SEL-1 S1 sha12 %s, pinned %s: the verb statement changed; render the FORM again for its read\n%s", got, pinned, form)
	}
}
