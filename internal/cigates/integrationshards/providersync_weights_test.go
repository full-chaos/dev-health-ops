package integrationshards_test

import (
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func providersyncWeightRows(t *testing.T) []string {
	t.Helper()
	var rows []string
	for _, l := range strings.Split(readText(t, filepath.Join(rootOf(t), "ci", "go_providersync_test_weights.tsv")), "\n") {
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		rows = append(rows, l)
	}
	return rows
}

func TestProvidersyncWeightsFileIsSortedUniqueAndPositive(t *testing.T) {
	t.Parallel()
	rows := providersyncWeightRows(t)
	if len(rows) < 100 {
		t.Fatalf("weights file has %d row(s); a truncated file would send every slow test back to its class weight", len(rows))
	}
	seconds := regexp.MustCompile(`^[0-9]+(\.[0-9]{1,2})?$`)
	prev := ""
	for i, row := range rows {
		fields := strings.Split(row, "\t")
		if len(fields) != 2 || !strings.HasPrefix(fields[0], "Test") || !seconds.MatchString(fields[1]) {
			t.Fatalf("row %d %q must be TestName<TAB>seconds", i+1, row)
		}
		if v, _ := strconv.ParseFloat(fields[1], 64); v <= 0 {
			t.Fatalf("row %d %q must be positive", i+1, row)
		}
		if i > 0 && fields[0] <= prev {
			t.Fatalf("row %d %q is not strictly after %q in byte order (sorted, no duplicates)", i+1, fields[0], prev)
		}
		prev = fields[0]
	}
}

func TestProvidersyncShardBudgetIsThreeQuartersOfTheGoTestTimeout(t *testing.T) {
	t.Parallel()
	script := readText(t, filepath.Join(rootOf(t), "ci", "check_go.sh"))
	var timeout int
	for _, line := range strings.Split(script, "\n") {
		if strings.Contains(line, "go test") && strings.Contains(line, "-run \"${test_regex}\"") && strings.Contains(line, "./internal/providersync") {
			m := regexp.MustCompile(`-timeout=(\d+)m\b`).FindStringSubmatch(line)
			if m == nil {
				t.Fatalf("providersync shard go test has no -timeout=<N>m: %s", line)
			}
			timeout, _ = strconv.Atoi(m[1])
		}
	}
	m := regexp.MustCompile(`DEV_HEALTH_GO_PROVIDER_SHARD_BUDGET:-(\d+)\}`).FindStringSubmatch(script)
	if timeout == 0 || m == nil {
		t.Fatalf("timeout %d or budget default (%v) not found in ci/check_go.sh", timeout, m)
	}
	budget, _ := strconv.Atoi(m[1])
	if want := timeout * 60 * 100 * 3 / 4; budget != want {
		t.Fatalf("shard budget default %d (10 ms units) must be 75%% of the %d min go test timeout = %d", budget, timeout, want)
	}
}
