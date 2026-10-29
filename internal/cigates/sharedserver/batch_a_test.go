package sharedserver_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// batchA lists the packages that moved to one shared server per test binary
// together (batch A of the adoption). A package that drops out of this list
// has to say why, so a TestMain edit cannot quietly turn the mode off.
var batchA = []string{
	"internal/fixturescli",
	"internal/jobs/metrics/remaining",
	"internal/queryapi/analytics",
	"internal/syncdispatchruntime",
	"internal/workerservice",
}

// repoScan walks the module's internal tree once for every test of this
// package that needs the real tree; the walk is most of the package's run time.
var (
	repoScanOnce     sync.Once
	repoScanOptedIn  []string
	repoScanFindings []finding
)

func repoScan(t *testing.T) ([]string, []finding) {
	t.Helper()
	repoScanOnce.Do(func() {
		root, err := moduleroot.Root()
		if err != nil {
			t.Fatal(err)
		}
		repoScanOptedIn, repoScanFindings = scan(t, filepath.Join(root, "internal"))
	})
	return repoScanOptedIn, repoScanFindings
}

func TestBatchAPackagesRunOnSharedServers(t *testing.T) {
	optedIn, _ := repoScan(t)
	have := map[string]bool{}
	for _, dir := range optedIn {
		have["internal/"+dir] = true
	}
	for _, pkg := range batchA {
		if !have[pkg] {
			t.Errorf("%s: no TestMain calls %s; wrap its run in containers.RunWithSharedServers", pkg, optInCall)
		}
	}
}

const activityTable = "FROM pg_stat_activity"

// activityPollsWithoutDatabase returns file:line of every pg_stat_activity
// query in a test file of an opted-in package that never names datname. On a
// shared server that view lists the sessions of every test database, so a
// poll that matches on wait_event or query text alone can be satisfied by a
// session of another test.
func activityPollsWithoutDatabase(t *testing.T, root string, optedIn []string) []string {
	t.Helper()
	var found []string
	for _, dir := range optedIn {
		entries, err := os.ReadDir(filepath.Join(root, dir))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(root, dir, entry.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(string(data), "\n")
			for number, line := range lines {
				if strings.HasPrefix(strings.TrimSpace(line), "//") || !strings.Contains(line, activityTable) {
					continue
				}
				statement := line
				for next := number + 1; next < len(lines) && next < number+16; next++ {
					statement += "\n" + lines[next]
					if strings.Contains(lines[next], "`") {
						break
					}
				}
				if !strings.Contains(statement, "datname") {
					rel, _ := filepath.Rel(root, path)
					found = append(found, filepath.ToSlash(rel)+":"+strconv.Itoa(number+1))
				}
			}
		}
	}
	return found
}

func TestSharedServerPackagesPollActivityByDatabase(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	optedIn, _ := repoScan(t)
	dirs := make([]string, 0, len(optedIn))
	for _, dir := range optedIn {
		if dir != "cigates/sharedserver" {
			dirs = append(dirs, filepath.Join("internal", dir))
		}
	}
	for _, where := range activityPollsWithoutDatabase(t, root, dirs) {
		t.Errorf("%s: pg_stat_activity query without datname in a package that runs on a shared server; "+
			"add `AND datname = current_database()` or use containers.StartPostgresIsolated in this file", where)
	}
}

func TestActivityGuardFlagsPlantedPoll(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "internal", "opted")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "package opted\n" +
		"var bad = `\n\tSELECT count(*) FROM pg_stat_activity\n\tWHERE wait_event_type = 'Lock'`\n" +
		"var good = `\n\tSELECT count(*) FROM pg_stat_activity\n\tWHERE datname = current_database()`\n" +
		"// SELECT count(*) FROM pg_stat_activity in a comment\n"
	if err := os.WriteFile(filepath.Join(dir, "poll_test.go"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got := activityPollsWithoutDatabase(t, root, []string{"internal/opted"})
	if strings.Join(got, ",") != "internal/opted/poll_test.go:3" {
		t.Errorf("planted poll findings = %v, want [internal/opted/poll_test.go:3]", got)
	}
}
