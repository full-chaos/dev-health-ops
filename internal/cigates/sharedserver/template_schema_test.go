package sharedserver_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// CHAOS-11760: a package that runs on shared servers builds its migrated schema through pgschema.Start, which
// clones one template per binary; a helper that still calls pgschema.Apply / ApplyURI replays the whole migration
// chain for every test. This census lists the calls left in the two packages of the adoption.

var applyCall = regexp.MustCompile(`\bpgschema\.Apply(URI)?\(`)

// allowedApply is a file that may still call pgschema.Apply, with the reason it has not moved. A row whose file no
// longer calls it is stale and fails.
type allowedApply struct {
	File   string
	Reason string
}

// workerserviceApplyAllowlist: the two workerservice helpers move in PR B of CHAOS-11760.
var workerserviceApplyAllowlist = []allowedApply{
	{File: "internal/workerservice/daily_no_repository_partition_integration_test.go", Reason: "PR B, CHAOS-11760"},
	{File: "internal/workerservice/multi_replica_integration_test.go", Reason: "PR B, CHAOS-11760"},
}

// applyCallSites returns file:line (slash form, relative to root) of every pgschema.Apply / ApplyURI call in a test
// file directly inside dir, comment lines excluded.
func applyCallSites(t *testing.T, root, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, dir))
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(root, dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		rel := filepath.ToSlash(filepath.Join(dir, entry.Name()))
		for number, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "//") || !applyCall.MatchString(line) {
				continue
			}
			found = append(found, rel+":"+strconv.Itoa(number+1))
		}
	}
	sort.Strings(found)
	return found
}

// unallowedApplySites returns the sites that no allowlist row covers, and the problems of the allowlist itself
// (a row without a reason, a row for a file with no call left).
func unallowedApplySites(sites []string, allowlist []allowedApply) (unallowed, problems []string) {
	covered := map[string]bool{}
	allowed := map[string]bool{}
	for _, row := range allowlist {
		if strings.TrimSpace(row.Reason) == "" {
			problems = append(problems, row.File+": the allowlist row has no reason")
		}
		allowed[row.File] = true
	}
	for _, site := range sites {
		file := site[:strings.LastIndex(site, ":")]
		if allowed[file] {
			covered[file] = true
			continue
		}
		unallowed = append(unallowed, site)
	}
	for _, row := range allowlist {
		if !covered[row.File] {
			problems = append(problems, row.File+": the allowlist row is stale (the file no longer calls pgschema.Apply)")
		}
	}
	return unallowed, problems
}

func TestSyncdispatchruntimeBuildsItsSchemaThroughTheTemplate(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	unallowed, problems := unallowedApplySites(applyCallSites(t, root, "internal/syncdispatchruntime"), nil)
	for _, site := range unallowed {
		t.Errorf("%s: pgschema.Apply in a package that runs on a shared server; call pgschema.Start instead", site)
	}
	for _, problem := range problems {
		t.Error(problem)
	}
}

func TestWorkerserviceBuildsItsSchemaThroughTheTemplate(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	unallowed, problems := unallowedApplySites(applyCallSites(t, root, "internal/workerservice"), workerserviceApplyAllowlist)
	for _, site := range unallowed {
		t.Errorf("%s: pgschema.Apply in a package that runs on a shared server; call pgschema.Start instead", site)
	}
	for _, problem := range problems {
		t.Error(problem)
	}
}

func TestApplyCensusClassifiesAPlantedTree(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "internal", "opted")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "package opted\n" +
		"// pgschema.Apply(ctx, t, pool) in a comment\n" +
		"func a() { pgschema.Apply(ctx, t, pool) }\n" +
		"func b() { pgschema.ApplyURI(ctx, t, uri) }\n" +
		"func c() { pgschema.Start(ctx, t) }\n"
	if err := os.WriteFile(filepath.Join(dir, "x_test.go"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "y.go"), []byte("package opted\nfunc d() { pgschema.Apply(ctx, t, pool) }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := applyCallSites(t, root, "internal/opted")
	if strings.Join(got, ",") != "internal/opted/x_test.go:3,internal/opted/x_test.go:4" {
		t.Fatalf("sites = %v, want the two calls of x_test.go (not the comment, not Start, not a non-test file)", got)
	}
}

func TestApplyAllowlistNeedsAReasonAndCannotGoStale(t *testing.T) {
	sites := []string{"internal/p/a_test.go:3", "internal/p/b_test.go:9"}
	unallowed, problems := unallowedApplySites(sites, []allowedApply{{File: "internal/p/a_test.go", Reason: "PR B"}})
	if len(unallowed) != 1 || unallowed[0] != "internal/p/b_test.go:9" || len(problems) != 0 {
		t.Fatalf("unallowed=%v problems=%v", unallowed, problems)
	}
	_, problems = unallowedApplySites(sites, []allowedApply{{File: "internal/p/a_test.go", Reason: " "}})
	if len(problems) != 1 || !strings.Contains(problems[0], "no reason") {
		t.Fatalf("an empty reason was accepted: %v", problems)
	}
	_, problems = unallowedApplySites(nil, []allowedApply{{File: "internal/p/gone_test.go", Reason: "PR B"}})
	if len(problems) != 1 || !strings.Contains(problems[0], "stale") {
		t.Fatalf("a stale row was accepted: %v", problems)
	}
}
