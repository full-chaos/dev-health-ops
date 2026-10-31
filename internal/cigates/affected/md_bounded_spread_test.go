package affected_test

import (
	"strings"
	"testing"
)

// pm D6610: an exec whose spread arguments the scan cannot resolve carries no Markdown evidence, so the md-bounded pull
// request plan does not select it. Every other plan still does: the exact pull request plan selects the package as an
// unbounded edge, and the merge_group and push plans are FULL. The scanner lists every such site.
const unresolvedSpread = "package x\n\nimport \"os/exec\"\n\nfunc run(name string, args ...string) error {\n\treturn exec.Command(name, args...).Run()\n}\n"

func spreadHead() map[string]string {
	return mdHead(map[string]string{"x/x_spread_test.go": unresolvedSpread,
		tablesDir + "edges.tsv": "#\nx\tx_spread_test.go\tunexpressed\n"})
}

func TestPlanVerdictUnresolvedSpreadIsNotMarkdownEvidence(t *testing.T) {
	md := []string{"--go-files-full", "--md-bounded"}
	q := []string{"--go-files-full"}
	h := spreadHead()
	runRows(t, []verdictRow{
		{name: "markdown_pr_does_not_select_the_site", head: h, extra: md, changed: []string{"README.md"}, want: "NONE"},
		{name: "exact_pr_selects_the_site", head: h, extra: q, changed: []string{"README.md"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "merge_group_is_full", head: h, event: "merge_group", extra: md, changed: []string{"README.md"}, want: "FULL event merge_group*"},
		{name: "push_is_full", head: h, event: "push", extra: md, changed: []string{"README.md"}, want: "FULL event push*"},
	})
}

func TestPlanReadsListsUnresolvedSpreadSites(t *testing.T) {
	resolved := "package r\n\nimport \"os/exec\"\n\nfunc run() error {\n\targs := []string{\"status\"}\n\treturn exec.Command(\"git\", args...).Run()\n}\n"
	root := readsFixture(t, map[string]string{"s/s_spread_test.go": strings.Replace(unresolvedSpread, "package x", "package s", 1), "r/r_spread_test.go": resolved})
	res := cmdSpec{dir: root, env: gitEnv}.run(cigate(t), "plan-reads", "--root", root, "--list-unresolved-spreads")
	if res.code != 0 || res.stdout != "s/s_spread_test.go:6\n" {
		t.Fatalf("exit %d, stdout %q, stderr %q", res.code, res.stdout, strings.TrimSpace(res.stderr))
	}
}
