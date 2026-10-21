package outboundtransport

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

var plantedPackages = []string{"decl", "defaultclient", "defaulttransport", "envproxy", "literal", "negative", "newclient", "nilassign", "proxyassign", "proxyfield", "unproven"}

// The walker is itself tested: each planted package under testdata/planted must yield exactly its sites, and the
// negative package none. A clause removed from kindsOf is RED here.
func TestTheWalkerFindsEveryPlantedKind(t *testing.T) {
	root := moduleRoot(t)
	var patterns []string
	for _, name := range plantedPackages {
		patterns = append(patterns, fixturesBase+"/"+name)
	}
	found, problems, _ := scan(t, root, patterns, func(string) bool { return false })
	for _, problem := range problems {
		t.Error(problem)
	}
	const dir = "internal/cigates/outboundtransport/testdata/planted/"
	want := map[site]int{
		{dir + "literal/literal.go", "Bare", "client-no-transport"}:                  1,
		{dir + "literal/literal.go", "TimeoutOnly", "client-no-transport"}:           1,
		{dir + "literal/literal.go", "NilTransport", "client-no-transport"}:          1,
		{dir + "newclient/newclient.go", "New", "client-no-transport"}:               1,
		{dir + "decl/decl.go", "var zero", "client-no-transport"}:                    1,
		{dir + "nilassign/nilassign.go", "Clear", "transport-nil"}:                   1,
		{dir + "defaulttransport/defaulttransport.go", "Clone", "default-transport"}: 1,
		{dir + "defaultclient/defaultclient.go", "Fetch", "default-client"}:          1,
		{dir + "defaultclient/defaultclient.go", "Probe", "default-client"}:          1,
		{dir + "defaultclient/defaultclient.go", "Values", "default-client"}:         3,
		{dir + "envproxy/envproxy.go", "Env", "proxy"}:                               1,
		{dir + "envproxy/envproxy.go", "Fixed", "proxy"}:                             1,
		{dir + "envproxy/envproxy.go", "Config", "proxy"}:                            1,
		{dir + "proxyfield/proxyfield.go", "Literal", "proxy"}:                       1,
		{dir + "proxyassign/proxyassign.go", "Assign", "proxy"}:                      1,
		{dir + "unproven/unproven.go", "NilVariable", "transport-unproven"}:          1,
		{dir + "unproven/unproven.go", "Parameter", "transport-unproven"}:            1,
		{dir + "unproven/unproven.go", "WrapperOverNil", "transport-unproven"}:       1,
		{dir + "unproven/unproven.go", "FieldRead", "transport-unproven"}:            1,
		{dir + "unproven/unproven.go", "Assigned", "transport-unproven"}:             1,
		{dir + "unproven/unproven.go", "Unkeyed", "transport-unproven"}:              1,
		{dir + "unproven/unproven.go", "ParameterReassigned", "transport-unproven"}:  1,
	}
	if got, expected := render(found), render(want); got != expected {
		t.Errorf("planted sites differ\n got:\n%s\nwant:\n%s", got, expected)
	}
}

func render(sites map[site]int) string {
	var lines []string
	for key, count := range sites {
		lines = append(lines, fmt.Sprintf("%s %s %s x%d", key.file, key.symbol, key.kind, count))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// The verdict names every defect of the allow rows: an unlisted site, a count that differs, a row without a reason
// and a stale row.
func TestTheVerdictNamesEveryDefect(t *testing.T) {
	found := map[site]int{
		{"a.go", "A", "default-transport"}: 1,
		{"b.go", "B", "proxy"}:             2,
		{"c.go", "C", "default-client"}:    1,
	}
	rows := map[site]allowRow{
		{"b.go", "B", "proxy"}:          {count: 1, reason: "documented"},
		{"c.go", "C", "default-client"}: {count: 1, reason: " "},
		{"d.go", "D", "proxy"}:          {count: 1, reason: "gone"},
	}
	got := strings.Join(compare(found, rows), "\n")
	want := strings.Join([]string{
		"COUNT differs: b.go B proxy: allow.tsv 1, code 2",
		"NO REASON: c.go C default-client",
		"STALE row: d.go D proxy",
		"UNLISTED site: a.go A default-transport (x1)",
	}, "\n")
	if got != want {
		t.Errorf("verdict\n got:\n%s\nwant:\n%s", got, want)
	}
}

// An allow row round-trips through allow.tsv for the transport-unproven kind: rows written for every planted site
// clear them all; dropping one row brings exactly that site back as UNLISTED; a row for a site that does not exist is
// STALE.
func TestATransportUnprovenAllowRowRoundTrips(t *testing.T) {
	root := moduleRoot(t)
	found, problems, _ := scan(t, root, []string{fixturesBase + "/unproven"}, func(string) bool { return false })
	for _, problem := range problems {
		t.Error(problem)
	}
	var lines []string
	for key, count := range found {
		if key.kind != "transport-unproven" {
			t.Fatalf("unexpected planted kind %v", key)
		}
		lines = append(lines, fmt.Sprintf("%s\t%s\t%s\t%d\tplanted", key.file, key.symbol, key.kind, count))
	}
	sort.Strings(lines)
	if len(lines) < 6 {
		t.Fatalf("the planted unproven package yielded %d sites, want every shape", len(lines))
	}
	path := filepath.Join(t.TempDir(), "allow.tsv")
	write := func(rows []string) {
		if err := os.WriteFile(path, []byte("# planted\n"+strings.Join(rows, "\n")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(lines)
	if got := compare(found, readAllow(t, path)); len(got) != 0 {
		t.Fatalf("rows for every planted site should clear them, got %v", got)
	}
	dropped := lines[0]
	write(lines[1:])
	want := "UNLISTED site: " + strings.Join(strings.SplitN(dropped, "\t", 4)[:3], " ")
	if got := compare(found, readAllow(t, path)); len(got) != 1 || !strings.HasPrefix(got[0], want) {
		t.Fatalf("dropping %q should leave exactly one UNLISTED site, got %v", dropped, got)
	}
	write(append(lines, "internal/nowhere.go\tGone\ttransport-unproven\t1\tplanted"))
	if got := compare(found, readAllow(t, path)); len(got) != 1 || got[0] != "STALE row: internal/nowhere.go Gone transport-unproven" {
		t.Fatalf("a row for a missing site should be STALE, got %v", got)
	}
}
