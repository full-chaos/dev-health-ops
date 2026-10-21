package outboundtransport

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

var plantedPackages = []string{"decl", "defaultclient", "defaulttransport", "envproxy", "literal", "negative", "newclient", "nilassign", "proxyassign", "proxyfield"}

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
		{dir + "envproxy/envproxy.go", "Env", "env-proxy"}:                           1,
		{dir + "envproxy/envproxy.go", "Fixed", "env-proxy"}:                         1,
		{dir + "envproxy/envproxy.go", "Config", "env-proxy"}:                        1,
		{dir + "proxyfield/proxyfield.go", "Literal", "env-proxy"}:                   1,
		{dir + "proxyassign/proxyassign.go", "Assign", "env-proxy"}:                  1,
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
		{"b.go", "B", "env-proxy"}:         2,
		{"c.go", "C", "default-client"}:    1,
	}
	rows := map[site]allowRow{
		{"b.go", "B", "env-proxy"}:      {count: 1, reason: "documented"},
		{"c.go", "C", "default-client"}: {count: 1, reason: " "},
		{"d.go", "D", "env-proxy"}:      {count: 1, reason: "gone"},
	}
	got := strings.Join(compare(found, rows), "\n")
	want := strings.Join([]string{
		"COUNT differs: b.go B env-proxy: allow.tsv 1, code 2",
		"NO REASON: c.go C default-client",
		"STALE row: d.go D env-proxy",
		"UNLISTED site: a.go A default-transport (x1)",
	}, "\n")
	if got != want {
		t.Errorf("verdict\n got:\n%s\nwant:\n%s", got, want)
	}
}
