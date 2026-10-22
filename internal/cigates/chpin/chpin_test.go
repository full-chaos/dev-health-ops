// Package chpin guards the ClickHouse server version across every venue: prod chart, ops compose, bigboy compose, the
// Go test image and the CI service containers. Runtime venues must pin a digest listed in pins.tsv; every venue must
// share the reference major.minor (the prod chart's), except a venue named in exceptions.tsv.
package chpin_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type venue struct {
	name    string
	ref     string
	runtime bool
}

var refRe = regexp.MustCompile(`^(?:ghcr\.io/full-chaos/)?clickhouse/clickhouse-server(?::([A-Za-z0-9._-]+))?(?:@(sha256:[0-9a-f]{64}))?$`)
var minorRe = regexp.MustCompile(`^(\d+\.\d+)(?:\.\d+)*$`)
var ticketRe = regexp.MustCompile(`^CHAOS-\d+$`)

func minorOf(version string) string {
	m := minorRe.FindStringSubmatch(version)
	if m == nil {
		return ""
	}
	return m[1]
}

func rows(text string) [][]string {
	var out [][]string
	for _, l := range strings.Split(text, "\n") {
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		out = append(out, strings.Split(l, "\t"))
	}
	return out
}

func check(venues []venue, pinsText, exceptionsText string) []string {
	var bad []string
	versions := map[string]string{}
	for _, r := range rows(pinsText) {
		if len(r) != 2 || minorOf(r[1]) == "" {
			bad = append(bad, fmt.Sprintf("pins.tsv row %q is not digest<TAB>version", strings.Join(r, "\t")))
			continue
		}
		versions[r[0]] = r[1]
	}
	exc := map[string]string{}
	for _, r := range rows(exceptionsText) {
		if len(r) != 4 || !ticketRe.MatchString(r[2]) || minorOf(r[1]) == "" || len(strings.Split(r[1], ".")) != 4 || strings.TrimSpace(r[3]) == "" {
			bad = append(bad, fmt.Sprintf("exceptions.tsv row %q is not venue<TAB>exact-version<TAB>CHAOS-n<TAB>note", strings.Join(r, "\t")))
			continue
		}
		exc[r[0]] = r[1]
	}
	minors := map[string]string{}
	for _, v := range venues {
		m := refRe.FindStringSubmatch(v.ref)
		if m == nil {
			bad = append(bad, fmt.Sprintf("%s: %q is not a clickhouse/clickhouse-server reference", v.name, v.ref))
			continue
		}
		tag, digest := m[1], m[2]
		want, excepted := exc[strings.SplitN(v.name, "#", 2)[0]]
		if tag == "" || tag == "latest" && digest == "" {
			bad = append(bad, fmt.Sprintf("%s: %q is untagged or an unpinned latest", v.name, v.ref))
			continue
		}
		if v.runtime && digest == "" && !excepted {
			bad = append(bad, fmt.Sprintf("%s: runtime pin %q has no digest", v.name, v.ref))
		}
		if excepted && tag != want {
			bad = append(bad, fmt.Sprintf("%s: tag %s differs from the exact version %s of its exceptions.tsv row", v.name, tag, want))
		}
		minor := minorOf(tag)
		if digest != "" {
			ver, ok := versions[digest]
			switch {
			case !ok:
				bad = append(bad, fmt.Sprintf("%s: digest %s is not in pins.tsv", v.name, digest))
			case tag != "latest" && minorOf(tag) != minorOf(ver):
				bad = append(bad, fmt.Sprintf("%s: tag %s disagrees with pins.tsv version %s for its digest", v.name, tag, ver))
			default:
				minor = minorOf(ver)
			}
		}
		if minor == "" {
			bad = append(bad, fmt.Sprintf("%s: cannot read a major.minor from %q", v.name, v.ref))
			continue
		}
		minors[v.name] = minor
	}
	ref, ok := minors["chart"]
	if !ok {
		return append(bad, "venue chart is missing: it defines the reference minor")
	}
	for name, minor := range minors {
		if want, excepted := exc[strings.SplitN(name, "#", 2)[0]]; excepted {
			if minor != minorOf(want) {
				bad = append(bad, fmt.Sprintf("%s: exception row says %s, venue is on %s", name, want, minor))
			}
			continue
		}
		if minor != ref {
			bad = append(bad, fmt.Sprintf("%s is on %s, the chart is on %s, and no exceptions.tsv row names it", name, minor, ref))
		}
	}
	sort.Strings(bad)
	return bad
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func read(t *testing.T, root, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

var (
	imageLine   = regexp.MustCompile(`(?m)^\s*(?:image:|-\s*name:)\s*['"]?(\S*clickhouse/clickhouse-server\S*?)['"]?\s*(?:#.*)?$`)
	harnessLine = regexp.MustCompile(`ClickHouseImage\s*=\s*"([^"]+)"`)
)

func scan(name, text string, re *regexp.Regexp, runtime bool) ([]venue, string) {
	var out []venue
	for i, m := range re.FindAllStringSubmatch(text, -1) {
		out = append(out, venue{name: fmt.Sprintf("%s#%d", name, i+1), ref: m[1], runtime: runtime})
	}
	mentions := 0
	for _, l := range strings.Split(text, "\n") {
		trim := strings.TrimSpace(l)
		if !strings.HasPrefix(trim, "#") && !strings.HasPrefix(trim, "//") && strings.Contains(l, "clickhouse/clickhouse-server") {
			mentions++
		}
	}
	if len(out) == 0 || len(out) != mentions {
		return nil, fmt.Sprintf("%s: %d ClickHouse image line(s) read, %d line(s) name the image: a venue is skipped", name, len(out), mentions)
	}
	return out, ""
}

func repoVenues(t *testing.T, root string) []venue {
	t.Helper()
	var chart struct {
		ClickHouse struct {
			Image struct {
				Repository string `yaml:"repository"`
				Tag        string `yaml:"tag"`
			} `yaml:"image"`
		} `yaml:"clickhouse"`
	}
	if err := yaml.Unmarshal([]byte(read(t, root, "deploy/helm/dev-health/values.yaml")), &chart); err != nil {
		t.Fatal(err)
	}
	img := chart.ClickHouse.Image
	vs := []venue{{name: "chart", ref: img.Repository + ":" + img.Tag, runtime: true}}
	one := func(name, rel string, re *regexp.Regexp, runtime bool) {
		got, problem := scan(name, read(t, root, rel), re, runtime)
		if problem != "" {
			t.Fatal(problem)
		}
		vs = append(vs, got...)
	}
	one("compose", "compose.yml", imageLine, true)
	one("bigboy", "ci/bigboy/compose.bigboy.yml", imageLine, true)
	one("testimage", "internal/testsupport/containers/harness.go", harnessLine, false)
	one("live-e2e", ".github/workflows/live-e2e.yml", imageLine, false)
	one("gitlab-ci", ".gitlab-ci.yml", imageLine, false)
	return vs
}

func TestRepoVenuesAgreeOnClickHouse(t *testing.T) {
	root := repoRoot(t)
	vs := repoVenues(t, root)
	if bad := check(vs, read(t, root, "internal/cigates/chpin/pins.tsv"), read(t, root, "internal/cigates/chpin/exceptions.tsv")); len(bad) > 0 {
		t.Fatalf("ClickHouse pins disagree:\n  %s", strings.Join(bad, "\n  "))
	}
}

const (
	d7 = "sha256:d7556a3841027651307b5aa08d72b5c467d0241d3db5b67d9e158ef3975626f5"
	pn = "sha256:d7556a3841027651307b5aa08d72b5c467d0241d3db5b67d9e158ef3975626f5\t26.7.1.1315\n"
	ex = "bigboy\t26.9.14.10\tCHAOS-1\tnote\n"
)

func TestCheckPlantedDefects(t *testing.T) {
	good := []venue{
		{"chart", "clickhouse/clickhouse-server:latest@" + d7, true},
		{"compose", "clickhouse/clickhouse-server:26.7.1.1315@" + d7, true},
		{"bigboy", "clickhouse/clickhouse-server:26.9.14.10", true},
		{"testimage", "clickhouse/clickhouse-server:26.7", false},
		{"ci", "ghcr.io/full-chaos/clickhouse/clickhouse-server:26.7", false},
	}
	if bad := check(good, pn, ex); len(bad) > 0 {
		t.Fatalf("good set refused: %v", bad)
	}
	mutate := func(name, ref string) []venue {
		out := append([]venue(nil), good...)
		for i := range out {
			if out[i].name == name {
				out[i].ref = ref
			}
		}
		return out
	}
	cases := []struct {
		name  string
		vs    []venue
		pins  string
		exc   string
		wants string
	}{
		{"latest_unpinned", mutate("compose", "clickhouse/clickhouse-server:latest"), pn, ex, "unpinned latest"},
		{"untagged", mutate("testimage", "clickhouse/clickhouse-server"), pn, ex, "untagged"},
		{"runtime_no_digest", mutate("compose", "clickhouse/clickhouse-server:26.7.1.1315"), pn, ex, "no digest"},
		{"unknown_digest", mutate("compose", "clickhouse/clickhouse-server:26.7@sha256:"+strings.Repeat("a", 64)), pn, ex, "not in pins.tsv"},
		{"tag_vs_table", mutate("compose", "clickhouse/clickhouse-server:26.8@"+d7), pn, ex, "disagrees"},
		{"test_minor_drift", mutate("testimage", "clickhouse/clickhouse-server:26.8"), pn, ex, "no exceptions.tsv row"},
		{"ci_minor_drift", mutate("ci", "ghcr.io/full-chaos/clickhouse/clickhouse-server:25.8"), pn, ex, "no exceptions.tsv row"},
		{"bigboy_without_row", good, pn, "", "no exceptions.tsv row"},
		{"stale_exception", mutate("bigboy", "clickhouse/clickhouse-server:26.7"), pn, ex, "exception row says"},
		{"exception_no_ticket", good, pn, "bigboy\t26.9\tTBD\tnote\n", "exceptions.tsv row"},
		{"bigboy_patch_drift", mutate("bigboy", "clickhouse/clickhouse-server:26.9.14.11"), pn, ex, "differs from the exact version"},
		{"foreign_registry", mutate("testimage", "quay.io/other/clickhouse/clickhouse-server:26.7"), pn, ex, "not a clickhouse"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bad := strings.Join(check(c.vs, c.pins, c.exc), "\n")
			if !strings.Contains(bad, c.wants) {
				t.Fatalf("want a refusal containing %q, got %q", c.wants, bad)
			}
		})
	}
}

func TestScanReadsEveryImageLineOrRefuses(t *testing.T) {
	text := "services:\n  a:\n    image: ghcr.io/full-chaos/clickhouse/clickhouse-server:26.8 # inline comment\n  b:\n    image: ghcr.io/full-chaos/clickhouse/clickhouse-server:26.7\n# image: clickhouse/clickhouse-server:25.1\n"
	got, problem := scan("x", text, imageLine, false)
	if problem != "" || len(got) != 2 || !strings.HasSuffix(got[0].ref, ":26.8") || !strings.HasSuffix(got[1].ref, ":26.7") {
		t.Fatalf("got %v %q", got, problem)
	}
	flow := text + "  c: { image: clickhouse/clickhouse-server:26.8 }\n"
	if _, problem := scan("x", flow, imageLine, false); !strings.Contains(problem, "skipped") {
		t.Fatalf("a flow-mapping image line was not refused: %q", problem)
	}
	if _, problem := scan("x", "services: {}\n", imageLine, false); !strings.Contains(problem, "skipped") {
		t.Fatalf("a file with no image line was not refused: %q", problem)
	}
}

func TestSameFileVenuesAreCheckedSeparately(t *testing.T) {
	vs := []venue{
		{"chart", "clickhouse/clickhouse-server:latest@" + d7, true},
		{"live-e2e#1", "clickhouse/clickhouse-server:26.8", false},
		{"live-e2e#2", "clickhouse/clickhouse-server:26.7", false},
	}
	if bad := strings.Join(check(vs, pn, ""), "\n"); !strings.Contains(bad, "live-e2e#1 is on 26.8") {
		t.Fatalf("first same-file venue was not checked: %q", bad)
	}
}
