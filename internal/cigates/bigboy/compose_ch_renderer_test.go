package bigboy_test

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The users.d declaration of dho_api_ch is rendered by a Python script from the Go manifest, and the Go side issues the
// same grants from the same manifest: one checked-in golden pins both. These guards run the REAL renderer (a real python3,
// a temporary dependency until the renderer is ported) on the REAL manifest and compare its grants with the golden, so a
// manifest layout the renderer cannot read, or a grant it drops, fails here and not on bigboy.

const (
	chManifest = "internal/storage/clickhouse/authorization.go"
	chGolden   = "internal/storage/clickhouse/testdata/api_grants.golden"
)

type chUsersXML struct {
	Queries []string `xml:"users>dho_api_ch>grants>query"`
}

func chRenderReal(g *guard) (queries []string, out string) {
	g.expect(composeRealBin("python3") != "", "python3 is not installed: the renderer cannot run, and this guard must not skip")
	dir := composeTempDir(g, g.tmp, "render-")
	env := []string{"PATH=" + os.Getenv("PATH"), "API_CH_PASSWORD=Zq7-throwaway-credential"}
	res, err := composeExec(g.root, env, 30*time.Second, "python3", g.path(composeCHRenderer), g.path(chManifest), filepath.Join(dir, "out.xml"))
	if err != nil {
		g.failf("the renderer could not run: %v", err)
		return nil, ""
	}
	if res.code != 0 {
		g.failf("the renderer failed on %s (rc=%d): %s%s", chManifest, res.code, res.stdout, res.stderr)
		return nil, ""
	}
	b, err := os.ReadFile(filepath.Join(dir, "out.xml"))
	if err != nil {
		g.failf("the renderer wrote no file: %v", err)
		return nil, ""
	}
	var parsed chUsersXML
	if err := xml.Unmarshal(b, &parsed); err != nil {
		g.failf("the rendered users.d file is not XML: %v", err)
	}
	return parsed.Queries, string(b)
}

func chGrantsEqualGolden(g *guard) {
	golden := strings.Split(strings.TrimSpace(g.read(chGolden)), "\n")
	g.expect(len(golden) > 0 && golden[0] != "", "the golden is empty: this guard would prove nothing")
	got, out := chRenderReal(g)
	g.expect(!strings.Contains(out, "Zq7-throwaway-credential"), "the rendered file holds the password")
	if !equal(got, golden) {
		g.failf("the renderer's grants differ from the golden %s: rendered %d, golden %d; missing %v; extra %v",
			chGolden, len(got), len(golden), chSubtract(golden, got), chSubtract(got, golden))
	}
}

func chOneGrantPerPrivilegedTable(g *guard) {
	body := g.read(chManifest)
	_, after, ok := strings.Cut(body, "func APIPosture(")
	g.expect(ok, "%s has no func APIPosture", chManifest)
	body, _, _ = strings.Cut(after, "\n}\n")
	rows := 0
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, "{Database: database, Table:") &&
			(strings.Contains(line, "AllowSelect: true") || strings.Contains(line, "AllowInsert: true") || strings.Contains(line, "AllowDelete: true")) {
			rows++
		}
	}
	g.expect(rows > 0, "the manifest could not be read: this guard would prove nothing")
	got, _ := chRenderReal(g)
	g.expect(len(got) == rows, "the manifest has %d privileged table rows and the renderer wrote %d grants", rows, len(got))
}

func chSubtract(a, b []string) []string {
	in := map[string]bool{}
	for _, x := range b {
		in[x] = true
	}
	var out []string
	for _, x := range a {
		if !in[x] {
			out = append(out, x)
		}
	}
	return out
}

func TestComposeCHRendererReal(t *testing.T) {
	runReal(t,
		check{"rendered_grants_equal_the_golden_the_go_manifest_test_is_pinned_to", chGrantsEqualGolden},
		check{"the_real_manifest_renders_one_grant_per_table_with_a_privilege", chOneGrantPerPrivilegedTable},
	)
}

func TestComposeCHRendererPlanted(t *testing.T) {
	files := []string{composeCHRenderer, chManifest, chGolden}
	runPlanted(t, []planted{
		{name: "a-real-grant-is-formatted-across-two-lines", copy: files, check: chGrantsEqualGolden, want: "differ from the golden",
			mutate: mut(chManifest, `{Database: database, Table: "work_items", AllowSelect: true},`, "{Database: database, Table: \"work_items\",\n\t\t\tAllowSelect: true},")},
		{name: "a-grant-is-added-to-the-manifest", copy: files, check: chGrantsEqualGolden, want: "differ from the golden",
			mutate: mut(chManifest, `{Database: database, Table: "work_items", AllowSelect: true},`, `{Database: database, Table: "work_items", AllowSelect: true},`+"\n\t\t"+`{Database: database, Table: "extra_table", AllowSelect: true},`)},
		{name: "the-golden-gains-a-grant", copy: files, check: chGrantsEqualGolden, want: "extra",
			mutate: mut(chGolden, "GRANT SELECT, INSERT, ALTER DELETE ON default.teams\n", "GRANT SELECT, INSERT, ALTER DELETE ON default.teams\nGRANT SELECT ON default.extra\n")},
		{name: "the-renderer-stops-reading-the-insert-privilege", copy: files, check: chOneGrantPerPrivilegedTable, want: "renderer wrote",
			mutate: mut(composeCHRenderer, "    (\"AllowInsert\", \"INSERT\"),\n", "")},
		{name: "the-renderer-script-is-missing", copy: []string{chManifest, chGolden}, check: chGrantsEqualGolden, want: "the renderer failed"},
	})
}
