package graph

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The served team view helpers are bound by scope (CHAOS-14376, r1 of the census): the census reads a helper call
// only through the file's import of rowview (a value named like the import is not the import), reads a table constant
// only when the argument binds to the package's top-level constant (a parameter or a local of the same name fails
// closed), walks the initializers of the package-level variables a read reaches, unwraps a parenthesized callee, and
// refuses any reference to a helper that is not a direct call. A constant whose value is another constant or an
// expression is refused (NOT pinned as a read: the safe direction).
func TestRootCensusBindsTheServedTeamViewHelpersByScope(t *testing.T) {
	root := t.TempDir()
	write := func(path, text string) {
		t.Helper()
		full := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("internal/queryapi/rowview/rowview.go", "package rowview\n\n"+
		"func ServedTeamSource(table, where string) string { return table + where }\n\n"+
		"func ServedTeamPick(table, where string) string { return table + where }\n")
	write("internal/reader/consts.go", "package reader\n\n"+
		"const otherFile = \"work_item_metrics_daily\"\n")
	write("internal/reader/reader.go", "package reader\n\n"+
		"import \""+rootCensusModule+"internal/queryapi/rowview\"\n\n"+
		"const metrics = \"work_item_state_durations_daily\"\n\n"+
		"const base = \"work_item_metrics_daily\"\n\n"+
		"const alias = base\n\n"+
		"const concat = \"work_item_\" + \"metrics_daily\"\n\n"+
		"type store struct{}\n\n"+
		"func (store) ServedTeamSource(table, where string) string { return table }\n\n"+
		"var initialized = rowview.ServedTeamSource(\"work_item_metrics_daily\", \"1\")\n\n"+
		"var tableVar = \"work_item_metrics_daily\"\n\n"+
		"var initializedVariable = rowview.ServedTeamPick(tableVar, \"1\")\n\n"+
		"func ShadowParam(metrics string) string { return rowview.ServedTeamPick(metrics, \"1\") }\n\n"+
		"func ShadowLocalConst() string { const base = \"teams\"; return rowview.ServedTeamPick(base, \"1\") }\n\n"+
		"func ShadowImport() string { rowview := store{}; return rowview.ServedTeamSource(\"work_item_metrics_daily\", \"1\") }\n\n"+
		"func TopLevelConst() string { return rowview.ServedTeamPick(base, \"1\") }\n\n"+
		"func OtherFileConst() string { return rowview.ServedTeamSource(otherFile, \"1\") }\n\n"+
		"func ConstAlias() string { return rowview.ServedTeamPick(alias, \"1\") }\n\n"+
		"func ConstConcat() string { return rowview.ServedTeamSource(concat, \"1\") }\n\n"+
		"func Initializer() string { return initialized }\n\n"+
		"func InitializerVariable() string { return initializedVariable }\n\n"+
		"func Parenthesized() string { return (rowview.ServedTeamSource)(\"work_item_metrics_daily\", \"1\") }\n\n"+
		"func ParenthesizedVariable(table string) string { return (rowview.ServedTeamPick)(table, \"1\") }\n\n"+
		"func Chosen() string { f := rowview.ServedTeamPick; return f(\"work_item_metrics_daily\", \"1\") }\n")
	tables := map[string]bool{"work_item_metrics_daily": false, "work_item_state_durations_daily": false, "teams": false}
	loader := newRootCensusLoader(t, root)
	pkg := loader.pkg(filepath.Join(root, "internal", "reader"))
	for _, c := range []struct {
		fn, tables, refusal string
	}{
		{"ShadowParam", "", "use a literal or same-package const table name"},
		{"ShadowLocalConst", "", "use a literal or same-package const table name"},
		{"ShadowImport", "", ""},
		{"TopLevelConst", "work_item_metrics_daily", ""},
		{"OtherFileConst", "work_item_metrics_daily", ""},
		{"ConstAlias", "", "use a literal or same-package const table name"},
		{"ConstConcat", "", "use a literal or same-package const table name"},
		{"Initializer", "work_item_metrics_daily", ""},
		{"InitializerVariable", "", "use a literal or same-package const table name"},
		{"Parenthesized", "work_item_metrics_daily", ""},
		{"ParenthesizedVariable", "", "use a literal or same-package const table name"},
		{"Chosen", "", "call the helper directly"},
	} {
		loader.refused = nil
		fn, ok := pkg.funcs[c.fn]
		if !ok {
			t.Fatalf("fixture function %s not loaded", c.fn)
		}
		keyed, unkeyed, _ := readsOf(loader.reads(fn), tables)
		got := strings.Join(append(keyed, unkeyed...), ",")
		refused := ""
		if len(loader.refused) > 0 {
			refused = loader.refused[0]
		}
		if got != c.tables || (c.refusal == "") != (refused == "") || !strings.HasSuffix(refused, c.refusal) {
			t.Errorf("%s: tables [%s] refused %q; want tables [%s] refused %q", c.fn, got, loader.refused, c.tables, c.refusal)
		}
	}
}
