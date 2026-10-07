package clicredentials

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const clickhousePackage = "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"

// classifiedOpenSites is the closed list of the places that open a ClickHouse
// connection (a call to the storage package's Open), keyed by file relative to
// internal/ or cmd/, with the number of calls and why what the place prints is
// safe. A verb's family is proved by TestNoVerbPrintsTheClickHouseLoginOrPassword
// (verbs_integration_test.go) or, for `dho sync`, by synccli's
// TestSyncTargetPrintsNeitherClickHouseLoginNorPassword. A new open site fails
// here until it is classified.
var classifiedOpenSites = map[string]struct {
	calls int
	why   string
}{
	"aicli/aicli.go":                              {1, "verb `dho ai allowlist`: every text through secrets.Boundary (verbs_integration_test.go)"},
	"chmigrate/command.go":                        {2, "verbs `dho migrate clickhouse upgrade|status|repair`: through secrets.Boundary (verbs_integration_test.go)"},
	"fixturescli/generate.go":                     {1, "verb `dho fixtures generate`: through secrets.Boundary (verbs_integration_test.go)"},
	"fixturescli/product_telemetry.go":            {1, "verb `dho fixtures product-telemetry`: through secrets.Boundary (verbs_integration_test.go)"},
	"fixturescli/synthetic.go":                    {1, "verb `dho fixtures load-synthetic`: through secrets.Boundary (verbs_integration_test.go)"},
	"metricscli/validateflags.go":                 {1, "verb `dho metrics validate-flags`: through secrets.Boundary (verbs_integration_test.go)"},
	"operationalbackfill/command.go":              {1, "verb `dho backfill operational`: through secrets.Boundary (verbs_integration_test.go)"},
	"synccli/synccli.go":                          {1, "verb `dho sync teams` (Atlassian): redact() on every returned text and the process logger wrapped for the run (synccli TestSyncTeamsLogsNeitherClickHouseLoginNorPassword)"},
	"synccli/inline.go":                           {1, "verbs `dho sync <target>`: the store open is redacted through secrets.Boundary and runTarget redacts every other error (synccli credential_redaction_test.go)"},
	"workersctl/main.go":                          {2, "verb `dho workers ...` (providersync cleanup, run marker): prints a constant message, never the error text"},
	"apiservice/admin/orgdeletion.go":             {1, "API handler: a constant warning, never the error text"},
	"apiservice/deps.go":                          {1, "daemon (api): the error is logged at startup; the open redacts its own error (storage/clickhouse factory)"},
	"queryapi/server/investment_explain_route.go": {1, "daemon (query-api): logged at runtime; the open redacts its own error"},
	"queryapi/server/workunit_explain_route.go":   {1, "daemon (query-api): logged at runtime; the open redacts its own error"},
	"streamrunnerservice/dependencies.go":         {1, "daemon (stream runner): logged at startup; the open redacts its own error"},
	"workerservice/daily.go":                      {8, "daemon (worker): logged at startup; the open redacts its own error"},
	"workerservice/provider_sync.go":              {2, "daemon (worker): logged at startup; the open redacts its own error"},
	"workerservice/reports.go":                    {1, "daemon (worker): logged at startup; the open redacts its own error"},
	"workerservice/sync_dispatch.go":              {1, "daemon (worker): logged at startup; the open redacts its own error"},
	"workerservice/workgraph.go":                  {2, "daemon (worker): logged at startup; the open redacts its own error"},
}

// TestEveryClickHouseOpenSiteIsClassified lists every call to the ClickHouse
// storage package's Open in non-test code and requires each place to be on
// classifiedOpenSites with the same number of calls.
func TestEveryClickHouseOpenSiteIsClassified(t *testing.T) {
	module, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]int{}
	files := 0
	for _, top := range []string{"internal", "cmd"} {
		root := filepath.Join(module, top)
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, _ := filepath.Rel(root, path)
			if top == "internal" && strings.HasPrefix(filepath.ToSlash(relative), "storage/clickhouse/") ||
				top == "internal" && strings.HasPrefix(filepath.ToSlash(relative), "testsupport/") {
				return nil
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			files++
			aliases := map[string]bool{}
			for _, spec := range parsed.Imports {
				if value, _ := strconv.Unquote(spec.Path.Value); value == clickhousePackage {
					name := "clickhouse"
					if spec.Name != nil {
						name = spec.Name.Name
					}
					aliases[name] = true
				}
			}
			if len(aliases) == 0 {
				return nil
			}
			ast.Inspect(parsed, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Open" {
					return true
				}
				if pkg, ok := selector.X.(*ast.Ident); ok && aliases[pkg.Name] {
					key := filepath.ToSlash(relative)
					if top == "cmd" {
						key = "cmd/" + key
					}
					found[key]++
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if files < 500 {
		t.Fatalf("the scan read %d files: it measured nothing", files)
	}
	var keys []string
	for key := range found {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		entry, ok := classifiedOpenSites[key]
		switch {
		case !ok:
			t.Errorf("%s opens ClickHouse %d time(s) and is not classified: if it prints a database error, add a row to verbs() in verbs_integration_test.go and classify it here", key, found[key])
		case entry.calls != found[key]:
			t.Errorf("%s opens ClickHouse %d time(s), classified %d (%s): classify the new open", key, found[key], entry.calls, entry.why)
		}
	}
	for key := range classifiedOpenSites {
		if found[key] == 0 {
			t.Errorf("%s is classified but no longer opens ClickHouse: remove the row", key)
		}
	}
}
