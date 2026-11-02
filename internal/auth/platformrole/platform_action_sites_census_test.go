package platformrole

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

type platformActionSite struct{ file, function, action, origin string }

// readsTheTargetsFlag are mix rows that also read a flag, but the flag of the
// one they act on (a fact about the target), never the caller's power.
var readsTheTargetsFlag = map[string]bool{
	"internal/auth/authapi/provisioning.go\tprovisioningHandlers.targetsRule": true,
}

func (s platformActionSite) key() string { return s.file + "\t" + s.function + "\t" + s.action }

var actionTakingCalls = map[string]bool{
	"Holds": true, "WrapPlatform": true, "WrapAdminOr": true, "BodyFirstPlatform": true, "BodyFirstAdminOr": true,
	"bodyFirstPlatform": true, "bodyFirstAdminOr": true,
}

// platformActionReads lists, per function, the actions it names in a Holds
// call: x.Holds(<ActionAdmin*|actionAdmin*|rule.ActionAdmin*>).
func platformActionReads(file *ast.File) map[string]map[string]bool {
	found := map[string]map[string]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			// A decision names its action in the first argument: a Holds call, or a platform
			// wrap of a route (WrapPlatform, WrapAdminOr and the body-first forms).
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !actionTakingCalls[sel.Sel.Name] || (sel.Sel.Name == "Holds" && len(call.Args) != 1) {
				return true
			}
			name := ""
			switch arg := call.Args[0].(type) {
			case *ast.Ident:
				name = arg.Name
			case *ast.SelectorExpr:
				name = arg.Sel.Name
			}
			trimmed := strings.TrimPrefix(strings.TrimPrefix(name, "Action"), "action")
			if trimmed == name || !strings.HasPrefix(trimmed, "Admin") {
				return true
			}
			action := "admin." + strings.ToLower(strings.TrimPrefix(trimmed, "Admin"))
			scope := funcName(fn)
			if found[scope] == nil {
				found[scope] = map[string]bool{}
			}
			found[scope][action] = true
			return true
		})
	}
	return found
}

// TestEveryPlatformActionSiteIsListed is the census of the sites that decide
// by an explicit platform action: each is listed with its action, every action
// is a platform action of the catalog, and a site that stood in for
// organization power (a mix row) no longer reads the superuser flag.
func TestEveryPlatformActionSiteIsListed(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	listed := readPlatformActionSites(t)
	found := map[string]bool{}
	fset := token.NewFileSet()
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			rel := filepath.ToSlash(strings.TrimPrefix(path, root+string(filepath.Separator)))
			for function, actions := range platformActionReads(file) {
				for action := range actions {
					found[rel+"\t"+function+"\t"+action] = true
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	var problems []string
	for key := range found {
		if _, ok := listed[key]; !ok {
			problems = append(problems, "unlisted platform action site (add it to testdata/platform_action_sites.tsv): "+key)
		}
	}
	flagged := readSuperuserSites(t)
	mixes := map[string]bool{}
	for key, site := range listed {
		if !found[key] {
			problems = append(problems, "listed platform action site not found (moved or changed): "+key)
		}
		if !rule.PlatformOf(site.action).Has(site.action) {
			problems = append(problems, "not a platform action of the catalog: "+key)
		}
		if _, n := strconv.Atoi(site.origin); n == nil {
			mixes[site.origin] = true
		}
		if _, reads := flagged[site.file+"\t"+site.function]; reads && !readsTheTargetsFlag[site.file+"\t"+site.function] {
			problems = append(problems, "a deciding site still reads the superuser flag: "+key)
		}
	}
	for i := 1; i <= 9; i++ {
		if !mixes[strconv.Itoa(i)] {
			problems = append(problems, "mix site "+strconv.Itoa(i)+" has no platform action row")
		}
	}
	sort.Strings(problems)
	for _, p := range problems {
		t.Error(p)
	}
}

func readPlatformActionSites(t *testing.T) map[string]platformActionSite {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "platform_action_sites.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	sites := map[string]platformActionSite{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 4 {
			t.Fatalf("platform_action_sites.tsv: %q is not file, function, action, origin", line)
		}
		site := platformActionSite{fields[0], fields[1], fields[2], fields[3]}
		if _, dup := sites[site.key()]; dup {
			t.Fatalf("platform_action_sites.tsv: %s listed twice", site.key())
		}
		sites[site.key()] = site
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return sites
}
