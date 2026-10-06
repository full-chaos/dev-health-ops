package syncadmin

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The stored sync_configurations.sync_targets list of a whole-integration
// config is a mirror of its dataset rows (CHAOS-8816). A reader that runs the
// canonical-incident gate on it must take its targets from
// providersync.IncidentGateTargets, or a target a row put in the list refuses
// a request or a scheduled run. The two censuses below keep that true for
// code that does not exist yet.
//
// What they pin: (1) the set of Go functions that hold a statement reading
// the column; (2) the set of Go functions that call one of the gate's entry
// points, and for each one that gates the STORED list, that the function
// calls one of the shared functions. What they do not pin: a reader that
// selects the column through `SELECT *` or builds the statement from parts,
// and the data flow inside a function (a function that calls the shared
// function and then gates a different list passes).

// storedTargetReaders is every Go function of the production tree that holds
// a statement reading sync_configurations.sync_targets, with what it does
// with the list. A new reader is added here on purpose, after the question
// "does a mirrored target change its result?" has an answer.
var storedTargetReaders = map[string]string{
	"internal/api/syncadmin.<package level>": "the api's column list (scanSyncConfig): the routes read the list from it; " +
		"the routes that gate it are named in incidentGateCallers",
	"internal/scheduler/sync.configuredIncidentGateTargets": "the scheduler's pre-mint gate: returns providersync.IncidentGateTargets of the list",
	"internal/scheduler/sync.loadMaterializationPlan": "the scheduler's plan: gates providersync.IncidentGateTargets of the list; " +
		"the list narrows the datasets of a child config only",
	"internal/scheduler/sync.preparePagerDutyRepair": "PagerDuty only (its list is never a mirror): the list must be exactly [operational]",
	"internal/synchandoff.LoadConfig": "the manual trigger and the webhook sync-now request: the list narrows the datasets of a child config only " +
		"and is copied to the trigger for a log line",
	"internal/synccoverage.loadConfig": "sync coverage: the list scopes the datasets of a child or not-planner-managed config, " +
		"intersected with the enabled rows; no gate",
	"internal/queryapi/datahealth.<package level>": "the data-health connector list: the first three items as a display label; no gate",
}

var storedTargetRead = regexp.MustCompile(`(?is)\bselect\b.*\bsync_targets\b|\bsync_targets::text\b`)

// incidentGateEntryPoints is the functions that decide "do these legacy
// targets need the canonical-incident feature".
var incidentGateEntryPoints = map[string]bool{
	"requireCanonicalIncident":            true,
	"syncTargetsRequireCanonicalIncident": true,
	"SyncTargetsRequireCanonicalIncident": true,
}

// sharedGateTargetFunctions is the functions that return the stored list
// without its mirrored items.
var sharedGateTargetFunctions = map[string]bool{
	"IncidentGateTargets":           true, // providersync
	"incidentGateTargets":           true, // syncadmin, over the list as the routes decode it
	"configuredIncidentGateTargets": true, // scheduler/sync, the pre-mint read
}

type incidentGateCaller struct {
	// storedList: the function gates the stored list of a config and must
	// call a shared function.
	storedList bool
	source     string
}

// incidentGateCallers is every Go function of the production tree that calls
// a gate entry point, with the list it gates.
var incidentGateCallers = map[string]incidentGateCaller{
	"internal/api/syncadmin.triggerSyncConfig":   {true, "Sync now: the stored list"},
	"internal/api/syncadmin.backfillSyncConfig":  {true, "backfill: the stored list"},
	"internal/api/syncadmin.replaceRepositories": {true, "PUT repositories: the stored list"},
	"internal/api/syncadmin.updateSyncConfigTx": {true, "the save: with a list, the targets the request adds and the passthrough targets; " +
		"with no list, the stored list"},
	"internal/api/syncadmin.createSyncConfig":                     {false, "the create: the submitted list (a request asked for every item)"},
	"internal/api/syncadmin.batchCreateSyncConfigs":               {false, "the batch create: the submitted list"},
	"internal/scheduler/sync.coordinatorEligibility":              {true, "the scheduler's pre-mint gate: the stored list"},
	"internal/scheduler/sync.loadMaterializationPlan":             {true, "the scheduler's plan: the stored list"},
	"internal/scheduler/sync.SyncTargetsRequireCanonicalIncident": {false, "the exported entry point itself"},
	"internal/syncdispatchruntime.datasetScopeRequiresCanonicalIncident": {false,
		"dispatch: the legacy targets of one dataset key of a run unit or an enabled row, from the registry"},
}

type gateCensus struct {
	readers []string
	// callers: function -> the names of every function it calls.
	callers map[string]map[string]bool
	files   int
}

func scanGateCensus(t *testing.T, repoRoot string, roots ...string) gateCensus {
	t.Helper()
	census := gateCensus{callers: map[string]map[string]bool{}}
	readers := map[string]bool{}
	for _, root := range roots {
		err := filepath.WalkDir(filepath.Join(repoRoot, root), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			census.files++
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(repoRoot, filepath.Dir(path))
			if err != nil {
				return err
			}
			for _, declaration := range parsed.Decls {
				name := "<package level>"
				if function, ok := declaration.(*ast.FuncDecl); ok {
					name = function.Name.Name
				}
				qualified := filepath.ToSlash(relative) + "." + name
				called := map[string]bool{}
				gates := false
				ast.Inspect(declaration, func(node ast.Node) bool {
					switch typed := node.(type) {
					case *ast.BasicLit:
						if typed.Kind == token.STRING {
							if text, err := strconv.Unquote(typed.Value); err == nil && storedTargetRead.MatchString(text) {
								readers[qualified] = true
							}
						}
					case *ast.CallExpr:
						callee := ""
						switch function := typed.Fun.(type) {
						case *ast.Ident:
							callee = function.Name
						case *ast.SelectorExpr:
							callee = function.Sel.Name
						}
						called[callee] = true
						if incidentGateEntryPoints[callee] {
							gates = true
						}
					}
					return true
				})
				if gates {
					census.callers[qualified] = called
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", root, err)
		}
	}
	for name := range readers {
		census.readers = append(census.readers, name)
	}
	sort.Strings(census.readers)
	return census
}

func gateCensusOfTheTree(t *testing.T) gateCensus {
	t.Helper()
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("no caller file")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	census := scanGateCensus(t, repoRoot, "internal", "cmd")
	if census.files < 100 || len(census.readers) == 0 || len(census.callers) == 0 {
		t.Fatalf("the scan read %d files and found %d readers and %d gate callers: it measured nothing",
			census.files, len(census.readers), len(census.callers))
	}
	return census
}

// TestEveryReaderOfTheStoredTargetListIsNamed: the functions that read
// sync_configurations.sync_targets are exactly the named list.
func TestEveryReaderOfTheStoredTargetListIsNamed(t *testing.T) {
	census := gateCensusOfTheTree(t)
	want := make([]string, 0, len(storedTargetReaders))
	for name, use := range storedTargetReaders {
		if strings.TrimSpace(use) == "" {
			t.Errorf("reader %s has no use", name)
		}
		want = append(want, name)
	}
	sort.Strings(want)
	if !reflect.DeepEqual(census.readers, want) {
		t.Fatalf("the functions that read sync_configurations.sync_targets changed.\n got  %v\n want %v\n"+
			"The stored list of a whole-integration config is a mirror of its dataset rows (CHAOS-8816): a new reader "+
			"that gates it must use providersync.IncidentGateTargets, and is named here with what it does with the list.",
			census.readers, want)
	}
}

// TestEveryIncidentGateOnTheStoredListUsesTheSharedTargets: the functions
// that call a gate entry point are exactly the named list, and each one that
// gates the stored list calls a shared function.
func TestEveryIncidentGateOnTheStoredListUsesTheSharedTargets(t *testing.T) {
	census := gateCensusOfTheTree(t)
	got := make([]string, 0, len(census.callers))
	for name := range census.callers {
		got = append(got, name)
	}
	sort.Strings(got)
	want := make([]string, 0, len(incidentGateCallers))
	storedListCallers := 0
	for name, caller := range incidentGateCallers {
		if strings.TrimSpace(caller.source) == "" {
			t.Errorf("gate caller %s does not say which list it gates", name)
		}
		want = append(want, name)
		if caller.storedList {
			storedListCallers++
		}
	}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the functions that run the canonical-incident gate on a target list changed.\n got  %v\n want %v\n"+
			"A new caller is named here with the list it gates; one that gates the stored list of a config must take its "+
			"targets from providersync.IncidentGateTargets (CHAOS-8816).", got, want)
	}
	if storedListCallers == 0 {
		t.Fatal("no named caller gates the stored list: the check below measured nothing")
	}
	for name, caller := range incidentGateCallers {
		if !caller.storedList {
			continue
		}
		shared := false
		for callee := range census.callers[name] {
			if sharedGateTargetFunctions[callee] {
				shared = true
			}
		}
		if !shared {
			t.Errorf("%s gates the stored list (%s) and calls none of the shared functions %v: a target the list names only "+
				"because a dataset row is on would refuse it", name, caller.source, sortedKeys(sharedGateTargetFunctions))
		}
	}
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TestStoredTargetReadPatternMatchesTheStatementsItMustFind keeps the reader
// scan honest: it finds each statement shape in use and ignores a write.
func TestStoredTargetReadPatternMatchesTheStatementsItMustFind(t *testing.T) {
	for text, want := range map[string]bool{
		"SELECT sync_targets::jsonb, provider FROM public.sync_configurations WHERE id = $1::uuid": true,
		"\nSELECT id, org_id, provider, sync_targets, is_active\nFROM public.sync_configurations":  true,
		"id, name, provider, sync_targets::text, sync_options::text, is_active,":                   true,
		"WITH locked AS (\n SELECT sync_targets FROM public.sync_configurations\n FOR UPDATE\n)":   true,
		"select c.provider, c.name, c.sync_targets from sync_configurations c":                     true,
		"INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets) VALUES ($1)":   false,
		"sync_targets = $%d::json":                            false,
		"SELECT sync_options FROM public.sync_configurations": false,
		"sync_targets": false,
	} {
		if got := storedTargetRead.MatchString(text); got != want {
			t.Errorf("%q: match %v, want %v", text, got, want)
		}
	}
}
