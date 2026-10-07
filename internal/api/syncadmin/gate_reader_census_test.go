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

// The stored sync_configurations.sync_targets list holds the targets requests
// asked for (CHAOS-8816): every reader runs the canonical-incident gate on
// the list as it is stored, so every writer must store only targets a gate
// read. A save of a whole-integration config stores the submitted targets
// that were stored before or that it adds, and gates exactly that list; the
// save of a parent gives the same list to its children. The censuses below
// keep the readers, the gate callers and the writers a named set for code
// that does not exist yet.
//
// What they pin: (1) the set of Go functions that hold a statement reading
// the column; (2) the set of Go functions that call one of the gate's entry
// points; (3) the set of Go functions that hold a statement writing the
// column, and the set of functions that call one of those; (4) that the one
// function that writes a list to a config the request does not name
// (cascadeToChildren) never reads the submitted list of the request. What
// they do not pin: a reader that selects the column through `SELECT *` or
// builds the statement from parts, a writer whose statement does not hold the
// column name, a write outside Go (the Alembic revisions, the fixture
// generator), and the data flow inside a function: that the save stores what
// its gate read is pinned by behaviour (TestTheSaveStoresWhatTheGateReads,
// TestASaveStoresOnlyWhatWasStoredOrWhatItAdds and the tests in
// child_cascade_integration_test.go and main_equality_integration_test.go).

// storedTargetReaders is every Go function of the production tree that holds
// a statement reading sync_configurations.sync_targets, with what it does
// with the list. A new reader is added here on purpose.
var storedTargetReaders = map[string]string{
	"internal/api/syncadmin.<package level>": "the api's column list (scanSyncConfig): the routes read the list from it; " +
		"the routes that gate it are named in incidentGateCallers",
	"internal/scheduler/sync.configuredSyncTargets":   "the scheduler's pre-mint gate: gates the list as stored",
	"internal/scheduler/sync.loadMaterializationPlan": "the scheduler's plan: gates the list as stored; the list narrows the datasets of a child config only",
	"internal/scheduler/sync.preparePagerDutyRepair":  "PagerDuty only: the list must be exactly [operational]",
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

// incidentGateCallers is every Go function of the production tree that calls
// a gate entry point, with the list it gates.
var incidentGateCallers = map[string]string{
	"internal/api/syncadmin.triggerSyncConfig":   "Sync now: the stored list",
	"internal/api/syncadmin.backfillSyncConfig":  "backfill: the stored list",
	"internal/api/syncadmin.replaceRepositories": "PUT repositories: the stored list",
	"internal/api/syncadmin.updateSyncConfigTx": "the save: with a list, the list the save stores (for a config whose rows own its selection the " +
		"submitted targets that were stored or that it adds, else the submitted list); with no list, the stored list",
	"internal/api/syncadmin.createSyncConfig":                            "the create: the submitted list (a request asked for every item)",
	"internal/api/syncadmin.batchCreateSyncConfigs":                      "the batch create: the submitted list",
	"internal/scheduler/sync.coordinatorEligibility":                     "the scheduler's pre-mint gate: the stored list",
	"internal/scheduler/sync.loadMaterializationPlan":                    "the scheduler's plan: the stored list",
	"internal/scheduler/sync.SyncTargetsRequireCanonicalIncident":        "the exported entry point itself",
	"internal/syncdispatchruntime.datasetScopeRequiresCanonicalIncident": "dispatch: the legacy targets of one dataset key of a run unit or an enabled row, from the registry",
}

// storedTargetWriters is every Go function of the production tree that holds
// a statement writing sync_configurations.sync_targets, with the list it
// writes.
var storedTargetWriters = map[string]string{
	"internal/api/syncadmin.createPlannerManagedConfig": "the create and the batch create: the submitted list, to the new whole-integration config only",
	"internal/api/syncadmin.writeConfigChanges":         "the save: the list its caller hands it, to the config its caller hands it",
	"internal/testsupport/pgseed.SyncConfiguration":     "a test seed (not in a production binary): a fixed list",
}

var storedTargetWrite = regexp.MustCompile(`(?is)\binsert\s+into\s+(public\.)?sync_configurations\b.*\bsync_targets\b|\bsync_targets\s*=[^=]`)

type targetWriterCaller struct {
	// toAnotherConfig: the function writes a list to a config the request
	// does not name. The list is the one the save stored on the config of the
	// request, so the function must not read the request's submitted list.
	toAnotherConfig bool
	source          string
}

// storedTargetWriterCallers is every Go function of the production tree that
// calls a function of storedTargetWriters, with the list it hands over.
var storedTargetWriterCallers = map[string]targetWriterCaller{
	"internal/api/syncadmin.createSyncConfigTx":       {false, "the create: the submitted list, for the config the request creates"},
	"internal/api/syncadmin.batchCreateSyncConfigsTx": {false, "the batch create: the submitted list, for the config the request creates"},
	"internal/api/syncadmin.updateSyncConfigTx": {false, "the save: for the config of the request, the list its gate read (the submitted targets " +
		"that were stored or that the save adds when its rows own the selection, else the submitted list)"},
	"internal/api/syncadmin.cascadeToChildren": {true, "the save of a parent: for each child, the list the save stored on the parent"},
}

// submittedListField is the field of the decoded request body that holds the
// submitted sync_targets list.
const submittedListField = "syncTargets"

type gateCensus struct {
	readers []string
	writers []string
	// calls: every function -> the names of every function it calls.
	calls map[string]map[string]bool
	// callers: function -> the names of every function it calls.
	callers map[string]map[string]bool
	// fields: function -> the names of every field or method it selects.
	fields map[string]map[string]bool
	files  int
}

func scanGateCensus(t *testing.T, repoRoot string, roots ...string) gateCensus {
	t.Helper()
	census := gateCensus{callers: map[string]map[string]bool{}, calls: map[string]map[string]bool{}, fields: map[string]map[string]bool{}}
	readers := map[string]bool{}
	writers := map[string]bool{}
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
				selected := map[string]bool{}
				gates := false
				ast.Inspect(declaration, func(node ast.Node) bool {
					switch typed := node.(type) {
					case *ast.BasicLit:
						if typed.Kind == token.STRING {
							if text, err := strconv.Unquote(typed.Value); err == nil {
								if storedTargetRead.MatchString(text) {
									readers[qualified] = true
								}
								if storedTargetWrite.MatchString(text) {
									writers[qualified] = true
								}
							}
						}
					case *ast.SelectorExpr:
						selected[typed.Sel.Name] = true
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
				if name != "<package level>" {
					census.calls[qualified] = called
					census.fields[qualified] = selected
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
	for name := range writers {
		census.writers = append(census.writers, name)
	}
	sort.Strings(census.writers)
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
	if census.files < 100 || len(census.readers) == 0 || len(census.callers) == 0 || len(census.writers) == 0 {
		t.Fatalf("the scan read %d files and found %d readers, %d writers and %d gate callers: it measured nothing",
			census.files, len(census.readers), len(census.writers), len(census.callers))
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
			"Every reader gates the stored list as it is stored (CHAOS-8816): a new reader is named here with what it "+
			"does with the list.",
			census.readers, want)
	}
}

// TestEveryCallerOfTheIncidentGateIsNamed: the functions that call a gate
// entry point are exactly the named list.
func TestEveryCallerOfTheIncidentGateIsNamed(t *testing.T) {
	census := gateCensusOfTheTree(t)
	got := make([]string, 0, len(census.callers))
	for name := range census.callers {
		got = append(got, name)
	}
	sort.Strings(got)
	want := make([]string, 0, len(incidentGateCallers))
	for name, source := range incidentGateCallers {
		if strings.TrimSpace(source) == "" {
			t.Errorf("gate caller %s does not say which list it gates", name)
		}
		want = append(want, name)
	}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the functions that run the canonical-incident gate on a target list changed.\n got  %v\n want %v\n"+
			"A new caller is named here with the list it gates (CHAOS-8816).", got, want)
	}
}

// TestEveryWriterOfTheStoredTargetListIsNamed: the functions that write
// sync_configurations.sync_targets, and the functions that hand them a list,
// are exactly the named sets; the one that writes to another config never
// reads the submitted list of the request.
func TestEveryWriterOfTheStoredTargetListIsNamed(t *testing.T) {
	census := gateCensusOfTheTree(t)
	want := make([]string, 0, len(storedTargetWriters))
	writerNames := map[string]bool{}
	for name, list := range storedTargetWriters {
		if strings.TrimSpace(list) == "" {
			t.Errorf("writer %s does not say which list it writes", name)
		}
		want = append(want, name)
		// A test seed is no production writer: its callers are tests.
		if !strings.HasPrefix(name, "internal/testsupport/") {
			writerNames[name[strings.LastIndex(name, ".")+1:]] = true
		}
	}
	sort.Strings(want)
	if !reflect.DeepEqual(census.writers, want) {
		t.Fatalf("the functions that write sync_configurations.sync_targets changed.\n got  %v\n want %v\n"+
			"Every reader gates the stored list as it is stored (CHAOS-8816): a new writer must store only targets a gate "+
			"read, and is named here with the list it writes.", census.writers, want)
	}
	got := []string{}
	for name, called := range census.calls {
		for callee := range called {
			if writerNames[callee] {
				got = append(got, name)
				break
			}
		}
	}
	sort.Strings(got)
	want = want[:0]
	toAnotherConfig := 0
	for name, caller := range storedTargetWriterCallers {
		if strings.TrimSpace(caller.source) == "" {
			t.Errorf("writer caller %s does not say which list it hands over", name)
		}
		want = append(want, name)
		if caller.toAnotherConfig {
			toAnotherConfig++
		}
	}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the functions that hand a list to a writer of sync_configurations.sync_targets changed.\n got  %v\n want %v\n"+
			"A new caller is named here with the list it hands over; one that writes a list to a config other than the one "+
			"the request names must write the list the save stored, never the submitted list (CHAOS-8816).", got, want)
	}
	if toAnotherConfig == 0 || !census.fields["internal/api/syncadmin.updateSyncConfigTx"][submittedListField] {
		t.Fatal("no named caller writes to another config, or the save does not read the submitted list by the field " +
			submittedListField + ": the check below measured nothing")
	}
	for name, caller := range storedTargetWriterCallers {
		if caller.toAnotherConfig && census.fields[name][submittedListField] {
			t.Errorf("%s writes a list to another config (%s) and reads the submitted list of the request: "+
				"an item the request names only because a dataset row is on would become the child's selection", name, caller.source)
		}
	}
}

// TestStoredTargetWritePatternMatchesTheStatementsItMustFind keeps the writer
// scan honest: it finds each statement shape in use and ignores a read.
func TestStoredTargetWritePatternMatchesTheStatementsItMustFind(t *testing.T) {
	for text, want := range map[string]bool{
		"INSERT INTO sync_configurations\n(id, org_id, name, provider, sync_targets, sync_options) VALUES ($1)": true,
		"insert into public.sync_configurations (id, sync_targets) values ($1, $2)":                             true,
		"sync_targets = $%d::json": true,
		"UPDATE public.sync_configurations SET sync_targets=$2::json WHERE id = $1":                   true,
		"UPDATE public.sync_configurations\nSET last_sync_at = $2, last_sync_stats = $5::json":        false,
		"SELECT sync_targets::jsonb, provider FROM public.sync_configurations WHERE id = $1::uuid":    false,
		"SELECT coalesce(bool_and(sync_targets::jsonb='[\"operational\"]'::jsonb),FALSE) FROM locked": false,
		"INSERT INTO integrations (id, org_id) VALUES ($1, $2)":                                       false,
		"sync_targets": false,
	} {
		if got := storedTargetWrite.MatchString(text); got != want {
			t.Errorf("%q: match %v, want %v", text, got, want)
		}
	}
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
