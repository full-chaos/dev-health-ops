package queuecreds_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var queueRowExpressions = []struct {
	pattern *regexp.Regexp
	key     func(match []string) string
}{
	{regexp.MustCompile(`\bargs\s*->>\s*'([a-z_]+)'`), func(m []string) string { return "args->>" + m[1] }},
	{regexp.MustCompile(`\bargs\s*->\s*'([a-z_]+)'\s*->>\s*'([a-z_]+)'`), func(m []string) string { return "args->" + m[1] + "->>" + m[2] }},
	{regexp.MustCompile(`\bargs\s*#>>\s*'\{([a-z_,]+)\}'`), func(m []string) string { return "args#>>" + m[1] }},
	{regexp.MustCompile(`\bargs\s*->\s*'([a-z_]+)'`), func(m []string) string { return "args->" + m[1] }},
	{regexp.MustCompile(`\b(?:[a-z_]+\.)?args\s*(?:::|,|\s+AS\b)`), func([]string) string { return "args" }},
	{regexp.MustCompile(`\bencoded_args\b`), func([]string) string { return "encoded_args" }},
	{regexp.MustCompile(`\bdedupe_key\b`), func([]string) string { return "dedupe_key" }},
}

// queueRowReads finds every SQL string literal outside tests that names a
// field of a queue row's JSON arguments or its dedupe key, keyed by the
// declaration that holds it.
func queueRowReads(t *testing.T, root string) map[string][]string {
	t.Helper()
	found := map[string]map[string]bool{}
	fset := token.NewFileSet()
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() && entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			for _, decl := range file.Decls {
				name := ""
				switch typed := decl.(type) {
				case *ast.FuncDecl:
					name = typed.Name.Name
				case *ast.GenDecl:
					for _, spec := range typed.Specs {
						if value, ok := spec.(*ast.ValueSpec); ok && len(value.Names) > 0 {
							name = value.Names[0].Name
						}
					}
				}
				ast.Inspect(decl, func(node ast.Node) bool {
					lit, ok := node.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						return true
					}
					text, err := strconv.Unquote(lit.Value)
					upper := strings.ToUpper(text)
					if err != nil || !(strings.Contains(upper, "SELECT") || strings.Contains(upper, "INSERT") || strings.Contains(upper, "UPDATE")) {
						return true
					}
					for _, expression := range queueRowExpressions {
						for _, match := range expression.pattern.FindAllStringSubmatch(text, -1) {
							where := rel + "#" + name
							if found[where] == nil {
								found[where] = map[string]bool{}
							}
							found[where][expression.key(match)] = true
						}
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	out := map[string][]string{}
	for where, keys := range found {
		for key := range keys {
			out[where] = append(out[where], key)
		}
		sort.Strings(out[where])
	}
	return out
}

// How a read of a queue row's JSON arguments or dedupe key is made safe.
const (
	// roleOutput: the value can reach a log line, a summary or any other
	// output; the named guard file calls the shared value detector on it.
	roleOutput = "output-guarded"
	// roleDecoded: the arguments go through jobcontract.Decode, which refuses a
	// credential-shaped value.
	roleDecoded = "decoded"
	// roleWritten: the arguments are written from jobcontract.MarshalCanonical.
	roleWritten = "written"
	// roleInternal: the value is a join key, a filter or a copied column and is
	// never printed, returned or logged by this declaration.
	roleInternal = "internal"
	// roleElsewhere: not a queue row (another store) or not operator code.
	roleElsewhere = "elsewhere"
)

type queueRowRead struct {
	exprs  string
	role   string
	guard  string
	needle string
	reason string
}

const guardCall = "credshape.AnyCredentialShaped("

var queueRowReadTable = map[string]queueRowRead{
	"internal/joboperator/postgres_backend.go#summaryQuery": {
		"args->>correlation_id, args->>organization_id, args->domain, args->domain->>id, args->domain->>type",
		roleOutput, "internal/joboperator/postgres_backend.go", guardCall, "jobs inspect, jobs list and the mutation results print these fields"},
	"internal/joboutbox/strand_repair.go#retiredKindsSQL": {
		"args->>organization_id",
		roleOutput, "internal/joboutbox/loop.go", guardCall + "observation.OrganizationID", "the retired-kind warning logs the organization id"},
	"internal/joboutbox/strand_repair.go#repairStrandedProviderUnitSQL": {
		"args#>>domain,id, args#>>domain,type, args->>organization_id, dedupe_key",
		roleOutput, "internal/joboutbox/loop.go", guardCall + "rearm.DedupeKey", "the provider-unit rearm log line prints the dedupe key"},
	"internal/joboutbox/strand_repair.go#repairStrandedPartitionSQL": {
		"args#>>domain,id, args#>>domain,type, args->>organization_id, dedupe_key",
		roleInternal, "", "", "join and filter on the domain link; the dedupe key stays in strandCandidate and is reported for the provider-unit shape only"},
	"internal/joboutbox/strand_repair.go#repairStrandedFinalizeSQL": {
		"args#>>domain,id, args#>>domain,type, args->>organization_id, dedupe_key",
		roleInternal, "", "", "same as the partition shape"},
	"internal/joboutbox/strand_repair.go#repairStrandedWorkGraphSQL": {
		"args#>>domain,id, args->>organization_id, dedupe_key",
		roleInternal, "", "", "same as the partition shape"},
	"internal/joboutbox/repository.go#rowColumns": {
		"args, dedupe_key",
		roleDecoded, "internal/joboutbox/inserter.go", "jobcontract.Decode(", "the relay claim row goes through Decode before any River insert"},
	"internal/joboutbox/repository.go#DeleteTerminalBefore": {
		"dedupe_key",
		roleInternal, "", "", "copies the key into the abandonment table inside one statement"},
	"internal/joboutbox/producer.go#publish": {
		"args, dedupe_key",
		roleWritten, "internal/joboutbox/producer.go", "jobcontract.MarshalCanonical(", "the outbox row is written from the canonical, guarded encoding"},
	"internal/joboutbox/terminal_delivery_repair.go#repairProviderUnitTerminalDeliverySQL": {
		"args, args#>>domain,id, args#>>domain,type, args#>>payload,unit_id, args->>organization_id, dedupe_key",
		roleInternal, "", "", "equality join against the unit row and the River job; the key equals sync.provider_unit:<unit id>"},
	"internal/joboutbox/undelivered_repair.go#undeliveredClassifySQL": {
		"args, args#>>domain,id, args->>organization_id",
		roleInternal, "", "", "join against the request row; the result carries request id and reason only"},
	"internal/joboutbox/undelivered_repair.go#undeliveredCancelRequestsSQL": {
		"args#>>domain,id", roleInternal, "", "", "join against the request row"},
	"internal/joboutbox/undelivered_repair.go#undeliveredMarkDeadSQL": {
		"args#>>domain,id", roleInternal, "", "", "join against the request row"},
	"internal/jobs/metrics/daily/redrive.go#ReconcileOrphanedFinalizeRedriveRuns": {
		"dedupe_key", roleInternal, "", "", "join key for the abandonment check; never logged"},
	"internal/jobs/metrics/remaining/redrive.go#deadHandoffReasonSQL": {
		"dedupe_key", roleInternal, "", "", "equality with a key built from a partition id"},
	"internal/syncreconciler/orphaned_unit_repair.go#readOrphanedSourceRowSQL": {
		"args, dedupe_key",
		roleDecoded, "internal/syncreconciler/orphaned_unit_repair.go", "jobcontract.Decode(", "the source arguments are decoded before a replacement row is written"},
	"internal/syncreconciler/orphaned_unit_repair.go#insertReclaimRowSQL": {
		"args, dedupe_key",
		roleDecoded, "internal/syncreconciler/orphaned_unit_repair.go", "jobcontract.MarshalCanonical(", "the replacement row is written from the canonical encoding"},
	"internal/syncreconciler/orphaned_unit_repair.go#selectOrphanedUnitDeliverySQL": {
		"args#>>domain,id, args#>>payload,unit_id, dedupe_key",
		roleInternal, "", "", "equality with keys built from unit ids"},
	"internal/syncreconciler/orphaned_unit_repair.go#noLiveDeliveryForUnitKeys": {
		"dedupe_key", roleInternal, "", "", "equality with keys built from unit ids"},
	"internal/syncreconciler/unreclaimable_sweep.go#selectPublishedDedupeKeysSQL": {
		"dedupe_key", roleInternal, "", "", "equality with keys built from unit ids"},
	"internal/syncreconciler/unreclaimable_sweep.go#selectUnreclaimableCandidatesSQL": {
		"dedupe_key", roleInternal, "", "", "equality with a key built from the unit id"},
	"internal/syncreconciler/unreclaimable_sweep.go#terminalizeTerminalDeliverySQL": {
		"dedupe_key", roleInternal, "", "", "equality with a key built from the unit id"},
	"internal/storage/river/telemetry.go#queueTelemetrySQL": {
		"args->>contract_version, args->contract_version",
		roleInternal, "", "", "only a digits-only contract version reaches the metric label"},
	"internal/scheduler/fixed/reports.go#replayedReportRunSQL": {
		"dedupe_key", roleElsewhere, "", "", "reads the handoff and abandonment tables by a key built from a run id"},
	"internal/providerfoundation/sinks.go#inspectClickHouseEnvelope": {
		"dedupe_key", roleElsewhere, "", "", "ClickHouse event table, not a queue row"},
	"internal/providersync/launchdarkly_effects_clickhouse.go#featureFlagEventInsert": {
		"dedupe_key", roleElsewhere, "", "", "ClickHouse event table, not a queue row"},
	"internal/providersync/launchdarkly_effects_clickhouse.go#inspectEvent": {
		"dedupe_key", roleElsewhere, "", "", "ClickHouse event table, not a queue row"},
	"internal/queryapi/reports/writeproofcases/cases.go#deleteObservedSQL": {
		"args, dedupe_key", roleElsewhere, "", "", "write-proof test support, run by tests only"},
	"internal/testsupport/pgseed/pgseed.go#WorkerJobOutbox": {
		"args, dedupe_key", roleElsewhere, "", "", "test seeding helper"},
}

// Every SQL literal that reads a field of a queue row's JSON arguments or its
// dedupe key is classified here. A new literal, or a new projected field in a
// classified one, fails until it is classified and, for an output, guarded.
func TestEveryQueueRowReadIsClassifiedAndItsGuardIsPresent(t *testing.T) {
	root := repoRoot(t)
	reads := queueRowReads(t, root)
	for where, keys := range reads {
		entry, ok := queueRowReadTable[where]
		if !ok {
			t.Errorf("unclassified queue row read %s projects %s: add it to queueRowReadTable and, if it can reach an output, guard it", where, strings.Join(keys, ", "))
			continue
		}
		if got := strings.Join(keys, ", "); got != entry.exprs {
			t.Errorf("%s reads %s, classified as %s: classify the new field and guard it if it can reach an output", where, got, entry.exprs)
		}
	}
	for where, entry := range queueRowReadTable {
		if _, ok := reads[where]; !ok {
			t.Errorf("classified read %s no longer exists", where)
		}
		if entry.reason == "" {
			t.Errorf("%s has no reason", where)
		}
		switch entry.role {
		case roleOutput, roleDecoded, roleWritten:
			data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(entry.guard)))
			if err != nil {
				t.Errorf("%s: guard file: %v", where, err)
				continue
			}
			if !strings.Contains(string(data), entry.needle) {
				t.Errorf("%s (%s): %s does not contain %q", where, entry.role, entry.guard, entry.needle)
			}
		case roleInternal, roleElsewhere:
			if entry.guard != "" {
				t.Errorf("%s: %s role takes no guard", where, entry.role)
			}
		default:
			t.Errorf("%s: unknown role %q", where, entry.role)
		}
	}
}

var (
	outputCall = regexp.MustCompile(`^(fmt\.(Errorf|Sprintf|Sprint|Sprintln|Fprintf|Printf|Println)|errors\.New|slog\.[A-Z][A-Za-z]*|.*(?:ogger|slog)(?:\(\))?\.(Debug|Info|Warn|Error)[A-Za-z]*)$`)
	rowKeyName = regexp.MustCompile(`(?i)^(dedupekey|sourcededupekey|idempotencykey|correlationid|encodedargs|args)$`)
)

// Queue-row fields that a job's own code reads after the decoder has accepted
// them; the value is guarded because Decode refused anything credential-shaped.
var decodedRowFieldOutputs = map[string]string{
	"internal/jobruntime/adapter.go#logStart":   "envelope fields after jobcontract.Decode",
	"internal/jobruntime/adapter.go#NewAdapter": "the typed argument type's registered kind name, not a queue row value",
}

// A queue row's dedupe key, idempotency key, correlation id or arguments must
// not be formatted into an error or a log line unless the enclosing function
// calls the shared value detector on it, or the value comes from a decoded envelope.
func TestNoQueueRowFieldIsFormattedIntoAnErrorOrLogUnguarded(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	seen := map[string]bool{}
	for _, dir := range []string{"internal/joboutbox", "internal/syncreconciler", "internal/joboperator", "internal/jobruntime", "internal/workersctl"} {
		entries, err := os.ReadDir(filepath.Join(root, dir))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(root, dir, entry.Name())
			source, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			file, err := parser.ParseFile(fset, path, source, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				body := string(source[fset.Position(fn.Pos()).Offset:fset.Position(fn.End()).Offset])
				where := dir + "/" + entry.Name() + "#" + fn.Name.Name
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok || !outputCall.MatchString(exprText(fset, source, call.Fun)) {
						return true
					}
					for _, arg := range call.Args {
						ast.Inspect(arg, func(inner ast.Node) bool {
							name := ""
							switch typed := inner.(type) {
							case *ast.Ident:
								name = typed.Name
							case *ast.SelectorExpr:
								name = typed.Sel.Name
							}
							if name == "" || !rowKeyName.MatchString(name) {
								return true
							}
							if strings.Contains(body, guardCall) {
								return true
							}
							if _, ok := decodedRowFieldOutputs[where]; ok {
								seen[where] = true
								return true
							}
							t.Errorf("%s formats %s into an error or log line without %s", where, name, guardCall)
							return true
						})
					}
					return true
				})
			}
		}
	}
	for where := range decodedRowFieldOutputs {
		if !seen[where] {
			t.Errorf("classified output %s no longer exists", where)
		}
	}
}

func exprText(fset *token.FileSet, source []byte, expr ast.Expr) string {
	return string(source[fset.Position(expr.Pos()).Offset:fset.Position(expr.End()).Offset])
}
