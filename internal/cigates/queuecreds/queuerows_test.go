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
	{regexp.MustCompile(`\bencoded_args\b`), func([]string) string { return "encoded_args" }},
	{regexp.MustCompile(`\bdedupe_key\b`), func([]string) string { return "dedupe_key" }},
}

var (
	sqlQuoted  = regexp.MustCompile(`'(?:[^']|'')*'|--[^\n]*`)
	bareArgs   = regexp.MustCompile(`(?i)\b(?:[a-z_][a-z0-9_]*\.)?args\b`)
	argsFollow = regexp.MustCompile(`^\s*(?:->|#>)`)
)

// bareArgsRead reports whether the text names the args column as a whole word whatever operator follows it. The
// operators ->, ->>, #> and #>> name the field they read through their own expressions, so they do not count here.
func bareArgsRead(text string) bool {
	stripped := sqlQuoted.ReplaceAllString(text, " ")
	for _, loc := range bareArgs.FindAllStringIndex(stripped, -1) {
		if !argsFollow.MatchString(stripped[loc[1]:]) {
			return true
		}
	}
	return false
}

// sqlKeys names the queue-row fields one SQL text reads.
func sqlKeys(text string) []string {
	seen := map[string]bool{}
	for _, expression := range queueRowExpressions {
		for _, match := range expression.pattern.FindAllStringSubmatch(text, -1) {
			seen[expression.key(match)] = true
		}
	}
	if bareArgsRead(text) {
		seen["args"] = true
	}
	out := make([]string, 0, len(seen))
	for key := range seen {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// queueRowSQLKeys names the queue-row fields a string literal reads when the literal is a SQL statement.
func queueRowSQLKeys(text string) []string {
	upper := strings.ToUpper(text)
	if !(strings.Contains(upper, "SELECT") || strings.Contains(upper, "INSERT") || strings.Contains(upper, "UPDATE") ||
		strings.Contains(upper, "DELETE") || strings.Contains(upper, "RETURNING") || strings.Contains(upper, "MERGE")) {
		return nil
	}
	return sqlKeys(text)
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
					if err != nil {
						return true
					}
					for _, key := range queueRowSQLKeys(text) {
						where := rel + "#" + name
						if found[where] == nil {
							found[where] = map[string]bool{}
						}
						found[where][key] = true
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
		"args, args#>>domain,id, args#>>domain,type, args->>organization_id, dedupe_key",
		roleInternal, "", "", "join and filter on the domain link; the dedupe key stays in strandCandidate and is reported for the provider-unit shape only"},
	"internal/joboutbox/strand_repair.go#repairStrandedFinalizeSQL": {
		"args, args#>>domain,id, args#>>domain,type, args->>organization_id, dedupe_key",
		roleInternal, "", "", "same as the partition shape"},
	"internal/joboutbox/strand_repair.go#repairStrandedWorkGraphSQL": {
		"args, args#>>domain,id, args->>organization_id, dedupe_key",
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
	"internal/queryapi/reports/writeproofcases/cases.go#Teardown": {
		"dedupe_key", roleElsewhere, "", "", "write-proof test support: a DELETE that filters on the key, with no RETURNING and no output, run by tests only"},
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
	rowKeyName = regexp.MustCompile(`(?i)^(dedupekey|sourcededupekey|idempotencykey|correlationid|encodedargs|args)$`)
)

// Queue-row fields that a job's own code reads after the decoder has accepted
// them; the value is guarded because Decode refused anything credential-shaped.
type outputException struct {
	values []string
	reason string
}

var decodedRowFieldOutputs = map[string]outputException{
	"internal/jobruntime/adapter.go#logStart":                                {[]string{"envelope.CorrelationID"}, "envelope field after jobcontract.Decode"},
	"internal/jobruntime/adapter.go#NewAdapter":                              {[]string{"args.Kind()"}, "the typed argument type's registered kind name, not a queue row value"},
	"internal/jobruntime/adapter.go#execute":                                 {[]string{"envelope.CorrelationID"}, "envelope correlation id on the handler logger, after jobcontract.Decode"},
	"internal/jobruntime/adapter.go#logFinish":                               {[]string{"attributes"}, "the finish-line attributes hold the envelope correlation id, after jobcontract.Decode"},
	"internal/jobruntime/adapter.go#startJobSpan":                            {[]string{"envelope.CorrelationID"}, "envelope correlation id as a span attribute, after jobcontract.Decode"},
	"internal/workersctl/trigger_investment.go#writeManualInvestmentTrigger": {[]string{"generation"}, "the generation string is built by the same command from the canonical organization id, parsed dates and a hashed rerun tag; it is not read from a queue row"},
	"internal/workersctl/trigger_workgraph.go#writeManualWorkgraphTrigger":   {[]string{"generation"}, "the generation string is built by the same command from the canonical organization id and parsed dates; it is not read from a queue row"},
	"internal/workersctl/trigger_workgraph.go#rollbackTriggerTx":             {[]string{"args"}, "a local slice named args that holds slog attributes the caller built from request id, organization id and generation; not queue row arguments"},
}

// A queue row's dedupe key, idempotency key, correlation id or arguments must
// not be formatted into an error or a log line unless the enclosing function
// calls the shared value detector on it, or the value comes from a decoded envelope.
func TestNoQueueRowFieldIsFormattedIntoAnErrorOrLogUnguarded(t *testing.T) {
	root := repoRoot(t)
	findings, seen := formatCensus(t, root, censusDirs)
	for _, finding := range findings {
		t.Error(finding)
	}
	for where, exception := range decodedRowFieldOutputs {
		for _, value := range exception.values {
			if !seen[where+"|"+value] {
				t.Errorf("classified output %s no longer formats %s", where, value)
			}
		}
	}
}

func exprText(fset *token.FileSet, source []byte, expr ast.Expr) string {
	return string(source[fset.Position(expr.Pos()).Offset:fset.Position(expr.End()).Offset])
}
