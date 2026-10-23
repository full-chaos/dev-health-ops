package workersctl

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// Every `dho workers` verb is in verbGates, and the table is checked against
// the dispatch functions of this package, so a verb added without a row fails
// here. Verbs gated through the job operator service are run with the real
// OperatorAuthorizer under a principal other than the operator; direct-write
// verbs are run the same way through their own table.

type verbKind int

const (
	gateDelegate verbKind = iota + 1
	gateArgument
	gateService
	gateDirect
	gateRead
)

type verbGate struct {
	kind   verbKind
	argv   []string
	direct []string
}

const unauthorizedJSON = "{\"error\":{\"code\":\"unauthorized\"}}\n"

func viaDelegate() verbGate { return verbGate{kind: gateDelegate} }
func viaService(argv ...string) verbGate {
	return verbGate{kind: gateService, argv: argv}
}
func viaDirect(keys ...string) verbGate { return verbGate{kind: gateDirect, direct: keys} }
func viaRead(argv ...string) verbGate   { return verbGate{kind: gateRead, argv: argv} }

var verbGates = map[string]verbGate{
	"dispatch:status":               viaService("status"),
	"dispatch:streams":              viaService("streams", "status"),
	"dispatch:contracts":            viaService("contracts", "--queue", "heartbeat"),
	"dispatch:external-recompute":   viaDelegate(),
	"dispatch:investment":           viaDelegate(),
	"dispatch:job-routes":           viaDelegate(),
	"dispatch:jobs":                 viaDelegate(),
	"dispatch:metrics":              viaDelegate(),
	"dispatch:providersync":         viaDelegate(),
	"dispatch:queues":               viaDelegate(),
	"dispatch:routes":               viaDelegate(),
	"dispatch:sync-dispatch-outbox": viaDelegate(),
	"dispatch:workgraph":            viaDelegate(),

	"dispatchExternalRecompute:replay": viaDirect("external-recompute replay"),
	"dispatchInvestment:trigger":       viaDirect("investment trigger"),

	"dispatchJobRoutes:status":   viaService("job-routes", "status", "post_sync"),
	"dispatchJobRoutes:apply":    viaService("job-routes", "apply", "--reason", "operator_test", "--correlation-id", "corr-1", "post_sync"),
	"dispatchJobRoutes:rollback": viaService("job-routes", "rollback", "--reason", "operator_test", "--correlation-id", "corr-1", "post_sync"),

	"dispatchJobs:list":    viaService("jobs", "list"),
	"dispatchJobs:inspect": viaService("jobs", "inspect", "42"),
	"dispatchJobs:cancel":  viaService("jobs", "cancel", "--reason", "operator_test", "--correlation-id", "corr-1", "42"),
	"dispatchJobs:retry":   viaService("jobs", "retry", "--reason", "operator_test", "--correlation-id", "corr-1", "42"),

	"dispatchMetrics:remaining":                               viaDelegate(),
	"dispatchMetrics:daily-start":                             viaDirect("metrics daily-start"),
	"dispatchMetrics:daily-redrive":                           viaDirect("metrics daily-redrive"),
	"dispatchMetrics:daily-finalize":                          viaDirect("metrics daily-finalize"),
	"dispatchMetrics:finalize-redrive":                        viaDirect("metrics finalize-redrive"),
	"dispatchMetrics:generation-repair":                       viaDirect("metrics generation-repair"),
	"dispatchMetrics:partition-recompute":                     viaDirect("metrics partition-recompute"),
	"dispatchMetrics:execution-repair":                        viaDirect("metrics exec-repair"),
	"dispatchMetrics:daily-blocked":                           viaRead("metrics", "daily-blocked", "--org", auditTestOrg),
	"dispatchMetrics:daily-marker-backfill":                   viaDirect("metrics daily-marker-backfill"),
	"dispatchMetrics:list-ambiguous-executions":               viaRead("metrics", "list-ambiguous-executions"),
	"dispatchMetricsRemaining:start":                          viaDirect("metrics remaining start", "metrics remaining start --recompute"),
	"dispatchMetricsRemaining:trigger-backstop":               viaDirect("metrics remaining trigger-backstop"),
	"dispatchMetricsRemaining:redrive":                        viaDirect("metrics remaining redrive"),
	"dispatchMetricsRemainingTriggerBackstop:capacity":        {kind: gateArgument},
	"dispatchMetricsRemainingTriggerBackstop:recommendations": {kind: gateArgument},

	"dispatchProvidersync:carry-team-ids":                        viaDirect("providersync carry-team-ids"),
	"dispatchProvidersync:compact-ownership-intervals":           viaDirect("providersync compact-ownership-intervals"),
	"dispatchProvidersync:drain-removed-dependencies":            viaDirect("providersync drain-removed-dependencies"),
	"dispatchProvidersync:retire-jira-key-projects":              viaDirect("providersync retire-jira-key-projects"),
	"dispatchProvidersync:retire-jira-project-as-team":           viaDirect("providersync retire-jira-project-as-team"),
	"dispatchProvidersync:retire-linear-pseudo-projects":         viaDirect("providersync retire-linear-pseudo-projects"),
	"dispatchProvidersync:retire-stale-linear-project-ownership": viaDirect("providersync retire-stale-linear-project-ownership"),

	"dispatchQueues:status":  viaService("queues", "status"),
	"dispatchQueues:pause":   viaService("queues", "pause", "--reason", "operator_test", "--correlation-id", "corr-1", "heartbeat"),
	"dispatchQueues:resume":  viaService("queues", "resume", "--reason", "operator_test", "--correlation-id", "corr-1", "heartbeat"),
	"dispatchQueues:drain":   viaService("queues", "drain", "--group", "ops", "--queue", "heartbeat", "--reason", "operator_test", "--correlation-id", "corr-1"),
	"dispatchQueues:undrain": viaService("queues", "undrain", "--group", "ops", "--queue", "heartbeat", "--reason", "operator_test", "--correlation-id", "corr-1"),

	"dispatchRoutes:status": viaService("routes", "status", "post_sync"),
	"dispatchRoutes:apply":  viaService("routes", "apply", "--reason", "operator_test", "--correlation-id", "corr-1", "post_sync"),
	"dispatchRoutes:pause":  viaService("routes", "pause", "--reason", "operator_test", "--correlation-id", "corr-1", "post_sync"),
	"dispatchRoutes:drain":  viaService("routes", "drain", "--reason", "operator_test", "--correlation-id", "corr-1", "post_sync"),
	"dispatchRoutes:resume": viaService("routes", "resume", "--reason", "operator_test", "--correlation-id", "corr-1", "post_sync"),

	"dispatchSyncDispatchOutbox:close-backlog": viaDirect("sync-dispatch-outbox close-backlog"),

	"dispatchWorkgraph:list-ambiguous":   viaRead("workgraph", "list-ambiguous"),
	"dispatchWorkgraph:list-undelivered": viaRead("workgraph", "list-undelivered"),
	"dispatchWorkgraph:repair":           viaDirect("workgraph repair"),
	"dispatchWorkgraph:trigger":          viaDirect("workgraph trigger"),
}

// dispatchedVerbs reads this package's non-test sources and returns every
// "<dispatch function>:<verb>" pair: a string case of a function named
// dispatch*, and a string compared with args[0] there.
func dispatchedVerbs(t *testing.T) []string {
	t.Helper()
	_, self, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test file")
	}
	dir := filepath.Dir(self)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, isFunc := decl.(*ast.FuncDecl)
			if !isFunc || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "dispatch") {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.CaseClause:
					for _, expr := range typed.List {
						if literal, isLit := expr.(*ast.BasicLit); isLit && literal.Kind == token.STRING {
							found[fn.Name.Name+":"+strings.Trim(literal.Value, "\"")] = true
						}
					}
				case *ast.BinaryExpr:
					index, isIndex := typed.X.(*ast.IndexExpr)
					literal, isLit := typed.Y.(*ast.BasicLit)
					if !isIndex || !isLit || literal.Kind != token.STRING || (typed.Op != token.EQL && typed.Op != token.NEQ) {
						return true
					}
					if base, isIdent := index.X.(*ast.Ident); isIdent && base.Name == "args" {
						found[fn.Name.Name+":"+strings.Trim(literal.Value, "\"")] = true
					}
				}
				return true
			})
		}
	}
	verbs := make([]string, 0, len(found))
	for verb := range found {
		verbs = append(verbs, verb)
	}
	sort.Strings(verbs)
	return verbs
}

// A measurement that found nothing fails: the walk must see the root
// dispatch function and a plausible number of verbs before the comparison
// means anything.
func TestEveryDispatchedVerbHasAnAuthorityRow(t *testing.T) {
	verbs := dispatchedVerbs(t)
	if len(verbs) < 40 || !slices.Contains(verbs, "dispatch:jobs") {
		t.Fatalf("the dispatch walk found %d verbs (%v); it read nothing useful", len(verbs), verbs)
	}
	for _, verb := range verbs {
		if _, listed := verbGates[verb]; !listed {
			t.Errorf("verb %s has no row in verbGates", verb)
		}
	}
	for verb := range verbGates {
		if !slices.Contains(verbs, verb) {
			t.Errorf("verbGates row %s names no dispatched verb", verb)
		}
	}
}

// The direct-write rows name exactly the verbs of directWriteVerbs, each once.
func TestDirectWriteRowsCoverEveryDirectWriteVerb(t *testing.T) {
	named := map[string]int{}
	for verb, gate := range verbGates {
		if gate.kind != gateDirect {
			continue
		}
		if len(gate.direct) == 0 {
			t.Errorf("%s names no direct-write verb", verb)
		}
		for _, key := range gate.direct {
			named[key]++
		}
	}
	table := directWriteVerbs()
	for key := range table {
		if named[key] != 1 {
			t.Errorf("direct-write verb %q is named %d times in verbGates, want 1", key, named[key])
		}
	}
	for key := range named {
		if _, listed := table[key]; !listed {
			t.Errorf("verbGates names direct-write verb %q that directWriteVerbs does not hold", key)
		}
	}
}

// foreignPrincipals are valid principals that are not the operator.
func foreignPrincipals() []joboperator.Principal {
	return []joboperator.Principal{
		{Type: "service_credential", ID: "00000000-0000-4000-8000-000000000001"},
		{Type: "user", ID: "dho-workers"},
		{Type: joboperator.OperatorPrincipalType, ID: "someone-else"},
	}
}

func inventoryRuntime(t *testing.T, principal joboperator.Principal) (*operatorRuntime, *commandBackend) {
	t.Helper()
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	backend := &commandBackend{queues: map[string]joboperator.QueueSummary{"heartbeat": {Group: "ops"}}}
	service, err := joboperator.New(joboperator.Dependencies{
		Registry: registry, Backend: backend, Authorizer: joboperator.OperatorAuthorizer(),
		DomainGuard: commandDomainGuard{}, Auditor: commandAuditor{},
		RouteController:    commandRouteController{},
		JobRouteController: commandJobRouteController{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &operatorRuntime{
		service:          service,
		queueControlMode: "direct",
		principal:        principal,
		queueStatusSource: workerQueueStatusSourceFunc(func(context.Context) (workerQueueStatusResponse, error) {
			return workerQueueStatusResponse{}, nil
		}),
	}, backend
}

// Through the production authorizer, a principal other than the operator is
// refused by every verb that goes through the job operator service, reads
// included, and the answer carries nothing on stdout.
func TestServiceVerbsRefuseEveryPrincipalButTheOperator(t *testing.T) {
	ran := 0
	for verb, gate := range verbGates {
		if gate.kind != gateService {
			continue
		}
		for _, principal := range foreignPrincipals() {
			runtime, backend := inventoryRuntime(t, principal)
			var stdout, stderr bytes.Buffer
			code := dispatch(context.Background(), runtime, gate.argv, &stdout, &stderr)
			if code != 1 || stderr.String() != unauthorizedJSON || stdout.Len() != 0 {
				t.Errorf("%s as %+v: code=%d stdout=%q stderr=%q, want unauthorized", verb, principal, code, stdout.String(), stderr.String())
			}
			if len(backend.drainRequests)+len(backend.undrainRequests)+len(backend.queuesRequests) != 0 {
				t.Errorf("%s as %+v reached the queue backend", verb, principal)
			}
			ran++
		}
		runtime, _ := inventoryRuntime(t, joboperator.OperatorPrincipal)
		var stdout, stderr bytes.Buffer
		dispatch(context.Background(), runtime, gate.argv, &stdout, &stderr)
		if stderr.String() == unauthorizedJSON {
			t.Errorf("%s as the operator was refused; the refusal above does not come from the principal", verb)
		}
	}
	if ran < 20*len(foreignPrincipals()) {
		t.Fatalf("only %d refusals were exercised; the inventory shrank", ran)
	}
}

// The same refusal for every direct-write verb, with the production
// authorizer, and with no audit intent and nothing on stdout.
func TestDirectWriteVerbsRefuseEveryPrincipalButTheOperator(t *testing.T) {
	for name, verb := range directWriteVerbs() {
		modes := map[string][]string{"write": auditFlags()}
		if verb.dryRun {
			modes["dry-run"] = []string{"--dry-run"}
		}
		for mode, flags := range modes {
			for _, principal := range foreignPrincipals() {
				auditor := &refusingAuditor{}
				runtime := verb.runtimeFor(commandRuntimeWithAuditor(t, joboperator.OperatorAuthorizer(), auditor))
				runtime.principal = principal
				var stdout, stderr bytes.Buffer
				code := dispatch(context.Background(), runtime, append(slices.Clone(verb.argv), flags...), &stdout, &stderr)
				if code != 1 || stderr.String() != unauthorizedJSON || stdout.Len() != 0 || len(auditor.events) != 0 {
					t.Errorf("%s (%s) as %+v: code=%d stdout=%q stderr=%q intents=%d, want unauthorized and no intent",
						name, mode, principal, code, stdout.String(), stderr.String(), len(auditor.events))
				}
			}
		}
	}
}

// Through the production authorizer, a principal other than the operator is
// refused by every read-only verb that does not go through the job operator
// service, before the verb looks at its backend: with no pools the answer is
// still unauthorized, not operator_backend_unavailable. The operator is not
// refused by the authorizer.
func TestReadVerbsRefuseEveryPrincipalButTheOperator(t *testing.T) {
	ran := 0
	for verb, gate := range verbGates {
		if gate.kind != gateRead {
			continue
		}
		for _, principal := range foreignPrincipals() {
			runtime, _ := inventoryRuntime(t, principal)
			var stdout, stderr bytes.Buffer
			code := dispatch(context.Background(), runtime, gate.argv, &stdout, &stderr)
			if code != 1 || stderr.String() != unauthorizedJSON || stdout.Len() != 0 {
				t.Errorf("%s as %+v: code=%d stdout=%q stderr=%q, want unauthorized", verb, principal, code, stdout.String(), stderr.String())
			}
			ran++
		}
		runtime, _ := inventoryRuntime(t, joboperator.OperatorPrincipal)
		var stdout, stderr bytes.Buffer
		dispatch(context.Background(), runtime, gate.argv, &stdout, &stderr)
		if stderr.String() == unauthorizedJSON {
			t.Errorf("%s as the operator was refused; the refusal above does not come from the principal", verb)
		}
	}
	if ran != 4*len(foreignPrincipals()) {
		t.Fatalf("%d refusals were exercised, want %d: the read-verb inventory changed", ran, 4*len(foreignPrincipals()))
	}
}

type rolePairing struct {
	label, role, check, checkPool, checkRole, diagnosePool, diagnosePosture string
}

var startupRolePairings = []rolePairing{
	{"domain", "domainRole", "CheckDomainAuthorization", "pools.Domain", "domainRole", "pools.Domain", "postgresstore.DomainPosture()"},
	{"queue", "queueRole", "CheckQueueAuthorization", "pools.QueueControl", "queueRole", "pools.QueueControl", "postgresstore.QueuePosture()"},
	{"coordinator", "coordinatorRole", "CheckCoordinatorAuthorization", "coordinatorPool", "coordinatorRole", "coordinatorPool", "postgresstore.CoordinatorPosture()"},
}

// This reads the source of configureRuntime; it is a pin on the wiring, not
// a run of it. Each startup posture check must pair its own pool, role and
// posture, the three must run in this order, and the service must be built
// with the operator authorizer and the operator principal.
func TestStartupPairsEachRolePostureWithItsOwnPool(t *testing.T) {
	_, self, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test file")
	}
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(filepath.Dir(self), "main.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var body *ast.BlockStmt
	for _, decl := range file.Decls {
		if fn, isFunc := decl.(*ast.FuncDecl); isFunc && fn.Name.Name == "configureRuntime" {
			body = fn.Body
		}
	}
	if body == nil {
		t.Fatal("configureRuntime was not found")
	}
	text := func(node ast.Node) string {
		if expr, isExpr := node.(ast.Expr); isExpr && expr != nil {
			return types.ExprString(expr)
		}
		return ""
	}

	var pairings []rolePairing
	var authorizer, principal string
	ast.Inspect(body, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.CompositeLit:
			if array, isArray := typed.Type.(*ast.ArrayType); isArray && array.Elt != nil && text(array.Elt) == "runtimeRolePostureCheck" {
				for _, element := range typed.Elts {
					literal, isLit := element.(*ast.CompositeLit)
					if !isLit {
						continue
					}
					var pairing rolePairing
					for _, field := range literal.Elts {
						pair, isPair := field.(*ast.KeyValueExpr)
						if !isPair {
							continue
						}
						switch text(pair.Key) {
						case "role":
							pairing.role = text(pair.Value)
						case "label":
							pairing.label = strings.Trim(text(pair.Value), "\"")
						case "check", "diagnose":
							fn, isFn := pair.Value.(*ast.FuncLit)
							if !isFn {
								continue
							}
							ast.Inspect(fn.Body, func(inner ast.Node) bool {
								call, isCall := inner.(*ast.CallExpr)
								if !isCall {
									return true
								}
								switch name := text(call.Fun); {
								case strings.HasPrefix(name, "postgresstore.Check") && len(call.Args) == 4:
									pairing.check = strings.TrimPrefix(name, "postgresstore.")
									pairing.checkPool, pairing.checkRole = text(call.Args[1]), text(call.Args[2])
								case name == "postgresstore.DiagnoseRolePosture" && len(call.Args) == 4:
									pairing.diagnosePool, pairing.diagnosePosture = text(call.Args[1]), text(call.Args[3])
								}
								return true
							})
						}
					}
					pairings = append(pairings, pairing)
				}
			}
			if typed.Type != nil && text(typed.Type) == "joboperator.Dependencies" {
				for _, field := range typed.Elts {
					if pair, isPair := field.(*ast.KeyValueExpr); isPair && text(pair.Key) == "Authorizer" {
						authorizer = text(pair.Value)
					}
				}
			}
		case *ast.UnaryExpr:
			if literal, isLit := typed.X.(*ast.CompositeLit); isLit && typed.Op == token.AND && text(literal.Type) == "operatorRuntime" {
				for _, field := range literal.Elts {
					if pair, isPair := field.(*ast.KeyValueExpr); isPair && text(pair.Key) == "principal" {
						principal = text(pair.Value)
					}
				}
			}
		}
		return true
	})
	if !slices.Equal(pairings, startupRolePairings) {
		t.Errorf("startup role checks = %+v, want %+v", pairings, startupRolePairings)
	}
	if authorizer != "joboperator.OperatorAuthorizer()" {
		t.Errorf("service authorizer = %q, want joboperator.OperatorAuthorizer()", authorizer)
	}
	if principal != "joboperator.OperatorPrincipal" {
		t.Errorf("runtime principal = %q, want joboperator.OperatorPrincipal", principal)
	}
}
