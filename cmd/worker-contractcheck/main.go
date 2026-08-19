package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/deploymentcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

const defaultContractRoot = "contracts/jobs/v1"
const defaultDeploymentManifest = "deploy/go-workers/deployment.json"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}
	switch args[0] {
	case "validate":
		return runValidate(args[1:], stdout, stderr)
	case "capabilities":
		return runCapabilities(args[1:], stdout, stderr)
	case "rollout":
		return runRollout(args[1:], stdout, stderr)
	case "compare":
		return runCompare(args[1:], stdout, stderr)
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func runValidate(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("root", defaultContractRoot, "contract v1 directory")
	deployment := flags.String("deployment", defaultDeploymentManifest, "Go worker deployment manifest")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "validate accepts no positional arguments")
		return 2
	}
	if err := jobcontract.ValidateTree(*root); err != nil {
		fmt.Fprintln(stderr, "contract validation failed:", err)
		return 1
	}
	registry, err := jobcontract.LoadRegistry(*root)
	if err != nil {
		fmt.Fprintln(stderr, "load registry:", err)
		return 1
	}
	_, budget, err := deploymentcontract.Load(*deployment, registry)
	if err != nil {
		fmt.Fprintln(stderr, "deployment validation failed:", err)
		return 1
	}
	fmt.Fprintln(stdout, "worker contracts valid")
	fmt.Fprintf(
		stdout,
		"deployment manifest valid: queue_session_clients=%d queue_session_headroom=%d coordinator_session_clients=%d coordinator_session_headroom=%d domain_transaction_clients=%d domain_transaction_headroom=%d server_footprint=%d server_headroom=%d\n",
		budget.QueueSessionClientConnections,
		budget.QueueSessionHeadroom,
		budget.CoordinatorSessionClientConnections,
		budget.CoordinatorSessionHeadroom,
		budget.DomainTransactionClientConnections,
		budget.DomainTransactionHeadroom,
		budget.ServerConnectionFootprint,
		budget.ServerConnectionHeadroom,
	)
	return 0
}

func runCapabilities(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("capabilities", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("root", defaultContractRoot, "contract v1 directory")
	var queues queueList
	flags.Var(&queues, "queues", "registered queues to consume (comma-separated or repeatable)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if len(queues) == 0 || flags.NArg() != 0 {
		fmt.Fprintln(stderr, "capabilities requires --queues and no positional arguments")
		return 2
	}
	registry, err := jobcontract.LoadRegistry(*root)
	if err != nil {
		fmt.Fprintln(stderr, "load registry:", err)
		return 1
	}
	report, err := jobcontract.CapabilitiesForQueues(*root, registry, queues)
	if err != nil {
		fmt.Fprintln(stderr, "build capability report:", err)
		return 1
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintln(stderr, "encode capability report:", err)
		return 1
	}
	return 0
}

func runRollout(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("rollout", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("root", defaultContractRoot, "contract v1 directory")
	var queues queueList
	flags.Var(&queues, "queues", "registered queues to check (comma-separated or repeatable)")
	var reportPaths stringList
	flags.Var(&reportPaths, "report", "capability report path (repeat for every live binary)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if len(queues) == 0 || len(reportPaths) == 0 || flags.NArg() != 0 {
		fmt.Fprintln(stderr, "rollout requires --queues, at least one --report, and no positional arguments")
		return 2
	}
	registry, err := jobcontract.LoadRegistry(*root)
	if err != nil {
		fmt.Fprintln(stderr, "load registry:", err)
		return 1
	}
	state, err := jobcontract.LoadMigrationState(*root, registry)
	if err != nil {
		fmt.Fprintln(stderr, "load migration state:", err)
		return 1
	}
	reports := make([]jobcontract.CapabilityReport, 0, len(reportPaths))
	for _, path := range reportPaths {
		report, err := jobcontract.LoadCapabilityReport(path)
		if err != nil {
			fmt.Fprintln(stderr, "load capability report:", err)
			return 1
		}
		reports = append(reports, report)
	}
	if _, err := jobcontract.CapabilitiesForQueues(*root, registry, queues); err != nil {
		fmt.Fprintln(stderr, "validate rollout queues:", err)
		return 1
	}
	selected := make(map[string]struct{}, len(queues))
	for _, queue := range queues {
		selected[queue] = struct{}{}
	}
	covered := make(map[string]struct{}, len(queues))
	for _, report := range reports {
		for _, queue := range report.Queues {
			if _, ok := selected[queue]; ok {
				covered[queue] = struct{}{}
			}
		}
	}
	for _, queue := range queues {
		if _, ok := covered[queue]; !ok {
			fmt.Fprintf(stderr, "rollout queue %q has no capability report\n", queue)
			return 1
		}
	}

	scopedRegistry := registry
	scopedRegistry.Jobs = make([]jobcontract.JobDefinition, 0, len(registry.Jobs))
	for _, definition := range registry.Jobs {
		if _, ok := selected[definition.Queue]; ok {
			scopedRegistry.Jobs = append(scopedRegistry.Jobs, definition)
		}
	}
	scopedState := state
	scopedState.Jobs = make([]jobcontract.MigrationJob, 0, len(state.Jobs))
	for _, migration := range state.Jobs {
		for _, definition := range scopedRegistry.Jobs {
			if definition.Kind == migration.Kind {
				scopedState.Jobs = append(scopedState.Jobs, migration)
				break
			}
		}
	}
	if err := jobcontract.CheckRollout(*root, scopedRegistry, scopedState, reports); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintln(stdout, "all live capability reports support producer versions")
	return 0
}

func runCompare(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	base := flags.String("base", "", "merge-base contract v1 directory")
	candidate := flags.String("candidate", defaultContractRoot, "candidate contract v1 directory")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *base == "" || flags.NArg() != 0 {
		fmt.Fprintln(stderr, "compare requires --base and no positional arguments")
		return 2
	}
	changes, err := jobcontract.CompareTrees(*base, *candidate)
	if err != nil {
		fmt.Fprintln(stderr, "compare contracts:", err)
		return 1
	}
	if len(changes) > 0 {
		fmt.Fprintln(stderr, "breaking contract changes detected:")
		fmt.Fprintln(stderr, jobcontract.FormatBreakingChanges(changes))
		return 1
	}
	fmt.Fprintln(stdout, "no breaking in-place contract changes")
	return 0
}

func printUsage(writer io.Writer) {
	fmt.Fprintln(writer, "usage: worker-contractcheck <validate|capabilities|rollout|compare> [flags]")
}

type stringList []string

func (values *stringList) String() string { return strings.Join(*values, ",") }
func (values *stringList) Set(value string) error {
	if value == "" {
		return fmt.Errorf("report path cannot be empty")
	}
	*values = append(*values, value)
	return nil
}

type queueList []string

func (values *queueList) String() string { return strings.Join(*values, ",") }
func (values *queueList) Set(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("queue selection cannot be empty")
	}
	seen := make(map[string]struct{}, len(*values))
	for _, queue := range *values {
		seen[queue] = struct{}{}
	}
	parsed := make([]string, 0, strings.Count(value, ",")+1)
	for _, item := range strings.Split(value, ",") {
		queue := strings.TrimSpace(item)
		if queue == "" {
			return fmt.Errorf("queue selection contains an empty queue")
		}
		if _, duplicate := seen[queue]; duplicate {
			return fmt.Errorf("queue %q is selected more than once", queue)
		}
		seen[queue] = struct{}{}
		parsed = append(parsed, queue)
	}
	*values = append(*values, parsed...)
	sort.Strings(*values)
	return nil
}
