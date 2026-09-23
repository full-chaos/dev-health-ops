// Package contractcheck is the `contracts` vertical of the dho binary:
// worker job-contract validation, folded from the standalone
// worker-contractcheck binary (spec S3, CHAOS-6302). The logic is
// unchanged from cmd/worker-contractcheck/main.go -- it already took its
// args, stdout and stderr as parameters and never called os.Exit or read
// os.Args directly, so this fold is a package move plus one cli.Command
// wrapper per verb; no parser change.
package contractcheck

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/deploymentcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

const defaultContractRoot = "contracts/jobs/v1"
const defaultDeploymentManifest = "deploy/go-workers/deployment.json"

// Command is the `contracts` vertical of the dho binary.
func Command() cli.Command {
	return cli.Command{
		Name:    "contracts",
		Kind:    cli.Group,
		Summary: "worker job-contract validation: schema, capability, rollout, compare",
		Children: []cli.Command{
			{
				Name: "validate", Kind: cli.Verb,
				Summary: "validate the job-contract tree and the Go worker deployment manifest against it",
				Run: func(_ context.Context, env cli.Env) int {
					return runValidate(env.Args, env.Stdout, env.Stderr)
				},
			},
			{
				Name: "capabilities", Kind: cli.Verb,
				Summary: "emit a capability report for a live binary's registered queues",
				Run: func(_ context.Context, env cli.Env) int {
					return runCapabilities(env.Args, env.Stdout, env.Stderr)
				},
			},
			{
				Name: "rollout", Kind: cli.Verb,
				Summary: "check that every live capability report supports its queues' producer versions",
				Run: func(_ context.Context, env cli.Env) int {
					return runRollout(env.Args, env.Stdout, env.Stderr)
				},
			},
			{
				Name: "compare", Kind: cli.Verb,
				Summary: "diff two contract trees for breaking in-place changes",
				Run: func(_ context.Context, env cli.Env) int {
					return runCompare(env.Args, env.Stdout, env.Stderr)
				},
			},
		},
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
