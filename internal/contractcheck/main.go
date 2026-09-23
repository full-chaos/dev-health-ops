// Package contractcheck is the `contracts` vertical of the dho binary:
// worker job-contract validation, folded from the standalone
// worker-contractcheck binary. The validation logic is unchanged; each
// verb's Run now goes through the binary-wide exit-code contract (a bad
// flag or a malformed positional argument exits 2, -h/--help exits 0, any
// other failure exits 1) via cli.WrapFlagParseError and cli.ExitForVerbError,
// the same mapping every other vertical in this binary uses.
package contractcheck

import (
	"context"
	"encoding/json"
	"errors"
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
			verbCommand("validate", "validate the job-contract tree and the Go worker deployment manifest against it", runValidate),
			verbCommand("capabilities", "emit a capability report for a live binary's registered queues", runCapabilities),
			verbCommand("rollout", "check that every live capability report supports its queues' producer versions", runRollout),
			verbCommand("compare", "diff two contract trees for breaking in-place changes", runCompare),
		},
	}
}

// verbCommand wraps one of this package's run* functions in the standard
// cli.Command shape: the verb's own error, if any and not a help request,
// is printed once here (never by the run* function itself), and mapped to
// this binary's exit-code contract by cli.ExitForVerbError.
func verbCommand(name, summary string, run func(args []string, stdout, flagErrOutput io.Writer) error) cli.Command {
	return cli.Command{
		Name: name, Kind: cli.Verb, Summary: summary,
		Run: func(_ context.Context, env cli.Env) int {
			err := run(env.Args, env.Stdout, env.Stderr)
			if err != nil && !errors.Is(err, flag.ErrHelp) {
				fmt.Fprintf(env.Stderr, "contracts %s: %v\n", name, err)
			}
			return cli.ExitForVerbError(err)
		},
	}
}

// usageError marks a hand-rolled usage problem (a missing required flag, a
// disallowed positional argument) with the SAME exit-2 semantics
// cli.WrapFlagParseError gives a real (*flag.FlagSet).Parse failure -- both
// are "nothing ran because the invocation was wrong", the class
// cli.ExitForVerbError maps to ExitUsage.
func usageError(err error) error {
	return &cli.FlagUsageError{Err: err}
}

func runValidate(args []string, stdout, flagErrOutput io.Writer) error {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	flags.SetOutput(flagErrOutput)
	root := flags.String("root", defaultContractRoot, "contract v1 directory")
	deployment := flags.String("deployment", defaultDeploymentManifest, "Go worker deployment manifest")
	if err := flags.Parse(args); err != nil {
		return cli.WrapFlagParseError(err)
	}
	if flags.NArg() != 0 {
		return usageError(errors.New("validate accepts no positional arguments"))
	}
	if err := jobcontract.ValidateTree(*root); err != nil {
		return fmt.Errorf("contract validation failed: %w", err)
	}
	registry, err := jobcontract.LoadRegistry(*root)
	if err != nil {
		return fmt.Errorf("load registry: %w", err)
	}
	_, budget, err := deploymentcontract.Load(*deployment, registry)
	if err != nil {
		return fmt.Errorf("deployment validation failed: %w", err)
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
	return nil
}

func runCapabilities(args []string, stdout, flagErrOutput io.Writer) error {
	flags := flag.NewFlagSet("capabilities", flag.ContinueOnError)
	flags.SetOutput(flagErrOutput)
	root := flags.String("root", defaultContractRoot, "contract v1 directory")
	var queues queueList
	flags.Var(&queues, "queues", "registered queues to consume (comma-separated or repeatable)")
	if err := flags.Parse(args); err != nil {
		return cli.WrapFlagParseError(err)
	}
	if len(queues) == 0 || flags.NArg() != 0 {
		return usageError(errors.New("capabilities requires --queues and no positional arguments"))
	}
	registry, err := jobcontract.LoadRegistry(*root)
	if err != nil {
		return fmt.Errorf("load registry: %w", err)
	}
	report, err := jobcontract.CapabilitiesForQueues(*root, registry, queues)
	if err != nil {
		return fmt.Errorf("build capability report: %w", err)
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return fmt.Errorf("encode capability report: %w", err)
	}
	return nil
}

func runRollout(args []string, stdout, flagErrOutput io.Writer) error {
	flags := flag.NewFlagSet("rollout", flag.ContinueOnError)
	flags.SetOutput(flagErrOutput)
	root := flags.String("root", defaultContractRoot, "contract v1 directory")
	var queues queueList
	flags.Var(&queues, "queues", "registered queues to check (comma-separated or repeatable)")
	var reportPaths stringList
	flags.Var(&reportPaths, "report", "capability report path (repeat for every live binary)")
	if err := flags.Parse(args); err != nil {
		return cli.WrapFlagParseError(err)
	}
	if len(queues) == 0 || len(reportPaths) == 0 || flags.NArg() != 0 {
		return usageError(errors.New("rollout requires --queues, at least one --report, and no positional arguments"))
	}
	registry, err := jobcontract.LoadRegistry(*root)
	if err != nil {
		return fmt.Errorf("load registry: %w", err)
	}
	state, err := jobcontract.LoadMigrationState(*root, registry)
	if err != nil {
		return fmt.Errorf("load migration state: %w", err)
	}
	reports := make([]jobcontract.CapabilityReport, 0, len(reportPaths))
	for _, path := range reportPaths {
		report, err := jobcontract.LoadCapabilityReport(path)
		if err != nil {
			return fmt.Errorf("load capability report: %w", err)
		}
		reports = append(reports, report)
	}
	if _, err := jobcontract.CapabilitiesForQueues(*root, registry, queues); err != nil {
		return fmt.Errorf("validate rollout queues: %w", err)
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
			return fmt.Errorf("rollout queue %q has no capability report", queue)
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
		return err
	}
	fmt.Fprintln(stdout, "all live capability reports support producer versions")
	return nil
}

func runCompare(args []string, stdout, flagErrOutput io.Writer) error {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(flagErrOutput)
	base := flags.String("base", "", "merge-base contract v1 directory")
	candidate := flags.String("candidate", defaultContractRoot, "candidate contract v1 directory")
	if err := flags.Parse(args); err != nil {
		return cli.WrapFlagParseError(err)
	}
	if *base == "" || flags.NArg() != 0 {
		return usageError(errors.New("compare requires --base and no positional arguments"))
	}
	changes, err := jobcontract.CompareTrees(*base, *candidate)
	if err != nil {
		return fmt.Errorf("compare contracts: %w", err)
	}
	if len(changes) > 0 {
		return fmt.Errorf("breaking contract changes detected:\n%s", jobcontract.FormatBreakingChanges(changes))
	}
	fmt.Fprintln(stdout, "no breaking in-place contract changes")
	return nil
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
