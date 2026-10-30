package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

// enableStore is what the enable verb uses of *workload.Store.
type enableStore interface {
	PlanEnable(ctx context.Context, name string, in workload.EnableInput) (workload.EnablePlan, error)
	EnableWorkload(ctx context.Context, name string, in workload.EnableInput) (workload.EnablePlan, error)
}

const enableMintsNothing = "no credential was minted: run bootstrap-create next"

// runEnable runs `workloads enable`. Without -apply it reads and reports the
// planned changes; it prints ids and states only.
func runEnable(ctx context.Context, store enableStore, parsed workloadsArgs, stdout, stderr io.Writer) int {
	in := workload.EnableInput{Operator: parsed.operator, EnablePrincipal: parsed.enablePrincipal}
	what := "enable"
	var (
		plan workload.EnablePlan
		err  error
	)
	if parsed.apply {
		plan, err = store.EnableWorkload(ctx, parsed.name, in)
	} else {
		what = "enable dry run"
		plan, err = store.PlanEnable(ctx, parsed.name, in)
	}
	if err != nil {
		class := failureClass(err)
		if errors.Is(err, workload.ErrBootstrapPrincipalDisabled) {
			class = "the workload's principal is disabled; add -enable-principal to enable it too"
		}
		fmt.Fprintf(stderr, "workloads error: %s failed (%s)\n", what, class)
		return 1
	}
	fmt.Fprintf(stdout, "workloads %s: workload=%s row=%s principal=%s operator=%s\n",
		what, parsed.name, state(plan.RowDisabled), state(plan.PrincipalDisabled), parsed.operator)
	verb := "enabled"
	if !parsed.apply {
		verb = "would enable"
	}
	if !plan.EnableRow && !plan.EnablePrincipal {
		fmt.Fprintln(stdout, "nothing to enable (no change written)")
		return 0
	}
	if plan.EnableRow {
		fmt.Fprintf(stdout, "%s: workload row\n", verb)
	}
	if plan.EnablePrincipal {
		fmt.Fprintf(stdout, "%s: principal %s (kind workload)\n", verb, plan.PrincipalID)
	}
	if parsed.apply {
		fmt.Fprintln(stdout, enableMintsNothing)
	} else {
		fmt.Fprintln(stdout, "(no change written)")
	}
	return 0
}

func state(disabled bool) string {
	if disabled {
		return "disabled"
	}
	return "active"
}
