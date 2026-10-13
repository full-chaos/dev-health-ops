package adminops

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
)

// `admin llm-budget stale|void-reservation` (CHAOS-9451, CHAOS-3240). A
// reservation that stays `reserved` counts at its maximum exposure: the
// provider may still have billed. Nothing frees it by age. `stale` lists such
// rows; `void-reservation` frees ONE by id, and only an operator who can prove
// the provider did not bill should run it. The reason is mandatory and is
// printed with the row, as the audit evidence.

const defaultStaleAge = time.Hour

func llmBudgetGroup() cli.Command {
	return cli.Command{
		Name: "llm-budget", Summary: "organization BYO LLM budget reservations", Kind: cli.Group,
		Children: []cli.Command{
			{Name: "stale", Summary: "list reservations still `reserved` after an age (read only)", Kind: cli.Verb, Run: runLLMBudgetStale},
			{Name: "void-reservation", Summary: "void one reservation the provider provably did not bill", Kind: cli.Verb, Run: runLLMBudgetVoid},
		},
	}
}

func runLLMBudgetStale(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin llm-budget stale")
	age := flags.Duration("older-than", defaultStaleAge, "list reservations created longer ago than this")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if *age <= 0 {
		fmt.Fprintln(env.Stderr, "argument error: --older-than must be positive")
		return cli.ExitUsage
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	rows, err := llmbudget.ListStale(ctx, op.Pool, *age, op.Now())
	if err != nil {
		return writeError(env.Stderr, "admin_failed", "could not list the stale reservations")
	}
	out := make([]map[string]any, 0, len(rows))
	var total int64
	for _, row := range rows {
		total += row.ReservedMicroUSD
		out = append(out, map[string]any{
			"id": row.ID, "org_id": row.OrgID, "window_start": row.WindowStart.UTC().Format(time.RFC3339),
			"provider": row.Provider, "model": row.Model, "reserved_micro_usd": row.ReservedMicroUSD,
			"created_at": row.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	return printJSON(env, map[string]any{"count": len(rows), "reserved_micro_usd_total": total, "reservations": out})
}

func runLLMBudgetVoid(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin llm-budget void-reservation")
	var id, reason optString
	flags.Var(&id, "id", "reservation id (required)")
	flags.Var(&reason, "reason", "why the provider cannot have billed this call (required, kept as audit evidence)")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if !id.set || strings.TrimSpace(id.value) == "" {
		fmt.Fprintln(env.Stderr, "argument error: --id is required")
		return cli.ExitUsage
	}
	if !reason.set || strings.TrimSpace(reason.value) == "" {
		fmt.Fprintln(env.Stderr, "argument error: --reason is required")
		return cli.ExitUsage
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	now := op.Now()
	if err := llmbudget.VoidReservation(ctx, op.Pool, strings.TrimSpace(id.value), now); err != nil {
		fmt.Fprintf(env.Stdout, "Error: %s\n", voidRefusal(err))
		return cli.ExitFailure
	}
	return printJSON(env, map[string]any{
		"voided": true, "id": strings.TrimSpace(id.value), "reason": strings.TrimSpace(reason.value),
		"voided_at": now.UTC().Format(time.RFC3339),
	})
}

func voidRefusal(err error) string {
	if err == llmbudget.ErrNotReserved {
		return llmbudget.ErrNotReserved.Error()
	}
	return "could not void the reservation"
}

func printJSON(env cli.Env, document any) int {
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		fmt.Fprintln(env.Stderr, "could not write the result")
		return cli.ExitFailure
	}
	fmt.Fprintln(env.Stdout, string(encoded))
	return cli.ExitOK
}
