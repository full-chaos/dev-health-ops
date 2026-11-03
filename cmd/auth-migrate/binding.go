package main

import (
	"context"
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

type bindingStore interface {
	LookupBinding(ctx context.Context, t workload.BindingTuple) (workload.BindingInfo, error)
	RebindBinding(ctx context.Context, t workload.BindingTuple, uid string, a workload.Actor) (workload.BindingInfo, bool, error)
	DisableBinding(ctx context.Context, bindingID uuid.UUID, a workload.Actor) error
	EnableBinding(ctx context.Context, bindingID uuid.UUID, a workload.Actor) error
}

type workloadRowStore interface {
	Lookup(ctx context.Context, name string) (*workload.Info, error)
	Disable(ctx context.Context, name string, a workload.Actor) error
}

func stateWord(disabled bool) string {
	if disabled {
		return "disabled"
	}
	return "enabled"
}

// runBinding runs rebind, binding-disable and binding-enable. No pin value is printed.
func runBinding(ctx context.Context, store bindingStore, parsed workloadsArgs, stdout, stderr io.Writer) int {
	info, err := store.LookupBinding(ctx, parsed.tuple)
	if err != nil {
		fmt.Fprintf(stderr, "workloads error: %s failed (%s)\n", parsed.verb, failureClass(err))
		return 1
	}
	switch parsed.verb {
	case "rebind":
		same := info.SamePin(parsed.binding.ServiceAccountUID)
		if !parsed.apply {
			fmt.Fprintf(stdout, "workloads rebind dry run: workload=%s binding=%s state=%s pinned=%t unchanged=%t (no change written)\n",
				info.Workload, info.ID, stateWord(info.Disabled), info.Pinned, same)
			return 0
		}
		_, changed, err := store.RebindBinding(ctx, parsed.tuple, parsed.binding.ServiceAccountUID, workload.Actor{})
		if err != nil {
			fmt.Fprintf(stderr, "workloads error: rebind failed (%s)\n", failureClass(err))
			return 1
		}
		fmt.Fprintf(stdout, "workloads rebind applied: workload=%s binding=%s state=%s changed=%t\n", info.Workload, info.ID, stateWord(info.Disabled), changed)
	default:
		disable := parsed.verb == "binding-disable"
		if !parsed.apply {
			fmt.Fprintf(stdout, "workloads %s dry run: workload=%s binding=%s state=%s unchanged=%t (no change written)\n",
				parsed.verb, info.Workload, info.ID, stateWord(info.Disabled), info.Disabled == disable)
			return 0
		}
		if disable {
			err = store.DisableBinding(ctx, info.ID, workload.Actor{})
		} else {
			err = store.EnableBinding(ctx, info.ID, workload.Actor{})
		}
		if err != nil {
			fmt.Fprintf(stderr, "workloads error: %s failed (%s)\n", parsed.verb, failureClass(err))
			return 1
		}
		fmt.Fprintf(stdout, "workloads %s applied: workload=%s binding=%s was=%s\n", parsed.verb, info.Workload, info.ID, stateWord(info.Disabled))
	}
	return 0
}

// runDisable sets the workload row's disabled mark. A repeat is a no-op.
func runDisable(ctx context.Context, store workloadRowStore, parsed workloadsArgs, stdout, stderr io.Writer) int {
	info, err := store.Lookup(ctx, parsed.name)
	if err != nil {
		fmt.Fprintf(stderr, "workloads error: disable failed (%s)\n", failureClass(err))
		return 1
	}
	if !parsed.apply {
		fmt.Fprintf(stdout, "workloads disable dry run: workload=%s state=%s unchanged=%t (no change written)\n", parsed.name, stateWord(info.Disabled), info.Disabled)
		return 0
	}
	if err := store.Disable(ctx, parsed.name, workload.Actor{}); err != nil {
		fmt.Fprintf(stderr, "workloads error: disable failed (%s)\n", failureClass(err))
		return 1
	}
	fmt.Fprintf(stdout, "workloads disable applied: workload=%s was=%s\n", parsed.name, stateWord(info.Disabled))
	return 0
}
