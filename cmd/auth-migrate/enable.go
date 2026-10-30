package main

import (
	"context"
	"io"

	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

type enableStore interface {
	PlanEnable(ctx context.Context, name string, in workload.EnableInput) (workload.EnablePlan, error)
	EnableWorkload(ctx context.Context, name string, in workload.EnableInput) (workload.EnablePlan, error)
}

func runEnable(context.Context, enableStore, workloadsArgs, io.Writer, io.Writer) int { return 1 }
