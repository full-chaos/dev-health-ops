package main

import (
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

func main() {
	_ = jobruntime.WithReason(jobruntime.Permanent(errors.New("boom")), jobruntime.ReasonHandlerPanic)
}
