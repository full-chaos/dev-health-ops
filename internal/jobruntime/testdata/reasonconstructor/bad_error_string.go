package main

import (
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

func main() {
	err := errors.New("boom")
	_ = jobruntime.WithReason(jobruntime.Permanent(err), err.Error())
}
