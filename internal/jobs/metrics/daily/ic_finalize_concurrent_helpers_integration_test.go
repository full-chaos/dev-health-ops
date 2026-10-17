//go:build integration

package daily

import (
	"context"
	"testing"
	"time"
)

func dayLockerForTest(*testing.T, context.Context) func(context.Context, string, time.Time) (func(), error) {
	return nil
}

func setDayLockerForTest(*ICFinalizeExecutor, func(context.Context, string, time.Time) (func(), error)) {
}
