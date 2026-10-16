package synccoverage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestFailureClassNamesTheCauseWithoutItsMessage(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"nil":          {nil, "none"},
		"flags":        {fmt.Errorf("x: %w", ErrInvalidProcessorFlags), "invalid_processor_flags"},
		"not found":    {fmt.Errorf("x: %w", ErrConfigNotFound), "config_not_found"},
		"pool":         {fmt.Errorf("x: %w", ErrPoolUnavailable), "pool_unavailable"},
		"deadline":     {fmt.Errorf("x: %w", context.DeadlineExceeded), "deadline_exceeded"},
		"canceled":     {fmt.Errorf("x: %w", context.Canceled), "context_canceled"},
		"postgres":     {fmt.Errorf("x: %w", &pgconn.PgError{Code: "40001", Message: "secret-detail"}), "postgres_40001"},
		"other":        {errors.New("secret-detail"), "other"},
		"typed nil pg": {fmt.Errorf("x: %w", (*pgconn.PgError)(nil)), "other"},
	} {
		if got := FailureClass(tc.err); got != tc.want || strings.Contains(got, "secret") {
			t.Errorf("%s: FailureClass = %q, want %q", name, got, tc.want)
		}
	}
}

func TestEffectiveDatasetKeysFlagFailuresAreClassified(t *testing.T) {
	t.Parallel()
	for name, raw := range map[string]string{"not a dict": `["x"]`, "not json": `{`} {
		_, err := effectiveDatasetKeys("work-items", json.RawMessage(raw))
		if err == nil || FailureClass(err) != "invalid_processor_flags" {
			t.Errorf("%s: err = %v, class = %q", name, err, FailureClass(err))
		}
	}
}
