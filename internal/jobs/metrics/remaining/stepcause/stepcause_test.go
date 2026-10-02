package stepcause

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// hostile is driver text the safe cause must NEVER carry: it stands for a table value, a query or a credential
// that a ClickHouse or Postgres error message can embed.
const hostile = "tenant-acme password=hunter2 SELECT * FROM work_items"

func TestStepFailureCarriesOnlyTheClosedStepAndCodes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"plain error", errors.New(hostile), "step=query work_items"},
		{"clickhouse exception", fmt.Errorf("read: %w", &clickhouse.Exception{Code: 241, Name: "DB::Exception", Message: hostile}), "step=query work_items ch_code=241"},
		{"postgres error", fmt.Errorf("exec: %w", &pgconn.PgError{Code: "23514", Message: hostile}), "step=query work_items sqlstate=23514"},
		{"postgres error with a hostile code is dropped", &pgconn.PgError{Code: "23514; DROP TABLE x", Message: hostile}, "step=query work_items"},
		{"postgres error with a lowercase code is dropped", &pgconn.PgError{Code: "abcde", Message: hostile}, "step=query work_items"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			wrapped := Failure(QueryWorkItems, test.err)
			cause, ok := jobruntime.SafeCause(wrapped)
			if !ok || cause != test.want {
				t.Fatalf("cause = %q ok=%v, want %q", cause, ok, test.want)
			}
			if strings.Contains(cause, "hunter2") || strings.Contains(cause, "tenant") || strings.Contains(cause, "SELECT") {
				t.Fatalf("the safe cause carries driver text: %q", cause)
			}
			if !errors.Is(wrapped, test.err) {
				t.Fatal("stepFailure broke errors.Is")
			}
			if got := wrapped.Error(); !strings.HasPrefix(got, "query work_items: ") {
				t.Fatalf("the message lost its step prefix: %q", got)
			}
		})
	}
	if Failure(QueryWorkItems, nil) != nil {
		t.Fatal("a nil error must stay nil")
	}
}

func TestStepFailureKeepsTheTypedCauseReachable(t *testing.T) {
	var exception *clickhouse.Exception
	if !errors.As(Failure(CountWorkItems, &clickhouse.Exception{Code: 60}), &exception) || exception.Code != 60 {
		t.Fatal("errors.As lost the ClickHouse exception")
	}
	var pgError *pgconn.PgError
	if !errors.As(Failure(SendWorkItemTeamAttributionsBatch, &pgconn.PgError{Code: "40001"}), &pgError) || pgError.Code != "40001" {
		t.Fatal("errors.As lost the Postgres error")
	}
}

func TestZeroStepFailsClosedToAFixedLabel(t *testing.T) {
	wrapped := Failure(Step{}, errors.New(hostile))
	cause, ok := jobruntime.SafeCause(wrapped)
	if !ok || cause != "step=unknown_step" {
		t.Fatalf("cause = %q ok=%v, want step=unknown_step", cause, ok)
	}
	if got := wrapped.Error(); !strings.HasPrefix(got, "unknown_step: ") {
		t.Fatalf("message = %q", got)
	}
}
