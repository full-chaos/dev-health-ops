package remaining

import (
	"errors"
	"fmt"
	"os"
	"regexp"
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
			wrapped := stepFailure("query work_items", test.err)
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
	if stepFailure("query work_items", nil) != nil {
		t.Fatal("a nil error must stay nil")
	}
}

func TestStepFailureKeepsTheTypedCauseReachable(t *testing.T) {
	var exception *clickhouse.Exception
	if !errors.As(stepFailure("count work_items", &clickhouse.Exception{Code: 60}), &exception) || exception.Code != 60 {
		t.Fatal("errors.As lost the ClickHouse exception")
	}
	var pgError *pgconn.PgError
	if !errors.As(stepFailure("send work_item_team_attributions batch", &pgconn.PgError{Code: "40001"}), &pgError) || pgError.Code != "40001" {
		t.Fatal("errors.As lost the Postgres error")
	}
}

// The handler tests below pin the four Retryable return sites of PartitionHandler.Work: each one now names its step
// in the safe cause, and none leaks the driver text. Removing a stepFailure wrap makes the matching case fail.
func TestPartitionHandlerRetryableFailuresNameTheirStep(t *testing.T) {
	run := Run{ID: handlerRunID, OrganizationID: handlerOrgID, Family: "capacity", Status: "running"}
	cases := []struct {
		name     string
		store    *handlerStore
		executor *handlerExecutor
		want     string
	}{
		{"claim", &handlerStore{claimErr: fmt.Errorf("claim: %w", &pgconn.PgError{Code: "57014", Message: hostile})}, &handlerExecutor{}, "step=claim_partition sqlstate=57014"},
		{"load run", &handlerStore{claim: handlerClaim(), loadRunErr: fmt.Errorf("load: %w", &pgconn.PgError{Code: "08006", Message: hostile})}, &handlerExecutor{}, "step=load_run sqlstate=08006"},
		{"compute", &handlerStore{run: run, claim: handlerClaim()}, &handlerExecutor{computeErr: fmt.Errorf("ch: %w", &clickhouse.Exception{Code: 241, Message: hostile})}, "step=compute_partition ch_code=241"},
		{"complete", &handlerStore{run: run, claim: handlerClaim(), completeErr: fmt.Errorf("complete: %w", &pgconn.PgError{Code: "40001", Message: hostile})}, &handlerExecutor{}, "step=complete_partition sqlstate=40001"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			handler, err := NewPartitionHandler[jobruntime.RemainingCapacityArgs](test.store, test.executor, "capacity")
			if err != nil {
				t.Fatal(err)
			}
			workErr := handler.Work(t.Context(), capacityExecution())
			if workErr == nil || !strings.Contains(workErr.Error(), string(jobruntime.CategoryRetryable)) {
				t.Fatalf("not Retryable: %v", workErr)
			}
			cause, ok := jobruntime.SafeCause(workErr)
			if !ok || cause != test.want {
				t.Fatalf("cause = %q ok=%v, want %q", cause, ok, test.want)
			}
			if strings.Contains(cause, "hunter2") || strings.Contains(workErr.Error(), "hunter2") {
				t.Fatal("driver text reached the returned error or the cause")
			}
		})
	}
}

// TestAttributionStepErrorsAreAllWrapped is a source guard for the 26 step errors of the work_item_attribution
// executor and writer (work_item_attribution_native_clickhouse.go, work_item_attribution_write.go): none may go
// back to a bare fmt.Errorf("<step>: %w", err), which would leave a retryable failure with no named step again.
// It FAILS when a file cannot be read or holds fewer wrapped sites than the 26 it had when this was written.
func TestAttributionStepErrorsAreAllWrapped(t *testing.T) {
	bare := regexp.MustCompile(`fmt\.Errorf\("[^"%]+: %w", err\)`)
	wrapped := 0
	for _, name := range []string{"work_item_attribution_native_clickhouse.go", "work_item_attribution_write.go"} {
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if found := bare.FindAll(source, -1); len(found) != 0 {
			t.Fatalf("%s holds %d bare step error(s) again: %q", name, len(found), found[0])
		}
		wrapped += strings.Count(string(source), `stepFailure("`)
	}
	if wrapped < 26 {
		t.Fatalf("only %d stepFailure sites in the attribution files, want at least 26", wrapped)
	}
}
