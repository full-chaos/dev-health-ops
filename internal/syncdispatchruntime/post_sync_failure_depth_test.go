package syncdispatchruntime

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/jackc/pgx/v5/pgconn"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func wrapDeep(err error, depth int) error {
	for range depth {
		err = fmt.Errorf("w: %w", err)
	}
	return err
}

// A stage and a SQLSTATE under 40 wrappers are inside the walk bound: both are read, and the staged error is not staged again.
func TestStageAndSQLStateSurviveADeepChain(t *testing.T) {
	deep := wrapDeep(unavailable(stageDaily, &pgconn.PgError{Code: "23505", Message: "dup"}), 40)
	if got := postSyncStageOf(deep); got != stageDaily {
		t.Fatalf("stage = %q, want %q", got, stageDaily)
	}
	if detail := failureDetail(deep); !strings.Contains(detail, " stage=daily") || !strings.Contains(detail, " code=23505") {
		t.Fatalf("failureDetail = %q, want stage=daily and code=23505", detail)
	}
	if restaged := stagedFailure(stageArgs, deep); restaged != deep {
		t.Fatalf("an already-staged deep error was staged again as %q", postSyncStageOf(restaged))
	}
}

// A chain past the bound loses its stage, and that is never silent: the chain-defect Warn and counter fire.
func TestAChainPastTheBoundIsReportedAsADefect(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test").Int64Counter("post_sync_error_chain_defect_total")
	if err != nil {
		t.Fatal(err)
	}
	previous := chainDefects
	chainDefects = counter
	t.Cleanup(func() { chainDefects = previous })

	tooDeep := wrapDeep(unavailable(stageDaily, nil), 70)
	if got := postSyncStageOf(tooDeep); got != "" {
		t.Fatalf("stage = %q past the bound, want none", got)
	}
	if got := chainDefect(tooDeep); got != chainDefectBound {
		t.Fatalf("chainDefect = %q, want %q", got, chainDefectBound)
	}
	var logs bytes.Buffer
	service := &NativePostSyncService{logger: synclog.New(slog.New(slog.NewJSONHandler(&logs, nil)))}
	service.observeFanoutFailure(context.Background(), PostSyncArgs{TransportArgs{OrgID: "00000000-0000-4000-8000-000000000001", RunID: "00000000-0000-4000-8000-000000000002"}}, tooDeep, stageArgs, true)
	if !strings.Contains(logs.String(), `"msg":"post_sync_fanout.chain_defect"`) || !strings.Contains(logs.String(), `"level":"WARN"`) || !strings.Contains(logs.String(), `"reason":"chain_bound"`) {
		t.Fatalf("no chain-defect Warn for a chain past the bound:\n%s", logs.String())
	}
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	for _, scope := range collected.ScopeMetrics {
		for _, found := range scope.Metrics {
			if sum, ok := found.Data.(metricdata.Sum[int64]); ok && found.Name == "post_sync_error_chain_defect_total" && len(sum.DataPoints) == 1 && sum.DataPoints[0].Value == 1 {
				return
			}
		}
	}
	t.Fatal("the chain-defect counter was not incremented")
}
