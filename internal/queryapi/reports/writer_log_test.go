package reports

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// A failed write is logged as a write failure, with the SQLSTATE as its cause.
func TestWriterLogsAFailedWriteAsAWrite(t *testing.T) {
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buffer, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	logWriteFailure(context.Background(), "saveReportSchedule", "insert job", &pgconn.PgError{Code: "23505"})
	logWriteFailure(context.Background(), "createSavedReport", "begin", errors.New("boom"))

	out := buffer.String()
	for _, want := range []string{"saved report write failed", "sqlstate_23505", "cause=query", "operation=saveReportSchedule"} {
		if !strings.Contains(out, want) {
			t.Errorf("log %q lacks %q", out, want)
		}
	}
	if strings.Contains(out, "read failed") {
		t.Errorf("a write failure was logged as a read failure: %q", out)
	}
}
