package issueprlinks

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestProduceFailureLogsCarryNoErrorText(t *testing.T) {
	const marker = "SECRET-MARKER-https://tenant.example/token=abc123"
	for name, conn := range map[string]*stubConn{
		"load":  {queryErr: errors.New(marker)},
		"write": {responses: stubResponsesForOneLink(), batchErr: errors.New(marker)},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			loader, err := NewLoader(conn)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := NewWriter(conn)
			if err != nil {
				t.Fatal(err)
			}
			service, err := NewService(loader, writer, slog.New(slog.NewTextHandler(&logs, nil)))
			if err != nil {
				t.Fatal(err)
			}
			service.now = func() time.Time { return time.Unix(0, 0) }
			if _, err := service.Produce(context.Background(), testOrg, Window{}); err == nil {
				t.Fatal("want a failure")
			}
			out := logs.String()
			if !strings.Contains(out, "level=ERROR") {
				t.Fatalf("failure line missing: %q", out)
			}
			if strings.Contains(out, "SECRET-MARKER") || strings.Contains(out, "abc123") {
				t.Fatalf("raw error text reached the log: %s", out)
			}
			if !strings.Contains(out, "error_class=") || !strings.Contains(out, "error_type=") {
				t.Fatalf("class/type missing: %s", out)
			}
		})
	}
}
