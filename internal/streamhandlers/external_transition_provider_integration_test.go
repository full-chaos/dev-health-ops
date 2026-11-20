//go:build integration

package streamhandlers

import (
	"bytes"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func externalTransitionRecord(system string) externalSinkRecord {
	key := "7"
	if system == "jira" || system == "linear" {
		key = "ENG-7"
	}
	return externalSinkRecord{Index: 0, Kind: "work_item_transition.v1", ExternalID: "t-" + system, Payload: map[string]any{
		"externalKey": key, "provider": system, "workItemType": "issue",
		"occurredAt": "2026-07-22T11:00:00Z", "fromStatus": "todo", "toStatus": "in_progress",
	}}
}

// A work_item_transition.v1 row stores the batch source system as provider,
// for every source system whose work_item.v1 rows the push accepts; it is read
// back with FINAL.
func TestExternalTransitionStoresTheBatchSourceSystem(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC) }
	var systems []string
	for system, kinds := range externalAllowedKinds {
		if _, ok := kinds["work_item.v1"]; ok {
			systems = append(systems, system)
		}
	}
	sort.Strings(systems)
	if len(systems) == 0 {
		t.Fatal("no source system accepts work_item.v1")
	}
	for _, system := range systems {
		t.Run(system, func(t *testing.T) {
			pointer := projectMembershipPointer()
			pointer.SourceSystem = system
			if _, err := sink.Write(ctx, externalSinkBatch{
				Pointer: pointer, SourceID: uuid.MustParse("9749bda0-fc9f-4076-b19d-7b26c4f306ff"),
				Records: []externalSinkRecord{externalTransitionRecord(system)},
			}); err != nil {
				t.Fatalf("write: %v", err)
			}
			var provider string
			if err := conn.QueryRow(ctx,
				`SELECT provider FROM work_item_transitions FINAL WHERE org_id = ? AND work_item_id = ?`,
				projectMembershipTestOrg, externalWorkItemID(system, externalWorkItemInstance(system, pointer.SourceInstance, ""), externalTransitionRecord(system).Payload["externalKey"].(string), "issue"),
			).Scan(&provider); err != nil {
				t.Fatalf("read back: %v", err)
			}
			if provider != system {
				t.Fatalf("stored provider=%q, want %q", provider, system)
			}
		})
	}
}

// A source system whose work_item.v1 rows the push refuses is refused for a
// transition row too, loudly: the batch write fails and an ERROR line names
// the org and the value. No row is stored.
func TestExternalTransitionRefusesASystemWorkItemsRefuses(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	t.Setenv("DEV_HEALTH_TEST_SWAPS_DEFAULT_LOGGER", "1")
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	pointer := projectMembershipPointer()
	pointer.SourceSystem = "custom"
	if _, ok := externalAllowedKinds["custom"]["work_item.v1"]; ok {
		t.Fatal("premise: custom accepts work_item.v1")
	}
	if _, err := sink.Write(ctx, externalSinkBatch{
		Pointer: pointer, SourceID: uuid.MustParse("9749bda0-fc9f-4076-b19d-7b26c4f306ff"),
		Records: []externalSinkRecord{externalTransitionRecord("custom")},
	}); err == nil {
		t.Fatal("a custom transition was written")
	}
	line := logged.String()
	if !strings.Contains(line, `"level":"ERROR"`) || !strings.Contains(line, `"organization_id":"`+projectMembershipTestOrg+`"`) ||
		!strings.Contains(line, `"source_system":"custom"`) {
		t.Fatalf("refusal log %q names no ERROR, org and source system", line)
	}
	var stored uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM work_item_transitions WHERE org_id = ?`, projectMembershipTestOrg).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != 0 {
		t.Fatalf("refused transition stored %d rows", stored)
	}
}
