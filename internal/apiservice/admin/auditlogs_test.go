package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWriteAuditPageIncludesResolvedDisplayNames(t *testing.T) {
	actorName := "Ari Admin"
	resourceName := "github.com production"
	response := auditPageResponse(t, &auditLog{
		ID:                  uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		OrgID:               uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		Action:              "ingest_source_registered",
		ResourceType:        "ingest_source",
		ResourceID:          "00000000-0000-0000-0000-000000000003",
		ActorDisplayName:    &actorName,
		ResourceDisplayName: &resourceName,
		Changes:             []byte(`{}`),
		RequestMetadata:     []byte(`{}`),
		Status:              "success",
		CreatedAt:           time.Date(2026, time.October, 4, 20, 0, 0, 0, time.UTC),
	})

	if got := response["actor_display_name"]; got != actorName {
		t.Errorf("actor_display_name = %#v; want %q", got, actorName)
	}
	if got := response["resource_display_name"]; got != resourceName {
		t.Errorf("resource_display_name = %#v; want %q", got, resourceName)
	}
}

func TestWriteAuditPageKeepsUnavailableDisplayNamesNull(t *testing.T) {
	response := auditPageResponse(t, &auditLog{
		ID:              uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		OrgID:           uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		Action:          "historical_extension_action",
		ResourceType:    "historical_extension",
		ResourceID:      "opaque-resource-id",
		Changes:         []byte(`{}`),
		RequestMetadata: []byte(`{}`),
		Status:          "success",
		CreatedAt:       time.Date(2026, time.October, 4, 20, 0, 0, 0, time.UTC),
	})

	for _, field := range []string{"actor_display_name", "resource_display_name"} {
		if got, ok := response[field]; !ok || got != nil {
			t.Errorf("%s = %#v, %t; want nil, true", field, got, ok)
		}
	}
}

func auditPageResponse(t *testing.T, log *auditLog) map[string]any {
	t.Helper()
	recorder := httptest.NewRecorder()
	(&handlers{}).writeAuditPage(context.Background(), recorder, []*auditLog{log}, 1, 50, 0)
	if recorder.Code != http.StatusOK {
		t.Fatalf("writeAuditPage() status = %d; want %d: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var payload struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode writeAuditPage response: %v", err)
	}
	if len(payload.Items) != 1 {
		t.Fatalf("writeAuditPage() items = %#v; want one item", payload.Items)
	}
	return payload.Items[0]
}
