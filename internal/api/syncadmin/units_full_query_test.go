package syncadmin

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// freshnessCase serves one work-items unit whose watermark is 30 days behind
// and whose integration's canonical work-items dataset row is datasetRow.
func freshnessCase(t *testing.T, provider string, datasetRow *workItemsDatasetRow, fail string) (*unitsReader, int, string) {
	t.Helper()
	unsetForTest(t, "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS")
	unsetForTest(t, "SYNC_WATERMARK_OVERLAP")
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	name, full, external := "repo", "org/repo", "org/repo"
	at := now.Add(-30 * 24 * time.Hour)
	integration := uuid.New()
	unit := runUnit{ID: uuid.New(), SyncRunID: faultRunID, IntegrationID: integration, SourceID: uuid.New(), OrgID: "org",
		Provider: provider, DatasetKey: "work-items", CostClass: "medium", Mode: "incremental", Status: "success",
		CreatedAt: now, UpdatedAt: now, HasSource: true, SourceName: &name, SourceFullName: &full, SourceExternalID: &external}
	reader := &unitsReader{faultReader: &faultReader{fail: fail}, units: []runUnit{unit},
		watermarks: []schedsync.WatermarkRow{{SourceID: "org/repo", DatasetKey: "work-items", RepoID: "r", Target: "t", LastSyncedAt: &at}}}
	if datasetRow != nil {
		row := *datasetRow
		row.IntegrationID = integration
		reader.datasetRows = []workItemsDatasetRow{row}
	}
	_, status, body := serveUnits(t, reader, "", now)
	return reader, status, body
}

// A full-query work-items dataset is planned as one window, so the view must
// not show it as catching up whatever its watermark.
func TestRunUnitsFullQueryDatasetIsNotCatchingUp(t *testing.T) {
	for name, options := range map[string]string{
		"fetch_all":    `{"fetch_all": true}`,
		"jql_override": `{"jql_override": "project = X"}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, status, body := freshnessCase(t, "jira", &workItemsDatasetRow{Enabled: true, Options: []byte(options)}, "")
			if status != http.StatusOK {
				t.Fatalf("%d %s", status, body)
			}
			for _, want := range []string{`"catching_up":false,"ticks_behind":null`, `"catching_up_dataset_count":0`} {
				if !strings.Contains(body, want) {
					t.Errorf("want %s in:\n%s", want, body)
				}
			}
			if strings.Contains(body, `"catching_up":true`) {
				t.Errorf("full-query dataset reported catching up:\n%s", body)
			}
		})
	}
}

// Control: every other row keeps the ratchet scope's answer.
func TestRunUnitsBoundedWorkItemsStayCatchingUp(t *testing.T) {
	unreadable := []byte(`not json`)
	for name, tc := range map[string]struct {
		provider string
		row      *workItemsDatasetRow
	}{
		"jira no row":          {"jira", nil},
		"jira empty options":   {"jira", &workItemsDatasetRow{Enabled: true, Options: []byte(`{}`)}},
		"jira fetch_all false": {"jira", &workItemsDatasetRow{Enabled: true, Options: []byte(`{"fetch_all": false, "jql_override": ""}`)}},
		"jira unreadable":      {"jira", &workItemsDatasetRow{Enabled: true, Options: unreadable}},
		"jira null options":    {"jira", &workItemsDatasetRow{Enabled: true}},
		"jira disabled":        {"jira", &workItemsDatasetRow{Enabled: false, Options: []byte(`{"fetch_all": true}`)}},
		"github fetch_all":     {"github", &workItemsDatasetRow{Enabled: true, Options: []byte(`{"fetch_all": true}`)}},
		"gitlab jql_override":  {"gitlab", &workItemsDatasetRow{Enabled: true, Options: []byte(`{"jql_override": "x"}`)}},
	} {
		t.Run(name, func(t *testing.T) {
			_, status, body := freshnessCase(t, tc.provider, tc.row, "")
			if status != http.StatusOK || !strings.Contains(body, `"catching_up":true`) ||
				!strings.Contains(body, `"catching_up_dataset_count":1`) {
				t.Fatalf("%d, want catching up:\n%s", status, body)
			}
		})
	}
}

// The dataset read is org scoped by its argument and asks for the unit's
// integration; a failed read is the dataset_freshness 500, and a unit with no
// work-items family dataset reads nothing.
func TestRunUnitsDatasetRowsRead(t *testing.T) {
	reader, status, _ := freshnessCase(t, "jira", &workItemsDatasetRow{Enabled: true, Options: []byte(`{}`)}, "")
	if status != http.StatusOK || reader.askedOrg != "org" || len(reader.askedIDs) != 1 || reader.askedIDs[0] != reader.units[0].IntegrationID {
		t.Fatalf("asked org %q ids %v, status %d", reader.askedOrg, reader.askedIDs, status)
	}
	reader, status, body := freshnessCase(t, "jira", nil, "workItemsDatasetRows")
	if status != http.StatusInternalServerError || reader.calls["workItemsDatasetRows"] != 1 {
		t.Fatalf("%d %s, want a 500 after one dataset read", status, body)
	}
}
