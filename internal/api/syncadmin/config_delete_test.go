package syncadmin

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// fakeWriter records deletes and fails them on request.
type fakeWriter struct {
	deletes [][3]string
	fail    bool
}

func (f *fakeWriter) deleteConfig(_ context.Context, orgID string, config *syncConfig) error {
	f.deletes = append(f.deletes, [3]string{orgID, config.Name, config.Provider})
	if f.fail {
		return errInjected
	}
	return nil
}

// noConfigReader finds no config.
type noConfigReader struct{ *faultReader }

func (noConfigReader) configByID(context.Context, string, uuid.UUID) (*syncConfig, error) {
	return nil, nil
}

// TestDeleteSyncConfigSteps pins delete_sync_config: an id uuid.UUID()
// refuses, or no config in the org, is 404 with no delete; a found config
// is deleted by its own name and provider in the caller's org and answers
// an empty 204; a failed read or delete is the bare 500.
func TestDeleteSyncConfigSteps(t *testing.T) {
	run := func(reader reader, writes *fakeWriter, id string) (int, string, http.Header, *bytes.Buffer) {
		var logs bytes.Buffer
		h := &handlers{store: reader, writes: writes, logger: slog.New(slog.NewTextHandler(&logs, nil))}
		recorder := serveAs(t, h, h.deleteSyncConfig, "/x", map[string]string{"config_id": id})
		return recorder.Code, recorder.Body.String(), recorder.Header(), &logs
	}
	notFound := `{"detail":"Sync configuration not found"}`

	writes := &fakeWriter{}
	if code, body, _, _ := run(&faultReader{}, writes, "not-a-uuid"); code != http.StatusNotFound || body != notFound || len(writes.deletes) != 0 {
		t.Errorf("bad id: %d %s, deletes %v", code, body, writes.deletes)
	}
	if code, body, _, _ := run(noConfigReader{&faultReader{}}, writes, uuid.NewString()); code != http.StatusNotFound || body != notFound || len(writes.deletes) != 0 {
		t.Errorf("unknown id: %d %s, deletes %v", code, body, writes.deletes)
	}

	code, body, headers, _ := run(&faultReader{}, writes, faultConfigID.String())
	if code != http.StatusNoContent || body != "" || headers.Get("Content-Type") != "application/json" || headers.Get("Content-Length") != "" {
		t.Errorf("found: %d %q %v", code, body, headers)
	}
	if len(writes.deletes) != 1 || writes.deletes[0][0] == "" || writes.deletes[0][1] != "n" || writes.deletes[0][2] != "github" {
		t.Errorf("found: deletes %v, want one by (org, n, github)", writes.deletes)
	}

	code, body, _, logs := run(&faultReader{fail: "configByID"}, &fakeWriter{}, faultConfigID.String())
	if code != http.StatusInternalServerError || body != `{"detail":"Internal Server Error"}` || !strings.Contains(logs.String(), "get_config") {
		t.Errorf("read failure: %d %s %s", code, body, logs)
	}
	failing := &fakeWriter{fail: true}
	code, body, _, logs = run(&faultReader{}, failing, faultConfigID.String())
	if code != http.StatusInternalServerError || !strings.Contains(logs.String(), "delete_config") || !strings.Contains(logs.String(), errInjected.Error()) {
		t.Errorf("delete failure: %d %s %s", code, body, logs)
	}
}
