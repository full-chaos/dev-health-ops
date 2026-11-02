package orgreactivate

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

func TestRenameSendsTheOrgTheActorAndTheName(t *testing.T) {
	if RenamePath != "/internal/v1/organizations/rename" {
		t.Fatalf("RenamePath = %q", RenamePath)
	}
	org, actor := uuid.New(), uuid.New()
	var gotAuth, gotMethod, gotPath string
	var body map[string]any
	client, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotMethod, gotPath = r.Header.Get("Authorization"), r.Method, r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		_, _ = w.Write([]byte(`{"organization_id":"` + org.String() + `","name":"Acme","renamed":true}`))
	})
	if err := client.Rename(context.Background(), org, actor, " Acme "); err != nil {
		t.Fatalf("Rename = %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != RenamePath || gotAuth != "Bearer "+testToken {
		t.Fatalf("request = %s %s auth=%v", gotMethod, gotPath, gotAuth == "Bearer "+testToken)
	}
	if len(body) != 3 || body["organization_id"] != org.String() || body["actor_principal_id"] != actor.String() || body["name"] != " Acme " {
		t.Fatalf("body = %v, want exactly organization_id, actor_principal_id and the name as given", body)
	}
}

// Each refusal of the rename route is named by its class, so go-api answers it; anything else is unavailable.
func TestRenameClassesItsRefusals(t *testing.T) {
	org := uuid.New()
	for name, tc := range map[string]struct {
		status int
		body   string
		want   error
	}{
		"422 name_invalid":           {http.StatusUnprocessableEntity, `{"reason_code":"name_invalid"}`, ErrNameInvalid},
		"409 organization_suspended": {http.StatusConflict, `{"reason_code":"organization_suspended"}`, ErrSuspended},
		"404":                        {http.StatusNotFound, `{"reason_code":"organization_not_found"}`, ErrNotFound},
		"403":                        {http.StatusForbidden, `{"reason_code":"admin_required"}`, ErrRefused},
		"409 state_changed":          {http.StatusConflict, `{"reason_code":"state_changed"}`, ErrRefused},
		"503":                        {http.StatusServiceUnavailable, `{}`, ErrUnavailable},
		"200, another org":           {http.StatusOK, `{"organization_id":"` + uuid.NewString() + `","name":"x","renamed":true}`, ErrUnavailable},
		"200, not JSON":              {http.StatusOK, `nope`, ErrUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			if err := client.Rename(context.Background(), org, uuid.New(), "Acme"); !errors.Is(err, tc.want) {
				t.Fatalf("Rename = %v, want %v", err, tc.want)
			}
		})
	}
}
