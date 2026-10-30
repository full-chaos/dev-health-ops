package orgreactivate

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

const stateRoutePath = "/internal/v1/organizations/state"

func TestStateReadsStatusAndRevisionWithTheServiceCredential(t *testing.T) {
	org := uuid.New()
	var gotAuth, gotMethod, gotPath, raw string
	client, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotMethod, gotPath = r.Header.Get("Authorization"), r.Method, r.URL.Path
		b, _ := io.ReadAll(r.Body)
		raw = string(b)
		_, _ = w.Write([]byte(`{"organization_id":"` + org.String() + `","status":"suspended","revision":7}`))
	})
	got, err := client.State(context.Background(), org)
	if err != nil || got.Status != "suspended" || got.Revision != 7 {
		t.Fatalf("State = %+v, %v, want suspended at 7", got, err)
	}
	if gotMethod != http.MethodPost || gotPath != stateRoutePath || gotAuth != "Bearer "+testToken {
		t.Fatalf("request = %s %s", gotMethod, gotPath)
	}
	if raw != `{"organization_id":"`+org.String()+`"}` {
		t.Fatalf("body = %s, want organization_id alone", raw)
	}
}

func TestStateFailuresAreErrorsAndNameTheirClass(t *testing.T) {
	org := uuid.New()
	id := `"organization_id":"` + org.String() + `"`
	for name, tc := range map[string]struct {
		status int
		body   string
		want   error
	}{
		"not found":         {404, `{"reason_code":"organization_not_found"}`, ErrRefused},
		"unauthorized":      {401, `{}`, ErrRefused},
		"server error":      {500, `{}`, ErrUnavailable},
		"not json":          {200, `nope`, ErrUnavailable},
		"other org":         {200, `{"organization_id":"` + uuid.NewString() + `","status":"active","revision":1}`, ErrUnavailable},
		"other status":      {200, `{` + id + `,"status":"deleted","revision":1}`, ErrUnavailable},
		"no status":         {200, `{` + id + `,"revision":1}`, ErrUnavailable},
		"no revision":       {200, `{` + id + `,"status":"active"}`, ErrUnavailable},
		"negative":          {200, `{` + id + `,"status":"active","revision":-1}`, ErrUnavailable},
		"string revision":   {200, `{` + id + `,"status":"active","revision":"1"}`, ErrUnavailable},
		"fraction":          {200, `{` + id + `,"status":"active","revision":1.5}`, ErrUnavailable},
		"overflow":          {200, `{` + id + `,"status":"active","revision":9223372036854775808}`, ErrUnavailable},
		"state_changed 409": {409, `{"reason_code":"state_changed"}`, ErrRefused},
	} {
		client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = w.Write([]byte(tc.body))
		})
		_, err := client.State(context.Background(), org)
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.want)
		}
		if err != nil && (strings.Contains(err.Error(), testToken) || strings.Contains(err.Error(), "reason_code")) {
			t.Errorf("%s: the error carries the credential or the answer text: %v", name, err)
		}
	}
}

// The mutating calls send if_revision when given one, and the body is byte-equal to today's when not.
func TestMutatingCallsSendIfRevisionOnlyWhenGiven(t *testing.T) {
	org, actor := uuid.New(), uuid.New()
	seven := int64(7)
	for name, call := range map[string]func(c *Client, rev *int64) error{
		"reactivate": func(c *Client, rev *int64) error { return c.Reactivate(context.Background(), org, actor, rev) },
		"suspend":    func(c *Client, rev *int64) error { return c.Suspend(context.Background(), org, actor, rev) },
	} {
		status := map[string]string{"reactivate": "active", "suspend": "suspended"}[name]
		var raw string
		client, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			raw = string(b)
			_, _ = w.Write([]byte(`{"organization_id":"` + org.String() + `","status":"` + status + `"}`))
		})
		if err := call(client, nil); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if want := `{"organization_id":"` + org.String() + `","actor_principal_id":"` + actor.String() + `"}`; raw != want {
			t.Fatalf("%s without a revision: body %s, want %s", name, raw, want)
		}
		if err := call(client, &seven); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var body map[string]any
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatal(err)
		}
		if len(body) != 3 || body["if_revision"] != float64(7) {
			t.Fatalf("%s with a revision: body %s, want if_revision 7 added", name, raw)
		}
		zero := int64(0)
		if err := call(client, &zero); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !strings.Contains(raw, `"if_revision":0`) {
			t.Fatalf("%s with revision 0: body %s, want if_revision 0 sent", name, raw)
		}
	}
}

// A 409 is ErrStateChanged (the revision sent is stale), distinct from a refusal and from an outage.
func TestAConflictIsErrStateChanged(t *testing.T) {
	seven := int64(7)
	client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"reason_code":"state_changed"}`))
	})
	for name, err := range map[string]error{
		"reactivate": client.Reactivate(context.Background(), uuid.New(), uuid.New(), &seven),
		"suspend":    client.Suspend(context.Background(), uuid.New(), uuid.New(), &seven),
	} {
		if !errors.Is(err, ErrStateChanged) || errors.Is(err, ErrUnavailable) || errors.Is(err, ErrRefused) {
			t.Errorf("%s: err = %v, want ErrStateChanged alone", name, err)
		}
	}
	other, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) })
	if err := other.Reactivate(context.Background(), uuid.New(), uuid.New(), &seven); errors.Is(err, ErrStateChanged) || !errors.Is(err, ErrRefused) {
		t.Errorf("a 403 = %v, want ErrRefused", err)
	}
}

var (
	_ Reactivator = (*Client)(nil)
	_ Suspender   = (*Client)(nil)
	_ StateReader = (*Client)(nil)
)
