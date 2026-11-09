package authprovision

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/workloadclient"
)

var (
	accountOrg   = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	accountOwner = uuid.MustParse("44444444-4444-4444-8444-444444444444")
)

const (
	accountCreated  = `{"service_account_id":"77777777-7777-4777-8777-777777777777","organization_id":"11111111-1111-4111-8111-111111111111","name":"ci-bot","status":"active","owner_principal_id":"44444444-4444-4444-8444-444444444444"}`
	accountDisabled = `{"service_account_id":"77777777-7777-4777-8777-777777777777","status":"disabled"}`
	accountEnabled  = `{"service_account_id":"77777777-7777-4777-8777-777777777777","status":"active"}`
	accountOwned    = `{"service_account_id":"77777777-7777-4777-8777-777777777777","owner_principal_id":"44444444-4444-4444-8444-444444444444"}`
)

// newAccountClient is an AccountClient of a server that records each request
// and answers it with answer(path).
func newAccountClient(t *testing.T, answer func(path string) (int, string)) (*AccountClient, *[]keyRequest, string) {
	t.Helper()
	var got []keyRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		got = append(got, keyRequest{r.Method, r.URL.Path, r.Header.Get("Authorization"), string(raw)})
		status, body := answer(r.URL.Path)
		writeJSON(w, status, body)
	}))
	t.Cleanup(server.Close)
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	wc, token := workloadclient.New(t)
	core, err := NewWorkloadCore(base, wc, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return NewAccountClient(core), &got, token
}

func accountAnswers(path string) (int, string) {
	switch path {
	case PathServiceAccountCreate:
		return http.StatusOK, accountCreated
	case PathServiceAccountDisable:
		return http.StatusOK, accountDisabled
	case PathServiceAccountEnable:
		return http.StatusOK, accountEnabled
	case PathServiceAccountOwner:
		return http.StatusOK, accountOwned
	}
	return http.StatusNotFound, `{}`
}

func TestAccountPathsAreTheAuthLifecycleRoutes(t *testing.T) {
	for got, want := range map[string]string{
		PathServiceAccountCreate:  "/internal/v1/service-accounts/create",
		PathServiceAccountDisable: "/internal/v1/service-accounts/disable",
		PathServiceAccountEnable:  "/internal/v1/service-accounts/enable",
		PathServiceAccountOwner:   "/internal/v1/service-accounts/owner",
	} {
		if got != want {
			t.Fatalf("path %q, want %q", got, want)
		}
	}
}

// Each verb posts its exact body (the auth route's admitted members, nothing
// more) with the workload token and passes the route's answer through
// unchanged.
func TestAccountVerbsSendTheirBodiesAndPassTheAnswer(t *testing.T) {
	client, got, token := newAccountClient(t, accountAnswers)
	ctx := context.Background()
	cases := []struct {
		name, path, body, answer string
		call                     func() (json.RawMessage, error)
	}{
		{"create", PathServiceAccountCreate,
			`{"actor_principal_id":"33333333-3333-4333-8333-333333333333","name":"ci-bot","organization_id":"11111111-1111-4111-8111-111111111111","owner_principal_id":"44444444-4444-4444-8444-444444444444"}`, accountCreated,
			func() (json.RawMessage, error) {
				return client.CreateAccount(ctx, accountOrg, accountOwner, keyActor, "ci-bot")
			}},
		{"disable", PathServiceAccountDisable,
			`{"actor_principal_id":"33333333-3333-4333-8333-333333333333","service_account_id":"77777777-7777-4777-8777-777777777777"}`, accountDisabled,
			func() (json.RawMessage, error) { return client.DisableAccount(ctx, keyAccount, keyActor) }},
		{"enable", PathServiceAccountEnable,
			`{"actor_principal_id":"33333333-3333-4333-8333-333333333333","service_account_id":"77777777-7777-4777-8777-777777777777"}`, accountEnabled,
			func() (json.RawMessage, error) { return client.EnableAccount(ctx, keyAccount, keyActor) }},
		{"owner", PathServiceAccountOwner,
			`{"actor_principal_id":"33333333-3333-4333-8333-333333333333","owner_principal_id":"44444444-4444-4444-8444-444444444444","service_account_id":"77777777-7777-4777-8777-777777777777"}`, accountOwned,
			func() (json.RawMessage, error) {
				return client.SetAccountOwner(ctx, keyAccount, accountOwner, keyActor)
			}},
	}
	for _, tc := range cases {
		answer, err := tc.call()
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if len(*got) == 0 {
			t.Fatalf("%s sent no request", tc.name)
		}
		last := (*got)[len(*got)-1]
		if last.method != http.MethodPost || last.path != tc.path || last.body != tc.body || last.auth != "Bearer "+token {
			t.Fatalf("%s sent %s %s auth %q body %s\nwant POST %s body %s", tc.name, last.method, last.path, last.auth, last.body, tc.path, tc.body)
		}
		if string(answer) != tc.answer {
			t.Fatalf("%s answer %s, want %s", tc.name, answer, tc.answer)
		}
	}
}

// A 4xx is a refusal with its reason; a 200 that names no service account, or
// another account than the one asked for, is unavailable.
func TestAccountVerbsRefusalAndWrongAnswer(t *testing.T) {
	client, _, _ := newAccountClient(t, func(path string) (int, string) {
		switch path {
		case PathServiceAccountCreate:
			return http.StatusConflict, `{"reason_code":"name_taken"}`
		case PathServiceAccountDisable:
			return http.StatusOK, `{"status":"disabled"}`
		case PathServiceAccountEnable:
			return http.StatusOK, `{"service_account_id":"99999999-9999-4999-8999-999999999999","status":"active"}`
		}
		return http.StatusOK, `[]`
	})
	ctx := context.Background()
	if _, err := client.CreateAccount(ctx, accountOrg, accountOwner, keyActor, "ci-bot"); ReasonOf(err) != "name_taken" {
		t.Fatalf("create error %v, want refusal name_taken", err)
	}
	for name, call := range map[string]func() (json.RawMessage, error){
		"disable without an id": func() (json.RawMessage, error) { return client.DisableAccount(ctx, keyAccount, keyActor) },
		"enable of another id":  func() (json.RawMessage, error) { return client.EnableAccount(ctx, keyAccount, keyActor) },
		"owner not an object": func() (json.RawMessage, error) {
			return client.SetAccountOwner(ctx, keyAccount, accountOwner, keyActor)
		},
	} {
		if _, err := call(); !errors.Is(err, ErrUnavailable) || ReasonOf(err) != "" {
			t.Fatalf("%s: error %v, want ErrUnavailable", name, err)
		}
	}
}
