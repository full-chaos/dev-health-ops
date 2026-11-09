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
	keyAccount = uuid.MustParse("77777777-7777-4777-8777-777777777777")
	keyActor   = uuid.MustParse("33333333-3333-4333-8333-333333333333")
)

const (
	keyReceipt = `{"schema_version":"credential-receipt.v1","kind":"issue","credential":{"id":"crd_88888888-8888-4888-8888-888888888888"},"secret":"fcsak_test-secret"}`
	keyEntry   = `{"schema_version":"credential-entry.v1","id":"crd_88888888-8888-4888-8888-888888888888","status":"revoked"}`
	keyList    = `{"schema_version":"credential-list.v1","items":[]}`
)

type keyRequest struct {
	method, path, auth, body string
}

// newKeyClient is a KeyClient of a server that records each request and
// answers it with answer(path).
func newKeyClient(t *testing.T, answer func(path string) (int, string)) (*KeyClient, *[]keyRequest, string) {
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
	return NewKeyClient(core), &got, token
}

func keyAnswers(path string) (int, string) {
	switch path {
	case PathServiceAccountKeyIssue, PathServiceAccountKeyRotate:
		return http.StatusOK, keyReceipt
	case PathServiceAccountKeyRevoke:
		return http.StatusOK, keyEntry
	case PathServiceAccountKeyList:
		return http.StatusOK, keyList
	}
	return http.StatusNotFound, `{}`
}

func TestKeyPathsAreTheAuthKeyRoutes(t *testing.T) {
	for got, want := range map[string]string{
		PathServiceAccountKeyIssue:  "/internal/v1/service-accounts/keys/issue",
		PathServiceAccountKeyRotate: "/internal/v1/service-accounts/keys/rotate",
		PathServiceAccountKeyRevoke: "/internal/v1/service-accounts/keys/revoke",
		PathServiceAccountKeyList:   "/internal/v1/service-accounts/keys/list",
	} {
		if got != want {
			t.Fatalf("path %q, want %q", got, want)
		}
	}
}

// Each verb posts its exact body with the workload token and passes the
// route's answer through unchanged.
func TestKeyVerbsSendTheirBodiesAndPassTheAnswer(t *testing.T) {
	client, got, token := newKeyClient(t, keyAnswers)
	ctx := context.Background()
	expires := int64(3600)
	cases := []struct {
		name, path, body, answer string
		call                     func() (json.RawMessage, error)
	}{
		{"issue", PathServiceAccountKeyIssue,
			`{"actor_principal_id":"33333333-3333-4333-8333-333333333333","expires_in_seconds":3600,"service_account_id":"77777777-7777-4777-8777-777777777777"}`, keyReceipt,
			func() (json.RawMessage, error) { return client.IssueKey(ctx, keyAccount, keyActor, &expires) }},
		{"issue default expiry", PathServiceAccountKeyIssue,
			`{"actor_principal_id":"33333333-3333-4333-8333-333333333333","service_account_id":"77777777-7777-4777-8777-777777777777"}`, keyReceipt,
			func() (json.RawMessage, error) { return client.IssueKey(ctx, keyAccount, keyActor, nil) }},
		{"rotate", PathServiceAccountKeyRotate,
			`{"actor_principal_id":"33333333-3333-4333-8333-333333333333","overlap_seconds":600,"service_account_id":"77777777-7777-4777-8777-777777777777"}`, keyReceipt,
			func() (json.RawMessage, error) { return client.RotateKey(ctx, keyAccount, keyActor, 600, nil) }},
		{"revoke", PathServiceAccountKeyRevoke,
			`{"actor_principal_id":"33333333-3333-4333-8333-333333333333","credential_id":"crd_88888888-8888-4888-8888-888888888888","reason":"compromised"}`, keyEntry,
			func() (json.RawMessage, error) {
				return client.RevokeKey(ctx, "crd_88888888-8888-4888-8888-888888888888", keyActor, "compromised")
			}},
		{"list", PathServiceAccountKeyList,
			`{"actor_principal_id":"33333333-3333-4333-8333-333333333333","service_account_id":"77777777-7777-4777-8777-777777777777"}`, keyList,
			func() (json.RawMessage, error) { return client.ListKeys(ctx, keyAccount, keyActor) }},
	}
	for _, tc := range cases {
		answer, err := tc.call()
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
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

// A 4xx is a refusal with its reason; a 200 of another wire form is
// unavailable, and its text is in no error.
func TestKeyVerbsRefusalAndWrongAnswer(t *testing.T) {
	client, _, _ := newKeyClient(t, func(path string) (int, string) {
		if path == PathServiceAccountKeyIssue {
			return http.StatusConflict, `{"reason_code":"key_live"}`
		}
		return http.StatusOK, `{"schema_version":"credential-entry.v1","secret":"fcsak_leak"}`
	})
	ctx := context.Background()
	if _, err := client.IssueKey(ctx, keyAccount, keyActor, nil); ReasonOf(err) != "key_live" {
		t.Fatalf("issue error %v, want refusal key_live", err)
	}
	_, err := client.RotateKey(ctx, keyAccount, keyActor, 0, nil)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("rotate error %v, want ErrUnavailable", err)
	}
	if text := err.Error(); text != "authprovision: auth-service provisioning route unavailable: answer is not a credential-receipt.v1" {
		t.Fatalf("rotate error text %q", text)
	}
}
