package sessionissue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestParseReadableAcceptsExactlyTheListing(t *testing.T) {
	long := strings.Repeat("a", 100) + "/" + strings.Repeat("b", 100) + "/" + strings.Repeat("c", 50)
	var max []string
	for i := range MaxReadableRepositories {
		max = append(max, fmt.Sprintf("%s%03d", long, i))
	}
	if len(max[0]) != 255 {
		t.Fatalf("key of %d bytes, want 255", len(max[0]))
	}
	slices.Sort(max)
	maxJSON := `["` + strings.Join(max, `","`) + `"]`
	for name, tc := range map[string]struct {
		answer string
		want   *Readable
	}{
		"organization": {`{"schema_version":"readable_repositories.v1","scope":"organization"}`, &Readable{Scope: ReadableOrganization}},
		"repositories": {`{"schema_version":"readable_repositories.v1","scope":"repositories","repositories":["acme/a","acme/b/c"]}`,
			&Readable{Scope: ReadableRepositories, Repositories: []string{"acme/a", "acme/b/c"}}},
		"no repository": {`{"schema_version":"readable_repositories.v1","scope":"repositories","repositories":[]}`,
			&Readable{Scope: ReadableRepositories, Repositories: []string{}}},
		"256 keys of 255 bytes": {`{"schema_version":"readable_repositories.v1","scope":"repositories","repositories":` + maxJSON + `}`,
			&Readable{Scope: ReadableRepositories, Repositories: max}},
		"denied": {`{"schema_version":"readable_repositories.v1","scope":"denied","reason_code":"repository_allowlist_too_large"}`,
			&Readable{Scope: ReadableDenied, Reason: ReasonAllowlistTooLarge}},
		"organization with a list":    {`{"schema_version":"readable_repositories.v1","scope":"organization","repositories":[]}`, nil},
		"repositories without a list": {`{"schema_version":"readable_repositories.v1","scope":"repositories"}`, nil},
		"repositories null":           {`{"schema_version":"readable_repositories.v1","scope":"repositories","repositories":null}`, nil},
		"unsorted":                    {`{"schema_version":"readable_repositories.v1","scope":"repositories","repositories":["acme/b","acme/a"]}`, nil},
		"repeated key":                {`{"schema_version":"readable_repositories.v1","scope":"repositories","repositories":["acme/a","acme/a"]}`, nil},
		"wildcard in a list":          {`{"schema_version":"readable_repositories.v1","scope":"repositories","repositories":["*"]}`, nil},
		"upper case key":              {`{"schema_version":"readable_repositories.v1","scope":"repositories","repositories":["Acme/a"]}`, nil},
		"one segment":                 {`{"schema_version":"readable_repositories.v1","scope":"repositories","repositories":["acme"]}`, nil},
		"257 keys":                    {`{"schema_version":"readable_repositories.v1","scope":"repositories","repositories":` + strings.Replace(maxJSON, `["`, `["0/0","`, 1) + `}`, nil},
		"denied without a reason":     {`{"schema_version":"readable_repositories.v1","scope":"denied"}`, nil},
		"denied with a list":          {`{"schema_version":"readable_repositories.v1","scope":"denied","reason_code":"no_membership","repositories":[]}`, nil},
		"another schema":              {`{"schema_version":"readable_repositories.v2","scope":"organization"}`, nil},
		"unknown scope":               {`{"schema_version":"readable_repositories.v1","scope":"all"}`, nil},
		"a member twice":              {`{"schema_version":"readable_repositories.v1","scope":"denied","scope":"organization"}`, nil},
		"an unknown member":           {`{"schema_version":"readable_repositories.v1","scope":"organization","extra":1}`, nil},
		"not an object":               {`["organization"]`, nil},
		"trailing data":               {`{"schema_version":"readable_repositories.v1","scope":"organization"}{}`, nil},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := parseReadable([]byte(tc.answer))
			if tc.want == nil {
				if err == nil {
					t.Fatalf("accepted %+v", got)
				}
				return
			}
			if err != nil || got.Scope != tc.want.Scope || got.Reason != tc.want.Reason || !slices.Equal(got.Repositories, tc.want.Repositories) ||
				(tc.want.Repositories != nil && got.Repositories == nil) {
				t.Fatalf("%s: %d keys, %v; want %s %d keys", got.Scope, len(got.Repositories), err, tc.want.Scope, len(tc.want.Repositories))
			}
		})
	}
}

// The client sends the workload token and the pair, and every failure is ErrUnavailable, never an empty listing.
func TestReadableRepositoriesClient(t *testing.T) {
	principal, org := uuid.New(), uuid.New()
	token := testToken(t)
	var status int
	var answer string
	var gotBody, gotAuth, gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
		gotBody, gotAuth, gotPath = string(raw), r.Header.Get("Authorization"), r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, answer)
	}))
	t.Cleanup(server.Close)
	base, _ := url.Parse(server.URL)
	client, err := newTestClient(t, base, token, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	status, answer = http.StatusOK, `{"schema_version":"readable_repositories.v1","scope":"organization"}`
	got, err := client.ReadableRepositories(context.Background(), principal, org)
	if err != nil || got.Scope != ReadableOrganization {
		t.Fatalf("%+v, %v", got, err)
	}
	if gotPath != PathReadableRepositories || gotAuth != "Bearer "+token ||
		gotBody != `{"organization_id":"`+org.String()+`","principal_id":"`+principal.String()+`"}` {
		t.Fatalf("sent %s %q %s", gotPath, gotAuth, gotBody)
	}
	for name, tc := range map[string]struct {
		status int
		answer string
	}{
		"route absent (404)":   {http.StatusNotFound, `404 page not found`},
		"refused (401)":        {http.StatusUnauthorized, `{"schema_version":"error.v1","status":401,"reason_code":"invalid_credentials"}`},
		"store down (503)":     {http.StatusServiceUnavailable, `{"schema_version":"error.v1","status":503,"reason_code":"store_unavailable"}`},
		"not the listing":      {http.StatusOK, `{"scope":"organization"}`},
		"over the answer size": {http.StatusOK, `{"schema_version":"readable_repositories.v1","scope":"denied","reason_code":"` + strings.Repeat("a", maxReadableAnswerBytes) + `"}`},
	} {
		t.Run(name, func(t *testing.T) {
			status, answer = tc.status, tc.answer
			got, err := client.ReadableRepositories(context.Background(), principal, org)
			if !errors.Is(err, ErrUnavailable) || got.Scope != "" {
				t.Fatalf("%+v, %v; want ErrUnavailable and no listing", got, err)
			}
		})
	}
}
