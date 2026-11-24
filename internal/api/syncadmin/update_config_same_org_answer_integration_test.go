//go:build integration

package syncadmin

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

type sameOrgAnswerTransport struct{}

func (sameOrgAnswerTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("no provider call is expected in this test")
}

type sameOrgAnswerDecryptor struct{}

func (sameOrgAnswerDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return []byte(`{"email":"bot@example.invalid","api_token":"synthetic-token","base_url":"https://jira.example.invalid"}`), nil
}

// startSameOrgAnswerVenue is startCascadeVenue with the update path's Jira discovery wired (a decryptor and a client
// that sends nothing), as a deployed go-api has it.
func startSameOrgAnswerVenue(t *testing.T) *cascadeVenue {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	org := uuid.NewString()
	var feature string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM feature_flags WHERE key = $1`, canonicalIncidentFeatureKey).Scan(&feature); err != nil {
		t.Fatal(err)
	}
	pgseed.Org(ctx, t, pool, org, "enterprise")
	pgseed.OrgOverride(ctx, t, pool, org, feature, true)
	mux := http.NewServeMux()
	for _, route := range Routes(Deps{Pool: pool, Guard: testGuard(t), Logger: quiet(),
		Decryptor: sameOrgAnswerDecryptor{}, JiraHTTP: &http.Client{Transport: sameOrgAnswerTransport{}}}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	materializer, err := schedsync.NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	return &cascadeVenue{t: t, pool: pool, org: org, server: server, materializer: materializer}
}

// sameOrgAnswers PATCHes one fresh own-org configuration per path and returns each answer as "<status> <body>" with
// every uuid and timestamp replaced (they differ per run; every other byte is the server's).
func sameOrgAnswers(t *testing.T) map[string]string {
	t.Helper()
	v := startSameOrgAnswerVenue(t)
	uuids := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	times := regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+(Z|[+-][0-9:]+)?"`)
	answers := map[string]string{}
	for _, cell := range []struct {
		name, provider, stored, body string
	}{
		{"jira options", "jira", `["work-items"]`, `{"sync_options":{"fetch_comments":false}}`},
		{"github targets", "github", `["tests"]`, `{"sync_targets":["tests","work-items"]}`},
		{"github options", "github", `["tests"]`, `{"sync_options":{"fetch_comments":false}}`},
		{"github empty body", "github", `["tests"]`, `{}`},
	} {
		one := v.seed(cell.provider, cell.stored, []string{"tests"})
		status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+one.config.String(), cell.body)
		body = times.ReplaceAllString(uuids.ReplaceAllString(body, "<uuid>"), `"<time>"`)
		answers[cell.name] = strconv.Itoa(status) + " " + body
	}
	return answers
}

// sameOrgAnswersAtMain is each answer of sameOrgAnswers at ops main ede68fe0f296 (no syncadmin change since
// 666fa8dfa92b, the base of CHAOS-14582), recorded by this cell on CI. Empty = not recorded yet: the cell fails and
// prints the answers.
var sameOrgAnswersAtMain = map[string]string{}

// A PATCH of an own-org configuration answers byte for byte as main on each path the CHAOS-14582 check sits in front of
// (Jira discovery, a dataset row insert, the runtime options, an empty body): the check narrows no same-org answer.
func TestSyncConfigPatchOfAnOwnIntegrationAnswersAsMain(t *testing.T) {
	got := sameOrgAnswers(t)
	for name, answer := range got {
		want, pinned := sameOrgAnswersAtMain[name]
		if !pinned {
			t.Errorf("SAMEORG %q: %s (not pinned)", name, answer)
			continue
		}
		if answer != want {
			t.Errorf("%s: %s\nwant main's %s", name, answer, want)
		}
	}
}
