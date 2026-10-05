//go:build integration

// CHAOS-8103 on a real ClickHouse at the migration head: the stored
// repository URL read (repos.settings, a Nullable(String) of JSON, read with
// FINAL) and the served repositories of a metric stored per repository.
package explain

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRepositoriesAndTheirStoredURLsOnARealClickHouse(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)

	const org, otherOrg = "explain-org-repositories", "explain-org-other"
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 9, 1, 4, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 9, 1, 5, 0, 0, 0, time.UTC)
	withURL, noSettings, sshURL, moved, foreign := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	text := func(value string) *string { return &value }
	for _, row := range []struct {
		id       uuid.UUID
		name     string
		org      string
		settings *string
		synced   time.Time
	}{
		{withURL, "acme/webapp", org, text(`{"source":"github","url":"https://github.com/acme/webapp","default_branch":"main"}`), newer},
		{noSettings, "acme/api", org, nil, newer},
		{sshURL, "acme/tools", org, text(`{"url":"git@github.com:acme/tools.git"}`), newer},
		// Two versions of one repository: FINAL keeps the later one.
		{moved, "acme/moved", org, text(`{"url":"https://gitlab.example.com/acme/old-name"}`), older},
		{moved, "acme/moved", org, text(`{"url":"https://gitlab.example.com/acme/moved"}`), newer},
		// Another organization's repository with a URL, never read for org.
		{foreign, "other/webapp", otherOrg, text(`{"url":"https://github.com/other/webapp"}`), newer},
	} {
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, settings, last_synced) VALUES (?, ?, 'github', ?, ?, ?, ?)`,
			row.id, row.name, row.org, row.synced, row.settings, row.synced); err != nil {
			t.Fatalf("insert repos row %s: %v", row.name, err)
		}
	}
	for index, id := range []uuid.UUID{withURL, noSettings, sshURL, moved} {
		touched := uint32(100 * (index + 1))
		if err := admin.Exec(ctx, `INSERT INTO repo_metrics_daily (org_id, repo_id, day, total_loc_touched, computed_at) VALUES (?, ?, ?, ?, ?)`,
			org, id, day, touched, newer); err != nil {
			t.Fatalf("insert repo_metrics_daily row %d: %v", index, err)
		}
	}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}

	// The read itself: the one stored web URL per repository of the
	// organization, the later version, nothing for no settings, an ssh
	// remote or another organization's id.
	urls, err := reader.fetchRepoSourceURLs(ctx, org, []string{withURL.String(), noSettings.String(), sshURL.String(), moved.String(), foreign.String()})
	if err != nil {
		t.Fatalf("fetchRepoSourceURLs: %v", err)
	}
	want := map[string]string{
		withURL.String(): "https://github.com/acme/webapp",
		moved.String():   "https://gitlab.example.com/acme/moved",
	}
	if len(urls) != len(want) {
		t.Fatalf("urls = %v, want %v", urls, want)
	}
	for id, url := range want {
		if urls[id] != url {
			t.Fatalf("urls = %v, want %v", urls, want)
		}
	}

	// Through the route's builder: churn is stored per repository.
	got, err := BuildExplainResponse(ctx, reader, org, Params{
		Metric: "churn", StartDay: day, EndDay: day.AddDate(0, 0, 1),
		CompareStart: day.AddDate(0, 0, -1), CompareEnd: day, ScopeLevel: "repo", ScopeIDs: []string{"acme/webapp"},
	})
	if err != nil {
		t.Fatalf("BuildExplainResponse: %v", err)
	}
	if got.Repositories == nil || len(*got.Repositories) != 1 {
		t.Fatalf("repositories = %v, want the one repository of the scope", got.Repositories)
	}
	repository := (*got.Repositories)[0]
	if repository.ID != withURL.String() || repository.Value != 100 ||
		repository.Name == nil || *repository.Name != "acme/webapp" ||
		repository.SourceURL == nil || *repository.SourceURL != "https://github.com/acme/webapp" {
		t.Fatalf("repository = %s", mustMarshal(t, repository))
	}
	if got.SourceURL == nil || *got.SourceURL != "https://github.com/acme/webapp" {
		t.Fatalf("source_url = %v, want the scope repository's URL", got.SourceURL)
	}

	// The organization: every repository with a row, the URL only where one
	// is stored.
	got, err = BuildExplainResponse(ctx, reader, org, Params{
		Metric: "churn", StartDay: day, EndDay: day.AddDate(0, 0, 1),
		CompareStart: day.AddDate(0, 0, -1), CompareEnd: day, ScopeLevel: "org",
	})
	if err != nil {
		t.Fatalf("BuildExplainResponse: %v", err)
	}
	if got.SourceURL != nil {
		t.Fatalf("an organization scope serves source_url %q", *got.SourceURL)
	}
	served := map[string]*string{}
	for _, repository := range *got.Repositories {
		served[repository.ID] = repository.SourceURL
	}
	if len(served) != 4 {
		t.Fatalf("repositories = %s, want four", mustMarshal(t, *got.Repositories))
	}
	for id, url := range served {
		wantURL, stored := want[id]
		if stored != (url != nil) || (url != nil && *url != wantURL) {
			t.Fatalf("repository %s: source_url %v, want %q (stored: %v)", id, url, wantURL, stored)
		}
	}
}
