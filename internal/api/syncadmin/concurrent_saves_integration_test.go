//go:build integration

package syncadmin

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"testing"
	"time"
)

// waitForSavesAtTheSelectionLock waits until `want` sessions wait for an
// advisory lock. A count that never comes is a failed measurement, never a
// pass.
func (v *cascadeVenue) waitForSavesAtTheSelectionLock(want int) {
	v.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var waiting int
	for time.Now().Before(deadline) {
		if err := v.pool.QueryRow(context.Background(),
			`SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted`).Scan(&waiting); err != nil {
			v.t.Fatal(err)
		}
		if waiting == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	v.t.Fatalf("harness: %d saves wait at the selection lock, want %d: the two saves were not interleaved, nothing is measured", waiting, want)
}

// TestTwoSavesAtOnceLoseNoTarget: two saves of one whole-integration
// configuration run at the same time. The test holds the selection lock of
// the integration until both saves wait for it, so both have done every read
// they do before the lock; then it lets them go, the first one first. Stored
// [git], the git rows on. Save A adds prs; save B (sent by a form that shows
// prs already checked) submits [git,prs,cicd]. Each save must compute its
// change from the stored list and the rows as they are when it holds the
// lock: after both, the parent and its child store [git,prs,cicd] and the
// rows of all three data types are on. A save that computes from a list it
// read before the lock drops prs from the stored list of the parent and of
// the child.
func TestTwoSavesAtOnceLoseNoTarget(t *testing.T) {
	v := startCascadeVenue(t, true)
	ctx := context.Background()
	for _, provider := range []string{"github", "gitlab"} {
		parent := v.seed(provider, `["git"]`, []string{"repo-metadata", "commits", "commit-stats", "files"})
		child := v.child(parent, provider, `["git"]`)
		holder, err := v.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := selectionLock(ctx, holder, v.org, parent.integration); err != nil {
			t.Fatal(err)
		}
		path := "/api/v1/admin/sync-configs/" + parent.config.String()
		statuses := make(chan string, 2)
		save := func(name, body string) {
			status, text := v.call("PATCH", path, body)
			statuses <- fmt.Sprintf("%s=%d %.200s", name, status, text)
		}
		go save("A", `{"sync_targets":["git","prs"]}`)
		v.waitForSavesAtTheSelectionLock(1)
		go save("B", `{"sync_targets":["git","prs","cicd"]}`)
		v.waitForSavesAtTheSelectionLock(2)
		if err := holder.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if answer := <-statuses; answer[2:5] != "200" {
				t.Errorf("%s: save %s", provider, answer)
			}
		}
		const want = `["git","prs","cicd"]`
		parentStored, childStored, rows := v.storedList(parent.config), v.storedList(child.config), v.rows(parent.integration)
		t.Logf("CONCURRENT %s :: parent stored=%s child stored=%s rows=[%s]", provider, parentStored, childStored, rows)
		if parentStored != want || childStored != want {
			t.Errorf("%s: after save A [git,prs] and save B [git,prs,cicd] the parent stores %s and the child %s, want %s for both: a target a save added is lost",
				provider, parentStored, childStored, want)
		}
		for _, key := range []string{"commits=true", "prs=true", "cicd=true"} {
			if !bytes.Contains([]byte(rows), []byte(key)) {
				t.Errorf("%s: the rows after both saves are [%s], without %s", provider, rows, key)
			}
		}
	}
}

func (v *cascadeVenue) rowsChangedCount(provider, direction string) int {
	v.t.Helper()
	var out bytes.Buffer
	if err := SelectionMetricsSource().WritePrometheus(&out); err != nil {
		v.t.Fatal(err)
	}
	found := regexp.MustCompile(`(?m)^` + selectionRowsChangedMetric + `\{provider="` + provider + `",direction="` + direction + `"\} (\d+)$`).FindStringSubmatch(out.String())
	if found == nil {
		return 0
	}
	count, err := strconv.Atoi(found[1])
	if err != nil {
		v.t.Fatal(err)
	}
	return count
}

// TestARefusedSaveCountsNoChangedRow: a save that switches rows on and is
// then refused (a malformed auto-import flag: 422) writes nothing, so the
// rows-changed counter must not move; the same save without the malformed
// flag is stored and moves the counter by the rows it switched on.
func TestARefusedSaveCountsNoChangedRow(t *testing.T) {
	v := startCascadeVenue(t, true)
	parent := v.seed("github", `["git"]`, []string{"repo-metadata", "commits", "commit-stats", "files"})
	path := "/api/v1/admin/sync-configs/" + parent.config.String()
	before, rowsBefore := v.rowsChangedCount("github", "enabled"), v.rows(parent.integration)
	status, body := v.call("PATCH", path, `{"sync_targets":["git","prs"],"sync_options":{"auto_import_teams":"yes"}}`)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("harness: the save with a malformed auto-import flag answers %d %.200s, want 422: nothing is measured", status, body)
	}
	if rows := v.rows(parent.integration); rows != rowsBefore {
		t.Fatalf("harness: the refused save changed the rows: [%s] -> [%s]", rowsBefore, rows)
	}
	if got := v.rowsChangedCount("github", "enabled"); got != before {
		t.Errorf("the refused save moved the rows-changed counter from %d to %d and no row changed", before, got)
	}
	if status, body := v.call("PATCH", path, `{"sync_targets":["git","prs"]}`); status != http.StatusOK {
		t.Fatalf("the save: %d %.200s", status, body)
	}
	if got := v.rowsChangedCount("github", "enabled"); got != before+3 {
		t.Errorf("the stored save moved the rows-changed counter from %d to %d, want %d (prs, pr-reviews, pr-comments switched on)", before, got, before+3)
	}
}
