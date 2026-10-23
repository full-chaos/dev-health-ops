//go:build integration

package syncadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// These tests force the interleavings of CHAOS-8860 with row locks held by the
// test, and wait on pg_locks (never on a sleep) until the order is reached: a
// harness step that does not reach its order is a t.Fatalf, never a pass.

func (v *cascadeVenue) lockWaiters(want int) {
	v.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var got int
	for time.Now().Before(deadline) {
		if err := v.pool.QueryRow(context.Background(), `SELECT count(DISTINCT pid) FROM pg_locks WHERE NOT granted`).Scan(&got); err != nil {
			v.t.Fatal(err)
		}
		if got == want {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	v.t.Fatalf("harness: %d blocked backends, want %d: the order was not forced, nothing is measured", got, want)
}

var lockProbeGitRows = []string{"repo-metadata", "commits", "commit-stats", "files"}

func (v *cascadeVenue) beginTx() pgx.Tx {
	v.t.Helper()
	tx, err := v.pool.Begin(context.Background())
	if err != nil {
		v.t.Fatal(err)
	}
	return tx
}

func (v *cascadeVenue) setDatasetRow(integration uuid.UUID, key string, on bool) {
	v.exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1,$2,$3,$4,$5,'{}'::json)
ON CONFLICT (org_id, integration_id, dataset_key) DO UPDATE SET is_enabled = EXCLUDED.is_enabled`, uuid.New(), v.org, integration, key, on)
}

func (v *cascadeVenue) holdDatasetRow(integration uuid.UUID, key string) pgx.Tx {
	v.t.Helper()
	tx := v.beginTx()
	var id uuid.UUID
	if err := tx.QueryRow(context.Background(), `SELECT id FROM integration_datasets WHERE org_id = $1 AND integration_id = $2 AND dataset_key = $3 FOR UPDATE`,
		v.org, integration, key).Scan(&id); err != nil {
		v.t.Fatalf("hold the %s row: %v", key, err)
	}
	return tx
}

func (v *cascadeVenue) holdConfigRow(config uuid.UUID) pgx.Tx {
	v.t.Helper()
	tx := v.beginTx()
	var id uuid.UUID
	if err := tx.QueryRow(context.Background(), `SELECT id FROM sync_configurations WHERE id = $1 FOR UPDATE`, config).Scan(&id); err != nil {
		v.t.Fatalf("hold the config row: %v", err)
	}
	return tx
}

func (v *cascadeVenue) answer(out chan string, name, method, path, body string) {
	go func() {
		status, text := v.call(method, path, body)
		out <- fmt.Sprintf("%s=%d %.160s", name, status, text)
	}()
}

func answered(t *testing.T, out chan string, count int) map[string]string {
	t.Helper()
	got := map[string]string{}
	for range count {
		select {
		case line := <-out:
			got[line[:1]] = line
		case <-time.After(60 * time.Second):
			t.Fatalf("harness: %d of %d answers after 60 s: a request is stuck", len(got), count)
		}
	}
	return got
}

// TestCrossedSaveAndDatasetEndpointDoNotDeadlock: a save that adds prs and
// drops cicd, and a dataset endpoint batch [cicd off, prs on], are crossed on
// purpose. The test holds the prs row until both requests have reached it,
// then lets them go. Before the fix the save holds prs and waits for cicd
// while the endpoint holds cicd and waits for prs: Postgres ends one with
// SQLSTATE 40P01 and the save answers 500. After it both answer 200 and the
// rows are the serial result.
func TestCrossedSaveAndDatasetEndpointDoNotDeadlock(t *testing.T) {
	v := startCascadeVenue(t, true)
	ctx := context.Background()
	parent := v.seed("github", `["git","cicd"]`, append(append([]string{}, lockProbeGitRows...), "cicd"))
	for _, key := range []string{"prs", "pr-reviews", "pr-comments"} {
		v.setDatasetRow(parent.integration, key, false)
	}
	holder := v.holdDatasetRow(parent.integration, "prs")
	out := make(chan string, 2)
	v.answer(out, "S", "PATCH", "/api/v1/admin/sync-configs/"+parent.config.String(), `{"sync_targets":["git","prs"]}`)
	v.lockWaiters(1)
	v.answer(out, "E", "PATCH", "/api/v1/admin/integrations/"+parent.integration.String()+"/datasets",
		`{"datasets":[{"dataset_key":"cicd","is_enabled":false},{"dataset_key":"prs","is_enabled":true}]}`)
	v.lockWaiters(2)
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	got := answered(t, out, 2)
	for name, line := range got {
		if line[2:5] != "200" {
			t.Errorf("request %s: %s", name, line)
		}
	}
	rows := v.rows(parent.integration)
	t.Logf("CROSSED %v stored=%s rows=[%s]", got, v.storedList(parent.config), rows)
	for _, want := range []string{"cicd=false", "pr-comments=true", "pr-reviews=true", "prs=true"} {
		if !strings.Contains(rows, want) {
			t.Errorf("rows after both requests are [%s], without %s", rows, want)
		}
	}
}

func (v *cascadeVenue) storedOptionKeys(config uuid.UUID) map[string]json.RawMessage {
	v.t.Helper()
	var text string
	if err := v.pool.QueryRow(context.Background(), `SELECT sync_options::text FROM sync_configurations WHERE id = $1`, config).Scan(&text); err != nil {
		v.t.Fatal(err)
	}
	options := map[string]json.RawMessage{}
	if err := json.Unmarshal([]byte(text), &options); err != nil {
		v.t.Fatalf("stored options %q: %v", text, err)
	}
	return options
}

// TestTwoOptionSavesAtOnceLoseNoKey: two saves with no list, or two saves of
// a source-pinned child, each set one option key. The test holds the
// configuration row until both saves have read the stored options, then lets
// them go. A save that merges into the options it read before any lock writes
// over the other's key.
func TestTwoOptionSavesAtOnceLoseNoKey(t *testing.T) {
	for _, shape := range []string{"parent", "child"} {
		t.Run(shape, func(t *testing.T) {
			v := startCascadeVenue(t, true)
			ctx := context.Background()
			parent := v.seed("github", `["git"]`, lockProbeGitRows)
			target := parent
			if shape == "child" {
				target = v.child(parent, "github", `["git"]`)
			}
			holder := v.holdConfigRow(target.config)
			out := make(chan string, 2)
			path := "/api/v1/admin/sync-configs/" + target.config.String()
			v.answer(out, "A", "PATCH", path, `{"sync_options":{"lock_probe_a":1}}`)
			v.lockWaiters(1)
			v.answer(out, "B", "PATCH", path, `{"sync_options":{"lock_probe_b":2}}`)
			v.lockWaiters(2)
			if err := holder.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			for name, line := range answered(t, out, 2) {
				if line[2:5] != "200" {
					t.Errorf("save %s: %s", name, line)
				}
			}
			options := v.storedOptionKeys(target.config)
			for _, key := range []string{"lock_probe_a", "lock_probe_b"} {
				if _, ok := options[key]; !ok {
					t.Errorf("%s: after both saves the stored options are %v, without %s: a save lost the key of the other", shape, options, key)
				}
			}
		})
	}
}

// TestParentCascadeAndChildSaveAtOnceAnswer200: a save of the parent with a
// list cascades to the child; a save of the child sets one option at the same
// time. Both must answer 200 and the child must hold both results: no
// deadlock, the serial result.
func TestParentCascadeAndChildSaveAtOnceAnswer200(t *testing.T) {
	v := startCascadeVenue(t, true)
	ctx := context.Background()
	parent := v.seed("github", `["git"]`, lockProbeGitRows)
	child := v.child(parent, "github", `["git"]`)
	holder := v.holdConfigRow(child.config)
	out := make(chan string, 2)
	v.answer(out, "P", "PATCH", "/api/v1/admin/sync-configs/"+parent.config.String(), `{"sync_targets":["git","prs"]}`)
	v.lockWaiters(1)
	v.answer(out, "C", "PATCH", "/api/v1/admin/sync-configs/"+child.config.String(), `{"sync_options":{"lock_probe_c":3}}`)
	v.lockWaiters(2)
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for name, line := range answered(t, out, 2) {
		if line[2:5] != "200" {
			t.Errorf("save %s: %s", name, line)
		}
	}
	if stored := v.storedList(child.config); stored != `["git","prs"]` {
		t.Errorf("the child stores %s, want [git prs]", stored)
	}
	if _, ok := v.storedOptionKeys(child.config)["lock_probe_c"]; !ok {
		t.Errorf("the child lost the option its own save set")
	}
}
