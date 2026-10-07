//go:build integration

package syncadmin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// fetched is what a scheduled sync of the config would fetch: the distinct
// dataset keys of the units the plan writes, in key order.
func (v *cascadeVenue) fetched(label string, one cascadeSeeded) string {
	v.t.Helper()
	ctx := context.Background()
	v.hour++
	when := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(v.hour) * time.Hour)
	digest := sha256.Sum256([]byte(fmt.Sprintf("fetched %s %s %d", label, one.config, v.hour)))
	tx, err := v.pool.Begin(ctx)
	if err != nil {
		v.t.Fatal(err)
	}
	result, err := v.materializer.Materialize(ctx, tx, schedsync.PendingOccurrence{
		ID: "sha256:" + hex.EncodeToString(digest[:]), IdentityVersion: schedsync.OccurrenceIdentityVersion,
		OrgID: v.org, ConfigID: one.config.String(), JobID: one.job.String(), ScheduledFor: when,
		ConfigActive: true, ConfigPlannerManaged: one.source == nil, ConfigSourceID: one.source, JobStatus: 0, JobType: "sync",
	})
	_ = tx.Rollback(ctx)
	if errors.Is(err, schedsync.ErrOccurrenceIneligible) {
		return "ineligible"
	}
	if err != nil {
		v.t.Fatalf("%s: plan: %v", label, err)
	}
	var keys string
	if err := v.pool.QueryRow(ctx, `SELECT coalesce(string_agg(DISTINCT dataset_key, ',' ORDER BY dataset_key), '') FROM sync_run_units WHERE sync_run_id = $1::uuid`,
		result.SyncRunID).Scan(&keys); err != nil {
		v.t.Fatal(err)
	}
	return keys
}

// plannedSource gives the planner-managed parent one source to plan.
func (v *cascadeVenue) plannedSource(parent cascadeSeeded, provider string) {
	v.t.Helper()
	v.sequence++
	v.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, $4, 'repo', $5, $5, $5, json_build_object('planner_managed_sync_config_id', $6::text), true, now(), now())`,
		uuid.New(), v.org, parent.integration, provider, fmt.Sprintf("acme/planned-%d", v.sequence), parent.config.String())
}

func answeredTargets(t *testing.T, body string) []string {
	t.Helper()
	var decoded struct {
		SyncTargets []string `json:"sync_targets"`
	}
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("answer %s: %v", body, err)
	}
	return decoded.SyncTargets
}

// TestAPatchThatAddsATargetTheFormDoesNotOfferSwitchesItsRowOn: an API client
// adds "security" or "blame" to a whole-integration configuration whose row
// of that target is off, or absent. The save switches that row on, as the
// create does for the same list: the answer and GET show the target, a
// scheduled sync fetches its dataset, no other row moves, the child gets the
// stored list, and a save of the list GET shows keeps all of it. A save that
// keeps "git" and leaves such a target out drops it from the stored list and
// moves no row.
func TestAPatchThatAddsATargetTheFormDoesNotOfferSwitchesItsRowOn(t *testing.T) {
	v := startCascadeVenue(t, true)
	gitOn := []string{"repo-metadata", "commits", "commit-stats", "files"}
	cases := 0
	for _, provider := range []string{"github", "gitlab"} {
		for _, target := range []string{"security", "blame"} {
			for _, rowState := range []string{"off", "absent"} {
				cases++
				label := fmt.Sprintf("%s %s row %s", provider, target, rowState)
				parent := v.seed(provider, `["git"]`, gitOn)
				if rowState == "off" {
					v.exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, $4, false, '{}'::json)`,
						uuid.New(), v.org, parent.integration, target)
				}
				v.plannedSource(parent, provider)
				child := v.child(parent, provider, `["git"]`)
				path := "/api/v1/admin/sync-configs/" + parent.config.String()
				// The plan makes the security row of an integration that has none
				// (on), so that state is not planned before the save.
				if target != "security" || rowState != "absent" {
					if before := v.fetched(label+" before", parent); strings.Contains(","+before+",", ","+target+",") {
						t.Fatalf("%s: the plan fetches %q before the save: [%s]", label, target, before)
					}
				}
				wantList := []string{"git", target}
				wantJSON := `["git","` + target + `"]`
				// Every row as it is now, and the row of the target on.
				rowsWith := func() string {
					rows := []string{target + "=true"}
					for _, row := range strings.Split(v.rows(parent.integration), ",") {
						if !strings.HasPrefix(row, target+"=") {
							rows = append(rows, row)
						}
					}
					slices.Sort(rows)
					return strings.Join(rows, ",")
				}
				wantRows := rowsWith()
				wantFetch := append([]string{target}, gitOn...)
				if target == "blame" {
					wantFetch = append(wantFetch, "security")
				}
				slices.Sort(wantFetch)

				status, body := v.call("PATCH", path, `{"sync_targets":`+wantJSON+`}`)
				if status != 200 {
					t.Fatalf("%s: save: %d %s", label, status, body)
				}
				check := func(step string, answer []string) {
					t.Helper()
					if got := v.rows(parent.integration); got != wantRows {
						t.Errorf("%s, %s: rows [%s], want [%s]: the row of the added target is on and no other row moved", label, step, got, wantRows)
					}
					if !slices.Equal(answer, wantList) {
						t.Errorf("%s, %s: the save answers %v, want %v", label, step, answer, wantList)
					}
					if got := v.shown(parent.config); got != wantJSON {
						t.Errorf("%s, %s: GET shows %s, want %s", label, step, got, wantJSON)
					}
					if got := v.storedList(parent.config); got != wantJSON {
						t.Errorf("%s, %s: stored %s, want %s", label, step, got, wantJSON)
					}
					if got := v.storedList(child.config); got != wantJSON {
						t.Errorf("%s, %s: the child stores %s, want the parent's stored list %s", label, step, got, wantJSON)
					}
					if got := v.fetched(label+" "+step, parent); got != strings.Join(wantFetch, ",") {
						t.Errorf("%s, %s: a scheduled sync fetches [%s], want [%s]", label, step, got, strings.Join(wantFetch, ","))
					}
				}
				check("after the add", answeredTargets(t, body))

				// The create of the same list is the reference: same answer, the
				// row of the target on.
				status, created := v.call("POST", "/api/v1/admin/sync-configs",
					fmt.Sprintf(`{"name":"created %d","provider":%q,"sync_targets":%s,"sync_options":{"all_repos":true}}`, cases, provider, wantJSON))
				if status != 201 {
					t.Fatalf("%s: create: %d %s", label, status, created)
				}
				var createdConfig struct {
					ID string `json:"id"`
				}
				if err := json.Unmarshal([]byte(created), &createdConfig); err != nil {
					t.Fatal(err)
				}
				var createdOn bool
				if err := v.pool.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM integration_datasets AS dataset
JOIN sync_configurations AS config ON config.integration_id = dataset.integration_id
WHERE config.id = $1::uuid AND dataset.dataset_key = $2 AND dataset.is_enabled)`, createdConfig.ID, target).Scan(&createdOn); err != nil {
					t.Fatal(err)
				}
				if !createdOn || !slices.Equal(answeredTargets(t, created), wantList) {
					t.Errorf("%s: the create of %s answers %v with the %s row on = %v: the save and the create must agree", label, wantJSON,
						answeredTargets(t, created), target, createdOn)
				}

				// A save of the list GET shows keeps the target, its row and the fetch.
				status, body = v.call("PATCH", path, `{"sync_targets":`+v.shown(parent.config)+`}`)
				if status != 200 {
					t.Fatalf("%s: save of the shown list: %d %s", label, status, body)
				}
				check("after a save of the shown list", answeredTargets(t, body))

				// A save that leaves the target out drops it from the stored list.
				// It switches the blame row off; no save switches security off.
				status, body = v.call("PATCH", path, `{"sync_targets":["git"]}`)
				if status != 200 {
					t.Fatalf("%s: save without the target: %d %s", label, status, body)
				}
				if got := v.rows(parent.integration); got != wantRows {
					t.Errorf("%s: a save that keeps git and leaves the target out moved a row: [%s], want [%s] (git names the blame key; security is never switched off)", label, got, wantRows)
				}
				if v.storedList(parent.config) != `["git"]` || v.shown(parent.config) != `["git"]` {
					t.Errorf("%s: after a save without the target: stored %s, GET shows %s, want [\"git\"] for both", label,
						v.storedList(parent.config), v.shown(parent.config))
				}
			}
		}
	}
	if cases != 8 {
		t.Fatalf("%d cases ran, want 8", cases)
	}
}
