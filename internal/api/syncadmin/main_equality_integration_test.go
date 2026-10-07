//go:build integration

package syncadmin

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// equalityCase is one state of a configuration and of its integration's
// dataset rows. want is the line the save that rebuilt the rows from the
// submitted list (the code before CHAOS-8816) answers for it: the line was
// recorded from an executed run of this test on that code, never written by
// hand. here is set only where this code is known to answer another line,
// with the reason.
type equalityCase struct {
	name, provider, stored string
	on, off                []string
	// noIntegration: the configuration has no integration (and so no rows
	// and no child pinned to a source).
	noIntegration bool
	childStored   string
	want          string
	here, why     string
}

func equalityResult(result cascadeResult) string {
	return fmt.Sprintf("%d/%d/%d/%d/%s/%s", result.SyncNow, result.Backfill, result.PutRepositories, result.PatchNoList, result.Scheduled, result.Plan)
}

// equalityCases: jira, gitlab, github and linear, with PagerDuty and a
// configuration with no integration as controls. Each line reads
//
//	before=<parent readers> save=<status> stored=<parent list> after=<parent readers> | child before=<readers> stored=<child list> after=<readers>
//
// and a reader group is Sync now / backfill / PUT repositories / PATCH with
// no list / scheduled pre-mint / scheduled plan.
var equalityCases = []equalityCase{
	{name: "jira: plain", provider: "jira", stored: `["work-items"]`, on: []string{"work-items"}, childStored: `["work-items"]`,
		want: `before=202/202/400/200/minted/planned save=200 stored=["work-items"] after=202/202/400/200/minted/planned | child before=202/202/400/200/minted/planned stored=["work-items"] after=202/202/400/200/minted/planned`},
	{name: "jira: incidents row on, the list does not name the target", provider: "jira", stored: `["work-items"]`,
		on: []string{"work-items", "incidents"}, childStored: `["work-items"]`,
		want: `before=202/202/400/200/minted/ineligible save=200 stored=["work-items"] after=202/202/400/200/minted/planned | child before=202/202/400/200/minted/planned stored=["work-items"] after=202/202/400/200/minted/planned`,
		here: `before=202/202/400/200/minted/ineligible save=200 stored=["work-items"] after=202/202/400/200/minted/ineligible | child before=202/202/400/200/minted/planned stored=["work-items"] after=202/202/400/200/minted/planned`,
		why:  "the save of the shown list keeps the incidents row on (the rows own the selection), so the plan stays ineligible; the other save switched the row off"},
	{name: "jira: the list names the gated target, row on", provider: "jira", stored: `["work-items","operational"]`,
		on: []string{"work-items", "incidents"}, childStored: `["work-items"]`,
		want: `before=403/403/403/403/refused_feature_disabled/ineligible save=403 stored=["work-items","operational"] after=403/403/403/403/refused_feature_disabled/ineligible | child before=202/202/400/200/minted/planned stored=["work-items"] after=202/202/400/200/minted/planned`},
	{name: "jira: the list names the gated target, row off", provider: "jira", stored: `["work-items","operational"]`,
		on: []string{"work-items"}, off: []string{"incidents"}, childStored: `["work-items","operational"]`,
		want: `before=403/403/403/403/refused_feature_disabled/ineligible save=403 stored=["work-items","operational"] after=403/403/403/403/refused_feature_disabled/ineligible | child before=403/403/403/403/refused_feature_disabled/ineligible stored=["work-items","operational"] after=403/403/403/403/refused_feature_disabled/ineligible`},
	{name: "gitlab: plain, the child is out of step", provider: "gitlab", stored: `["git"]`, on: []string{"commits"}, childStored: `["git","prs"]`,
		want: `before=202/202/200/200/minted/planned save=200 stored=["git"] after=202/202/200/200/minted/planned | child before=202/202/200/200/minted/planned stored=["git"] after=202/202/200/200/minted/planned`},
	{name: "gitlab: incidents row on, the list does not name the target", provider: "gitlab", stored: `["git"]`,
		on: []string{"commits", "incidents"}, childStored: `["git"]`,
		want: `before=202/202/200/200/minted/ineligible save=200 stored=["git"] after=202/202/200/200/minted/planned | child before=202/202/200/200/minted/planned stored=["git"] after=202/202/200/200/minted/planned`,
		here: `before=202/202/200/200/minted/ineligible save=200 stored=["git"] after=202/202/200/200/minted/ineligible | child before=202/202/200/200/minted/planned stored=["git"] after=202/202/200/200/minted/planned`,
		why:  "the save of the shown list keeps the incidents row on (the rows own the selection), so the plan stays ineligible; the other save switched the row off"},
	{name: "gitlab: the list names the gated target, row on", provider: "gitlab", stored: `["git","incidents"]`,
		on: []string{"commits", "incidents"}, childStored: `["git","incidents"]`,
		want: `before=403/403/403/403/refused_feature_disabled/ineligible save=403 stored=["git","incidents"] after=403/403/403/403/refused_feature_disabled/ineligible | child before=403/403/403/403/refused_feature_disabled/ineligible stored=["git","incidents"] after=403/403/403/403/refused_feature_disabled/ineligible`},
	{name: "gitlab: the list names the gated target, row off", provider: "gitlab", stored: `["git","incidents"]`,
		on: []string{"commits"}, off: []string{"incidents"}, childStored: `["git"]`,
		want: `before=403/403/403/403/refused_feature_disabled/ineligible save=403 stored=["git","incidents"] after=403/403/403/403/refused_feature_disabled/ineligible | child before=202/202/200/200/minted/planned stored=["git"] after=202/202/200/200/minted/planned`},
	{name: "github: a row is on that the list does not name", provider: "github", stored: `["git"]`, on: []string{"commits", "cicd"}, childStored: `["git"]`,
		want: `before=202/202/200/200/minted/planned save=200 stored=["git"] after=202/202/200/200/minted/planned | child before=202/202/200/200/minted/planned stored=["git"] after=202/202/200/200/minted/planned`},
	{name: "github: the list names the gated target", provider: "github", stored: `["git","incidents"]`, on: []string{"commits"}, childStored: `["git"]`,
		want: `before=403/403/403/403/refused_feature_disabled/ineligible save=403 stored=["git","incidents"] after=403/403/403/403/refused_feature_disabled/ineligible | child before=202/202/200/200/minted/planned stored=["git"] after=202/202/200/200/minted/planned`},
	{name: "github: the list names a target whose rows are off", provider: "github", stored: `["git","prs"]`,
		on: []string{"commits"}, off: []string{"prs", "pr-reviews", "pr-comments"}, childStored: `["prs"]`,
		want: `before=202/202/200/200/minted/planned save=200 stored=["git","prs"] after=202/202/200/200/minted/planned | child before=202/202/200/200/minted/planned stored=["git","prs"] after=202/202/200/200/minted/planned`},
	{name: "linear: plain", provider: "linear", stored: `["work-items"]`, on: []string{"work-items"}, childStored: `["work-items"]`,
		want: `before=202/202/400/200/minted/planned save=200 stored=["work-items"] after=202/202/400/200/minted/planned | child before=202/202/400/200/minted/planned stored=["work-items"] after=202/202/400/200/minted/planned`},
	{name: "linear: the list names a gated target", provider: "linear", stored: `["work-items","incidents"]`, on: []string{"work-items"}, childStored: `["work-items"]`,
		want: `before=403/403/403/403/refused_feature_disabled/ineligible save=403 stored=["work-items","incidents"] after=403/403/403/403/refused_feature_disabled/ineligible | child before=202/202/400/200/minted/planned stored=["work-items"] after=202/202/400/200/minted/planned`},
	{name: "linear: the list names a target whose rows are off", provider: "linear", stored: `["work-items"]`,
		off: []string{"work-items", "work-item-labels"}, childStored: `[]`,
		want: `before=202/202/400/200/minted/planned save=200 stored=["work-items"] after=202/202/400/200/minted/planned | child before=202/202/400/200/minted/planned stored=["work-items"] after=202/202/400/200/minted/planned`},
	{name: "pagerduty control: the operational list", provider: "pagerduty", stored: `["operational"]`,
		on: []string{"incidents", "services"}, childStored: `["operational"]`,
		want: `before=403/403/403/403/refused_feature_disabled/ineligible save=403 stored=["operational"] after=403/403/403/403/refused_feature_disabled/ineligible | child before=403/403/403/403/refused_feature_disabled/ineligible stored=["operational"] after=403/403/403/403/refused_feature_disabled/ineligible`},
	{name: "pagerduty control: an empty list", provider: "pagerduty", stored: `[]`, on: []string{"incidents", "services"}, childStored: `[]`,
		want: `before=409/409/400/200/minted/planned save=200 stored=[] after=409/409/400/200/minted/planned | child before=409/409/400/200/minted/planned stored=[] after=409/409/400/200/minted/planned`},
	{name: "no integration control: a plain list", provider: "github", stored: `["git"]`, noIntegration: true,
		want: `before=400/400/409/200/minted/ineligible save=200 stored=["git"] after=400/400/409/200/minted/ineligible`},
	{name: "no integration control: the list names the gated target", provider: "gitlab", stored: `["git","incidents"]`, noIntegration: true,
		want: `before=403/403/403/403/refused_feature_disabled/ineligible save=403 stored=["git","incidents"] after=403/403/403/403/refused_feature_disabled/ineligible`},
}

// TestEveryReaderAndEveryStoredListEqualsTheSaveThatRebuiltTheRows is the
// equality table of CHAOS-8816 against the code before it. The org does NOT
// have the canonical-incident feature. For each state it reads every reader
// of the parent and of a child configuration (Sync now, backfill, PUT
// repositories, PATCH with no list, the scheduled pre-mint gate, the
// scheduled plan), saves the list GET shows on the parent (once plain and
// once with each base list a client can send), and reads all of it again,
// with the stored list of the parent and of the child.
//
// What it pins: the answers and the stored lists are the recorded ones, so a
// save of the shown list never changes what a reader of the stored list
// answers, a base list changes nothing, and the child holds the parent's
// stored list. Not compared: the dataset rows themselves (the rows own the
// selection: that is the change) and the list GET shows.
func TestEveryReaderAndEveryStoredListEqualsTheSaveThatRebuiltTheRows(t *testing.T) {
	v := startCascadeVenue(t, false)
	known := 0
	for _, testCase := range equalityCases {
		for _, base := range []string{``, `,"sync_targets_base":[]`, `,"sync_targets_base":["incidents","operational","git","work-items","prs","cicd"]`} {
			label := testCase.name
			if base != `` {
				label += " base" + base[len(`,"sync_targets_base":`):]
			}
			var parent, child cascadeSeeded
			if testCase.noIntegration {
				v.sequence++
				parent = cascadeSeeded{config: uuid.New(), job: uuid.New()}
				v.exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5::json, '{"schedule_cron":"0 * * * *"}'::json, true, false, now(), now())`,
					parent.config, v.org, fmt.Sprintf("no integration %d", v.sequence), testCase.provider, testCase.stored)
				v.exec(`INSERT INTO scheduled_jobs (id,org_id,name,sync_config_id,job_type,schedule_cron,timezone,status,is_running,created_at,updated_at)
VALUES ($1::uuid,$2,'job-'||$1::text,$3::uuid,'sync','0 * * * *','UTC',1,FALSE,now(),now())`, parent.job, v.org, parent.config)
			} else {
				parent = v.seed(testCase.provider, testCase.stored, testCase.on)
				for _, key := range testCase.off {
					v.exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, $4, false, '{}'::json)`,
						uuid.New(), v.org, parent.integration, key)
				}
				child = v.child(parent, testCase.provider, testCase.childStored)
			}
			line := "before=" + equalityResult(v.read(label+": parent, before", parent))
			childBefore := ""
			if !testCase.noIntegration {
				childBefore = equalityResult(v.read(label+": child, before", child))
			}
			status, _ := v.call("PATCH", "/api/v1/admin/sync-configs/"+parent.config.String(), `{"sync_targets":`+v.shown(parent.config)+base+`}`)
			line += fmt.Sprintf(" save=%d stored=%s after=%s", status, v.storedList(parent.config), equalityResult(v.read(label+": parent, after", parent)))
			if !testCase.noIntegration {
				line += fmt.Sprintf(" | child before=%s stored=%s after=%s", childBefore, v.storedList(child.config), equalityResult(v.read(label+": child, after", child)))
			}
			t.Logf("EQUALITY %s :: %s", label, line)
			want := testCase.want
			if testCase.here != "" {
				if strings.TrimSpace(testCase.why) == "" {
					t.Errorf("%s: a line that differs from the recorded one has no reason", label)
				}
				want = testCase.here
				known++
			}
			if line != want {
				t.Errorf("%s:\n got  %s\n want %s", label, line, want)
			}
		}
	}
	if known != 2*3 {
		t.Errorf("%d runs answer a line other than the recorded one, want the two named states only (3 runs each)", known)
	}
}

// TestASaveOfTheShownListKeepsARowThatIsOffOff pins the second known
// difference from the save that rebuilt the rows: the stored list names a
// target with a dataset and the rows of that target are off (the dataset
// switch turned them off). GET shows the target (the list is shown as
// stored). A save of the shown list writes no row, so the rows stay off; the
// other save switched them on. The stored list and every reader of it are
// the same either way (the equality table holds this state too). The dataset
// switch, or a remove and an add of the target, turns the rows on.
func TestASaveOfTheShownListKeepsARowThatIsOffOff(t *testing.T) {
	v := startCascadeVenue(t, false)
	for _, testCase := range []struct {
		provider, stored, target string
		on, off                  []string
	}{
		{"github", `["git","prs"]`, "prs", []string{"commits"}, []string{"prs", "pr-reviews", "pr-comments"}},
		{"gitlab", `["git","cicd"]`, "cicd", []string{"commits"}, []string{"cicd"}},
		{"jira", `["work-items"]`, "work-items", nil, []string{"work-items", "work-item-labels"}},
		{"linear", `["work-items"]`, "work-items", nil, []string{"work-items", "work-item-labels"}},
	} {
		parent := v.seed(testCase.provider, testCase.stored, testCase.on)
		for _, key := range testCase.off {
			v.exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, $4, false, '{}'::json)`,
				uuid.New(), v.org, parent.integration, key)
		}
		rows, shown := v.rows(parent.integration), v.shown(parent.config)
		if !strings.Contains(shown, `"`+testCase.target+`"`) {
			t.Errorf("%s: GET shows %s, want the stored target %q in it", testCase.provider, shown, testCase.target)
		}
		if status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+parent.config.String(), `{"sync_targets":`+shown+`}`); status != 200 {
			t.Fatalf("%s: the save of the shown list %s: %d %s", testCase.provider, shown, status, body)
		}
		if after := v.rows(parent.integration); after != rows || v.storedList(parent.config) != testCase.stored {
			t.Errorf("%s: a save of the shown list %s: rows [%s] (were [%s]), stored %s (was %s): want both as they were",
				testCase.provider, shown, after, rows, v.storedList(parent.config), testCase.stored)
		}
	}
}
