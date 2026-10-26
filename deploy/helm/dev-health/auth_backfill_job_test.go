package devhealth_test

import (
	"fmt"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var backfillOn = append(append([]string{}, authBase...),
	"--set", "authService.backfill.enabled=true",
	"--set", "authService.backfill.verb=orgs-backfill",
	"--set", "authService.backfill.runId=r1")

func withSets(base []string, sets ...string) []string {
	out := append([]string{}, base...)
	for _, s := range sets {
		out = append(out, "--set", s)
	}
	return out
}

func backfillJobs(docs []map[string]any) []map[string]any {
	var jobs []map[string]any
	for _, doc := range docs {
		meta, _ := doc["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		if doc["kind"] == "Job" && strings.HasPrefix(name, "a-dev-health-auth-backfill-") {
			jobs = append(jobs, doc)
		}
	}
	return jobs
}

func renderBackfill(t *testing.T, args []string) (service, job map[string]any) {
	t.Helper()
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	jobs := backfillJobs(docs)
	if len(jobs) != 1 {
		t.Fatalf("want one backfill Job, got %d", len(jobs))
	}
	return authDoc(docs, "Deployment", "a-dev-health-auth-service"), jobs[0]
}

func cloneDoc(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	raw, err := yaml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := yaml.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func setContainerEnv(doc map[string]any, name, value string) {
	container := at(doc, "spec", "template", "spec", "containers", 0).(map[string]any)
	env, _ := container["env"].([]any)
	for _, entry := range env {
		if at(entry, "name") == name {
			entry.(map[string]any)["value"] = value
			return
		}
	}
	container["env"] = append(env, map[string]any{"name": name, "value": value})
}

// ownedSwitchesAgree is the equality the render tests rely on: the service and
// the Job carry byte-equal AUTH_USERS_OWNED and AUTH_ORGS_OWNED (both unset
// counts as equal).
func ownedSwitchesAgree(t *testing.T, service, job map[string]any) error {
	t.Helper()
	for _, name := range []string{"AUTH_USERS_OWNED", "AUTH_ORGS_OWNED"} {
		sv, sset := containerEnvValue(t, service, name)
		jv, jset := containerEnvValue(t, job, name)
		if sv != jv || sset != jset {
			return fmt.Errorf("%s: auth-service %q (set=%v), Job %q (set=%v)", name, sv, sset, jv, jset)
		}
	}
	return nil
}

func TestAuthOrgsOwnedDefaultUnset(t *testing.T) {
	service, job := renderBackfill(t, backfillOn)
	for _, doc := range []map[string]any{service, job} {
		if value, set := containerEnvValue(t, doc, "AUTH_ORGS_OWNED"); set {
			t.Errorf("default render sets AUTH_ORGS_OWNED=%q; want it unset", value)
		}
		if value, set := containerEnvValue(t, doc, "AUTH_USERS_OWNED"); set {
			t.Errorf("default render sets AUTH_USERS_OWNED=%q; want it unset", value)
		}
	}
}

// The owned switches come from authService.phase alone: auth-service and the Job agree in every rendered phase
// (4 users owned included), and phase 5 (orgs owned) is refused until its own public-table writer leaf.
func TestAuthOrgsOwnedOneKeyAllConsumers(t *testing.T) {
	for _, verb := range []string{"users-backfill", "orgs-backfill"} {
		for _, phase := range []string{"1", "2", "3", "4"} {
			t.Run(verb+"/phase "+phase, func(t *testing.T) {
				args := withSets(append(append([]string{}, backfillOn...), phase3Secrets...), "authService.backfill.verb="+verb, "authService.phase="+phase)
				if phase != "1" {
					args = append(append(args, sessionReaders...), sessionSuccessor...)
				}
				if phase == "4" {
					args = append(withSets(args, "authService.usersOwnedStage=owned", "goApi.enabled=true"), authTrusted...)
				}
				service, job := renderBackfill(t, args)
				if err := ownedSwitchesAgree(t, service, job); err != nil {
					t.Fatal(err)
				}
			})
		}
		for _, phase := range []string{"5"} {
			t.Run(verb+"/phase "+phase+" refused", func(t *testing.T) {
				_, out, err := renderAuth(t, withSets(backfillOn, "authService.backfill.verb="+verb, "authService.phase="+phase)...)
				if err == nil || !strings.Contains(out, "phase 5 (orgs owned) needs its own public-table writer leaf") {
					t.Fatalf("phase %s rendered: err=%v out=%.300s", phase, err, out)
				}
			})
		}
	}
}

// Each consumer, read a different value or a literal, makes the equality fail.
func TestAuthOrgsOwnedEqualityFailsForEachConsumer(t *testing.T) {
	service, job := renderBackfill(t, backfillOn)
	if err := ownedSwitchesAgree(t, service, job); err != nil {
		t.Fatalf("baseline: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(service, job map[string]any)
	}{
		{"N1 auth-service reads a literal", func(s, _ map[string]any) { setContainerEnv(s, "AUTH_ORGS_OWNED", "false") }},
		{"N2 Job reads a literal", func(_, j map[string]any) { setContainerEnv(j, "AUTH_ORGS_OWNED", "false") }},
		{"N3 Job reads a different key for users", func(_, j map[string]any) { setContainerEnv(j, "AUTH_USERS_OWNED", "true") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, j := cloneDoc(t, service), cloneDoc(t, job)
			tc.mutate(s, j)
			if ownedSwitchesAgree(t, s, j) == nil {
				t.Fatal("equality did not catch the mismatch")
			}
		})
	}
}

// A stray key is no second source: only authService.phase feeds the switch.
func TestAuthOrgsOwnedStrayKeyIsIgnoredByTheJob(t *testing.T) {
	service, job := renderBackfill(t, withSets(backfillOn, "authService.backfill.orgsOwned=true"))
	for name, doc := range map[string]map[string]any{"auth-service": service, "Job": job} {
		if value, set := containerEnvValue(t, doc, "AUTH_ORGS_OWNED"); set {
			t.Errorf("%s: a stray authService.backfill.orgsOwned rendered AUTH_ORGS_OWNED=%q", name, value)
		}
	}
}

func TestAuthOrgsOwnedCannotBeSetThroughExtraEnv(t *testing.T) {
	for _, key := range []string{"authService.extraEnv", "authService.backfill.extraEnv"} {
		for _, name := range []string{"AUTH_ORGS_OWNED", "AUTH_USERS_OWNED"} {
			t.Run(key+"/"+name, func(t *testing.T) {
				args := append(append([]string{}, backfillOn...), "--set-json", fmt.Sprintf(`%s=[{"name":%q,"value":"true"}]`, key, " "+strings.ToLower(name)))
				_, out, err := renderAuth(t, args...)
				if err == nil || !strings.Contains(out, key+" must not set") {
					t.Fatalf("extraEnv %s rendered: err=%v out=%.300s", name, err, out)
				}
			})
		}
	}
}

func TestAuthBackfillJobOffByDefault(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	if jobs := backfillJobs(docs); len(jobs) != 0 {
		t.Fatalf("default render has %d backfill Jobs", len(jobs))
	}
}

func TestAuthBackfillJobVerbEnum(t *testing.T) {
	for _, verb := range []string{"", "users-reconcile", "orgs-backfill --x"} {
		_, out, err := renderAuth(t, withSets(backfillOn, "authService.backfill.verb="+verb)...)
		if err == nil || !strings.Contains(out, "authService.backfill.verb") {
			t.Errorf("verb %q rendered: err=%v out=%.200s", verb, err, out)
		}
	}
}

func TestAuthBackfillJobBatchIsBounded(t *testing.T) {
	for _, batch := range []string{"0", "10001", "-1"} {
		_, out, err := renderAuth(t, withSets(backfillOn, "authService.backfill.batch="+batch)...)
		if err == nil || !strings.Contains(out, "authService.backfill.batch") {
			t.Errorf("batch %s rendered: err=%v out=%.200s", batch, err, out)
		}
	}
}

func TestAuthBackfillJobNeedsRunIdAndAuthService(t *testing.T) {
	noRun := append(append([]string{}, authBase...), "--set", "authService.backfill.enabled=true", "--set", "authService.backfill.verb=orgs-backfill")
	if _, out, err := renderAuth(t, noRun...); err == nil || !strings.Contains(out, "authService.backfill.runId") {
		t.Errorf("enabled without runId rendered: err=%v out=%.200s", err, out)
	}
	off := []string{"--set", "authService.backfill.enabled=true", "--set", "authService.backfill.verb=orgs-backfill", "--set", "authService.backfill.runId=r1"}
	if _, out, err := renderAuth(t, off...); err == nil || !strings.Contains(out, "authService.phase >= 1") {
		t.Errorf("Job without auth-service rendered: err=%v out=%.200s", err, out)
	}
}

func TestAuthBackfillJobDryRunDefaultTrue(t *testing.T) {
	_, job := renderBackfill(t, backfillOn)
	args := fmt.Sprint(at(job, "spec", "template", "spec", "containers", 0, "args"))
	if !strings.Contains(args, "orgs-backfill") || !strings.Contains(args, "--dry-run") {
		t.Errorf("default args %s: want orgs-backfill with --dry-run", args)
	}
	_, job = renderBackfill(t, withSets(backfillOn, "authService.backfill.dryRun=false"))
	if args = fmt.Sprint(at(job, "spec", "template", "spec", "containers", 0, "args")); strings.Contains(args, "--dry-run") {
		t.Errorf("dryRun=false args %s still carry --dry-run", args)
	}
}

func TestAuthBackfillJobUsesTheAuthServiceSecretReference(t *testing.T) {
	service, job := renderBackfill(t, backfillOn)
	ref := func(doc map[string]any) string {
		container := at(doc, "spec", "template", "spec", "containers", 0)
		for _, entry := range at(container, "env").([]any) {
			if at(entry, "name") == "AUTH_DATABASE_URI" {
				if at(entry, "value") != nil {
					t.Fatal("AUTH_DATABASE_URI is a literal value")
				}
				return fmt.Sprint(at(entry, "valueFrom", "secretKeyRef"))
			}
		}
		t.Fatal("AUTH_DATABASE_URI missing")
		return ""
	}
	if a, b := ref(service), ref(job); a != b {
		t.Errorf("secretKeyRef differs: auth-service %s, Job %s", a, b)
	}
	for _, schema := range []string{"", "authx"} {
		args := backfillOn
		if schema != "" {
			args = withSets(backfillOn, "authService.database.schema="+schema)
		}
		svc, j := renderBackfill(t, args)
		want := schema
		if want == "" {
			want = "auth"
		}
		jv, jset := containerEnvValue(t, j, "AUTH_DATABASE_SCHEMA")
		serviceArgs := fmt.Sprint(at(svc, "spec", "template", "spec", "containers", 0, "args"))
		if !jset || jv != want || !strings.Contains(serviceArgs, "--database-schema="+want) {
			t.Errorf("schema %q: Job AUTH_DATABASE_SCHEMA %q (set=%v), auth-service args %s, want both %q", schema, jv, jset, serviceArgs, want)
		}
	}
}

func TestAuthBackfillJobUsesTheDhoImage(t *testing.T) {
	_, job := renderBackfill(t, withSets(backfillOn, "goApi.image.repository=example.test/dho", "goApi.image.tag=sha-abc"))
	if image := at(job, "spec", "template", "spec", "containers", 0, "image"); image != "example.test/dho:sha-abc" {
		t.Errorf("image = %v, want the goApi dho image", image)
	}
}

func TestAuthBackfillJobRunsOnce(t *testing.T) {
	_, job := renderBackfill(t, backfillOn)
	meta, _ := job["metadata"].(map[string]any)
	if ann, _ := meta["annotations"].(map[string]any); ann["helm.sh/hook"] != nil {
		t.Errorf("Job is a hook: %v", ann)
	}
	if v := at(job, "spec", "backoffLimit"); v != 0 {
		t.Errorf("backoffLimit = %v, want 0", v)
	}
	if v := at(job, "spec", "ttlSecondsAfterFinished"); v == nil || v == 0 {
		t.Errorf("ttlSecondsAfterFinished = %v, want set", v)
	}
	if v := at(job, "spec", "template", "spec", "restartPolicy"); v != "Never" {
		t.Errorf("restartPolicy = %v", v)
	}
}

func jobName(job map[string]any) string {
	meta, _ := job["metadata"].(map[string]any)
	name, _ := meta["name"].(string)
	return name
}

func TestAuthBackfillJobNameIsStableAndFollowsItsInputs(t *testing.T) {
	_, first := renderBackfill(t, backfillOn)
	_, again := renderBackfill(t, backfillOn)
	if jobName(first) != jobName(again) {
		t.Fatalf("same inputs, two names: %s %s", jobName(first), jobName(again))
	}
	for _, change := range []string{"authService.backfill.dryRun=false", "authService.backfill.verb=users-backfill", "authService.backfill.batch=100", "authService.backfill.runId=r2", "goApi.image.tag=sha-other"} {
		_, other := renderBackfill(t, withSets(backfillOn, change))
		if jobName(other) == jobName(first) {
			t.Errorf("%s did not change the Job name %s", change, jobName(first))
		}
	}
}

// legacy-link-purge deletes go-api's link-token rows: the chart renders it only where users are owned
// (authService.phase >= 4) and with a slug run id.
func TestAuthBackfillJobLegacyLinkPurgeNeedsUsersOwnedAndASlugRunID(t *testing.T) {
	for _, phase := range []string{"1", "3"} {
		args := withSets(append(append([]string{}, backfillOn...), phase3Secrets...), "authService.backfill.verb=legacy-link-purge", "authService.phase="+phase)
		if phase != "1" {
			args = append(append(args, sessionReaders...), sessionSuccessor...)
		}
		_, out, err := renderAuth(t, args...)
		if err == nil || !strings.Contains(out, "legacy-link-purge needs users owned") {
			t.Errorf("phase %s: err=%v out=%.300s", phase, err, out)
		}
	}
	for _, runID := range []string{"Roll 1", "R1", "-r1", strings.Repeat("a", 64)} {
		_, out, err := renderAuth(t, withSets(backfillOn, "authService.backfill.verb=legacy-link-purge", "authService.backfill.runId="+runID)...)
		if err == nil || !strings.Contains(out, "must be a lowercase slug") {
			t.Errorf("run id %q: err=%v out=%.300s", runID, err, out)
		}
	}
}

// Where users are owned (phase 4) the backfill Job runs `dho auth legacy-link-purge` with the slug run id and
// carries AUTH_USERS_OWNED=true; the Job is the one purge step, so a render without it must fail this test.
func TestAuthBackfillJobLegacyLinkPurgeRendersWhereUsersAreOwned(t *testing.T) {
	for _, runID := range []string{"r1", "roll-1.a_b", strings.Repeat("a", 63)} {
		t.Run(runID, func(t *testing.T) {
			args := withSets(append(append([]string{}, backfillOn...), phase3Secrets...),
				"authService.backfill.verb=legacy-link-purge", "authService.backfill.runId="+runID, "authService.phase=4", "authService.usersOwnedStage=owned", "goApi.enabled=true")
			args = append(append(append(args, sessionReaders...), sessionSuccessor...), authTrusted...)
			service, job := renderBackfill(t, args)
			got := jobArgs(t, job)
			if len(got) < 4 || got[0] != "auth" || got[1] != "legacy-link-purge" || !strings.HasPrefix(got[2], "--batch=") {
				t.Fatalf("purge Job args = %v, want auth legacy-link-purge --batch=N ...", got)
			}
			if !containsString(got, "--run-id="+runID) {
				t.Errorf("purge Job args = %v, want --run-id=%s", got, runID)
			}
			if value, set := containerEnvValue(t, job, "AUTH_USERS_OWNED"); !set || value != "true" {
				t.Errorf("purge Job AUTH_USERS_OWNED=%q set=%v; want true", value, set)
			}
			if err := ownedSwitchesAgree(t, service, job); err != nil {
				t.Error(err)
			}
		})
	}
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
