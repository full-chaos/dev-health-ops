package devhealth_test

import (
	"fmt"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type egressPolicy struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		PodSelector netpolSelector   `yaml:"podSelector"`
		PolicyTypes []string         `yaml:"policyTypes"`
		Ingress     []any            `yaml:"ingress"`
		Egress      []map[string]any `yaml:"egress"`
	} `yaml:"spec"`
}

func policyDoc(t *testing.T, docs []map[string]any, name string) egressPolicy {
	t.Helper()
	doc := authDoc(docs, "NetworkPolicy", name)
	if doc == nil {
		t.Fatalf("no NetworkPolicy %s in the render", name)
	}
	raw, err := yaml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var out egressPolicy
	if err := yaml.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func jobPodLabels(job map[string]any, instance string) map[string]string {
	labels := map[string]string{}
	for k, v := range at(job, "spec", "template", "metadata", "labels").(map[string]any) {
		labels[k] = fmt.Sprint(v)
	}
	if instance != "" {
		labels["app.kubernetes.io/instance"] = instance
	}
	return labels
}

// (a) The Job has its own policy whenever it renders, with the release-wide policy off.
func TestAuthBackfillJobHasItsOwnPolicyWithTheReleasePolicyOff(t *testing.T) {
	docs, out, err := renderAuth(t, backfillOn...)
	if err != nil {
		t.Fatalf("render: %v\n%.300s", err, out)
	}
	jobs := backfillJobs(docs)
	if len(jobs) != 1 {
		t.Fatalf("want one Job, got %d", len(jobs))
	}
	policy := policyDoc(t, docs, "a-dev-health-auth-backfill")
	if !policy.Spec.PodSelector.selects(jobPodLabels(jobs[0], "")) {
		t.Errorf("the Job's own policy does not select the Job pod: %v", policy.Spec.PodSelector)
	}
	if len(policy.Spec.PodSelector.MatchLabels) == 0 || len(policy.Spec.PodSelector.MatchExpressions) != 0 {
		t.Errorf("the Job policy selector must be the pod labels only: %+v", policy.Spec.PodSelector)
	}
	if got := strings.Join(policy.Spec.PolicyTypes, ","); got != "Ingress,Egress" || len(policy.Spec.Ingress) != 0 {
		t.Errorf("policyTypes=%s ingress=%v, want Ingress,Egress and no ingress rule (deny all)", got, policy.Spec.Ingress)
	}
}

// With the Job off there is no Job policy either.
func TestAuthBackfillPolicyFollowsTheJob(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.300s", err, out)
	}
	if authDoc(docs, "NetworkPolicy", "a-dev-health-auth-backfill") != nil {
		t.Error("the default render has a backfill NetworkPolicy")
	}
}

func releaseWidePolicies(t *testing.T) (labels map[string]string, policies []netpolDoc) {
	t.Helper()
	docs, out, err := renderAuth(t, backfillOn...)
	if err != nil {
		t.Fatalf("render: %v\n%.300s", err, out)
	}
	labels = jobPodLabels(backfillJobs(docs)[0], "np")
	return labels, renderNetpols(t, backfillOn...)
}

// (b) With networkPolicy.enabled no policy but the Job's own selects the Job pod.
func TestAuthBackfillJobIsOutsideTheReleaseWidePolicy(t *testing.T) {
	labels, policies := releaseWidePolicies(t)
	own := 0
	for _, policy := range policies {
		if !policy.Spec.PodSelector.selects(labels) {
			continue
		}
		if policy.Metadata.Name == "np-dev-health-auth-backfill" {
			own++
			continue
		}
		t.Errorf("policy %s selects the backfill Job pod", policy.Metadata.Name)
	}
	if own != 1 {
		t.Errorf("the Job's own policy selects the pod %d times, want 1", own)
	}
}

// (c) The Job's rules are exactly the auth-migrate rules: no ingress, DNS, the auth database.
func TestAuthBackfillPolicyEqualsTheAuthMigratePolicyRules(t *testing.T) {
	docs, out, err := renderAuth(t, backfillOn...)
	if err != nil {
		t.Fatalf("render: %v\n%.300s", err, out)
	}
	job := policyDoc(t, docs, "a-dev-health-auth-backfill")
	migrate := policyDoc(t, docs, "a-dev-health-auth-migrate")
	if !reflect.DeepEqual(job.Spec.Egress, migrate.Spec.Egress) {
		t.Errorf("egress differs from auth-migrate:\njob     %v\nmigrate %v", job.Spec.Egress, migrate.Spec.Egress)
	}
	if len(job.Spec.Egress) != 2 {
		t.Fatalf("egress rules = %d, want 2 (DNS, database)", len(job.Spec.Egress))
	}
	if !reflect.DeepEqual(job.Spec.PolicyTypes, migrate.Spec.PolicyTypes) || len(job.Spec.Ingress) != len(migrate.Spec.Ingress) {
		t.Errorf("policyTypes or ingress differ from auth-migrate")
	}
	if fmt.Sprint(job.Spec.Egress[0]["ports"]) != "[map[port:53 protocol:UDP] map[port:53 protocol:TCP]]" {
		t.Errorf("the first rule is not DNS 53 UDP+TCP: %v", job.Spec.Egress[0])
	}
	if !strings.Contains(fmt.Sprint(job.Spec.Egress[1]["to"]), "auth-postgres") {
		t.Errorf("the second rule is not the configured auth database: %v", job.Spec.Egress[1])
	}
}

// (d) Dropping auth-backfill from the exclusion list would make (b) fail.
func TestAuthBackfillExclusionIsWhatKeepsItOutOfTheReleasePolicy(t *testing.T) {
	labels, policies := releaseWidePolicies(t)
	var found bool
	for _, policy := range policies {
		for i, expression := range policy.Spec.PodSelector.MatchExpressions {
			if expression.Operator != "NotIn" || !hasString(expression.Values, "auth-migrate") {
				continue
			}
			found = true
			if !hasString(expression.Values, "auth-backfill") {
				t.Fatalf("the release-wide exclusion list lacks auth-backfill: %v", expression.Values)
			}
			var kept []string
			for _, value := range expression.Values {
				if value != "auth-backfill" {
					kept = append(kept, value)
				}
			}
			policy.Spec.PodSelector.MatchExpressions[i].Values = kept
			if !policy.Spec.PodSelector.selects(labels) {
				t.Error("without the exclusion the release-wide policy would not select the Job: the test cannot fail")
			}
		}
	}
	if !found {
		t.Fatal("no release-wide policy with an exclusion list")
	}
}

func hasString(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

func renderRelease(t *testing.T, release string, args ...string) []map[string]any {
	t.Helper()
	out, err := exec.Command("helm", append([]string{"template", release, "."}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("render: %v\n%.300s", err, out)
	}
	var docs []map[string]any
	decoder := yaml.NewDecoder(strings.NewReader(string(out)))
	for {
		var doc map[string]any
		if decoder.Decode(&doc) != nil {
			break
		}
		if doc != nil {
			docs = append(docs, doc)
		}
	}
	return docs
}

func TestAuthBackfillNamesFitTheLimitForALongRelease(t *testing.T) {
	for _, release := range []string{strings.Repeat("r", 53), strings.Repeat("r", 39), strings.Repeat("r", 25) + "-" + strings.Repeat("q", 27)} {
		docs := renderRelease(t, release, backfillOn...)
		jobs := backfillJobs2(docs)
		if len(jobs) != 1 {
			t.Fatalf("release %d chars: %d Jobs", len(release), len(jobs))
		}
		for _, doc := range append(jobs, backfillPolicies(docs)...) {
			name := jobName(doc)
			if len(name) > 63 || strings.HasSuffix(name, "-") || strings.Contains(name, "--") {
				t.Errorf("release of %d chars: name %q (%d chars) is not a valid object name", len(release), name, len(name))
			}
		}
	}
}

func backfillJobs2(docs []map[string]any) []map[string]any {
	var out []map[string]any
	for _, doc := range docs {
		if doc["kind"] == "Job" && strings.Contains(jobName(doc), "-auth-backfill-") {
			out = append(out, doc)
		}
	}
	return out
}

func backfillPolicies(docs []map[string]any) []map[string]any {
	var out []map[string]any
	for _, doc := range docs {
		if doc["kind"] == "NetworkPolicy" && strings.HasSuffix(jobName(doc), "-auth-backfill") {
			out = append(out, doc)
		}
	}
	return out
}

// The Job name follows the whole rendered pod template: each input class changes it.
func TestAuthBackfillJobNameFollowsEveryPodTemplateInput(t *testing.T) {
	_, first := renderBackfill(t, backfillOn)
	cases := [][]string{
		{"--set", "authService.database.secretName=auth-db-next"},
		{"--set", "authService.database.secretKey=OTHER_KEY"},
		{"--set", "authService.database.schema=authx"},
		{"--set-json", `authService.backfill.extraEnv=[{"name":"GOGC","value":"50"}]`},
		{"--set", "authService.backfill.resources.limits.memory=512Mi"},
		{"--set", "goApi.image.pullPolicy=Always"},
		{"--set", "goApi.image.tag=sha-other"},
		{"--set", "authService.backfill.runId=r2"},
		{"--set", "authService.backfill.verb=users-backfill"},
		{"--set", "authService.backfill.dryRun=false"},
		{"--set", "authService.backfill.batch=100"},
		{"--set-json", `global.imagePullSecrets=[{"name":"pull"}]`},
	}
	seen := map[string]string{jobName(first): "base"}
	for _, change := range cases {
		args := append(append([]string{}, backfillOn...), change...)
		docs, out, err := renderAuth(t, args...)
		if err != nil {
			t.Errorf("%v: render failed: %v\n%.200s", change, err, out)
			continue
		}
		jobs := backfillJobs(docs)
		if len(jobs) != 1 {
			t.Errorf("%v: %d Jobs", change, len(jobs))
			continue
		}
		name := jobName(jobs[0])
		if prior, dup := seen[name]; dup {
			t.Errorf("%v did not change the Job name (same as %s): %s", change, prior, name)
		}
		seen[name] = fmt.Sprint(change)
	}
}
