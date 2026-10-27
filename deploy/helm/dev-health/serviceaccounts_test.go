package devhealth_test

import (
	"fmt"
	"strings"
	"testing"
)

var saArgs = append(append([]string{}, authBase...),
	"--set", "goApi.enabled=true", "--set", "queryApi.enabled=true",
	"--set", "authService.migrate.sessionReaders.goApi=devhealth_api",
	"--set", "postgresql.enabled=true", "--set", "redis.enabled=true",
	"--set", "migrations.hook.enabled=true",
	"--set", "migrations.hook.provisionRoles.enabled=true",
	"--set", "migrations.hook.riverMigrate.enabled=true",
	"--set", "migrations.hook.riverMigrate.image=ghcr.io/full-chaos/dho:sha-0123456789ab",
	"--set", "migrations.hook.routeActivate.enabled=true",
	"--set", "migrations.hook.routeActivate.image=ghcr.io/full-chaos/dho:sha-0123456789ab",
	"--set", "goWorkers.pgbouncer.secret.create=true",
	"--set-json", `global.imagePullSecrets=[{"name":"ghcr-pull"}]`,
)

type saPod struct {
	kind, name string
	spec       map[string]any
}

func saRender(t *testing.T, extra ...string) (pods []saPod, accounts map[string]map[string]any) {
	t.Helper()
	docs, out, err := renderAuth(t, append(append([]string{}, saArgs...), extra...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.800s", err, out)
	}
	accounts = map[string]map[string]any{}
	for _, d := range docs {
		kind, _ := d["kind"].(string)
		name := toString(at(d, "metadata", "name"))
		switch kind {
		case "ServiceAccount":
			accounts[name] = d
		case "Deployment", "StatefulSet", "Job":
			if spec, ok := at(d, "spec", "template", "spec").(map[string]any); ok {
				pods = append(pods, saPod{kind, name, spec})
			}
		}
	}
	if len(pods) < 12 {
		t.Fatalf("only %d pod specs rendered; the fixture is too small", len(pods))
	}
	return pods, accounts
}

func TestWorkloadServiceAccountsDistinct(t *testing.T) {
	pods, accounts := saRender(t)
	owner := map[string]string{}
	var checked []string
	for _, p := range pods {
		if p.kind != "Deployment" || strings.Contains(p.name, "-pgbouncer-") || strings.HasSuffix(p.name, "-valkey") {
			continue
		}
		sa := toString(p.spec["serviceAccountName"])
		if sa == "" || sa == "default" {
			t.Errorf("%s %s runs as the namespace default ServiceAccount", p.kind, p.name)
			continue
		}
		if prev, dup := owner[sa]; dup {
			t.Errorf("%s and %s share ServiceAccount %s", prev, p.name, sa)
		}
		owner[sa] = p.name
		checked = append(checked, p.name)
		if accounts[sa] == nil {
			t.Errorf("%s names ServiceAccount %s, which the chart does not render", p.name, sa)
		}
	}
	for _, want := range []string{"-web", "-query-api", "-go-api", "-auth-service", "-go-reconciler"} {
		found := false
		for _, n := range checked {
			found = found || strings.HasSuffix(n, want)
		}
		if !found {
			t.Errorf("no workload Deployment ending %s was checked", want)
		}
	}
}

func TestWorkloadPodsDoNotMountTheServiceAccountToken(t *testing.T) {
	pods, accounts := saRender(t)
	for _, job := range []string{"-migrate", "-auth-migrate", "-auth-grants", "-provision-roles", "-river-migrate", "-route-activate"} {
		found := false
		for _, p := range pods {
			found = found || (p.kind == "Job" && strings.HasSuffix(p.name, job))
		}
		if !found {
			t.Errorf("no hook Job ending %s rendered; the fixture lost it", job)
		}
	}
	for _, p := range pods {
		if v, ok := p.spec["automountServiceAccountToken"].(bool); !ok || v {
			t.Errorf("%s %s does not set automountServiceAccountToken: false", p.kind, p.name)
		}
	}
	for name, sa := range accounts {
		if v, ok := sa["automountServiceAccountToken"].(bool); !ok || v {
			t.Errorf("ServiceAccount %s does not set automountServiceAccountToken: false", name)
		}
	}
}

func TestEveryChartImagePodResolvesAPullSecret(t *testing.T) {
	pods, accounts := saRender(t)
	checked := 0
	for _, p := range pods {
		if !podHasGHCRImage(p.spec) {
			continue
		}
		checked++
		if !hasPull(p.spec) && !hasPull(accounts[toString(p.spec["serviceAccountName"])]) {
			t.Errorf("%s %s pulls a ghcr image and no pull secret reaches it (pod spec or ServiceAccount)", p.kind, p.name)
		}
	}
	if checked < 6 {
		t.Fatalf("only %d ghcr pods checked", checked)
	}
}

func podHasGHCRImage(spec map[string]any) bool {
	for _, group := range []string{"containers", "initContainers"} {
		for _, c := range asList(spec[group]) {
			if strings.HasPrefix(toString(at(c, "image")), "ghcr.io/") {
				return true
			}
		}
	}
	return false
}

func hasPull(v map[string]any) bool {
	if v == nil {
		return false
	}
	for _, s := range asList(v["imagePullSecrets"]) {
		if toString(at(s, "name")) == "ghcr-pull" {
			return true
		}
	}
	return false
}

func TestOldSharedServiceAccountKeyIsRefused(t *testing.T) {
	for _, set := range []string{"serviceAccount.name=shared", "serviceAccount.create=false"} {
		_, out, err := renderAuth(t, append(append([]string{}, saArgs...), "--set", set)...)
		if err == nil || !strings.Contains(out, "serviceAccount") {
			t.Errorf("--set %s must refuse the render naming serviceAccount; got err=%v", set, fmt.Sprint(err))
		}
	}
}
