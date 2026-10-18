package devhealth_test

import (
	"os/exec"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-4886: the Auth Control Plane is deployed DORMANT. These tests pin the
// parts of the chart that carry its security posture: secrets by reference only,
// probes on the operator port, the signing-key staging that keeps the custody
// contract (mode 0400, uid 65532) satisfiable on Kubernetes, the migration as a
// separate hook Job that the long-running pod never sees, and a NetworkPolicy
// that is the pods' whole boundary.

var authBase = []string{
	"--set", "authService.enabled=true",
	"--set", "authService.database.secretName=auth-db",
	"--set", "authService.signingKey.secretName=auth-key",
	"--set", "authService.signingKey.keyId=auth-2026-10",
	"--set", "authService.migrate.secretName=auth-migrate-db",
	"--set", "authService.migrate.secretKey=MIGRATION_URI",
	"--set-json", `authService.networkPolicy.egress.database.to=[{"podSelector":{"matchLabels":{"app":"auth-postgres"}}}]`,
}

var authNetworkPolicy = []string{
	"--set", "networkPolicy.enabled=true",
	"--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24",
}

func renderAuth(t *testing.T, args ...string) ([]map[string]any, string, error) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	argv := append([]string{"template", "a", "."}, args...)
	out, err := exec.Command("helm", argv...).CombinedOutput()
	if err != nil {
		return nil, string(out), err
	}
	var docs []map[string]any
	decoder := yaml.NewDecoder(strings.NewReader(string(out)))
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			break
		}
		if doc != nil {
			docs = append(docs, doc)
		}
	}
	return docs, string(out), nil
}

func authDoc(docs []map[string]any, kind, name string) map[string]any {
	for _, doc := range docs {
		meta, _ := doc["metadata"].(map[string]any)
		if doc["kind"] == kind && meta["name"] == name {
			return doc
		}
	}
	return nil
}

func at(v any, path ...any) any {
	for _, key := range path {
		switch k := key.(type) {
		case string:
			m, _ := v.(map[string]any)
			v = m[k]
		case int:
			l, _ := v.([]any)
			if k >= len(l) {
				return nil
			}
			v = l[k]
		}
	}
	return v
}

func envNames(container any) map[string]bool {
	names := map[string]bool{}
	env, _ := at(container, "env").([]any)
	for _, e := range env {
		if name, ok := at(e, "name").(string); ok {
			names[name] = true
		}
	}
	return names
}

func TestAuthServiceIsOffByDefault(t *testing.T) {
	docs, out, err := renderAuth(t)
	if err != nil {
		t.Fatalf("default render failed: %v\n%.400s", err, out)
	}
	for _, doc := range docs {
		meta, _ := doc["metadata"].(map[string]any)
		if name, _ := meta["name"].(string); strings.Contains(name, "auth-service") || strings.Contains(name, "auth-migrate") {
			t.Errorf("the default render carries %v %s: the service must be opt-in", doc["kind"], name)
		}
	}
}

func TestAuthServiceDeployment(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	deployment := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if deployment == nil {
		t.Fatal("no auth-service Deployment")
	}
	pod := at(deployment, "spec", "template", "spec")
	container := at(pod, "containers", 0)

	// Probes use the operator port, never the API port.
	for _, probe := range []struct{ field, path string }{{"livenessProbe", "/healthz"}, {"readinessProbe", "/readyz"}} {
		if at(container, probe.field, "httpGet", "path") != probe.path || at(container, probe.field, "httpGet", "port") != "operator" {
			t.Errorf("%s is not %s on the operator port: %v", probe.field, probe.path, at(container, probe.field))
		}
	}

	// Hardened pod and container.
	if at(container, "securityContext", "readOnlyRootFilesystem") != true || at(container, "securityContext", "runAsNonRoot") != true ||
		at(container, "securityContext", "allowPrivilegeEscalation") != false {
		t.Errorf("container securityContext is not hardened: %v", at(container, "securityContext"))
	}
	if at(pod, "automountServiceAccountToken") != false {
		t.Error("the pod mounts a service account token it does not use")
	}

	// Secrets by reference. The runtime DSN is a secretKeyRef; no key material and no
	// migration DSN reaches this container; the direct-value signing variable is absent.
	names := envNames(container)
	if !names["AUTH_DATABASE_URI"] {
		t.Error("AUTH_DATABASE_URI is not set")
	}
	for _, forbidden := range []string{"AUTH_SIGNING_KEY", "AUTH_MIGRATION_DATABASE_URI"} {
		if names[forbidden] {
			t.Errorf("the long-running container carries %s", forbidden)
		}
	}
	if at(container, "env", 0, "valueFrom", "secretKeyRef", "name") != "auth-db" {
		t.Errorf("the runtime DSN does not come from the named Secret: %v", at(container, "env", 0))
	}
	for _, kind := range []string{"Secret", "ConfigMap"} {
		for _, doc := range docs {
			meta, _ := doc["metadata"].(map[string]any)
			if name, _ := meta["name"].(string); doc["kind"] == kind && strings.Contains(name, "auth-") {
				t.Errorf("the chart renders a %s %s for the auth service: secrets are referenced, never created", kind, name)
			}
		}
	}

	// The signing key: the Secret is mounted 0440 (octal 288) for the init container
	// only; the service container sees the staged 0400 copy, read-only.
	var source map[string]any
	for _, v := range at(pod, "volumes").([]any) {
		if at(v, "name") == "signing-key-source" {
			source = v.(map[string]any)
		}
	}
	if source == nil || at(source, "secret", "secretName") != "auth-key" || at(source, "secret", "defaultMode") != 288 {
		t.Errorf("the signing-key Secret volume is not mounted 0440 from auth-key: %v", source)
	}
	init := at(pod, "initContainers", 0)
	if joined := strings.Join(toStrings(at(init, "command")), " "); joined != "install -m 0400 /in/signing-key.pem /out/signing-key.pem" {
		t.Errorf("the staging init container does not install the key at mode 0400: %q", joined)
	}
	if at(init, "securityContext", "runAsUser") != 65532 {
		t.Error("the staging init container must run as uid 65532 so the staged key is owned by the service's uid")
	}
	for _, m := range at(container, "volumeMounts").([]any) {
		if at(m, "name") == "signing-key-source" {
			t.Error("the service container mounts the raw Secret volume")
		}
		if at(m, "name") == "signing-key" && at(m, "readOnly") != true {
			t.Error("the staged key is mounted writable")
		}
	}
	if at(pod, "securityContext", "fsGroup") != 65532 {
		t.Error("fsGroup 65532 is what lets the init container read the 0440 Secret as a non-root uid")
	}
	args := strings.Join(toStrings(at(container, "args")), " ")
	for _, want := range []string{"--signing-key-file=/run/auth/signing/signing-key.pem", "--signing-key-id=auth-2026-10", "--operator-http-addr=:8096", "--http-addr=:8095"} {
		if !strings.Contains(args, want) {
			t.Errorf("args lack %s: %s", want, args)
		}
	}
}

func TestAuthMigrateIsASeparateHookJob(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	job := authDoc(docs, "Job", "a-dev-health-auth-migrate")
	if job == nil {
		t.Fatal("no auth-migrate Job")
	}
	if hook := at(job, "metadata", "annotations", "helm.sh/hook"); hook != "pre-install,pre-upgrade" {
		t.Errorf("auth-migrate is not a pre-install/pre-upgrade hook: %v", hook)
	}
	container := at(job, "spec", "template", "spec", "containers", 0)
	names := envNames(container)
	if !names["AUTH_MIGRATION_DATABASE_URI"] || !names["AUTH_RUNTIME_DATABASE_ROLE"] {
		t.Errorf("the Job lacks its migration DSN or runtime role: %v", names)
	}
	if names["AUTH_DATABASE_URI"] {
		t.Error("the migration Job carries the runtime login's DSN")
	}
	if at(container, "securityContext", "readOnlyRootFilesystem") != true {
		t.Error("the migration container is not read-only")
	}
	// The Deployment must not run the migration at startup: its container has no
	// migration setting at all (asserted above) and the Job is the only DDL path.
	if at(job, "spec", "template", "spec", "serviceAccountName") != nil {
		t.Error("a pre-install hook Job names the release ServiceAccount, which does not exist yet on a fresh install")
	}
	if at(job, "spec", "template", "spec", "restartPolicy") != "Never" {
		t.Error("the Job restarts on failure instead of failing the release")
	}
}

func TestAuthNetworkPolicyIsTheWholeBoundary(t *testing.T) {
	docs, out, err := renderAuth(t, append(append([]string{}, authBase...), authNetworkPolicy...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}

	// The release-wide policy admits every pod of the namespace and all HTTPS egress, so the
	// auth pods must be excluded from it.
	release := authDoc(docs, "NetworkPolicy", "a-dev-health")
	if release == nil {
		t.Fatal("no release-wide NetworkPolicy")
	}
	excluded := false
	for _, expr := range at(release, "spec", "podSelector", "matchExpressions").([]any) {
		if at(expr, "key") == "app.kubernetes.io/component" && at(expr, "operator") == "NotIn" {
			values := toStrings(at(expr, "values"))
			excluded = contains(values, "auth-service") && contains(values, "auth-migrate")
		}
	}
	if !excluded {
		t.Error("the release-wide policy still selects the auth pods: it would admit every in-namespace pod and all HTTPS egress")
	}

	policy := authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service")
	if policy == nil {
		t.Fatal("no auth-service NetworkPolicy")
	}
	// Nothing consumes the service yet: with no allowedFrom / metricsFrom there is NO ingress
	// rule (an absent or empty `from` would admit every source).
	if ingress, _ := at(policy, "spec", "ingress").([]any); len(ingress) != 0 {
		t.Errorf("ingress is not empty by default: %v", ingress)
	}
	// Egress: DNS and the database only (no key custody peers listed).
	egress := at(policy, "spec", "egress").([]any)
	if len(egress) != 2 {
		t.Fatalf("egress has %d rules, want DNS + database: %v", len(egress), egress)
	}
	if at(egress, 1, "ports", 0, "port") != 5432 || at(egress, 1, "to", 0, "podSelector", "matchLabels", "app") != "auth-postgres" {
		t.Errorf("the database egress rule is wrong: %v", egress[1])
	}
	for _, rule := range egress {
		for _, peer := range at(rule, "to").([]any) {
			if at(peer, "ipBlock") != nil {
				t.Errorf("egress allows an ipBlock by default: %v", peer)
			}
		}
	}
	job := authDoc(docs, "NetworkPolicy", "a-dev-health-auth-migrate")
	if job == nil {
		t.Fatal("no auth-migrate NetworkPolicy")
	}
	if ingress, _ := at(job, "spec", "ingress").([]any); len(ingress) != 0 {
		t.Errorf("the migration Job accepts connections: %v", ingress)
	}

	// Listed consumers and scrapers reach exactly their port.
	docs, out, err = renderAuth(t, append(append(append([]string{}, authBase...), authNetworkPolicy...),
		"--set", "authService.networkPolicy.allowedFrom[0].matchLabels.app=consumer",
		"--set", "authService.networkPolicy.metricsFrom[0].matchLabels.app=scraper")...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	service := authDoc(docs, "Service", "a-dev-health-auth-service")
	if at(service, "spec", "ports", 1, "port") != 8096 {
		t.Errorf("listed scrapers cannot reach the operator port through the Service: %v", at(service, "spec", "ports"))
	}
	policy = authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service")
	ingress := at(policy, "spec", "ingress").([]any)
	if len(ingress) != 2 || at(ingress, 0, "ports", 0, "port") != 8095 || at(ingress, 1, "ports", 0, "port") != 8096 {
		t.Errorf("consumer and scraper rules do not target the API and operator ports: %v", ingress)
	}
}

// Enabling the service alone (the release-wide networkPolicy.enabled left at its
// default false) must still deploy it WITH its boundary; opting out is explicit.
func TestAuthNetworkPolicyRendersWithoutTheReleaseWidePolicy(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	if authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service") == nil || authDoc(docs, "NetworkPolicy", "a-dev-health-auth-migrate") == nil {
		t.Fatal("enabling authService without networkPolicy.enabled rendered no auth NetworkPolicy")
	}
	docs, out, err = renderAuth(t, append(append([]string{}, authBase...), "--set", "authService.networkPolicy.enabled=false")...)
	if err != nil {
		t.Fatalf("opt-out render failed: %v\n%.600s", err, out)
	}
	if authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service") != nil {
		t.Fatal("authService.networkPolicy.enabled=false still rendered a policy")
	}
}

func TestAuthServiceRefusals(t *testing.T) {
	cases := map[string]struct {
		args []string
		want string
	}{
		"no database secret":  {[]string{"--set", "authService.enabled=true", "--set", "authService.signingKey.secretName=k", "--set", "authService.signingKey.keyId=x"}, "authService.database.secretName is required"},
		"no signing secret":   {[]string{"--set", "authService.enabled=true", "--set", "authService.database.secretName=d", "--set", "authService.signingKey.keyId=x"}, "authService.signingKey.secretName is required"},
		"no key id":           {[]string{"--set", "authService.enabled=true", "--set", "authService.database.secretName=d", "--set", "authService.signingKey.secretName=k"}, "authService.signingKey.keyId is required"},
		"no migration secret": {append(append([]string{}, authBase[:8]...), "--set", "authService.migrate.secretName=", "--set-json", `authService.networkPolicy.egress.database.to=[{"podSelector":{"matchLabels":{"app":"x"}}}]`), "authService.migrate.secretName is required"},
		"same port":           {append(append([]string{}, authBase...), "--set", "authService.operatorPort=8095"), "must differ"},
		"shutdown too long":   {append(append([]string{}, authBase...), "--set", "authService.shutdownTimeout=30s"), "must be below authService.terminationGracePeriodSeconds"},
		"direct signing key":  {append(append([]string{}, authBase...), "--set", "authService.extraEnv[0].name=AUTH_SIGNING_KEY", "--set", "authService.extraEnv[0].value=x"), "AUTH_SIGNING_KEY"},
		"policy without db":   {[]string{"--set", "authService.enabled=true", "--set", "authService.database.secretName=d", "--set", "authService.signingKey.secretName=k", "--set", "authService.signingKey.keyId=x", "--set", "authService.migrate.secretName=m"}, "authService.networkPolicy.egress.database.to is required"},
		"empty podSelector":   {append(append([]string{}, authBase...), "--set-json", `authService.networkPolicy.allowedFrom=[{"matchLabels":{}}]`), "non-empty matchLabels"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, tc.args...)
			if err == nil {
				t.Fatalf("the render succeeded; it must refuse")
			}
			if !strings.Contains(out, tc.want) {
				t.Fatalf("the refusal does not say %q: %.400s", tc.want, out)
			}
		})
	}
}

func toStrings(v any) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))
	for _, item := range list {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}
