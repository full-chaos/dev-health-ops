package devhealth_test

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-10921: the in-chart Valkey loads an ACL file from a Secret when
// valkey.acl.enabled is set, so the auth-service rate-limit user survives a
// restart. Off by default: nothing changes without the value.

func valkeyDeployment(t *testing.T, args ...string) (map[string]any, string) {
	t.Helper()
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	deployment := authDoc(docs, "Deployment", "a-dev-health-valkey")
	if deployment == nil {
		t.Fatal("no valkey Deployment rendered")
	}
	return deployment, out
}

func valkeyArgs(deployment map[string]any) []string {
	return toStrings(at(deployment, "spec", "template", "spec", "containers", 0, "args"))
}

func TestValkeyHasNoAclFileUnlessTheValueIsSet(t *testing.T) {
	deployment, out := valkeyDeployment(t)
	want := []string{"--appendonly", "yes", "--appendfsync", "everysec", "--maxmemory", "256mb", "--maxmemory-policy", "allkeys-lru"}
	got := valkeyArgs(deployment)
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("valkey args = %v, want the unchanged %v", got, want)
	}
	for _, token := range []string{"aclfile", "valkey-acl", "users.acl"} {
		if strings.Contains(valkeySection(out), token) {
			t.Errorf("the default render carries %q in the valkey manifests", token)
		}
	}
}

func TestValkeyLoadsTheAclFileFromTheNamedSecret(t *testing.T) {
	deployment, out := valkeyDeployment(t, "--set", "valkey.acl.enabled=true", "--set", "networkPolicy.enabled=true", "--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24", "--set", "valkey.acl.secretName=valkey-users")
	got := valkeyArgs(deployment)
	want := []string{"--aclfile", "/etc/valkey-acl/users.acl"}
	if len(got) < 2 || got[len(got)-2] != want[0] || got[len(got)-1] != want[1] {
		t.Errorf("valkey args = %v, want them to end with %v", got, want)
	}
	spec := at(deployment, "spec", "template", "spec")
	var volume map[string]any
	for _, v := range at(spec, "volumes").([]any) {
		if at(v, "name") == "valkey-acl" {
			volume, _ = v.(map[string]any)
		}
	}
	if volume == nil {
		t.Fatal("no valkey-acl volume")
	}
	if at(volume, "secret", "secretName") != "valkey-users" {
		t.Errorf("the volume reads Secret %v, want valkey-users", at(volume, "secret", "secretName"))
	}
	items := at(volume, "secret", "items").([]any)
	if len(items) != 1 || at(items, 0, "key") != "users.acl" || at(items, 0, "path") != "users.acl" {
		t.Errorf("the volume projects %v, want only users.acl", items)
	}
	var mounted bool
	for _, m := range at(spec, "containers", 0, "volumeMounts").([]any) {
		if at(m, "name") == "valkey-acl" {
			mounted = at(m, "mountPath") == "/etc/valkey-acl" && at(m, "readOnly") == true
		}
	}
	if !mounted {
		t.Error("the ACL file is not mounted read-only at /etc/valkey-acl")
	}
	// The data volume is untouched beside it.
	if !strings.Contains(valkeySection(out), "valkey-data") {
		t.Error("the persistence volume went missing")
	}
}

func TestValkeyAclKeyIsHonouredAndNoPasswordIsRendered(t *testing.T) {
	deployment, out := valkeyDeployment(t, "--set", "valkey.acl.enabled=true", "--set", "networkPolicy.enabled=true", "--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24", "--set", "valkey.acl.secretName=valkey-users", "--set", "valkey.acl.secretKey=acl.txt")
	got := valkeyArgs(deployment)
	if got[len(got)-1] != "/etc/valkey-acl/acl.txt" {
		t.Errorf("valkey args = %v, want the path to follow secretKey", got)
	}
	// The chart names a Secret and a key; it carries no rule, no hash and no password.
	section := valkeySection(out)
	for _, token := range []string{"auth-service-ratelimit", "user default", "requirepass", "nopass", "password"} {
		if strings.Contains(strings.ToLower(section), token) {
			t.Errorf("the valkey manifests carry %q", token)
		}
	}
}

func TestValkeyAclFileOffWithoutTheInClusterValkey(t *testing.T) {
	docs, out, err := renderAuth(t, "--set", "valkey.enabled=false", "--set", "valkey.acl.enabled=true", "--set", "networkPolicy.enabled=true", "--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24", "--set", "valkey.acl.secretName=valkey-users")
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	if authDoc(docs, "Deployment", "a-dev-health-valkey") != nil {
		t.Error("a valkey Deployment rendered with valkey.enabled=false")
	}
}

// valkeySection is the rendered text of the in-chart Valkey's manifests only.
func valkeySection(out string) string {
	var keep []string
	for _, part := range strings.Split(out, "\n---\n") {
		if strings.Contains(part, "-valkey") && (strings.Contains(part, "kind: Deployment") || strings.Contains(part, "kind: Service") || strings.Contains(part, "kind: PersistentVolumeClaim")) {
			keep = append(keep, part)
		}
	}
	return strings.Join(keep, "\n---\n")
}

func TestValkeyAclIsRefusedWithoutTheSecretItReads(t *testing.T) {
	for name, args := range map[string][]string{
		"no secret name": {"--set", "valkey.acl.enabled=true", "--set", "networkPolicy.enabled=true", "--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24"},
		"no secret key":  {"--set", "valkey.acl.enabled=true", "--set", "networkPolicy.enabled=true", "--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24", "--set", "valkey.acl.secretName=valkey-users", "--set", "valkey.acl.secretKey="},
	} {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, args...)
			if err == nil {
				t.Fatal("the render succeeded; it must refuse")
			}
			if !strings.Contains(out, "valkey.acl.enabled needs valkey.acl.secretName and valkey.acl.secretKey") {
				t.Fatalf("the refusal does not say so: %.400s", out)
			}
		})
	}
	// A name with the value off is inert: nothing is read, nothing refused.
	deployment, out := valkeyDeployment(t, "--set", "valkey.acl.secretName=valkey-users")
	if strings.Join(valkeyArgs(deployment), " ") != "--appendonly yes --appendfsync everysec --maxmemory 256mb --maxmemory-policy allkeys-lru" || strings.Contains(out, "valkey-users") {
		t.Error("a Secret name with valkey.acl.enabled=false changed the render")
	}
}

// One Secret holds both keys; each pod sees only its own. Valkey projects only
// users.acl, auth-service reads only AUTH_VALKEY_URI through one secretKeyRef,
// and nothing takes the whole Secret (no envFrom, no volume without items).
func TestEachPodSeesOnlyItsKeyOfTheSharedSecret(t *testing.T) {
	docs, out, err := renderAuth(t, append(append([]string{}, authBase...),
		"--set", "valkey.acl.enabled=true", "--set", "networkPolicy.enabled=true", "--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24", "--set", "valkey.acl.secretName=auth-valkey",
		"--set", "authService.valkey.secretName=auth-valkey",
		"--set-json", `authService.networkPolicy.egress.valkey.to=[{"podSelector":{"matchLabels":{"app":"valkey"}}}]`)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	referring := 0
	for _, doc := range docs {
		raw := yamlText(doc)
		if !strings.Contains(raw, "auth-valkey") {
			continue
		}
		referring++
		name, _ := at(doc, "metadata", "name").(string)
		if doc["kind"] != "Deployment" || (name != "a-dev-health-valkey" && name != "a-dev-health-auth-service") {
			t.Errorf("%s %s refers to the shared Secret", doc["kind"], name)
		}
		if strings.Contains(raw, "envFrom") {
			t.Errorf("%s uses envFrom: the whole Secret would be in its environment", name)
		}
	}
	if referring != 2 {
		t.Errorf("%d documents refer to the Secret, want exactly the Valkey and auth-service Deployments", referring)
	}
	valkey := authDoc(docs, "Deployment", "a-dev-health-valkey")
	for _, v := range at(valkey, "spec", "template", "spec", "volumes").([]any) {
		if at(v, "name") == "valkey-acl" {
			items := at(v, "secret", "items").([]any)
			if len(items) != 1 || at(items, 0, "key") != "users.acl" {
				t.Errorf("Valkey projects %v, want only users.acl", items)
			}
		}
	}
	service := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	keys := 0
	for _, e := range at(service, "spec", "template", "spec", "containers", 0, "env").([]any) {
		if at(e, "valueFrom", "secretKeyRef", "name") == "auth-valkey" {
			keys++
			if at(e, "valueFrom", "secretKeyRef", "key") != "AUTH_VALKEY_URI" {
				t.Errorf("auth-service reads key %v of the shared Secret, want only AUTH_VALKEY_URI", at(e, "valueFrom", "secretKeyRef", "key"))
			}
		}
	}
	if keys != 1 {
		t.Errorf("auth-service has %d references to the shared Secret, want exactly one key", keys)
	}
}

func yamlText(v any) string {
	raw, _ := yaml.Marshal(v)
	return string(raw)
}

func TestValkeyAclIsRefusedWithoutTheNetworkPolicyThatBoundsTheDefaultUser(t *testing.T) {
	_, out, err := renderAuth(t, "--set", "valkey.acl.enabled=true", "--set", "valkey.acl.secretName=valkey-users")
	if err == nil {
		t.Fatal("the render succeeded with the ACL file on and networkPolicy.enabled=false; it must refuse")
	}
	if !strings.Contains(out, "valkey.acl.enabled needs networkPolicy.enabled=true") {
		t.Fatalf("the refusal does not say so: %.400s", out)
	}
	deployment, _ := valkeyDeployment(t)
	if strings.Contains(strings.Join(valkeyArgs(deployment), " "), "aclfile") {
		t.Error("the default render (ACL off, network policy off) changed")
	}
}

func TestValkeyAclSecretFileModeIsPinned(t *testing.T) {
	deployment, _ := valkeyDeployment(t, "--set", "valkey.acl.enabled=true", "--set", "networkPolicy.enabled=true", "--set", "goWorkers.pgbouncer.postgres.networkPolicyCIDR=10.0.0.0/24", "--set", "valkey.acl.secretName=valkey-users")
	for _, v := range at(deployment, "spec", "template", "spec", "volumes").([]any) {
		if at(v, "name") == "valkey-acl" {
			if at(v, "secret", "defaultMode") != 0o444 && at(v, "secret", "defaultMode") != 292 {
				t.Errorf("the ACL Secret volume mode = %v, want 0444 (the file holds hashes only; the image user is not root)", at(v, "secret", "defaultMode"))
			}
			return
		}
	}
	t.Fatal("no valkey-acl volume")
}
