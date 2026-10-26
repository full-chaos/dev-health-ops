package devhealth_test

import (
	"sort"
	"testing"
)

// One Valkey pod runs at a time: with the default rolling update the new pod
// starts on the same ReadWriteOnce volume and append-only file while the old
// one still writes it.
func TestValkeyDeploymentRecreatesBeforeItsVolumeIsReused(t *testing.T) {
	for name, args := range map[string][]string{
		"persistence on":  nil,
		"persistence off": {"--set", "valkey.persistence.enabled=false"},
	} {
		t.Run(name, func(t *testing.T) {
			deployment, _ := valkeyDeployment(t, args...)
			strategy, ok := at(deployment, "spec", "strategy").(map[string]any)
			if !ok {
				t.Fatalf("the valkey Deployment has no strategy: %v", at(deployment, "spec", "strategy"))
			}
			if strategy["type"] != "Recreate" {
				t.Errorf("valkey strategy type = %v, want Recreate", strategy["type"])
			}
			if _, rolling := strategy["rollingUpdate"]; rolling || len(strategy) != 1 {
				t.Errorf("valkey strategy = %v, want only type Recreate", strategy)
			}
		})
	}
}

// valkeyStepBase is a prod-shaped release before the ACL steps: the release-wide
// policy on, auth-service reading the store URI from the Secret it has today.
func valkeyStepBase() []string {
	args := append(append([]string{}, authBase...), authNetworkPolicy...)
	return append(args,
		"--set", "authService.valkey.secretName=auth-valkey",
		"--set-json", `authService.networkPolicy.egress.valkey.to=[{"podSelector":{"matchLabels":{"app":"valkey"}}}]`)
}

var (
	valkeyAclStep    = []string{"--set", "valkey.acl.enabled=true", "--set", "valkey.acl.secretName=auth-valkey-acl"}
	authValkeySwitch = []string{"--set", "authService.valkey.secretName=auth-valkey-acl"}
)

// changedDocs renders both value sets and returns "Kind/name" of every document
// that differs, is added or is removed.
func changedDocs(t *testing.T, before, after []string) []string {
	t.Helper()
	render := func(args []string) map[string]string {
		docs, out, err := renderAuth(t, args...)
		if err != nil {
			t.Fatalf("render failed: %v\n%.600s", err, out)
		}
		byName := map[string]string{}
		for _, doc := range docs {
			meta, _ := doc["metadata"].(map[string]any)
			key, _ := doc["kind"].(string)
			name, _ := meta["name"].(string)
			key += "/" + name
			if _, dup := byName[key]; dup {
				t.Fatalf("two rendered documents are both %s", key)
			}
			byName[key] = yamlText(doc)
		}
		return byName
	}
	a, b := render(before), render(after)
	if len(a) == 0 || len(b) == 0 {
		t.Fatal("a render produced no documents")
	}
	var changed []string
	for key, text := range a {
		if b[key] != text {
			changed = append(changed, key)
		}
	}
	for key := range b {
		if _, ok := a[key]; !ok {
			changed = append(changed, key)
		}
	}
	sort.Strings(changed)
	return changed
}

// The ACL file is a data-store step of its own: turning it on restarts Valkey
// and changes no other workload.
func TestValkeyAclStepChangesOnlyTheValkeyDeployment(t *testing.T) {
	base := valkeyStepBase()
	got := changedDocs(t, base, append(append([]string{}, base...), valkeyAclStep...))
	if len(got) != 1 || got[0] != "Deployment/a-dev-health-valkey" {
		t.Errorf("the ACL step changes %v, want only Deployment/a-dev-health-valkey", got)
	}
}

// Moving auth-service to the ACL login is an app step of its own: it changes the
// auth-service Deployment and not Valkey.
func TestAuthValkeySecretSwitchChangesOnlyAuthService(t *testing.T) {
	step1 := append(valkeyStepBase(), valkeyAclStep...)
	got := changedDocs(t, step1, append(append([]string{}, step1...), authValkeySwitch...))
	if len(got) != 1 || got[0] != "Deployment/a-dev-health-auth-service" {
		t.Errorf("the Secret switch changes %v, want only Deployment/a-dev-health-auth-service", got)
	}
}
