package devhealth_test

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// A Helm hook runs before (pre-*) or after (post-*) the release's normal
// resources. A pre-install hook pod that references a ServiceAccount, Secret or
// ConfigMap the same chart renders as a NORMAL resource is rejected on a fresh
// install (the object does not exist yet): the pod is never created and nothing
// starts. CHAOS-4886 round 3 found it for the auth migration Job's service
// account. This test is the class check: for every hook resource of several
// renders, every ServiceAccount, Secret and ConfigMap it references must be either
// an object the chart does not render at all (it pre-exists) or itself a hook
// resource with a LOWER weight.

type hookKey struct{ kind, name string }

func chartDir() string {
	if dir := os.Getenv("CHART_DIR"); dir != "" {
		return dir
	}
	return "."
}

func renderDocs(t *testing.T, args ...string) []map[string]any {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	out, err := exec.Command("helm", append([]string{"template", "h", chartDir()}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
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
	return docs
}

func hookWeight(doc map[string]any) (int, bool) {
	annotations, _ := at(doc, "metadata", "annotations").(map[string]any)
	if annotations["helm.sh/hook"] == nil {
		return 0, false
	}
	weight, _ := strconv.Atoi(strings.TrimSpace(toString(annotations["helm.sh/hook-weight"])))
	return weight, true
}

func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// hookPodSpec returns the pod specs of a hook resource (Job, Pod).
func hookPodSpec(doc map[string]any) map[string]any {
	if spec, ok := at(doc, "spec", "template", "spec").(map[string]any); ok {
		return spec
	}
	spec, _ := at(doc, "spec").(map[string]any)
	return spec
}

func references(spec map[string]any) []hookKey {
	var refs []hookKey
	if name := toString(spec["serviceAccountName"]); name != "" {
		refs = append(refs, hookKey{"ServiceAccount", name})
	}
	for _, v := range asList(at(spec, "volumes")) {
		if name := toString(at(v, "secret", "secretName")); name != "" {
			refs = append(refs, hookKey{"Secret", name})
		}
		if name := toString(at(v, "configMap", "name")); name != "" {
			refs = append(refs, hookKey{"ConfigMap", name})
		}
	}
	for _, group := range []string{"containers", "initContainers"} {
		list, _ := spec[group].([]any)
		for _, c := range list {
			for _, e := range asList(at(c, "env")) {
				if name := toString(at(e, "valueFrom", "secretKeyRef", "name")); name != "" {
					refs = append(refs, hookKey{"Secret", name})
				}
				if name := toString(at(e, "valueFrom", "configMapKeyRef", "name")); name != "" {
					refs = append(refs, hookKey{"ConfigMap", name})
				}
			}
			for _, e := range asList(at(c, "envFrom")) {
				if name := toString(at(e, "secretRef", "name")); name != "" {
					refs = append(refs, hookKey{"Secret", name})
				}
				if name := toString(at(e, "configMapRef", "name")); name != "" {
					refs = append(refs, hookKey{"ConfigMap", name})
				}
			}
		}
	}
	return refs
}

func TestHookResourcesReferenceOnlyObjectsThatExistWhenTheHookRuns(t *testing.T) {
	renders := map[string][]string{
		"defaults":           {"--set", "web.env.BACKEND_URL=http://x", "--set", "web.authSecret.secretName=web-auth-placeholder"},
		"auth enabled":       authBase,
		"auth, create creds": append(append([]string{}, authBase...), "--set", "secrets.create=true"),
	}
	checked := 0
	for name, args := range renders {
		docs := renderDocs(t, args...)
		rendered := map[hookKey]map[string]any{}
		for _, doc := range docs {
			meta, _ := doc["metadata"].(map[string]any)
			if n, _ := meta["name"].(string); n != "" {
				rendered[hookKey{toString(doc["kind"]), n}] = doc
			}
		}
		for _, doc := range docs {
			weight, isHook := hookWeight(doc)
			if !isHook || (doc["kind"] != "Job" && doc["kind"] != "Pod") {
				continue
			}
			meta, _ := doc["metadata"].(map[string]any)
			for _, ref := range references(hookPodSpec(doc)) {
				checked++
				target, exists := rendered[ref]
				if !exists {
					continue // not rendered by the chart: it pre-exists (external Secret, default account)
				}
				targetWeight, targetIsHook := hookWeight(target)
				if !targetIsHook || targetWeight >= weight {
					t.Errorf("%s: hook %v %v references %s %s, which this chart renders as a normal resource or a hook that is not earlier: a fresh install rejects the pod",
						name, doc["kind"], meta["name"], ref.kind, ref.name)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("no hook reference was examined: the test measured nothing")
	}
}

func asList(v any) []any {
	list, _ := v.([]any)
	return list
}
