package inputs

import (
	"bytes"
	"errors"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type renderDoc struct {
	Kind, Name string
	Hook       string
}

// decodeDocs reads every doc of a multi-doc stream; Hook is the parsed
// metadata.annotations["helm.sh/hook"] string (any kind), not a text match.
func decodeDocs(t *testing.T, stream []byte) []renderDoc {
	t.Helper()
	var docs []renderDoc
	dec := yaml.NewDecoder(bytes.NewReader(stream))
	for {
		var d struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name        string            `yaml:"name"`
				Annotations map[string]string `yaml:"annotations"`
			} `yaml:"metadata"`
		}
		err := dec.Decode(&d)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("decode render: %v", err)
		}
		if d.Kind == "" {
			continue
		}
		docs = append(docs, renderDoc{d.Kind, d.Metadata.Name, d.Metadata.Annotations["helm.sh/hook"]})
	}
	return docs
}

func readsAnnotation(d renderDoc) bool { return d.Hook != "" }

// listedHook: the name rule (hookDoc) or a hook's own companion doc
// (<fullname>-<hook>-secrets, -conn, -config).
func listedHook(name string) bool {
	if hookDoc(name) {
		return true
	}
	for _, h := range migrateHooks {
		if strings.Contains(name, "-"+h+"-") {
			return true
		}
	}
	return false
}

func chartFullname(release string) string {
	if strings.Contains(release, "dev-health") {
		return release
	}
	return release + "-dev-health"
}

// exactExempt: route-activate rides an app roll; its Job and Secret are
// exempt by EXACT Kind/name for the release.
func exactExempt(release string) func(renderDoc) bool {
	fn := chartFullname(release)
	return func(d renderDoc) bool {
		return (d.Kind == "Job" && d.Name == fn+"-route-activate") ||
			(d.Kind == "Secret" && d.Name == fn+"-route-activate-secrets")
	}
}

func suffixExempt(d renderDoc) bool {
	return (d.Kind == "Job" && strings.HasSuffix(d.Name, "-route-activate")) ||
		(d.Kind == "Secret" && strings.HasSuffix(d.Name, "-route-activate-secrets"))
}

// hookCensus returns each annotated doc that is neither a listed hook nor exempt.
func hookCensus(docs []renderDoc, isHook func(renderDoc) bool, exempt func(renderDoc) bool) []string {
	var bad []string
	for _, d := range docs {
		if isHook(d) && !listedHook(d.Name) && !exempt(d) {
			bad = append(bad, d.Kind+"/"+d.Name)
		}
	}
	sort.Strings(bad)
	return bad
}

const syntheticHook = `apiVersion: batch/v1
kind: Job
metadata:
  name: ` + "%s" + `-seed-data
  annotations:
    helm.sh/hook: "pre-install,pre-upgrade"
`

func withSyntheticHook(render []byte, fullname string) []byte {
	doc := strings.Replace(syntheticHook, "%s", fullname, 1)
	return append(append(append([]byte{}, render...), []byte("\n---\n")...), []byte(doc)...)
}

// CHAOS-14344: every doc of a real render that carries helm.sh/hook is a
// listed hook or the exact route-activate exemption; a new hook with a new
// name is red here until it is listed.
func TestEveryAnnotatedHookDocOfARealRenderIsListedOrExempt(t *testing.T) {
	img := ".spec.template.spec.containers[0].image"
	for _, release := range []string{"dev-health", "dev-health-ops", "acme"} {
		t.Run(release, func(t *testing.T) {
			render := chartRenderBytes(t, release)
			docs := decodeDocs(t, render)
			exempt := exactExempt(release)

			hooks := 0
			for _, d := range docs {
				if readsAnnotation(d) {
					hooks++
				}
			}
			if hooks != 15 {
				t.Fatalf("%s: the render holds %d annotated hook docs, want 15", release, hooks)
			}
			if bad := hookCensus(docs, readsAnnotation, exempt); len(bad) > 0 {
				t.Fatalf("%s: annotated hook docs neither in the hook name list nor exempt: %v", release, bad)
			}

			// the real hook Jobs are refused as rows; the exempt route-activate row passes.
			for _, d := range docs {
				if d.Kind != "Job" || !readsAnnotation(d) {
					continue
				}
				_, err := Parse([]byte(goodDev + row("Job/"+d.Name, img, oldDig, newDig)))
				var r *Refusal
				if exempt(d) {
					if err != nil {
						t.Errorf("%s: exempt %s row is refused: %v", release, d.Name, err)
					}
					continue
				}
				if !errors.As(err, &r) || !r.NotBuilt {
					t.Errorf("%s: annotated hook Job %s row is not refused: %v", release, d.Name, err)
				}
			}

			// synthetic 9th hook with a new name.
			syn := decodeDocs(t, withSyntheticHook(render, chartFullname(release)))
			bad := hookCensus(syn, readsAnnotation, exempt)
			want := "Job/" + chartFullname(release) + "-seed-data"
			if len(bad) != 1 || bad[0] != want {
				t.Fatalf("%s: synthetic 9th hook: census = %v, want [%s]", release, bad, want)
			}

			// mutant: annotation read removed -> the synthetic hook goes unseen (cell would be red).
			never := func(renderDoc) bool { return false }
			if got := hookCensus(syn, never, exempt); len(got) != 0 {
				t.Fatalf("%s: mutant (no annotation read) still reports %v", release, got)
			}
			// mutant: exempt by suffix -> a synthetic <x>-route-activate Job is waved through.
			sneaky := append(append([]byte{}, render...), []byte("\n---\napiVersion: batch/v1\nkind: Job\nmetadata:\n  name: other-route-activate\n  annotations:\n    helm.sh/hook: pre-upgrade\n")...)
			sd := decodeDocs(t, sneaky)
			if got := hookCensus(sd, readsAnnotation, exempt); len(got) != 1 || got[0] != "Job/other-route-activate" {
				t.Fatalf("%s: exact exempt let a foreign route-activate by: %v", release, got)
			}
			if got := hookCensus(sd, readsAnnotation, suffixExempt); len(got) != 0 {
				t.Fatalf("%s: mutant (suffix exempt) still reports %v", release, got)
			}
		})
	}
}

func TestAnnotationValueFormsAreRead(t *testing.T) {
	stream := []byte("kind: ConfigMap\nmetadata:\n  name: a\n  annotations:\n    helm.sh/hook: pre-install\n---\nkind: Secret\nmetadata:\n  name: b\n  annotations:\n    helm.sh/hook-weight: \"1\"\n---\nkind: Job\nmetadata:\n  name: c\n  annotations:\n    helm.sh/hook: \"pre-install,pre-upgrade,pre-rollback\"\n---\nkind: Pod\nmetadata:\n  name: d\n")
	var hooks []string
	for _, d := range decodeDocs(t, stream) {
		if readsAnnotation(d) {
			hooks = append(hooks, d.Kind+"/"+d.Name)
		}
	}
	if strings.Join(hooks, ",") != "ConfigMap/a,Job/c" {
		t.Fatalf("hooks = %v (hook-weight alone is not a hook; any kind counts)", hooks)
	}
}
