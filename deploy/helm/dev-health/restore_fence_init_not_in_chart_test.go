package devhealth_test

import (
	"strings"
	"testing"
)

// `restore-fence init` writes the first fence row of a database that is NOT a restore. Only the dev compose stack
// runs it automatically (an explicit one-shot); no Job, initContainer or container of the chart may run it, in the
// auth-service render and in the default render, so a restored database is never marked fenced by a rollout.
func TestNoWorkloadOfTheChartRunsRestoreFenceInit(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"auth phase 1", authBase},
		{"auth service off", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			docs, out, err := renderAuth(t, tc.args...)
			if err != nil {
				t.Fatalf("render failed: %v\n%.600s", err, out)
			}
			if len(docs) == 0 {
				t.Fatal("the render holds no object: the assertion would measure nothing")
			}
			for _, doc := range docs {
				if hit := findRestoreFence(doc); hit != "" {
					meta, _ := doc["metadata"].(map[string]any)
					t.Errorf("%v %v runs restore-fence: %s", doc["kind"], meta["name"], hit)
				}
			}
		})
	}
}

func findRestoreFence(v any) string {
	switch x := v.(type) {
	case string:
		if strings.Contains(x, "restore-fence") {
			return x
		}
	case []any:
		for _, e := range x {
			if hit := findRestoreFence(e); hit != "" {
				return hit
			}
		}
	case map[string]any:
		for _, e := range x {
			if hit := findRestoreFence(e); hit != "" {
				return hit
			}
		}
	}
	return ""
}
