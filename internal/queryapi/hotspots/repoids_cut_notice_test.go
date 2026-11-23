package hotspots

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

// CHAOS-14515: a caller repoIds list over MaxRepoIDsBound entries must not be
// cut with no notice. Resolve refuses it with an error, or serves a field
// outside the fields of today (Rows, Repos) that names the cut.
func TestRepoIdsOver1000IsRefusedOrDisclosed(t *testing.T) {
	repoIDs := make([]string, MaxRepoIDsBound+1)
	for i := range repoIDs {
		repoIDs[i] = fmt.Sprintf("r-%04d", i)
	}
	client := &fakeClient{responses: []*fakeRowScanner{{rows: [][]any{}}}}
	got, err := Resolve(context.Background(), client, "org-1",
		mustTime(t, "2026-08-01T00:00:00Z"), mustTime(t, "2026-08-01T23:59:59Z"), repoIDs, nil)
	if err != nil {
		return
	}
	v := reflect.ValueOf(*got)
	for i := 0; i < v.NumField(); i++ {
		if name := v.Type().Field(i).Name; name != "Rows" && name != "Repos" && !v.Field(i).IsZero() {
			return
		}
	}
	bound := -1
	for _, b := range client.bindings[0] {
		if b.Name == "repo_ids" {
			bound = len(b.Value.([]string))
		}
	}
	t.Fatalf("%d repoIds: no error and no served field names the cut; repo_ids bound %d, rows=%d; want an error or a served cut field",
		len(repoIDs), bound, len(got.Rows))
}
