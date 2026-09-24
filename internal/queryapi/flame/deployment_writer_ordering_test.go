package flame

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// TestBuildDeploymentFlameResponseCreationBeforeStartOrdering pins the
// ordering a provider-derived deployment row actually carries: deployed_at
// is the deployment's creation instant, started_at/finished_at come from
// status entries recorded after creation (deployed_at <= started_at <
// finished_at), and merged_at, when known, precedes creation. Under this
// ordering buildDeploymentFlameResponse (flame.go:412-473) answers 200 with
// no gap in both cases -- the root frame alone spans [start, end], which is
// enough for validateFlameFrames -- but the "Deploy" sub-frame (flame.go:
// 455-459) never appears: it requires deployed_at strictly after the
// pipeline's own start (== started_at), and creation is never after the
// first status entry recorded once work began.
func TestBuildDeploymentFlameResponseCreationBeforeStartOrdering(t *testing.T) {
	t.Parallel()

	t.Run("with merged_at before creation", func(t *testing.T) {
		t.Parallel()
		client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
			if !strings.Contains(query, "FROM deployments FINAL") {
				t.Fatalf("unexpected query: %s", query)
			}
			return &fixtureRowScanner{rows: [][]any{{
				"success", "production",
				day(2026, 1, 10, 10, 1, 0), day(2026, 1, 10, 10, 5, 0), day(2026, 1, 10, 10, 0, 0), day(2026, 1, 10, 9, 0, 0),
			}}}, nil
		}}
		got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":order-merged"})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		if got.Timeline.Start != day(2026, 1, 10, 10, 1, 0) || got.Timeline.End != day(2026, 1, 10, 10, 5, 0) {
			t.Fatalf("timeline=%+v", got.Timeline)
		}
		if len(got.Frames) != 3 {
			t.Fatalf("frames=%+v, want 3 (root, queue, pipeline)", got.Frames)
		}
		for _, frame := range got.Frames {
			if strings.HasSuffix(frame.ID, ":deploy") {
				t.Fatalf("deploy sub-frame unexpectedly present: %+v", frame)
			}
		}
		if !strings.HasSuffix(got.Frames[1].ID, ":queue") || !strings.HasSuffix(got.Frames[2].ID, ":pipeline") {
			t.Fatalf("frames=%+v, want queue then pipeline", got.Frames)
		}
	})

	t.Run("without merged_at", func(t *testing.T) {
		t.Parallel()
		client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
			if !strings.Contains(query, "FROM deployments FINAL") {
				t.Fatalf("unexpected query: %s", query)
			}
			return &fixtureRowScanner{rows: [][]any{{
				"success", "production",
				day(2026, 1, 10, 10, 1, 0), day(2026, 1, 10, 10, 5, 0), day(2026, 1, 10, 10, 0, 0), nil,
			}}}, nil
		}}
		got, err := BuildResponse(context.Background(), client, "org-1", Params{EntityType: "deployment", EntityID: repoID + ":order-nomerge"})
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		if got.Timeline.Start != day(2026, 1, 10, 10, 1, 0) || got.Timeline.End != day(2026, 1, 10, 10, 5, 0) {
			t.Fatalf("timeline=%+v", got.Timeline)
		}
		if len(got.Frames) != 2 {
			t.Fatalf("frames=%+v, want 2 (root, pipeline)", got.Frames)
		}
		for _, frame := range got.Frames {
			if strings.HasSuffix(frame.ID, ":queue") || strings.HasSuffix(frame.ID, ":deploy") {
				t.Fatalf("queue or deploy sub-frame unexpectedly present: %+v", frame)
			}
		}
		if !strings.HasSuffix(got.Frames[1].ID, ":pipeline") {
			t.Fatalf("frames=%+v, want pipeline second", got.Frames)
		}
	})
}
