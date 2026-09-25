package sync

import (
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
)

// TestSyncNowSelectionPromotesOnlyIncrementalAndNamesChildDatasets pins each
// clause of syncNowSelection (CHAOS-6695): the full_resync promotion applies
// to an incremental request only, and a child configuration names its
// datasets, or none (nil, never an empty non-nil list, which Mint would read
// as "run no datasets") when its targets map to nothing.
func TestSyncNowSelectionPromotesOnlyIncrementalAndNamesChildDatasets(t *testing.T) {
	t.Parallel()
	source := "source-1"
	flagged := map[string]any{"full_resync": true}
	cases := []struct {
		name     string
		config   synchandoff.Config
		mode     string
		wantMode string
		wantNil  bool
	}{
		{"incremental with the flag is promoted", synchandoff.Config{Provider: "github", SyncOptions: flagged}, "incremental", "full_resync", true},
		{"a non-incremental request keeps its mode despite the flag", synchandoff.Config{Provider: "github", SyncOptions: flagged}, "backfill", "backfill", true},
		{"incremental without the flag stays incremental", synchandoff.Config{Provider: "github"}, "incremental", "incremental", true},
		{"a child whose targets map to nothing names no datasets (nil)", synchandoff.Config{Provider: "github", SourceID: &source, SyncTargets: []string{"no-such-target"}}, "incremental", "incremental", true},
	}
	for _, tc := range cases {
		mode, keys := syncNowSelection(&tc.config, tc.mode)
		if mode != tc.wantMode {
			t.Errorf("%s: mode = %q, want %q", tc.name, mode, tc.wantMode)
		}
		if tc.wantNil && keys != nil {
			t.Errorf("%s: dataset keys = %#v, want nil", tc.name, keys)
		}
	}
	child := synchandoff.Config{Provider: "github", SourceID: &source, SyncTargets: []string{"git"}}
	if _, keys := syncNowSelection(&child, "incremental"); len(keys) == 0 || !reflect.DeepEqual(keys, providersync.DatasetKeysForTargets("github", []string{"git"})) {
		t.Errorf("a child with a real target names its datasets: got %v", keys)
	}
	parent := synchandoff.Config{Provider: "github", SyncTargets: []string{"git"}}
	if _, keys := syncNowSelection(&parent, "incremental"); keys != nil {
		t.Errorf("a parent (no source) names no datasets: got %v", keys)
	}
}
