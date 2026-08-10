package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestGitLabFilesEffectsValidateProviderAndEmptyBatches(t *testing.T) {
	sink := GitLabFilesClickHouseEffects{
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	claim := nativeTestClaim("gitlab", "files")
	empty := mustGitLabFileEffect(t, nil)
	if err := sink.WriteEffect(context.Background(), claim, empty); err != nil {
		t.Fatalf("empty write error=%v", err)
	}
	if inspection, err := sink.InspectEffect(context.Background(), claim, empty); err != nil || inspection != EffectAbsent {
		t.Fatalf("empty inspect=%s error=%v", inspection, err)
	}
	wrongClaim := nativeTestClaim("github", "files")
	if err := sink.WriteEffect(context.Background(), wrongClaim, empty); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong provider write error=%v", err)
	}
	if _, err := sink.InspectEffect(context.Background(), wrongClaim, empty); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong provider inspect error=%v", err)
	}
}

func TestGitLabFilesEffectsAssertLeaseBeforeDecoding(t *testing.T) {
	sink := GitLabFilesClickHouseEffects{
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return errors.New("lease gone") }),
	}
	claim := nativeTestClaim("gitlab", "files")
	malformed := EffectBatch{Destination: "git_files", Rows: []json.RawMessage{{'n', 'o', 't', '-', 'j', 's', 'o', 'n'}}}
	if err := sink.WriteEffect(context.Background(), claim, malformed); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("write lease error=%v", err)
	}
	if _, err := sink.InspectEffect(context.Background(), claim, malformed); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("inspect lease error=%v", err)
	}
}

func TestGitLabFilesEffectsPathsOnlyReadbackAllowsExistingContent(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	expected := gitFileRow{
		RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Path: "src/main.go",
		LastSynced: now, OrgID: "org-a",
	}
	content := "package existing\n"
	actual := expected
	actual.Contents = &content
	if got := compareGitLabFileVersion(expected, gitFileVersion{Row: actual, LastSynced: now, Found: true}); got != EffectExact {
		t.Fatalf("paths-only readback=%s want exact", got)
	}
}

func mustGitLabFileEffect(t *testing.T, rows []gitFileRow) EffectBatch {
	t.Helper()
	if rows == nil {
		rows = []gitFileRow{}
	}
	effect, err := effectBatchFromValues("git_files", EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
