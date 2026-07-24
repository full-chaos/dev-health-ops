package syncreconciler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/jackc/pgx/v5"
)

// CUT-07 acceptance includes "no reconciler operation publishes a Celery task."
// That is easy to satisfy today and easy to regress later: the reconciler sits
// next to a Celery-bridge HTTP client and a Celery transport route value, so a
// future change could plausibly reach for a Celery signature without anyone
// noticing in review. This test makes the boundary mechanical.
//
// It scans source rather than behavior on purpose. A behavioral test can only
// prove that the paths it exercises stay clean; the whole package has to.
var celeryConstructs = []string{
	"apply_async",
	".delay(",
	"send_task",
	"celery.signature",
	"kombu",
	"CELERY_",
}

func packageGoFiles(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read %s: %v", directory, err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		files = append(files, filepath.Join(directory, entry.Name()))
	}
	if len(files) == 0 {
		t.Fatalf("no Go sources found under %s", directory)
	}
	return files
}

func TestReconcilerNeverConstructsACelerySignature(t *testing.T) {
	for _, directory := range []string{".", filepath.Join("..", "syncdispatchruntime")} {
		for _, path := range packageGoFiles(t, directory) {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			source, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			text := string(source)
			for _, construct := range celeryConstructs {
				if strings.Contains(text, construct) {
					t.Errorf(
						"%s references the Celery construct %q; the reconciler must publish "+
							"only through River",
						path, construct,
					)
				}
			}
		}
	}
}

// The reconciler's River publication set must be derived from the durable
// transport route, never from a compiled list. A kernel built with a
// Celery-routed descriptor has to hold an empty River kind set, which is what
// makes the two route planes independently transitionable.
func TestKernelPublicationSetFollowsTheDurableRoute(t *testing.T) {
	noTransaction := func(context.Context) (pgx.Tx, error) {
		return nil, errors.New("the publication set must not need a transaction")
	}
	celeryOnly := riverRegistry(t)
	kernel, err := newKernel(celeryOnly, KernelModeMutation, &kernelStepper{}, noTransaction)
	if err != nil {
		t.Fatalf("newKernel() = %v", err)
	}
	if len(kernel.riverKinds) != 0 {
		t.Fatalf("kernel claims River kinds %v with an all-Celery contract", kernel.riverKinds)
	}

	promoted := riverRegistry(t, syncdispatchcontract.KindDispatchSyncRun)
	kernel, err = newKernel(promoted, KernelModeMutation, &kernelStepper{}, noTransaction)
	if err != nil {
		t.Fatalf("newKernel() = %v", err)
	}
	if len(kernel.riverKinds) != 1 ||
		kernel.riverKinds[0] != syncdispatchcontract.KindDispatchSyncRun {
		t.Fatalf("kernel River kinds = %v, want only the promoted kind", kernel.riverKinds)
	}
}
