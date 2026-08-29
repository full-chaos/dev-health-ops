//go:build integration

package main

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/riverqueue/river"
)

// TestSyncCoordinatorReportsItsRegisteredKind closes the second registration
// blind spot: sync.team_autoimport is a bounded registry kind whose worker
// lives in the coordinator's own private River client. Before CUT-02 that
// builder returned only a lifecycle component, so the kind was constructed but
// unobservable to startup validation.
//
// Integration-tagged (CHAOS-4175): reference_discovery's native ClickHouse
// readback verification made buildSyncCoordinatorWorker require a real,
// reachable ClickHouse connection (clickhousestore.Open pings it), which
// this plain unit test file's other cases don't otherwise pay for.
func TestSyncCoordinatorReportsItsRegisteredKind(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := clickhouseInstance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	// CHAOS-4226: the coordinator's finalize now bumps the home-dashboard
	// cache epoch in Valkey, so Valkey is a hard build dependency of this
	// family the same way ClickHouse became one under CHAOS-4175.
	valkeyInstance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := valkeyInstance.Close(closeCtx); err != nil {
			t.Errorf("terminate Valkey: %v", err)
		}
	})

	for _, test := range []struct {
		name      string
		promote   []string
		wantKinds []string
	}{
		{
			// sync.team_repo_ownership_derivation (CHAOS-4365 item 1b) is
			// river-unconditionally (state=celery_removed) -- unlike
			// team_autoimport, it has no Celery-routed state to demote to,
			// so buildSyncCoordinatorWorker always reports it regardless of
			// team_autoimport's own promotion state.
			name:      "celery routed kind is not consumed at all",
			wantKinds: []string{jobcontract.KindTeamRepoOwnershipDerivation},
		},
		{
			name:      "promoted kind is registered and reported",
			promote:   []string{jobcontract.KindTeamAutoimport},
			wantKinds: []string{jobcontract.KindTeamAutoimport, jobcontract.KindTeamRepoOwnershipDerivation},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// sync.team_autoimport now ships at go_default in the checked-in
			// contract, so the "not consumed at all" case has to demote it
			// back to Celery explicitly; promotedContractRoot alone would be
			// a no-op against an already-promoted production tree.
			var registry *jobruntime.Registry
			if len(test.promote) == 0 {
				registry, _ = demotedContractRoot(t, jobcontract.KindTeamAutoimport)
			} else {
				registry, _ = promotedContractRoot(t, test.promote...)
			}
			family, err := buildSyncCoordinatorWorker(
				ctx,
				config.Config{
					Queues:                         []string{"sync", "sync_provider"},
					WorkerQueueConcurrency:         map[string]int{"sync": 13, "sync_provider": 7},
					RiverDatabaseSchema:            "river",
					OperationalBridgeURL:           "http://localhost",
					OperationalBridgeToken:         secrets.NewValue("test-bridge-token"),
					OperationalBridgeTimeout:       time.Second,
					OperationalBridgeAllowInsecure: true,
					ClickHouseURI:                  secrets.NewValue(clickhouseInstance.URI),
					ValkeyURI:                      secrets.NewValue(valkeyInstance.URI),
					// CHAOS-4431: buildSyncCoordinatorWorker now constructs a
					// credential decryptor for the native team-catalog
					// collector path (same newWorkerCredentialCipher
					// provider_sync_test.go already requires), so this
					// family's build now needs it too.
					SettingsEncryptionKey: secrets.NewValue("test-master-key"),
				},
				reportBuilderDatabase(t),
				registry,
				reportTestObserver(t),
				slog.Default(),
				river.NewWorkers(),
			)
			if err != nil {
				t.Fatalf("buildSyncCoordinatorWorker: %v", err)
			}
			t.Cleanup(func() {
				for _, cleanup := range family.cleanups {
					_ = cleanup()
				}
			})
			if len(family.queues) == 0 {
				t.Fatal("coordinator did not declare its selected queue")
			}
			if len(family.handlers) != len(test.wantKinds) {
				t.Fatalf("reported handlers = %#v, want %v", family.handlers, test.wantKinds)
			}
			for index, kind := range test.wantKinds {
				if family.handlers[index].Kind != kind {
					t.Fatalf("reported handler %d = %s, want %s", index, family.handlers[index].Kind, kind)
				}
			}
			// The coordinator's native, non-registry dispatch handlers also
			// consume the sync queue. Its queue budget remains present even
			// while team auto-import still routes to Celery.
			if len(family.queues) != 1 || family.queues[0].Queue != syncCoordinatorQueue ||
				family.queues[0].MaxWorkers != 13 {
				t.Fatalf("reported queues = %#v", family.queues)
			}
		})
	}
}

// TestSyncCoordinatorRefusesToBuildWithoutValkeyConfigured is the
// discriminating half of the CHAOS-4226 reachability proof: with the SAME
// reachable ClickHouse the happy path above builds against, leaving only
// VALKEY_URI unconfigured must refuse the family. A coordinator that built
// without Valkey would finalize forever with the cache-invalidation hop
// silently skipped (a permanent emitted - consumed gap at best).
func TestSyncCoordinatorRefusesToBuildWithoutValkeyConfigured(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = clickhouseInstance.Close(closeCtx)
	})
	registry, _ := demotedContractRoot(t, jobcontract.KindTeamAutoimport)
	_, err = buildSyncCoordinatorWorker(
		ctx,
		config.Config{
			Queues:                         []string{"sync", "sync_provider"},
			WorkerQueueConcurrency:         map[string]int{"sync": 13, "sync_provider": 7},
			RiverDatabaseSchema:            "river",
			OperationalBridgeURL:           "http://localhost",
			OperationalBridgeToken:         secrets.NewValue("test-bridge-token"),
			OperationalBridgeTimeout:       time.Second,
			OperationalBridgeAllowInsecure: true,
			ClickHouseURI:                  secrets.NewValue(clickhouseInstance.URI),
			// ValkeyURI deliberately left unconfigured.
		},
		reportBuilderDatabase(t),
		registry,
		reportTestObserver(t),
		slog.Default(),
		river.NewWorkers(),
	)
	if !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("buildSyncCoordinatorWorker error=%v want=%v", err, errWorkerDependencyUnavailable)
	}
}
