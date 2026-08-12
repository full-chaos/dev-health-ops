//go:build integration

package providerfoundation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresPagerDutyOAuthRefreshLockSerializesWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if closeErr := instance.Close(closeCtx); closeErr != nil {
			t.Errorf("terminate PostgreSQL test dependency: %v", closeErr)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
CREATE TABLE provider_oauth_credentials (
    org_id text NOT NULL,
    provider text NOT NULL,
    credential_name text NOT NULL,
    token_encrypted text NOT NULL,
    version integer NOT NULL,
    binding_id text,
    expires_at timestamptz,
    granted_scopes json,
    has_refresh_token boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, provider, credential_name)
);
INSERT INTO provider_oauth_credentials (
    org_id, provider, credential_name, token_encrypted, version, binding_id
) VALUES ('org-1', 'pagerduty', 'operations', 'v1:ciphertext', 7, 'binding-1')`); err != nil {
		t.Fatal(err)
	}

	repository := PostgresPagerDutyOAuthTokenRepository{Pool: pool}
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
	defer release()
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- repository.WithRefreshLock(
			ctx, "org-1", "operations",
			func(PagerDutyOAuthTokenRecord) (*PagerDutyOAuthTokenRotation, error) {
				close(firstEntered)
				<-releaseFirst
				return nil, nil
			},
		)
	}()
	<-firstEntered

	secondEntered := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- repository.WithRefreshLock(
			ctx, "org-1", "operations",
			func(PagerDutyOAuthTokenRecord) (*PagerDutyOAuthTokenRotation, error) {
				close(secondEntered)
				return nil, nil
			},
		)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		err := pool.QueryRow(ctx, `
SELECT count(*)
FROM pg_stat_activity
WHERE wait_event_type = 'Lock'
  AND query LIKE '%provider_oauth_credentials%FOR UPDATE%'`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		select {
		case <-secondEntered:
			release()
			t.Fatal("second refresh entered before the first row lock was released")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("second refresh did not become a PostgreSQL row-lock waiter")
		}
		time.Sleep(20 * time.Millisecond)
	}

	release()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("second refresh did not enter after row lock release")
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}
