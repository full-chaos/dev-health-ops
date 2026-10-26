//go:build integration

package jobruntime

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// dropReplyConn forwards the client's INSERT into worker_instances to the
// server, then fails every read: the statement commits on the server and the
// client never sees the answer, the way a pooler that dies mid-reply behaves.
type dropReplyConn struct {
	net.Conn
	armed   *atomic.Bool
	dropped atomic.Bool
}

func (conn *dropReplyConn) Write(data []byte) (int, error) {
	written, err := conn.Conn.Write(data)
	if err == nil && conn.armed.Load() && bytes.Contains(data, []byte("INSERT INTO public.worker_instances")) {
		conn.dropped.Store(true)
	}
	return written, err
}

func (conn *dropReplyConn) Read(data []byte) (int, error) {
	if conn.dropped.Load() {
		// Let the server finish and commit before the client gives up; the answer is never read.
		time.Sleep(500 * time.Millisecond)
		_ = conn.Conn.Close()
		return 0, io.ErrUnexpectedEOF
	}
	return conn.Conn.Read(data)
}

func TestWorkerPresenceRetryAfterLostReplyRegistersOnceAndDoesNotFail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if _, err := admin.Exec(ctx, `
		CREATE TABLE public.worker_instances (
			instance_id uuid PRIMARY KEY,
			worker_group varchar(64) NOT NULL,
			queues json NOT NULL,
			state varchar(16) NOT NULL CHECK (state IN ('accepting', 'draining')),
			started_at timestamptz NOT NULL,
			heartbeat_at timestamptz NOT NULL,
			expires_at timestamptz NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}

	var armed atomic.Bool
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &dropReplyConn{Conn: conn, armed: &armed}, nil
	}
	lossy, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lossy.Close)

	id := uuid.NewString()
	presence, err := NewWorkerPresence(lossy, "tenant-worker-a", []string{"webhooks", "coverage"}, id)
	if err != nil {
		t.Fatal(err)
	}
	presence.ttl = time.Minute

	armed.Store(true)
	if err := presence.Start(ctx); err == nil {
		t.Fatal("first Start() = nil, want the lost-reply error")
	}
	armed.Store(false)
	deadline := time.Now().Add(10 * time.Second)
	for {
		var rows int
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM public.worker_instances WHERE instance_id = $1`, id).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the INSERT never committed (rows=%d); the lost-reply setup is wrong", rows)
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err := presence.Start(ctx); err != nil {
		t.Fatalf("retried Start() = %v, want nil: the row is this process's own earlier insert", err)
	}
	t.Cleanup(func() { _ = presence.Shutdown(context.Background()) })
	var rows int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM public.worker_instances WHERE instance_id = $1`, id).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows for the instance = %d, want exactly 1", rows)
	}
}

func TestWorkerPresenceRetryAfterLostReplyStillRefusesADifferentOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_instances (
			instance_id uuid PRIMARY KEY,
			worker_group varchar(64) NOT NULL,
			queues json NOT NULL,
			state varchar(16) NOT NULL CHECK (state IN ('accepting', 'draining')),
			started_at timestamptz NOT NULL,
			heartbeat_at timestamptz NOT NULL,
			expires_at timestamptz NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.worker_instances VALUES ($1, 'another-group', '["webhooks"]', 'accepting',
			statement_timestamp(), statement_timestamp(), statement_timestamp() + interval '1 minute')`, id); err != nil {
		t.Fatal(err)
	}
	presence, err := NewWorkerPresence(pool, "tenant-worker-a", []string{"webhooks"}, id)
	if err != nil {
		t.Fatal(err)
	}
	presence.ttl = time.Minute
	if err := presence.Start(ctx); err == nil {
		t.Fatal("Start() = nil over another owner's row, want a refusal")
	}
	if err := presence.Start(ctx); err == nil {
		t.Fatal("second Start() = nil over another owner's row, want a refusal")
	}
	var group string
	if err := pool.QueryRow(ctx, `SELECT worker_group FROM public.worker_instances WHERE instance_id = $1`, id).Scan(&group); err != nil || group != "another-group" {
		t.Fatalf("existing row = %q, %v; must be untouched", group, err)
	}
}
