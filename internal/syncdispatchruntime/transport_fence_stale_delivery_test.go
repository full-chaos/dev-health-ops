package syncdispatchruntime

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// A fake Postgres wire server drives the real Dispatch pass, so a competing
// writer can commit at an exact point between two of the pass's statements.

const (
	fenceOrg         = "10000000-0000-4000-8000-000000000001"
	fenceRun         = "10000000-0000-4000-8000-000000000002"
	fenceOutbox      = "10000000-0000-4000-8000-000000000003"
	fenceIntegration = "10000000-0000-4000-8000-000000000004"
	fenceGeneration  = 3
)

type fenceRow struct {
	status, transport, routeTransport string
	generation, routeGeneration       int64
	attempts                          int64
}

func fenceDispatched(attempts int64) fenceRow {
	return fenceRow{"dispatched", "river", "river", fenceGeneration, fenceGeneration, attempts}
}

type fenceState struct {
	outbox                fenceRow
	runStatus, unitStatus string
}

// fenceTx holds an open transaction's own writes. Reads see them over the
// latest committed state (READ COMMITTED); COMMIT applies them, ROLLBACK drops them.
type fenceTx struct {
	run, unit *string
	outbox    *fenceRow
}

type fenceServer struct {
	delivered           int64
	finishedUnit        bool
	wakeAfterGuard      string
	redeliverAfterGuard bool
	guardDone           bool

	committed fenceState
	tx        *fenceTx
	trace     []string
}

func (s *fenceServer) outboxRow() fenceRow {
	if s.tx != nil && s.tx.outbox != nil {
		return *s.tx.outbox
	}
	return s.committed.outbox
}

func (s *fenceServer) unitStatus() string {
	if s.tx != nil && s.tx.unit != nil {
		return *s.tx.unit
	}
	return s.committed.unitStatus
}

func (s *fenceServer) runStatus() string {
	if s.tx != nil && s.tx.run != nil {
		return *s.tx.run
	}
	return s.committed.runStatus
}

func (s *fenceServer) write(apply func(*fenceTx)) {
	if s.tx == nil {
		s.tx = &fenceTx{}
		defer s.commit()
	}
	apply(s.tx)
}

func (s *fenceServer) commit() {
	if s.tx == nil {
		return
	}
	if s.tx.run != nil {
		s.committed.runStatus = *s.tx.run
	}
	if s.tx.unit != nil {
		s.committed.unitStatus = *s.tx.unit
	}
	if s.tx.outbox != nil {
		s.committed.outbox = *s.tx.outbox
	}
	s.tx = nil
}

func (s *fenceServer) current(row fenceRow, sql string) (bool, error) {
	ok := row.status == "dispatched" && row.transport == "river" && row.routeTransport == "river" &&
		row.generation == fenceGeneration && row.routeGeneration == fenceGeneration
	if strings.Contains(sql, "outbox.attempts") {
		if !strings.Contains(sql, fmt.Sprintf("outbox.attempts = '%d'", s.delivered)) {
			return false, fmt.Errorf("attempt clause does not carry the delivered attempt %d: %s", s.delivered, sql)
		}
		ok = ok && row.attempts == s.delivered
	}
	return ok, nil
}

func fenceRows(backend *pgproto3.Backend, oids []uint32, rows ...[]string) {
	fields := make([]pgproto3.FieldDescription, 0, len(oids))
	for _, oid := range oids {
		fields = append(fields, pgproto3.FieldDescription{Name: []byte("field"), DataTypeOID: oid, DataTypeSize: -1, TypeModifier: -1})
	}
	backend.Send(&pgproto3.RowDescription{Fields: fields})
	for _, row := range rows {
		values := make([][]byte, len(row))
		for i, value := range row {
			if value != "<NULL>" {
				values[i] = []byte(value)
			}
		}
		backend.Send(&pgproto3.DataRow{Values: values})
	}
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", len(rows)))})
}

func fenceCommand(backend *pgproto3.Backend, tag string) {
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

// Exec is the competing worker's wake: its own statement, committed at once.
func (s *fenceServer) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if sql != syncrunrollup.WakeDispatchSQL || args[1] != fenceRun {
		return pgconn.CommandTag{}, fmt.Errorf("unexpected wake")
	}
	// OutboxWakeupOnConflictSQL: status 'pending', dispatched transport and generation cleared, attempts kept.
	s.committed.outbox.status, s.committed.outbox.transport, s.committed.outbox.generation = "pending", "", 0
	s.trace = append(s.trace, "wake:"+s.wakeAfterGuard)
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

// afterGuard runs the competing writer once, right after the pass's start guard.
func (s *fenceServer) afterGuard() error {
	if s.guardDone {
		return nil
	}
	s.guardDone = true
	if s.wakeAfterGuard != "" && !syncrunrollup.WakeDispatch(context.Background(), s, fenceRun, time.Now(), s.wakeAfterGuard) {
		return fmt.Errorf("fixture wake did not report an armed outbox")
	}
	if s.redeliverAfterGuard {
		// The relay claimed and redelivered the row: the next attempt, dispatched to the same route.
		s.committed.outbox.attempts++
		s.trace = append(s.trace, "redelivery")
	}
	return nil
}

// referenceRow answers readTransportReference (the Dispatch start guard and its
// commit-time diagnosis) from the row the statement sees.
func (s *fenceServer) referenceRow(backend *pgproto3.Backend, sql string) error {
	if !strings.Contains(sql, fenceOutbox) || !strings.Contains(sql, fenceRun) || !strings.Contains(sql, fenceOrg) ||
		!strings.Contains(sql, "outbox.attempts") {
		return fmt.Errorf("reference statement lost its identity or attempt columns: %s", sql)
	}
	row := s.outboxRow()
	status, transport, generation := row.status, row.transport, strconv.FormatInt(row.generation, 10)
	if status == "dispatched(feature_disabled)" {
		status = "dispatched"
	}
	if transport == "" {
		transport, generation = "<NULL>", "<NULL>"
	}
	fenceRows(backend, []uint32{25, 20, 16, 16, 25, 25, 20, 20}, []string{row.routeTransport, strconv.FormatInt(row.routeGeneration, 10), "f",
		"t", status, transport, generation, strconv.FormatInt(row.attempts, 10)})
	if !s.guardDone {
		s.trace = append(s.trace, "guard-read")
		return s.afterGuard()
	}
	s.trace = append(s.trace, "diagnosis-read")
	return nil
}

func (s *fenceServer) fenceQuery(backend *pgproto3.Backend, sql string) (bool, error) {
	if strings.HasPrefix(sql, "SELECT route.transport, route.generation, route.paused, outbox.found,") {
		return true, s.referenceRow(backend, sql)
	}
	if !strings.Contains(sql, "public.sync_dispatch_outbox AS outbox") {
		return false, nil
	}
	if !strings.Contains(sql, fenceOutbox) || !strings.Contains(sql, fenceRun) || !strings.Contains(sql, fenceOrg) ||
		!strings.Contains(sql, fmt.Sprintf("route.generation = '%d'", fenceGeneration)) {
		return false, fmt.Errorf("fence statement lost its identity clauses: %s", sql)
	}
	ok, err := s.current(s.outboxRow(), sql)
	if err != nil {
		return false, err
	}
	switch {
	case strings.HasPrefix(sql, "SELECT EXISTS ( SELECT 1 FROM public.sync_dispatch_outbox AS outbox"):
		s.trace = append(s.trace, fmt.Sprintf("guard=%t", ok))
		fenceRows(backend, []uint32{16}, []string{strconv.FormatBool(ok)})
		if err := s.afterGuard(); err != nil {
			return false, err
		}
	case strings.HasPrefix(sql, "SELECT 1 FROM public.sync_dispatch_outbox AS outbox") && strings.Contains(sql, "FOR NO KEY UPDATE OF outbox NOWAIT"):
		// A wake that only re-armed the row (pending, unclaimed, same attempt) does not supersede the delivery.
		if row := s.outboxRow(); !ok && row.status == "pending" && row.routeTransport == "river" && row.routeGeneration == fenceGeneration &&
			row.attempts == s.delivered && strings.Contains(sql, "outbox.claim_token IS NULL") {
			ok = true
		}
		s.trace = append(s.trace, fmt.Sprintf("commit-check=%t", ok))
		if ok {
			fenceRows(backend, []uint32{23}, []string{"1"})
		} else {
			fenceRows(backend, []uint32{23})
		}
	default:
		return false, nil
	}
	return true, nil
}

func (s *fenceServer) domainQuery(backend *pgproto3.Backend, sql string) bool {
	switch {
	case strings.HasPrefix(sql, "SELECT id::text, org_id, integration_id::text"):
		total := "1"
		if s.finishedUnit {
			total = "2"
		}
		fenceRows(backend, []uint32{25, 25, 25, 25, 23, 23, 23, 1184, 114, 25},
			[]string{fenceRun, fenceOrg, fenceIntegration, s.runStatus(), total, "0", "0", "<NULL>", "{}", "<NULL>"})
	case strings.HasPrefix(sql, "SELECT provider, dataset_key FROM public.sync_run_units"):
		fenceRows(backend, []uint32{25, 25}, []string{"pagerduty", "incidents"})
	case strings.HasPrefix(sql, "SELECT feature.min_tier, feature.is_enabled,"):
		fenceRows(backend, []uint32{25, 16, 16, 1184, 114, 25, 114, 25},
			[]string{"enterprise", "t", "f", "<NULL>", "<NULL>", "<NULL>", "<NULL>", "enterprise"})
	case strings.HasPrefix(sql, "SELECT DISTINCT org_id, provider, cost_class"):
		fenceRows(backend, []uint32{25, 25, 25}, []string{fenceOrg, "pagerduty", "rest_core"})
	case strings.HasPrefix(sql, "SELECT pg_try_advisory_xact_lock"):
		fenceRows(backend, []uint32{16}, []string{"t"})
	case strings.HasPrefix(sql, "SELECT id::text, lease_owner FROM public.sync_run_units"):
		fenceRows(backend, []uint32{25, 25})
	case strings.HasPrefix(sql, "UPDATE public.sync_run_units SET status = 'failed'"):
		failed := "failed"
		s.write(func(tx *fenceTx) { tx.unit = &failed })
		s.trace = append(s.trace, "unit-write")
		fenceCommand(backend, "UPDATE 1")
	case strings.HasPrefix(sql, "SELECT 1 FROM public.sync_runs WHERE id ="):
		fenceRows(backend, []uint32{23}, []string{"1"})
	case strings.HasPrefix(sql, "SELECT status FROM public.sync_run_units"):
		rows := [][]string{{s.unitStatus()}}
		if s.finishedUnit {
			rows = append(rows, []string{"success"})
		}
		fenceRows(backend, []uint32{25}, rows...)
	case strings.HasPrefix(sql, "UPDATE public.sync_runs SET failed_units =") && strings.Contains(sql, "status = 'failed'"):
		failed := "failed"
		s.write(func(tx *fenceTx) { tx.run = &failed })
		s.trace = append(s.trace, "run-write")
		fenceCommand(backend, "UPDATE 1")
	case strings.HasPrefix(sql, "UPDATE public.sync_run_reference_discoveries"):
		fenceCommand(backend, "UPDATE 0")
	case strings.HasPrefix(sql, "UPDATE public.sync_dispatch_outbox SET status = 'dispatched'") && strings.Contains(sql, "AND status = 'pending'"):
		if row := s.outboxRow(); row.status == "pending" {
			parked := fenceRow{status: "dispatched(feature_disabled)", routeTransport: row.routeTransport, routeGeneration: row.routeGeneration, attempts: row.attempts}
			s.write(func(tx *fenceTx) { tx.outbox = &parked })
			s.trace = append(s.trace, "dispatch-outbox-write")
			fenceCommand(backend, "UPDATE 1")
		} else {
			fenceCommand(backend, "UPDATE 0")
		}
	case strings.HasPrefix(sql, "INSERT INTO public.sync_dispatch_outbox"):
		fenceCommand(backend, "INSERT 0 1")
	case strings.HasPrefix(sql, "SELECT id::text, celery_task_id FROM public.backfill_jobs"):
		fenceRows(backend, []uint32{25, 25})
	case strings.HasPrefix(sql, "UPDATE public.backfill_jobs"), strings.HasPrefix(sql, "UPDATE public.job_runs"):
		fenceCommand(backend, "UPDATE 0")
	default:
		return false
	}
	return true
}

func (s *fenceServer) serve(connection net.Conn) error {
	defer connection.Close()
	backend := pgproto3.NewBackend(connection, connection)
	message, err := backend.ReceiveStartupMessage()
	if _, ok := message.(*pgproto3.StartupMessage); err != nil || !ok {
		return fmt.Errorf("startup: %T %v", message, err)
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return err
	}
	status := byte('I')
	for {
		message, err := backend.Receive()
		if err != nil {
			return err
		}
		switch query := message.(type) {
		case *pgproto3.Terminate:
			return nil
		case *pgproto3.Query:
			sql := strings.Join(strings.Fields(query.String), " ")
			switch {
			case sql == "begin":
				status = 'T'
				s.tx = &fenceTx{}
				fenceCommand(backend, "BEGIN")
			case sql == "rollback":
				status = 'I'
				s.tx = nil
				s.trace = append(s.trace, "rollback")
				fenceCommand(backend, "ROLLBACK")
			case sql == "commit":
				status = 'I'
				s.commit()
				s.trace = append(s.trace, "commit")
				fenceCommand(backend, "COMMIT")
			default:
				handled, fenceErr := s.fenceQuery(backend, sql)
				if fenceErr != nil {
					return fenceErr
				}
				if !handled && !s.domainQuery(backend, sql) {
					return fmt.Errorf("unexpected SQL: %s", sql)
				}
			}
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
			if err := backend.Flush(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected frontend message: %T", message)
		}
	}
}

func runFenceDispatch(t *testing.T, s *fenceServer) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig("postgres://fence:fence@127.0.0.1/fence?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 1
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	done := make(chan error, 1)
	config.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() { done <- s.serve(server) }()
		return client, nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	logger := synclog.New(slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelInfo})))
	args, err := Convert(Claim{OutboxID: fenceOutbox, Kind: outboxKindDispatchSyncRun, RouteGeneration: fenceGeneration, DeliveryAttempt: s.delivered},
		DomainReference{OrganizationID: fenceOrg, SyncRunID: fenceRun})
	if err != nil {
		t.Fatal(err)
	}
	registry := noopPolicyRegistry{}
	producer, err := joboutbox.NewProducer(pool, registry)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewNativeDispatchSyncRunService(pool, logger, noopBudgetEstimator{}, producer, registry)
	if err != nil {
		t.Fatal(err)
	}
	worker := &dispatchWorker{service: service}
	workErr := worker.Work(ctx, &river.Job[DispatchSyncRunArgs]{Args: args.(DispatchSyncRunArgs)})
	pool.Close()
	if serverErr := <-done; serverErr != nil {
		t.Fatal(serverErr)
	}
	return output.String(), workErr
}

// TestTransportFenceRefusesStaleOrRearmedDelivery: a superseded delivery
// commits no write and names the failing clause in
// dispatch_sync_run.delivery_not_current: an attempt superseded before the
// guard (P1-2, start refusal) or during the pass (commit-time refusal). A
// same-attempt wake after the guard (P1-3) does not supersede the delivery, but
// on this feature-disabled terminal path the pass parks every pending row of
// the run, its re-armed own row included; the commit-time check sees that own
// write and refuses. The wake survives and its delivery runs the branch to the end.
func TestTransportFenceRefusesStaleOrRearmedDelivery(t *testing.T) {
	pending := fenceRow{status: "pending", routeTransport: "river", routeGeneration: fenceGeneration, attempts: 1}
	nextGeneration := fenceDispatched(2)
	nextGeneration.generation, nextGeneration.routeGeneration = fenceGeneration+1, fenceGeneration+1
	cases := []struct {
		name                string
		row                 fenceRow
		delivered           int64
		wakeAfterGuard      string
		redeliverAfterGuard bool
		wantWrites          bool
		wantOutbox          string
		wantAttempts        int64
		wantReason          string // "" = no delivery_not_current line
	}{
		{"current_first_attempt", fenceDispatched(1), 1, "", false, true, "dispatched", 1, ""},
		{"current_redelivery", fenceDispatched(2), 2, "", false, true, "dispatched", 2, ""},
		{"pending_before_guard", pending, 1, "", false, false, "pending", 1, "outbox_not_dispatched"},
		{"older_route_generation", nextGeneration, 1, "", false, false, "dispatched", 2, "route_generation_mismatch"},
		{"holdback_redelivery_committed_before_guard", fenceDispatched(2), 1, "", false, false, "dispatched", 2, "delivery_attempt_mismatch"},
		{"unit_end_redelivery_committed_before_guard", fenceDispatched(2), 1, "", false, false, "dispatched", 2, "delivery_attempt_mismatch"},
		{"holdback_redelivery_committed_after_guard", fenceDispatched(1), 1, "", true, false, "dispatched", 2, "delivery_attempt_mismatch"},
		{"unit_end_redelivery_committed_after_guard", fenceDispatched(1), 1, "", true, false, "dispatched", 2, "delivery_attempt_mismatch"},
		{"holdback_rearmed_after_guard", fenceDispatched(1), 1, syncrunrollup.DispatchWakePathHoldBack, false, false, "pending", 1, "outbox_other_transport"},
		{"unit_end_rearmed_after_guard", fenceDispatched(1), 1, syncrunrollup.DispatchWakePathProviderUnit, false, false, "pending", 1, "outbox_other_transport"},
		{"holdback_rearmed_redelivery_after_guard", fenceDispatched(2), 2, syncrunrollup.DispatchWakePathHoldBack, false, false, "pending", 2, "outbox_other_transport"},
		{"unit_end_rearmed_redelivery_after_guard", fenceDispatched(2), 2, syncrunrollup.DispatchWakePathProviderUnit, false, false, "pending", 2, "outbox_other_transport"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &fenceServer{delivered: c.delivered, wakeAfterGuard: c.wakeAfterGuard, redeliverAfterGuard: c.redeliverAfterGuard,
				finishedUnit: strings.HasPrefix(c.name, "unit_end"),
				committed:    fenceState{outbox: c.row, runStatus: "running", unitStatus: "planned"}}
			output, err := runFenceDispatch(t, s)
			wrote := s.committed.runStatus != "running" || s.committed.unitStatus != "planned"
			t.Logf("worker=%v delivered_attempt=%d row_attempt=%d statements=%s committed: run=%s unit=%s outbox=%s/%d",
				err, c.delivered, c.row.attempts, strings.Join(s.trace, ","), s.committed.runStatus,
				s.committed.unitStatus, s.committed.outbox.status, s.committed.outbox.attempts)
			if err != nil || wrote != c.wantWrites {
				t.Errorf("want committed writes=%t, got %t (worker=%v)", c.wantWrites, wrote, err)
			}
			if s.committed.outbox.status != c.wantOutbox || s.committed.outbox.attempts != c.wantAttempts {
				t.Errorf("outbox status=%s attempts=%d, want %s/%d", s.committed.outbox.status, s.committed.outbox.attempts, c.wantOutbox, c.wantAttempts)
			}
			refusal := ""
			for _, line := range strings.Split(output, "\n") {
				if strings.Contains(line, "msg=dispatch_sync_run.delivery_not_current") {
					refusal = line
				}
			}
			switch {
			case c.wantReason == "" && refusal != "":
				t.Errorf("current delivery logged a refusal: %s", refusal)
			case c.wantReason != "" && !strings.Contains(refusal, "reason="+c.wantReason+" "):
				t.Errorf("want dispatch_sync_run.delivery_not_current reason=%s, got %q", c.wantReason, refusal)
			}
		})
	}
}
