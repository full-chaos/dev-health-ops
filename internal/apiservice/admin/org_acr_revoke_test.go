package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrlisting"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrrevoke"
)

// The ruled operator-facing texts, byte for byte.
const (
	wantDisableACRRevokeBody           = `{"detail":"Organization disabled; its acr credentials were not revoked; repeat the request."}`
	wantDisableACRRevokeIncompleteBody = `{"detail":"Organization disabled; its acr credentials were not all revoked: more remains; repeat the request."}`
	wantDeleteACRRevokeBody            = `{"detail":"The acr credentials of this organization were not revoked; the organization is now disabled; run the delete again. Nothing was deleted."}`
	wantDeleteACRRevokeIncompleteBody  = `{"detail":"The acr credentials of this organization were not all revoked: more remains; the organization is now disabled; run the delete again. Nothing was deleted."}`
)

type orgRevokeCall struct{ org, actor, reason string }

// recordingOrgRevoker answers each call with the next error (nil when none is left) and records it in log.
type recordingOrgRevoker struct {
	log   *orderLog
	errs  []error
	calls []orgRevokeCall
}

func (r *recordingOrgRevoker) RevokeOrganization(_ context.Context, orgID, actorID, reason string) (acrrevoke.OrganizationRevocation, error) {
	if r.log != nil {
		r.log.add("acr-revoke")
	}
	r.calls = append(r.calls, orgRevokeCall{orgID, actorID, reason})
	var err error
	if len(r.errs) > 0 {
		err, r.errs = r.errs[0], r.errs[1:]
	}
	if err != nil {
		return acrrevoke.OrganizationRevocation{Calls: 1}, err
	}
	return acrrevoke.OrganizationRevocation{Revoked: 2, CallerRevoked: 1, Calls: 1}, nil
}

func acrSkipCounts(t *testing.T) map[string]int64 {
	t.Helper()
	deprovisionSkipCounts(t) // installs the one reader
	var collected metricdata.ResourceMetrics
	if err := deprovisionReader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "devhealth_api_org_acr_revoke_skipped_total" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				event, _ := point.Attributes.Value("event")
				reason, _ := point.Attributes.Value("reason")
				counts[event.AsString()+"/"+reason.AsString()] += point.Value
			}
		}
	}
	return counts
}

func disableHandlers(db *lockDB, revoker OrgCredentialRevoker, logs *bytes.Buffer) *handlers {
	h := &handlers{store: pgStore{Pool: db, Now: time.Now}, logger: slog.New(slog.NewJSONHandler(logs, nil))}
	if revoker != nil {
		h.acrOrgRevoker = revoker
	}
	return h
}

// D1: a stored disable revokes the organization's acr credentials once, after the commit, for the acting admin with
// reason organization_disabled; the answer is the PATCH's own.
func TestPatchDisableRevokesTheOrgACRCredentialsAfterTheCommit(t *testing.T) {
	for _, active := range []bool{true, false} {
		log := &orderLog{}
		db := &lockDB{log: log, active: active}
		revoker := &recordingOrgRevoker{log: log}
		var logs bytes.Buffer
		code, body := patchOrg(disableHandlers(db, revoker, &logs), `{"is_active": false}`)
		if answeredIsActive(t, code, body) {
			t.Fatalf("read saw active=%v: answer is_active true", active)
		}
		want := orgRevokeCall{validUUID, validUUID, "organization_disabled"}
		if len(revoker.calls) != 1 || revoker.calls[0] != want {
			t.Fatalf("read saw active=%v: revoke calls %+v, want [%+v]", active, revoker.calls, want)
		}
		if commit, call := log.index("tx:commit"), log.index("acr-revoke"); commit < 0 || call < commit {
			t.Fatalf("order %v, want the commit before the acr revoke", log.steps)
		}
	}
}

// D2: a failed revoke after the stored disable is a 503 with the ruled text; the disable stays stored.
func TestPatchDisableAnswers503WhenTheACRRevokeFails(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		body string
	}{
		"acr down":          {&acrrevoke.Error{Kind: acrlisting.KindUnavailable, Status: http.StatusServiceUnavailable}, wantDisableACRRevokeBody},
		"organization live": {acrrevoke.ErrOrganizationLive, wantDisableACRRevokeBody},
		"exchange refused":  {&acrrevoke.Error{Kind: acrlisting.KindUnauthorized, Status: http.StatusUnauthorized}, wantDisableACRRevokeBody},
		"bound reached":     {acrrevoke.ErrOrganizationIncomplete, wantDisableACRRevokeIncompleteBody},
	} {
		log := &orderLog{}
		db := &lockDB{log: log, active: true}
		var logs bytes.Buffer
		code, body := patchOrg(disableHandlers(db, &recordingOrgRevoker{log: log, errs: []error{tc.err}}, &logs), `{"is_active": false}`)
		if code != http.StatusServiceUnavailable || strings.TrimSpace(body) != tc.body {
			t.Fatalf("%s: %d %s, want 503 %s", name, code, body, tc.body)
		}
		if db.active {
			t.Fatalf("%s: the disable was not stored", name)
		}
		if !strings.Contains(logs.String(), `"msg":"admin: org acr credential revoke failed"`) {
			t.Fatalf("%s: log %q, want the failure line", name, logs.String())
		}
		if name == "organization live" && !strings.Contains(logs.String(), `"decision":"organization_live"`) {
			t.Fatalf("log %q, want decision organization_live", logs.String())
		}
	}
}

// Q-O2: a repeat after the bound is reached continues the revoke and answers the PATCH.
func TestPatchDisableRepeatAfterTheBoundIsReachedFinishes(t *testing.T) {
	log := &orderLog{}
	db := &lockDB{log: log, active: true}
	revoker := &recordingOrgRevoker{log: log, errs: []error{acrrevoke.ErrOrganizationIncomplete}}
	var logs bytes.Buffer
	h := disableHandlers(db, revoker, &logs)
	if code, _ := patchOrg(h, `{"is_active": false}`); code != http.StatusServiceUnavailable {
		t.Fatalf("first PATCH %d, want 503", code)
	}
	code, body := patchOrg(h, `{"is_active": false}`)
	if answeredIsActive(t, code, body) || len(revoker.calls) != 2 {
		t.Fatalf("repeat %d %s, %d calls; want 200 and a second call", code, body, len(revoker.calls))
	}
}

// D3: with no acr revoke client the disable answers as before, with one ERROR line and one count.
func TestPatchDisableWithoutAnACRClientProceedsAndCounts(t *testing.T) {
	log := &orderLog{}
	db := &lockDB{log: log, active: true}
	var logs bytes.Buffer
	before := acrSkipCounts(t)
	code, body := patchOrg(disableHandlers(db, nil, &logs), `{"is_active": false}`)
	if answeredIsActive(t, code, body) {
		t.Fatal("answer is_active true")
	}
	after := acrSkipCounts(t)
	if after["disable/no_client"] != before["disable/no_client"]+1 {
		t.Fatalf("counter disable/no_client moved %d, want 1", after["disable/no_client"]-before["disable/no_client"])
	}
	lines := errorLines(t, &logs)
	if len(lines) != 1 || lines[0]["msg"] != "admin: org acr credential revoke not configured; acr refuses the credentials at use" ||
		lines[0]["org_id"] != validUUID || lines[0]["event"] != "disable" || lines[0]["reason"] != "no_client" {
		t.Fatalf("error lines %v, want the one no_client line", lines)
	}
}

// D4: reactivation and a PATCH without is_active make no acr call.
func TestPatchWithoutADisableMakesNoACRCall(t *testing.T) {
	for _, body := range []string{`{"is_active": true}`, `{"name": "Renamed"}`} {
		log := &orderLog{}
		db := &lockDB{log: log, active: false}
		revoker := &recordingOrgRevoker{log: log}
		var logs bytes.Buffer
		code, answer := patchOrg(disableHandlers(db, revoker, &logs), body)
		if code != http.StatusOK || len(revoker.calls) != 0 {
			t.Fatalf("%s: %d %s, %d acr calls; want 200 and none", body, code, answer, len(revoker.calls))
		}
	}
}

// deleteDB is a pgDB for the delete's first statements: the disable UPDATE answers rows (1 for a present
// organization), every other statement fails, so the step after the acr revoke is visible in the log.
type deleteDB struct {
	mu      sync.Mutex
	log     *orderLog
	rows    int64
	sqls    []string
	updated bool
}

func (d *deleteDB) record(sql string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	flat := strings.Join(strings.Fields(sql), " ")
	d.sqls = append(d.sqls, flat)
	return flat
}

func (d *deleteDB) Begin(context.Context) (pgx.Tx, error) {
	d.record("BEGIN")
	d.log.add("db:begin")
	return nil, errTouched
}

func (d *deleteDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	flat := d.record(sql)
	if strings.HasPrefix(flat, "UPDATE organizations SET is_active = false") {
		d.log.add("db:disable")
		d.updated = d.rows > 0
		if d.rows > 0 {
			return pgconn.NewCommandTag("UPDATE 1"), nil
		}
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}
	d.log.add("db:exec")
	return pgconn.CommandTag{}, errTouched
}

func (d *deleteDB) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	d.record(sql)
	d.log.add("db:query")
	return nil, errTouched
}

func (d *deleteDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	d.record(sql)
	d.log.add("db:select")
	return errRow{}
}

// closableConn is a ClickHouse connection the delete can close.
type closableConn struct{ countingConn }

func (*closableConn) Close() error { return nil }

func deleteHandlers(db *deleteDB, revoker OrgCredentialRevoker, logs *bytes.Buffer) *handlers {
	h := &handlers{store: pgStore{Pool: db, Now: time.Now}, logger: slog.New(slog.NewJSONHandler(logs, nil)),
		openOrgDeletionClickHouse: func(context.Context) (*orgDeletionClickHouse, error) {
			return &orgDeletionClickHouse{conn: driver.Conn(&closableConn{}), tables: []ChTable{{Name: "events", OrgIDType: "String"}}}, nil
		}}
	if revoker != nil {
		h.acrOrgRevoker = revoker
	}
	return h
}

func deleteOrg(h *handlers, query string, user *policy.User) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/orgs/"+validUUID+query, nil)
	request.SetPathValue("org_id", validUUID)
	request = request.WithContext(policy.WithUser(request.Context(), user))
	recorder := httptest.NewRecorder()
	h.deleteOrganization(recorder, request)
	return recorder
}

var deleteActor = &policy.User{IsSuperuser: true, UserID: "1c7f0a4e-5b2d-4e8f-9a1b-2c3d4e5f6a7b", ID: uuid.MustParse("1c7f0a4e-5b2d-4e8f-9a1b-2c3d4e5f6a7b")}

// X1: a real delete disables the organization, then asks acr to revoke its credentials for the acting admin with
// reason organization_deleted, before any count, PagerDuty revoke or purge.
func TestOrgDeleteDisablesThenRevokesTheACRCredentialsBeforeAnyPurge(t *testing.T) {
	log := &orderLog{}
	db := &deleteDB{log: log, rows: 1}
	revoker := &recordingOrgRevoker{log: log}
	var logs bytes.Buffer
	deleteOrg(deleteHandlers(db, revoker, &logs), "", deleteActor)
	want := orgRevokeCall{validUUID, deleteActor.ID.String(), "organization_deleted"}
	if len(revoker.calls) != 1 || revoker.calls[0] != want {
		t.Fatalf("revoke calls %+v, want [%+v]", revoker.calls, want)
	}
	if len(log.steps) < 3 || log.steps[0] != "db:disable" || log.steps[1] != "acr-revoke" {
		t.Fatalf("order %v, want the disable, then the acr revoke, then the rest", log.steps)
	}
	if !strings.HasPrefix(db.sqls[0], "UPDATE organizations SET is_active = false, updated_at = $2 WHERE id = $1") {
		t.Fatalf("first statement %q", db.sqls[0])
	}
}

// X2: a failed revoke stops the delete with the ruled text: the organization stays, disabled, and nothing else runs.
func TestOrgDeleteStopsBeforeAnyPurgeWhenTheACRRevokeFails(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		body string
	}{
		"acr down":          {&acrrevoke.Error{Kind: acrlisting.KindUnavailable, Status: http.StatusServiceUnavailable}, wantDeleteACRRevokeBody},
		"organization live": {acrrevoke.ErrOrganizationLive, wantDeleteACRRevokeBody},
		"bound reached":     {acrrevoke.ErrOrganizationIncomplete, wantDeleteACRRevokeIncompleteBody},
	} {
		log := &orderLog{}
		db := &deleteDB{log: log, rows: 1}
		var logs bytes.Buffer
		recorder := deleteOrg(deleteHandlers(db, &recordingOrgRevoker{log: log, errs: []error{tc.err}}, &logs), "", deleteActor)
		if recorder.Code != http.StatusServiceUnavailable || strings.TrimSpace(recorder.Body.String()) != tc.body {
			t.Fatalf("%s: %d %s, want 503 %s", name, recorder.Code, recorder.Body.String(), tc.body)
		}
		if strings.Join(log.steps, ",") != "db:disable,acr-revoke" || !db.updated {
			t.Fatalf("%s: steps %v, want only the disable and the call", name, log.steps)
		}
	}
}

// X2: a repeat after a failure runs the delete again from the disable and goes on.
func TestOrgDeleteRepeatAfterAFailedRevokeGoesOn(t *testing.T) {
	log := &orderLog{}
	db := &deleteDB{log: log, rows: 1}
	revoker := &recordingOrgRevoker{log: log, errs: []error{acrrevoke.ErrOrganizationIncomplete}}
	var logs bytes.Buffer
	h := deleteHandlers(db, revoker, &logs)
	if code := deleteOrg(h, "", deleteActor).Code; code != http.StatusServiceUnavailable {
		t.Fatalf("first delete %d, want 503", code)
	}
	*log = orderLog{}
	deleteOrg(h, "", deleteActor)
	if len(revoker.calls) != 2 || len(log.steps) < 3 || log.steps[0] != "db:disable" || log.steps[1] != "acr-revoke" {
		t.Fatalf("repeat: %d calls, steps %v; want the disable, the call, then the delete's next step", len(revoker.calls), log.steps)
	}
}

// X3: a dry run disables nothing and calls nothing; an organization with no row is not disabled and not called.
func TestOrgDeleteDryRunAndAMissingOrgMakeNoACRCall(t *testing.T) {
	for name, tc := range map[string]struct {
		query string
		rows  int64
	}{"dry run": {"?dry_run=true", 1}, "no organization row": {"", 0}} {
		log := &orderLog{}
		db := &deleteDB{log: log, rows: tc.rows}
		revoker := &recordingOrgRevoker{log: log}
		var logs bytes.Buffer
		deleteOrg(deleteHandlers(db, revoker, &logs), tc.query, deleteActor)
		if len(revoker.calls) != 0 || db.updated {
			t.Fatalf("%s: %d acr calls, disabled %v; want none", name, len(revoker.calls), db.updated)
		}
		if tc.query != "" && log.index("db:disable") != -1 {
			t.Fatalf("dry run wrote: %v", log.steps)
		}
	}
}

// X4: with no acr revoke client the delete disables the organization and goes on, with one ERROR line and one count.
func TestOrgDeleteWithoutAnACRClientProceedsAndCounts(t *testing.T) {
	log := &orderLog{}
	db := &deleteDB{log: log, rows: 1}
	var logs bytes.Buffer
	before := acrSkipCounts(t)
	deleteOrg(deleteHandlers(db, nil, &logs), "", deleteActor)
	after := acrSkipCounts(t)
	if after["delete/no_client"] != before["delete/no_client"]+1 {
		t.Fatalf("counter delete/no_client moved %d, want 1", after["delete/no_client"]-before["delete/no_client"])
	}
	if len(log.steps) < 2 || log.steps[0] != "db:disable" || log.index("acr-revoke") != -1 {
		t.Fatalf("steps %v, want the disable and then the delete's next step", log.steps)
	}
	var found bool
	for _, line := range errorLines(t, &logs) {
		if line["msg"] == "admin: org acr credential revoke not configured; acr refuses the credentials at use" && line["event"] == "delete" {
			found = true
		}
	}
	if !found {
		t.Fatalf("log %q, want the no_client line", logs.String())
	}
}

// The operator verb acts as "operator" and prints the ruled delete text.
func TestOperatorDeleteOrgRevokesAsTheOperatorAndRefusesWithTheRuledText(t *testing.T) {
	log := &orderLog{}
	db := &deleteDB{log: log, rows: 1}
	revoker := &recordingOrgRevoker{log: log, errs: []error{acrrevoke.ErrOrganizationLive}}
	var logs bytes.Buffer
	h := deleteHandlers(db, revoker, &logs)
	ctx := policy.WithUser(context.Background(), &policy.User{})
	_, err := h.deleteOrganizationData(ctx, uuid.MustParse(validUUID), false)
	if !errors.Is(err, errOrgDeletionACRRevoke) || orgDeletionACRRevokeAnswer(err) != orgDeletionACRRevokeDetail {
		t.Fatalf("err = %v", err)
	}
	if len(revoker.calls) != 1 || revoker.calls[0].actor != "operator" {
		t.Fatalf("calls %+v, want one by the operator actor", revoker.calls)
	}
	var detail struct{ Detail string }
	if json.Unmarshal([]byte(wantDeleteACRRevokeBody), &detail) != nil || detail.Detail != orgDeletionACRRevokeDetail {
		t.Fatal("the pinned body is not the detail")
	}
}

// D5/D6: with the org-provisioning switch on, the stored state is auth-service's. A PATCH true that auth keeps
// suspended stores is_active false, and a PATCH false that auth keeps active stores true: neither is a stored
// disable, so neither calls acr.
func TestPatchWithTheSwitchOnRevokesOnlyAStoredDisable(t *testing.T) {
	for name, tc := range map[string]struct {
		body   string
		active bool
		state  []orgreactivate.State
		stored bool
	}{
		"reactivation auth keeps suspended": {`{"is_active": true, "name": "Renamed"}`, false,
			[]orgreactivate.State{{Status: "suspended", Revision: 1}, {Status: "suspended", Revision: 3}}, false},
		"disable auth keeps active": {`{"is_active": false, "name": "Renamed"}`, true,
			[]orgreactivate.State{{Status: "active", Revision: 1}, {Status: "active", Revision: 3}}, true},
	} {
		log := &orderLog{}
		db := &lockDB{log: log, active: tc.active}
		var logs bytes.Buffer
		h := lockHandlers(db, &fakeLifecycle{log: log}, &fakeAuthState{log: log, seq: tc.state}, &logs)
		revoker := &recordingOrgRevoker{log: log}
		h.acrOrgRevoker = revoker
		code, body := patchOrg(h, tc.body)
		if got := answeredIsActive(t, code, body); got != tc.stored {
			t.Fatalf("%s: answer is_active %v, want %v", name, got, tc.stored)
		}
		if len(revoker.calls) != 0 {
			t.Fatalf("%s: acr calls %+v, want none: no disable was stored", name, revoker.calls)
		}
	}
}
