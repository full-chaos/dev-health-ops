package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

var (
	deprovisionReaderOnce sync.Once
	deprovisionReader     *sdkmetric.ManualReader
)

// The global meter provider delegates to its first SDK only, so the cells read the counter through one reader and
// compare deltas.
func deprovisionSkipCounts(t *testing.T) map[string]int64 {
	t.Helper()
	deprovisionReaderOnce.Do(func() {
		deprovisionReader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(deprovisionReader)))
	})
	var collected metricdata.ResourceMetrics
	if err := deprovisionReader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "devhealth_api_orgdelete_auth_deprovision_skipped_total" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				reason, _ := point.Attributes.Value("reason")
				counts[reason.AsString()] += point.Value
			}
		}
	}
	return counts
}

// recordingDeprovisioner answers err and records each call.
type recordingDeprovisioner struct {
	err   error
	calls []string
}

func (d *recordingDeprovisioner) Deprovision(_ context.Context, org, actor uuid.UUID) error {
	d.calls = append(d.calls, org.String()+"/"+actor.String())
	return d.err
}

// recordingProvisioner is the switch-on provisioner: only its Deprovision is reached here.
type recordingProvisioner struct {
	authprovision.Provisioner
	recordingDeprovisioner
}

func (p *recordingProvisioner) Deprovision(ctx context.Context, org, actor uuid.UUID) error {
	return p.recordingDeprovisioner.Deprovision(ctx, org, actor)
}

const deprovisionSecretText = "orgdel-unit-secret-1b7e"

func errorLines(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if entry["level"] == "ERROR" {
			out = append(out, entry)
		}
	}
	return out
}

func deprovisionCtx() (context.Context, uuid.UUID) {
	actor := uuid.New()
	return policy.WithUser(context.Background(), &policy.User{ID: actor}), actor
}

// With AUTH_PROXY_ORG_PROVISIONING off and an auth-service configured, every outcome of the deprovision call lets the
// delete go on: success and organization_not_found log nothing; each other outcome logs ONE error line with the org
// id and its own reason, counts that reason once, and never carries a secret or the auth answer.
func TestOrgDeleteSwitchOffDeprovisionNeverStopsTheDelete(t *testing.T) {
	cases := []struct {
		name   string
		client OrgDeprovisioner
		err    error
		reason string
		code   string
	}{
		{name: "deprovisioned", err: nil},
		{name: "not held by auth", err: &authprovision.Refused{Status: http.StatusNotFound, Reason: "organization_not_found"}},
		{name: "no client", reason: deprovisionSkipNoClient},
		{name: "routes off", err: &authprovision.Refused{Status: http.StatusNotFound}, reason: deprovisionSkipRoutesOff},
		{name: "users not owned", err: &authprovision.Refused{Status: http.StatusConflict, Reason: "users_not_owned"}, reason: deprovisionSkipUsersNotOwned, code: "users_not_owned"},
		{name: "other refusal", err: &authprovision.Refused{Status: http.StatusForbidden, Reason: "superuser_required"}, reason: deprovisionSkipRefused, code: "superuser_required"},
		{name: "auth 5xx", err: fmt.Errorf("%w: status 503 %s", authprovision.ErrUnavailable, deprovisionSecretText), reason: deprovisionSkipAuthUnavailable},
		{name: "timeout", err: fmt.Errorf("%w: %w", authprovision.ErrUnavailable, context.DeadlineExceeded), reason: deprovisionSkipAuthUnavailable},
		{name: "transport", err: errors.New("dial tcp: connection refused " + deprovisionSecretText), reason: deprovisionSkipAuthUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			h := &handlers{logger: slog.New(slog.NewJSONHandler(&logs, nil)), orgDeletionAuth: true}
			fake := &recordingDeprovisioner{err: tc.err}
			if tc.reason != deprovisionSkipNoClient {
				h.orgDeprovision = fake
			}
			ctx, actor := deprovisionCtx()
			org := uuid.New()
			before := deprovisionSkipCounts(t)
			if err := h.deprovisionBeforeDelete(ctx, org); err != nil {
				t.Fatalf("deprovisionBeforeDelete = %v, want nil: the delete goes on", err)
			}
			if tc.reason != deprovisionSkipNoClient && (len(fake.calls) != 1 || fake.calls[0] != org.String()+"/"+actor.String()) {
				t.Fatalf("deprovision calls = %v, want one for the org and the acting user", fake.calls)
			}
			after := deprovisionSkipCounts(t)
			lines := errorLines(t, &logs)
			if tc.reason == "" {
				if len(lines) != 0 {
					t.Fatalf("error lines = %v, want none", lines)
				}
				for reason, n := range after {
					if n != before[reason] {
						t.Fatalf("counter %s moved by %d, want 0", reason, n-before[reason])
					}
				}
				return
			}
			if len(lines) != 1 {
				t.Fatalf("error lines = %d (%v), want exactly 1", len(lines), lines)
			}
			line := lines[0]
			if line["msg"] != "admin: org deletion left the auth organization live" || line["org_id"] != org.String() ||
				line["reason"] != tc.reason || line["reason_code"] != tc.code {
				t.Fatalf("error line = %v, want org_id %s, reason %s, reason_code %q", line, org, tc.reason, tc.code)
			}
			if strings.Contains(logs.String(), deprovisionSecretText) {
				t.Fatal("the error line carries the auth error text")
			}
			for reason, n := range after {
				want := before[reason]
				if reason == tc.reason {
					want++
				}
				if n != want {
					t.Fatalf("counter %s = %d, want %d", reason, n, want)
				}
			}
			if after[tc.reason] != before[tc.reason]+1 {
				t.Fatalf("counter %s did not move by 1", tc.reason)
			}
		})
	}
}

// With no auth-service configured (AUTH_PROXY_URL unset) the delete makes no call, logs nothing and counts nothing,
// as before; a dry run never reaches the step.
func TestOrgDeleteWithoutAuthServiceMakesNoCall(t *testing.T) {
	var logs bytes.Buffer
	fake := &recordingDeprovisioner{err: errors.New("must not be called")}
	h := &handlers{logger: slog.New(slog.NewJSONHandler(&logs, nil)), orgDeprovision: fake}
	ctx, _ := deprovisionCtx()
	before := deprovisionSkipCounts(t)
	if err := h.deprovisionBeforeDelete(ctx, uuid.New()); err != nil {
		t.Fatal(err)
	}
	if len(fake.calls) != 0 || len(errorLines(t, &logs)) != 0 {
		t.Fatalf("calls %v, log %q: want no call and no error line", fake.calls, logs.String())
	}
	after := deprovisionSkipCounts(t)
	for reason, n := range after {
		if n != before[reason] {
			t.Fatalf("counter %s moved", reason)
		}
	}
}

// With AUTH_PROXY_ORG_PROVISIONING on the step is today's: any refusal or failure stops the delete (the route's 503),
// organization_not_found lets it go on, and the switch-off client is never used.
func TestOrgDeleteSwitchOnRefusalStillStopsTheDelete(t *testing.T) {
	for name, tc := range map[string]struct {
		err   error
		stops bool
	}{
		"refused":     {err: &authprovision.Refused{Status: http.StatusConflict, Reason: "users_not_owned"}, stops: true},
		"unavailable": {err: fmt.Errorf("%w: status 503", authprovision.ErrUnavailable), stops: true},
		"routes off":  {err: &authprovision.Refused{Status: http.StatusNotFound}, stops: true},
		"not held":    {err: &authprovision.Refused{Status: http.StatusNotFound, Reason: "organization_not_found"}},
		"success":     {},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			on := &recordingProvisioner{recordingDeprovisioner: recordingDeprovisioner{err: tc.err}}
			off := &recordingDeprovisioner{}
			h := &handlers{logger: slog.New(slog.NewJSONHandler(&logs, nil)), orgProvision: on, orgDeletionAuth: true, orgDeprovision: off}
			ctx, _ := deprovisionCtx()
			before := deprovisionSkipCounts(t)
			err := h.deprovisionBeforeDelete(ctx, uuid.New())
			var denied *orgDeletionAuthError
			if tc.stops != errors.As(err, &denied) {
				t.Fatalf("deprovisionBeforeDelete = %v, want stop=%v (an *orgDeletionAuthError)", err, tc.stops)
			}
			if !tc.stops && err != nil {
				t.Fatalf("deprovisionBeforeDelete = %v, want nil", err)
			}
			if len(on.calls) != 1 || len(off.calls) != 0 {
				t.Fatalf("switch-on calls %v, switch-off calls %v: want 1 and 0", on.calls, off.calls)
			}
			after := deprovisionSkipCounts(t)
			for reason, n := range after {
				if n != before[reason] {
					t.Fatalf("switch on moved the switch-off counter %s", reason)
				}
			}
		})
	}
}
