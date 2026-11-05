package shadowvenue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/accessrouting"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// allowPDP answers allow to every call, each with its own decision id.
func allowPDP(t *testing.T) *pep.Client {
	t.Helper()
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Resource struct {
				Org *string `json:"organization_id"`
			} `json:"resource"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		var org any
		if body.Resource.Org != nil {
			org = *body.Resource.Org
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"schema_version": "authorization_decision.v1", "decision_id": fmt.Sprintf("dec_%026d", n.Add(1)), "allowed": true,
			"reason_code": "allowed_by_role", "principal_id": "prn_TESTPRINCIPAL", "organization_id": org, "policy_revision": 1,
			"expires_at": time.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		})
	}))
	t.Cleanup(srv.Close)
	client, err := pep.New(pep.Config{BaseURL: srv.URL, AllowPlainHTTP: true, HTTPClient: srv.Client(), ClientService: accessrouting.Service, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// The judge reads what the real producer writes: go-api's Shadow, built from the embedded go-api profile rows,
// observes three slots and its JSON log is judged. A member the legacy guard refuses on GET /api/v1/admin/teams is a
// mismatch whose tuple comes from the WARN line; an admin it serves is a match with a decision id.
func TestJudgeReadsTheRealShadowLines(t *testing.T) {
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, accessrouting.Service)
	if err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	shadow, err := policy.NewShadow(policy.ShadowConfig{Decider: allowPDP(t), Rows: rows, HighRisk: true,
		Logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))})
	if err != nil {
		t.Fatal(err)
	}
	const grace = 200 * time.Millisecond
	cells := []struct {
		row, caller, route string
		legacy             bool
	}{
		{"C2", "member", "/api/v1/admin/teams", false},
		{"58", "admin", "/api/v1/admin/teams", true},
		{"61", "admin", "/api/v1/admin/teams/{team_id}", true},
	}
	var slots []Slot
	for i, cell := range cells {
		lines := strings.Count(logs.String(), MsgResult)
		slot := Slot{Seq: i + 1, Row: cell.row, Caller: cell.caller, Principal: cell.caller, Expect: "match", K: 1, Method: "GET", Route: cell.route,
			Path: cell.route, Start: time.Now().UTC(), Status: 200}
		if strings.HasPrefix(cell.row, "C") {
			slot.Expect = "mismatch"
		}
		shadow.Observe(context.Background(), policy.ShadowObservation{Pattern: "GET " + cell.route, Bearer: "bearer-not-logged", OrgID: testOrgA,
			Caller: cell.caller, UserAccessToken: true, LegacyAllowed: cell.legacy})
		deadline := time.Now().Add(3 * time.Second)
		for strings.Count(logs.String(), MsgResult) == lines && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		slot.End = time.Now().UTC()
		slots = append(slots, slot)
		time.Sleep(grace + 100*time.Millisecond)
	}
	if strings.Contains(logs.String(), "bearer-not-logged") {
		t.Fatal("the shadow logged the bearer")
	}
	ev, err := ParseLog(strings.NewReader(logs.String()))
	if err != nil {
		t.Fatal(err)
	}
	report := Judge(slots, ev, nil, JudgeOptions{K: 1, Grace: grace})
	got := []string{report.Slots[0].Verdict, report.Slots[1].Verdict, report.Slots[2].Verdict}
	want := []string{VerdictMismatch, VerdictMatch, VerdictMatch}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("verdicts %v, want %v\nlog:\n%s", got, want, logs.String())
	}
	tuple := report.Slots[0].Tuple
	if tuple.Legacy != "deny" || tuple.Shadow != "allow" || tuple.Reason != "allowed_by_role" || tuple.Class == "" {
		t.Fatalf("tuple %+v", tuple)
	}
	if report.Slots[1].DecisionID == "" || report.Slots[1].DecisionID == report.Slots[2].DecisionID {
		t.Fatalf("decision ids %q %q", report.Slots[1].DecisionID, report.Slots[2].DecisionID)
	}
}
