package policy

import (
	"bytes"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// A request with no user access token on an enforced row is refused before the unconfigured and impersonation branches:
// one error_deny, one ERROR log, no decision point question, no unjudged count, and the plain refusal body.
func TestATokenlessRequestOnAnEnforcedRowIsRefusedBeforeEveryOtherBranch(t *testing.T) {
	rows := productionGoAPIRows(t)
	namespaces := catalogNamespaces()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	rec := recordEnforce(t)
	pdp := newTestPDP(t, allowAll)
	configured, err := NewEnforcer(EnforceConfig{Decider: pdp.client(t), Rows: rows, Switch: onFor(namespaces...), Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	unconfigured, err := NewEnforcer(EnforceConfig{Unconfigured: true, Rows: rows, Switch: onFor(namespaces...), Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	answer := func(row ShadowRow, verdict enforceVerdict) (int, string) {
		w := &bufferedAnswer{header: http.Header{}}
		writeEnforceRefusal(w, nil, Admin, verdict, DenyText{})
		return w.status, w.body.String()
	}
	modes := []struct {
		name          string
		enf           *Enforcer
		impersonating bool
		read          bool
		platformWrap  bool
	}{
		{"plain", configured, false, true, false},
		{"unconfigured", unconfigured, false, true, false},
		{"impersonation read", configured, true, true, false},
		{"impersonation write", configured, true, false, false},
		{"impersonation platform", configured, true, true, true},
	}
	inputs := []struct {
		name   string
		bearer string
		token  bool
	}{
		{"no bearer, no token", "", false},
		{"bearer, not a user token", shadowBearer, false},
		{"user token flag, no bearer", "", true},
	}
	plainStatus, plainBody := answer(ShadowRow{}, enforceDeny)
	if plainStatus != http.StatusForbidden {
		t.Fatalf("the plain refusal is %d, want 403", plainStatus)
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			checked := 0
			for pattern, row := range rows {
				if liveRowExempt(pattern) {
					continue
				}
				for _, in := range inputs {
					logs.Reset()
					before := len(rec.snapshot())
					verdict := mode.enf.decide(t.Context(), enforceObservation{pattern: pattern, bearer: in.bearer, userAccessToken: in.token,
						impersonating: mode.impersonating, read: mode.read, platformWrap: mode.platformWrap})
					if verdict != enforceDeny {
						t.Fatalf("%s [%s]: verdict %v, want enforceDeny", pattern, in.name, verdict)
					}
					if status, body := answer(row, verdict); status != plainStatus || body != plainBody {
						t.Fatalf("%s [%s]: answer %d %q, want %d %q", pattern, in.name, status, body, plainStatus, plainBody)
					}
					if got, want := rec.snapshot()[before:], []string{row.Namespace + "/" + enforceOutcomeErrorDeny}; !slices.Equal(got, want) {
						t.Fatalf("%s [%s]: outcomes %v, want %v", pattern, in.name, got, want)
					}
					if strings.Count(logs.String(), "level=ERROR") != 1 || strings.Count(logs.String(), "level=") != 1 {
						t.Fatalf("%s [%s]: log lines %q, want exactly one ERROR", pattern, in.name, logs.String())
					}
					checked++
				}
			}
			if checked < 3*100 {
				t.Fatalf("only %d inputs checked", checked)
			}
		})
	}
	if u := rec.unjudgedSnapshot(); len(u) != 0 {
		t.Fatalf("unjudged counts %v, want none", u)
	}
	if calls := pdp.calls(); len(calls) != 0 {
		t.Fatalf("the decision point saw %d questions, want none", len(calls))
	}
}
