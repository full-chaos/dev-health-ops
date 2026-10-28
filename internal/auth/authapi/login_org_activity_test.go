package authapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/orgactivity"
)

type activityUsers struct {
	fakeUsers
	activities map[uuid.UUID]orgactivity.Activity
	err        error
}

func (a activityUsers) OrgActivity(_ context.Context, ids []uuid.UUID) (map[uuid.UUID]orgactivity.Activity, error) {
	if a.err != nil {
		return nil, a.err
	}
	out := map[uuid.UUID]orgactivity.Activity{}
	for _, id := range ids {
		if known, ok := a.activities[id]; ok {
			out[id] = known
		} else {
			out[id] = orgactivity.Activity{Unknown: true}
		}
	}
	return out, nil
}

func loginChoice(t *testing.T, users Users, memberships []Membership, requested *uuid.UUID) (*Membership, string) {
	t.Helper()
	var logs bytes.Buffer
	h := compatHandlers{CompatDeps: CompatDeps{Users: users}, rec: recorder{logger: slog.New(slog.NewTextHandler(&logs, nil))}}
	return h.chooseLoginMembership(context.Background(), memberships, requested), logs.String()
}

func TestLoginTakesTheOrganizationWithMetricsNotTheEarliestJoin(t *testing.T) {
	first, second := uuid.New(), uuid.New()
	joined := func(day int) *time.Time { v := time.Date(2026, 1, day, 0, 0, 0, 0, time.UTC); return &v }
	memberships := []Membership{
		{OrgID: first, JoinedAt: joined(1), CreatedAt: *joined(1)},
		{OrgID: second, JoinedAt: joined(2), CreatedAt: *joined(2)},
	}
	last := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	users := activityUsers{activities: map[uuid.UUID]orgactivity.Activity{
		first:  {},
		second: {HasData: true, Last: &last},
	}}
	got, logs := loginChoice(t, users, memberships, nil)
	if got == nil || got.OrgID != second {
		t.Fatalf("login took %v, want the organization with metrics %v", got, second)
	}
	if logs != "" {
		t.Fatalf("a known activity logged: %s", logs)
	}
}

func TestLoginFallsBackToTheEarliestJoinWhenActivityIsUnknown(t *testing.T) {
	first, second := uuid.New(), uuid.New()
	joined := func(day int) *time.Time { v := time.Date(2026, 1, day, 0, 0, 0, 0, time.UTC); return &v }
	memberships := []Membership{
		{OrgID: first, JoinedAt: joined(1), CreatedAt: *joined(1)},
		{OrgID: second, JoinedAt: joined(2), CreatedAt: *joined(2)},
	}
	last := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	for name, users := range map[string]activityUsers{
		"no row for one organization":  {activities: map[uuid.UUID]orgactivity.Activity{second: {HasData: true, Last: &last}}},
		"the projection is unreadable": {err: errors.New("boom")},
	} {
		t.Run(name, func(t *testing.T) {
			got, logs := loginChoice(t, users, memberships, nil)
			if got == nil || got.OrgID != first {
				t.Fatalf("login took %v, want the earliest join %v", got, first)
			}
			if !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "active organization chosen by earliest join") {
				t.Fatalf("no WARN for the fallback: %q", logs)
			}
			if strings.Contains(logs, first.String()) || strings.Contains(logs, "boom") {
				t.Fatalf("the WARN names an id or the error text: %q", logs)
			}
		})
	}
}

func TestLoginKeepsTheRequestedOrganizationAndTheSingleMembership(t *testing.T) {
	first, second := uuid.New(), uuid.New()
	memberships := []Membership{{OrgID: first, CreatedAt: time.Now()}, {OrgID: second, CreatedAt: time.Now().Add(time.Hour)}}
	users := activityUsers{err: errors.New("never read")}
	if got, _ := loginChoice(t, users, memberships, &second); got == nil || got.OrgID != second {
		t.Fatalf("requested org: got %v", got)
	}
	if got, logs := loginChoice(t, users, memberships[:1], nil); got == nil || got.OrgID != first || logs != "" {
		t.Fatalf("one membership: got %v logs %q (the projection must not be read)", got, logs)
	}
	if got, _ := loginChoice(t, fakeUsers{}, memberships, nil); got == nil || got.OrgID != first {
		t.Fatalf("a Users without the activity reader: got %v, want the earliest join", got)
	}
}

// The fallback WARN carries the number of memberships and nothing else: no organization id, no user, no
// email, no name, no error text.
func TestLoginFallbackWarnCarriesOnlyTheMembershipCount(t *testing.T) {
	first, second := uuid.New(), uuid.New()
	memberships := []Membership{{OrgID: first, CreatedAt: time.Now()}, {OrgID: second, CreatedAt: time.Now().Add(time.Hour)}}
	for name, users := range map[string]activityUsers{
		"no row":     {},
		"read error": {err: errors.New("boom for user@example.test")},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			h := compatHandlers{CompatDeps: CompatDeps{Users: users}, rec: recorder{logger: slog.New(slog.NewJSONHandler(&logs, nil))}}
			_ = h.chooseLoginMembership(context.Background(), memberships, nil)
			var record map[string]any
			if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
				t.Fatalf("one JSON log record wanted: %v: %q", err, logs.String())
			}
			delete(record, "time")
			if len(record) != 3 || record["level"] != "WARN" || record["memberships"] != float64(2) || record["msg"] == nil {
				t.Fatalf("WARN fields = %v, want exactly level, msg, memberships=2", record)
			}
		})
	}
}
