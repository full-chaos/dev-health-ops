package datahealth

import (
	"context"
	"errors"
	"testing"
)

func teamRowsCH(teams ...string) *routingCH {
	var rows [][]any
	for _, id := range teams {
		rows = append(rows, []any{id})
	}
	return &routingCH{rules: []chRule{{match: "FROM teams FINAL", rows: rows}}}
}

func TestResolveTeam(t *testing.T) {
	str := func(s string) *string { return &s }
	cases := []struct {
		name       string
		requested  *string
		wantTeam   string
		wantLegacy string
	}{
		{"nil", nil, "", ""},
		{"empty", str(""), "", ""},
		{"ALL", str("ALL"), "", LegacyKindAll},
		{"default", str("default"), "", LegacyKindDefault},
		{"own org id", str("org-1"), "", LegacyKindOrgID},
		{"real team", str("team-a"), "team-a", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			team, legacy, err := (&Reader{ClickHouse: teamRowsCH("team-a")}).ResolveTeam(context.Background(), "org-1", tc.requested)
			if err != nil || team != tc.wantTeam || legacy != tc.wantLegacy {
				t.Fatalf("team=%q legacy=%q err=%v, want %q / %q", team, legacy, err, tc.wantTeam, tc.wantLegacy)
			}
		})
	}
	if _, _, err := (&Reader{ClickHouse: teamRowsCH()}).ResolveTeam(context.Background(), "org-1", str("team-x")); !errors.Is(err, ErrUnknownTeam) {
		t.Fatalf("an unknown team must be ErrUnknownTeam, got %v", err)
	}
	if _, _, err := (&Reader{ClickHouse: &routingCH{rules: []chRule{{match: "FROM teams FINAL", err: errors.New("boom")}}}}).ResolveTeam(context.Background(), "org-1", str("team-a")); err == nil || errors.Is(err, ErrUnknownTeam) {
		t.Fatalf("a failed lookup must fail the field, not refuse the team: %v", err)
	}
}
