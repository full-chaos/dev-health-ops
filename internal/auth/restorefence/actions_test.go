package restorefence

import (
	"strings"
	"testing"
)

func TestActionLinesCarryOnlyKindSourceCountAndIDs(t *testing.T) {
	lines := ActionLines([]OperatorAction{
		{Kind: ActionForcePasswordReset, Source: "public.users", IDs: []string{"00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000001"}},
		{Kind: ActionRequeueProviderRevocation, Source: "public.provider_oauth_revocations", IDs: []string{"00000000-0000-4000-8000-000000000009"}},
		{Kind: ActionForcePasswordReset, Source: "auth.users", IDs: nil},
	})
	if len(lines) != 2 {
		t.Fatalf("lines = %v, want one per action that has ids", lines)
	}
	if !strings.HasPrefix(lines[0], "operator-action force_password_reset source=public.users count=2 ids=00000000-0000-4000-8000-000000000001,00000000-0000-4000-8000-000000000002") {
		t.Errorf("ids are not listed in order: %s", lines[0])
	}
	for _, l := range lines {
		if strings.Contains(l, "hash") || strings.Contains(l, "token") && !strings.Contains(l, "provider") {
			t.Errorf("a line names credential material: %s", l)
		}
	}
}

func TestActionsOfEverySourceAreMergedByKindAndSource(t *testing.T) {
	got := mergeActions([]OperatorAction{
		{Kind: ActionForcePasswordReset, Source: "public.users", IDs: []string{"b"}},
		{Kind: ActionForcePasswordReset, Source: "public.users", IDs: []string{"a", "b"}},
	})
	if len(got) != 1 || strings.Join(got[0].IDs, ",") != "a,b" {
		t.Fatalf("merged = %+v", got)
	}
}
