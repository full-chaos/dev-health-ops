package restorefence

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
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
	if !strings.HasSuffix(lines[0], " follow-up=none") || !strings.HasSuffix(lines[1], " follow-up="+requeueRunbookStep) {
		t.Errorf("each action names its follow-up leaf: %v", lines)
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

type bytesRow struct {
	raw []byte
	err error
}

func (r bytesRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	switch d := dest[0].(type) {
	case *string:
		*d = walFileOnTimeline2
	case *[]byte:
		*d = r.raw
	}
	return nil
}

type bytesQuerier struct{ row bytesRow }

func (q bytesQuerier) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row { return q.row }

func TestPendingActionsReadTheStoredListAndFailClosedOnABadRead(t *testing.T) {
	got, err := PendingActions(context.Background(), bytesQuerier{bytesRow{raw: []byte(`[{"kind":"force_password_reset","source":"public.users","ids":["a"]}]`)}}, testSchema(t))
	if err != nil || len(got) != 1 || got[0].Kind != ActionForcePasswordReset || got[0].IDs[0] != "a" {
		t.Fatalf("PendingActions = %+v, %v", got, err)
	}
	if _, err := PendingActions(context.Background(), bytesQuerier{bytesRow{raw: []byte(`not json`)}}, testSchema(t)); !errors.Is(err, ErrCheckFailed) {
		t.Fatalf("a bad stored list = %v, want ErrCheckFailed", err)
	}
	if _, err := PendingActions(context.Background(), bytesQuerier{bytesRow{err: errors.New("down")}}, testSchema(t)); !errors.Is(err, ErrCheckFailed) {
		t.Fatalf("an unreadable state = %v, want ErrCheckFailed", err)
	}
}
