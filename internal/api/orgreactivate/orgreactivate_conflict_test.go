package orgreactivate

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

// Only a 409 whose reason is state_changed, with a revision sent, is a stale revision; any other 409 is a refusal.
func TestOnlyAStateChangedConflictIsErrStateChanged(t *testing.T) {
	seven := int64(7)
	for name, tc := range map[string]struct {
		body string
		rev  *int64
		want error
		not  error
	}{
		"state_changed with a revision":         {`{"reason_code":"state_changed"}`, &seven, ErrStateChanged, ErrRefused},
		"organization_conflict with a revision": {`{"reason_code":"organization_conflict"}`, &seven, ErrRefused, ErrStateChanged},
		"no reason with a revision":             {`{}`, &seven, ErrRefused, ErrStateChanged},
		"not json with a revision":              {`nope`, &seven, ErrRefused, ErrStateChanged},
		"state_changed with no revision":        {`{"reason_code":"state_changed"}`, nil, ErrRefused, ErrStateChanged},
		"organization_conflict, no revision":    {`{"reason_code":"organization_conflict"}`, nil, ErrRefused, ErrStateChanged},
	} {
		client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(tc.body))
		})
		for verb, err := range map[string]error{
			"reactivate": client.Reactivate(context.Background(), uuid.New(), uuid.New(), tc.rev),
			"suspend":    client.Suspend(context.Background(), uuid.New(), uuid.New(), tc.rev),
		} {
			if !errors.Is(err, tc.want) || errors.Is(err, tc.not) || errors.Is(err, ErrUnavailable) {
				t.Errorf("%s %s: err = %v, want %v and not %v", name, verb, err, tc.want, tc.not)
			}
		}
	}
}
