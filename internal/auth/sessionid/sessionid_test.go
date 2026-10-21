package sessionid

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestWireRoundTrips(t *testing.T) {
	for range 100 {
		id := uuid.New()
		got, err := Parse(Wire(id))
		if err != nil || got != id {
			t.Fatalf("Parse(Wire(%s)) = %s, %v", id, got, err)
		}
	}
	for _, bad := range []string{"", "ses_", "ses_" + "AAAA", uuid.NewString(), "prn_" + Wire(uuid.New())[4:], "ses_" + Wire(uuid.New())[4:] + "A", "ses_!!!!!!!!!!!!!!!!!!!!!!"} {
		if _, err := Parse(bad); !errors.Is(err, ErrMalformed) {
			t.Errorf("Parse(%q) err = %v, want ErrMalformed", bad, err)
		}
	}
}
