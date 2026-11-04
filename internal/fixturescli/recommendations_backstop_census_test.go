package fixturescli

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestBackstopOpenFailureHoldsNoLoginOfAnUnparsableDSN(t *testing.T) {
	for form, dsn := range map[string]string{
		"uri":       "postgres://census_login_q7x9:census_password_q7x9@[::1",
		"key=value": "user=census_login_q7x9 password=census_password_q7x9 host=[::1 sslmode=bogus",
	} {
		_, err := startRecommendationsBackstop(context.Background(), slog.New(slog.DiscardHandler), dsn, "00000000-0000-0000-0000-000000000001")
		if err == nil {
			t.Fatalf("%s: no error for an unparsable DSN", form)
		}
		for _, leaked := range []string{"census_login_q7x9", "census_password_q7x9", "[::1"} {
			if strings.Contains(err.Error(), leaked) {
				t.Errorf("%s: the error holds %q: %v", form, leaked, err)
			}
		}
	}
}
