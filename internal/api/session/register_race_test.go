package session

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsUniqueViolationOnlyForSQLState23505(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"unique violation", &pgconn.PgError{Code: "23505"}, true},
		{"wrapped unique violation", fmt.Errorf("insert user: %w", &pgconn.PgError{Code: "23505"}), true},
		{"foreign key violation", &pgconn.PgError{Code: "23503"}, false},
		{"plain error", errors.New("23505"), false},
		{"nil", nil, false},
	} {
		if got := isUniqueViolation(tc.err); got != tc.want {
			t.Errorf("%s: isUniqueViolation = %v, want %v", tc.name, got, tc.want)
		}
	}
}
