package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestConfigurationRejectedReasonIsAClosedCode(t *testing.T) {
	t.Parallel()
	rows := []struct {
		err  error
		want string
	}{
		{ErrDomainDatabaseRequired, "domain_dsn_missing"},
		{ErrQueueControlRequired, "queue_dsn_missing"},
		{ErrCoordinatorDatabaseRequired, "coordinator_dsn_missing"},
		{ErrQueueControlTransactionMode, "queue_control_transaction_mode"},
		{ErrCoordinatorTransactionMode, "coordinator_transaction_mode"},
		{ErrRuntimeRolesNotSeparated, "runtime_roles_not_separated"},
		{ErrRuntimeRoleConfiguration, "runtime_role_configuration"},
		{ErrInvalidConfig, "invalid_config"},
		{fmt.Errorf("wrapped: %w", ErrQueueControlTransactionMode), "queue_control_transaction_mode"},
		{errors.New("something else"), "configuration_rejected"},
		{nil, "configuration_rejected"},
	}
	for _, row := range rows {
		if got := ConfigurationRejectedReason(row.err); got != row.want {
			t.Fatalf("ConfigurationRejectedReason(%v) = %q, want %q", row.err, got, row.want)
		}
	}
	planted := fmt.Errorf("cannot parse postgres://leak-user:leak-pass@leak-host.example/db: %w", ErrInvalidConfig)
	got := ConfigurationRejectedReason(planted)
	for _, marker := range []string{"leak-user", "leak-pass", "leak-host", "postgres://"} {
		if strings.Contains(got, marker) {
			t.Fatalf("reason %q carries %q", got, marker)
		}
	}
}
