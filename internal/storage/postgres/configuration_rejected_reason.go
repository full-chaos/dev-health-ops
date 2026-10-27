package postgres

import "errors"

// ConfigurationRejectedReason maps a rejected runtime configuration to a closed
// reason code that is safe to log. It never returns err's text, which may carry
// DSN parts when the rejection came from a parser.
func ConfigurationRejectedReason(err error) string {
	for _, entry := range []struct {
		sentinel error
		reason   string
	}{
		{ErrDomainDatabaseRequired, "domain_dsn_missing"},
		{ErrQueueControlRequired, "queue_dsn_missing"},
		{ErrCoordinatorDatabaseRequired, "coordinator_dsn_missing"},
		{ErrQueueControlTransactionMode, "queue_control_transaction_mode"},
		{ErrCoordinatorTransactionMode, "coordinator_transaction_mode"},
		{ErrRuntimeRolesNotSeparated, "runtime_roles_not_separated"},
		{ErrRuntimeRoleConfiguration, "runtime_role_configuration"},
		{ErrInvalidConfig, "invalid_config"},
	} {
		if errors.Is(err, entry.sentinel) {
			return entry.reason
		}
	}
	return "configuration_rejected"
}
