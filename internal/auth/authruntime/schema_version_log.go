package authruntime

import (
	"context"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// logSchemaChecked writes the one line a roll readback reads from the service
// log. It names the newest migration this binary embeds. It does not name the
// applied version: the runtime role cannot read the lineage table. The
// "schema" and "embedded_head_version" keys must stay free of the protected
// key words in internal/platform/logging (an "auth_" prefix would be masked).
func logSchemaChecked(ctx context.Context, logger *slog.Logger, schema string) {
	head, err := authschema.HeadVersion()
	if err != nil {
		logger.WarnContext(ctx, "auth schema head unreadable", "schema", schema)
		return
	}
	logger.InfoContext(ctx, "auth schema checked", "schema", schema, "embedded_head_version", head)
}
