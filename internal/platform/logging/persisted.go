package logging

import (
	"context"
	"log/slog"
)

// RedactPersisted is RedactText for text a writer stores in a database column. When the
// redactor changed the text it writes one debug line with the column name and byte counts,
// never the text. column is a fixed code label, not data.
func RedactPersisted(ctx context.Context, logger *slog.Logger, column, text string) string {
	redacted := RedactText(text)
	if redacted != text {
		if logger == nil {
			logger = slog.Default()
		}
		logger.DebugContext(ctx, "persisted error text redacted",
			"column", column, "raw_bytes", len(text), "stored_bytes", len(redacted))
	}
	return redacted
}
