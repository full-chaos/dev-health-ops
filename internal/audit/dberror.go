package audit

import "log/slog"

// DBError is the log attribute for an error that may come from the database.
// Stub: logs the error text.
func DBError(err error) slog.Attr { return slog.Any("error", err) }
