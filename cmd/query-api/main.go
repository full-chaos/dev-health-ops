// Command query-api is the read-only Go query plane. It serves the code in
// internal/queryapi/server; this file only hands that package the process
// arguments, environment and streams, and exits with the code it returns.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/server"
)

func main() {
	// The redacting handler is the process default before anything can log;
	// server.Run installs its own on the same stream when it starts.
	logging.InstallDefault(logging.NewJSON(os.Stdout, slog.LevelInfo))
	os.Exit(server.Run(context.Background(), os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}
