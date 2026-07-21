package main

import "github.com/full-chaos/dev-health-ops/internal/platform/shell"

func main() {
	shell.Main(shell.Spec{
		Service:        "dev-health-worker",
		Profiles:       []string{"latency", "sync", "heavy", "ops"},
		DefaultProfile: "latency",
	})
}
