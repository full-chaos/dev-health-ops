package main

import "github.com/full-chaos/dev-health-ops/internal/jobruntime"

func main() {
	_ = jobruntime.Reason{value: "tenant-dsn-leak"}
}
