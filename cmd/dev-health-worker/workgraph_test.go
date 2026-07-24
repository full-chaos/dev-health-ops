package main

import "testing"

func TestWorkgraphCompatibilityHTTPClientUsesRiverExecutionDeadline(t *testing.T) {
	t.Parallel()
	client := workgraphCompatibilityHTTPClient()
	if client == nil || client.Timeout != 0 {
		t.Fatalf("workgraph compatibility timeout=%v want=0", client.Timeout)
	}
}
