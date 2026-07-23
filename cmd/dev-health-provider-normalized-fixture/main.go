// dev-health-provider-normalized-fixture validates the Go sink envelope side
// of the sanitized cross-language provider corpus.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type fixture struct {
	SchemaVersion string        `json:"schema_version"`
	Cases         []fixtureCase `json:"cases"`
}

type fixtureCase struct {
	ID       string                                `json:"id"`
	Shape    string                                `json:"shape"`
	Envelope providerfoundation.NormalizedEnvelope `json:"envelope"`
}

type result struct {
	SchemaVersion string       `json:"schema_version"`
	Cases         []resultCase `json:"cases"`
}

type resultCase struct {
	ID       string                                `json:"id"`
	Envelope providerfoundation.NormalizedEnvelope `json:"envelope"`
}

func run(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("dev-health-provider-normalized-fixture", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("fixture", "internal/providerfoundation/testdata/normalized_envelope_parity.json", "sanitized normalized provider fixture path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	content, err := os.ReadFile(*path)
	if err != nil {
		return fmt.Errorf("read normalized provider fixture: %w", err)
	}
	var input fixture
	if err := json.Unmarshal(content, &input); err != nil || input.SchemaVersion != "v1" || len(input.Cases) == 0 {
		return fmt.Errorf("invalid normalized provider fixture")
	}
	output := result{SchemaVersion: input.SchemaVersion, Cases: make([]resultCase, 0, len(input.Cases))}
	seenIDs := make(map[string]struct{}, len(input.Cases))
	seenDedupe := make(map[string]string, len(input.Cases))
	for _, item := range input.Cases {
		if item.ID == "" || item.Shape == "" {
			return fmt.Errorf("invalid normalized provider fixture case")
		}
		if _, exists := seenIDs[item.ID]; exists {
			return fmt.Errorf("duplicate normalized provider fixture id")
		}
		seenIDs[item.ID] = struct{}{}
		if err := item.Envelope.Validate(); err != nil {
			return fmt.Errorf("invalid normalized provider fixture envelope")
		}
		if priorID, exists := seenDedupe[item.Envelope.DedupeKey]; exists && priorID != item.ID {
			return fmt.Errorf("duplicate normalized provider fixture dedupe key")
		}
		seenDedupe[item.Envelope.DedupeKey] = item.ID
		output.Cases = append(output.Cases, resultCase{ID: item.ID, Envelope: item.Envelope})
	}
	return json.NewEncoder(stdout).Encode(output)
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "normalized provider fixture evaluation failed")
		os.Exit(1)
	}
}
