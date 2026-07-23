// dev-health-provider-fixture evaluates the sanitized provider parity corpus.
// Python shadow tests can invoke this command and compare its JSON output
// without passing credentials, request bodies, or provider response payloads.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type fixture struct {
	SchemaVersion string        `json:"schema_version"`
	Cases         []fixtureCase `json:"cases"`
}
type fixtureCase struct {
	ID             string                        `json:"id"`
	Provider       string                        `json:"provider"`
	Status         int                           `json:"status"`
	Headers        map[string]string             `json:"headers"`
	Message        string                        `json:"message"`
	Classification providerfoundation.ErrorClass `json:"classification"`
}
type result struct {
	SchemaVersion string       `json:"schema_version"`
	Cases         []resultCase `json:"cases"`
}
type resultCase struct {
	ID             string                        `json:"id"`
	Classification providerfoundation.ErrorClass `json:"classification"`
}

func run(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("dev-health-provider-fixture", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("fixture", "internal/providerfoundation/testdata/provider_parity.json", "sanitized provider fixture path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	content, err := os.ReadFile(*path)
	if err != nil {
		return fmt.Errorf("read provider fixture: %w", err)
	}
	var input fixture
	if err := json.Unmarshal(content, &input); err != nil || input.SchemaVersion != "v1" {
		return fmt.Errorf("invalid provider fixture")
	}
	output := result{SchemaVersion: input.SchemaVersion, Cases: make([]resultCase, 0, len(input.Cases))}
	for _, item := range input.Cases {
		headers := http.Header{}
		for key, value := range item.Headers {
			headers.Set(key, value)
		}
		classification := providerfoundation.ClassifyHTTPWithMessage(item.Provider, item.Status, headers, item.Message)
		if classification == nil {
			return fmt.Errorf("fixture %s has a successful status", item.ID)
		}
		output.Cases = append(output.Cases, resultCase{ID: item.ID, Classification: classification.Class})
	}
	return json.NewEncoder(stdout).Encode(output)
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "provider fixture evaluation failed")
		os.Exit(1)
	}
}
