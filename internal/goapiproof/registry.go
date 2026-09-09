package goapiproof

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// ErrNoBuildIdentity reports that the running query-api did not tell us
// which BUILD it is.
//
// This is a hard refusal, not a fallback to an operator-supplied name.
// go_api_proof_run's key includes candidate_build, and a receipt is
// evidence for exactly one build -- so a receipt built from a
// hand-typed sha proves that somebody typed a sha, not that that build
// served the request. `enable`'s --candidate-build flag is documented as
// "the ops commit sha the running query-api image was built from", by
// CONVENTION, unverified; the fifteen live rows carry
// b18e56fa79cfe20ce0f75df148144b832d92be36 with nothing having checked
// it. That is the gap this refusal closes.
var ErrNoBuildIdentity = errors.New("goapiproof: the running query-api reports no build identity, so no receipt can name the build that served these requests")

// registryBody is GET /registry's response.
//
// Build is OPTIONAL in the wire shape and REQUIRED by this command: the
// route as merged carries only schema_digest and operations, so an older
// deployment answers without it and gets a named refusal rather than a
// confusing decode error.
type registryBody struct {
	SchemaDigest string `json:"schema_digest"`
	Operations   []struct {
		Operation      string `json:"operation"`
		DocumentDigest string `json:"document_digest"`
	} `json:"operations"`
	Build *struct {
		Commit    string `json:"commit"`
		Version   string `json:"version"`
		BuildTime string `json:"build_time"`
		Modified  bool   `json:"modified"`
	} `json:"build"`
}

// FetchRegistry asks the RUNNING process what it serves.
//
// Never a checkout, never a checked-in mirror: the six-day outage
// CHAOS-5416 records happened because rows were seeded at a digest no
// running binary computed, and every surface that could have said so was
// reading the same stale source as the thing that was wrong.
func FetchRegistry(ctx context.Context, client *http.Client, registryURL string) (RegistryView, error) {
	if client == nil {
		client = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, registryURL, nil)
	if err != nil {
		return RegistryView{}, fmt.Errorf("goapiproof: build registry request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return RegistryView{}, fmt.Errorf("goapiproof: read %s: %w", registryURL, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return RegistryView{}, fmt.Errorf("goapiproof: %s answered HTTP %d", registryURL, response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return RegistryView{}, fmt.Errorf("goapiproof: read registry body: %w", err)
	}

	var parsed registryBody
	if err := json.Unmarshal(body, &parsed); err != nil {
		return RegistryView{}, fmt.Errorf("goapiproof: decode registry body: %w", err)
	}
	if parsed.SchemaDigest == "" {
		return RegistryView{}, fmt.Errorf("goapiproof: %s reported an empty schema digest", registryURL)
	}
	if len(parsed.Operations) == 0 {
		return RegistryView{}, fmt.Errorf("goapiproof: %s registers no operations -- there is nothing to prove", registryURL)
	}

	view := RegistryView{
		SchemaDigest:   parsed.SchemaDigest,
		DocumentDigest: make(map[string]string, len(parsed.Operations)),
	}
	for _, operation := range parsed.Operations {
		view.DocumentDigest[operation.Operation] = operation.DocumentDigest
	}

	switch {
	case parsed.Build == nil || strings.TrimSpace(parsed.Build.Commit) == "":
		return view, ErrNoBuildIdentity
	case parsed.Build.Commit == "unknown":
		return view, fmt.Errorf("%w: it reports commit=\"unknown\", which is internal/platform/version's default for a build with no -ldflags and no VCS stamp", ErrNoBuildIdentity)
	case parsed.Build.Modified:
		return view, fmt.Errorf("%w: it reports a MODIFIED working tree, so its commit does not identify the source it was built from", ErrNoBuildIdentity)
	}
	view.BuildIdentity = parsed.Build.Commit
	return view, nil
}

// registrydumpDocument is one element of `registrydump -file
// cmd/query-api/query_route.go`'s JSON output.
type registrydumpDocument struct {
	Operation string `json:"operation"`
	Document  string `json:"document"`
	Digest    string `json:"digest"`
}

// LoadDocuments reads registrydump's JSON output and returns operation ->
// registered document TEXT.
//
// Why a file produced by another tool rather than re-parsing
// query_route.go here: registrydump already does that enumeration, and
// its AST walk carries four separately-reviewed guards against silent
// under-enumeration (grouped consts, post-literal map writes, a duplicate
// digestByOperation identifier, orphaned consts). A second implementation
// of that walk in this command would be a second thing to keep correct,
// and the one that drifted would under-report documents while looking
// fine -- which is the exact failure class registrydump exists to stop.
//
// The staleness risk a file introduces is closed by VerifyDocuments: the
// text must digest to what the RUNNING process registers, or the run
// refuses. A stale documents file cannot produce a receipt.
func LoadDocuments(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // operator-supplied path to their own registrydump output
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read documents file: %w", err)
	}
	var documents []registrydumpDocument
	if err := json.Unmarshal(raw, &documents); err != nil {
		return nil, fmt.Errorf("goapiproof: decode documents file (expected `registrydump -file ...` output): %w", err)
	}
	if len(documents) == 0 {
		return nil, fmt.Errorf("goapiproof: documents file %s is empty", path)
	}

	byOperation := make(map[string]string, len(documents))
	for _, document := range documents {
		if document.Document == "" {
			return nil, fmt.Errorf("goapiproof: documents file carries no text for %q", document.Operation)
		}
		byOperation[document.Operation] = document.Document
	}
	return byOperation, nil
}

// VerifyDocuments proves the local document text is byte-identical to
// what the running process registers, by comparing digests it computes
// over that text against the digests the process reports.
//
// digestFn is passed in so this package does not depend on
// cmd/query-api/internal/digest (which Go's internal rule puts out of
// reach here); the caller hands in that exact function, so there is still
// only ONE digest implementation in play.
func VerifyDocuments(documents map[string]string, registry RegistryView, digestFn func(string) string) error {
	var drifted []string
	for operation, registered := range registry.DocumentDigest {
		text, ok := documents[operation]
		if !ok {
			// Not fatal here: the runner refuses this operation by name
			// (RefusalDocumentDigestDrift) so one missing document does not
			// abort the whole run and lose the other fourteen receipts.
			continue
		}
		if got := digestFn(text); got != registered {
			drifted = append(drifted, fmt.Sprintf("%s: local text digests to %s, the running process registers %s", operation, got, registered))
		}
	}
	if len(drifted) > 0 {
		return fmt.Errorf("goapiproof: document drift between this checkout and the running query-api -- rebuild registrydump's output against the deployed sha: %s", strings.Join(drifted, "; "))
	}
	return nil
}
