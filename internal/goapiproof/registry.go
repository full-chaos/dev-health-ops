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

// registryBody is GET /registry's response. Deliberately just the schema
// digest and the operation map -- the build identity lives at
// /buildinfo, see FetchBuildIdentity.
type registryBody struct {
	SchemaDigest string `json:"schema_digest"`
	Operations   []struct {
		Operation      string `json:"operation"`
		DocumentDigest string `json:"document_digest"`
	} `json:"operations"`
}

// buildInfoBody is GET /buildinfo's response.
type buildInfoBody struct {
	Commit    string `json:"commit"`
	Version   string `json:"version"`
	BuildTime string `json:"build_time"`
	Modified  bool   `json:"modified"`
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

	return view, nil
}

// FetchBuildIdentity asks the RUNNING process which build it is, via the
// authenticated GET /buildinfo route.
//
// Every failure mode is a REFUSAL, never a fallback to an
// operator-supplied name:
//
//   - an empty commit, or "unknown" (internal/platform/version's default
//     for a build with no -ldflags and no VCS stamp): the process cannot
//     say what it is, so no receipt can say it either.
//   - a MODIFIED tree: the commit does not describe what was compiled, so
//     naming it on a receipt would be a false claim, not an approximate
//     one.
//   - a 404: the deployment predates /buildinfo. Also a refusal -- an old
//     build that cannot identify itself is exactly the case this check
//     exists for.
func FetchBuildIdentity(ctx context.Context, client *http.Client, buildInfoURL string, credential *Credential) (string, error) {
	if client == nil {
		client = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, buildInfoURL, nil)
	if err != nil {
		return "", fmt.Errorf("goapiproof: build buildinfo request: %w", err)
	}
	if err := credential.Apply(ctx, request); err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("goapiproof: read %s: %w", buildInfoURL, err)
	}
	defer func() { _ = response.Body.Close() }()

	switch response.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return "", fmt.Errorf("%w: %s answered 404, so this deployment predates the /buildinfo route", ErrNoBuildIdentity, buildInfoURL)
	case http.StatusUnauthorized, http.StatusForbidden:
		return "", fmt.Errorf("goapiproof: %s rejected the %s credential (HTTP %d) -- /buildinfo checks the effective-principal envelope, not the edge access token", buildInfoURL, credential.Kind(), response.StatusCode)
	default:
		return "", fmt.Errorf("goapiproof: %s answered HTTP %d", buildInfoURL, response.StatusCode)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("goapiproof: read buildinfo body: %w", err)
	}
	var parsed buildInfoBody
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("goapiproof: decode buildinfo body: %w", err)
	}

	commit := strings.TrimSpace(parsed.Commit)
	switch {
	case commit == "":
		return "", ErrNoBuildIdentity
	case commit == "unknown":
		return "", fmt.Errorf("%w: it reports commit=%q, internal/platform/version's default for a build with no -ldflags and no VCS stamp", ErrNoBuildIdentity, commit)
	case parsed.Modified:
		return "", fmt.Errorf("%w: it reports a MODIFIED working tree, so its commit does not identify the source it was built from", ErrNoBuildIdentity)
	}
	return commit, nil
}

// VerifyBuildStable re-reads /buildinfo AFTER a run and refuses when the
// serving build moved underneath it.
//
// This bounds the residual codex r1's F5 named. Per-request build binding
// is available only on the proof route, which stamps the serving build on
// its own responses; the EDGE route cannot provide it, because the Python
// dispatcher's _forward_to_go builds a new Response carrying only content,
// status and media_type and drops every header query-api set. Closing that
// half needs the edge to forward the header -- a Python change this lane is
// barred from making, and one that belongs to the dispatcher's owner.
//
// So an edge-route receipt's build claim rests on: the routing rows and
// /buildinfo agreeing before the run (VerifyCandidateBuild), and the same
// build still answering after it (this). What that leaves uncovered is
// narrow and nameable: two replicas serving DIFFERENT builds simultaneously
// for the whole run. That is the staggered-rollout state the fleet's own
// lockstep rule already forbids -- migrate and every go-* image rebuild
// together, never staggered -- so the residual is a deployment invariant,
// not an unexamined hole. It is stated on the report rather than assumed.
func VerifyBuildStable(ctx context.Context, client *http.Client, buildInfoURL string, credential *Credential, before string) error {
	after, err := FetchBuildIdentity(ctx, client, buildInfoURL, credential)
	if err != nil {
		return fmt.Errorf("goapiproof: re-reading the build identity after the run failed, so the receipts cannot be shown to name the build that served them: %w", err)
	}
	if after != before {
		return fmt.Errorf("goapiproof: the serving build moved DURING the run (%s -> %s) -- every receipt this run wrote names a build that was not serving for all of it", before, after)
	}
	return nil
}

// VerifyCandidateBuild cross-checks the build the process reports against
// what the routing rows point at.
//
// The direction matters. The RUNNING process is the authority on which
// build served the request; the row's current_candidate_build is a
// claim an operator typed. When they disagree, the rows are describing a
// build that is not the one answering requests -- the same class of
// silent mismatch as the stale schema digest in CHAOS-5416, and a receipt
// written across it would attribute evidence to the wrong build.
//
// An operator-supplied --candidate-build is checked here too, as a
// cross-check ONLY. It never becomes the value written (team-lead ruling
// R51, 2026-09-09).
func VerifyCandidateBuild(running string, expected string) error {
	if expected != "" && expected != running {
		return fmt.Errorf("goapiproof: --candidate-build %q does not match the running build %q -- the flag is a cross-check, never the source", expected, running)
	}
	return nil
}

// StaleRoutingRows reports which routing rows name a build the running
// process is not, as operation -> the build the row names.
//
// This USED to refuse the whole run, and CHAOS-5484's JOB 4 showed that
// was both wrong and blocking. Wrong, because the comparison does not
// protect what its refusal claimed to protect:
//
//   - Reachability does not depend on this column. PostgresSwitch's own
//     doc comment says so in terms: "current_candidate_build is NOT bound
//     to reachability. Enabled answers 'is this operation's mode
//     canary/primary', not 'is THIS candidate build the one currently
//     live'". The Python edge never reads the column at all. So a stale
//     row does not change which build answers the request that gets
//     measured.
//   - The receipt cannot name the row's build even if it wanted to.
//     CandidateBuild on every receipt is RegistryView.BuildIdentity, read
//     from the running process's own /buildinfo. It names what served the
//     request, by construction.
//
// What actually protects a receipt from authorizing the wrong build is
// the four-column key: a proof recorded at the running build satisfies
// `enable --candidate-build <that build>` and nothing else. A stale row
// was never able to defeat that.
//
// Blocking, because after a redeploy EVERY row is stale until an operator
// re-points it, and `enable` -- the only verb that writes
// current_candidate_build -- accepts canary|primary only. A SHADOW row
// therefore cannot be re-pointed by any supported command, and shadow
// operations are precisely what /query/proof exists to prove. One
// un-re-pointable row failed the entire run.
//
// So it is recorded rather than enforced: counted in the summary, named
// per operation in the report, and written into the receipt's review
// evidence, so a reader of go_api_proof_run months later can see that the
// routing row named an older build when the measurement was taken.
func StaleRoutingRows(running string, routing map[string]RoutingRow) map[string]string {
	stale := map[string]string{}
	for operation, row := range routing {
		if row.CandidateBuild != "" && row.CandidateBuild != running {
			stale[operation] = row.CandidateBuild
		}
	}
	return stale
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
