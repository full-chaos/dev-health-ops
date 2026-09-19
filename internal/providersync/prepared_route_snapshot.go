package providersync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
)

const (
	preparedRouteSnapshotSchemaVersion = "v1"
	maxPreparedRouteSnapshotBytes      = 64 << 20
)

// ErrPreparedRouteSnapshotOversize is a manifest that encodes past
// maxPreparedRouteSnapshotBytes. It is an ErrEffectRecoveryUnsafe, so every
// caller that refuses an unsafe snapshot still refuses it; the executor
// alone tells it apart, to commit such a batch without a snapshot on routes
// that allow that.
var ErrPreparedRouteSnapshotOversize = fmt.Errorf(
	"%w: prepared route snapshot exceeds its size cap", ErrEffectRecoveryUnsafe,
)

// ErrPreparedRouteSnapshotSensitiveKey is a manifest whose rows or result
// carry a key the snapshot must never store. Like the oversize error it is an
// ErrEffectRecoveryUnsafe; the executor tells it apart to commit such a batch
// without a snapshot on routes that allow that.
var ErrPreparedRouteSnapshotSensitiveKey = fmt.Errorf(
	"%w: prepared route snapshot would store a sensitive key", ErrEffectRecoveryUnsafe,
)

// preparedSnapshotSensitiveKeyError names the matched key -- the key name
// only, never its value -- so the fallback line can say which key it was.
type preparedSnapshotSensitiveKeyError struct{ key string }

func (err preparedSnapshotSensitiveKeyError) Error() string {
	return ErrPreparedRouteSnapshotSensitiveKey.Error() + ": key " + err.key
}

func (err preparedSnapshotSensitiveKeyError) Unwrap() error {
	return ErrPreparedRouteSnapshotSensitiveKey
}

type PreparedRouteSnapshotReference struct {
	SchemaVersion string `json:"schema_version"`
	ContentDigest string `json:"content_digest"`
	PayloadBytes  int    `json:"payload_bytes"`
}

func (reference PreparedRouteSnapshotReference) validate() error {
	if reference.SchemaVersion != preparedRouteSnapshotSchemaVersion ||
		!validDigest(reference.ContentDigest) || reference.PayloadBytes < 1 ||
		reference.PayloadBytes > maxPreparedRouteSnapshotBytes {
		return ErrEffectLedgerConflict
	}
	return nil
}

func samePreparedRouteSnapshotReference(
	left *PreparedRouteSnapshotReference,
	right *PreparedRouteSnapshotReference,
) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

type PreparedRouteManifest struct {
	Batch        CompleteRouteBatch
	Comparison   ShadowComparison
	NormalizedAt time.Time
}

type storedPreparedRouteSnapshot struct {
	SchemaVersion string                 `json:"schema_version"`
	Generation    string                 `json:"generation"`
	OrgID         string                 `json:"org_id"`
	Provider      string                 `json:"provider"`
	Dataset       string                 `json:"dataset"`
	NormalizedAt  time.Time              `json:"normalized_at"`
	Effects       []storedPreparedEffect `json:"effects"`
	Result        json.RawMessage        `json:"result"`
	Watermark     *time.Time             `json:"watermark,omitempty"`
	Evidence      FetchEvidence          `json:"evidence"`
	Comparison    ShadowComparison       `json:"comparison"`
}

// preparedSnapshotWorklogResultKey carries a batch's Jira worklog
// observations inside the stored result object rather than as a field of its
// own: an earlier binary's strict decoder accepts any result key, so it
// replays such a snapshot (without the observations) instead of refusing it,
// and this binary takes the observations back out of the result on decode.
const preparedSnapshotWorklogResultKey = "_prepared_snapshot_worklog_observations"

type storedPreparedEffect struct {
	Destination   string               `json:"destination"`
	ContentDigest string               `json:"content_digest"`
	Recovery      EffectRecoveryPolicy `json:"recovery"`
	Rows          []json.RawMessage    `json:"rows"`
	PayloadBytes  int                  `json:"payload_bytes"`
	// MembershipRejections (CHAOS-4320 round 7, codex round 6 P1, executed
	// repro): EffectBatch.MembershipRejections is a separate durable field
	// from Rows, and this prepared-route recovery envelope is a SEPARATE
	// JSON-serialized projection of EffectBatch from the in-process value --
	// a pending effect recovered through THIS envelope (fetch succeeded,
	// commit had not happened yet) rebuilds its EffectBatch via
	// BuildEffectBatch(stored.Destination, stored.Recovery, stored.Rows),
	// which never touches this field, so recovery silently dropped every
	// rejection observation for the same reason a `json:"-"` tag would have
	// (round 2's own class, one layer further out: not a missing tag this
	// time, but a whole separate stored type that never named the field at
	// all). `omitempty` matches every OTHER destination's effect, which
	// never populates this and must not gain a spurious `[]` in its stored
	// JSON.
	MembershipRejections []json.RawMessage `json:"membership_rejections,omitempty"`
}

func encodePreparedRouteManifest(
	claim Claim,
	batch CompleteRouteBatch,
	comparison ShadowComparison,
	normalizedAt time.Time,
) ([]byte, PreparedRouteSnapshotReference, error) {
	if validatePreparedRouteManifestIdentity(claim, batch, normalizedAt) != nil ||
		!comparison.Match {
		return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
	}
	if _, reserved := batch.Result[preparedSnapshotWorklogResultKey]; reserved {
		return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
	}
	// A snapshot row holds only what its sink writes or reads: the route's
	// effects are projected before the ledger digest is taken.
	if !preparedRouteRowsAreProjected(batch.Effects) {
		return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
	}
	storedResult := batch.Result
	if len(batch.WorklogObservations) > 0 {
		storedResult = make(map[string]any, len(batch.Result)+1)
		maps.Copy(storedResult, batch.Result)
		storedResult[preparedSnapshotWorklogResultKey] = batch.WorklogObservations
	}
	result, err := json.Marshal(storedResult)
	if err != nil || len(result) == 0 || bytes.Equal(result, []byte("null")) {
		return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
	}
	var resultValue any
	if json.Unmarshal(result, &resultValue) != nil {
		return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
	}
	// The result is built by the route, not taken from provider content: a
	// protected key there is a defect in the route, so it keeps refusing
	// rather than committing without a snapshot.
	if containsPreparedRouteSensitiveKey(resultValue) {
		return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
	}
	ordered := append([]EffectBatch(nil), batch.Effects...)
	sortEffectBatches(ordered)
	effects := make([]storedPreparedEffect, 0, len(ordered))
	for _, effect := range ordered {
		for _, row := range effect.Rows {
			var rowValue any
			if json.Unmarshal(row, &rowValue) != nil {
				return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
			}
			if key, found := preparedRouteSensitiveKey(rowValue); found {
				return nil, PreparedRouteSnapshotReference{}, preparedSnapshotSensitiveKeyError{key: key}
			}
		}
		// Same sensitive-key scrub as Rows above, applied to
		// MembershipRejections for the same reason: this envelope has no
		// separate trust tier for a second payload field just because it
		// is not part of the ContentDigest/RowCount identity below.
		for _, rejection := range effect.MembershipRejections {
			var rejectionValue any
			if json.Unmarshal(rejection, &rejectionValue) != nil {
				return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
			}
			if key, found := preparedRouteSensitiveKey(rejectionValue); found {
				return nil, PreparedRouteSnapshotReference{}, preparedSnapshotSensitiveKeyError{key: key}
			}
		}
		effects = append(effects, storedPreparedEffect{
			Destination: effect.Destination, ContentDigest: effect.ContentDigest,
			Recovery: effect.Recovery, Rows: effect.Rows, PayloadBytes: effect.PayloadBytes,
			MembershipRejections: effect.MembershipRejections,
		})
	}
	watermark := batch.Watermark
	if watermark != nil {
		value := watermark.UTC()
		watermark = &value
	}
	snapshot := storedPreparedRouteSnapshot{
		SchemaVersion: preparedRouteSnapshotSchemaVersion,
		Generation:    claim.GenerationKey(), OrgID: claim.OrgID,
		Provider: claim.Provider, Dataset: claim.Dataset,
		NormalizedAt: normalizedAt.UTC(), Effects: effects, Result: result,
		Watermark: watermark, Evidence: batch.Evidence, Comparison: comparison,
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil || len(encoded) < 1 {
		return nil, PreparedRouteSnapshotReference{}, ErrEffectRecoveryUnsafe
	}
	if len(encoded) > maxPreparedRouteSnapshotBytes {
		return nil, PreparedRouteSnapshotReference{}, fmt.Errorf(
			"%w: %d bytes", ErrPreparedRouteSnapshotOversize, len(encoded),
		)
	}
	digest := sha256.Sum256(encoded)
	reference := PreparedRouteSnapshotReference{
		SchemaVersion: preparedRouteSnapshotSchemaVersion,
		ContentDigest: hex.EncodeToString(digest[:]), PayloadBytes: len(encoded),
	}
	return encoded, reference, nil
}

func decodePreparedRouteManifest(
	raw []byte,
	claim Claim,
	state EffectLedgerState,
) (PreparedRouteManifest, error) {
	if claim.Validate() != nil || state.validate() != nil ||
		state.SchemaVersion != "v2" || state.PreparedSnapshot == nil ||
		state.Generation != claim.GenerationKey() || state.Provider != claim.Provider ||
		state.Dataset != claim.Dataset || len(raw) < 1 ||
		len(raw) > maxPreparedRouteSnapshotBytes ||
		len(raw) != state.PreparedSnapshot.PayloadBytes {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != state.PreparedSnapshot.ContentDigest {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	decoder := json.NewDecoder(io.LimitReader(bytes.NewReader(raw), maxPreparedRouteSnapshotBytes+1))
	decoder.DisallowUnknownFields()
	var snapshot storedPreparedRouteSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	if err := requirePreparedRouteSnapshotEOF(decoder); err != nil {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	if snapshot.SchemaVersion != preparedRouteSnapshotSchemaVersion ||
		snapshot.Generation != claim.GenerationKey() || snapshot.OrgID != claim.OrgID ||
		snapshot.Provider != claim.Provider || snapshot.Dataset != claim.Dataset ||
		!snapshot.NormalizedAt.Equal(state.CreatedAt) || !snapshot.Comparison.Match ||
		len(snapshot.Effects) != len(state.Effects) || len(snapshot.Result) == 0 {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	if snapshot.Evidence.Provider != claim.Provider || snapshot.Evidence.Dataset != claim.Dataset ||
		snapshot.Evidence.Requests < 0 || snapshot.Evidence.Pages < 0 || snapshot.Evidence.Records < 0 {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	var result map[string]any
	if decodeResultObjectExact(snapshot.Result, &result) != nil || result == nil ||
		containsPreparedRouteSensitiveKey(result) {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	var worklogObservations []JiraWorklogFetchObservation
	if stored, present := result[preparedSnapshotWorklogResultKey]; present {
		encoded, err := json.Marshal(stored)
		if err != nil {
			return PreparedRouteManifest{}, ErrEffectLedgerConflict
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&worklogObservations) != nil || len(worklogObservations) == 0 {
			return PreparedRouteManifest{}, ErrEffectLedgerConflict
		}
		delete(result, preparedSnapshotWorklogResultKey)
	}
	effects := make([]EffectBatch, 0, len(snapshot.Effects))
	for index, stored := range snapshot.Effects {
		rebuilt, err := BuildEffectBatch(stored.Destination, stored.Recovery, stored.Rows)
		if err != nil || rebuilt.ContentDigest != stored.ContentDigest ||
			rebuilt.PayloadBytes != stored.PayloadBytes ||
			state.Effects[index].Destination != rebuilt.Destination ||
			state.Effects[index].ContentDigest != rebuilt.ContentDigest ||
			state.Effects[index].RowCount != len(rebuilt.Rows) ||
			state.Effects[index].Recovery != rebuilt.Recovery {
			return PreparedRouteManifest{}, ErrEffectLedgerConflict
		}
		// BuildEffectBatch only ever constructs Rows -- it deliberately does
		// not accept or validate MembershipRejections (that field is not
		// part of any OTHER destination's contract, and giving every caller
		// of BuildEffectBatch a new parameter for one destination's field
		// was the exact blast radius this design avoided). Reattach it here,
		// the one place a stored effect becomes a live EffectBatch again.
		rebuilt.MembershipRejections = stored.MembershipRejections
		effects = append(effects, rebuilt)
	}
	batch := CompleteRouteBatch{
		Effects: effects, Result: result, Watermark: snapshot.Watermark,
		Evidence: snapshot.Evidence, WorklogObservations: worklogObservations,
	}
	// By here the payload is authentically this claim's: its digest matched
	// the ledger and its tenant, generation and route matched the claim. A
	// destination set that differs from the route's descriptor is therefore a
	// document written before a manifest change, and it keeps its own error so
	// the executor can discard it; every other identity failure refuses.
	if err := validatePreparedRouteManifestIdentity(claim, batch, snapshot.NormalizedAt); err != nil {
		if errors.Is(err, ErrPreparedSnapshotManifestMismatch) {
			// The superseded document's own rows travel with the error: a
			// discard must read back any effect that may already have landed.
			return PreparedRouteManifest{
				Batch: batch, Comparison: snapshot.Comparison,
				NormalizedAt: snapshot.NormalizedAt.UTC(),
			}, ErrPreparedSnapshotManifestMismatch
		}
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	return PreparedRouteManifest{
		Batch: batch, Comparison: snapshot.Comparison,
		NormalizedAt: snapshot.NormalizedAt.UTC(),
	}, nil
}

// decodeResultObjectExact decodes a persisted result object keeping every
// number as written, so an integer past 2^53 (a GitLab project_id) survives a
// decode and re-encode byte for byte instead of rounding through float64.
func decodeResultObjectExact(raw []byte, target *map[string]any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return requirePreparedRouteSnapshotEOF(decoder)
}

func requirePreparedRouteSnapshotEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrEffectLedgerConflict
	}
	return nil
}

// preparedManifestRouteDestinations is the allow-list of routes that recover
// from a prepared snapshot: github/work-items, github and gitlab deployments,
// and github and gitlab prs. A route qualifies when its re-collected rows can
// differ from the crashed pass for reasons outside the ledger's control -- a
// mutable provider selection (work-items), or a best-effort per-row lookup
// whose outcome can change between the crash and the retry (the deployments'
// pull request and statuses lookups, the PR-social review enrichment).
// Replaying the stored rows is the only recovery that does not need the
// provider to answer the same way twice. Only canonical dataset identities
// are listed: aliases are never planned or executed on their own. The
// destination set is the route's descriptor's, so a manifest is checked
// against exactly what the route declares it emits.
func preparedManifestRouteDestinations(provider, dataset string) ([]string, bool) {
	switch {
	case provider == "github" && dataset == "work-items",
		(provider == "github" || provider == "gitlab") && dataset == "deployments",
		(provider == "github" || provider == "gitlab") && dataset == "prs",
		(provider == "github" || provider == "gitlab") && preparedCodeFamilyDataset(dataset),
		(provider == "gitlab" || provider == "launchdarkly") && dataset == "feature-flags",
		(provider == "gitlab" || provider == "jira") && dataset == "incidents":
	default:
		return nil, false
	}
	descriptor, ok := Descriptor(provider, dataset)
	if !ok || len(descriptor.Destinations) == 0 {
		return nil, false
	}
	return append([]string(nil), descriptor.Destinations...), true
}

// preparedCodeFamilyDataset reports whether a GitHub or GitLab dataset is one
// of the repository code routes enrolled in prepared-snapshot recovery.
func preparedCodeFamilyDataset(dataset string) bool {
	switch dataset {
	case "commit-stats", "commits", "files", "repo-metadata", "security":
		return true
	default:
		return false
	}
}

// preparedManifestRouteRequiresSnapshot reports whether a recovering unit of
// this route must refuse a ledger written without a prepared snapshot.
// github/work-items has required one since its recovery contract existed
// (contract point 7). The other routes enrolled later: a ledger written
// without a snapshot by an earlier binary, still in flight at deploy, recovers
// the way it was written -- by re-collecting -- rather than stranding the unit.
func preparedManifestRouteRequiresSnapshot(provider, dataset string) bool {
	return provider == "github" && dataset == "work-items"
}

// preparedRouteManifestDestinationsMatch compares a manifest's destination set
// against the one the claim's route emits TODAY.
//
// It is split out of the identity check, and returns its OWN error, because the
// two failures need different answers. Every other identity failure means the
// document is not this claim's, or has been tampered with, and the only safe
// response is to refuse. A destination-set difference means something else
// entirely: the document is authentically ours and simply predates a manifest
// change. Collapsing them into one error forced the caller to treat a routine
// deploy like tampering.
func preparedRouteManifestDestinationsMatch(claim Claim, batch CompleteRouteBatch) error {
	want, ok := preparedManifestRouteDestinations(claim.Provider, claim.Dataset)
	if !ok {
		return ErrEffectRecoveryUnsafe
	}
	got := make([]string, 0, len(batch.Effects))
	for _, effect := range batch.Effects {
		got = append(got, effect.Destination)
	}
	sort.Strings(got)
	want = append([]string(nil), want...)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		return ErrPreparedSnapshotManifestMismatch
	}
	return nil
}

// preparedSnapshotReplayable reports whether a persisted ledger may be
// DISCARDED and its route re-run from the claim.
//
// Discarding is only safe if every effect the old document describes can be
// produced again and land idempotently. Readback-fenced and replay-safe effects
// both qualify: the committer inspects before writing and replays only what the
// readback reports absent. An effect recorded as recovery-BLOCKED does not --
// that classification exists precisely to say "this cannot be redone safely" --
// so a document containing one is refused rather than discarded, loudly.
//
// Checked against the PERSISTED document rather than assumed from today's
// builder. Today every github destination is built EffectReadbackRequired
// (BuildGitHubWorkItemEffects), so this answers true for every real snapshot;
// but the document being judged was written by an OLDER binary, which is the
// whole situation, and reading its own recorded classification is the only
// honest way to ask.
func preparedSnapshotReplayable(state EffectLedgerState) bool {
	for _, effect := range state.Effects {
		if effect.Recovery == EffectRecoveryBlocked {
			return false
		}
	}
	return true
}

func validatePreparedRouteManifestIdentity(
	claim Claim,
	batch CompleteRouteBatch,
	normalizedAt time.Time,
) error {
	if _, ok := preparedManifestRouteDestinations(claim.Provider, claim.Dataset); claim.Validate() != nil || !ok ||
		normalizedAt.IsZero() || batch.Result == nil ||
		batch.Evidence.Provider != claim.Provider || batch.Evidence.Dataset != claim.Dataset {
		return ErrEffectRecoveryUnsafe
	}
	if err := preparedRouteManifestDestinationsMatch(claim, batch); err != nil {
		return err
	}
	return nil
}

func containsPreparedRouteSensitiveKey(value any) bool {
	_, found := preparedRouteSensitiveKey(value)
	return found
}

// protectedKeyWords and protectedKeyWordPairs are the one list of what makes
// a key protected: a key is protected when any of its words, or any two
// adjacent words, is listed. Wide on purpose -- over-matching only costs the
// unit its snapshot (it commits without one and recovers by re-collecting),
// while under-matching stores or logs a secret.
var protectedKeyWords = map[string]bool{
	"token": true, "secret": true, "password": true, "passwd": true, "credential": true,
	"authorization": true, "auth": true, "bearer": true, "cookie": true, "session": true,
	"signature": true, "apikey": true, "header": true, "ciphertext": true, "cert": true, "pem": true,
}

var protectedKeyWordPairs = map[[2]string]bool{
	{"api", "key"}: true, {"private", "key"}: true, {"client", "secret"}: true, {"access", "key"}: true,
	{"raw", "payload"}: true, {"response", "body"}: true, {"request", "body"}: true,
	{"source", "metadata"}: true, {"integration", "config"}: true,
}

// protectedKeyFragments are matched inside the key's letters and digits run
// together, so a key whose words carry no separator ("apitoken",
// "ACCESSTOKEN", "userpassword") is still caught. Only fragments that are not
// ordinary parts of other words are listed; "auth", "pem" or "cert" stay
// word-level so "author" or "concert" are not protected.
var protectedKeyFragments = []string{
	"token", "secret", "password", "passwd", "credential", "authorization", "bearer",
	"apikey", "privatekey", "clientsecret", "accesskey", "ciphertext",
}

// keyWords splits a key into lower-case words at every separator (anything
// but a letter or digit: "_", "-", ".", spaces) and at camelCase boundaries,
// including an acronym followed by a word ("APIKey" -> "api", "key"). Every
// word is folded by singularProtectedWord before either lookup.
func keyWords(key string) []string {
	var words []string
	var current []rune
	runes := []rune(key)
	flush := func() {
		if len(current) > 0 {
			words = append(words, strings.ToLower(string(current)))
			current = current[:0]
		}
	}
	for index, r := range runes {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush()
			continue
		}
		if unicode.IsUpper(r) && len(current) > 0 {
			previous := current[len(current)-1]
			nextIsLower := index+1 < len(runes) && unicode.IsLower(runes[index+1])
			if unicode.IsLower(previous) || unicode.IsDigit(previous) || (unicode.IsUpper(previous) && nextIsLower) {
				flush()
			}
		}
		current = append(current, r)
	}
	flush()
	for index, word := range words {
		words[index] = singularProtectedWord(word)
	}
	return words
}

// singularProtectedWord folds a plural ("-s", "-es", "-ies") onto the listed
// word it spells, whether that word stands alone or is one half of a listed
// pair, so "raw_payloads" and "response_bodies" match as "raw_payload" and
// "response_body". A word that is not a plural of a listed word is returned
// unchanged, so the run-together fragment check still sees its letters.
func singularProtectedWord(word string) string {
	candidates := make([]string, 0, 3)
	if stem, found := strings.CutSuffix(word, "ies"); found {
		candidates = append(candidates, stem+"y")
	}
	if stem, found := strings.CutSuffix(word, "es"); found {
		candidates = append(candidates, stem)
	}
	if stem, found := strings.CutSuffix(word, "s"); found {
		candidates = append(candidates, stem)
	}
	for _, candidate := range candidates {
		if protectedKeyWords[candidate] || protectedKeyPairWords[candidate] {
			return candidate
		}
	}
	return word
}

// protectedKeyPairWords holds every word that is one half of a listed pair.
var protectedKeyPairWords = func() map[string]bool {
	words := make(map[string]bool, 2*len(protectedKeyWordPairs))
	for pair := range protectedKeyWordPairs {
		words[pair[0]] = true
		words[pair[1]] = true
	}
	return words
}()

// isPreparedRouteSensitiveKeyName reports whether a key is protected, and
// returns the words that made it so, joined by "_": a name built only from
// the protected vocabulary, safe to log.
func isPreparedRouteSensitiveKeyName(key string) (string, bool) {
	words := keyWords(key)
	for index, word := range words {
		if protectedKeyWords[word] {
			return word, true
		}
		if index+1 < len(words) && protectedKeyWordPairs[[2]string{word, words[index+1]}] {
			return word + "_" + words[index+1], true
		}
	}
	joined := strings.Join(words, "")
	for _, fragment := range protectedKeyFragments {
		if strings.Contains(joined, fragment) {
			return fragment, true
		}
	}
	return "", false
}

// preparedRouteSensitiveKey returns the first key, at any depth, whose
// normalized name the snapshot must never store.
func preparedRouteSensitiveKey(value any) (string, bool) {
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) == 0 {
			return "", false
		}
		for key, nested := range typed {
			if normalized, sensitive := isPreparedRouteSensitiveKeyName(key); sensitive {
				return normalized, true
			}
			if found, ok := preparedRouteSensitiveKey(nested); ok {
				return found, true
			}
		}
	case []any:
		for _, nested := range typed {
			if found, ok := preparedRouteSensitiveKey(nested); ok {
				return found, true
			}
		}
	}
	return "", false
}

func (repository *PostgresRepository) PrepareRouteSnapshot(
	ctx context.Context,
	claim Claim,
	batch CompleteRouteBatch,
	comparison ShadowComparison,
	normalizedAt time.Time,
) (EffectLedgerState, error) {
	if repository == nil || repository.Pool == nil || ctx == nil {
		return EffectLedgerState{}, ErrInvalidConfiguration
	}
	payload, reference, err := encodePreparedRouteManifest(
		claim, batch, comparison, normalizedAt,
	)
	if err != nil {
		return EffectLedgerState{}, err
	}
	desired, err := NewEffectLedgerState(claim, batch.Effects, normalizedAt)
	if err != nil {
		return EffectLedgerState{}, err
	}
	return repository.prepareRouteSnapshotPayload(ctx, claim, desired, payload, reference, normalizedAt)
}

// prepareRouteSnapshotPayload writes an encoded snapshot and its v2 ledger in
// one transaction, or verifies an existing pair is exactly this one.
func (repository *PostgresRepository) prepareRouteSnapshotPayload(
	ctx context.Context,
	claim Claim,
	desired EffectLedgerState,
	payload []byte,
	reference PreparedRouteSnapshotReference,
	normalizedAt time.Time,
) (EffectLedgerState, error) {
	desired.SchemaVersion = "v2"
	desired.PreparedSnapshot = &reference
	var prepared EffectLedgerState
	err := repository.mutateGenerationJournalTx(
		ctx, claim, normalizedAt,
		func(tx pgx.Tx, document map[string]json.RawMessage) error {
			raw := document[effectLedgerResultKey]
			if len(raw) != 0 {
				current, decodeErr := decodeEffectLedgerState(raw)
				prepared = current
				if decodeErr != nil || !sameEffectManifest(prepared, desired) {
					return ErrEffectLedgerConflict
				}
				return verifyPreparedRouteSnapshotRow(ctx, tx, claim, prepared, payload)
			}
			prepared = desired
			prepared.CreatedAt = normalizedAt.UTC()
			prepared.UpdatedAt = normalizedAt.UTC()
			encoded := encodeEffectLedgerState(prepared)
			if len(encoded) == 0 {
				return ErrEffectRecoveryUnsafe
			}
			if _, err := tx.Exec(
				ctx, insertPreparedRouteSnapshotSQL,
				claim.OrgID, claim.ID, claim.GenerationKey(), claim.Provider, claim.Dataset,
				reference.SchemaVersion, reference.ContentDigest, reference.PayloadBytes,
				payload, normalizedAt.UTC(),
			); err != nil {
				return ErrEffectLedgerConflict
			}
			document[effectLedgerResultKey] = encoded
			return nil
		},
	)
	return prepared, err
}

func (repository *PostgresRepository) LoadRouteSnapshot(
	ctx context.Context,
	claim Claim,
	state EffectLedgerState,
	now time.Time,
) (PreparedRouteManifest, error) {
	if repository == nil || repository.Pool == nil || ctx == nil || now.IsZero() ||
		claim.Validate() != nil || state.validate() != nil ||
		state.SchemaVersion != "v2" || state.PreparedSnapshot == nil {
		return PreparedRouteManifest{}, ErrInvalidConfiguration
	}
	var schemaVersion, contentDigest string
	var payloadBytes int
	var payload []byte
	var leaseLive, runLive bool
	if err := repository.Pool.QueryRow(
		ctx, loadPreparedRouteSnapshotSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(), claim.Provider, claim.Dataset,
		claim.Owner, now.UTC(),
	).Scan(
		&schemaVersion, &contentDigest, &payloadBytes, &payload,
		&leaseLive, &runLive,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return PreparedRouteManifest{}, ErrPreparedRouteSnapshotNotFound
		}
		return PreparedRouteManifest{}, err
	}
	// Order matters twice over. The snapshot demonstrably exists for this
	// tenant/unit/generation, so a refusal here is about entitlement, not
	// absence -- reporting it as "not found" told a recovering worker to fail
	// closed for the wrong reason and hid genuine handoffs. And a terminal run
	// is checked BEFORE the lease, because a finished run explains a live
	// lease that is simply no longer relevant; the reverse order would send an
	// operator hunting a lease problem that does not exist.
	if !runLive {
		return PreparedRouteManifest{}, ErrPreparedRouteSnapshotRunTerminal
	}
	if !leaseLive {
		return PreparedRouteManifest{}, ErrLeaseLost
	}
	if schemaVersion != state.PreparedSnapshot.SchemaVersion ||
		contentDigest != state.PreparedSnapshot.ContentDigest ||
		payloadBytes != state.PreparedSnapshot.PayloadBytes || payloadBytes != len(payload) {
		return PreparedRouteManifest{}, ErrEffectLedgerConflict
	}
	return decodePreparedRouteManifest(payload, claim, state)
}

func verifyPreparedRouteSnapshotRow(
	ctx context.Context,
	tx pgx.Tx,
	claim Claim,
	state EffectLedgerState,
	wantPayload []byte,
) error {
	var schemaVersion, digest string
	var payloadBytes int
	var payload []byte
	if err := tx.QueryRow(
		ctx, loadPreparedRouteSnapshotRowSQL,
		claim.OrgID, claim.ID, claim.GenerationKey(), claim.Provider, claim.Dataset,
	).Scan(&schemaVersion, &digest, &payloadBytes, &payload); err != nil {
		// Only an absent row is a ledger conflict. Folding every error into
		// ErrEffectLedgerConflict is what let a permission failure -- one that
		// happened on EVERY re-prepare in production -- read as ordinary
		// disagreement between the ledger and the sidecar. It would not have
		// retried forever: it would have burned the unit's MaxAttempts and
		// then terminalized under the generic provider_unit_exhausted
		// category, with the actual cause (SQLSTATE 42501) nowhere in the
		// record. A database error must arrive as itself.
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrEffectLedgerConflict
		}
		return err
	}
	reference := state.PreparedSnapshot
	if reference == nil || schemaVersion != reference.SchemaVersion ||
		digest != reference.ContentDigest || payloadBytes != reference.PayloadBytes ||
		payloadBytes != len(payload) || !bytes.Equal(payload, wantPayload) {
		return ErrEffectLedgerConflict
	}
	return nil
}

// A superseded snapshot is discarded and its route re-prepared for the SAME
// generation. ResetPreparedEffectsForReplan deletes the old row
// (deletePreparedRouteSnapshotSQL) in the transaction that deletes the ledger,
// so the replacement insert below never meets it.
//
// A plain INSERT, deliberately without ON CONFLICT. The primary key is
// (org_id, sync_run_unit_id, generation), and this statement runs only on the
// branch where the ledger key was absent -- so a conflict would mean a
// snapshot row exists for a generation whose ledger does not, which is a state
// no code path produces and which should fail loudly rather than be papered
// over by an upsert.
const insertPreparedRouteSnapshotSQL = `
INSERT INTO public.sync_run_unit_effect_snapshots (
    org_id, sync_run_unit_id, generation, provider, dataset_key,
    schema_version, content_digest, payload_bytes, payload, created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`

// loadPreparedRouteSnapshotSQL answers three questions separately on purpose:
// does this snapshot exist for this tenant/unit/generation, is the caller
// still the unit's writer, and is the owning run still live. Folding them into
// one WHERE clause made all three the same sentinel, so "someone else took
// over", "this run already finished" and "there is nothing to recover" were
// indistinguishable to the caller and in any log.
//
// Splitting lease from run status is not pedantry: a finalized run with a
// perfectly live lease reported ErrLeaseLost, which sends an operator to look
// at leases when the actual cause is a run that already reached a terminal
// state. Each term now carries its own error.
//
// The fence mirrors loadEffectLedgerSQL clause for clause, IS NOT NULL guard
// and terminal-run exclusion included: two queries deciding the same
// entitlement must not drift apart.
//
// The payload is gated behind both fences rather than selected unconditionally
// -- a refused load has no business moving up to 64 MiB across the wire, and
// the CASE means the column is never even detoasted on that path.
const loadPreparedRouteSnapshotSQL = `
SELECT fenced.schema_version, fenced.content_digest, fenced.payload_bytes,
       CASE WHEN fenced.lease_live AND fenced.run_live
            THEN fenced.payload ELSE ''::bytea END,
       fenced.lease_live, fenced.run_live
FROM (
  SELECT snapshot.schema_version, snapshot.content_digest,
         snapshot.payload_bytes, snapshot.payload,
         COALESCE(
           unit.status = 'running'
           AND unit.lease_owner = $6
           AND unit.lease_expires_at IS NOT NULL
           AND unit.lease_expires_at > $7,
           false
         ) AS lease_live,
         COALESCE(
           run.status NOT IN ('success', 'partial_failed', 'failed'),
           false
         ) AS run_live
  FROM public.sync_run_unit_effect_snapshots AS snapshot
  JOIN public.sync_run_units AS unit
    ON unit.id = snapshot.sync_run_unit_id
   AND unit.org_id = snapshot.org_id
  JOIN public.sync_runs AS run
    ON run.id = unit.sync_run_id AND run.org_id = unit.org_id
  WHERE snapshot.org_id = $1
    AND snapshot.sync_run_unit_id = $2
    AND snapshot.generation = $3
    AND snapshot.provider = $4
    AND snapshot.dataset_key = $5
) AS fenced`

// loadPreparedRouteSnapshotRowSQL deliberately takes NO row lock. It used to
// end in FOR UPDATE, which is a permission error rather than a lock: any
// row-locking clause requires an UPDATE-class privilege, and the domain role
// holds only SELECT/INSERT/DELETE here (see required_table_privileges). That
// made every re-prepare fail with "permission denied" in production while
// passing in tests, which run as the owner. Widening the grant to buy the lock
// would hand the role a privilege nothing else needs -- and the lock is
// already held anyway.
//
// The serialization comes from the caller: this runs only inside
// mutateGenerationJournalTx's callback, and that transaction opens with
// lockGenerationJournalSQL (generation_journal.go), which ends in
// `FOR UPDATE OF unit` on the owning sync_run_units row. A snapshot row is
// keyed by (org_id, sync_run_unit_id, generation) and is only ever written by
// the holder of that unit's lease, so a competing writer must take the same
// unit lock first. Locking the snapshot row again adds nothing.
//
// sync_run_units is where the repo already pays for this: the domain role is
// granted UPDATE on it partly because Fanout's FOR SHARE needs the privilege.
const loadPreparedRouteSnapshotRowSQL = `
SELECT schema_version, content_digest, payload_bytes, payload
FROM public.sync_run_unit_effect_snapshots
WHERE org_id = $1 AND sync_run_unit_id = $2 AND generation = $3
  AND provider = $4 AND dataset_key = $5`
