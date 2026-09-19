package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type CompleteRouteBatch struct {
	Effects             []EffectBatch
	Result              map[string]any
	Watermark           *time.Time
	Evidence            FetchEvidence
	WorklogObservations []JiraWorklogFetchObservation
}

func (batch CompleteRouteBatch) validate(descriptor CompleteRouteDescriptor) error {
	if len(batch.Effects) != len(descriptor.Destinations) {
		return ErrInvalidConfiguration
	}
	got := make([]string, 0, len(batch.Effects))
	for _, effect := range batch.Effects {
		if effect.Destination == "" || !validDigest(effect.ContentDigest) ||
			!validEffectRecovery(effect.Recovery) {
			return ErrInvalidConfiguration
		}
		got = append(got, effect.Destination)
	}
	sort.Strings(got)
	want := append([]string(nil), descriptor.Destinations...)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		return ErrInvalidConfiguration
	}
	return nil
}

type CompleteRouteHandler interface {
	Collect(
		context.Context,
		Claim,
		providerfoundation.Credential,
		*providerfoundation.HTTPClient,
		time.Time,
	) (CompleteRouteBatch, error)
}

// ChunkRouteEmission is one bounded normalized page. A route may emit many
// pages during one provider-unit execution; the executor persists each page
// before requesting the next one. CursorAfter is intentionally opaque and is
// persisted only on the final prepared subchunk for this emission. Final is a
// metadata-only completion emission when the provider inventory is complete.
type ChunkRouteEmission struct {
	Batch        CompleteRouteBatch
	CursorBefore string
	CursorAfter  string
	Final        bool
}

// ChunkedCompleteRouteHandler is the opt-in streaming contract for routes
// that have a durable chunk policy. resumeCursor is the last cursor committed
// to the checkpoint. Implementations must not retain provider pages after the
// callback returns and must emit a final metadata emission before returning
// nil. The callback may return ChunkContinuationError to stop at a durable
// attempt boundary.
type ChunkedCompleteRouteHandler interface {
	CollectChunks(
		context.Context,
		Claim,
		providerfoundation.Credential,
		*providerfoundation.HTTPClient,
		time.Time,
		string,
		func(ChunkRouteEmission) error,
	) error
}

// RecoveringCompleteRouteHandler lets a route rebuild an in-flight provider
// batch from durable effect identity before ordinary source reselection. Most
// routes only need the stable normalization instant above. Bounded routes
// whose selection depends on already-written analytics rows (GitHub blame is
// the first) need the persisted generation as well, otherwise an accepted
// write can disappear from coverage and produce a new manifest before
// readback has a chance to reconcile the old one.
type RecoveringCompleteRouteHandler interface {
	CollectRecovery(
		context.Context,
		Claim,
		providerfoundation.Credential,
		*providerfoundation.HTTPClient,
		time.Time,
		EffectLedgerState,
	) (CompleteRouteBatch, error)
}

// SafeReplanningCompleteRouteHandler may authorize discarding a prepared
// provider-dependent manifest only after proving that its first ordered
// durable effect has no rows for this generation. The ledger performs a
// second, transactional state check before it removes the manifest.
type SafeReplanningCompleteRouteHandler interface {
	CanReplanRecovery(
		context.Context,
		Claim,
		EffectLedgerState,
	) (bool, error)
}

type CompleteRouteComparator interface {
	CompareCompleteRoute(
		context.Context,
		Claim,
		CompleteRouteBatch,
	) (ShadowComparison, error)
}

// CompleteRouteEffectsFactory binds effect persistence to the resolved
// credential before any provider request. Most routes construct their sink at
// worker startup. Account-scoped routes such as PagerDuty need the decrypted
// provider instance to fence empty-snapshot readback and reconciliation, so
// they construct the same typed sink per attempt through this narrow seam.
type CompleteRouteEffectsFactory func(
	providerfoundation.Credential,
) (EffectSink, EffectReadback, error)

type CompleteRouteExecutor struct {
	Credentials       providerfoundation.CredentialResolver
	Doer              providerfoundation.HTTPDoer
	Retry             providerfoundation.RetryPolicy
	Budget            providerfoundation.BudgetStore
	BudgetLimits      map[CostClass]int
	BudgetTTL         time.Duration
	Gate              BackoffGateFactory
	Metrics           *providerfoundation.Metrics
	Handler           CompleteRouteHandler
	Comparator        CompleteRouteComparator
	Committer         EffectCommitter
	EffectsFactory    CompleteRouteEffectsFactory
	HeartbeatInterval time.Duration
	Now               func() time.Time
}

type CompleteRouteExecutionResult struct {
	Fetch               FetchEvidence
	Result              map[string]any
	Watermark           *time.Time
	Comparison          ShadowComparison
	Effects             EffectCommitResult
	WorklogObservations []JiraWorklogFetchObservation
	// CommittedRows is the chunked checkpoint's CUMULATIVE committed row count
	// for this unit, across every attempt, as of the last checkpoint this
	// execution read. It is deliberately reported on the FAILURE path too: the
	// caller needs to know that a unit it is about to terminalize already owns
	// durable rows (CHAOS-4130). Effects.Written cannot answer that -- it
	// counts only this attempt, and the attempts that died in the CHAOS-4130
	// loop committed nothing before the page budget refused them. Zero for
	// non-chunked routes, which have no checkpoint.
	CommittedRows int64
}

func (executor CompleteRouteExecutor) now() time.Time {
	if executor.Now != nil {
		return executor.Now().UTC()
	}
	return time.Now().UTC()
}

func (executor CompleteRouteExecutor) Execute(
	ctx context.Context,
	session *LeaseSession,
	descriptor CompleteRouteDescriptor,
) (CompleteRouteExecutionResult, error) {
	if ctx == nil || session == nil || !session.valid() ||
		descriptor.Provider != session.Claim.Provider ||
		descriptor.RequestedDataset != session.Claim.Dataset ||
		descriptor.RouteDataset != session.Claim.Dataset ||
		!descriptor.RouteReady || !descriptor.Plannable ||
		executor.Doer == nil ||
		executor.Credentials.Repository == nil ||
		executor.Credentials.Decryptor == nil ||
		executor.Budget == nil || executor.Gate == nil ||
		executor.Handler == nil || executor.Comparator == nil ||
		executor.HeartbeatInterval <= 0 || executor.BudgetTTL <= 0 ||
		executor.BudgetLimits[session.Claim.CostClass] < 1 {
		return CompleteRouteExecutionResult{}, ErrInvalidConfiguration
	}
	if executor.Committer.Ledger == nil ||
		(executor.Committer.Sink == nil && executor.EffectsFactory == nil) {
		return CompleteRouteExecutionResult{}, ErrInvalidConfiguration
	}
	if descriptor.PreparedManifestRecovery {
		if _, ok := preparedManifestRouteDestinations(descriptor.Provider, descriptor.RouteDataset); !ok {
			return CompleteRouteExecutionResult{}, ErrInvalidConfiguration
		}
	}
	if descriptor.Chunked && descriptor.PreparedManifestRecovery {
		return CompleteRouteExecutionResult{}, ErrInvalidConfiguration
	}
	if descriptor.Chunked {
		return executor.executeChunked(ctx, session, descriptor)
	}
	preparedLedger, preparedRecovery := executor.Committer.Ledger.(PreparedEffectLedger)
	if descriptor.PreparedManifestRecovery && !preparedRecovery {
		return CompleteRouteExecutionResult{}, ErrInvalidConfiguration
	}
	var result CompleteRouteExecutionResult
	// recovery names how this attempt produced the batch it committed, for
	// the completion line: "none" on a first attempt, otherwise the recovery
	// path the persisted ledger sent it down.
	recovery := "none"
	err := session.Run(ctx, executor.HeartbeatInterval, func(
		workContext context.Context,
		guard providerfoundation.LeaseGuard,
	) error {
		committer := executor.Committer
		normalizedAt := executor.now()
		var recoveredEffects *EffectLedgerState
		// Load the durable manifest before touching credentials or provider
		// state. Snapshot-backed recovery must be able to resume the exact batch
		// even when the live provider selection has changed since prepare.
		state, loadErr := committer.Ledger.LoadEffects(
			workContext, session.Claim, normalizedAt,
		)
		// bindCredential resolves the unit's credential and, for a route whose
		// sinks are built from it, builds them -- once per attempt, wherever
		// it is first needed. A snapshot replay needs the sinks before it
		// commits, so it binds early; the call and its failures are the same
		// either way, and neither makes a provider request.
		var credential providerfoundation.Credential
		credentialBound := false
		bindCredential := func() error {
			if credentialBound {
				return nil
			}
			resolved, err := executor.Credentials.Resolve(workContext, guard, session.Claim.TenantScope())
			if err != nil {
				return err
			}
			if executor.EffectsFactory != nil {
				committer.Sink, committer.Readback, err = executor.EffectsFactory(resolved)
				if err != nil {
					return err
				}
				if committer.Sink == nil {
					return ErrInvalidConfiguration
				}
				// A route that recovers from a snapshot settles a replayed
				// effect by reading it back; a factory that binds no readback
				// would commit a first attempt whose replay could never finish.
				if descriptor.PreparedManifestRecovery && committer.Readback == nil {
					return ErrInvalidConfiguration
				}
			}
			credential, credentialBound = resolved, true
			return nil
		}
		switch {
		case loadErr == nil:
			if state.Generation != session.Claim.GenerationKey() ||
				state.Provider != session.Claim.Provider ||
				state.Dataset != session.Claim.Dataset {
				return ErrEffectLedgerConflict
			}
			normalizedAt = state.CreatedAt.UTC()
			recoveredEffects = &state
			recovery = "recollect"
		case errors.Is(loadErr, ErrEffectLedgerNotFound):
		default:
			return loadErr
		}
		// legacyLedger is set when a route enrolled in prepared recovery finds a
		// ledger written without a snapshot. That unit finishes the way its
		// ledger was written: re-collect and Commit, never PrepareRouteSnapshot,
		// whose v2 manifest could not match the persisted v1 one.
		legacyLedger := false
		if recoveredEffects != nil && descriptor.PreparedManifestRecovery &&
			(recoveredEffects.SchemaVersion != "v2" || recoveredEffects.PreparedSnapshot == nil) {
			// Contract point 7. A route that requires prepared recovery must
			// never resume from a document written before that contract
			// existed. Both decoders below also refuse it; the policy belongs
			// here, where the route declares the requirement, rather than
			// surviving only as a side effect of two independent decoders that
			// a later change could relax one at a time.
			if preparedManifestRouteRequiresSnapshot(session.Claim.Provider, session.Claim.Dataset) {
				return ErrEffectRecoveryUnsafe
			}
			legacyLedger = true
			recovery = "legacy_ledger"
			slog.Warn(
				"provider_sync.prepared_recovery_legacy_ledger",
				"provider", session.Claim.Provider, "dataset", session.Claim.Dataset,
				"unit", session.Claim.ID, "generation", session.Claim.GenerationKey(),
				"schema_version", recoveredEffects.SchemaVersion,
			)
		}
		if recoveredEffects != nil && descriptor.PreparedManifestRecovery && !legacyLedger {
			if executor.EffectsFactory != nil {
				if err := bindCredential(); err != nil {
					return err
				}
			}
			manifest, err := preparedLedger.LoadRouteSnapshot(
				workContext, session.Claim, *recoveredEffects, executor.now(),
			)
			// A snapshot that is authentically this claim's but describes a
			// destination set the route no longer emits is STALE, not
			// untrustworthy, and the two need opposite answers. Refusing it --
			// which is what every other load failure earns -- leaves the unit
			// retrying a document that can never become valid: stuck, and
			// silent, because nothing reports "recovery keeps refusing the same
			// snapshot". Every future destination added to the manifest would
			// strand whatever was in flight at deploy.
			//
			// So it is DISCARDED and the route replayed from the claim. That
			// honours the v1->v2 precedent a few lines above rather than
			// contradicting it: that rule forbids RESUMING from a document
			// written before a contract existed, and this does not resume from
			// it at all -- it throws the document away and re-runs the route.
			//
			// Discarding is only safe if every effect the old document
			// describes can be produced again and land idempotently, so that is
			// checked against the document's OWN recorded classification rather
			// than assumed from today's builder -- the document was written by
			// an older binary, which is the entire situation. A recovery-
			// BLOCKED effect means "this cannot be redone safely", so such a
			// unit stops instead, with its own reason, loud rather than stuck.
			if errors.Is(err, ErrPreparedSnapshotManifestMismatch) {
				reason := "manifest_mismatch"
				switch {
				case !preparedSnapshotReplayable(*recoveredEffects):
					reason = "manifest_mismatch_unreplayable"
				case !isSafePreparedManifestReplanState(session.Claim, *recoveredEffects):
					// Something in the document already committed. Discarding
					// would delete the generation journal that records it, so
					// the evidence a write landed would go with it. Stops
					// loudly instead -- a partially committed generation is
					// exactly when a person should look.
					reason = "manifest_mismatch_partially_committed"
				default:
					// A writing effect's sink write may already have landed.
					// Discarding would erase the only record of it, so each
					// one is read back against the superseded snapshot's own
					// rows first; only a ledger whose writing effects all read
					// back absent is discarded.
					reason = supersededSnapshotWritingEffectsVerdict(
						workContext, committer.Readback, session.Claim, manifest, *recoveredEffects,
					)
				}
				executor.Metrics.RecordPreparedSnapshotDiscarded(
					session.Claim.Provider, session.Claim.Dataset, reason,
				)
				slog.Warn(
					"provider_sync.prepared_snapshot_discarded",
					"provider", session.Claim.Provider, "dataset", session.Claim.Dataset,
					"unit", session.Claim.ID, "generation", session.Claim.GenerationKey(),
					"reason", reason,
					"persisted_effects", len(recoveredEffects.Effects),
					"expected_effects", len(descriptor.Destinations),
				)
				if reason != "manifest_mismatch" {
					return ErrEffectRecoveryUnsafe
				}
				replanner, ok := committer.Ledger.(EffectLedgerReplanner)
				if !ok {
					return ErrInvalidConfiguration
				}
				replannedAt := executor.now()
				if err := replanner.ResetPreparedEffectsForReplan(
					workContext, session.Claim, *recoveredEffects, replannedAt,
				); err != nil {
					return err
				}
				recoveredEffects = nil
				normalizedAt = replannedAt
				recovery = "snapshot_discarded"
			} else if err != nil {
				return err
			}
			if recoveredEffects != nil {
				manifest.Batch.Result, manifest.Batch.Watermark, err =
					applyGitHubWorkItemsIncompletePolicy(
						session.Claim.Provider, session.Claim.Dataset,
						manifest.Batch.Result, manifest.Batch.Watermark,
					)
				if err != nil {
					return ErrEffectLedgerConflict
				}
				if err := manifest.Batch.validate(descriptor); err != nil ||
					!manifest.NormalizedAt.Equal(normalizedAt) {
					return ErrEffectLedgerConflict
				}
				result.Fetch, result.Result, result.Watermark =
					manifest.Batch.Evidence, manifest.Batch.Result, manifest.Batch.Watermark
				result.WorklogObservations = manifest.Batch.WorklogObservations
				result.Comparison = manifest.Comparison
				recovery = "snapshot_replay"
				result.Effects, err = committer.CommitPrepared(
					workContext, session.Claim, manifest.Batch.Effects, *recoveredEffects,
				)
				return err
			}
			// Fell through: the snapshot was discarded and the route replays
			// from the claim below, exactly as an ordinary first attempt would.
		}
		if err := bindCredential(); err != nil {
			return err
		}
		client, err := (Executor{
			Doer: executor.Doer, Retry: executor.Retry,
		}).newClient(credential, guard)
		if err != nil {
			return err
		}
		client.Budget = executor.Budget
		client.BudgetKey = providerfoundation.BudgetKey{
			Provider:  session.Claim.Provider,
			OrgID:     session.Claim.OrgID,
			Host:      client.BaseURL.Hostname(),
			CostClass: string(session.Claim.CostClass),
			Limit:     executor.BudgetLimits[session.Claim.CostClass],
			TTL:       executor.BudgetTTL,
		}
		client.Gate = executor.Gate(session.Claim, client)
		if client.Gate == nil {
			return ErrInvalidConfiguration
		}
		client.Metrics = executor.Metrics
		// Every attempt for this unit occurrence must rebuild byte-identical
		// rows, not just expired-lease recoveries. Effect digests cover the
		// serialized rows, so a wall-clock timestamp regenerated on an ordinary
		// River retry would change the digest and make PrepareEffects reject
		// the manifest with ErrEffectLedgerConflict before any readback could
		// run — wedging the unit until it exhausts. ReleaseForRetry returns the
		// unit to `dispatching`, so the next claim is *not* Recovered; gating
		// this on Recovered covered only a fraction of the real retry paths.
		if recoveredEffects != nil {
			if replanning, ok := executor.Handler.(SafeReplanningCompleteRouteHandler); ok {
				canReplan, replanErr := replanning.CanReplanRecovery(
					workContext, session.Claim, *recoveredEffects,
				)
				if replanErr != nil {
					return replanErr
				}
				if canReplan {
					ledger, ok := committer.Ledger.(EffectLedgerReplanner)
					if !ok {
						return ErrInvalidConfiguration
					}
					replannedAt := executor.now()
					if err := ledger.ResetPreparedEffectsForReplan(
						workContext, session.Claim, *recoveredEffects, replannedAt,
					); err != nil {
						return err
					}
					recoveredEffects = nil
					normalizedAt = replannedAt
					recovery = "replanned"
				}
			}
		}
		var batch CompleteRouteBatch
		if recoveredEffects != nil {
			if recovering, ok := executor.Handler.(RecoveringCompleteRouteHandler); ok {
				batch, err = recovering.CollectRecovery(
					workContext, session.Claim, credential, client, normalizedAt,
					*recoveredEffects,
				)
			} else {
				batch, err = executor.Handler.Collect(
					workContext, session.Claim, credential, client, normalizedAt,
				)
			}
		} else {
			batch, err = executor.Handler.Collect(
				workContext, session.Claim, credential, client, normalizedAt,
			)
		}
		if err != nil {
			return err
		}
		batch.Result, batch.Watermark, err = applyGitHubWorkItemsIncompletePolicy(
			session.Claim.Provider, session.Claim.Dataset, batch.Result, batch.Watermark,
		)
		if err != nil {
			return err
		}
		if err := batch.validate(descriptor); err != nil {
			return err
		}
		result.Fetch, result.Result, result.Watermark =
			batch.Evidence, batch.Result, batch.Watermark
		result.WorklogObservations = batch.WorklogObservations
		comparison, err := executor.Comparator.CompareCompleteRoute(
			workContext, session.Claim, batch,
		)
		if err != nil {
			return err
		}
		result.Comparison = comparison
		if !comparison.Match {
			return ErrShadowMismatch
		}
		// The same instant that stamped these rows must become the persisted
		// ledger CreatedAt. Letting the committer read its own clock would
		// persist a later time than the rows were built with, so the next
		// attempt would reload that later time, rebuild different rows, and be
		// rejected on digest — the wedge in a second disguise.
		preparedCommit := descriptor.PreparedManifestRecovery && !legacyLedger
		if preparedCommit {
			prepared, prepareErr := preparedLedger.PrepareRouteSnapshot(
				workContext, session.Claim, batch, comparison, normalizedAt,
			)
			switch {
			case prepareErr == nil:
				result.Effects, err = committer.CommitPrepared(
					workContext, session.Claim, batch.Effects, prepared,
				)
			case errors.Is(prepareErr, ErrPreparedRouteSnapshotOversize) &&
				!preparedManifestRouteRequiresSnapshot(session.Claim.Provider, session.Claim.Dataset):
				// A batch whose effects each fit their own cap can still exceed
				// the one snapshot cap. It commits the way it would without a
				// snapshot, so its recovery re-collects.
				slog.Warn(
					"provider_sync.prepared_snapshot_oversize_fallback",
					"provider", session.Claim.Provider, "dataset", session.Claim.Dataset,
					"unit", session.Claim.ID, "generation", session.Claim.GenerationKey(),
					"cause", prepareErr.Error(),
				)
				preparedCommit = false
			case errors.Is(prepareErr, ErrPreparedRouteSnapshotSensitiveKey) &&
				!preparedManifestRouteRequiresSnapshot(session.Claim.Provider, session.Claim.Dataset):
				// Provider content (a work item's custom field, a flag's
				// payload) can carry a key the snapshot must never store. The
				// batch commits the way it would without a snapshot; the line
				// names the key, never its value.
				var sensitive preparedSnapshotSensitiveKeyError
				key := ""
				if errors.As(prepareErr, &sensitive) {
					key = sensitive.key
				}
				slog.Warn(
					"provider_sync.prepared_snapshot_sensitive_key_fallback",
					"provider", session.Claim.Provider, "dataset", session.Claim.Dataset,
					"unit", session.Claim.ID, "generation", session.Claim.GenerationKey(),
					"key", key,
				)
				preparedCommit = false
			default:
				return prepareErr
			}
		}
		if !preparedCommit {
			if descriptor.PreparedManifestRecovery && !legacyLedger {
				// A route that recovers from a snapshot committed this batch
				// without one: its recovery re-collects, and the unit result
				// says so.
				recovery = "recollect"
				result.Result["recovery"] = "recollect"
			}
			result.Effects, err = committer.Commit(
				workContext, session.Claim, batch.Effects, normalizedAt,
			)
		}
		return err
	})
	// CHAOS-5351: the deleted Python `run_work_items_sync_job` logged an
	// Info-level "fetched N items and M transitions" summary operators relied
	// on when running it manually (`dev-hops sync work-items`). This executor
	// is the shared completion point for every provider/dataset route (not
	// just work-items), and `result.Result` already carries each route's own
	// `*_synced` counts (e.g. `work_items_synced`, `transitions_synced` --
	// github_work_items_route.go, gitlab_work_items_route.go,
	// jira_work_items_route.go, jira_atlassian_route.go,
	// linear_work_items_route.go all populate it), so this is a straight
	// surface-the-existing-data log, not a new computation.
	if err == nil {
		slog.Default().LogAttrs(context.Background(), slog.LevelInfo,
			"provider_sync.route_completed",
			routeCompletedLogFor(session.Claim, recovery, result).attrs()...,
		)
	}
	return result, err
}

// supersededSnapshotWritingEffectsVerdict judges the writing effects of a
// superseded snapshot's ledger by reading each one back against the snapshot's
// own rows. It answers "manifest_mismatch" (discard) only when every writing
// effect reads back absent: an exact or conflicting readback means the write
// landed, and a readback that fails or cannot be made is never taken as
// absent. The ledger and snapshot effects are index-aligned: the snapshot
// decoder refuses a payload whose effects differ from the ledger's by
// destination, digest or row count.
func supersededSnapshotWritingEffectsVerdict(
	ctx context.Context,
	readback EffectReadback,
	claim Claim,
	manifest PreparedRouteManifest,
	state EffectLedgerState,
) string {
	if len(manifest.Batch.Effects) != len(state.Effects) {
		return "manifest_mismatch_readback_failed"
	}
	for index, effect := range state.Effects {
		if effect.Status != GenerationBlockWriting {
			continue
		}
		if readback == nil {
			return "manifest_mismatch_readback_unavailable"
		}
		inspection, err := readback.InspectEffect(ctx, claim, manifest.Batch.Effects[index])
		if err != nil {
			return "manifest_mismatch_readback_failed"
		}
		switch inspection {
		case EffectAbsent:
		case EffectExact, EffectConflict:
			return "manifest_mismatch_write_landed"
		default:
			return "manifest_mismatch_readback_failed"
		}
	}
	return "manifest_mismatch"
}

// routeCompletedLog is everything provider_sync.route_completed carries:
// identity from the claim, this package's own recovery word, effect and fetch
// counts, and the unit result's "*_synced" counts. No value on the line comes
// from provider content; the unit's stored result keeps everything else.
type routeCompletedLog struct {
	Provider               string
	Dataset                string
	Unit                   string
	Recovery               string
	EffectsWritten         int
	EffectsSkipped         int
	EffectsMarkedCommitted int
	EffectsResetForReplay  int
	Records                int
	Requests               int
	Pages                  int
	Counts                 []routeCompletedCount
}

// routeCompletedCount is one integer "*_synced" entry of the unit result.
type routeCompletedCount struct {
	Name  string
	Value int64
}

func routeCompletedLogFor(claim Claim, recovery string, result CompleteRouteExecutionResult) routeCompletedLog {
	return routeCompletedLog{
		Provider: claim.Provider, Dataset: claim.Dataset, Unit: claim.ID, Recovery: recovery,
		EffectsWritten: result.Effects.Written, EffectsSkipped: result.Effects.Skipped,
		EffectsMarkedCommitted: result.Effects.MarkedCommitted,
		EffectsResetForReplay:  result.Effects.ResetForReplay,
		Records:                result.Fetch.Records, Requests: result.Fetch.Requests, Pages: result.Fetch.Pages,
		Counts: routeCompletedCounts(result.Result),
	}
}

// routeCompletedCounts keeps only result entries whose key ends in
// "_synced" and whose value is an integer: counts this package's routes
// compute. Strings, nested values and anything else stay in the stored result.
func routeCompletedCounts(result map[string]any) []routeCompletedCount {
	var counts []routeCompletedCount
	if len(result) == 0 {
		return nil
	}
	for key, value := range result {
		if !strings.HasSuffix(key, "_synced") || !routeCompletedCountName.MatchString(key) {
			continue
		}
		var count int64
		switch typed := value.(type) {
		case int:
			count = int64(typed)
		case int64:
			count = typed
		case json.Number:
			parsed, err := typed.Int64()
			if err != nil {
				continue
			}
			count = parsed
		default:
			continue
		}
		counts = append(counts, routeCompletedCount{Name: key, Value: count})
	}
	sort.Slice(counts, func(left, right int) bool { return counts[left].Name < counts[right].Name })
	return counts
}

var routeCompletedCountName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func (line routeCompletedLog) attrs() []slog.Attr {
	attrs := []slog.Attr{
		slog.String("provider", line.Provider), slog.String("dataset", line.Dataset),
		slog.String("unit", line.Unit), slog.String("recovery", line.Recovery),
		slog.Int("effects_written", line.EffectsWritten), slog.Int("effects_skipped", line.EffectsSkipped),
		slog.Int("effects_marked_committed", line.EffectsMarkedCommitted),
		slog.Int("effects_reset_for_replay", line.EffectsResetForReplay),
		slog.Int("records", line.Records), slog.Int("requests", line.Requests), slog.Int("pages", line.Pages),
	}
	for _, count := range line.Counts {
		attrs = append(attrs, slog.Int64(count.Name, count.Value))
	}
	return attrs
}
