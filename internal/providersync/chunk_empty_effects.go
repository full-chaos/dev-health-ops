package providersync

import "time"

// A chunked route's sub-chunk carries every destination of its route, and most of them are empty. An empty effect writes
// nothing, so the committer leaves it pending and MarkChunkCommitted commits it in the transaction that commits the chunk:
// two ledger transactions per empty effect are saved, and the ledger entries, digests and positions stay as prepared.

// emptyEffectDeferrer is a ledger whose chunk mark commits the empty effects the committer left pending.
type emptyEffectDeferrer interface {
	defersEmptyEffects() bool
}

// chunkEmptyEffectStore is a ChunkedEffectStore whose MarkChunkCommitted calls commitDeferredEmptyEffects.
type chunkEmptyEffectStore interface {
	commitsDeferredEmptyEffects()
}

// emptyEffectNoOpSink is a sink whose WriteEffect of an empty batch has no effect but its lease check.
type emptyEffectNoOpSink interface {
	emptyEffectIsNoOp(destination string) bool
}

func (*PostgresRepository) commitsDeferredEmptyEffects()   {}
func (*InProcessChunkLedger) commitsDeferredEmptyEffects() {}

func (sink TestOpsClickHouseEffects) emptyEffectIsNoOp(destination string) bool {
	return githubTestsDestination(destination)
}

func (sink GitHubFilesClickHouseEffects) emptyEffectIsNoOp(destination string) bool {
	return destination == "git_files"
}

func (adapter chunkEffectLedgerAdapter) defersEmptyEffects() bool {
	_, ok := adapter.store.(chunkEmptyEffectStore)
	return ok
}

func (committer EffectCommitter) defersEmptyEffect(entry EffectLedgerEntry, batch EffectBatch) bool {
	ledger, ledgerOK := committer.Ledger.(emptyEffectDeferrer)
	sink, sinkOK := committer.Sink.(emptyEffectNoOpSink)
	return ledgerOK && sinkOK && ledger.defersEmptyEffects() && sink.emptyEffectIsNoOp(batch.Destination) &&
		entry.Status == GenerationBlockPending && provablyEmptyEffect(entry, batch)
}

// provablyEmptyEffect: the entry and its payload batch both hold no row and no membership rejection (which the content
// digest does not cover), and the entry's digest is the digest of an empty batch of its destination.
func provablyEmptyEffect(entry EffectLedgerEntry, batch EffectBatch) bool {
	if entry.RowCount != 0 || len(batch.Rows) != 0 || len(batch.MembershipRejections) != 0 ||
		batch.Destination != entry.Destination || batch.ContentDigest != entry.ContentDigest {
		return false
	}
	empty, err := BuildEffectBatch(entry.Destination, entry.Recovery, nil)
	return err == nil && empty.ContentDigest == entry.ContentDigest
}

// commitDeferredEmptyEffects commits, in the caller's mark transaction, every pending entry that is provably empty. Any
// other entry that is not committed refuses the mark. It reports whether the ledger changed.
func commitDeferredEmptyEffects(chunk *PreparedProviderChunk, now time.Time) (bool, error) {
	batches := append([]EffectBatch(nil), chunk.Effects...)
	sortEffectBatches(batches)
	if len(batches) != len(chunk.Ledger.Effects) {
		return false, ErrChunkCheckpointConflict
	}
	current := now.UTC()
	changed := false
	for index := range chunk.Ledger.Effects {
		entry := &chunk.Ledger.Effects[index]
		switch {
		case entry.Status == GenerationBlockCommitted:
		case entry.Status == GenerationBlockPending && provablyEmptyEffect(*entry, batches[index]):
			entry.Status, entry.StartedAt, entry.CommittedAt = GenerationBlockCommitted, &current, &current
			changed = true
		default:
			return false, ErrChunkCheckpointConflict
		}
	}
	if changed {
		chunk.Ledger.UpdatedAt = current
	}
	return changed, nil
}
