# Materialize Fan-Out and Partitioning

This page describes the partitioned fan-out and caching mechanisms used during investment materialization.

## Partitioned Fan-Out Flow

To handle large work graphs efficiently, the system partitions components and processes them in parallel using Celery chords.

```mermaid
flowchart TD
    Start([Post-Sync Sync Trigger]) --> Dispatch[dispatch_investment_materialize_partitioned]
    Dispatch --> FetchEdges[Fetch Work Graph Edges]
    FetchEdges --> BuildComp[Build Components]
    BuildComp --> Chunk[Split into Chunks of Size N]
    
    subgraph ChordHeader [Celery Chord Header]
        Chunk1[run_investment_materialize_chunk 0]
        Chunk2[run_investment_materialize_chunk 1]
        ChunkN[run_investment_materialize_chunk N]
    end
    
    Chunk --> ChordHeader
    
    subgraph ChunkExecution [Per-Chunk Execution]
        CheckCP{Already Completed?}
        CheckCP -- Yes --> SkipChunk[Skip Chunk]
        CheckCP -- No --> MarkRunning[Mark Checkpoint Running]
        MarkRunning --> Materialize[materialize_investments]
        Materialize --> MarkDone[Mark Checkpoint Completed]
    end
    
    ChordHeader --> ChunkExecution
    
    subgraph ChordCallback [Celery Chord Callback]
        Finalize[finalize_investment_materialize_partitioned]
        CheckBackfill{Run Membership Backfill?}
        CheckBackfill -- Yes --> Backfill[backfill_memberships]
        CheckBackfill -- No --> End([End])
        Backfill --> End
    end
    
    ChunkExecution --> Finalize
    Finalize --> CheckBackfill
```

## Sequence of Execution

The sequence diagram below shows the interaction between the dispatcher, chunk tasks, checkpoints, and the finalizer.

```mermaid
sequenceDiagram
    autonumber
    participant D as Dispatcher
    participant C as Celery Worker (Chunk)
    participant P as Postgres (Checkpoints)
    participant CH as ClickHouse (Sink)
    participant F as Finalizer

    D->>D: Fetch edges and build components
    D->>D: Partition component indexes into chunks
    D->>C: Dispatch chunks via Celery Chord
    
    Note over C: For each chunk task
    C->>P: Check if already completed (is_completed)
    P-->>C: Not completed
    C->>P: Mark checkpoint running (mark_running)
    
    C->>CH: Query existing input hashes (if force=False)
    CH-->>C: Return existing hashes
    Note over C: Skip LLM for matching hashes
    
    C->>C: Run LLM for new/changed components
    C->>CH: Write work_unit_investments
    C->>P: Mark checkpoint completed (mark_completed)
    
    C->>F: Send chunk stats
    Note over F: After all chunks complete
    F->>F: Aggregate run statistics
    F->>CH: Run membership backfill (if full run)
    F-->>D: Return final summary
```

## Key Mechanisms

### 1. Checkpoint Protocol
Each chunk task is tracked in Postgres using a unique checkpoint scope ID derived from the `run_id` and `chunk_index`. If a task fails and retries, or if the worker restarts, the system checks the checkpoint table. Completed chunks are skipped, preventing redundant LLM calls.

### 2. Input Hash Caching
Before calling the LLM, the materializer computes a SHA-256 hash of the serialized evidence bundle for each component. If `force` is `False`, the system queries ClickHouse for existing valid records with the same hash and model version. Matching components are skipped. If `force` is `True`, the system bypasses this check and forces a fresh LLM call.

### 3. Membership Backfill Unification
To prevent partial-coverage bugs, the windowed materializer does not write `work_unit_membership` rows or completion markers. Instead, the finalizer triggers `backfill_memberships` at the end of a full run. This projection runs without the LLM, rebuilding the full work graph and projecting membership rows from the already persisted investments.
