package restorefence

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	revocationSection = "public.provider_oauth_revocations"
	requeuedSection   = revocationSection + ".requeued"
	// queuedSuffix names the report line of the revocations a presence section queued.
	queuedSuffix       = ".queued"
	revocationIDColumn = "id"
)

// SourceRead is what one snapshot of the shadow restore gives the apply. Every field but Timeline is a jsonb array that
// only PostgreSQL reads: the revocations hold the stored ciphertext and the lockouts an identifier hash or an email, so
// the fence never parses or prints them.
type SourceRead struct {
	Timeline    int64
	Revocations string
	AuthLocks   string
	LegacyLocks string
}

// RevocationSource reads the shadow restore that exported the manifest: the provider revocation rows of the manifest's
// ids and the live login lockouts.
type RevocationSource interface {
	Revocations(ctx context.Context, schema authschema.ValidatedIdentifier, ids []string) (SourceRead, error)
}

// PGRevocationSource reads the source over its own connection, in one read-only repeatable-read transaction, so the
// timeline and the rows come from one snapshot.
type PGRevocationSource struct{ Pool *pgxpool.Pool }

const revocationSourceSQL = `SELECT coalesce(jsonb_agg(jsonb_build_object('id', id, 'org_id', org_id, 'provider', provider,
  'credential_name', credential_name, 'purpose', purpose, 'token_encrypted', token_encrypted, 'token_key_version', token_key_version,
  'created_at', created_at) ORDER BY id), '[]'::jsonb)::text
FROM public.provider_oauth_revocations WHERE id = ANY($1::uuid[])`

func (s PGRevocationSource) Revocations(ctx context.Context, schema authschema.ValidatedIdentifier, ids []string) (SourceRead, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return SourceRead{}, fmt.Errorf("opening the source transaction (sqlstate %s)", sqlState(err))
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var out SourceRead
	if out.Timeline, err = Timeline(ctx, tx); err != nil {
		return SourceRead{}, fmt.Errorf("reading the source timeline")
	}
	if err := tx.QueryRow(ctx, revocationSourceSQL, ids).Scan(&out.Revocations); err != nil {
		return SourceRead{}, fmt.Errorf("reading the source revocations (sqlstate %s)", sqlState(err))
	}
	if err := tx.QueryRow(ctx, authLocksSourceSQL(schema)).Scan(&out.AuthLocks); err != nil {
		return SourceRead{}, fmt.Errorf("reading the source lockouts (sqlstate %s)", sqlState(err))
	}
	if err := tx.QueryRow(ctx, legacyLocksSourceSQL).Scan(&out.LegacyLocks); err != nil {
		return SourceRead{}, fmt.Errorf("reading the source legacy lockouts (sqlstate %s)", sqlState(err))
	}
	return out, nil
}

// requeueMissingSQL counts the manifest revocations ($1) the restored database lacks and the source rows ($2) lack too.
const requeueMissingSQL = `WITH m AS (SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(id uuid)),
s AS (SELECT * FROM jsonb_to_recordset($2::jsonb) AS y(id uuid)),
l AS (SELECT x.id FROM m x WHERE NOT EXISTS (SELECT 1 FROM public.provider_oauth_revocations t WHERE t.id = x.id))
SELECT count(*) FROM l WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.id = l.id)`

// requeueInsertSQL puts back the source rows ($2) of the manifest revocations ($1) the restored database lacks, as
// fresh pending work, and returns each id with whether a disconnect or a callback can deliver it: a setup revoke is
// drained by any of the org, any other by one of its credential, which needs its credential row.
const requeueInsertSQL = `WITH m AS (SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(id uuid)),
s AS (SELECT * FROM jsonb_to_recordset($2::jsonb) AS y(id uuid, org_id text, provider text, credential_name text, purpose text,
  token_encrypted text, token_key_version text, created_at timestamptz)),
i AS (INSERT INTO public.provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, last_error, created_at, updated_at)
  SELECT s.id, s.org_id, s.provider, s.credential_name, s.purpose, s.token_encrypted, s.token_key_version, 'pending', 0, NULL, s.created_at, clock_timestamp()
  FROM s JOIN m ON m.id = s.id
  ON CONFLICT (id) DO NOTHING
  RETURNING id, org_id, provider, credential_name, purpose)
SELECT i.id::text, (i.purpose = 'setup' OR EXISTS (SELECT 1 FROM public.integration_credentials c WHERE c.org_id = i.org_id AND c.provider = i.provider AND c.name = i.credential_name))
FROM i ORDER BY 1`

// revocationRequeue is what Apply read from the source before its transaction.
type revocationRequeue struct {
	ids         string // the manifest revocation ids, a jsonb array of {"id": ...}
	rows        string // the source rows, a jsonb array; never parsed here
	authLocks   string // the source's live auth lockouts, a jsonb array; never parsed here
	legacyLocks string // the source's live legacy lockouts, a jsonb array; never parsed here
}

// readRevocationSource reads the source rows of the manifest's revocation ids. It refuses a source on another timeline
// than the export's: the rows must come from the shadow restore that exported the manifest. Errors carry no source
// text.
func readRevocationSource(ctx context.Context, src RevocationSource, schema authschema.ValidatedIdentifier, m Manifest) (revocationRequeue, error) {
	ids := []string{}
	for _, s := range m.Sections {
		if s.Name != revocationSection {
			continue
		}
		for _, r := range s.Rows {
			if id, ok := r[revocationIDColumn].(string); ok {
				ids = append(ids, id)
			}
		}
	}
	sort.Strings(ids)
	read, err := src.Revocations(ctx, schema, ids)
	if err != nil {
		return revocationRequeue{}, fmt.Errorf("%w: the revocation source is unreadable", ErrApply)
	}
	if read.Timeline != m.SourceTimeline {
		return revocationRequeue{}, fmt.Errorf("%w: the revocation source is on timeline %d, the manifest was exported on timeline %d: point it at the shadow restore that exported the manifest", ErrApply, read.Timeline, m.SourceTimeline)
	}
	keyed := make([]map[string]string, len(ids))
	for i, id := range ids {
		keyed[i] = map[string]string{"id": id}
	}
	raw, err := json.Marshal(keyed)
	if err != nil {
		return revocationRequeue{}, fmt.Errorf("%w: encoding the revocation ids", ErrApply)
	}
	return revocationRequeue{ids: string(raw), rows: read.Revocations, authLocks: read.AuthLocks, legacyLocks: read.LegacyLocks}, nil
}

// requeueRevocations runs inside the apply transaction. Any manifest revocation the restored database lacks and the
// source lacks too refuses the whole apply.
func requeueRevocations(ctx context.Context, tx audit.TxOps, rq revocationRequeue, report *Report) error {
	var missing int64
	if err := tx.QueryRow(ctx, requeueMissingSQL, rq.ids, rq.rows).Scan(&missing); err != nil {
		return fmt.Errorf("counting the revocations missing at the source (sqlstate %s)", sqlState(err))
	}
	if missing > 0 {
		return fmt.Errorf("%d manifest revocation(s) the restored database lacks are missing at the source: nothing was written", missing)
	}
	r, err := tx.Query(ctx, requeueInsertSQL, rq.ids, rq.rows)
	if err != nil {
		return fmt.Errorf("re-queueing the revocations (sqlstate %s)", sqlState(err))
	}
	defer r.Close()
	var deliverable, blocked []string
	for r.Next() {
		var id string
		var ok bool
		if err := r.Scan(&id, &ok); err != nil {
			return fmt.Errorf("re-queueing the revocations (sqlstate %s)", sqlState(err))
		}
		if ok {
			deliverable = append(deliverable, id)
		} else {
			blocked = append(blocked, id)
		}
	}
	if err := r.Err(); err != nil {
		return fmt.Errorf("re-queueing the revocations (sqlstate %s)", sqlState(err))
	}
	if len(deliverable) > 0 {
		report.Actions = append(report.Actions, OperatorAction{Kind: ActionProviderRevocationRequeued, Source: revocationSection, IDs: deliverable})
	}
	if len(blocked) > 0 {
		report.Actions = append(report.Actions, OperatorAction{Kind: ActionProviderRevocationRequeuedBlocked, Source: revocationSection, IDs: blocked})
	}
	n := int64(len(deliverable) + len(blocked))
	report.Sections = append(report.Sections, SectionReport{Name: requeuedSection, Changed: n})
	report.Revocations += n
	return nil
}
