package restorefence

import (
	"context"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	authLocksSection   = "auth.login_attempts.source"
	legacyLocksSection = "public.login_attempts.source"
)

// authLocksSourceSQL reads the source's live auth lockouts: only a lock still holding is carried (a lock lasts 15
// minutes). The identifier hash travels as hex inside the jsonb, never through Go.
func authLocksSourceSQL(schema authschema.ValidatedIdentifier) string {
	return `SELECT coalesce(jsonb_agg(jsonb_build_object('identifier_hash', encode(identifier_hash, 'hex'), 'attempt_count', attempt_count,
  'first_attempt_at', first_attempt_at, 'locked_until', locked_until) ORDER BY id), '[]'::jsonb)::text
FROM ` + authschema.Quote(schema) + `.login_attempts WHERE locked_until > clock_timestamp()`
}

// legacyLocksSourceSQL reads the source's live go-api lockouts (keyed by email).
const legacyLocksSourceSQL = `SELECT coalesce(jsonb_agg(jsonb_build_object('email', email, 'attempt_count', attempt_count,
  'first_attempt_at', first_attempt_at, 'locked_until', locked_until) ORDER BY id), '[]'::jsonb)::text
FROM public.login_attempts WHERE locked_until > clock_timestamp()`

// authLocksRaiseSQL puts the source's live auth lockouts ($1) into the restored table, raising only: a missing row is
// inserted, an existing one keeps the greater count and the later lock (the live-lock trigger allows only raises).
func authLocksRaiseSQL(schema authschema.ValidatedIdentifier) string {
	return `WITH s AS (SELECT * FROM jsonb_to_recordset($1::jsonb) AS y(identifier_hash text, attempt_count integer, first_attempt_at timestamptz, locked_until timestamptz)),
u AS (INSERT INTO ` + authschema.Quote(schema) + `.login_attempts AS t (identifier_hash, attempt_count, first_attempt_at, locked_until, updated_at)
  SELECT decode(s.identifier_hash, 'hex'), s.attempt_count, s.first_attempt_at, s.locked_until, clock_timestamp() FROM s
  ON CONFLICT (identifier_hash) DO UPDATE SET attempt_count = GREATEST(t.attempt_count, EXCLUDED.attempt_count),
    first_attempt_at = LEAST(t.first_attempt_at, EXCLUDED.first_attempt_at), locked_until = GREATEST(t.locked_until, EXCLUDED.locked_until),
    updated_at = clock_timestamp()
  WHERE t.locked_until IS NULL OR t.locked_until < EXCLUDED.locked_until OR t.attempt_count < EXCLUDED.attempt_count
  RETURNING 1)
SELECT count(*) FROM u`
}

// legacyLocksRaiseSQL is authLocksRaiseSQL for the go-api table (keyed by email; its id has no default).
const legacyLocksRaiseSQL = `WITH s AS (SELECT * FROM jsonb_to_recordset($1::jsonb) AS y(email text, attempt_count integer, first_attempt_at timestamptz, locked_until timestamptz)),
u AS (INSERT INTO public.login_attempts AS t (id, email, attempt_count, first_attempt_at, locked_until, created_at, updated_at)
  SELECT gen_random_uuid(), s.email, s.attempt_count, s.first_attempt_at, s.locked_until, clock_timestamp(), clock_timestamp() FROM s
  ON CONFLICT (email) DO UPDATE SET attempt_count = GREATEST(t.attempt_count, EXCLUDED.attempt_count),
    first_attempt_at = LEAST(t.first_attempt_at, EXCLUDED.first_attempt_at), locked_until = GREATEST(t.locked_until, EXCLUDED.locked_until),
    updated_at = clock_timestamp()
  WHERE t.locked_until IS NULL OR t.locked_until < EXCLUDED.locked_until OR t.attempt_count < EXCLUDED.attempt_count
  RETURNING 1)
SELECT count(*) FROM u`

// raiseLockouts runs inside the apply transaction. It reports counts only: the rows are rate limiting, not authority,
// and their keys are an identifier hash or an email.
func raiseLockouts(ctx context.Context, tx audit.TxOps, schema authschema.ValidatedIdentifier, rq revocationRequeue, report *Report) error {
	for _, step := range []struct{ name, sql, rows string }{
		{authLocksSection, authLocksRaiseSQL(schema), rq.authLocks},
		{legacyLocksSection, legacyLocksRaiseSQL, rq.legacyLocks},
	} {
		var n int64
		if err := tx.QueryRow(ctx, step.sql, step.rows).Scan(&n); err != nil {
			return fmt.Errorf("raising the lockouts of %s (sqlstate %s)", step.name, sqlState(err))
		}
		report.Sections = append(report.Sections, SectionReport{Name: step.name, Changed: n})
		report.Revocations += n
	}
	return nil
}
