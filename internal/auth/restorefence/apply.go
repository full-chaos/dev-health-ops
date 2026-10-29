package restorefence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrApply reports a refused or failed apply. The transaction rolled back and
// nothing was written; readiness stays closed.
var ErrApply = errors.New("restorefence: apply failed")

// ErrPrivileges reports tables the migration role cannot write.
var ErrPrivileges = errors.New("restorefence: the migration role lacks privileges the fence needs")

// errDryRun ends the transaction of a dry run after the counts are read.
var errDryRun = errors.New("restorefence: dry run")

// errAlready ends the transaction when this timeline and manifest were applied.
var errAlready = errors.New("restorefence: already applied")

// Options set one apply.
type Options struct {
	// Apply writes the change. Without it the run reads the counts inside a
	// transaction that rolls back.
	Apply bool
	// BlindMargin raises every restored revision by this much. It is the
	// fallback of last resort, used only when no manifest and no usable consumer
	// input exists (the WAL after T is gone): consumers read the jump as a gap.
	BlindMargin int64
	// Inputs are consumer high-water inputs besides the registered ones.
	Inputs []FloorInput
}

// SectionReport is the count of one table.
type SectionReport struct {
	Name string `json:"name"`
	// Changed is the rows revoked again, raised or deleted.
	Changed int64 `json:"changed"`
	// Absent is the manifest rows with no restored row: created after T, so
	// they cannot authenticate. They are reported and never inserted.
	Absent int64 `json:"absent"`
}

// Report is what one apply did (or, as a dry run, would do).
type Report struct {
	Timeline       int64           `json:"timeline"`
	Source         string          `json:"source"`
	Digest         string          `json:"digest"`
	DryRun         bool            `json:"dry_run"`
	AlreadyApplied bool            `json:"already_applied"`
	FloorsRaised   int64           `json:"floors_raised"`
	MarginRows     int64           `json:"margin_rows"`
	Revocations    int64           `json:"revocations_reapplied"`
	RowsRemoved    int64           `json:"rows_removed"`
	Sections       []SectionReport `json:"sections"`
	// Inputs is one line per floor input: used or skipped, and why.
	Inputs []InputReport `json:"inputs"`
}

const blindDigest = "blind_margin"

// Preflight lists the tables the migration role cannot write, by name only.
func Preflight(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier) ([]string, error) {
	var missing []string
	var user string
	if err := pool.QueryRow(ctx, `SELECT current_user`).Scan(&user); err != nil {
		return nil, fmt.Errorf("%w: reading the connected role", ErrApply)
	}
	check := func(name, qualified, privilege string) error {
		var ok *bool
		if err := pool.QueryRow(ctx, `SELECT CASE WHEN to_regclass($1) IS NULL THEN NULL ELSE has_table_privilege($3::text, $1, $2) END`,
			qualified, privilege, user).Scan(&ok); err != nil {
			return fmt.Errorf("%w: reading privileges", ErrApply)
		}
		if ok == nil || !*ok {
			missing = append(missing, name+" "+privilege)
		}
		return nil
	}
	q := authschema.Quote(schema)
	for _, p := range []string{"SELECT", "INSERT", "UPDATE"} {
		if err := check(floorsSection, q+".aggregate_revisions", p); err != nil {
			return nil, err
		}
	}
	if err := check("auth.restore_fence_state", q+".restore_fence_state", "INSERT"); err != nil {
		return nil, err
	}
	for _, s := range sections {
		privilege := "UPDATE"
		if s.kind == kindPresence {
			privilege = "DELETE"
		}
		if err := check(s.name, s.qualified(schema), privilege); err != nil {
			return nil, err
		}
	}
	return missing, nil
}

func complete(m Manifest) error {
	have := map[string]bool{}
	for _, s := range m.Sections {
		have[s.Name] = true
	}
	var absent []string
	for _, s := range sections {
		if !have[s.name] {
			absent = append(absent, s.name)
		}
	}
	if len(absent) > 0 {
		return fmt.Errorf("%w: sections missing from the manifest: %s", ErrManifest, strings.Join(absent, ", "))
	}
	return nil
}

// Apply runs the fence on the restored database as the migration role, in ONE
// audit.Commit transaction: the margin and the revision floors, the auth
// revocations, the named legacy section, and the state row for the current
// timeline. m may be nil only with a blind margin. Any error rolls everything
// back. A second run for the same timeline and manifest changes nothing.
func Apply(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, m *Manifest, opts Options) (Report, error) {
	if opts.BlindMargin < 0 || opts.BlindMargin >= maxRevision {
		return Report{}, fmt.Errorf("%w: the blind margin is outside 0..2^53-2", ErrApply)
	}
	if m != nil {
		if err := complete(*m); err != nil {
			return Report{}, err
		}
		d, err := m.Digest()
		if err != nil || d != m.SHA256 {
			return Report{}, fmt.Errorf("%w: digest does not match the content", ErrManifest)
		}
	}
	plan, err := resolveInputs(ctx, m, opts, append(RegisteredFloorInputs(), opts.Inputs...))
	if err != nil {
		return Report{}, err
	}
	missing, err := Preflight(ctx, pool, schema)
	if err != nil {
		return Report{}, err
	}
	if len(missing) > 0 {
		return Report{}, fmt.Errorf("%w: %s", ErrPrivileges, strings.Join(missing, "; "))
	}
	digest, source := plan.digest, plan.source
	floorsJSON, err := json.Marshal(plan.floors)
	if err != nil {
		return Report{}, fmt.Errorf("%w: encoding the floors", ErrApply)
	}

	q := authschema.Quote(schema)
	report := Report{Source: source, Digest: digest, DryRun: !opts.Apply, Inputs: plan.reports}
	commitErr := audit.Commit(ctx, pool, schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var timeline *int64
			if err := tx.QueryRow(ctx, `SELECT `+q+`.restore_fence_timeline()`).Scan(&timeline); err != nil {
				return fmt.Errorf("reading the timeline")
			}
			if timeline == nil {
				return fmt.Errorf("the server is still in recovery")
			}
			report.Timeline = *timeline
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+q+`.restore_fence_state WHERE timeline_id = $1 AND manifest_sha256 = $2)`,
				*timeline, digest).Scan(&exists); err != nil {
				return fmt.Errorf("reading the fence state")
			}
			if exists {
				return errAlready
			}
			if plan.margin > 0 {
				tag, err := tx.Exec(ctx, `UPDATE `+q+`.aggregate_revisions SET revision = LEAST(revision + $1, `+fmt.Sprint(maxRevision)+`), updated_at = clock_timestamp()`, plan.margin)
				if err != nil {
					return fmt.Errorf("raising the revisions by the margin (sqlstate %s)", sqlState(err))
				}
				report.MarginRows = tag.RowsAffected()
			}
			tag, err := tx.Exec(ctx, `
				WITH m AS (SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(aggregate_type text, aggregate_id text, revision bigint))
				INSERT INTO `+q+`.aggregate_revisions AS r (aggregate_type, aggregate_id, revision)
				SELECT aggregate_type, aggregate_id, revision FROM m
				ON CONFLICT (aggregate_type, aggregate_id)
				DO UPDATE SET revision = EXCLUDED.revision, updated_at = clock_timestamp()
				WHERE r.revision < EXCLUDED.revision`, string(floorsJSON))
			if err != nil {
				return fmt.Errorf("raising the revision floors (sqlstate %s)", sqlState(err))
			}
			report.FloorsRaised = tag.RowsAffected()
			for _, s := range sections {
				rows := []Row{}
				if m != nil {
					for _, ms := range m.Sections {
						if ms.Name == s.name {
							rows = ms.Rows
						}
					}
				}
				raw, err := json.Marshal(rows)
				if err != nil {
					return fmt.Errorf("encoding section %s", s.name)
				}
				var changed, absent int64
				if err := tx.QueryRow(ctx, s.applySQL(schema), string(raw)).Scan(&changed, &absent); err != nil {
					return fmt.Errorf("applying section %s (sqlstate %s)", s.name, sqlState(err))
				}
				report.Sections = append(report.Sections, SectionReport{Name: s.name, Changed: changed, Absent: absent})
				if s.kind == kindPresence {
					report.RowsRemoved += changed
				} else {
					report.Revocations += changed
				}
			}
			if !opts.Apply {
				return errDryRun
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+q+`.restore_fence_state (timeline_id, manifest_sha256, source, floors_raised, revocations_reapplied)
				VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`,
				*timeline, digest, source, report.FloorsRaised+report.MarginRows, report.Revocations+report.RowsRemoved); err != nil {
				return fmt.Errorf("recording the fence (sqlstate %s)", sqlState(err))
			}
			return nil
		},
		Audit: audit.AuditEvent{
			EventType: "auth.restore_fence.applied", Outcome: audit.OutcomeAllowed,
		},
		OffStream: audit.OffStreamRestoreFence,
	})
	switch {
	case errors.Is(commitErr, errDryRun):
		return report, nil
	case errors.Is(commitErr, errAlready):
		report.AlreadyApplied = true
		report.Sections = nil
		report.FloorsRaised, report.MarginRows, report.Revocations, report.RowsRemoved = 0, 0, 0, 0
		return report, nil
	case commitErr != nil:
		return Report{}, fmt.Errorf("%w: %v", ErrApply, commitErr)
	}
	return report, nil
}

// sqlState is the SQLSTATE of a database error, or "none": the error text itself
// can quote a row, so only the code is reported.
func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return "none"
}
