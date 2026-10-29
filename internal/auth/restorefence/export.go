package restorefence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrExport reports that the manifest could not be built from the source.
var ErrExport = errors.New("restorefence: exporting the manifest failed")

// ExportManifest reads the manifest from the source database: a shadow restore
// of the WAL archive to its end, connected as the migration role. One read-only
// repeatable-read transaction; nothing is written. Timestamps are read in UTC.
func ExportManifest(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier) (Manifest, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: beginning", ErrExport)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SET LOCAL TimeZone = 'UTC'`); err != nil {
		return Manifest{}, fmt.Errorf("%w: time zone", ErrExport)
	}
	q := authschema.Quote(schema)
	var timeline *int64
	if err := tx.QueryRow(ctx, `SELECT `+q+`.restore_fence_timeline()`).Scan(&timeline); err != nil {
		return Manifest{}, fmt.Errorf("%w: reading the timeline", ErrExport)
	}
	if timeline == nil {
		return Manifest{}, fmt.Errorf("%w: the source is still in recovery; export after it is promoted", ErrExport)
	}
	m := Manifest{Version: ManifestVersion, SourceTimeline: *timeline}

	rows, err := tx.Query(ctx, `SELECT aggregate_type, aggregate_id, revision FROM `+q+`.aggregate_revisions ORDER BY aggregate_type, aggregate_id`)
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: reading the revision floors", ErrExport)
	}
	for rows.Next() {
		var f Floor
		if err := rows.Scan(&f.AggregateType, &f.AggregateID, &f.Revision); err != nil {
			rows.Close()
			return Manifest{}, fmt.Errorf("%w: scanning a floor", ErrExport)
		}
		m.Floors = append(m.Floors, f)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Manifest{}, fmt.Errorf("%w: reading the revision floors", ErrExport)
	}
	if m.Floors == nil {
		m.Floors = []Floor{}
	}

	for _, s := range sections {
		var raw []byte
		if err := tx.QueryRow(ctx, s.exportSQL(schema)).Scan(&raw); err != nil {
			return Manifest{}, fmt.Errorf("%w: reading section %s", ErrExport, s.name)
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var section []Row
		if err := dec.Decode(&section); err != nil {
			return Manifest{}, fmt.Errorf("%w: decoding section %s", ErrExport, s.name)
		}
		if section == nil {
			section = []Row{}
		}
		m.Sections = append(m.Sections, Section{Name: s.name, Rows: section})
	}
	if err := m.Seal(); err != nil {
		return Manifest{}, fmt.Errorf("%w: sealing", ErrExport)
	}
	return m, nil
}
