// Package refresh is the one writer of public.org_activity. It links the
// ClickHouse driver, so only the metrics worker imports it; the auth plane
// imports package orgactivity, which does not.
package refresh

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/orgactivity/chread"
)

// ErrUnknown is returned by Refresh when a metrics table could not be read: the
// organization's row is left as it was, because a partial fact is not stored.
var ErrUnknown = errors.New("orgactivity: the metrics activity could not be established")

// DB is the Postgres handle the refresher writes through (a pgx pool of the domain role).
type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Refresher is the ONE writer of public.org_activity: it reads an organization's
// activity from ClickHouse (internal/orgactivity/chread, go-api's statements)
// and upserts the row. The upsert is idempotent; the newest read wins.
type Refresher struct {
	CH     driver.Conn
	DB     DB
	Logger *slog.Logger
	// Now is the clock of refreshed_at; nil means time.Now.
	Now func() time.Time
}

func (r *Refresher) logger() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return slog.Default()
}

func (r *Refresher) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

// Refresh reads the activity of one organization and upserts its row. When a
// table read fails it writes nothing and returns ErrUnknown.
func (r *Refresher) Refresh(ctx context.Context, orgID uuid.UUID) error {
	if r == nil || r.CH == nil || r.DB == nil {
		return errors.New("orgactivity: the refresher has no ClickHouse connection or database")
	}
	result := chread.Read(ctx, r.CH, orgID.String(), func(table string, err error) {
		r.logger().WarnContext(ctx, "org activity table read failed, the projection row is left as it was",
			slog.String("table", table))
	})
	if len(result.Failed) > 0 {
		return fmt.Errorf("%w: %s", ErrUnknown, strings.Join(result.Failed, ","))
	}
	var computedAt *time.Time
	if result.HasData {
		computedAt = result.Last
	}
	_, err := r.DB.Exec(ctx, `
INSERT INTO public.org_activity (org_id, has_data, computed_at, refreshed_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (org_id) DO UPDATE
SET has_data = EXCLUDED.has_data, computed_at = EXCLUDED.computed_at, refreshed_at = EXCLUDED.refreshed_at`,
		orgID, result.HasData, computedAt, r.now())
	if err != nil {
		return fmt.Errorf("orgactivity: upserting the row: %w", err)
	}
	return nil
}

// RefreshMissing refreshes up to limit organizations that have no row yet (the
// first fill after the table exists, and organizations created since). It returns how
// many rows it wrote; an organization that cannot be read is skipped and counted.
func (r *Refresher) RefreshMissing(ctx context.Context, limit int) (written, skipped int, err error) {
	if r == nil || r.DB == nil {
		return 0, 0, errors.New("orgactivity: the refresher has no database")
	}
	rows, err := r.DB.Query(ctx, `
SELECT o.id FROM public.organizations o
WHERE NOT EXISTS (SELECT 1 FROM public.org_activity a WHERE a.org_id = o.id)
ORDER BY o.id LIMIT $1`, limit)
	if err != nil {
		return 0, 0, fmt.Errorf("orgactivity: listing organizations without a row: %w", err)
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, 0, fmt.Errorf("orgactivity: listing organizations without a row: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("orgactivity: listing organizations without a row: %w", err)
	}
	for _, id := range ids {
		if err := r.Refresh(ctx, id); err != nil {
			skipped++
			continue
		}
		written++
	}
	return written, skipped, nil
}
