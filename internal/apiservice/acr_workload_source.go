package apiservice

import (
	"context"
	"encoding/base64"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/acr"
)

const (
	acrWorkloadName     = "acr-api"
	acrWorkloadAudience = "dev-health-api"
	// acrSubjectTTL is how long a registry answer is kept: a rotation to a new principal, or a disabled workload,
	// reaches the internal acr routes within it, with no restart.
	acrSubjectTTL = 30 * time.Second
)

// acrWorkloadQuery reads every platform workload row named acr-api with its principal's disabled mark;
// {auth_schema} is the quoted AUTH_DATABASE_SCHEMA. It reads workloads(principal_id, name, organization_id,
// audiences, disabled_at) and principals(id, disabled_at) only.
const acrWorkloadQuery = `SELECT w.principal_id, w.audiences, w.disabled_at IS NOT NULL OR p.disabled_at IS NOT NULL
FROM {auth_schema}.workloads w JOIN {auth_schema}.principals p ON p.id = w.principal_id
WHERE w.organization_id IS NULL AND w.name = $1`

// registryQuerier is what the reader uses of the api pool.
type registryQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type acrWorkloadRow struct {
	principal uuid.UUID
	audiences []string
	disabled  bool
}

// acrSubjectOf is the registry's answer: the session wire form of the principal of the one live acr-api workload,
// the row that is not disabled and admits the go-api audience. No such row, or more than one, is not configured.
func acrSubjectOf(rows []acrWorkloadRow) (string, error) {
	var live []acrWorkloadRow
	for _, row := range rows {
		if !row.disabled && slices.Contains(row.audiences, acrWorkloadAudience) {
			live = append(live, row)
		}
	}
	if len(live) != 1 {
		return "", acr.ErrSubjectNotConfigured
	}
	return acrWirePrincipalID(live[0].principal), nil
}

// newACRSubjectSource reads the acr workload from the workload registry in the auth schema through the api pool. A
// nil pool has no registry: nil, so the routes refuse every workload token as not_configured.
func newACRSubjectSource(pool registryQuerier, authSchema string) acr.SubjectSource {
	if pool == nil {
		return nil
	}
	query := strings.ReplaceAll(acrWorkloadQuery, "{auth_schema}", pgx.Identifier{authSchema}.Sanitize())
	return &acr.CachedSubject{TTL: acrSubjectTTL, Read: func(ctx context.Context) (string, error) {
		rows, err := pool.Query(ctx, query, acrWorkloadName)
		if err != nil {
			return "", fmt.Errorf("acr workload registry read: %w", err)
		}
		defer rows.Close()
		var read []acrWorkloadRow
		for rows.Next() {
			var row acrWorkloadRow
			if err := rows.Scan(&row.principal, &row.audiences, &row.disabled); err != nil {
				return "", fmt.Errorf("acr workload registry read: %w", err)
			}
			read = append(read, row)
		}
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("acr workload registry read: %w", err)
		}
		return acrSubjectOf(read)
	}}
}

// acrSubjectSourceOf is newACRSubjectSource over the api pool: a nil *pgxpool.Pool is no registry (a nil interface,
// not a typed nil).
func acrSubjectSourceOf(pool *pgxpool.Pool, authSchema string) acr.SubjectSource {
	if pool == nil {
		return nil
	}
	return newACRSubjectSource(pool, authSchema)
}

// acrWirePrincipalID is the session wire form of a principal id, "prn_" and the 22 base64url characters of its 16
// bytes: the form a workload token's sub carries (session.WirePrincipalID, not imported here: that package would link
// the whole session stack into every binary that links go-api's routes).
func acrWirePrincipalID(id uuid.UUID) string {
	return "prn_" + base64.RawURLEncoding.EncodeToString(id[:])
}
