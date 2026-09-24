// Package admin's org-deletion route (CHAOS-6306): org_deletion.py's
// OrganizationDeletionService, a dynamic scan+purge of every org-scoped
// Postgres table (orgDeletionTargets, in internal/apiservice/admin/
// orgdeletion_targets.go) and every org_id-bearing ClickHouse table
// (discovered live from system.columns, not by regexing migration files
// the way org_deletion.py does -- a named, accepted divergence proven
// equivalent by a live oracle), plus a best-effort PagerDuty OAuth
// revocation before the encrypted local credential copy is deleted.
package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// jobStatusDisabled is JobStatus.DISABLED.value (settings.py's int enum).
const jobStatusDisabled = 2

// whereClause returns this target's scoping SQL (a $1-parameterized WHERE
// condition, no leading "WHERE") and whether $1 must bind the org's
// uuid.UUID (a native uuid column) or its string form (text/varchar).
func (t deletionTarget) whereClause() (clause string, bindUUID bool) {
	if t.Direct != nil {
		return fmt.Sprintf("%s = $1", t.Direct.Column), t.Direct.UUIDType
	}
	v := t.Via
	return fmt.Sprintf("%s IN (SELECT %s FROM %s WHERE %s = $1)", v.Column, v.SubIDColumn, v.SubTable, v.SubOrgColumn), v.UUIDOrgColumn
}

func (t deletionTarget) bindValue(orgUUID uuid.UUID, orgIDStr string) any {
	_, bindUUID := t.whereClause()
	if bindUUID {
		return orgUUID
	}
	return orgIDStr
}

func (t deletionTarget) countSQL() string {
	clause, _ := t.whereClause()
	return fmt.Sprintf("SELECT count(*) FROM %s WHERE %s", t.Table, clause)
}

func (t deletionTarget) deleteSQL() string {
	clause, _ := t.whereClause()
	return fmt.Sprintf("DELETE FROM %s WHERE %s", t.Table, clause)
}

// deleteOrganization is orgs.py's delete_organization: DELETE
// /orgs/{org_id}?dry_run=bool, Superuser. A nonexistent org_id is not a
// 404 -- every count comes back 0 and the response is the same shape as a
// real deletion, matching org_deletion.py exactly (it never checks
// existence before scanning).
func (h *handlers) deleteOrganization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// org_deletion.py's _uuid_org_id: a malformed org_id is caught by the
	// ROUTER as ValueError -> HTTPException(400, "Invalid organization
	// id") -- unlike getOrganization's bare uuid.Parse (that route has no
	// such try/except and answers the generic 500 instead). The two
	// routes genuinely differ here; this is not a copy of the other.
	orgUUID, parseErr := uuid.Parse(r.PathValue("org_id"))
	if parseErr != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid organization id", nil)
		return
	}
	orgIDStr := orgUUID.String()

	values, queryErr := url.ParseQuery(r.URL.RawQuery)
	if queryErr != nil {
		policy.WriteInternal(w)
		return
	}
	dryRun, boolErr := queryBool(values, "dry_run", false)
	if boolErr != nil {
		writeQueryError(w, boolErr)
		return
	}

	result := &orgDeletionResult{
		organizationID:     orgIDStr,
		dryRun:             dryRun,
		timestamp:          pyTimeString(h.now()),
		postgresTableCount: map[string]int64{},
		clickhouseCount:    map[string]int64{},
	}

	if err := h.orgDeletionCredentialCounts(ctx, orgUUID, orgIDStr, result); err != nil {
		h.logger.ErrorContext(ctx, "admin: org deletion credential counts failed", "error", err)
		policy.WriteInternal(w)
		return
	}

	// PagerDuty is revoked BEFORE the per-target count below (never
	// after), on a real delete only -- matching org_deletion.py's own
	// sequence exactly: a revoked pending revocation row is gone by the
	// time provider_oauth_revocations is counted, so the response's count
	// there is 0, not 1, on both planes.
	if !dryRun {
		if err := h.orgDeletionRevokePagerDuty(ctx, orgIDStr); err != nil {
			h.logger.ErrorContext(ctx, "admin: org deletion pagerduty revoke failed", "error", err)
			policy.WriteInternal(w)
			return
		}
	}

	if err := h.orgDeletionCountTargets(ctx, orgUUID, orgIDStr, result); err != nil {
		h.logger.ErrorContext(ctx, "admin: org deletion target counts failed", "error", err)
		policy.WriteInternal(w)
		return
	}

	if !dryRun {
		if err := h.orgDeletionPurgePostgres(ctx, orgUUID, orgIDStr, result); err != nil {
			h.logger.ErrorContext(ctx, "admin: org deletion postgres purge failed", "error", err)
			policy.WriteInternal(w)
			return
		}
	}

	h.orgDeletionPurgeClickHouse(ctx, orgIDStr, dryRun, result)

	policy.WriteModel(w, http.StatusOK, result.json(), nil)
}

// orgDeletionResult mirrors DeletionResult/DeletionResultResponse, field
// order matching the pydantic response model (pyjson.Object preserves
// insertion order).
type orgDeletionResult struct {
	organizationID     string
	dryRun             bool
	timestamp          string
	postgresTotal      int64
	postgresTables     []string
	postgresTableCount map[string]int64
	clickhouseTotal    int64
	clickhouseTables   []string
	clickhouseCount    map[string]int64
	disabledJobs       int64
	credentialsDeleted int64
	warnings           []string
}

func (r *orgDeletionResult) json() *pyjson.Object {
	postgres := pyjson.NewObject()
	postgres.Set("total", r.postgresTotal)
	postgresTables := pyjson.NewObject()
	for _, table := range r.postgresTables {
		postgresTables.Set(table, r.postgresTableCount[table])
	}
	postgres.Set("tables", postgresTables)

	clickhouseObj := pyjson.NewObject()
	clickhouseObj.Set("total", r.clickhouseTotal)
	clickhouseTables := pyjson.NewObject()
	for _, table := range r.clickhouseTables {
		clickhouseTables.Set(table, r.clickhouseCount[table])
	}
	clickhouseObj.Set("tables", clickhouseTables)

	warnings := make([]pyjson.Value, len(r.warnings))
	for i, w := range r.warnings {
		warnings[i] = w
	}

	out := pyjson.NewObject()
	out.Set("organization_id", r.organizationID)
	out.Set("dry_run", r.dryRun)
	out.Set("timestamp", r.timestamp)
	out.Set("postgres", postgres)
	out.Set("clickhouse", clickhouseObj)
	out.Set("disabled_jobs", r.disabledJobs)
	out.Set("credentials_deleted", r.credentialsDeleted)
	out.Set("warnings", warnings)
	return out
}

// orgDeletionCredentialCounts is delete()'s own first two counts
// (disabled_jobs, credentials_deleted) -- org_deletion.py runs these
// BEFORE _revoke_pagerduty_oauth_before_delete, and the per-target loop
// (orgDeletionCountTargets) AFTER it, so a real delete's response counts
// reflect PagerDuty's pending-revocation row already being gone. See the
// handler's own call sequence for why this is split from that loop rather
// than counted together.
func (h *handlers) orgDeletionCredentialCounts(ctx context.Context, orgUUID uuid.UUID, orgIDStr string, result *orgDeletionResult) error {
	var disabledJobs int64
	if err := h.store.Pool.QueryRow(ctx, "SELECT count(*) FROM scheduled_jobs WHERE org_id = $1", orgIDStr).Scan(&disabledJobs); err != nil {
		return fmt.Errorf("count scheduled_jobs: %w", err)
	}
	result.disabledJobs = disabledJobs

	var credentialRows, encryptedSettings, ssoSecretRows int64
	if err := h.store.Pool.QueryRow(ctx, "SELECT count(*) FROM integration_credentials WHERE org_id = $1", orgIDStr).Scan(&credentialRows); err != nil {
		return fmt.Errorf("count integration_credentials: %w", err)
	}
	if err := h.store.Pool.QueryRow(ctx, "SELECT count(*) FROM settings WHERE org_id = $1 AND is_encrypted = true", orgIDStr).Scan(&encryptedSettings); err != nil {
		return fmt.Errorf("count encrypted settings: %w", err)
	}
	if err := h.store.Pool.QueryRow(ctx, "SELECT count(*) FROM sso_providers WHERE org_id = $1 AND encrypted_secrets IS NOT NULL", orgUUID).Scan(&ssoSecretRows); err != nil {
		return fmt.Errorf("count sso_providers: %w", err)
	}
	result.credentialsDeleted = credentialRows + encryptedSettings + ssoSecretRows
	return nil
}

// orgDeletionCountTargets is delete()'s per-target counting loop, run
// AFTER orgDeletionRevokePagerDuty on a real delete (see the handler's own
// call sequence) so provider_oauth_revocations' count already reflects the
// pending row PagerDuty's own revoke just deleted -- matching Python's
// count = 0 there rather than counting it before revocation removes it.
func (h *handlers) orgDeletionCountTargets(ctx context.Context, orgUUID uuid.UUID, orgIDStr string, result *orgDeletionResult) error {
	for _, target := range orgDeletionTargets {
		var count int64
		if err := h.store.Pool.QueryRow(ctx, target.countSQL(), target.bindValue(orgUUID, orgIDStr)).Scan(&count); err != nil {
			return fmt.Errorf("count %s: %w", target.Table, err)
		}
		result.postgresTables = append(result.postgresTables, target.Table)
		result.postgresTableCount[target.Table] = count
		result.postgresTotal += count
	}
	return nil
}

// orgDeletionRevokePagerDuty is _revoke_pagerduty_oauth_before_delete:
// revoke every stored PagerDuty OAuth grant -- an active credential (whose
// decrypted payload is OAuthTokens' own JSON shape; the refresh token is
// revoked when present, else the access token, matching
// `tokens.refresh_token or tokens.access_token`), or a pending revocation
// record (whose decrypted payload IS the raw token, no JSON wrapping) --
// before its encrypted local copy is deleted below. A revoked pending
// revocation row is deleted here, same as Python's
// `await self.session.delete(pending)`; a credential row is left for the
// main purge pass, which deletes provider_oauth_credentials wholesale.
func (h *handlers) orgDeletionRevokePagerDuty(ctx context.Context, orgIDStr string) error {
	type credentialRow struct {
		credentialName string
		encrypted      string
	}
	credentialRowsQuery, err := h.store.Pool.Query(ctx,
		`SELECT credential_name, token_encrypted FROM provider_oauth_credentials WHERE org_id = $1 AND provider = 'pagerduty'`, orgIDStr)
	if err != nil {
		return fmt.Errorf("query pagerduty credentials: %w", err)
	}
	var credentials []credentialRow
	for credentialRowsQuery.Next() {
		var row credentialRow
		if err := credentialRowsQuery.Scan(&row.credentialName, &row.encrypted); err != nil {
			credentialRowsQuery.Close()
			return fmt.Errorf("scan pagerduty credential: %w", err)
		}
		credentials = append(credentials, row)
	}
	credentialRowsQuery.Close()

	type revocationRow struct {
		id        uuid.UUID
		encrypted string
	}
	revocationRowsQuery, err := h.store.Pool.Query(ctx,
		`SELECT id, token_encrypted FROM provider_oauth_revocations WHERE org_id = $1 AND provider = 'pagerduty'`, orgIDStr)
	if err != nil {
		return fmt.Errorf("query pagerduty revocations: %w", err)
	}
	var revocations []revocationRow
	for revocationRowsQuery.Next() {
		var row revocationRow
		if err := revocationRowsQuery.Scan(&row.id, &row.encrypted); err != nil {
			revocationRowsQuery.Close()
			return fmt.Errorf("scan pagerduty revocation: %w", err)
		}
		revocations = append(revocations, row)
	}
	revocationRowsQuery.Close()

	if len(credentials) == 0 && len(revocations) == 0 {
		return nil
	}
	if h.pagerDuty.ClientID == "" {
		return fmt.Errorf("pagerduty oauth configuration is unavailable for deletion")
	}

	for _, row := range credentials {
		plaintext, decryptErr := h.decryptor.Decrypt(secrets.NewValue(row.encrypted))
		if decryptErr != nil {
			return fmt.Errorf("decrypt pagerduty credential %s: %w", row.credentialName, decryptErr)
		}
		token, decodeErr := pagerDutyRevokeToken(plaintext)
		if decodeErr != nil {
			return fmt.Errorf("decode pagerduty credential %s: %w", row.credentialName, decodeErr)
		}
		if revokeErr := providerfoundation.RevokePagerDutyOAuthToken(ctx, h.httpDoer, h.pagerDuty, token); revokeErr != nil {
			return fmt.Errorf("revoke pagerduty credential %s: %w", row.credentialName, revokeErr)
		}
	}
	for _, row := range revocations {
		plaintext, decryptErr := h.decryptor.Decrypt(secrets.NewValue(row.encrypted))
		if decryptErr != nil {
			return fmt.Errorf("decrypt pagerduty revocation %s: %w", row.id, decryptErr)
		}
		if revokeErr := providerfoundation.RevokePagerDutyOAuthToken(ctx, h.httpDoer, h.pagerDuty, string(plaintext)); revokeErr != nil {
			return fmt.Errorf("revoke pagerduty pending revocation %s: %w", row.id, revokeErr)
		}
		if _, err := h.store.Pool.Exec(ctx, `DELETE FROM provider_oauth_revocations WHERE id = $1`, row.id); err != nil {
			return fmt.Errorf("delete pagerduty revocation %s: %w", row.id, err)
		}
	}
	return nil
}

// pagerDutyRevokeToken is OAuthTokens.model_validate_json(...) followed by
// `tokens.refresh_token or tokens.access_token` -- the token a live
// credential's decrypted JSON payload actually revokes.
func pagerDutyRevokeToken(plaintext []byte) (string, error) {
	var tokens struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(plaintext, &tokens); err != nil {
		return "", err
	}
	if tokens.RefreshToken != "" {
		return tokens.RefreshToken, nil
	}
	return tokens.AccessToken, nil
}

// orgDeletionPurgePostgres disables every scheduled job for the org, then
// deletes every target with a nonzero count, all in one transaction --
// org_deletion.py's session.flush() at the end of its one implicit
// transaction is the same shape.
func (h *handlers) orgDeletionPurgePostgres(ctx context.Context, orgUUID uuid.UUID, orgIDStr string, result *orgDeletionResult) error {
	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`UPDATE scheduled_jobs SET status = $2, is_running = false, next_run_at = NULL, updated_at = $3 WHERE org_id = $1`,
		orgIDStr, jobStatusDisabled, h.now()); err != nil {
		return fmt.Errorf("disable scheduled_jobs: %w", err)
	}

	for _, target := range orgDeletionTargets {
		if result.postgresTableCount[target.Table] == 0 {
			continue
		}
		if _, err := tx.Exec(ctx, target.deleteSQL(), target.bindValue(orgUUID, orgIDStr)); err != nil {
			return fmt.Errorf("delete %s: %w", target.Table, err)
		}
	}

	return tx.Commit(ctx)
}

// orgDeletionPurgeClickHouse discovers every org_id-bearing ClickHouse
// table live from system.columns (the accepted divergence from
// org_deletion.py's static regex-parse of migration files -- see this
// file's package doc comment), counts and (not dry_run) deletes each,
// exactly as org_deletion.py's own per-table loop does. Every failure here
// -- no ClickHouse configured, a table's own count/delete erroring -- is a
// warning, never a request failure: ClickHouse is a secondary analytics
// store and org_deletion.py treats it the same way.
func (h *handlers) orgDeletionPurgeClickHouse(ctx context.Context, orgIDStr string, dryRun bool, result *orgDeletionResult) {
	if h.clickHouseDSN == "" {
		result.warnings = append(result.warnings, "ClickHouse URI not configured; analytics tables were not verified.")
		return
	}
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(h.clickHouseDSN))
	if err != nil {
		result.warnings = append(result.warnings, "ClickHouse URI not configured; analytics tables were not verified.")
		return
	}
	defer conn.Close()

	tables, err := DiscoverClickHouseOrgTables(ctx, conn)
	if err != nil || len(tables) == 0 {
		result.warnings = append(result.warnings, "ClickHouse migration table catalog is empty.")
		return
	}

	// mutations_sync=1 makes each ALTER TABLE DELETE below block until
	// ClickHouse has actually applied it, rather than merely queuing an
	// async mutation -- same synchronous-write discipline
	// internal/providersync's own ClickHouse ALTER TABLE DELETE callers
	// already use (linear_pseudo_project_cleanup.go,
	// linear_stale_project_ownership_cleanup.go). Without this, a 200
	// response here proves nothing about whether the rows are actually
	// gone: a codex-review round on this PR reproduced target rows
	// surviving a "successful" delete by pausing merges on the seeded
	// table and observing the response return before the mutation applied.
	syncCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"mutations_sync": "1",
	}))

	for _, t := range tables {
		condition := "org_id = ?"
		var bind any = orgIDStr
		if IsUUIDColumnType(t.OrgIDType) {
			condition = "org_id = toUUID(?)"
		}
		var count uint64
		countRow := conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM `%s` WHERE %s", t.Name, condition), bind)
		if err := countRow.Scan(&count); err != nil {
			result.warnings = append(result.warnings, fmt.Sprintf("ClickHouse table %s missing or has no org_id column; skipped.", t.Name))
			continue
		}
		result.clickhouseTables = append(result.clickhouseTables, t.Name)
		result.clickhouseCount[t.Name] = int64(count)
		result.clickhouseTotal += int64(count)
		if dryRun || count == 0 {
			continue
		}
		if err := conn.Exec(syncCtx, fmt.Sprintf("ALTER TABLE `%s` DELETE WHERE %s", t.Name, condition), bind); err != nil {
			result.warnings = append(result.warnings, fmt.Sprintf("Unable to delete ClickHouse table %s.", t.Name))
		}
	}
}

// ChTable is one org_id-bearing ClickHouse table, as
// DiscoverClickHouseOrgTables finds it.
type ChTable struct{ Name, OrgIDType string }

// DiscoverClickHouseOrgTables is the live table-discovery half of the
// CHAOS-6306 named divergence from org_deletion.py's static regex-parse of
// migration files (see this file's package doc comment): every table in
// the connected database carrying a column literally named org_id, read
// straight from system.columns rather than duplicated as a second,
// hand-maintained list. It is exported and is the ONE place this query is
// written -- both orgDeletionPurgeClickHouse and the live table-set
// agreement oracle (orgdeletion_clickhouse_oracle_test.go, package
// admin_test) call it, so a test asserting the two discovery mechanisms
// agree is never quietly comparing itself.
func DiscoverClickHouseOrgTables(ctx context.Context, conn driver.Conn) ([]ChTable, error) {
	// A plain VIEW (engine = 'View') has no storage of its own -- it is a
	// saved SELECT over another table this query already discovers
	// directly. A MaterializedView is excluded too: verified live against
	// this repo's actual schema (git_blame_dirty_paths_mv, currently the
	// only live MV any migration creates), ClickHouse flatly refuses
	// ALTER TABLE ... DELETE against a MATERIALIZED VIEW object --
	// "MATERIALIZED VIEW targets existing table X. Execute the statement
	// directly on it." (error 80) -- so it can never be a real purge
	// target by its own name; the table it feeds (git_blame_dirty_paths
	// here) already carries its own org_id column and is discovered and
	// purged directly, so nothing is left unpurged by excluding the MV.
	rows, err := conn.Query(ctx,
		`SELECT c.table, c.type FROM system.columns AS c
		 JOIN system.tables AS t ON t.database = c.database AND t.name = c.table
		 WHERE c.database = currentDatabase() AND c.name = 'org_id' AND t.engine NOT IN ('View', 'MaterializedView')
		 ORDER BY c.table`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []ChTable
	for rows.Next() {
		var t ChTable
		if err := rows.Scan(&t.Name, &t.OrgIDType); err != nil {
			continue
		}
		tables = append(tables, t)
	}
	return tables, nil
}

// IsUUIDColumnType reports whether a ClickHouse column type string names
// UUID (its own type, or a Nullable(UUID)/LowCardinality(UUID) wrapper).
// Exported so a test can bind the same way orgDeletionPurgeClickHouse does,
// never a second hand-authored copy of this rule.
func IsUUIDColumnType(chType string) bool {
	for i := 0; i+4 <= len(chType); i++ {
		if chType[i:i+4] == "UUID" {
			return true
		}
	}
	return false
}
