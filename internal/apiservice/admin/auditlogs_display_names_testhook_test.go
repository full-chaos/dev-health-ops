package admin

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// AuditLogProjectionSQLForTest gives the external real-Postgres venue test
// the exact production projection. This _test.go seam is absent from the
// production binary.
func AuditLogProjectionSQLForTest() string {
	return `SELECT ` + auditLogColumns + auditLogFrom
}

// AuditLogProjectionExplainSQLForTest runs the exact production projection
// through PostgreSQL's JSON plan form. The caller supplies the production
// WHERE clause and bindings. This _test.go seam is absent from the production
// binary.
func AuditLogProjectionExplainSQLForTest() string {
	return `EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) ` + AuditLogProjectionSQLForTest()
}

// AuditLogJoinBranchStateForTest holds only branch booleans. It carries no
// labels, IDs, or row content.
type AuditLogJoinBranchStateForTest struct {
	TargetIDType        string
	TargetOrgIDType     string
	ResourceTypeMatches bool
	UUIDGuardMatches    bool
	TargetIDMatches     bool
	TargetScopeMatches  bool
	JoinedRowPresent    bool
	DisplayPresent      bool
}

// AuditLogJoinDiagnosticStateForTest is the bounded state from one exact
// auditLogFrom statement. The target checks use correlated EXISTS predicates
// over the same audit row; the final two booleans in each branch read the
// joined alias from auditLogFrom itself.
type AuditLogJoinDiagnosticStateForTest struct {
	ResourceType   string
	ResourceIDType string
	OrgIDType      string
	Organization   AuditLogJoinBranchStateForTest
	Provider       AuditLogJoinBranchStateForTest
	Source         AuditLogJoinBranchStateForTest
	Token          AuditLogJoinBranchStateForTest
}

// AuditLogJoinDiagnosticSQLForTest keeps auditLogFrom byte-for-byte. Each
// branch captures its resource-type guard, UUID guard, ID lookup, audited-org
// lookup, and the resulting joined alias in one statement. The CASE projection
// is measured by AuditLogResourceDisplayNameStateForTest, which scans
// AuditLogProjectionSQLForTest. auditLogColumns has no COALESCE branch; its
// string branches use nullif(btrim(...), empty string).
func AuditLogJoinDiagnosticSQLForTest() string {
	const uuidGuard = `a.resource_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'`
	const resourceID = `CASE WHEN ` + uuidGuard + ` THEN a.resource_id::uuid END`
	return `SELECT a.resource_type,
	pg_typeof(a.resource_id)::text,
	pg_typeof(a.org_id)::text,
	a.resource_type = 'organization',
	` + uuidGuard + `,
	EXISTS (SELECT 1 FROM organizations probe WHERE probe.id = ` + resourceID + `),
	EXISTS (SELECT 1 FROM organizations probe WHERE probe.id = ` + resourceID + ` AND probe.id = a.org_id),
	pg_typeof(resource_org.id)::text,
	pg_typeof(resource_org.id)::text,
	resource_org.id IS NOT NULL,
	resource_org.name IS NOT NULL,
	a.resource_type = 'sso_provider',
	` + uuidGuard + `,
	EXISTS (SELECT 1 FROM sso_providers probe WHERE probe.id = ` + resourceID + `),
	EXISTS (SELECT 1 FROM sso_providers probe WHERE probe.id = ` + resourceID + ` AND probe.org_id = a.org_id),
	pg_typeof(provider.id)::text,
	pg_typeof(provider.org_id)::text,
	provider.id IS NOT NULL,
	provider.name IS NOT NULL,
	a.resource_type = 'ingest_source',
	` + uuidGuard + `,
	EXISTS (SELECT 1 FROM external_ingest_sources probe WHERE probe.id = ` + resourceID + `),
	EXISTS (SELECT 1 FROM external_ingest_sources probe WHERE probe.id = ` + resourceID + ` AND probe.org_id = a.org_id::text),
	pg_typeof(source.id)::text,
	pg_typeof(source.org_id)::text,
	source.id IS NOT NULL,
	nullif(btrim(source.display_name), '') IS NOT NULL,
	a.resource_type = 'ingest_token',
	` + uuidGuard + `,
	EXISTS (SELECT 1 FROM external_ingest_tokens probe WHERE probe.id = ` + resourceID + `),
	EXISTS (SELECT 1 FROM external_ingest_tokens probe WHERE probe.id = ` + resourceID + ` AND probe.org_id = a.org_id::text),
	pg_typeof(token.id)::text,
	pg_typeof(token.org_id)::text,
	token.id IS NOT NULL,
	token.name IS NOT NULL` + auditLogFrom
}

// AuditLogJoinStateForTest reads the bounded exact-statement diagnostic.
func AuditLogJoinStateForTest(row pgx.Row) (state AuditLogJoinDiagnosticStateForTest, err error) {
	err = row.Scan(
		&state.ResourceType,
		&state.ResourceIDType,
		&state.OrgIDType,
		&state.Organization.ResourceTypeMatches,
		&state.Organization.UUIDGuardMatches,
		&state.Organization.TargetIDMatches,
		&state.Organization.TargetScopeMatches,
		&state.Organization.TargetIDType,
		&state.Organization.TargetOrgIDType,
		&state.Organization.JoinedRowPresent,
		&state.Organization.DisplayPresent,
		&state.Provider.ResourceTypeMatches,
		&state.Provider.UUIDGuardMatches,
		&state.Provider.TargetIDMatches,
		&state.Provider.TargetScopeMatches,
		&state.Provider.TargetIDType,
		&state.Provider.TargetOrgIDType,
		&state.Provider.JoinedRowPresent,
		&state.Provider.DisplayPresent,
		&state.Source.ResourceTypeMatches,
		&state.Source.UUIDGuardMatches,
		&state.Source.TargetIDMatches,
		&state.Source.TargetScopeMatches,
		&state.Source.TargetIDType,
		&state.Source.TargetOrgIDType,
		&state.Source.JoinedRowPresent,
		&state.Source.DisplayPresent,
		&state.Token.ResourceTypeMatches,
		&state.Token.UUIDGuardMatches,
		&state.Token.TargetIDMatches,
		&state.Token.TargetScopeMatches,
		&state.Token.TargetIDType,
		&state.Token.TargetOrgIDType,
		&state.Token.JoinedRowPresent,
		&state.Token.DisplayPresent,
	)
	return state, err
}

// AuditLogPlanNodeForTest retains only fixed structural plan facts. It never
// retains PostgreSQL expression text, row values, IDs, labels, or credentials.
type AuditLogPlanNodeForTest struct {
	NodeType                         string
	JoinType                         string
	RelationName                     string
	Alias                            string
	PathDepth                        int
	ChildSide                        string
	ActualRows                       float64
	ActualLoops                      float64
	ActualRowsPresent                bool
	ActualLoopsPresent               bool
	RowsRemovedByJoinFilter          float64
	RowsRemovedByJoinFilterPresent   bool
	RowsRemovedByFilter              float64
	RowsRemovedByFilterPresent       bool
	RowsRemovedByIndexRecheck        float64
	RowsRemovedByIndexRecheckPresent bool
	JoinFilterPresent                bool
	HashCondPresent                  bool
	MergeCondPresent                 bool
	IndexCondPresent                 bool
	FilterPresent                    bool
	JoinFilterSHA256                 string
	HashCondSHA256                   string
	MergeCondSHA256                  string
	IndexCondSHA256                  string
	FilterSHA256                     string
}

// AuditLogProjectionPlanStateForTest is the fixed subset of one exact
// production projection plan. Nodes has each fixed alias once. TargetPaths
// retains every root-to-target parent/child node for the four resource joins.
// Aliases and relation names are limited to auditLogFrom's fixed identifiers.
type AuditLogProjectionPlanStateForTest struct {
	Nodes       map[string]AuditLogPlanNodeForTest
	TargetPaths map[string][]AuditLogPlanNodeForTest
}

type auditLogPlanDocumentForTest struct {
	Plan auditLogPlanJSONForTest `json:"Plan"`
}

type auditLogPlanJSONForTest struct {
	NodeType                  string                    `json:"Node Type"`
	JoinType                  string                    `json:"Join Type"`
	RelationName              string                    `json:"Relation Name"`
	Alias                     string                    `json:"Alias"`
	ActualRows                *float64                  `json:"Actual Rows"`
	ActualLoops               *float64                  `json:"Actual Loops"`
	RowsRemovedByJoinFilter   *float64                  `json:"Rows Removed by Join Filter"`
	RowsRemovedByFilter       *float64                  `json:"Rows Removed by Filter"`
	RowsRemovedByIndexRecheck *float64                  `json:"Rows Removed by Index Recheck"`
	JoinFilter                json.RawMessage           `json:"Join Filter"`
	HashCond                  json.RawMessage           `json:"Hash Cond"`
	MergeCond                 json.RawMessage           `json:"Merge Cond"`
	IndexCond                 json.RawMessage           `json:"Index Cond"`
	Filter                    json.RawMessage           `json:"Filter"`
	Plans                     []auditLogPlanJSONForTest `json:"Plans"`
}

// AuditLogProjectionPlanForTest parses one EXPLAIN JSON row. It rejects
// absent or malformed plans so the external venue cannot read an omitted
// measurement as evidence.
func AuditLogProjectionPlanForTest(row pgx.Row) (AuditLogProjectionPlanStateForTest, error) {
	var payload []byte
	if err := row.Scan(&payload); err != nil {
		return AuditLogProjectionPlanStateForTest{}, err
	}
	var documents []auditLogPlanDocumentForTest
	if err := json.Unmarshal(payload, &documents); err != nil {
		return AuditLogProjectionPlanStateForTest{}, fmt.Errorf("decode audit projection plan JSON: %w", err)
	}
	if len(documents) != 1 || documents[0].Plan.NodeType == "" {
		return AuditLogProjectionPlanStateForTest{}, fmt.Errorf("audit projection plan JSON lacks one root node")
	}
	state := AuditLogProjectionPlanStateForTest{
		Nodes:       make(map[string]AuditLogPlanNodeForTest),
		TargetPaths: make(map[string][]AuditLogPlanNodeForTest),
	}
	if err := auditLogCollectPlanNodesForTest(documents[0].Plan, nil, "root", state.Nodes, state.TargetPaths); err != nil {
		return AuditLogProjectionPlanStateForTest{}, err
	}
	if len(state.Nodes) == 0 {
		return AuditLogProjectionPlanStateForTest{}, fmt.Errorf("audit projection plan JSON lacks fixed aliases")
	}
	for _, alias := range []string{"resource_org", "provider", "source", "token"} {
		if len(state.TargetPaths[alias]) < 2 {
			return AuditLogProjectionPlanStateForTest{}, fmt.Errorf("audit projection plan JSON lacks parent path for %s", alias)
		}
	}
	return state, nil
}

func auditLogCollectPlanNodesForTest(
	node auditLogPlanJSONForTest,
	parentPath []AuditLogPlanNodeForTest,
	childSide string,
	nodes map[string]AuditLogPlanNodeForTest,
	targetPaths map[string][]AuditLogPlanNodeForTest,
) error {
	planNode, err := auditLogPlanNodeForTest(node, len(parentPath), childSide)
	if err != nil {
		return err
	}
	path := append(append([]AuditLogPlanNodeForTest(nil), parentPath...), planNode)
	if auditLogFixedPlanAliasForTest(node.Alias) {
		if _, exists := nodes[node.Alias]; exists {
			return fmt.Errorf("audit projection plan JSON has duplicate fixed alias %s", node.Alias)
		}
		nodes[node.Alias] = planNode
	}
	if auditLogTargetPlanAliasForTest(node.Alias) {
		if _, exists := targetPaths[node.Alias]; exists {
			return fmt.Errorf("audit projection plan JSON has duplicate target path for %s", node.Alias)
		}
		targetPaths[node.Alias] = path
	}
	for index, child := range node.Plans {
		if err := auditLogCollectPlanNodesForTest(child, path, auditLogPlanChildSideForTest(node, index), nodes, targetPaths); err != nil {
			return err
		}
	}
	return nil
}

func auditLogPlanNodeForTest(node auditLogPlanJSONForTest, pathDepth int, childSide string) (AuditLogPlanNodeForTest, error) {
	if node.NodeType == "" {
		return AuditLogPlanNodeForTest{}, fmt.Errorf("audit projection plan JSON has a node without node type")
	}
	if !auditLogFixedPlanAliasForTest(node.Alias) && node.Alias != "" {
		return AuditLogPlanNodeForTest{}, fmt.Errorf("audit projection plan JSON has unapproved alias")
	}
	if !auditLogFixedPlanRelationForTest(node.RelationName) {
		return AuditLogPlanNodeForTest{}, fmt.Errorf("audit projection plan JSON has unapproved relation")
	}
	planNode := AuditLogPlanNodeForTest{
		NodeType:          node.NodeType,
		JoinType:          node.JoinType,
		RelationName:      node.RelationName,
		Alias:             node.Alias,
		PathDepth:         pathDepth,
		ChildSide:         childSide,
		JoinFilterPresent: len(node.JoinFilter) != 0,
		HashCondPresent:   len(node.HashCond) != 0,
		MergeCondPresent:  len(node.MergeCond) != 0,
		IndexCondPresent:  len(node.IndexCond) != 0,
		FilterPresent:     len(node.Filter) != 0,
		JoinFilterSHA256:  auditLogPlanConditionHashForTest(node.JoinFilter),
		HashCondSHA256:    auditLogPlanConditionHashForTest(node.HashCond),
		MergeCondSHA256:   auditLogPlanConditionHashForTest(node.MergeCond),
		IndexCondSHA256:   auditLogPlanConditionHashForTest(node.IndexCond),
		FilterSHA256:      auditLogPlanConditionHashForTest(node.Filter),
	}
	if node.ActualRows != nil {
		planNode.ActualRows = *node.ActualRows
		planNode.ActualRowsPresent = true
	}
	if node.ActualLoops != nil {
		planNode.ActualLoops = *node.ActualLoops
		planNode.ActualLoopsPresent = true
	}
	if node.RowsRemovedByJoinFilter != nil {
		planNode.RowsRemovedByJoinFilter = *node.RowsRemovedByJoinFilter
		planNode.RowsRemovedByJoinFilterPresent = true
	}
	if node.RowsRemovedByFilter != nil {
		planNode.RowsRemovedByFilter = *node.RowsRemovedByFilter
		planNode.RowsRemovedByFilterPresent = true
	}
	if node.RowsRemovedByIndexRecheck != nil {
		planNode.RowsRemovedByIndexRecheck = *node.RowsRemovedByIndexRecheck
		planNode.RowsRemovedByIndexRecheckPresent = true
	}
	return planNode, nil
}

func auditLogPlanConditionHashForTest(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	digest := sha256.Sum256(raw)
	return fmt.Sprintf("%x", digest)
}

func auditLogPlanChildSideForTest(parent auditLogPlanJSONForTest, childIndex int) string {
	if parent.JoinType != "" {
		switch childIndex {
		case 0:
			return "outer"
		case 1:
			return "inner"
		default:
			return fmt.Sprintf("join-child-%d", childIndex)
		}
	}
	return fmt.Sprintf("child-%d", childIndex)
}

func auditLogTargetPlanAliasForTest(alias string) bool {
	switch alias {
	case "resource_org", "provider", "source", "token":
		return true
	default:
		return false
	}
}

func auditLogFixedPlanAliasForTest(alias string) bool {
	switch alias {
	case "a", "actor", "resource_membership", "resource_user", "resource_org", "provider", "source", "token":
		return true
	default:
		return false
	}
}

func auditLogFixedPlanRelationForTest(relation string) bool {
	switch relation {
	case "", "audit_logs", "users", "memberships", "organizations", "sso_providers", "external_ingest_sources", "external_ingest_tokens":
		return true
	default:
		return false
	}
}

// AuditLogResourceDisplayNameStateForTest executes the production scanner and
// object encoder for the exact projection supplied by
// AuditLogProjectionSQLForTest. The external venue test records only
// presence, never a fixture label or row contents.
func AuditLogResourceDisplayNameStateForTest(row pgx.Row) (found, scannedPresent, encodedPresent bool, err error) {
	log, err := scanAuditLog(row)
	if err != nil || log == nil {
		return false, false, false, err
	}
	object, err := auditLogObject(log)
	if err != nil {
		return true, log.ResourceDisplayName != nil, false, err
	}
	value, ok := object.Get("resource_display_name")
	if !ok {
		return true, log.ResourceDisplayName != nil, false, nil
	}
	_, encodedPresent = value.(string)
	return true, log.ResourceDisplayName != nil, encodedPresent, nil
}
