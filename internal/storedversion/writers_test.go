package storedversion

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// inScopeTables are the ReplacingMergeTree tables whose keys the invariant
// covers.
var inScopeTables = map[string]bool{
	"git_pull_requests": true, "git_pull_request_reviews": true, "git_commits": true,
	"deployments": true, "work_items": true, "repos": true, "identities": true,
}

// contractedWriters write an in-scope table under a stored-version contract;
// each package's own tests pin its inserts to its enumerated contracts.
var contractedWriters = map[string]string{
	"internal/streamhandlers/external_clickhouse.go|git_commits":                      "external commit.v1",
	"internal/streamhandlers/external_clickhouse.go|git_pull_requests":                "external pull_request.v1",
	"internal/streamhandlers/external_clickhouse.go|git_pull_request_reviews":         "external review.v1",
	"internal/streamhandlers/external_clickhouse.go|identities":                       "external identity.v1",
	"internal/streamhandlers/external_clickhouse.go|repos":                            "external repository.v1",
	"internal/streamhandlers/external_clickhouse.go|work_items":                       "external work_item.v1",
	"internal/streamhandlers/internal_ingest.go|deployments":                          "internal deployments",
	"internal/streamhandlers/internal_ingest.go|git_commits":                          "internal commits",
	"internal/streamhandlers/internal_ingest.go|git_pull_requests":                    "internal pull-requests",
	"internal/streamhandlers/internal_ingest.go|git_pull_request_reviews":             "internal reviews",
	"internal/streamhandlers/internal_ingest.go|work_items":                           "internal work-items",
	"internal/providersync/stored_version.go|git_pull_requests":                       "provider sync pull requests",
	"internal/providersync/stored_version.go|git_pull_request_reviews":                "provider sync reviews",
	"internal/providersync/github_work_items_direct_effects_clickhouse.go|work_items": "provider sync work items (github, gitlab, jira rows)",
	"internal/providersync/linear_work_items_effects.go|work_items":                   "provider sync linear work items",
	"internal/providersync/stored_version.go|repos":                                   "provider sync repositories (github, gitlab)",
	"internal/providersync/stored_version.go|git_commits":                             "provider sync commits",
	"internal/providersync/stored_version.go|deployments":                             "provider sync deployments (github, gitlab)",
}

// outOfScopeWriters write operational_* tables: revisioned snapshots under
// their own ordering contract, outside the stored-version invariant.
var outOfScopeWriters = map[string]string{
	"internal/streamhandlers/external_clickhouse.go|operational_alerts":                                         "operational",
	"internal/streamhandlers/external_clickhouse.go|operational_escalation_policies":                            "operational",
	"internal/streamhandlers/external_clickhouse.go|operational_incident_notes":                                 "operational",
	"internal/streamhandlers/external_clickhouse.go|operational_incident_responders":                            "operational",
	"internal/streamhandlers/external_clickhouse.go|operational_incident_timeline_events":                       "operational",
	"internal/streamhandlers/external_clickhouse.go|operational_incidents":                                      "operational",
	"internal/streamhandlers/external_clickhouse.go|operational_on_call_assignments":                            "operational",
	"internal/streamhandlers/external_clickhouse.go|operational_on_call_schedules":                              "operational",
	"internal/streamhandlers/external_clickhouse.go|operational_service_repository_mappings":                    "operational",
	"internal/streamhandlers/external_clickhouse.go|operational_services":                                       "operational",
	"internal/streamhandlers/external_clickhouse.go|operational_teams":                                          "operational",
	"internal/streamhandlers/external_clickhouse.go|operational_users":                                          "operational",
	"internal/streamhandlers/internal_ingest.go|operational_incidents":                                          "operational",
	"internal/streamhandlers/internal_ingest.go|operational_service_repository_mappings":                        "operational",
	"internal/streamhandlers/internal_ingest.go|operational_services":                                           "operational",
	"internal/providersync/jira_incidents_effects_clickhouse.go|operational_incidents":                          "operational",
	"internal/providersync/gitlab_incidents_effects_clickhouse.go|operational_services":                         "operational",
	"internal/providersync/gitlab_incidents_effects_clickhouse.go|operational_service_repository_mappings":      "operational",
	"internal/providersync/pagerduty_schedules_effects_clickhouse.go|operational_on_call_schedules":             "operational",
	"internal/providersync/pagerduty_users_effects_clickhouse.go|operational_users":                             "operational",
	"internal/providersync/pagerduty_business_services_effects_clickhouse.go|operational_services":              "operational",
	"internal/providersync/pagerduty_services_effects_clickhouse.go|operational_services":                       "operational",
	"internal/providersync/pagerduty_services_effects_clickhouse.go|operational_service_repository_mappings":    "operational",
	"internal/providersync/pagerduty_teams_effects_clickhouse.go|operational_teams":                             "operational",
	"internal/providersync/pagerduty_oncalls_effects_clickhouse.go|operational_on_call_assignments":             "operational",
	"internal/providersync/pagerduty_incidents_effects_clickhouse.go|operational_incidents":                     "operational",
	"internal/providersync/pagerduty_incidents_effects_clickhouse.go|operational_alerts":                        "operational",
	"internal/providersync/pagerduty_incidents_effects_clickhouse.go|operational_incident_timeline_events":      "operational",
	"internal/providersync/pagerduty_incidents_effects_clickhouse.go|operational_incident_notes":                "operational",
	"internal/providersync/pagerduty_escalation_policies_effects_clickhouse.go|operational_escalation_policies": "operational",
	"internal/providersync/pagerduty_incident_responders.go|operational_incident_responders":                    "operational",
}

// unresolvedWriters build the table name at run time; each names why it
// cannot write an in-scope table.
var unresolvedWriters = map[string]string{
	"internal/providerfoundation/sinks.go":          "writes the normalized provider-entity schema (schema_version, dedupe_key, attributes_json), which no in-scope table has",
	"internal/storage/postgres/authschema/apply.go": "PostgreSQL schema-migration ledger",
}

// scanInserts finds every Go string literal in the module's production source
// that starts an INSERT statement, keyed "file|table" (file only when the
// table name is not in the literal).
func scanInserts(t *testing.T) map[string]bool {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test")
	}
	root := filepath.Join(filepath.Dir(filename), "..", "..")
	found := map[string]bool{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() {
			if name == "testdata" || name == "node_modules" || name == ".venv" || (strings.HasPrefix(name, ".") && path != root) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !strings.Contains(string(source), "INSERT INTO") {
			return nil
		}
		parsed, err := parser.ParseFile(fset, path, source, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		ast.Inspect(parsed, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(literal.Value)
			if err != nil {
				return true
			}
			text = strings.TrimSpace(text)
			if !strings.HasPrefix(text, "INSERT INTO") {
				return true
			}
			fields := strings.Fields(strings.TrimPrefix(text, "INSERT INTO"))
			table := ""
			if len(fields) > 0 {
				table = strings.TrimSuffix(strings.SplitN(fields[0], "(", 2)[0], " ")
			}
			if table == "" || strings.ContainsAny(table, "%$") {
				found[rel] = true
			} else {
				found[rel+"|"+table] = true
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// Every Go writer of an in-scope key is under a stored-version contract;
// every operational writer and every run-time table name is named with its
// reason. The lists equal the module scan exactly, so a new writer of an
// in-scope key without a contract, or a stale row, fails.
func TestEveryWriterOfAnInScopeKeyIsContractedOrListed(t *testing.T) {
	found := scanInserts(t)
	listed := map[string]string{}
	for _, list := range []map[string]string{contractedWriters, outOfScopeWriters, unresolvedWriters} {
		for key, reason := range list {
			if _, dup := listed[key]; dup {
				t.Errorf("%s is on more than one list", key)
			}
			listed[key] = reason
		}
	}
	var missing, stale []string
	for key := range found {
		if _, ok := listed[key]; ok {
			continue
		}
		table := key[strings.IndexByte(key+"|", '|')+1:]
		if !strings.Contains(key, "|") || inScopeTables[table] || strings.HasPrefix(table, "operational_") {
			missing = append(missing, key)
		}
	}
	for key := range listed {
		if !found[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	for _, key := range missing {
		t.Errorf("%s writes an in-scope or operational table and is on no list", key)
	}
	for _, key := range stale {
		t.Errorf("%s is listed but the module scan does not find it", key)
	}
	for key := range contractedWriters {
		if !inScopeTables[key[strings.IndexByte(key, '|')+1:]] {
			t.Errorf("contracted writer %s is not an in-scope table", key)
		}
	}
	t.Logf("module scan: %d INSERT sites; contracted %d, out of scope %d, unresolved %d",
		len(found), len(contractedWriters), len(outOfScopeWriters), len(unresolvedWriters))
}
