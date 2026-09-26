package postgres

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/roleacl"
)

// CHAOS-6862 / CHAOS-6804 D2616: the login identity has ONE definition
// (roleacl.IdentityPredicateSQL) and EVERY posture check site must use it. This
// test enumerates the sites two ways so a NEW check cannot appear without it:
//
//  1. the registered posture queries (every query the readiness checks run) must
//     each contain the predicate text verbatim;
//  2. every non-test Go file under internal/ that probes the calling login's
//     privileges in SQL (`has_*_privilege(current_user`, `pg_has_role(current_user`,
//     `current_user = $1`) must be a known site that references the shared predicate, so a check written elsewhere fails
//     here instead of silently reading only current_user.
func TestEveryPostureCheckSiteUsesTheSharedIdentityPredicate(t *testing.T) {
	t.Parallel()
	queries := map[string]string{
		"rolePostureQuery (domain, coordinator, api, generic, every cached wrapper)": rolePostureQuery,
		"queueAuthorizationQuery (queue)":                                            queueAuthorizationQuery,
	}
	for name, query := range queries {
		if !strings.Contains(query, roleacl.IdentityPredicateSQL) {
			t.Errorf("%s does not contain roleacl.IdentityPredicateSQL: the pool's identity is not proven there", name)
		}
	}

	// Sites that bind a login to a role in Go source. Each must reference the shared
	// predicate (directly, or by calling checkRoleIdentity which does).
	known := map[string]string{
		"storage/postgres/domain_authorization.go": "IdentityPredicateSQL",
		"storage/postgres/queue_authorization.go":  "IdentityPredicateSQL",
		"storage/postgres/role_identity.go":        "IdentityPredicateSQL",
		"storage/roleacl/roleacl.go":               "IdentityPredicateSQL", // the definition
	}
	// Not a login assertion: reads a NAMED role's grants for the migrate-time executed
	// gate and for refusal diagnostics (it takes the role as a parameter and never
	// claims the connected login is that role). Its own doc comments mention
	// current_user only to say so.
	exempt := map[string]string{
		"storage/postgres/posture_diagnostics.go": "DiagnoseRolePosture reads the named role's grants; it asserts no login",
	}
	root := filepath.Join("..", "..") // internal/
	var found []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(raw)
		// A posture check probes the CALLING login's privileges: any of these
		// current_user-bound probes marks a check site.
		probes := strings.Contains(text, "has_table_privilege(current_user") || strings.Contains(text, "pg_has_role(current_user") ||
			strings.Contains(text, "has_schema_privilege(current_user") || strings.Contains(text, "has_database_privilege(current_user") ||
			strings.Contains(text, "has_sequence_privilege(current_user") || strings.Contains(text, "has_function_privilege(current_user") ||
			strings.Contains(text, "current_user = $1")
		if probes {
			rel, _ := filepath.Rel(root, path)
			found = append(found, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(found)
	for _, rel := range found {
		if _, skip := exempt[rel]; skip {
			continue
		}
		marker, ok := known[rel]
		if !ok {
			t.Errorf("%s binds a login to a role in SQL but is not a registered posture check site: it must use roleacl.IdentityPredicateSQL and be added to this test", rel)
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), marker) {
			t.Errorf("%s is a posture check site but does not reference %s", rel, marker)
		}
	}
	// The sites above must actually have been found (a walk that finds nothing would
	// pass vacuously).
	for rel := range known {
		if rel == "storage/postgres/role_identity.go" {
			continue // uses the predicate but does not itself bind current_user in SQL text
		}
		present := false
		for _, f := range found {
			if f == rel {
				present = true
			}
		}
		if !present {
			t.Errorf("the site walk did not find %s: the enumeration is not looking where the checks are", rel)
		}
	}
}
