package postgres

import (
	"regexp"
	"strings"
	"testing"
)

// CHAOS-12828 gives devhealth_api and the worker domain login EXECUTE on one
// function in the auth schema. CheckRolePosture must stay blind to the auth
// schema: if it ever read an auth object, that grant would show as an excess
// privilege to an older or newer image and take the app out of Ready (the
// public.saml_assertion_replays incident, CHAOS-12729). This pins the query
// text and both manifests.
func TestRolePostureQueryReadsNoAuthSchemaObject(t *testing.T) {
	schemaFilter := regexp.MustCompile(`nspname\s*=\s*('[^']*'|\$\d+)`)
	for _, match := range schemaFilter.FindAllStringSubmatch(rolePostureQuery, -1) {
		switch match[1] {
		case "'public'", "$2": // $2 is the River schema argument
		default:
			t.Errorf("rolePostureQuery filters on a schema %s; only public and the River schema are read", match[1])
		}
	}
	for _, needle := range []string{"'auth'", "auth.", "append_external_push_lifecycle_event", "regnamespace"} {
		if strings.Contains(rolePostureQuery, needle) {
			t.Errorf("rolePostureQuery mentions %q: the role posture check must not read the auth schema", needle)
		}
	}
}

func TestRolePostureManifestsDeclareNoAuthObject(t *testing.T) {
	for name, posture := range map[string]RolePosture{"api": APIPosture(false), "api sessions owned by auth": APIPosture(true), "domain": DomainPosture()} {
		for _, function := range posture.RequiredFunctions {
			if strings.Contains(function, "append_external_push_lifecycle_event") || strings.HasPrefix(function, "auth.") {
				t.Errorf("%s posture declares the auth function %q; the auth grant is the auth-migrate grant pass's, not the role posture's", name, function)
			}
		}
		for _, table := range posture.RequiredTables {
			if strings.HasPrefix(table.TableName, "auth.") {
				t.Errorf("%s posture declares the auth table %q", name, table.TableName)
			}
		}
	}
}
