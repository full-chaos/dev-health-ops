package session

// standardPermissions is models/users.py STANDARD_PERMISSIONS' names, in
// declaration order.
var standardPermissions = []string{
	"analytics:read", "analytics:export", "metrics:read", "metrics:compute", "work_items:read", "work_items:sync",
	"git:read", "git:sync", "teams:read", "teams:write", "settings:read", "settings:write", "integrations:read",
	"integrations:write", "members:read", "members:invite", "members:manage", "org:read", "org:write", "org:delete",
	"admin:users", "admin:orgs",
}

// roleHierarchy is services/permissions.py ROLE_HIERARCHY with each role's
// own ROLE_PERMISSIONS entry; a role holds its own and every lower role's.
var roleHierarchy = []struct {
	role        string
	permissions []string
}{
	{"viewer", []string{"analytics:read", "metrics:read", "work_items:read", "git:read", "teams:read", "settings:read", "members:read", "org:read"}},
	{"member", []string{"analytics:export"}},
	{"admin", []string{"metrics:compute", "work_items:sync", "git:sync", "teams:write", "settings:write", "integrations:read",
		"integrations:write", "members:invite", "members:manage", "org:write"}},
	{"owner", []string{"org:delete"}},
}

// rolePermissions is _get_role_permissions(role): empty for an unknown
// role. The result is in standardPermissions order.
func rolePermissions(role string) []string {
	held := map[string]bool{}
	known := false
	for _, entry := range roleHierarchy {
		for _, permission := range entry.permissions {
			held[permission] = true
		}
		if entry.role == role {
			known = true
			break
		}
	}
	if !known {
		return []string{}
	}
	return inOrder(held)
}

func inOrder(held map[string]bool) []string {
	out := []string{}
	for _, permission := range standardPermissions {
		if held[permission] {
			out = append(out, permission)
		}
	}
	return out
}

// userPermissions is get_user_permissions: an impersonation session's
// target role, else every permission for a superuser, else the role's.
// Python returns list(set), whose order depends on the process's string
// hash seed; this returns standardPermissions order.
func userPermissions(targetRole *string, isSuperuser bool, role string) []string {
	if targetRole != nil {
		return rolePermissions(*targetRole)
	}
	if isSuperuser {
		return append([]string{}, standardPermissions...)
	}
	return rolePermissions(role)
}
