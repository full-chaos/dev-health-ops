package providersync

import "strings"

// splitGitLabWorkItemID splits "gitlab:<path><#|!><n>" into its path and the
// rest ("#n" or "!n"); ok is false for any other id.
func splitGitLabWorkItemID(id string) (path, rest string, ok bool) {
	body, found := strings.CutPrefix(id, "gitlab:")
	if !found {
		return "", "", false
	}
	index := strings.LastIndexAny(body, "#!")
	if index < 1 || index == len(body)-1 {
		return "", "", false
	}
	for _, char := range body[index+1:] {
		if char < '0' || char > '9' {
			return "", "", false
		}
	}
	return body[:index], body[index:], true
}

// gitLabDependencyPaths are the project paths the GitLab work-item ids of rows
// name, other than their owners' (see canonicalGitLabDependencies).
func gitLabDependencyPaths(rows []gitlabWorkItemDependencyRow, owners []string) []string {
	seen := map[string]bool{}
	paths := make([]string, 0)
	for index, row := range rows {
		for _, id := range [2]string{row.SourceWorkItemID, row.TargetWorkItemID} {
			if index < len(owners) && id == owners[index] {
				continue
			}
			if path, _, ok := splitGitLabWorkItemID(id); ok && !seen[strings.ToLower(path)] {
				seen[strings.ToLower(path)] = true
				paths = append(paths, path)
			}
		}
	}
	return paths
}

func appendGitLabDependencyOwner(owners []string, owner string, rows int) []string {
	for range rows {
		owners = append(owners, owner)
	}
	return owners
}

// canonicalGitLabDependencies rewrites the GitLab work-item ids of rows from
// the current project path a payload names (references.full, a group/proj#n
// in text) to the identity path the project's own work items carry
// (CHAOS-3383). owners[i] is the synced item row i was read from: that side
// already carries the identity path and is never rewritten (another project
// may hold that path now). A row that then links an item to itself is
// dropped, and a row the rewrite made equal to an earlier one on the table key
// keeps the earlier.
func canonicalGitLabDependencies(rows []gitlabWorkItemDependencyRow, owners []string, identityPaths map[string]string) []gitlabWorkItemDependencyRow {
	if len(owners) != len(rows) {
		panic("canonicalGitLabDependencies: one owner per row")
	}
	canonical := func(id, owner string) string {
		path, rest, ok := splitGitLabWorkItemID(id)
		if !ok || id == owner {
			return id
		}
		if identity, held := identityPaths[strings.ToLower(path)]; held && identity != "" {
			return "gitlab:" + identity + rest
		}
		return id
	}
	type key struct{ source, target, relationship string }
	seen := make(map[key]bool, len(rows))
	out := make([]gitlabWorkItemDependencyRow, 0, len(rows))
	for index, row := range rows {
		row.SourceWorkItemID = canonical(row.SourceWorkItemID, owners[index])
		row.TargetWorkItemID = canonical(row.TargetWorkItemID, owners[index])
		if row.SourceWorkItemID == row.TargetWorkItemID {
			continue
		}
		k := key{row.SourceWorkItemID, row.TargetWorkItemID, row.RelationshipType}
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, row)
	}
	return out
}
