package providersync

import (
	"sort"
	"strings"
)

// parseTrustedSCMHostEntries reads a trusted-SCM-host env list. An entry is "host" or "host/url-root"; the second form
// names the relative URL root of a self-managed instance (GitLab's relative_url_root, e.g. "gitlab.example.com/gitlab"),
// which is part of every web URL but never part of a project path. The host is case-folded; the root keeps its case and
// is compared exactly, as URL paths are case-sensitive.
func parseTrustedSCMHostEntries(raw string, defaults ...string) (map[string]struct{}, map[string][][]string) {
	hosts := make(map[string]struct{}, len(defaults))
	roots := make(map[string][][]string)
	for _, host := range defaults {
		hosts[host] = struct{}{}
	}
	for _, value := range strings.Split(raw, ",") {
		host, root, _ := strings.Cut(strings.TrimSpace(value), "/")
		if host = strings.ToLower(host); host == "" {
			continue
		}
		hosts[host] = struct{}{}
		var segments []string
		if root = strings.Trim(root, "/"); root != "" {
			segments = strings.Split(root, "/")
		}
		roots[host] = appendSCMRoot(roots[host], segments)
	}
	for host, list := range roots {
		if len(list) == 1 && len(list[0]) == 0 {
			delete(roots, host)
			continue
		}
		sort.SliceStable(list, func(i, j int) bool { return len(list[i]) > len(list[j]) })
	}
	return hosts, roots
}

// appendSCMRoot adds root to the host's roots once; a bare "host" entry beside "host/root" entries stays as the empty
// root, tried last.
func appendSCMRoot(list [][]string, root []string) [][]string {
	for _, existing := range list {
		if strings.Join(existing, "/") == strings.Join(root, "/") {
			return list
		}
	}
	return append(list, root)
}

// stripSCMURLRoot removes the configured URL root of host from the front of the path segments, trying the host's roots
// longest first. ok is false when roots are configured and the path starts with none of them: the URL is not under that
// instance, and the caller counts it as a reject.
func stripSCMURLRoot(roots map[string][][]string, host string, parts []string) ([]string, bool) {
	list := roots[strings.ToLower(host)]
	if len(list) == 0 {
		return parts, true
	}
	for _, root := range list {
		if len(parts) <= len(root) {
			continue
		}
		matched := true
		for index, segment := range root {
			if parts[index] != segment {
				matched = false
				break
			}
		}
		if matched {
			return parts[len(root):], true
		}
	}
	return nil, false
}

// scmURLNamesChangeRequest reports a path that ends as a pull or merge request URL: such a URL under no trusted root is
// a counted reject, not a silent drop.
func scmURLNamesChangeRequest(parts []string) bool {
	return len(parts) >= 3 && (parts[len(parts)-2] == "merge_requests" || parts[len(parts)-2] == "pull")
}
