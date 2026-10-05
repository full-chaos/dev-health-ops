package providersync

import "strings"

// parseTrustedSCMHostEntries reads a trusted-SCM-host env list. An entry is "host" or "host/url-root"; the second form
// names the relative URL root of a self-managed instance (GitLab's relative_url_root, e.g. "gitlab.example.com/gitlab"),
// which is part of every web URL but never part of a project path. The host is case-folded; the root keeps its case and
// is compared exactly, as URL paths are case-sensitive.
func parseTrustedSCMHostEntries(raw string, defaults ...string) (map[string]struct{}, map[string][]string) {
	hosts := make(map[string]struct{}, len(defaults))
	roots := make(map[string][]string)
	for _, host := range defaults {
		hosts[host] = struct{}{}
	}
	for _, value := range strings.Split(raw, ",") {
		host, root, _ := strings.Cut(strings.TrimSpace(value), "/")
		if host = strings.ToLower(host); host == "" {
			continue
		}
		hosts[host] = struct{}{}
		if root = strings.Trim(root, "/"); root != "" {
			roots[host] = strings.Split(root, "/")
		}
	}
	return hosts, roots
}

// stripSCMURLRoot removes the configured URL root of host from the front of the path segments. ok is false when a root
// is configured and the path does not start with it: the URL is not under that instance, so it must not become an id.
func stripSCMURLRoot(roots map[string][]string, host string, parts []string) ([]string, bool) {
	root := roots[strings.ToLower(host)]
	if len(root) == 0 {
		return parts, true
	}
	if len(parts) <= len(root) {
		return nil, false
	}
	for index, segment := range root {
		if parts[index] != segment {
			return nil, false
		}
	}
	return parts[len(root):], true
}
