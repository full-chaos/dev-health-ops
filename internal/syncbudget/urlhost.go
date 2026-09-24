package syncbudget

import "github.com/full-chaos/dev-health-ops/internal/pythonparity"

// urlHostname is urllib.parse.urlparse(value).hostname on CPython 3.14 (the
// interpreter uv.lock pins): ok is false where Python returns None, and err
// is set where urlparse raises ValueError. The parse is
// pythonparity.SplitURL, the one shared port.
func urlHostname(value string) (host string, ok bool, err error) {
	split, err := pythonparity.SplitURL(value)
	if err != nil {
		return "", false, err
	}
	host, ok = split.Hostname()
	return host, ok, nil
}
