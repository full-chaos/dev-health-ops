package pythonparity

import (
	"path"
	"strings"
	"time"
	"unicode/utf8"
)

// ZoneInfoKeyValid is whether zoneinfo.ZoneInfo(key) builds a zone for a
// non-empty str key: _tzpath._validate_tzfile_path's three refusals, then
// the key must name a TZif file in the zone database.
//
//   - an absolute key ("/UTC") is refused;
//   - a key that normpath shortens ("a/./b", "a/b/", "a//b") is refused;
//   - a key that resolves outside the database ("..", "../UTC", ".") is
//     refused;
//   - otherwise the key must load as a zone. Python looks the file up on its
//     TZPATH, then in the tzdata package; Go's time.LoadLocation looks in the
//     system zoneinfo directories, then in the embedded database. A file
//     that is not TZif ("zone.tab", "tzdata.zi") is refused by both.
//
// "Local" is refused: Python has no such file, while time.LoadLocation
// returns the process-local zone for it.
//
// Named limit: the two planes read the zone database their own process
// finds, so a key that exists in one tzdata release and not the other (a
// newly added or removed zone) is decided by each plane's own database.
func ZoneInfoKeyValid(key string) bool {
	if key == "" || strings.HasPrefix(key, "/") {
		return false
	}
	normalized := path.Clean(key)
	if utf8.RuneCountInString(normalized) != utf8.RuneCountInString(key) {
		return false
	}
	// os.path.normpath(os.path.join("_/", key)) must stay under "_/". Kept
	// as Python's own rule, though no key reaches the zone load that it
	// alone refuses: every key it refuses ("..", "../UTC") keeps its length
	// under normpath and holds a ".." element, which time.LoadLocation
	// refuses too (an equivalent guard in a kill run).
	if !strings.HasPrefix(path.Clean("_/"+normalized), "_/") {
		return false
	}
	if key == "Local" || strings.ContainsRune(key, 0) {
		return false
	}
	_, err := time.LoadLocation(key)
	return err == nil
}
