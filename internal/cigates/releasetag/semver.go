package releasetag

import (
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyunicodedata"
)

// semver is the tuple (major, minor, patch, pre_type, pre_num) of parse_semver; preType "" is Python's None.
type semver struct {
	major, minor, patch *big.Int
	preType             string
	preNum              *big.Int
}

// isNd is the \d of a Python str pattern: a decimal digit of the Unicode of CPython 3.14.
func isNd(r rune) bool { return pyunicodedata.Category(r) == "Nd" }

func isASCIILetter(r rune) bool { return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') }

// takeRun cuts the longest prefix of runes that satisfy keep.
func takeRun(rs []rune, keep func(rune) bool) (run, rest []rune) {
	i := 0
	for i < len(rs) && keep(rs[i]) {
		i++
	}
	return rs[:i], rs[i:]
}

func startsWith(rs []rune, r rune) bool { return len(rs) > 0 && rs[0] == r }

// parseSemver is re.match(r"^(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z]+)[\.-]?(\d+))?$", s) and the int() of each digit group. ok is
// false when the pattern does not match; err is the ValueError of an integer of more than 4300 digits.
func parseSemver(s string) (v semver, ok bool, err error) {
	rest := []rune(s)
	var groups [3][]rune
	for i := range groups {
		if i > 0 {
			if !startsWith(rest, '.') {
				return semver{}, false, nil
			}
			rest = rest[1:]
		}
		groups[i], rest = takeRun(rest, isNd)
		if len(groups[i]) == 0 {
			return semver{}, false, nil
		}
	}
	var preType, preNum []rune
	if startsWith(rest, '-') {
		preType, rest = takeRun(rest[1:], isASCIILetter)
		if len(preType) == 0 {
			return semver{}, false, nil
		}
		if len(rest) > 0 && (rest[0] == '.' || rest[0] == '-') {
			rest = rest[1:]
		}
		preNum, rest = takeRun(rest, isNd)
		if len(preNum) == 0 {
			return semver{}, false, nil
		}
	}
	if len(rest) == 1 && rest[0] == '\n' {
		rest = nil
	}
	if len(rest) != 0 {
		return semver{}, false, nil
	}
	v.preType = string(preType)
	ints := []**big.Int{&v.major, &v.minor, &v.patch}
	for i, group := range groups {
		n, perr := parseInt(group)
		if perr != nil {
			return semver{}, false, perr
		}
		*ints[i] = n
	}
	if len(preNum) > 0 {
		n, perr := parseInt(preNum)
		if perr != nil {
			return semver{}, false, perr
		}
		v.preNum = n
	}
	return v, true, nil
}

// parseInt is int(group) of a run of Nd characters; more than 4300 digits is the ValueError of CPython 3.11+.
func parseInt(group []rune) (*big.Int, error) {
	n, err := pythonparity.ParseInt(string(group))
	if errors.Is(err, pythonparity.ErrIntStringTooLong) {
		return nil, fmt.Errorf("Exceeds the limit (%d digits) for integer string conversion: value has %d digits; use sys.set_int_max_str_digits() to increase the limit", pythonparity.MaxIntStringDigits, len(group))
	}
	return n, err
}

// compareVersions is compare_versions of the Python script.
func compareVersions(a, b semver) int {
	for _, pair := range [][2]*big.Int{{a.major, b.major}, {a.minor, b.minor}, {a.patch, b.patch}} {
		if c := pair[0].Cmp(pair[1]); c != 0 {
			return c
		}
	}
	switch {
	case a.preType == "" && b.preType != "":
		return 1
	case a.preType != "" && b.preType == "":
		return -1
	case a.preType == "":
		return 0
	}
	if a.preType != b.preType {
		return strings.Compare(a.preType, b.preType)
	}
	return a.preNum.Cmp(b.preNum)
}

var errStrTooLong = fmt.Errorf("Exceeds the limit (%d digits) for integer string conversion; use sys.set_int_max_str_digits() to increase the limit", pythonparity.MaxIntStringDigits)

// itoa is str(int): Python refuses to write an integer of more than 4300 digits.
func itoa(n *big.Int) (string, error) {
	s := n.String()
	if len(strings.TrimPrefix(s, "-")) > pythonparity.MaxIntStringDigits {
		return "", errStrTooLong
	}
	return s, nil
}
