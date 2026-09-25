package synccli

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// This file is the small slice of Python's argparse that `dev-hops sync
// <target>` users rely on, ported so `dho sync <target>` accepts and refuses
// exactly the same command lines: long options with an `=` or a following
// value, unique-prefix abbreviations (`--back 3`), attached short values
// (`-sorg/*`), combined short flags, and the argparse type conversions
// (`int`, `float`, `date.fromisoformat`). Go's flag package differs on every
// one of those, and a differing parser is how one command line means two
// things in the two binaries.
//
// The port is proven against the real argparse by the live oracle in
// live_python_oracle_test.go; anything it cannot express is listed in
// Named limitations there.

type optKind int

const (
	optValue optKind = iota // takes exactly one argument
	optFlag                 // store_true
	optHelp                 // -h/--help
)

type optSpec struct {
	strs []string // option strings, e.g. "-s", "--search"
	dest string
	kind optKind
}

// argError is an argparse usage error: Python exits 2.
type argError struct{ msg string }

func (e *argError) Error() string { return e.msg }

// parsedArgs is the outcome of parsing: last value per dest, plus whether the
// dest appeared at all.
type parsedArgs struct {
	values map[string]string
	flags  map[string]bool
	help   bool
	// unrecognized holds arguments argparse would report ("unrecognized
	// arguments: ..."), which parse_args turns into exit 2.
	unrecognized []string
}

var negativeNumber = regexp.MustCompile(`^-\d+$|^-\d*\.\d+$`)

type parser struct {
	specs   []optSpec
	byStr   map[string]*optSpec
	hasNegN bool
}

func newParser(specs []optSpec) *parser {
	p := &parser{specs: specs, byStr: map[string]*optSpec{}}
	for i := range p.specs {
		for _, s := range p.specs[i].strs {
			p.byStr[s] = &p.specs[i]
			if negativeNumber.MatchString(s) {
				p.hasNegN = true
			}
		}
	}
	return p
}

// optionMatch mirrors argparse's (action, option_string, explicit_arg).
type optionMatch struct {
	spec        *optSpec
	optionStr   string
	sep         string // "=" when the explicit argument followed an equals sign
	explicitArg *string
}

// classify mirrors ArgumentParser._parse_optional: nil means "positional"
// (an 'A' in argparse's pattern); a match with a nil spec is an unknown
// optional (an 'O' that later reports as unrecognized).
func (p *parser) classify(arg string) (*optionMatch, *argError) {
	if arg == "" || !strings.HasPrefix(arg, "-") {
		return nil, nil
	}
	if spec, ok := p.byStr[arg]; ok {
		return &optionMatch{spec: spec, optionStr: arg}, nil
	}
	if len(arg) == 1 {
		return nil, nil
	}
	if i := strings.Index(arg, "="); i >= 0 {
		optionStr, explicit := arg[:i], arg[i+1:]
		if spec, ok := p.byStr[optionStr]; ok {
			return &optionMatch{spec: spec, optionStr: optionStr, sep: "=", explicitArg: &explicit}, nil
		}
	}
	matches := p.prefixMatches(arg)
	if len(matches) > 1 {
		names := make([]string, len(matches))
		for i, m := range matches {
			names[i] = m.optionStr
		}
		return nil, &argError{fmt.Sprintf("ambiguous option: %s could match %s", arg, strings.Join(names, ", "))}
	}
	if len(matches) == 1 {
		return &matches[0], nil
	}
	if negativeNumber.MatchString(arg) && !p.hasNegN {
		return nil, nil
	}
	if strings.Contains(arg, " ") {
		return nil, nil
	}
	return &optionMatch{optionStr: arg}, nil
}

// prefixMatches mirrors _get_option_tuples (allow_abbrev=True).
func (p *parser) prefixMatches(arg string) []optionMatch {
	var out []optionMatch
	if strings.HasPrefix(arg, "--") {
		prefix, explicit, sep := arg, (*string)(nil), ""
		if i := strings.Index(arg, "="); i >= 0 {
			e := arg[i+1:]
			prefix, explicit, sep = arg[:i], &e, "="
		}
		for _, spec := range p.specs {
			for _, s := range spec.strs {
				if strings.HasPrefix(s, prefix) {
					spec := spec
					out = append(out, optionMatch{spec: &spec, optionStr: s, sep: sep, explicitArg: explicit})
				}
			}
		}
		return out
	}
	if strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, "--") {
		shortPrefix := arg[:2]
		shortExplicit := arg[2:]
		for _, spec := range p.specs {
			for _, s := range spec.strs {
				if s == shortPrefix {
					spec := spec
					e := shortExplicit
					out = append(out, optionMatch{spec: &spec, optionStr: s, explicitArg: &e})
				} else if strings.HasPrefix(s, arg) {
					spec := spec
					out = append(out, optionMatch{spec: &spec, optionStr: s})
				}
			}
		}
	}
	return out
}

// parse mirrors ArgumentParser.parse_known_args for a parser with only
// optionals. Actions run in encounter order, as argparse's do; the type
// conversion and mutual-exclusion checks belong to the caller's `convert`.
func (p *parser) parse(args []string, act func(spec *optSpec, value string) *argError) (*parsedArgs, *argError) {
	result := &parsedArgs{values: map[string]string{}, flags: map[string]bool{}}
	var classes []*optionMatch
	sawDoubleDash := false
	isOption := make([]bool, len(args))
	for i, arg := range args {
		if sawDoubleDash {
			continue
		}
		if arg == "--" {
			sawDoubleDash = true
			classes = append(classes, nil)
			continue
		}
		m, err := p.classify(arg)
		if err != nil {
			return nil, err
		}
		classes = append(classes, m)
		isOption[i] = m != nil
	}
	if len(classes) < len(args) {
		classes = append(classes, make([]*optionMatch, len(args)-len(classes))...)
	}

	i := 0
	for i < len(args) {
		m := classes[i]
		if args[i] == "--" || m == nil {
			// A positional (or `--` / anything after it): this parser has no
			// positionals, so argparse leaves them in the unrecognized list.
			result.unrecognized = append(result.unrecognized, args[i])
			i++
			continue
		}
		if m.spec == nil {
			result.unrecognized = append(result.unrecognized, args[i])
			i++
			continue
		}
		next, err := p.consumeOptional(args, i, m, isOption, result, act)
		if err != nil {
			return nil, err
		}
		i = next
		if result.help {
			// argparse's help action exits the moment it runs: nothing after
			// it is parsed, so a later bad argument cannot turn help into a
			// usage error.
			return result, nil
		}
	}
	return result, nil
}

// consumeOptional mirrors ArgumentParser.consume_optional, including the
// single-dash combined-flags path (`-hs`).
func (p *parser) consumeOptional(
	args []string, start int, m *optionMatch, isOption []bool, result *parsedArgs,
	act func(spec *optSpec, value string) *argError,
) (int, *argError) {
	type pending struct {
		spec  *optSpec
		value string
	}
	var actions []pending
	spec, optionStr, explicit, sep := m.spec, m.optionStr, m.explicitArg, m.sep
	next := start + 1
	for {
		if explicit == nil {
			if spec.kind == optValue {
				if next >= len(args) || isOption[next] || args[next] == "--" {
					return 0, &argError{fmt.Sprintf("argument %s: expected one argument", optionStr)}
				}
				actions = append(actions, pending{spec, args[next]})
				next++
			} else {
				actions = append(actions, pending{spec, ""})
			}
			break
		}
		if spec.kind == optValue {
			actions = append(actions, pending{spec, *explicit})
			break
		}
		// A zero-argument option with attached text.
		if len(optionStr) >= 2 && optionStr[1] != '-' && *explicit != "" {
			// A short flag followed by more characters: those may be further
			// short options (-hx), exactly as argparse chains them. An equals
			// sign or a leading dash inside the tail is an error.
			if sep != "" || strings.HasPrefix(*explicit, "-") {
				return 0, &argError{fmt.Sprintf("argument %s: ignored explicit argument %q", optionStr, *explicit)}
			}
			actions = append(actions, pending{spec, ""})
			runes := []rune(*explicit)
			nextStr := "-" + string(runes[0])
			nextSpec, ok := p.byStr[nextStr]
			if !ok {
				// Not an option: the rest is an unrecognized argument, but the
				// options already collected still run (a -h in front of it
				// still prints help).
				result.unrecognized = append(result.unrecognized, "-"+*explicit)
				break
			}
			spec, optionStr = nextSpec, nextStr
			rest := string(runes[1:])
			switch {
			case rest == "":
				explicit, sep = nil, ""
			case strings.HasPrefix(rest, "="):
				rest = rest[1:]
				explicit, sep = &rest, "="
			default:
				explicit, sep = &rest, ""
			}
			continue
		}
		return 0, &argError{fmt.Sprintf("argument %s: ignored explicit argument %q", optionStr, *explicit)}
	}
	for _, a := range actions {
		if err := act(a.spec, a.value); err != nil {
			return 0, err
		}
		switch a.spec.kind {
		case optValue:
			result.values[a.spec.dest] = a.value
		case optFlag:
			result.flags[a.spec.dest] = true
			result.values[a.spec.dest] = "true"
		case optHelp:
			result.help = true
		}
	}
	return next, nil
}

// pyInt is int(text): surrounding whitespace, an optional sign, ASCII digits
// with single underscores between digits. It reports ok=false for anything
// Python's int() refuses; values outside int64 are refused too (Python keeps
// arbitrary precision -- named limitation).
func pyInt(text string) (int64, bool) {
	s := asciiDigits(strings.TrimFunc(text, isPySpace))
	negative := false
	if s != "" && (s[0] == '+' || s[0] == '-') {
		negative = s[0] == '-'
		s = s[1:]
	}
	if s == "" || strings.HasPrefix(s, "_") || strings.HasSuffix(s, "_") || strings.Contains(s, "__") {
		return 0, false
	}
	digits := strings.ReplaceAll(s, "_", "")
	for _, r := range digits {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	if negative {
		digits = "-" + digits
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// asciiDigits maps every Unicode decimal digit (category Nd) to its ASCII
// digit, as Python's int() and float() read them.
func asciiDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r > unicode.MaxASCII && unicode.Is(unicode.Nd, r) {
			start := r
			for unicode.Is(unicode.Nd, start-1) {
				start--
			}
			b.WriteRune('0' + (r-start)%10)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

var pyFloatPattern = regexp.MustCompile(`^[+-]?(?:(?:\d+(?:_\d+)*)(?:\.(?:\d+(?:_\d+)*)?)?|\.\d+(?:_\d+)*)(?:[eE][+-]?\d+(?:_\d+)*)?$`)

// pyFloat is float(text): decimal and exponent forms with optional
// underscores between digits, and the inf/infinity/nan spellings.
func pyFloat(text string) (float64, bool) {
	s := asciiDigits(strings.TrimFunc(text, isPySpace))
	switch strings.ToLower(strings.TrimLeft(s, "+-")) {
	case "inf", "infinity":
		if strings.HasPrefix(s, "-") {
			return math.Inf(-1), true
		}
		return math.Inf(1), true
	case "nan":
		return math.NaN(), true
	}
	if !pyFloatPattern.MatchString(s) {
		return 0, false
	}
	f, err := strconv.ParseFloat(strings.ReplaceAll(s, "_", ""), 64)
	if err != nil && !math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

var (
	isoCalendar     = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})$`)
	isoBasic        = regexp.MustCompile(`^(\d{4})(\d{2})(\d{2})$`)
	isoWeekExtended = regexp.MustCompile(`^(\d{4})-W(\d{2})(?:-(\d))?$`)
	isoWeekBasic    = regexp.MustCompile(`^(\d{4})W(\d{2})(\d)?$`)
)

// pyDate is date.fromisoformat (Python 3.11+): YYYY-MM-DD, YYYYMMDD,
// YYYY-Www[-D] and YYYYWww[D].
func pyDate(text string) (time.Time, bool) {
	atoi := func(s string) int { n, _ := strconv.Atoi(s); return n }
	calendar := func(y, m, d int) (time.Time, bool) {
		if y < 1 {
			return time.Time{}, false
		}
		t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
		if t.Year() != y || int(t.Month()) != m || t.Day() != d {
			return time.Time{}, false
		}
		return t, true
	}
	if m := isoCalendar.FindStringSubmatch(text); m != nil {
		return calendar(atoi(m[1]), atoi(m[2]), atoi(m[3]))
	}
	if m := isoBasic.FindStringSubmatch(text); m != nil {
		return calendar(atoi(m[1]), atoi(m[2]), atoi(m[3]))
	}
	week := isoWeekExtended.FindStringSubmatch(text)
	if week == nil {
		week = isoWeekBasic.FindStringSubmatch(text)
	}
	if week == nil {
		return time.Time{}, false
	}
	year, wk, day := atoi(week[1]), atoi(week[2]), 1
	if week[3] != "" {
		day = atoi(week[3])
	}
	if year < 1 || wk < 1 || day < 1 || day > 7 {
		return time.Time{}, false
	}
	// Week 1 is the week containing Jan 4th; its Monday anchors the count.
	jan4 := time.Date(year, 1, 4, 0, 0, 0, 0, time.UTC)
	weekday := int(jan4.Weekday())
	if weekday == 0 {
		weekday = 7
	}
	monday := jan4.AddDate(0, 0, -(weekday - 1))
	result := monday.AddDate(0, 0, (wk-1)*7+(day-1))
	// A week number past the year's last ISO week is refused.
	if wk > 52 {
		isoYear, isoWeek := result.ISOWeek()
		if isoYear != year || isoWeek != wk {
			return time.Time{}, false
		}
	}
	return result, true
}
