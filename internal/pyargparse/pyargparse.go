// Package pyargparse is the small slice of Python's argparse that the dho
// command lines share with `dev-hops`, ported once (R299) so every command line
// means the same thing in both binaries: long options with an `=` or a
// following value, unique-prefix abbreviations (`--back 3`), attached short
// values (`-sorg/*`), combined short flags, and argparse's own classification
// of every argument (an ambiguous abbreviation is an error where it is taken).
//
// It is proven against the real argparse by the live oracles of its users
// (internal/synccli, internal/cli); anything it cannot express is listed there.
package pyargparse

import (
	"fmt"
	"regexp"
	"strings"
)

// Kind is what an option takes.
type Kind int

const (
	OptValue Kind = iota // takes exactly one argument
	OptFlag              // store_true
	OptHelp              // -h/--help
)

// Spec is one option: its option strings (e.g. "-s", "--search"), the name its
// value is stored under, and what it takes.
type Spec struct {
	Strs []string // option strings, e.g. "-s", "--search"
	Dest string
	Kind Kind
}

// Error is an argparse usage error: Python exits 2.
type Error struct{ Msg string }

func (e *Error) Error() string { return e.Msg }

// Parsed is the outcome of parsing: last value per dest, plus whether the
// dest appeared at all.
type Parsed struct {
	Values map[string]string
	Flags  map[string]bool
	Help   bool
	// Unrecognized holds arguments argparse would report ("unrecognized
	// arguments: ..."), which parse_args turns into exit 2.
	Unrecognized []string
	// Rest is, for ParseRoot, the arguments from the first positional on (the
	// subcommand and everything after it).
	Rest []string
}

// negativeNumber is argparse 3.14's _negative_number_matcher: digits with
// PEP 515 underscores, an optional fraction and an optional exponent. (Python's
// \d is any Unicode decimal digit.)
var negativeNumber = regexp.MustCompile(`^-(?:\p{Nd}+(?:_\p{Nd}+)*(?:\.\p{Nd}+(?:_\p{Nd}+)*)?|\.\p{Nd}+(?:_\p{Nd}+)*)(?:[eE][+-]?\p{Nd}+(?:_\p{Nd}+)*)?$`)

// Parser parses one set of optionals.
type Parser struct {
	specs   []Spec
	byStr   map[string]*Spec
	hasNegN bool
}

// New builds a parser over specs.
func New(specs []Spec) *Parser {
	p := &Parser{specs: specs, byStr: map[string]*Spec{}}
	for i := range p.specs {
		for _, s := range p.specs[i].Strs {
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
	spec        *Spec
	optionStr   string
	sep         string // "=" when the explicit argument followed an equals sign
	explicitArg *string
	// ambiguous is set when the argument abbreviates more than one option: argparse (3.14) notes it
	// as an option and raises the error only when it takes that option, so an earlier --help still exits 0.
	ambiguous *Error
}

// classify mirrors ArgumentParser._parse_optional: nil means "positional"
// (an 'A' in argparse's pattern); a match with a nil spec is an unknown
// optional (an 'O' that later reports as unrecognized).
func (p *Parser) classify(arg string) (*optionMatch, *Error) {
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
		return &optionMatch{optionStr: arg, ambiguous: &Error{fmt.Sprintf("ambiguous option: %s could match %s", arg, strings.Join(names, ", "))}}, nil
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
func (p *Parser) prefixMatches(arg string) []optionMatch {
	var out []optionMatch
	if strings.HasPrefix(arg, "--") {
		prefix, explicit, sep := arg, (*string)(nil), ""
		if i := strings.Index(arg, "="); i >= 0 {
			e := arg[i+1:]
			prefix, explicit, sep = arg[:i], &e, "="
		}
		for _, spec := range p.specs {
			for _, s := range spec.Strs {
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
			for _, s := range spec.Strs {
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

// Parse mirrors ArgumentParser.parse_known_args for a Parser with only
// optionals. Actions run in encounter order, as argparse's do; the type
// conversion and mutual-exclusion checks belong to the caller's `convert`.
func (p *Parser) Parse(args []string, act func(spec *Spec, value string) *Error) (*Parsed, *Error) {
	return p.parse(args, act, false)
}

// ParseRoot is Parse for a parser that owns only the arguments BEFORE its
// first positional (argparse's root parser with a subcommand: the first
// positional takes itself and everything after it). Only the arguments up to
// that positional are looked at, as in argparse 3.14: an option the parser does
// not know, or an ambiguous abbreviation, is an error before it and none after
// it. Rest holds the positional and what follows.
func (p *Parser) ParseRoot(args []string, act func(spec *Spec, value string) *Error) (*Parsed, *Error) {
	return p.parse(args, act, true)
}

func (p *Parser) parse(args []string, act func(spec *Spec, value string) *Error, root bool) (*Parsed, *Error) {
	result := &Parsed{Values: map[string]string{}, Flags: map[string]bool{}}
	// classes[i] is argparse's classification of args[i]: nil for a positional,
	// a match (with a nil spec for an option the parser does not know) for an
	// option. Parse classifies every argument up front, as argparse does. A
	// root parser classifies lazily: argparse (3.14) does not look at what
	// follows the first positional, which belongs to the subcommand.
	classes := make([]*optionMatch, len(args))
	classified := make([]bool, len(args))
	var lazyErr *Error
	classify := func(i int) *optionMatch {
		if classified[i] {
			return classes[i]
		}
		classified[i] = true
		if args[i] == "--" {
			return nil
		}
		m, err := p.classify(args[i])
		if err != nil {
			if lazyErr == nil {
				lazyErr = err
			}
			return nil
		}
		classes[i] = m
		return m
	}
	if !root {
		sawDoubleDash := false
		for i := range args {
			if sawDoubleDash {
				classified[i] = true
				continue
			}
			if args[i] == "--" {
				sawDoubleDash = true
			}
			classify(i)
			if lazyErr != nil {
				return nil, lazyErr
			}
		}
	}
	isOption := func(i int) bool {
		if root {
			return classify(i) != nil
		}
		return classes[i] != nil
	}

	i := 0
	for i < len(args) {
		m := classify(i)
		if lazyErr != nil {
			return nil, lazyErr
		}
		if root && (args[i] == "--" || m == nil) {
			result.Rest = args[i:]
			return result, nil
		}
		if m != nil && m.ambiguous != nil {
			return nil, m.ambiguous
		}
		if root && m.spec == nil {
			return nil, &Error{fmt.Sprintf("unrecognized arguments: %s", args[i])}
		}
		if args[i] == "--" || m == nil {
			// A positional (or `--` / anything after it): this Parser has no
			// positionals, so argparse leaves them in the unrecognized list.
			result.Unrecognized = append(result.Unrecognized, args[i])
			i++
			continue
		}
		if m.spec == nil {
			result.Unrecognized = append(result.Unrecognized, args[i])
			i++
			continue
		}
		next, err := p.consumeOptional(args, i, m, isOption, result, act)
		if lazyErr != nil {
			return nil, lazyErr
		}
		if err != nil {
			return nil, err
		}
		i = next
		if result.Help {
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
func (p *Parser) consumeOptional(
	args []string, start int, m *optionMatch, isOption func(int) bool, result *Parsed,
	act func(spec *Spec, value string) *Error,
) (int, *Error) {
	type pending struct {
		spec  *Spec
		value string
	}
	var actions []pending
	spec, optionStr, explicit, sep := m.spec, m.optionStr, m.explicitArg, m.sep
	next := start + 1
	for {
		if explicit == nil {
			if spec.Kind == OptValue {
				if next >= len(args) || isOption(next) || args[next] == "--" {
					return 0, &Error{fmt.Sprintf("argument %s: expected one argument", optionStr)}
				}
				actions = append(actions, pending{spec, args[next]})
				next++
			} else {
				actions = append(actions, pending{spec, ""})
			}
			break
		}
		if spec.Kind == OptValue {
			actions = append(actions, pending{spec, *explicit})
			break
		}
		// A zero-argument option with attached text.
		if len(optionStr) >= 2 && optionStr[1] != '-' && *explicit != "" {
			// A short flag followed by more characters: those may be further
			// short options (-hx), exactly as argparse chains them. An equals
			// sign or a leading dash inside the tail is an error.
			if sep != "" || strings.HasPrefix(*explicit, "-") {
				return 0, &Error{fmt.Sprintf("argument %s: ignored explicit argument %q", optionStr, *explicit)}
			}
			actions = append(actions, pending{spec, ""})
			runes := []rune(*explicit)
			nextStr := "-" + string(runes[0])
			nextSpec, ok := p.byStr[nextStr]
			if !ok {
				// Not an option: the rest is an unrecognized argument, but the
				// options already collected still run (a -h in front of it
				// still prints help).
				result.Unrecognized = append(result.Unrecognized, "-"+*explicit)
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
		return 0, &Error{fmt.Sprintf("argument %s: ignored explicit argument %q", optionStr, *explicit)}
	}
	for _, a := range actions {
		if err := act(a.spec, a.value); err != nil {
			return 0, err
		}
		switch a.spec.Kind {
		case OptValue:
			result.Values[a.spec.Dest] = a.value
		case OptFlag:
			result.Flags[a.spec.Dest] = true
			result.Values[a.spec.Dest] = "true"
		case OptHelp:
			result.Help = true
		}
	}
	return next, nil
}

// ParseWithPositional is Parse for a parser with exactly one positional after its optionals
// (nargs=None, required), as `service-credentials rotate <credential_id> ...` has: argparse's
// consume_positionals over the A/O/- pattern of the arguments, where everything after the first
// "--" is an argument ('A') and the "--" itself ('-') is absorbed by the positional next to it
// (`-*A-*`) and dropped. The positional is nil when none was consumed (the caller reports the
// missing required argument); every argument left over is in Parsed.Unrecognized.
//
// It mirrors _parse_known_args of Python 3.14's argparse, not a reading of its documentation; the
// live oracle of its users compares it with the real parser over a generated corpus.
func (p *Parser) ParseWithPositional(args []string, act func(spec *Spec, value string) *Error) (*Parsed, *string, *Error) {
	result := &Parsed{Values: map[string]string{}, Flags: map[string]bool{}}
	pattern := make([]byte, len(args))
	classes := make([]*optionMatch, len(args))
	afterTerminator := false
	max := -1
	for i, arg := range args {
		switch {
		case afterTerminator:
			pattern[i] = 'A'
		case arg == "--":
			pattern[i] = '-'
			afterTerminator = true
		default:
			match, err := p.classify(arg)
			if err != nil {
				return nil, nil, err
			}
			if match == nil {
				pattern[i] = 'A'
			} else {
				classes[i] = match
				pattern[i] = 'O'
				max = i
			}
		}
	}
	isOption := func(i int) bool { return classes[i] != nil }

	var positional *string
	consume := func(start int) int {
		if positional != nil {
			return start
		}
		i := start
		for i < len(pattern) && pattern[i] == '-' {
			i++
		}
		if i >= len(pattern) || pattern[i] != 'A' {
			return start
		}
		end := i + 1
		for end < len(pattern) && pattern[end] == '-' {
			end++
		}
		taken := append([]string(nil), args[start:end]...)
		if strings.ContainsRune(string(pattern[start:end]), '-') {
			for k, item := range taken {
				if item == "--" {
					taken = append(taken[:k], taken[k+1:]...)
					break
				}
			}
		}
		value := taken[0] // only the first "--" is ever '-', so exactly one argument is left
		positional = &value
		return end
	}

	var extras []string
	start := 0
	for start <= max {
		next := start
		for next <= max && classes[next] == nil {
			next++
		}
		if start != next {
			end := consume(start)
			if end > start {
				start = end
				continue
			}
		}
		if classes[start] == nil {
			extras = append(extras, args[start:next]...)
			start = next
		}
		match := classes[start]
		if match.ambiguous != nil {
			return nil, nil, match.ambiguous
		}
		if match.spec == nil { // an option the parser does not know
			extras = append(extras, args[start])
			start++
			continue
		}
		stop, err := p.consumeOptional(args, start, match, isOption, result, act)
		if err != nil {
			return nil, nil, err
		}
		start = stop
		if result.Help {
			return result, positional, nil
		}
	}
	stop := consume(start)
	extras = append(extras, args[stop:]...)
	result.Unrecognized = extras
	return result, positional, nil
}
