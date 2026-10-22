package docstools

import (
	_ "embed"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Python standard-library calls the publication validator made, beyond the helpers of dc_pyhelp.go: str.title,
// the stem of a path, csv.DictReader, csv.DictWriter, and the TypeError of sorted() on keys that cannot be compared.

//go:embed pytables/title.tsv
var dcTitleTSV string

//go:embed pytables/cased.tsv
var dcCasedTSV string

var dcTitleTable = dcLoadTable(dcTitleTSV)

// dcCasedRanges is the Cased property of Python 3.14.7 (what str.title tests; recorded as islower, isupper or Lt).
var dcCasedRanges = func() []dcRange {
	var out []dcRange
	for _, line := range strings.Split(strings.TrimSuffix(dcCasedTSV, "\n"), "\n") {
		lo, hi, _ := strings.Cut(line, "\t")
		l, err1 := strconv.ParseInt(lo, 16, 32)
		h, err2 := strconv.ParseInt(hi, 16, 32)
		if err1 != nil || err2 != nil {
			panic("docstools: bad pytables row " + line)
		}
		out = append(out, dcRange{rune(l), rune(h)})
	}
	return out
}()

// dcLowerAt is the lower_ucs4 step of str.title: the lower-case form of runes[i] (the final-sigma rule needs the
// neighbours).
func dcLowerAt(runes []rune, i int) string {
	r := runes[i]
	if r == 0x3a3 {
		j := i - 1
		for j >= 0 && dcInRanges(runes[j], dcCaseIgnorable) {
			j--
		}
		final := j >= 0 && dcInRanges(runes[j], dcCasedOnly)
		if final {
			j = i + 1
			for j < len(runes) && dcInRanges(runes[j], dcCaseIgnorable) {
				j++
			}
			final = j == len(runes) || !dcInRanges(runes[j], dcCasedOnly)
		}
		if final {
			return string(rune(0x3c2))
		}
		return string(rune(0x3c3))
	}
	if low, ok := dcLowerTable[r]; ok {
		return low
	}
	return string(r)
}

// dcTitle is str.title() (3.14.7): a character after a cased one is lower-cased, any other is title-cased.
func dcTitle(s string) string {
	runes := []rune(s)
	var b strings.Builder
	prevCased := false
	for i, r := range runes {
		if prevCased {
			b.WriteString(dcLowerAt(runes, i))
		} else if t, ok := dcTitleTable[r]; ok {
			b.WriteString(t)
		} else {
			b.WriteRune(r)
		}
		prevCased = dcInRanges(r, dcCasedRanges)
	}
	return b.String()
}

// dcPyStem is PurePath(p).stem (3.14).
func dcPyStem(p string) string {
	s := a1PathStr(p)
	if s == "." || s == "/" || s == "//" {
		return ""
	}
	name := s[strings.LastIndex(s, "/")+1:]
	if suffix := dcPathSuffix(s); suffix != "" {
		return strings.TrimSuffix(name, suffix)
	}
	return name
}

// ---- csv.DictReader

const dcCSVFieldLimit = 131072

type dcCSVRowMap map[string]*string // a column whose cell is missing holds nil (None)

// dcCSVRows is list(csv.DictReader(handle, delimiter="\t")) for a file opened as UTF-8 with newline="". text is the raw
// decoded text: no newline translation.
func dcCSVRows(text string) ([]dcCSVRowMap, error) {
	records, err := dcCSVParse(text)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	fields := records[0]
	var rows []dcCSVRowMap
	for _, rec := range records[1:] {
		if len(rec) == 0 {
			continue
		}
		d := dcCSVRowMap{}
		for i, name := range fields {
			if i < len(rec) {
				v := rec[i]
				d[name] = &v
			} else {
				d[name] = nil
			}
		}
		rows = append(rows, d)
	}
	return rows, nil
}

// dcCSVParse is _csv.reader with delimiter "\t" over the lines of a newline="" text: every row, a blank line as [].
func dcCSVParse(text string) ([][]string, error) {
	const (
		startRecord = iota
		startField
		inField
		inQuoted
		quoteInQuoted
		eatCRNL
	)
	var rows [][]string
	var fields []string
	var field []rune
	state := startRecord
	saveField := func() { fields = append(fields, string(field)); field = nil }
	addChar := func(c rune) error {
		if len(field) >= dcCSVFieldLimit {
			return &dcYErr{"_csv.Error", fmt.Sprintf("field larger than field limit (%d)", dcCSVFieldLimit)}
		}
		field = append(field, c)
		return nil
	}
	const eol = rune(-2)
	process := func(c rune) error {
		switch state {
		case startRecord:
			if c == eol {
				return nil
			}
			if c == '\n' || c == '\r' {
				state = eatCRNL
				return nil
			}
			state = startField
			fallthrough
		case startField:
			switch {
			case c == '\n' || c == '\r' || c == eol:
				saveField()
				if c == eol {
					state = startRecord
				} else {
					state = eatCRNL
				}
			case c == '"':
				state = inQuoted
			case c == '\t':
				saveField()
			default:
				if err := addChar(c); err != nil {
					return err
				}
				state = inField
			}
		case inField:
			switch {
			case c == '\n' || c == '\r' || c == eol:
				saveField()
				if c == eol {
					state = startRecord
				} else {
					state = eatCRNL
				}
			case c == '\t':
				saveField()
				state = startField
			default:
				return addChar(c)
			}
		case inQuoted:
			switch {
			case c == eol:
			case c == '"':
				state = quoteInQuoted
			default:
				return addChar(c)
			}
		case quoteInQuoted:
			switch {
			case c == '"':
				if err := addChar(c); err != nil {
					return err
				}
				state = inQuoted
			case c == '\t':
				saveField()
				state = startField
			case c == '\n' || c == '\r' || c == eol:
				saveField()
				if c == eol {
					state = startRecord
				} else {
					state = eatCRNL
				}
			default:
				if err := addChar(c); err != nil {
					return err
				}
				state = inField
			}
		case eatCRNL:
			switch {
			case c == '\n' || c == '\r':
			case c == eol:
				state = startRecord
			default:
				return &dcYErr{"_csv.Error", "new-line character seen in unquoted field - do you need to open the file with newline=''?"}
			}
		}
		return nil
	}
	flush := func() {
		if state == startRecord {
			rows = append(rows, fields)
			fields = nil
		}
	}
	for _, line := range dcCSVLines(text) {
		for _, c := range line {
			if err := process(c); err != nil {
				return nil, err
			}
		}
		if err := process(eol); err != nil {
			return nil, err
		}
		flush()
	}
	if len(field) != 0 || state == inQuoted {
		saveField()
		rows = append(rows, fields)
	}
	return rows, nil
}

// dcCSVLines splits text the way iterating a file opened with newline="" does: after each "\n", "\r" or "\r\n".
func dcCSVLines(text string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '\n':
			lines = append(lines, text[start:i+1])
			start = i + 1
		case '\r':
			if i+1 < len(text) && text[i+1] == '\n' {
				i++
			}
			lines = append(lines, text[start:i+1])
			start = i + 1
		}
	}
	if start < len(text) {
		lines = append(lines, text[start:])
	}
	return lines
}

// dcReadRawText reads a file as open(path, encoding="utf-8", newline="") does: no newline translation.
func dcReadRawText(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if !utf8.Valid(raw) {
		return "", a1DecodeError{path}
	}
	return string(raw), nil
}

// dcPubCSVRow is csv.writer(delimiter="\t", lineterminator="\n").writerow.
func dcPubCSVRow(fields []string) string {
	return strings.TrimSuffix(dcCSVRow(fields), "\r\n") + "\n"
}

// ---- sorted() with a key that may be None

// dcSortKeyTypeError replays the comparisons CPython's list.sort makes for fewer than 64 items (count_run, then binary
// insertion) until one involves a None; keys[i] == nil is None. It returns the TypeError text of that comparison, or ""
// when no comparison fails. For 64 items or more the merge phase is not replayed and the first-run message is used (the
// text may differ from Python's; the refusal and exit code do not).
func dcSortKeyTypeError(keys []*string) string {
	n := len(keys)
	if n < 2 {
		return ""
	}
	typeName := func(k *string) string {
		if k == nil {
			return "NoneType"
		}
		return "str"
	}
	lt := func(a, b *string) (bool, string) {
		if a == nil || b == nil {
			return false, fmt.Sprintf("'<' not supported between instances of '%s' and '%s'", typeName(a), typeName(b))
		}
		return *a < *b, ""
	}
	idx := make([]*string, n)
	copy(idx, keys)
	// count_run
	run := 2
	less, msg := lt(idx[1], idx[0])
	if msg != "" {
		return msg
	}
	if less {
		for run < n {
			l, m := lt(idx[run], idx[run-1])
			if m != "" {
				return m
			}
			if !l {
				break
			}
			run++
		}
		for i, j := 0, run-1; i < j; i, j = i+1, j-1 {
			idx[i], idx[j] = idx[j], idx[i]
		}
	} else {
		for run < n {
			l, m := lt(idx[run], idx[run-1])
			if m != "" {
				return m
			}
			if l {
				break
			}
			run++
		}
	}
	if n >= 64 {
		for i := run; i < n; i++ {
			if idx[i] == nil {
				return "'<' not supported between instances of 'NoneType' and 'str'"
			}
		}
		return ""
	}
	for start := run; start < n; start++ {
		pivot := idx[start]
		l, r := 0, start
		for l < r {
			p := l + (r-l)/2
			less, m := lt(pivot, idx[p])
			if m != "" {
				return m
			}
			if less {
				r = p
			} else {
				l = p + 1
			}
		}
		copy(idx[l+1:start+1], idx[l:start])
		idx[l] = pivot
	}
	return ""
}
