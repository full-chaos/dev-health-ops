package pythonparity

import (
	"fmt"
	"strings"
)

// CSVFieldLimit is csv.field_size_limit() at its CPython default.
const CSVFieldLimit = 131072

// CSVError is a _csv.Error; Error() is Python's message text.
type CSVError struct{ Msg string }

func (e *CSVError) Error() string { return e.Msg }

type csvState int

const (
	csvStartRecord csvState = iota
	csvStartField
	csvInField
	csvInQuotedField
	csvQuoteInQuotedField
	csvEatCRNL
)

const csvEOL rune = -2

type csvReader struct {
	state  csvState
	field  []rune
	fields []string
}

func (r *csvReader) save() {
	r.fields = append(r.fields, string(r.field))
	r.field = r.field[:0]
}

func (r *csvReader) add(c rune) error {
	if len(r.field) >= CSVFieldLimit {
		return &CSVError{Msg: fmt.Sprintf("field larger than field limit (%d)", CSVFieldLimit)}
	}
	r.field = append(r.field, c)
	return nil
}

// process is parse_process_char of CPython 3.12 _csv.c (3.14 behaves the same) for delimiter TAB, quotechar '"', doublequote, no escapechar,
// skipinitialspace off, strict off, QUOTE_MINIMAL.
func (r *csvReader) process(c rune) error {
	nl := c == '\n' || c == '\r'
	switch r.state {
	case csvStartRecord:
		if c == csvEOL {
			return nil
		}
		if nl {
			r.state = csvEatCRNL
			return nil
		}
		r.state = csvStartField
		fallthrough
	case csvStartField:
		switch {
		case nl || c == csvEOL:
			r.save()
			r.state = csvEatCRNL
			if c == csvEOL {
				r.state = csvStartRecord
			}
		case c == '"':
			r.state = csvInQuotedField
		case c == '\t':
			r.save()
		default:
			if err := r.add(c); err != nil {
				return err
			}
			r.state = csvInField
		}
	case csvInField:
		switch {
		case nl || c == csvEOL:
			r.save()
			r.state = csvEatCRNL
			if c == csvEOL {
				r.state = csvStartRecord
			}
		case c == '\t':
			r.save()
			r.state = csvStartField
		default:
			return r.add(c)
		}
	case csvInQuotedField:
		switch {
		case c == csvEOL:
		case c == '"':
			r.state = csvQuoteInQuotedField
		default:
			return r.add(c)
		}
	case csvQuoteInQuotedField:
		switch {
		case c == '"':
			if err := r.add(c); err != nil {
				return err
			}
			r.state = csvInQuotedField
		case c == '\t':
			r.save()
			r.state = csvStartField
		case nl || c == csvEOL:
			r.save()
			r.state = csvEatCRNL
			if c == csvEOL {
				r.state = csvStartRecord
			}
		default:
			if err := r.add(c); err != nil {
				return err
			}
			r.state = csvInField
		}
	case csvEatCRNL:
		switch {
		case nl:
		case c == csvEOL:
			r.state = csvStartRecord
		default:
			return &CSVError{Msg: "new-line character seen in unquoted field - do you need to open the file with newline=''?"}
		}
	}
	return nil
}

// csvLines splits like iterating io.StringIO(data, newline=""): each line ends after \n, \r\n or a lone \r.
func csvLines(data string) [][]rune {
	var lines [][]rune
	runes := []rune(data)
	start := 0
	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case '\n':
			lines = append(lines, runes[start:i+1])
			start = i + 1
		case '\r':
			if i+1 < len(runes) && runes[i+1] == '\n' {
				i++
			}
			lines = append(lines, runes[start:i+1])
			start = i + 1
		}
	}
	if start < len(runes) {
		lines = append(lines, runes[start:])
	}
	return lines
}

// ReadCSVTab is list(csv.reader(io.StringIO(data, newline=""), delimiter="\t")) on CPython 3.14.7 (and 3.12.3).
func ReadCSVTab(data string) ([][]string, error) {
	rows := [][]string{}
	lines := csvLines(data)
	next := 0
	for {
		r := &csvReader{state: csvStartRecord, fields: []string{}}
		for {
			if next >= len(lines) {
				if len(r.field) != 0 || r.state == csvInQuotedField {
					r.save()
					return append(rows, r.fields), nil
				}
				return rows, nil
			}
			for _, c := range lines[next] {
				if err := r.process(c); err != nil {
					return nil, err
				}
			}
			next++
			if err := r.process(csvEOL); err != nil {
				return nil, err
			}
			if r.state == csvStartRecord {
				break
			}
		}
		rows = append(rows, r.fields)
	}
}

// WriteCSVTabRow is what csv.writer(f, delimiter="\t", lineterminator="\n").writerow(fields) writes on CPython 3.14.7 (and 3.12.3).
func WriteCSVTabRow(fields []string) (string, error) {
	var b strings.Builder
	for i, f := range fields {
		if i > 0 {
			b.WriteByte('\t')
		}
		if !strings.ContainsAny(f, "\t\"\n\r") {
			b.WriteString(f)
			continue
		}
		b.WriteByte('"')
		b.WriteString(strings.ReplaceAll(f, `"`, `""`))
		b.WriteByte('"')
	}
	if len(fields) == 1 && fields[0] == "" {
		return "\"\"\n", nil
	}
	b.WriteByte('\n')
	return b.String(), nil
}
