package docstools

import "fmt"

// pyCSVReader is csv.reader(file, delimiter="\t") with the default dialect (quotechar ", doublequote, not strict,
// no escapechar, skipinitialspace False), a port of the state machine of CPython's _csv.c at 3.14.7.

const pyCSVFieldLimit = 131072

type pyCSVError struct{ msg string }

func (e pyCSVError) Error() string { return e.msg }

type csvState int

const (
	csvStartRecord csvState = iota
	csvStartField
	csvInField
	csvInQuoted
	csvQuoteInQuoted
	csvEatCRNL
)

const csvEOL = rune(-2)

type pyCSVReader struct{ file *pyTextFile }

// next returns the fields of the next record; ok is false at end of input.
func (r *pyCSVReader) next() (fields []string, ok bool, err error) {
	state := csvStartRecord
	var field []rune
	fields = []string{}
	save := func() {
		fields = append(fields, string(field))
		field = field[:0]
	}
	process := func(c rune) error {
		add := func(c rune) error {
			if len(field) >= pyCSVFieldLimit {
				return pyCSVError{fmt.Sprintf("field larger than field limit (%d)", pyCSVFieldLimit)}
			}
			field = append(field, c)
			return nil
		}
		switch state {
		case csvStartRecord:
			if c == csvEOL {
				return nil
			}
			if c == '\n' || c == '\r' {
				state = csvEatCRNL
				return nil
			}
			state = csvStartField
			fallthrough
		case csvStartField:
			switch {
			case c == '\n' || c == '\r' || c == csvEOL:
				save()
				if c == csvEOL {
					state = csvStartRecord
				} else {
					state = csvEatCRNL
				}
			case c == '"':
				state = csvInQuoted
			case c == '\t':
				save()
			default:
				state = csvInField
				return add(c)
			}
		case csvInField:
			switch {
			case c == '\n' || c == '\r' || c == csvEOL:
				save()
				if c == csvEOL {
					state = csvStartRecord
				} else {
					state = csvEatCRNL
				}
			case c == '\t':
				save()
				state = csvStartField
			default:
				return add(c)
			}
		case csvInQuoted:
			switch c {
			case csvEOL:
			case '"':
				state = csvQuoteInQuoted
			default:
				return add(c)
			}
		case csvQuoteInQuoted:
			switch {
			case c == '"':
				state = csvInQuoted
				return add('"')
			case c == '\t':
				save()
				state = csvStartField
			case c == '\n' || c == '\r' || c == csvEOL:
				save()
				if c == csvEOL {
					state = csvStartRecord
				} else {
					state = csvEatCRNL
				}
			default:
				state = csvInField
				return add(c)
			}
		case csvEatCRNL:
			switch {
			case c == '\n' || c == '\r':
			case c == csvEOL:
				state = csvStartRecord
			default:
				return pyCSVError{"new-line character seen in unquoted field - do you need to open the file with newline=''?"}
			}
		}
		return nil
	}
	for {
		line, more, err := r.file.readLine()
		if err != nil {
			return nil, false, err
		}
		if !more {
			if len(field) != 0 || state == csvInQuoted {
				save()
				return fields, true, nil
			}
			return nil, false, nil
		}
		for _, c := range line {
			if err := process(c); err != nil {
				return nil, false, err
			}
		}
		if err := process(csvEOL); err != nil {
			return nil, false, err
		}
		if state == csvStartRecord {
			return fields, true, nil
		}
	}
}
