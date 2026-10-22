package docstools

import (
	"errors"
	"unicode/utf8"
)

// pyTextFile reads bytes the way open(path, encoding="utf-8", newline="") does: the file is decoded in chunks of 8192
// bytes, on demand, so a byte that is not UTF-8 raises only when a read reaches its chunk (a row read from an earlier
// chunk never sees it; verified on Python 3.14.7: bad byte at offset 8191 raises, at 8192 does not for a first row).
type pyTextFile struct {
	data     []byte
	consumed int // bytes decoded so far
	buf      []rune
	pos      int
}

const pyTextChunk = 8192

var errPyDecode = errors.New("'utf-8' codec can't decode input: invalid UTF-8")

func (f *pyTextFile) eof() bool { return f.consumed >= len(f.data) }

func (f *pyTextFile) fill() error {
	end := f.consumed + pyTextChunk
	if end > len(f.data) {
		end = len(f.data)
	}
	start := f.consumed
	seg := f.data[start:end]
	for len(seg) > 0 {
		r, size := utf8.DecodeRune(seg)
		if r == utf8.RuneError && size <= 1 {
			if utf8.FullRune(seg) {
				return errPyDecode
			}
			// A sequence cut by the end of the chunk waits for the next read; at the end of the file it is an error only
			// once a read needs more text (the final decode), as in Python's incremental decoder.
			if f.consumed > start {
				return nil
			}
			if end < len(f.data) {
				return nil
			}
			return errPyDecode
		}
		f.buf = append(f.buf, r)
		f.consumed += size
		seg = seg[size:]
	}
	return nil
}

// readLine returns the next line with its terminator (\n, \r or \r\n); ok is false at end of file.
func (f *pyTextFile) readLine() (line []rune, ok bool, err error) {
	scan := f.pos
	for {
		for ; scan < len(f.buf); scan++ {
			c := f.buf[scan]
			if c == '\n' {
				return f.take(scan + 1), true, nil
			}
			if c == '\r' {
				if scan+1 >= len(f.buf) && !f.eof() {
					if err := f.fill(); err != nil {
						return nil, false, err
					}
				}
				if scan+1 < len(f.buf) && f.buf[scan+1] == '\n' {
					return f.take(scan + 2), true, nil
				}
				return f.take(scan + 1), true, nil
			}
		}
		if f.eof() {
			if f.pos >= len(f.buf) {
				return nil, false, nil
			}
			return f.take(len(f.buf)), true, nil
		}
		if err := f.fill(); err != nil {
			return nil, false, err
		}
	}
}

func (f *pyTextFile) take(end int) []rune {
	line := f.buf[f.pos:end]
	f.pos = end
	return line
}
