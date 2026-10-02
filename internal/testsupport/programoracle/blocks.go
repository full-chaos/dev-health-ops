package programoracle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unicode/utf8"
)

// An oracle that sweeps every code point has millions of answers: too many to
// freeze as text. Such an oracle freezes one SHA-256 per block of answers
// instead. The program writes one canonical line per answer and prints the
// block digests (BlocksPython); the test writes the same line from the Go
// answer and compares digests (RequireBlocks). The comparison stays
// exhaustive; a difference names the block, not the answer inside it.
//
// The format is stated here, once, for both halves. A canonical line is the
// fields of one answer joined by "|". A field is a list of code points
// (decimal numbers joined by ",": code_points / AppendCodePoints), a string
// (its UTF-8 in hexadecimal: text / AppendText), or a fixed ASCII token. So a
// line is ASCII with no line break, and two different answers never give one
// line. A block is BlockLines lines, each followed by a line feed; the last
// block may be shorter. Both the format and the block size are in the program
// text, so a golden recorded for another format is refused by its request key.

// BlockLines is the number of canonical lines one digest covers.
const BlockLines = 1024

// BlocksPython is Python source for the head of an oracle program. It defines
// code_points and text, the two fields of a canonical line, and block_digests,
// whose result the program prints as JSON (see Blocks).
const BlocksPython = `
import hashlib, json

BLOCK_LINES = 1024

def code_points(values):
    return ",".join(str(value) for value in values)

def text(value):
    # The string a JSON reader gives: an escaped surrogate pair is one code
    # point, a lone surrogate is U+FFFD. The field is its UTF-8 in hexadecimal.
    value = json.loads(json.dumps(value))
    return "".join("\ufffd" if "\ud800" <= c <= "\udfff" else c for c in value).encode("utf-8").hex()

def block_digests(lines):
    blocks = []
    for first in range(0, len(lines), BLOCK_LINES):
        block = lines[first:first + BLOCK_LINES]
        digest = hashlib.sha256()
        for line in block:
            if "\n" in line:
                raise ValueError("a canonical line holds a line break")
            digest.update(line.encode("ascii") + b"\n")
        blocks.append({"first": first, "lines": len(block), "sha256": digest.hexdigest()})
    return {"block_lines": BLOCK_LINES, "lines": len(lines), "block_count": len(blocks), "blocks": blocks}
`

// AppendCodePoints appends the canonical field of a list of code points: the
// numbers in decimal, joined by commas. It is the Go half of Python's
// code_points.
func AppendCodePoints(dst []byte, runes []rune) []byte {
	for index, r := range runes {
		if index > 0 {
			dst = append(dst, ',')
		}
		dst = strconv.AppendInt(dst, int64(r), 10)
	}
	return dst
}

// AppendText appends the canonical field of a string: its UTF-8 in
// hexadecimal. It is the Go half of Python's text. A string that is not valid
// UTF-8 has no Python equal, and gets a field no producer writes.
func AppendText(dst []byte, value string) []byte {
	if !utf8.ValidString(value) {
		dst = append(dst, "invalid-utf8:"...)
	}
	return hex.AppendEncode(dst, []byte(value))
}

// Block is one frozen block: the index of its first answer, the number of
// answers in it, and the SHA-256 of their canonical lines.
type Block struct {
	First  int    `json:"first"`
	Lines  int    `json:"lines"`
	SHA256 string `json:"sha256"`
}

// Blocks is what block_digests printed: the block size, the number of
// answers, the number of blocks, and each block.
type Blocks struct {
	BlockLines int     `json:"block_lines"`
	Lines      int     `json:"lines"`
	BlockCount int     `json:"block_count"`
	Blocks     []Block `json:"blocks"`
}

// DecodeBlocks reads the JSON a program printed from block_digests. It refuses
// a block size that is not BlockLines, and blocks that do not cover the
// answers exactly, in order: a missing tail or a dropped block is an error
// here, not a shorter list.
func DecodeBlocks(output string) (Blocks, error) {
	var blocks Blocks
	decoder := json.NewDecoder(strings.NewReader(output))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&blocks); err != nil {
		return Blocks{}, fmt.Errorf("decode the block digests: %w", err)
	}
	if blocks.BlockLines != BlockLines {
		return Blocks{}, fmt.Errorf("the producer hashed blocks of %d answers, this test hashes blocks of %d", blocks.BlockLines, BlockLines)
	}
	if want := (blocks.Lines + BlockLines - 1) / BlockLines; blocks.BlockCount != want || len(blocks.Blocks) != want {
		return Blocks{}, fmt.Errorf("%d blocks (the producer counted %d) for %d answers, want %d", len(blocks.Blocks), blocks.BlockCount, blocks.Lines, want)
	}
	for index, block := range blocks.Blocks {
		first := index * BlockLines
		if block.First != first || block.Lines != min(BlockLines, blocks.Lines-first) || len(block.SHA256) != sha256.Size*2 {
			return Blocks{}, fmt.Errorf("block %d is {first %d, %d answers, digest %q}, want first %d and %d answers",
				index, block.First, block.Lines, block.SHA256, first, min(BlockLines, blocks.Lines-first))
		}
	}
	return blocks, nil
}

// digest is the SHA-256 of the canonical lines of block, written by line.
func (blocks Blocks) digest(buffer []byte, block int, line func(dst []byte, index int) []byte) (string, []byte, error) {
	frozen := blocks.Blocks[block]
	digest := sha256.New()
	for index := frozen.First; index < frozen.First+frozen.Lines; index++ {
		buffer = line(buffer[:0], index)
		if !isCanonical(buffer) {
			return "", buffer, fmt.Errorf("line %d is not canonical (ASCII, no line break): %q", index, buffer)
		}
		buffer = append(buffer, '\n')
		digest.Write(buffer)
	}
	return hex.EncodeToString(digest.Sum(nil)), buffer, nil
}

// Differing returns the indexes of the blocks whose digest is not the digest
// of the caller's own canonical lines. line appends the canonical line of
// answer index to dst and returns it; blocks are hashed on several goroutines,
// so line must be safe to call for different indexes at once. It is an error
// when count is not the producer's number of answers, or when a line is not
// canonical.
func (blocks Blocks) Differing(count int, line func(dst []byte, index int) []byte) ([]int, error) {
	if count != blocks.Lines {
		return nil, fmt.Errorf("%d answers here, %d from the producer: an input was added or dropped", count, blocks.Lines)
	}
	differs := make([]bool, len(blocks.Blocks))
	failures := make([]error, len(blocks.Blocks))
	var next atomic.Int64
	var workers sync.WaitGroup
	for range min(runtime.GOMAXPROCS(0), len(blocks.Blocks)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var buffer []byte
			for {
				block := int(next.Add(1)) - 1
				if block >= len(blocks.Blocks) {
					return
				}
				var found string
				found, buffer, failures[block] = blocks.digest(buffer, block, line)
				differs[block] = found != blocks.Blocks[block].SHA256
			}
		}()
	}
	workers.Wait()
	var differing []int
	for block := range blocks.Blocks {
		if failures[block] != nil {
			return nil, failures[block]
		}
		if differs[block] {
			differing = append(differing, block)
		}
	}
	return differing, nil
}

func isCanonical(line []byte) bool {
	for _, b := range line {
		if b >= utf8.RuneSelf || b == '\n' {
			return false
		}
	}
	return true
}

// RequireBlocks fails the test unless the caller's own canonical lines have
// the block digests the program printed in output. count is the number of
// answers; line appends the canonical line of answer index to dst (see
// Blocks.Differing). The failure names every block that differs with its
// range of answers and, through describe, prints the answers here of the
// first one: describe(index) is the input and the answer of line index, for a
// reader.
func RequireBlocks(t *testing.T, name, output string, count int, line func(dst []byte, index int) []byte, describe func(index int) string) {
	t.Helper()
	if err := blocksErr(name, output, count, line, describe); err != nil {
		t.Fatal(err)
	}
}

func blocksErr(name, output string, count int, line func(dst []byte, index int) []byte, describe func(index int) string) error {
	blocks, err := DecodeBlocks(output)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	differing, err := blocks.Differing(count, line)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if len(differing) == 0 {
		return nil
	}
	var report strings.Builder
	fmt.Fprintf(&report, "%s: %d of %d blocks of answers differ from the frozen Python answers. "+
		"A frozen block is the digest of %d answers: it names the block, not the answer in it.", name, len(differing), len(blocks.Blocks), BlockLines)
	for position, block := range differing {
		if position == 20 {
			fmt.Fprintf(&report, "\nand %d more blocks", len(differing)-position)
			break
		}
		frozen := blocks.Blocks[block]
		fmt.Fprintf(&report, "\nblock %d (answers %d to %d)", block, frozen.First, frozen.First+frozen.Lines-1)
	}
	first := blocks.Blocks[differing[0]]
	fmt.Fprintf(&report, "\nthe answers here of block %d:", differing[0])
	for index := first.First; index < first.First+first.Lines; index++ {
		fmt.Fprintf(&report, "\n  %d: %s", index, describe(index))
	}
	return errors.New(report.String())
}

// RequireFindsDefect fails the test unless the frozen digests refuse one wrong
// answer in the sweep: with defect as the canonical line of answer index, and
// every other line as line writes it, the block of index must differ. A sweep
// test names a defect a port could have and proves on every run that the
// digest form still finds it, and where.
func RequireFindsDefect(t *testing.T, name, output string, index int, line func(dst []byte, index int) []byte, defect func(dst []byte) []byte) {
	t.Helper()
	if err := findsDefectErr(name, output, index, line, defect); err != nil {
		t.Fatal(err)
	}
}

func findsDefectErr(name, output string, index int, line func(dst []byte, index int) []byte, defect func(dst []byte) []byte) error {
	blocks, err := DecodeBlocks(output)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if index < 0 || index >= blocks.Lines {
		return fmt.Errorf("%s: answer %d is not one of the %d answers", name, index, blocks.Lines)
	}
	block := index / BlockLines
	// The control: with the right answer at index (line as it is) the block must
	// digest to the frozen digest. If it does not, the Go lines of the block are
	// not the frozen answers, and ANY defect planted in it would "differ" for
	// that reason alone: the gate would pass on a plant that did nothing.
	control, _, err := blocks.digest(nil, block, line)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if control != blocks.Blocks[block].SHA256 {
		return fmt.Errorf("%s: block %d of the Go answers does not digest to the frozen digest before any defect is planted: the gate cannot tell a planted defect from the answers that already differ", name, block)
	}
	found, _, err := blocks.digest(nil, block, func(dst []byte, at int) []byte {
		if at == index {
			return defect(dst)
		}
		return line(dst, at)
	})
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if found == blocks.Blocks[block].SHA256 {
		return fmt.Errorf("%s: the frozen digest of block %d accepts the defect as answer %d: the block digests no longer find it", name, block, index)
	}
	return nil
}
