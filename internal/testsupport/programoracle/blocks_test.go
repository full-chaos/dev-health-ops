package programoracle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// blockLinesProgram writes 2500 canonical lines (three blocks, the last one
// short) with both field kinds: a code point list, and a string that holds an
// astral code point, an escaped surrogate pair and a lone surrogate.
const blockLinesProgram = BlocksPython + `
lines = []
for n in range(2500):
    lines.append(code_points([n, n * 7 % 0x110000]) + "|" + text("\u00e9" + chr(0x1F600) + "\ud83d\ude00" + "\ud800" + str(n)))
print(json.dumps(block_digests(lines)))
`

// blockLine is the Go half of blockLinesProgram's line n. A JSON reader gives
// the escaped pair as U+1F600 and the lone surrogate as U+FFFD.
func blockLine(dst []byte, n int) []byte {
	dst = AppendCodePoints(dst, []rune{rune(n), rune(n * 7 % 0x110000)})
	dst = append(dst, '|')
	return AppendText(dst, "\u00e9\U0001F600\U0001F600\uFFFD"+fmt.Sprint(n))
}

// fixedLines gives each of lines as it is.
func fixedLines(lines []string) func(dst []byte, index int) []byte {
	return func(dst []byte, index int) []byte { return append(dst, lines[index]...) }
}

// TestTheGoHalfOfTheBlockDigestsIsThePythonHalf runs the Python half frozen and
// the Go half here over the same answers: no block differs. One changed
// answer makes exactly its block differ.
func TestTheGoHalfOfTheBlockDigestsIsThePythonHalf(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	spec := venueoracle.GoldenSpec{
		Path:        "testdata/golden/blocks.golden.json",
		PythonBuild: runGoldenPythonBuild,
		SHA256:      "924f5d6ac7d4474abcb1cefc9cdcea80e20f52cbf6a31b39c99e7d56384b6ec4",
		Recipe: "git worktree add --detach $DIR " + runGoldenPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/testsupport/programoracle/ -test '^TestTheGoHalfOfTheBlockDigestsIsThePythonHalf$' -python-root $DIR",
	}
	answers := Run(t, spec, root, []Program{{Name: "block digests", Text: blockLinesProgram}})
	if answers[0].ExitCode != 0 {
		t.Fatalf("the program exited %d: %q", answers[0].ExitCode, answers[0].Stdout)
	}
	blocks, err := DecodeBlocks(answers[0].Stdout)
	if err != nil {
		t.Fatal(err)
	}
	if blocks.Lines != 2500 || blocks.BlockCount != 3 || blocks.Blocks[2].First != 2048 || blocks.Blocks[2].Lines != 452 {
		t.Fatalf("the producer counted %d answers in %d blocks, the last one %+v", blocks.Lines, blocks.BlockCount, blocks.Blocks[2])
	}
	differing, err := blocks.Differing(2500, blockLine)
	if err != nil || len(differing) != 0 {
		t.Fatalf("blocks %v differ (%v): the two halves do not write the same lines", differing, err)
	}
	differing, err = blocks.Differing(2500, func(dst []byte, n int) []byte {
		if n == 1500 {
			return AppendText(dst, "changed")
		}
		return blockLine(dst, n)
	})
	if err != nil || len(differing) != 1 || differing[0] != 1 {
		t.Fatalf("one changed answer in block 1: blocks %v differ (%v)", differing, err)
	}
}

func digestOf(lines ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n") + "\n"))
	return hex.EncodeToString(sum[:])
}

// frozenBlocks is the frozen form of lines, as the Python half prints it.
func frozenBlocks(lines []string) Blocks {
	blocks := Blocks{BlockLines: BlockLines, Lines: len(lines)}
	for first := 0; first < len(lines); first += BlockLines {
		block := lines[first:min(first+BlockLines, len(lines))]
		blocks.Blocks = append(blocks.Blocks, Block{First: first, Lines: len(block), SHA256: digestOf(block...)})
	}
	blocks.BlockCount = len(blocks.Blocks)
	return blocks
}

func numberedLines(count int) []string {
	lines := make([]string, count)
	for index := range lines {
		lines[index] = fmt.Sprint(index)
	}
	return lines
}

func TestDifferingNamesEachBlockThatIsNotTheProducers(t *testing.T) {
	lines := numberedLines(3*BlockLines + 2)
	blocks := frozenBlocks(lines)
	if differing, err := blocks.Differing(len(lines), fixedLines(lines)); err != nil || len(differing) != 0 {
		t.Fatalf("equal lines: blocks %v differ (%v)", differing, err)
	}
	changes := map[string]struct {
		change func(lines []string)
		want   []int
	}{
		"one answer changed in the middle of the sweep": {func(lines []string) { lines[BlockLines+500] = "x" }, []int{1}},
		"two answers swapped inside a block": {func(lines []string) {
			lines[2*BlockLines+7], lines[2*BlockLines+8] = lines[2*BlockLines+8], lines[2*BlockLines+7]
		}, []int{2}},
		"the last answer changed":                     {func(lines []string) { lines[len(lines)-1] = "y" }, []int{3}},
		"a change in the first and in the last block": {func(lines []string) { lines[0], lines[len(lines)-1] = "x", "y" }, []int{0, 3}},
	}
	for name, c := range changes {
		changed := append([]string(nil), lines...)
		c.change(changed)
		if differing, err := blocks.Differing(len(changed), fixedLines(changed)); err != nil || fmt.Sprint(differing) != fmt.Sprint(c.want) {
			t.Errorf("%s: blocks %v differ (%v), want %v", name, differing, err, c.want)
		}
	}
	// A digest is compared whole: one that is right only in its first half differs.
	half := frozenBlocks(lines)
	half.Blocks[0].SHA256 = half.Blocks[0].SHA256[:32] + strings.Repeat("0", 32)
	if differing, err := half.Differing(len(lines), fixedLines(lines)); err != nil || len(differing) != 1 || differing[0] != 0 {
		t.Fatalf("a digest right in its first half only: blocks %v differ (%v)", differing, err)
	}
	refused := map[string][]string{
		"the last answer dropped":  lines[:len(lines)-1],
		"one answer more":          append(append([]string(nil), lines...), "extra"),
		"a line with a break":      append(append([]string(nil), lines[:len(lines)-1]...), "a\nb"),
		"a line that is not ASCII": append(append([]string(nil), lines[:len(lines)-1]...), "\u00e9"),
	}
	for name, candidate := range refused {
		if _, err := blocks.Differing(len(candidate), fixedLines(candidate)); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// TestDecodeBlocksNeedsBlocksThatCoverTheAnswers pins what the frozen form
// must say: the block size of this package, and blocks that cover the answers
// exactly, in order. A dropped block or a missing tail is refused here.
func TestDecodeBlocksNeedsBlocksThatCoverTheAnswers(t *testing.T) {
	encode := func(change func(blocks *Blocks)) string {
		blocks := frozenBlocks(numberedLines(2*BlockLines + 1))
		change(&blocks)
		encoded, err := json.Marshal(blocks)
		if err != nil {
			t.Fatal(err)
		}
		return string(encoded)
	}
	blocks, err := DecodeBlocks(encode(func(*Blocks) {}))
	if err != nil || blocks.Lines != 2*BlockLines+1 || len(blocks.Blocks) != 3 || blocks.Blocks[2].Lines != 1 {
		t.Fatalf("blocks = %+v, %v", blocks, err)
	}
	if blocks, err := DecodeBlocks(`{"block_lines": 1024, "lines": 0, "block_count": 0, "blocks": []}`); err != nil || blocks.Lines != 0 {
		t.Fatalf("no answers: %+v, %v", blocks, err)
	}
	refused := map[string]string{
		"another block size":            encode(func(blocks *Blocks) { blocks.BlockLines = 512 }),
		"the last block dropped":        encode(func(blocks *Blocks) { blocks.Blocks = blocks.Blocks[:2] }),
		"a middle block dropped":        encode(func(blocks *Blocks) { blocks.Blocks = append(blocks.Blocks[:1], blocks.Blocks[2:]...) }),
		"a block dropped and uncounted": encode(func(blocks *Blocks) { blocks.Blocks, blocks.BlockCount = blocks.Blocks[:2], 2 }),
		"a tail dropped and recounted": encode(func(blocks *Blocks) {
			blocks.Blocks, blocks.BlockCount, blocks.Lines = blocks.Blocks[:2], 2, 2*BlockLines-1
		}),
		"another block count":            encode(func(blocks *Blocks) { blocks.BlockCount = 4 }),
		"a block that starts elsewhere":  encode(func(blocks *Blocks) { blocks.Blocks[1].First = BlockLines + 1 }),
		"a block of another length":      encode(func(blocks *Blocks) { blocks.Blocks[1].Lines = BlockLines - 1 }),
		"two blocks swapped":             encode(func(blocks *Blocks) { blocks.Blocks[0], blocks.Blocks[1] = blocks.Blocks[1], blocks.Blocks[0] }),
		"a digest that is not a SHA-256": encode(func(blocks *Blocks) { blocks.Blocks[0].SHA256 = "abc" }),
		"another field":                  `{"block_lines": 1024, "lines": 0, "block_count": 0, "blocks": [], "answers": []}`,
		"not JSON":                       `Traceback`,
	}
	for name, output := range refused {
		if _, err := DecodeBlocks(output); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestTheCanonicalFieldsAreUnambiguous(t *testing.T) {
	if got := string(AppendCodePoints(nil, []rune{0, 0xD800, 0x10FFFF})); got != "0,55296,1114111" {
		t.Errorf("AppendCodePoints = %q", got)
	}
	if got := string(AppendCodePoints([]byte("x|"), nil)); got != "x|" {
		t.Errorf("AppendCodePoints of no code point = %q", got)
	}
	if got := string(AppendText(nil, "a|,\n\u00e9")); got != "617c2c0ac3a9" {
		t.Errorf("AppendText = %q", got)
	}
	if got := string(AppendText(nil, "a\xff")); got != "invalid-utf8:61ff" {
		t.Errorf("AppendText of invalid UTF-8 = %q", got)
	}
}

// TestTheProgramHalfHashesBlocksOfBlockLines pins the one number the two
// halves share: the Python half's block size is BlockLines.
func TestTheProgramHalfHashesBlocksOfBlockLines(t *testing.T) {
	if want := fmt.Sprintf("\nBLOCK_LINES = %d\n", BlockLines); !strings.Contains(BlocksPython, want) {
		t.Fatalf("BlocksPython does not hold %q", want)
	}
}

// TestADifferingBlockIsNamedWithItsAnswers pins the failure a sweep oracle
// gives: equal answers pass; a changed answer names its block, the range of
// answers it covers, and every answer here of the first block that differs.
func TestADifferingBlockIsNamedWithItsAnswers(t *testing.T) {
	lines := numberedLines(2*BlockLines + 2)
	encoded, err := json.Marshal(frozenBlocks(lines))
	if err != nil {
		t.Fatal(err)
	}
	output := string(encoded)
	describe := func(index int) string { return fmt.Sprintf("answer-%d;", index) }
	if err := blocksErr("sweep", output, len(lines), fixedLines(lines), describe); err != nil {
		t.Fatalf("equal answers were refused: %v", err)
	}
	changed := append([]string(nil), lines...)
	changed[BlockLines+500], changed[2*BlockLines+1] = "changed", "changed"
	err = blocksErr("sweep", output, len(changed), fixedLines(changed), describe)
	if err == nil {
		t.Fatal("changed answers were accepted")
	}
	for _, part := range []string{"sweep: 2 of 3 blocks", "block 1 (answers 1024 to 2047)", "block 2 (answers 2048 to 2049)",
		"the answers here of block 1:", "answer-1024;", "answer-1524;", "answer-2047;"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("the failure does not name %q", part)
		}
	}
	for _, part := range []string{"answer-0;", "answer-1023;", "answer-2048;"} {
		if strings.Contains(err.Error(), part) {
			t.Errorf("the failure prints %q, an answer outside the first block that differs", part)
		}
	}
	if err := blocksErr("sweep", output, len(lines)-1, fixedLines(lines), describe); err == nil || !strings.Contains(err.Error(), "an input was added or dropped") {
		t.Errorf("a dropped answer: %v", err)
	}
	if err := blocksErr("sweep", "Traceback", len(lines), fixedLines(lines), describe); err == nil {
		t.Error("an output that is not block digests was accepted")
	}
}

// TestAKnownDefectMustBeFoundInItsBlock pins the probe a sweep test runs on
// every pass: a wrong answer makes its block differ; an answer that is the
// right one is not a defect, and the probe says so.
func TestAKnownDefectMustBeFoundInItsBlock(t *testing.T) {
	lines := numberedLines(2*BlockLines + 2)
	encoded, err := json.Marshal(frozenBlocks(lines))
	if err != nil {
		t.Fatal(err)
	}
	output := string(encoded)
	wrong := func(dst []byte) []byte { return append(dst, "wrong"...) }
	for _, index := range []int{0, BlockLines + 500, 2*BlockLines + 1} {
		if err := findsDefectErr("sweep", output, index, fixedLines(lines), wrong); err != nil {
			t.Errorf("a wrong answer %d was not found: %v", index, err)
		}
	}
	right := func(dst []byte) []byte { return append(dst, lines[BlockLines+500]...) }
	if err := findsDefectErr("sweep", output, BlockLines+500, fixedLines(lines), right); err == nil || !strings.Contains(err.Error(), "block 1") {
		t.Errorf("the right answer passed as a defect: %v", err)
	}
	if err := findsDefectErr("sweep", output, len(lines), fixedLines(lines), wrong); err == nil {
		t.Error("a defect outside the sweep was accepted")
	}
}
