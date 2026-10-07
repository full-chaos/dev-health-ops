package decision

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var blockHeader = regexp.MustCompile(`^\[(issue|pr|commit)\] (E[0-9]+)$`)

// ParsedBlock is one `[type] E<n>` block of a SourceBlock.
type ParsedBlock struct {
	SourceType string
	Handle     string
	Text       string
}

// ParseSourceBlock splits a SourceBlock into its blocks. Source texts are
// whitespace-collapsed by the producer (units.BuildTextBundle), so a text
// holds no newline and the blocks are separated by exactly one blank line.
func ParseSourceBlock(sourceBlock string) ([]ParsedBlock, error) {
	if sourceBlock == "" {
		return nil, fmt.Errorf("source block is empty")
	}
	var out []ParsedBlock
	for i, part := range strings.Split(sourceBlock, "\n\n") {
		header, text, ok := strings.Cut(part, "\n")
		m := blockHeader.FindStringSubmatch(header)
		if !ok || m == nil {
			return nil, fmt.Errorf("source block part %d has no `[type] E<n>` header line", i)
		}
		if strings.ContainsRune(text, '\n') || text == "" {
			return nil, fmt.Errorf("source block part %d (%s) text is empty or holds a newline", i, m[2])
		}
		out = append(out, ParsedBlock{SourceType: m[1], Handle: m[2], Text: text})
	}
	return out, nil
}

func handleNumber(handle string) int {
	n, err := strconv.Atoi(strings.TrimPrefix(handle, "E"))
	if err != nil {
		return 1 << 30
	}
	return n
}
