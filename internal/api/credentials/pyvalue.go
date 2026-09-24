package credentials

import (
	"strconv"
	"strings"
)

func itoa(n int) string { return strconv.Itoa(n) }

func join(parts []string) string { return strings.Join(parts, ", ") }
