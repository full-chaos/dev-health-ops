package affected

import "io"

// Test-only bridges keep the production seam private while external tests share it.
func PlanVerdictFrom(scan func(string) ([]Read, error), args []string, stdout, stderr io.Writer) int {
	return planVerdictFrom(scan, args, stdout, stderr)
}

func ScannedGoSources(root string) ([]string, error) {
	return goSources(root, repoFiles(root))
}
