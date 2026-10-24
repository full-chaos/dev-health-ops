package emitscan

// SetWorkBudget lets a test shrink the work budget; the returned function restores it.
func SetWorkBudget(base, perNode int) func() {
	oldBase, oldPerNode := workBase, workPerNode
	workBase, workPerNode = base, perNode
	return func() { workBase, workPerNode = oldBase, oldPerNode }
}
