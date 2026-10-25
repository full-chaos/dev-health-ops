//go:build race

package emitscan_test

// raceSlowdown scales the wall-time limits of the cost rows: the race detector slows the scan several times over.
const raceSlowdown = 10
