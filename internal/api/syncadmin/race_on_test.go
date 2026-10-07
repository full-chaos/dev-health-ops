//go:build race

package syncadmin

// subsetWalkStep: under the race detector the every-subset walk visits one
// subset in 16 (the full walk runs in the plain test leg).
const subsetWalkStep = 16
