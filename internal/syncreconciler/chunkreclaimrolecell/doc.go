// Package chunkreclaimrolecell runs the chunk sidecar reclaim stage as the
// reconciler's real domain role. It is a separate package so it fits the
// posture integration cap: package syncreconciler itself runs for about 1500 s
// under -tags=integration.
package chunkreclaimrolecell
