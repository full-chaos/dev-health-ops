// Package domainrolecell runs the daily metrics store as the worker's real
// domain role. It is a separate package so it fits the posture integration cap:
// package daily itself runs for about 2400 s under -tags=integration.
package domainrolecell
