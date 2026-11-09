package exceptionledger

// MarkerRow reports a row whose needle carries a marker the scan finds: a TEMPORARY or interim comment, or a Tracked or
// RemovedBy field.
func MarkerRow(r Row) bool {
	return !r.exact() && (commentMarker.MatchString(r.Needle) || fieldMarker.MatchString(r.Needle))
}
