// Package batchstatusvenue holds the venue differential oracle for the
// external-ingest batch status route (GET /api/v1/external-ingest/batches/{id}):
// the real Python api and the real dho api answer the same requests over the
// same external_ingest_batches rows, whose three JSON columns (record_counts,
// error_summary, recompute_scope) hold every shape the tests draw, and the
// responses are compared byte for byte. It is its own package so the venue run
// has its own time budget.
package batchstatusvenue
