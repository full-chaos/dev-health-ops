// Package ownershipvenue holds the venue differential oracle for the
// operational ownership check both customer-push source registration
// (POST /api/v1/admin/customer-push/sources) and batch accept (POST
// /api/v1/external-ingest/batches) run: the real Python api and the real dho
// api answer the same requests over the same managed integrations, credentials
// (readable, unreadable and absent payloads) and sources, and the responses
// are compared byte for byte. It is its own package so the venue run has its
// own time budget.
package ownershipvenue
