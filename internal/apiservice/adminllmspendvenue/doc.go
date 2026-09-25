// Package adminllmspendvenue holds the venue differential oracle for the admin
// LLM spend read (GET /api/v1/admin/llm-settings/spend): the real Python api
// and the real dho api answer the same requests over the same ClickHouse rows,
// and the response text is compared byte for byte. It is its own package so
// the venue run has its own time budget.
package adminllmspendvenue
