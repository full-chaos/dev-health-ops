package apiservice

// Response model routes of the billing route family (CHAOS-6722): moved from
// the shared table in responsemodel.go so a PR that adds a route here no
// longer conflicts with every other route PR. See responsemodel_register.go.
func init() {
	registerResponseModelRoutes(map[string]bool{
		"DELETE /api/v1/billing/plans/{plan_id}":           true,
		"GET /api/v1/billing/audit":                        true,
		"GET /api/v1/billing/audit/{audit_id}":             true,
		"GET /api/v1/billing/entitlements/{org_id}":        true,
		"GET /api/v1/billing/invoices":                     true,
		"GET /api/v1/billing/invoices/{invoice_id}":        true,
		"GET /api/v1/billing/plans":                        true,
		"GET /api/v1/billing/plans/{plan_id}":              true,
		"GET /api/v1/billing/refunds":                      true,
		"GET /api/v1/billing/refunds/{refund_id}":          true,
		"GET /api/v1/billing/subscriptions":                true,
		"GET /api/v1/billing/subscriptions/history":        true,
		"GET /api/v1/billing/subscriptions/list":           true,
		"POST /api/v1/billing/audit/{audit_id}/resolve":    true,
		"POST /api/v1/billing/checkout":                    true,
		"POST /api/v1/billing/invoices/{invoice_id}/void":  true,
		"POST /api/v1/billing/plans":                       true,
		"POST /api/v1/billing/plans/pull-stripe":           true,
		"POST /api/v1/billing/plans/{plan_id}/sync-stripe": true,
		"POST /api/v1/billing/portal":                      true,
		"POST /api/v1/billing/reconcile":                   true,
		"POST /api/v1/billing/refunds":                     true,
		"POST /api/v1/billing/subscriptions/cancel":        true,
		"POST /api/v1/billing/subscriptions/change-plan":   true,
		"POST /api/v1/billing/webhooks/stripe":             true,
		"POST /api/v1/billing/subscriptions/reactivate":    true,
		"PUT /api/v1/billing/plans/{plan_id}":              true,
	})
}
