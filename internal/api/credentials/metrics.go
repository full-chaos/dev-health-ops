package credentials

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// decryptFailedName is the Python api's INTEGRATION_CREDENTIAL_DECRYPT_FAILED_TOTAL
// (metrics/prometheus.py), which IntegrationCredentialsService.
// get_decrypted_credentials_by_id_with_outcome increments when a stored
// payload exists but decrypt_value/json.loads raises.
const decryptFailedName = "devhealth_integration_credential_decrypt_failed_total"

var decryptFailed = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/api/credentials").Int64Counter(
		decryptFailedName,
		metric.WithDescription("Stored integration credential rows whose credentials_encrypted payload existed but could not be decrypted and parsed when read."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(decryptFailedName)
	}
	return counter
}()

// RecordDecryptFailed counts one unreadable stored credential (exported: the one implementation every reader of a stored credential payload calls), labelled as
// Python labels it: provider=sanitize_for_log(provider).
func RecordDecryptFailed(ctx context.Context, provider string) {
	decryptFailed.Add(ctx, 1, metric.WithAttributes(attribute.String("provider", sanitizeForLog(provider))))
}

// sanitizeForLogMaxLength is sanitize_for_log's default max_length.
const sanitizeForLogMaxLength = 1000

// sanitizeForLog is api/utils/logging.py's sanitize_for_log for a str:
// CRLF, CR and LF each become a space, then every character below U+0020
// and U+007F is dropped, and a result longer than 1000 characters is cut
// to 1000 with "...[truncated]" appended.
func sanitizeForLog(text string) string {
	cleaned := strings.NewReplacer("\r\n", " ", "\r", " ", "\n", " ").Replace(text)
	runes := make([]rune, 0, len(cleaned))
	for _, r := range cleaned {
		if r >= ' ' && r != 0x7f {
			runes = append(runes, r)
		}
	}
	if len(runes) > sanitizeForLogMaxLength {
		return string(runes[:sanitizeForLogMaxLength]) + "...[truncated]"
	}
	return string(runes)
}
