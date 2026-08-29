package payment

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

// Fingerprint hashes normalized, validated payment fields in an explicit,
// versioned order. It never hashes the caller's raw JSON representation.
func Fingerprint(normalized CreateRequest) string {
	canonical := canonicalFingerprintInput(normalized)
	digest := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(digest[:])
}

func canonicalFingerprintInput(request CreateRequest) string {
	var builder strings.Builder
	writeFingerprintField(&builder, "fingerprint_version", "1")
	writeFingerprintField(&builder, "customer_id", request.CustomerID)
	writeFingerprintField(&builder, "merchant_id", request.MerchantID)
	writeFingerprintField(&builder, "device_id", request.DeviceID)
	writeFingerprintField(&builder, "amount_minor", strconv.FormatInt(request.AmountMinor, 10))
	writeFingerprintField(&builder, "currency", request.Currency)
	writeFingerprintField(&builder, "country", request.Country)
	return builder.String()
}

func writeFingerprintField(builder *strings.Builder, name, value string) {
	builder.WriteString(name)
	builder.WriteByte(':')
	builder.WriteString(strconv.Itoa(len([]byte(value))))
	builder.WriteByte(':')
	builder.WriteString(value)
	builder.WriteByte('\n')
}
