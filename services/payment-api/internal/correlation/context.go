package correlation

import (
	"context"
	"strings"
)

type requestIDKey struct{}

// RequestID returns the validated UUIDv4 attached to a request context.
func RequestID(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDKey{}).(string)
	return requestID
}

// WithRequestID attaches a valid, normalized UUIDv4. Invalid values are
// ignored so downstream event contracts cannot be polluted.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	requestID, valid := NormalizeRequestID(requestID)
	if !valid {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey{}, requestID)
}

// NormalizeRequestID validates the UUID version and RFC 4122 variant used by
// RiskFlow correlation identifiers.
func NormalizeRequestID(value string) (string, bool) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return "", false
	}
	for index, character := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return "", false
		}
	}
	if value[14] != '4' || (value[19] != '8' && value[19] != '9' && value[19] != 'a' && value[19] != 'b') {
		return "", false
	}
	return value, true
}
