package payment

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// NormalizeAndValidate trims identifiers, uppercases codes, and validates the
// exact values that will be persisted and fingerprinted.
func NormalizeAndValidate(request CreateRequest) (CreateRequest, error) {
	normalized := CreateRequest{
		CustomerID:  strings.TrimSpace(request.CustomerID),
		MerchantID:  strings.TrimSpace(request.MerchantID),
		DeviceID:    strings.TrimSpace(request.DeviceID),
		AmountMinor: request.AmountMinor,
		Currency:    strings.ToUpper(strings.TrimSpace(request.Currency)),
		Country:     strings.ToUpper(strings.TrimSpace(request.Country)),
	}

	fields := make(map[string]string)
	validateIdentifier(fields, "customer_id", normalized.CustomerID)
	validateIdentifier(fields, "merchant_id", normalized.MerchantID)
	validateIdentifier(fields, "device_id", normalized.DeviceID)

	if normalized.AmountMinor <= 0 {
		fields["amount_minor"] = "must be greater than zero"
	}
	if !isUpperASCII(normalized.Currency, 3) {
		fields["currency"] = "must be a three-letter currency code"
	}
	if !isUpperASCII(normalized.Country, 2) {
		fields["country"] = "must be a two-letter country code"
	}

	if len(fields) > 0 {
		return CreateRequest{}, &ValidationError{Fields: fields}
	}

	return normalized, nil
}

func normalizeIdempotencyKey(key string) (string, error) {
	normalized := strings.TrimSpace(key)
	if normalized == "" {
		return "", ErrIdempotencyKeyRequired
	}
	if utf8.RuneCountInString(normalized) > 255 {
		return "", ErrIdempotencyKeyInvalid
	}
	for _, character := range normalized {
		if unicode.IsControl(character) {
			return "", ErrIdempotencyKeyInvalid
		}
	}

	return normalized, nil
}

func validateIdentifier(fields map[string]string, name, value string) {
	if value == "" {
		fields[name] = "is required"
		return
	}
	if utf8.RuneCountInString(value) > 255 {
		fields[name] = "must be at most 255 characters"
	}
}

func isUpperASCII(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, character := range value {
		if character < 'A' || character > 'Z' {
			return false
		}
	}

	return true
}
