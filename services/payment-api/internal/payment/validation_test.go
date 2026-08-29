package payment

import (
	"errors"
	"testing"
)

func TestNormalizeAndValidate(t *testing.T) {
	t.Parallel()

	normalized, err := NormalizeAndValidate(CreateRequest{
		CustomerID:  "  customer-1  ",
		MerchantID:  " merchant-1 ",
		DeviceID:    " device-1 ",
		AmountMinor: 1250,
		Currency:    " usd ",
		Country:     " in ",
	})
	if err != nil {
		t.Fatalf("normalize valid request: %v", err)
	}

	if normalized.CustomerID != "customer-1" || normalized.MerchantID != "merchant-1" || normalized.DeviceID != "device-1" {
		t.Fatalf("identifiers were not trimmed: %+v", normalized)
	}
	if normalized.Currency != "USD" || normalized.Country != "IN" {
		t.Fatalf("codes were not normalized: %+v", normalized)
	}
}

func TestNormalizeAndValidateReportsFields(t *testing.T) {
	t.Parallel()

	_, err := NormalizeAndValidate(CreateRequest{
		CustomerID:  " ",
		MerchantID:  "merchant-1",
		DeviceID:    "",
		AmountMinor: 0,
		Currency:    "US",
		Country:     "123",
	})

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("error = %v, want ValidationError", err)
	}
	for _, field := range []string{"customer_id", "device_id", "amount_minor", "currency", "country"} {
		if validationErr.Fields[field] == "" {
			t.Errorf("missing validation message for %s", field)
		}
	}
}

func TestNormalizeIdempotencyKey(t *testing.T) {
	t.Parallel()

	key, err := normalizeIdempotencyKey("  checkout-123  ")
	if err != nil {
		t.Fatalf("normalize key: %v", err)
	}
	if key != "checkout-123" {
		t.Fatalf("key = %q, want checkout-123", key)
	}

	if _, err := normalizeIdempotencyKey(" "); !errors.Is(err, ErrIdempotencyKeyRequired) {
		t.Fatalf("blank key error = %v, want required", err)
	}
}
