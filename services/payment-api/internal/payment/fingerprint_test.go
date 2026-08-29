package payment

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestFingerprintUsesDeterministicNormalizedFieldOrder(t *testing.T) {
	t.Parallel()

	request := CreateRequest{
		CustomerID:  "cust-123",
		MerchantID:  "merchant-9",
		DeviceID:    "device-7",
		AmountMinor: 1250,
		Currency:    "USD",
		Country:     "IN",
	}

	wantCanonical := "fingerprint_version:1:1\n" +
		"customer_id:8:cust-123\n" +
		"merchant_id:10:merchant-9\n" +
		"device_id:8:device-7\n" +
		"amount_minor:4:1250\n" +
		"currency:3:USD\n" +
		"country:2:IN\n"
	if canonical := canonicalFingerprintInput(request); canonical != wantCanonical {
		t.Fatalf("canonical input = %q, want %q", canonical, wantCanonical)
	}

	digest := sha256.Sum256([]byte(wantCanonical))
	wantFingerprint := hex.EncodeToString(digest[:])
	if fingerprint := Fingerprint(request); fingerprint != wantFingerprint {
		t.Fatalf("fingerprint = %q, want %q", fingerprint, wantFingerprint)
	}
}

func TestEquivalentRawRequestsProduceSameFingerprintAfterNormalization(t *testing.T) {
	t.Parallel()

	first, err := NormalizeAndValidate(CreateRequest{
		CustomerID: "customer-1", MerchantID: "merchant-1", DeviceID: "device-1",
		AmountMinor: 500, Currency: "usd", Country: "in",
	})
	if err != nil {
		t.Fatalf("normalize first request: %v", err)
	}
	second, err := NormalizeAndValidate(CreateRequest{
		CustomerID: " customer-1 ", MerchantID: "merchant-1 ", DeviceID: " device-1",
		AmountMinor: 500, Currency: " USD ", Country: "IN",
	})
	if err != nil {
		t.Fatalf("normalize second request: %v", err)
	}

	if Fingerprint(first) != Fingerprint(second) {
		t.Fatal("equivalent normalized requests produced different fingerprints")
	}

	second.AmountMinor++
	if Fingerprint(first) == Fingerprint(second) {
		t.Fatal("different normalized requests produced the same fingerprint")
	}
}
