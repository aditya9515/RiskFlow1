package payment

import (
	"encoding/hex"
	"strings"
)

// NormalizeID validates the canonical UUID representation used by the API and
// normalizes hexadecimal letters to lowercase before querying PostgreSQL.
func NormalizeID(raw string) (string, error) {
	if len(raw) != 36 || raw[8] != '-' || raw[13] != '-' || raw[18] != '-' || raw[23] != '-' {
		return "", ErrPaymentIDInvalid
	}

	compact := strings.NewReplacer("-", "").Replace(raw)
	if len(compact) != 32 {
		return "", ErrPaymentIDInvalid
	}
	if _, err := hex.DecodeString(compact); err != nil {
		return "", ErrPaymentIDInvalid
	}

	return strings.ToLower(raw), nil
}
