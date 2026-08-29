package payment

import (
	"errors"
	"testing"
)

func TestNormalizeID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr error
	}{
		{
			name: "canonical lowercase",
			raw:  "10000000-0000-4000-8000-000000000001",
			want: "10000000-0000-4000-8000-000000000001",
		},
		{
			name: "uppercase hexadecimal",
			raw:  "ABCDEF00-0000-4000-8000-00000000000A",
			want: "abcdef00-0000-4000-8000-00000000000a",
		},
		{name: "empty", raw: "", wantErr: ErrPaymentIDInvalid},
		{name: "missing hyphens", raw: "10000000000040008000000000000001", wantErr: ErrPaymentIDInvalid},
		{name: "wrong hyphen positions", raw: "1000000-00000-4000-8000-000000000001", wantErr: ErrPaymentIDInvalid},
		{name: "non hexadecimal", raw: "not-a-uuid-0000-4000-8000-000000000001", wantErr: ErrPaymentIDInvalid},
		{name: "leading whitespace", raw: " 10000000-0000-4000-8000-000000000001", wantErr: ErrPaymentIDInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := NormalizeID(tt.raw)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("normalized ID = %q, want %q", got, tt.want)
			}
		})
	}
}
