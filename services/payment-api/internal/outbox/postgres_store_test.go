package outbox

import (
	"testing"
	"time"
)

func TestDeliveryBackoffIsExponentialAndCapped(t *testing.T) {
	t.Parallel()

	minimum := 100 * time.Millisecond
	maximum := 500 * time.Millisecond
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 100 * time.Millisecond},
		{attempt: 2, want: 200 * time.Millisecond},
		{attempt: 3, want: 400 * time.Millisecond},
		{attempt: 4, want: 500 * time.Millisecond},
		{attempt: 1000, want: 500 * time.Millisecond},
	}

	for _, tt := range tests {
		if got := deliveryBackoff(tt.attempt, minimum, maximum); got != tt.want {
			t.Errorf("attempt %d delay = %s, want %s", tt.attempt, got, tt.want)
		}
	}
}
