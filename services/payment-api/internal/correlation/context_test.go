package correlation

import (
	"context"
	"testing"
)

func TestWithRequestIDAcceptsOnlyNormalizedUUIDv4(t *testing.T) {
	t.Parallel()

	ctx := WithRequestID(context.Background(), " 10000000-0000-4000-8000-00000000000A ")
	if got := RequestID(ctx); got != "10000000-0000-4000-8000-00000000000a" {
		t.Fatalf("request ID = %q", got)
	}
	if got := RequestID(WithRequestID(context.Background(), "not-a-uuid")); got != "" {
		t.Fatalf("invalid request ID was stored: %q", got)
	}
}
