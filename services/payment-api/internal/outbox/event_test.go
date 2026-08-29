package outbox

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestMarshalEnvelopeIncludesRequiredContract(t *testing.T) {
	t.Parallel()

	event := validEvent()
	encoded, err := marshalEnvelope(event)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	var envelope Envelope
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if envelope.EventID != event.ID || envelope.EventType != event.EventType || envelope.AggregateID != event.AggregateID {
		t.Fatalf("unexpected envelope identifiers: %+v", envelope)
	}
	if envelope.SchemaVersion != 1 || envelope.TraceID != event.TraceID {
		t.Fatalf("unexpected envelope metadata: %+v", envelope)
	}
	if envelope.OccurredAt.Location() != time.UTC {
		t.Fatalf("occurred_at location = %s, want UTC", envelope.OccurredAt.Location())
	}

	var payload map[string]any
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		t.Fatalf("decode nested payload: %v", err)
	}
	if payload["payment_id"] != event.AggregateID {
		t.Fatalf("payload payment_id = %v", payload["payment_id"])
	}
}

func TestMarshalEnvelopeRejectsInvalidEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Event)
	}{
		{name: "missing event ID", mutate: func(event *Event) { event.ID = "" }},
		{name: "missing event type", mutate: func(event *Event) { event.EventType = "" }},
		{name: "invalid schema", mutate: func(event *Event) { event.SchemaVersion = 0 }},
		{name: "missing time", mutate: func(event *Event) { event.OccurredAt = time.Time{} }},
		{name: "array payload", mutate: func(event *Event) { event.Payload = json.RawMessage(`[]`) }},
		{name: "trailing payload", mutate: func(event *Event) { event.Payload = json.RawMessage(`{"payment_id":"x"} {}`) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			event := validEvent()
			tt.mutate(&event)
			if _, err := marshalEnvelope(event); err == nil || !strings.Contains(err.Error(), "invalid outbox event") {
				t.Fatalf("error = %v, want invalid outbox event", err)
			}
		})
	}
}

func validEvent() Event {
	aggregateID := "10000000-0000-4000-8000-000000000001"
	return Event{
		ID:            "20000000-0000-4000-8000-000000000001",
		EventType:     "payments.created",
		AggregateID:   aggregateID,
		SchemaVersion: 1,
		OccurredAt:    time.Date(2026, time.August, 29, 12, 0, 0, 0, time.FixedZone("test", 5*60*60+30*60)),
		TraceID:       "30000000-0000-4000-8000-000000000001",
		Payload:       json.RawMessage(`{"payment_id":"` + aggregateID + `","amount_minor":1250}`),
	}
}
