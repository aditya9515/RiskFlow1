package outbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// Event is the durable PostgreSQL representation of an integration event.
type Event struct {
	ID            string
	EventType     string
	AggregateID   string
	SchemaVersion int
	OccurredAt    time.Time
	TraceID       string
	Payload       json.RawMessage
}

// Envelope is the stable message contract written to Kafka.
type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	AggregateID   string          `json:"aggregate_id"`
	SchemaVersion int             `json:"schema_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	TraceID       string          `json:"trace_id"`
	Payload       json.RawMessage `json:"payload"`
}

func marshalEnvelope(event Event) ([]byte, error) {
	if err := validateEvent(event); err != nil {
		return nil, err
	}

	encoded, err := json.Marshal(Envelope{
		EventID:       event.ID,
		EventType:     event.EventType,
		AggregateID:   event.AggregateID,
		SchemaVersion: event.SchemaVersion,
		OccurredAt:    event.OccurredAt.UTC(),
		TraceID:       event.TraceID,
		Payload:       event.Payload,
	})
	if err != nil {
		return nil, fmt.Errorf("encode event envelope: %w", err)
	}

	return encoded, nil
}

func validateEvent(event Event) error {
	switch {
	case strings.TrimSpace(event.ID) == "":
		return fmt.Errorf("invalid outbox event: event_id is required")
	case strings.TrimSpace(event.EventType) == "":
		return fmt.Errorf("invalid outbox event %s: event_type is required", event.ID)
	case strings.TrimSpace(event.AggregateID) == "":
		return fmt.Errorf("invalid outbox event %s: aggregate_id is required", event.ID)
	case event.SchemaVersion < 1:
		return fmt.Errorf("invalid outbox event %s: schema_version must be positive", event.ID)
	case event.OccurredAt.IsZero():
		return fmt.Errorf("invalid outbox event %s: occurred_at is required", event.ID)
	case strings.TrimSpace(event.TraceID) == "":
		return fmt.Errorf("invalid outbox event %s: trace_id is required", event.ID)
	}

	decoder := json.NewDecoder(bytes.NewReader(event.Payload))
	var payload map[string]json.RawMessage
	if err := decoder.Decode(&payload); err != nil || payload == nil {
		return fmt.Errorf("invalid outbox event %s: payload must be a JSON object", event.ID)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("invalid outbox event %s: payload must contain one JSON object", event.ID)
	}

	return nil
}
