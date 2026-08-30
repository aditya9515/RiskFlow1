package decision

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

const (
	EventTypeRiskDecisionCompleted = "risk.decision.completed"
	DecisionSchemaVersion          = 2
	DecisionAllow                  = "ALLOW"
	DecisionReview                 = "REVIEW"
	DecisionBlock                  = "BLOCK"
)

// ErrInvalidEvent marks an event that cannot satisfy the schema-v2 contract.
var ErrInvalidEvent = errors.New("invalid risk decision event")

// Event is the risk.decisions schema-v2 envelope.
type Event struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	AggregateID   string    `json:"aggregate_id"`
	SchemaVersion int       `json:"schema_version"`
	OccurredAt    time.Time `json:"occurred_at"`
	TraceID       string    `json:"trace_id"`
	Payload       Payload   `json:"payload"`
}

// Payload contains the automated rules-and-model decision evidence.
type Payload struct {
	DecisionID           string          `json:"decision_id"`
	PaymentID            string          `json:"payment_id"`
	SourceEventID        string          `json:"source_event_id"`
	Decision             string          `json:"decision"`
	RiskScore            int             `json:"risk_score"`
	RuleScore            int             `json:"rule_score"`
	ModelScore           int             `json:"model_score"`
	ModelProbability     float64         `json:"model_probability"`
	ModelReviewThreshold float64         `json:"model_review_threshold"`
	ReasonCodes          []string        `json:"reason_codes"`
	RuleVersion          string          `json:"rule_version"`
	ModelVersion         string          `json:"model_version"`
	DecisionAt           time.Time       `json:"decision_at"`
	Features             FeatureSnapshot `json:"features"`
}

// FeatureSnapshot is the immutable online feature state used for a decision.
type FeatureSnapshot struct {
	Velocity5m      int       `json:"velocity_5m"`
	NewDevice       bool      `json:"new_device"`
	CrossBorder     bool      `json:"cross_border"`
	BaselineCountry string    `json:"baseline_country"`
	DecisionAt      time.Time `json:"decision_at"`
}

// ParseEvent strictly decodes and normalizes a schema-v2 risk decision.
func ParseEvent(value []byte) (Event, error) {
	if err := requireEventFields(value); err != nil {
		return Event{}, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}

	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()

	var event Event
	if err := decoder.Decode(&event); err != nil {
		return Event{}, fmt.Errorf("%w: decode: %v", ErrInvalidEvent, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Event{}, fmt.Errorf("%w: expected one JSON object", ErrInvalidEvent)
	}

	if err := normalizeAndValidate(&event); err != nil {
		return Event{}, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	return event, nil
}

func requireEventFields(value []byte) error {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(value, &envelope); err != nil {
		return fmt.Errorf("decode required fields: %w", err)
	}
	if err := requireFields(envelope, "envelope",
		"event_id", "event_type", "aggregate_id", "schema_version", "occurred_at", "trace_id", "payload",
	); err != nil {
		return err
	}

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(envelope["payload"], &payload); err != nil {
		return fmt.Errorf("payload must be an object: %w", err)
	}
	if err := requireFields(payload, "payload",
		"decision_id", "payment_id", "source_event_id", "decision", "risk_score",
		"rule_score", "model_score", "model_probability", "model_review_threshold",
		"reason_codes", "rule_version", "model_version", "decision_at", "features",
	); err != nil {
		return err
	}

	var features map[string]json.RawMessage
	if err := json.Unmarshal(payload["features"], &features); err != nil {
		return fmt.Errorf("payload.features must be an object: %w", err)
	}
	return requireFields(features, "payload.features",
		"velocity_5m", "new_device", "cross_border", "baseline_country", "decision_at",
	)
}

func requireFields(object map[string]json.RawMessage, objectName string, names ...string) error {
	if object == nil {
		return fmt.Errorf("%s must be an object", objectName)
	}
	for _, name := range names {
		value, present := object[name]
		if !present || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("%s.%s is required", objectName, name)
		}
	}
	return nil
}

// Fingerprint returns a stable digest of the validated, normalized event.
func Fingerprint(event Event) (string, error) {
	event.OccurredAt = event.OccurredAt.UTC()
	event.Payload.DecisionAt = event.Payload.DecisionAt.UTC()
	event.Payload.Features.DecisionAt = event.Payload.Features.DecisionAt.UTC()
	encoded, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("encode normalized decision event: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func normalizeAndValidate(event *Event) error {
	var err error
	if event.EventID, err = normalizeUUID(event.EventID); err != nil {
		return fmt.Errorf("event_id: %w", err)
	}
	if event.AggregateID, err = normalizeUUID(event.AggregateID); err != nil {
		return fmt.Errorf("aggregate_id: %w", err)
	}
	if event.TraceID, err = normalizeUUID(event.TraceID); err != nil {
		return fmt.Errorf("trace_id: %w", err)
	}
	if event.Payload.DecisionID, err = normalizeUUID(event.Payload.DecisionID); err != nil {
		return fmt.Errorf("payload.decision_id: %w", err)
	}
	if event.Payload.PaymentID, err = normalizeUUID(event.Payload.PaymentID); err != nil {
		return fmt.Errorf("payload.payment_id: %w", err)
	}
	if event.Payload.SourceEventID, err = normalizeUUID(event.Payload.SourceEventID); err != nil {
		return fmt.Errorf("payload.source_event_id: %w", err)
	}

	event.EventType = strings.TrimSpace(event.EventType)
	event.Payload.Decision = strings.TrimSpace(event.Payload.Decision)
	event.Payload.RuleVersion = strings.TrimSpace(event.Payload.RuleVersion)
	event.Payload.ModelVersion = strings.TrimSpace(event.Payload.ModelVersion)
	event.Payload.Features.BaselineCountry = strings.TrimSpace(event.Payload.Features.BaselineCountry)
	for index := range event.Payload.ReasonCodes {
		event.Payload.ReasonCodes[index] = strings.TrimSpace(event.Payload.ReasonCodes[index])
	}

	switch {
	case event.EventType != EventTypeRiskDecisionCompleted:
		return fmt.Errorf("event_type must be %q", EventTypeRiskDecisionCompleted)
	case event.SchemaVersion != DecisionSchemaVersion:
		return fmt.Errorf("schema_version must be %d", DecisionSchemaVersion)
	case event.EventID != event.Payload.DecisionID:
		return errors.New("event_id must match payload.decision_id")
	case event.AggregateID != event.Payload.PaymentID:
		return errors.New("aggregate_id must match payload.payment_id")
	case event.OccurredAt.IsZero() || event.Payload.DecisionAt.IsZero() || event.Payload.Features.DecisionAt.IsZero():
		return errors.New("decision timestamps are required")
	case !event.OccurredAt.Equal(event.Payload.DecisionAt):
		return errors.New("occurred_at must match payload.decision_at")
	case !event.Payload.DecisionAt.Equal(event.Payload.Features.DecisionAt):
		return errors.New("payload.decision_at must match features.decision_at")
	case !validDecision(event.Payload.Decision):
		return errors.New("decision must be ALLOW, REVIEW, or BLOCK")
	case !validScore(event.Payload.RiskScore) || !validScore(event.Payload.RuleScore) || !validScore(event.Payload.ModelScore):
		return errors.New("risk, rule, and model scores must be between 0 and 100")
	case math.IsNaN(event.Payload.ModelProbability) || math.IsInf(event.Payload.ModelProbability, 0) || event.Payload.ModelProbability < 0 || event.Payload.ModelProbability > 1:
		return errors.New("model_probability must be between 0 and 1")
	case math.IsNaN(event.Payload.ModelReviewThreshold) || math.IsInf(event.Payload.ModelReviewThreshold, 0) || event.Payload.ModelReviewThreshold <= 0 || event.Payload.ModelReviewThreshold >= 1:
		return errors.New("model_review_threshold must be between 0 and 1 exclusively")
	case len(event.Payload.ReasonCodes) == 0:
		return errors.New("reason_codes must contain at least one value")
	case event.Payload.RuleVersion == "" || len(event.Payload.RuleVersion) > 255:
		return errors.New("rule_version must contain 1-255 characters")
	case event.Payload.ModelVersion == "" || len(event.Payload.ModelVersion) > 255:
		return errors.New("model_version must contain 1-255 characters")
	case event.Payload.Features.Velocity5m < 1:
		return errors.New("velocity_5m must be at least 1")
	case !validCountry(event.Payload.Features.BaselineCountry):
		return errors.New("baseline_country must contain two uppercase letters")
	}

	for _, reason := range event.Payload.ReasonCodes {
		if !validReasonCode(reason) {
			return fmt.Errorf("invalid reason_code %q", reason)
		}
	}

	event.OccurredAt = event.OccurredAt.UTC()
	event.Payload.DecisionAt = event.Payload.DecisionAt.UTC()
	event.Payload.Features.DecisionAt = event.Payload.Features.DecisionAt.UTC()
	return nil
}

func normalizeUUID(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) != 36 || raw[8] != '-' || raw[13] != '-' || raw[18] != '-' || raw[23] != '-' {
		return "", errors.New("must be a UUID")
	}
	compact := strings.NewReplacer("-", "").Replace(raw)
	if _, err := hex.DecodeString(compact); err != nil {
		return "", errors.New("must be a UUID")
	}
	return strings.ToLower(raw), nil
}

func validDecision(value string) bool {
	return value == DecisionAllow || value == DecisionReview || value == DecisionBlock
}

func validScore(value int) bool {
	return value >= 0 && value <= 100
}

func validCountry(value string) bool {
	return len(value) == 2 && value[0] >= 'A' && value[0] <= 'Z' && value[1] >= 'A' && value[1] <= 'Z'
}

func validReasonCode(value string) bool {
	if value == "" || len(value) > 100 {
		return false
	}
	for _, character := range value {
		if (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '_' {
			continue
		}
		return false
	}
	return true
}
