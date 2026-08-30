package decision

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseEventAcceptsAndNormalizesSchemaV2(t *testing.T) {
	t.Parallel()

	event, err := ParseEvent(validEventValue())
	if err != nil {
		t.Fatalf("parse event: %v", err)
	}
	if event.EventID != "10000000-0000-4000-8000-000000000001" {
		t.Fatalf("event ID = %q", event.EventID)
	}
	if event.Payload.Decision != DecisionReview || event.Payload.Features.Velocity5m != 3 {
		t.Fatalf("decision/features = %q/%d", event.Payload.Decision, event.Payload.Features.Velocity5m)
	}
	if event.OccurredAt.Location() != time.UTC || event.Payload.DecisionAt.Location() != time.UTC {
		t.Fatalf("timestamps were not normalized to UTC")
	}
	fingerprint, err := Fingerprint(event)
	if err != nil || len(fingerprint) != 64 {
		t.Fatalf("fingerprint/error = %q/%v", fingerprint, err)
	}
}

func TestFingerprintUsesNormalizedEventFields(t *testing.T) {
	t.Parallel()

	first, err := ParseEvent(validEventValue())
	if err != nil {
		t.Fatal(err)
	}
	secondValue := strings.NewReplacer(
		"10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000001",
		"2026-08-30T12:00:00Z", "2026-08-30T17:30:00+05:30",
		`"rules-v1"`, `" rules-v1 "`,
	).Replace(string(validEventValue()))
	second, err := ParseEvent([]byte(secondValue))
	if err != nil {
		t.Fatal(err)
	}
	firstFingerprint, _ := Fingerprint(first)
	secondFingerprint, _ := Fingerprint(second)
	if firstFingerprint != secondFingerprint {
		t.Fatalf("normalized fingerprints differ: %s != %s", firstFingerprint, secondFingerprint)
	}
}

func TestParseEventRejectsContractViolations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		replace string
		with    string
	}{
		{name: "unknown field", replace: `"trace_id":`, with: `"unexpected":true,"trace_id":`},
		{name: "wrong schema", replace: `"schema_version":2`, with: `"schema_version":1`},
		{name: "mismatched payment", replace: `"payment_id":"20000000-0000-4000-8000-000000000001"`, with: `"payment_id":"20000000-0000-4000-8000-000000000099"`},
		{name: "bad score", replace: `"risk_score":55`, with: `"risk_score":101`},
		{name: "missing numeric field", replace: `"model_score":6,`, with: ``},
		{name: "null boolean field", replace: `"cross_border":false`, with: `"cross_border":null`},
		{name: "empty reasons", replace: `"reason_codes":["HIGH_AMOUNT","ML_HIGH_RISK"]`, with: `"reason_codes":[]`},
		{name: "trailing value", replace: "}", with: "}{}"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := strings.Replace(string(validEventValue()), test.replace, test.with, 1)
			_, err := ParseEvent([]byte(value))
			if !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("error = %v, want ErrInvalidEvent", err)
			}
		})
	}
}

func validEventValue() []byte {
	return []byte(`{
        "event_id":"10000000-0000-4000-8000-000000000001",
        "event_type":"risk.decision.completed",
        "aggregate_id":"20000000-0000-4000-8000-000000000001",
        "schema_version":2,
        "occurred_at":"2026-08-30T12:00:00Z",
        "trace_id":"30000000-0000-4000-8000-000000000001",
        "payload":{
            "decision_id":"10000000-0000-4000-8000-000000000001",
            "payment_id":"20000000-0000-4000-8000-000000000001",
            "source_event_id":"40000000-0000-4000-8000-000000000001",
            "decision":"REVIEW",
            "risk_score":55,
            "rule_score":55,
            "model_score":6,
            "model_probability":0.056593,
            "model_review_threshold":0.05,
            "reason_codes":["HIGH_AMOUNT","ML_HIGH_RISK"],
            "rule_version":"rules-v1",
            "model_version":"xgb-synthetic-v1",
            "decision_at":"2026-08-30T12:00:00Z",
            "features":{
                "velocity_5m":3,
                "new_device":true,
                "cross_border":false,
                "baseline_country":"IN",
                "decision_at":"2026-08-30T12:00:00Z"
            }
        }
    }`)
}
