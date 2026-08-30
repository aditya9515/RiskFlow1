package decision

import (
	"context"
	"errors"
)

var (
	ErrPaymentNotFound      = errors.New("decision payment not found")
	ErrDecisionConflict     = errors.New("decision identity conflict")
	ErrSourceRecordConflict = errors.New("Kafka source record conflict")
	ErrPaymentStateConflict = errors.New("payment state conflicts with risk decision")
)

// SourceRecord identifies one immutable Kafka record.
type SourceRecord struct {
	Topic       string
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Value       []byte
}

// ApplyResult reports whether this delivery changed domain state.
type ApplyResult struct {
	Applied  bool
	Replayed bool
}

// Store atomically persists accepted or rejected Kafka records.
type Store interface {
	Apply(context.Context, Event, SourceRecord) (ApplyResult, error)
	Reject(context.Context, SourceRecord, string, error) error
}
