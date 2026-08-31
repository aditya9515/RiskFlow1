package decision

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaConsumer is a manually committed consumer-group member.
type KafkaConsumer struct {
	client *kgo.Client
}

func NewKafkaConsumer(brokers []string, topic, group, autoOffsetReset string) (*KafkaConsumer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	if topic == "" || group == "" {
		return nil, errors.New("Kafka decision topic and consumer group are required")
	}
	resetOffset := kgo.NewOffset().AtStart()
	if autoOffsetReset == "latest" {
		resetOffset = kgo.NewOffset().AtEnd()
	} else if autoOffsetReset != "earliest" {
		return nil, errors.New("Kafka auto offset reset must be earliest or latest")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("riskflow-risk-decision-consumer"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(resetOffset),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka decision consumer: %w", err)
	}
	return &KafkaConsumer{client: client}, nil
}

func (c *KafkaConsumer) Poll(ctx context.Context) (SourceRecord, error) {
	for {
		fetches := c.client.PollRecords(ctx, 1)
		var source *SourceRecord
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			if source != nil || len(partition.Records) == 0 {
				return
			}
			record := partition.Records[0]
			lag := partition.HighWatermark - record.Offset - 1
			if lag < 0 {
				lag = 0
			}
			source = &SourceRecord{
				Topic:       record.Topic,
				Partition:   record.Partition,
				Offset:      record.Offset,
				LeaderEpoch: record.LeaderEpoch,
				Lag:         lag,
				Value:       record.Value,
			}
		})
		if source != nil {
			return *source, nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return SourceRecord{}, fmt.Errorf("fetch Kafka decision: %w", errs[0].Err)
		}
		if ctx.Err() != nil {
			return SourceRecord{}, ctx.Err()
		}
		// Group assignment changes may produce an empty fetch. Poll again
		// without treating normal coordination as a broker failure.
	}
}

func (c *KafkaConsumer) Commit(ctx context.Context, record SourceRecord) error {
	return c.client.CommitRecords(ctx, kafkaRecord(record))
}

func kafkaRecord(record SourceRecord) *kgo.Record {
	return &kgo.Record{
		Topic:       record.Topic,
		Partition:   record.Partition,
		Offset:      record.Offset,
		LeaderEpoch: record.LeaderEpoch,
	}
}

func (c *KafkaConsumer) AllowRebalance() {
	c.client.AllowRebalance()
}

func (c *KafkaConsumer) Close() {
	c.client.Close()
}
