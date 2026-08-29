package outbox

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaPublisher synchronously waits for Kafka acknowledgements.
type KafkaPublisher struct {
	client *kgo.Client
}

// NewKafkaPublisher creates a producer configured to require acknowledgements
// from all in-sync replicas.
func NewKafkaPublisher(brokers []string) (*KafkaPublisher, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one Kafka broker is required")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("riskflow-outbox-publisher"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}

	return &KafkaPublisher{client: client}, nil
}

// Publish sends one record and waits for Kafka to acknowledge it.
func (p *KafkaPublisher) Publish(ctx context.Context, message Message) error {
	record := &kgo.Record{
		Topic: message.Topic,
		Key:   []byte(message.Key),
		Value: message.Value,
	}
	for _, header := range message.Headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{
			Key:   header.Key,
			Value: []byte(header.Value),
		})
	}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce Kafka record: %w", err)
	}
	return nil
}

// Close releases the Kafka client's resources after in-flight synchronous
// publication has completed or been cancelled.
func (p *KafkaPublisher) Close() {
	p.client.Close()
}
