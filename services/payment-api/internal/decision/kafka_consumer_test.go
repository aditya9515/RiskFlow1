package decision

import "testing"

func TestKafkaCommitRecordPreservesLeaderEpoch(t *testing.T) {
	t.Parallel()

	source := SourceRecord{
		Topic:       "risk.decisions",
		Partition:   2,
		Offset:      41,
		LeaderEpoch: 7,
	}
	record := kafkaRecord(source)
	if record.Topic != source.Topic || record.Partition != source.Partition || record.Offset != source.Offset {
		t.Fatalf("commit coordinate = %s[%d]@%d", record.Topic, record.Partition, record.Offset)
	}
	if record.LeaderEpoch != source.LeaderEpoch {
		t.Fatalf("leader epoch = %d, want %d", record.LeaderEpoch, source.LeaderEpoch)
	}
}
