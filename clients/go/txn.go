package volant

import (
	"fmt"

	"github.com/volant-mq/volant/clients/go/codec"
)

// TxnOffset is one (topic, partition, offset) triple queued by
// [TransactionalProducer.AddOffsets]. Sent on Commit as a
// [codec.TxnOffsetCommit] (empty metadata).
type TxnOffset struct {
	Topic     string
	Partition uint32
	Offset    uint64
}

// Re-exported EndTxn row types (v0.57).
type (
	TxnOffsetCommit  = codec.TxnOffsetCommit
	TxnProduceResult = codec.TxnProduceResult
)

// TransactionalProducer is a thin wrapper around Client BeginTxn / EndTxn
// (v0.63). Matches crates/volant-client/src/txn.rs. Native opcodes 50–53
// only; not Kafka transactions. Produce is write-through.
type TransactionalProducer struct {
	client  *Client
	pending []codec.TxnOffsetCommit
	open    bool
}

// NewTransactionalProducer wraps a Client that already has a non-empty
// transactional_id (same check as BeginTransaction).
func NewTransactionalProducer(c *Client) (*TransactionalProducer, error) {
	if c == nil || c.transactionalID == "" {
		return nil, fmt.Errorf("transactional_id not configured")
	}
	return &TransactionalProducer{client: c}, nil
}

// Client returns the underlying Client.
func (p *TransactionalProducer) Client() *Client {
	return p.client
}

// Begin opens a native transaction. Double-begin while open is an error.
func (p *TransactionalProducer) Begin() error {
	if p.open {
		return fmt.Errorf("transaction already open")
	}
	if err := p.client.BeginTransaction(); err != nil {
		return err
	}
	p.pending = p.pending[:0]
	p.open = true
	return nil
}

// Produce delegates to [Client.Produce] (write-through).
func (p *TransactionalProducer) Produce(topic string, partition int, key, value []byte) (int64, error) {
	return p.client.Produce(topic, partition, key, value)
}

// AddOffsets queues group offsets locally. Nothing is sent until Commit.
func (p *TransactionalProducer) AddOffsets(groupID string, offsets []TxnOffset) {
	for _, o := range offsets {
		p.pending = append(p.pending, codec.TxnOffsetCommit{
			GroupID:   groupID,
			Topic:     o.Topic,
			Partition: o.Partition,
			Offset:    o.Offset,
		})
	}
}

// AddOffset queues a single topic/partition/offset triple.
func (p *TransactionalProducer) AddOffset(groupID, topic string, partition uint32, offset uint64) {
	p.AddOffsets(groupID, []TxnOffset{{Topic: topic, Partition: partition, Offset: offset}})
}

// Commit sends EndTxn committed=1 with the queued offsets.
func (p *TransactionalProducer) Commit() ([]codec.TxnProduceResult, error) {
	if !p.open {
		return nil, fmt.Errorf("transaction is not open")
	}
	offsets := p.pending
	p.pending = nil
	results, err := p.client.CommitTransaction(offsets)
	if err != nil {
		return nil, err
	}
	p.open = false
	return results, nil
}

// Abort clears the offset queue and sends EndTxn committed=0.
func (p *TransactionalProducer) Abort() error {
	if !p.open {
		return fmt.Errorf("transaction is not open")
	}
	p.pending = nil
	if err := p.client.AbortTransaction(); err != nil {
		return err
	}
	p.open = false
	return nil
}

// IsOpen reports whether a transaction is open locally.
func (p *TransactionalProducer) IsOpen() bool {
	return p.open
}
