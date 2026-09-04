package volant_test

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	volant "github.com/volant-mq/volant/clients/go"
	"github.com/volant-mq/volant/clients/go/codec"
	"github.com/volant-mq/volant/clients/go/frame"
)

// Native ErrorCode::Timeout / InvalidTxnState (crates/volant-protocol).
const (
	txnTimeoutCode     uint16 = 7
	txnInvalidTxnState uint16 = 22
)

type txnServerState struct {
	opcodes     []uint16
	initTxnIDs  []string
	beginReqs   []codec.BeginTxnRequest
	produceReqs []codec.ProduceRequest
	endReqs     []codec.EndTxnRequest
	beginCodes  []uint16
	endCodes    []uint16
	err         error
}

func nextTxnCode(codes *[]uint16) uint16 {
	if len(*codes) == 0 {
		return 0
	}
	if len(*codes) == 1 {
		return (*codes)[0]
	}
	c := (*codes)[0]
	*codes = (*codes)[1:]
	return c
}

func serveTxn(t *testing.T, beginError, endError uint16) (addr string, got *txnServerState, stop func()) {
	t.Helper()
	return serveTxnCodes(t, []uint16{beginError}, []uint16{endError})
}

func serveTxnCodes(t *testing.T, beginCodes, endCodes []uint16) (addr string, got *txnServerState, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := &txnServerState{beginCodes: append([]uint16(nil), beginCodes...), endCodes: append([]uint16(nil), endCodes...)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			res.err = err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 0, 4096)
		tmp := make([]byte, 4096)
		for {
			f, rest, err := frame.TryDecode(buf)
			if err != nil {
				res.err = err
				return
			}
			if f == nil {
				n, err := conn.Read(tmp)
				if n > 0 {
					buf = append(buf, tmp[:n]...)
				}
				if err != nil {
					return
				}
				continue
			}
			buf = append([]byte(nil), rest...)
			res.opcodes = append(res.opcodes, f.Opcode)
			payload, replyOp, herr := handleTxn(f, res)
			if herr != nil {
				res.err = herr
				return
			}
			raw, err := frame.Encode(replyOp, f.CorrelationID, payload)
			if err != nil {
				res.err = err
				return
			}
			if _, err := conn.Write(raw); err != nil {
				res.err = err
				return
			}
		}
	}()
	return ln.Addr().String(), res, func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
}

func handleTxn(f *frame.Frame, got *txnServerState) ([]byte, uint16, error) {
	switch f.Opcode {
	case codec.OpInitProducerId:
		req, err := codec.DecodeInitProducerIdRequest(f.Payload)
		if err != nil {
			return nil, 0, err
		}
		got.initTxnIDs = append(got.initTxnIDs, req.TransactionalID)
		payload, err := codec.EncodeInitProducerIdResponse(codec.InitProducerIdResponse{
			ProducerID: 7, Epoch: 0, ErrorCode: 0,
		})
		return payload, codec.OpInitProducerIdResponse, err
	case codec.OpBeginTxn:
		req, err := codec.DecodeBeginTxnRequest(f.Payload)
		if err != nil {
			return nil, 0, err
		}
		got.beginReqs = append(got.beginReqs, req)
		payload, err := codec.EncodeBeginTxnResponse(codec.BeginTxnResponse{ErrorCode: nextTxnCode(&got.beginCodes)})
		return payload, codec.OpBeginTxnResponse, err
	case codec.OpProduce:
		req, err := codec.DecodeProduceRequest(f.Payload)
		if err != nil {
			return nil, 0, err
		}
		got.produceReqs = append(got.produceReqs, req)
		part := req.Partition
		if part < 0 {
			part = 0
		}
		payload, err := codec.EncodeProduceResponse(codec.ProduceResponse{
			Topic: req.Topic, Partition: uint32(part), BaseOffset: 0, Count: uint32(len(req.Messages)), ErrorCode: 0,
		})
		return payload, codec.OpProduce, err
	case codec.OpEndTxn:
		req, err := codec.DecodeEndTxnRequest(f.Payload)
		if err != nil {
			return nil, 0, err
		}
		got.endReqs = append(got.endReqs, req)
		endError := nextTxnCode(&got.endCodes)
		var results []codec.TxnProduceResult
		if req.Committed && endError == 0 {
			results = []codec.TxnProduceResult{{Topic: "t", Partition: 0, BaseOffset: 10, Count: 1}}
		}
		payload, err := codec.EncodeEndTxnResponse(codec.EndTxnResponse{ErrorCode: endError, Results: results})
		return payload, codec.OpEndTxnResponse, err
	default:
		return nil, 0, fmt.Errorf("unexpected opcode %d", f.Opcode)
	}
}

func TestBeginProduceCommit(t *testing.T) {
	addr, got, stop := serveTxn(t, 0, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetTransactionalID("txn-1")
	if err := c.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Produce("t", 0, nil, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	results, err := c.CommitTransaction([]codec.TxnOffsetCommit{
		{GroupID: "g", Topic: "t", Partition: 0, Offset: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.opcodes) != 4 ||
		got.opcodes[0] != codec.OpInitProducerId ||
		got.opcodes[1] != codec.OpBeginTxn ||
		got.opcodes[2] != codec.OpProduce ||
		got.opcodes[3] != codec.OpEndTxn {
		t.Fatalf("opcodes %v", got.opcodes)
	}
	if len(got.initTxnIDs) != 1 || got.initTxnIDs[0] != "txn-1" {
		t.Fatalf("init ids %v", got.initTxnIDs)
	}
	if got.produceReqs[0].ProducerID != 7 || got.produceReqs[0].BaseSequence != 0 {
		t.Fatalf("produce trailer %+v", got.produceReqs[0])
	}
	if !got.endReqs[0].Committed || len(got.endReqs[0].Offsets) != 1 {
		t.Fatalf("end %+v", got.endReqs[0])
	}
	if len(results) != 1 || results[0].BaseOffset != 10 {
		t.Fatalf("results %+v", results)
	}
}

func TestAbortRewindsSequence(t *testing.T) {
	addr, got, stop := serveTxn(t, 0, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetTransactionalID("txn-1")
	if err := c.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Produce("t", 0, nil, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.AbortTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Produce("t", 0, nil, []byte("b")); err != nil {
		t.Fatal(err)
	}
	if len(got.produceReqs) != 2 {
		t.Fatalf("produce count %d", len(got.produceReqs))
	}
	if got.produceReqs[0].BaseSequence != 0 || got.produceReqs[1].BaseSequence != 0 {
		t.Fatalf("seqs %d %d", got.produceReqs[0].BaseSequence, got.produceReqs[1].BaseSequence)
	}
	if got.endReqs[0].Committed {
		t.Fatalf("expected abort")
	}
}

func TestTransactionalIDGetter(t *testing.T) {
	addr, _, stop := serveTxn(t, 0, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := c.TransactionalID(); got != "" {
		t.Fatalf("after DialTimeout TransactionalID() = %q want empty", got)
	}
	c.SetTransactionalID("txn-1")
	if got := c.TransactionalID(); got != "txn-1" {
		t.Fatalf("after SetTransactionalID TransactionalID() = %q want txn-1", got)
	}
	c.SetTransactionalID("")
	if got := c.TransactionalID(); got != "" {
		t.Fatalf("after clear TransactionalID() = %q want empty", got)
	}
}

func TestMissingTransactionalIDErrorsBeforeSend(t *testing.T) {
	addr, got, stop := serveTxn(t, 0, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = c.BeginTransaction()
	if err == nil || !strings.Contains(err.Error(), "transactional_id") {
		t.Fatalf("err %v", err)
	}
	if len(got.opcodes) != 0 {
		t.Fatalf("sent opcodes %v", got.opcodes)
	}
}

func TestBeginTxnError22Raises(t *testing.T) {
	addr, got, stop := serveTxn(t, txnInvalidTxnState, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetTransactionalID("txn-1")
	err = c.BeginTransaction()
	var be *volant.BrokerError
	if !errors.As(err, &be) || be.Code != txnInvalidTxnState || be.Op != "begin_txn" {
		t.Fatalf("err %#v", err)
	}
	if len(got.opcodes) != 2 || got.opcodes[0] != codec.OpInitProducerId || got.opcodes[1] != codec.OpBeginTxn {
		t.Fatalf("opcodes %v", got.opcodes)
	}
}

func TestDefaultMaxRetriesZeroRaisesOnBeginTimeout(t *testing.T) {
	addr, got, stop := serveTxn(t, txnTimeoutCode, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetTransactionalID("txn-1")
	err = c.BeginTransaction()
	var be *volant.BrokerError
	if !errors.As(err, &be) || be.Code != txnTimeoutCode || be.Op != "begin_txn" {
		t.Fatalf("err %#v", err)
	}
	if len(got.beginReqs) != 1 {
		t.Fatalf("begin count %d want 1", len(got.beginReqs))
	}
	if len(got.opcodes) != 2 || got.opcodes[0] != codec.OpInitProducerId || got.opcodes[1] != codec.OpBeginTxn {
		t.Fatalf("opcodes %v", got.opcodes)
	}
}

func TestCommitTransactionEmptyEncodesNoOffsets(t *testing.T) {
	addr, got, stop := serveTxnCodes(t, []uint16{0}, []uint16{0})
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetTransactionalID("txn-1")
	if err := c.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	results, err := c.CommitTransactionEmpty()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.endReqs) != 1 {
		t.Fatalf("end count %d want 1", len(got.endReqs))
	}
	if !got.endReqs[0].Committed || len(got.endReqs[0].Offsets) != 0 {
		t.Fatalf("end %+v", got.endReqs[0])
	}
	if len(results) != 1 || results[0].BaseOffset != 10 {
		t.Fatalf("results %+v", results)
	}
}

func TestEndTxnRetriesTimeoutThenOk(t *testing.T) {
	addr, got, stop := serveTxnCodes(t, []uint16{0}, []uint16{txnTimeoutCode, 0})
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetTransactionalID("txn-1")
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)
	if err := c.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	results, err := c.CommitTransaction(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].BaseOffset != 10 {
		t.Fatalf("results %+v", results)
	}
	if len(got.endReqs) != 2 {
		t.Fatalf("end count %d want 2", len(got.endReqs))
	}
	if !got.endReqs[0].Committed || !got.endReqs[1].Committed {
		t.Fatalf("end %+v", got.endReqs)
	}
}

func TestAbortRetriesTimeoutThenOk(t *testing.T) {
	addr, got, stop := serveTxnCodes(t, []uint16{0}, []uint16{txnTimeoutCode, 0})
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetTransactionalID("txn-1")
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)
	if err := c.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := c.AbortTransaction(); err != nil {
		t.Fatal(err)
	}
	if len(got.endReqs) != 2 {
		t.Fatalf("end count %d want 2", len(got.endReqs))
	}
	if got.endReqs[0].Committed || got.endReqs[1].Committed {
		t.Fatalf("end %+v", got.endReqs)
	}
}

func TestInvalidTxnStateIsNotRetried(t *testing.T) {
	addr, got, stop := serveTxn(t, txnInvalidTxnState, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetTransactionalID("txn-1")
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)
	err = c.BeginTransaction()
	var be *volant.BrokerError
	if !errors.As(err, &be) || be.Code != txnInvalidTxnState || be.Op != "begin_txn" {
		t.Fatalf("err %#v", err)
	}
	if len(got.beginReqs) != 1 {
		t.Fatalf("begin count %d want 1", len(got.beginReqs))
	}
	if len(got.opcodes) != 2 || got.opcodes[0] != codec.OpInitProducerId || got.opcodes[1] != codec.OpBeginTxn {
		t.Fatalf("opcodes %v", got.opcodes)
	}
}

func TestEndTxnExhaustedRetriesRaises(t *testing.T) {
	addr, got, stop := serveTxn(t, 0, txnTimeoutCode)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetTransactionalID("txn-1")
	c.SetMaxRetries(2)
	c.SetRetryBackoff(0)
	if err := c.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	_, err = c.CommitTransaction(nil)
	var be *volant.BrokerError
	if !errors.As(err, &be) || be.Code != txnTimeoutCode || be.Op != "end_txn" {
		t.Fatalf("err %#v", err)
	}
	if len(got.endReqs) != 3 {
		t.Fatalf("end count %d want 3", len(got.endReqs))
	}
}

func TestTransactionalProducerBeginProduceAddOffsetsCommit(t *testing.T) {
	addr, got, stop := serveTxn(t, 0, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetTransactionalID("txn-1")
	p, err := volant.NewTransactionalProducer(c)
	if err != nil {
		t.Fatal(err)
	}
	if p.IsOpen() {
		t.Fatal("expected closed")
	}
	if err := p.Begin(); err != nil {
		t.Fatal(err)
	}
	if !p.IsOpen() {
		t.Fatal("expected open")
	}
	if _, err := p.Produce("t", 0, nil, []byte("x")); err != nil {
		t.Fatal(err)
	}
	p.AddOffsets("g", []volant.TxnOffset{{Topic: "t", Partition: 0, Offset: 1}})
	results, err := p.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if p.IsOpen() {
		t.Fatal("expected closed after commit")
	}
	if len(got.opcodes) != 4 ||
		got.opcodes[0] != codec.OpInitProducerId ||
		got.opcodes[1] != codec.OpBeginTxn ||
		got.opcodes[2] != codec.OpProduce ||
		got.opcodes[3] != codec.OpEndTxn {
		t.Fatalf("opcodes %v", got.opcodes)
	}
	if !got.endReqs[0].Committed || len(got.endReqs[0].Offsets) != 1 {
		t.Fatalf("end %+v", got.endReqs[0])
	}
	off := got.endReqs[0].Offsets[0]
	if off.GroupID != "g" || off.Topic != "t" || off.Partition != 0 || off.Offset != 1 {
		t.Fatalf("offset %+v", off)
	}
	if len(results) != 1 || results[0].BaseOffset != 10 {
		t.Fatalf("results %+v", results)
	}
}

func TestTransactionalProducerAbortClearsQueue(t *testing.T) {
	addr, got, stop := serveTxn(t, 0, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetTransactionalID("txn-1")
	p, err := volant.NewTransactionalProducer(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Begin(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Produce("t", 0, nil, []byte("x")); err != nil {
		t.Fatal(err)
	}
	p.AddOffset("g", "t", 0, 1)
	if err := p.Abort(); err != nil {
		t.Fatal(err)
	}
	if p.IsOpen() {
		t.Fatal("expected closed after abort")
	}
	if err := p.Begin(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	if got.endReqs[0].Committed || len(got.endReqs[0].Offsets) != 0 {
		t.Fatalf("abort end %+v", got.endReqs[0])
	}
	if !got.endReqs[1].Committed || len(got.endReqs[1].Offsets) != 0 {
		t.Fatalf("second commit should not replay aborted offsets: %+v", got.endReqs[1])
	}
}

func TestTransactionalProducerMissingTransactionalID(t *testing.T) {
	addr, got, stop := serveTxn(t, 0, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = volant.NewTransactionalProducer(c)
	if err == nil || !strings.Contains(err.Error(), "transactional_id") {
		t.Fatalf("err %v", err)
	}
	if len(got.opcodes) != 0 {
		t.Fatalf("sent opcodes %v", got.opcodes)
	}
}

func TestTransactionalProducerCommitWhileNotOpen(t *testing.T) {
	addr, got, stop := serveTxn(t, 0, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetTransactionalID("txn-1")
	p, err := volant.NewTransactionalProducer(c)
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Commit()
	if err == nil || !strings.Contains(err.Error(), "not open") {
		t.Fatalf("commit err %v", err)
	}
	err = p.Abort()
	if err == nil || !strings.Contains(err.Error(), "not open") {
		t.Fatalf("abort err %v", err)
	}
	if len(got.opcodes) != 0 {
		t.Fatalf("sent opcodes %v", got.opcodes)
	}
}

func TestTransactionalProducerDoubleBegin(t *testing.T) {
	addr, got, stop := serveTxn(t, 0, 0)
	defer stop()
	c, err := volant.DialTimeout(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetTransactionalID("txn-1")
	p, err := volant.NewTransactionalProducer(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Begin(); err != nil {
		t.Fatal(err)
	}
	err = p.Begin()
	if err == nil || !strings.Contains(err.Error(), "already open") {
		t.Fatalf("err %v", err)
	}
	if len(got.opcodes) != 2 || got.opcodes[0] != codec.OpInitProducerId || got.opcodes[1] != codec.OpBeginTxn {
		t.Fatalf("opcodes %v", got.opcodes)
	}
}
