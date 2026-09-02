package codec

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestProduceValueOnlyExactBytes(t *testing.T) {
	req := ProduceRequest{
		Topic:     "t",
		Partition: 0,
		Acks:      1,
		Messages: []ProduceMessage{
			{Key: nil, Value: []byte("v"), TimestampMs: -1},
		},
		ProducerID:    0,
		ProducerEpoch: 0,
		BaseSequence:  -1,
	}
	raw, err := EncodeProduceRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t,
		"0100"+ // string len 1
			"74"+ // 't'
			"00000000"+ // partition 0 i32
			"01"+ // acks
			"01000000"+ // 1 message
			"ffffffff"+ // null key
			"01000000"+ // value len 1
			"76"+ // 'v'
			"ffffffffffffffff"+ // timestamp -1
			"00000000"+ // 0 headers
			"0000000000000000"+ // producer_id
			"0000"+ // epoch
			"ffffffff", // base_sequence -1
	)
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeProduceRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Topic != req.Topic || decoded.Partition != req.Partition || decoded.Acks != req.Acks {
		t.Fatalf("decoded header %+v", decoded)
	}
	if decoded.ProducerID != 0 || decoded.ProducerEpoch != 0 || decoded.BaseSequence != -1 {
		t.Fatalf("trailer %+v", decoded)
	}
	if len(decoded.Messages) != 1 || decoded.Messages[0].Key != nil || string(decoded.Messages[0].Value) != "v" {
		t.Fatalf("messages %+v", decoded.Messages)
	}
}

func TestProduceKeyedWithHeaders(t *testing.T) {
	req := ProduceRequest{
		Topic:     "events",
		Partition: -1,
		Acks:      1,
		Messages: []ProduceMessage{
			{
				Key:         []byte("k"),
				Value:       []byte("v"),
				TimestampMs: -1,
				Headers:     []Header{{Name: "h", Value: []byte("hv")}},
			},
		},
		ProducerID:    0,
		ProducerEpoch: 0,
		BaseSequence:  -1,
	}
	raw, err := EncodeProduceRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t,
		"0600"+
			"6576656e7473"+
			"ffffffff"+
			"01"+
			"01000000"+
			"01000000"+
			"6b"+
			"01000000"+
			"76"+
			"ffffffffffffffff"+
			"01000000"+
			"0100"+
			"68"+
			"02000000"+
			"6876"+
			"0000000000000000"+
			"0000"+
			"ffffffff",
	)
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeProduceRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Topic != "events" || decoded.Partition != -1 {
		t.Fatalf("decoded %+v", decoded)
	}
	if string(decoded.Messages[0].Key) != "k" || string(decoded.Messages[0].Headers[0].Value) != "hv" {
		t.Fatalf("msg %+v", decoded.Messages[0])
	}
}

func TestProduceLegacyWithoutTrailer(t *testing.T) {
	raw := mustHex(t,
		"010074"+
			"00000000"+
			"01"+
			"01000000"+
			"ffffffff"+
			"0100000076"+
			"ffffffffffffffff"+
			"00000000",
	)
	decoded, err := DecodeProduceRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ProducerID != 0 || decoded.ProducerEpoch != 0 || decoded.BaseSequence != -1 {
		t.Fatalf("trailer %+v", decoded)
	}
	if string(decoded.Messages[0].Value) != "v" || decoded.Messages[0].Key != nil {
		t.Fatalf("msg %+v", decoded.Messages[0])
	}
}

func TestProduceResponseRoundtrip(t *testing.T) {
	resp := ProduceResponse{Topic: "t", Partition: 0, BaseOffset: 0, Count: 1, ErrorCode: 0}
	raw, err := EncodeProduceResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "010074"+"00000000"+"0000000000000000"+"01000000"+"0000")
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeProduceResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != resp {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestFetchRequestExactBytes(t *testing.T) {
	req := FetchRequest{
		Topic:       "t",
		Partition:   0,
		FromOffset:  0,
		MaxMessages: 10,
		MaxBytes:    4096,
		MaxWaitMs:   0,
	}
	raw, err := EncodeFetchRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t,
		"010074"+
			"00000000"+
			"0000000000000000"+
			"0a000000"+
			"00100000"+
			"00000000",
	)
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeFetchRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != req {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestFetchResponseNullKey(t *testing.T) {
	resp := FetchResponse{
		Topic:         "t",
		Partition:     0,
		HighWatermark: 1,
		ErrorCode:     0,
		Records: []FetchRecord{
			{Offset: 0, TimestampMs: -1, Key: nil, Value: []byte("hello"), Headers: []Header{}},
		},
	}
	raw, err := EncodeFetchResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t,
		"010074"+
			"00000000"+
			"0100000000000000"+
			"0000"+
			"01000000"+
			"0000000000000000"+
			"ffffffffffffffff"+
			"ffffffff"+
			"05000000"+
			"68656c6c6f"+
			"00000000",
	)
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeFetchResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Topic != "t" || decoded.HighWatermark != 1 || len(decoded.Records) != 1 {
		t.Fatalf("decoded %+v", decoded)
	}
	if decoded.Records[0].Key != nil || string(decoded.Records[0].Value) != "hello" {
		t.Fatalf("rec %+v", decoded.Records[0])
	}
}

func TestCreateTopicRequest(t *testing.T) {
	req := CreateTopicRequest{Name: "t", Partitions: 1, Configs: [][2]string{}}
	raw, err := EncodeCreateTopicRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "010074"+"01000000"+"00000000")
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeCreateTopicRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Name != "t" || decoded.Partitions != 1 || len(decoded.Configs) != 0 {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestCreateTopicLegacyWithoutConfigs(t *testing.T) {
	raw := mustHex(t, "010074"+"02000000")
	decoded, err := DecodeCreateTopicRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Name != "t" || decoded.Partitions != 2 || len(decoded.Configs) != 0 {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestCreateTopicResponse(t *testing.T) {
	resp := CreateTopicResponse{TopicID: 1, Name: "t", Partitions: 1, ErrorCode: 0}
	raw, err := EncodeCreateTopicResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "01000000"+"010074"+"01000000"+"0000")
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeCreateTopicResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != resp {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestDeleteTopicRoundtrip(t *testing.T) {
	req := DeleteTopicRequest{Name: "t"}
	raw, err := EncodeDeleteTopicRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, mustHex(t, "010074")) {
		t.Fatalf("req %x", raw)
	}
	decoded, err := DecodeDeleteTopicRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != req {
		t.Fatalf("decoded %+v", decoded)
	}
	resp := DeleteTopicResponse{Name: "t", ErrorCode: 0}
	rraw, err := EncodeDeleteTopicResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rraw, mustHex(t, "0100740000")) {
		t.Fatalf("resp %x", rraw)
	}
	dresp, err := DecodeDeleteTopicResponse(rraw)
	if err != nil {
		t.Fatal(err)
	}
	if dresp != resp {
		t.Fatalf("decoded resp %+v", dresp)
	}
}

func TestMetadataRequestAllTopics(t *testing.T) {
	req := MetadataRequest{Topics: []string{}}
	raw, err := EncodeMetadataRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, mustHex(t, "00000000")) {
		t.Fatalf("raw %x", raw)
	}
	decoded, err := DecodeMetadataRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Topics) != 0 {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestMetadataResponseOneBrokerOnePartition(t *testing.T) {
	resp := MetadataResponse{
		Brokers: []BrokerInfo{{NodeID: 1, Host: "127.0.0.1", Port: 9092}},
		Topics: []TopicInfo{
			{
				Name:      "t",
				TopicID:   1,
				ErrorCode: 0,
				Partitions: []PartitionInfo{
					{
						PartitionID: 0,
						Leader:      1,
						HWM:         0,
						Replicas:    []uint32{1},
						ISR:         []uint32{1},
						LeaderEpoch: 0,
					},
				},
			},
		},
	}
	raw, err := EncodeMetadataResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t,
		"01000000"+ // 1 broker
			"01000000"+ // node 1
			"0900"+ // host len 9
			"3132372e302e302e31"+ // 127.0.0.1
			"8423"+ // port 9092 le
			"01000000"+ // 1 topic
			"010074"+
			"01000000"+ // topic_id
			"0000"+ // error
			"01000000"+ // 1 partition
			"00000000"+ // id 0
			"01000000"+ // leader 1
			"0000000000000000"+ // hwm
			"01000000"+ // 1 replica
			"01000000"+
			"01000000"+ // 1 isr
			"01000000"+
			"00000000", // leader_epoch
	)
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeMetadataResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Brokers[0] != resp.Brokers[0] {
		t.Fatalf("broker %+v", decoded.Brokers[0])
	}
	if decoded.Topics[0].Name != "t" || decoded.Topics[0].Partitions[0].Leader != 1 {
		t.Fatalf("topic %+v", decoded.Topics[0])
	}
}

func TestDecodeResponseDispatch(t *testing.T) {
	raw, err := EncodeProduceResponse(ProduceResponse{Topic: "t", Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeResponse(OpProduce, raw)
	if err != nil {
		t.Fatal(err)
	}
	pr, ok := got.(ProduceResponse)
	if !ok || pr.Topic != "t" || pr.Count != 1 {
		t.Fatalf("got %#v", got)
	}
	_, err = DecodeResponse(0x00AB, nil)
	if err == nil {
		t.Fatal("expected unknown opcode")
	}
}
