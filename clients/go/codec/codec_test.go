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

func TestOffsetCommitRequestPayloadRS(t *testing.T) {
	req := OffsetCommitRequest{
		GroupID:    "g1",
		MemberID:   "m1",
		Generation: 2,
		Entries: []OffsetCommitEntry{
			{Topic: "events", Partition: 1, Offset: 42, Metadata: "cli"},
		},
	}
	raw, err := EncodeOffsetCommitRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t,
		"0200"+
			"6731"+
			"0200"+
			"6d31"+
			"02000000"+
			"01000000"+
			"0600"+
			"6576656e7473"+
			"01000000"+
			"2a00000000000000"+
			"0300"+
			"636c69",
	)
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeOffsetCommitRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GroupID != req.GroupID || decoded.MemberID != req.MemberID || decoded.Generation != req.Generation {
		t.Fatalf("decoded header %+v", decoded)
	}
	if len(decoded.Entries) != 1 || decoded.Entries[0] != req.Entries[0] {
		t.Fatalf("entries %+v", decoded.Entries)
	}
}

func TestOffsetCommitRequestAdminShape(t *testing.T) {
	req := OffsetCommitRequest{
		GroupID:    "g",
		MemberID:   "",
		Generation: 0,
		Entries:    []OffsetCommitEntry{{Topic: "t", Partition: 0, Offset: 5, Metadata: ""}},
	}
	raw, err := EncodeOffsetCommitRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "010067"+"0000"+"00000000"+"01000000"+"010074"+"00000000"+"0500000000000000"+"0000")
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeOffsetCommitRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GroupID != "g" || decoded.MemberID != "" || decoded.Generation != 0 {
		t.Fatalf("decoded %+v", decoded)
	}
	if decoded.Entries[0].Topic != "t" || decoded.Entries[0].Offset != 5 {
		t.Fatalf("entry %+v", decoded.Entries[0])
	}
}

func TestOffsetCommitResponse(t *testing.T) {
	resp := OffsetCommitResponse{ErrorCode: 0}
	raw, err := EncodeOffsetCommitResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, mustHex(t, "0000")) {
		t.Fatalf("raw %x", raw)
	}
	decoded, err := DecodeOffsetCommitResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != resp {
		t.Fatalf("decoded %+v", decoded)
	}
	got, err := DecodeResponse(OpOffsetCommit, raw)
	if err != nil {
		t.Fatal(err)
	}
	if cr, ok := got.(OffsetCommitResponse); !ok || cr.ErrorCode != 0 {
		t.Fatalf("dispatch %#v", got)
	}
}

func TestOffsetFetchRequestPayloadRS(t *testing.T) {
	req := OffsetFetchRequest{
		GroupID: "g1",
		Entries: []OffsetEntry{{Topic: "events", Partition: 1}},
	}
	raw, err := EncodeOffsetFetchRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "02006731"+"01000000"+"06006576656e7473"+"01000000")
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeOffsetFetchRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GroupID != "g1" || len(decoded.Entries) != 1 || decoded.Entries[0] != req.Entries[0] {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestOffsetFetchRequestEmptyEntries(t *testing.T) {
	req := OffsetFetchRequest{GroupID: "g1", Entries: []OffsetEntry{}}
	raw, err := EncodeOffsetFetchRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "02006731"+"00000000")
	if !bytes.Equal(raw, expected) {
		t.Fatalf("raw %x", raw)
	}
	decoded, err := DecodeOffsetFetchRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GroupID != "g1" || len(decoded.Entries) != 0 {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestOffsetFetchResponseUnknownOffset(t *testing.T) {
	resp := OffsetFetchResponse{
		ErrorCode: 0,
		Entries: []OffsetFetchEntry{
			{Topic: "events", Partition: 0, Offset: ^uint64(0), Metadata: ""},
		},
	}
	raw, err := EncodeOffsetFetchResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t,
		"0000"+
			"01000000"+
			"06006576656e7473"+
			"00000000"+
			"ffffffffffffffff"+
			"0000",
	)
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeOffsetFetchResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ErrorCode != 0 || len(decoded.Entries) != 1 || decoded.Entries[0].Offset != ^uint64(0) {
		t.Fatalf("decoded %+v", decoded)
	}
	got, err := DecodeResponse(OpOffsetFetch, raw)
	if err != nil {
		t.Fatal(err)
	}
	if fr, ok := got.(OffsetFetchResponse); !ok || fr.Entries[0].Topic != "events" {
		t.Fatalf("dispatch %#v", got)
	}
}

func TestOffsetFetchResponseCommitted(t *testing.T) {
	resp := OffsetFetchResponse{
		ErrorCode: 0,
		Entries:   []OffsetFetchEntry{{Topic: "t", Partition: 0, Offset: 5, Metadata: ""}},
	}
	raw, err := EncodeOffsetFetchResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "0000"+"01000000"+"010074"+"00000000"+"0500000000000000"+"0000")
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeOffsetFetchResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Entries[0] != resp.Entries[0] {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestJoinGroupRequestPayloadRS(t *testing.T) {
	req := JoinGroupRequest{
		GroupID:          "g1",
		MemberID:         "",
		SessionTimeoutMs: 10_000,
		Topics:           []string{"events", "logs"},
		GroupInstanceID:  "",
	}
	raw, err := EncodeJoinGroupRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t,
		"0200"+
			"6731"+
			"0000"+
			"10270000"+
			"02000000"+
			"0600"+
			"6576656e7473"+
			"0400"+
			"6c6f6773"+
			"0000",
	)
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeJoinGroupRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GroupID != req.GroupID || decoded.MemberID != "" || decoded.SessionTimeoutMs != 10_000 {
		t.Fatalf("decoded header %+v", decoded)
	}
	if len(decoded.Topics) != 2 || decoded.Topics[0] != "events" || decoded.Topics[1] != "logs" {
		t.Fatalf("topics %+v", decoded.Topics)
	}
	if decoded.GroupInstanceID != "" {
		t.Fatalf("instance %q", decoded.GroupInstanceID)
	}
}

func TestJoinGroupRequestWithInstance(t *testing.T) {
	req := JoinGroupRequest{
		GroupID:          "g1",
		MemberID:         "",
		SessionTimeoutMs: 10_000,
		Topics:           []string{"events"},
		GroupInstanceID:  "pod-1",
	}
	raw, err := EncodeJoinGroupRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "02006731"+"0000"+"10270000"+"01000000"+"06006576656e7473"+"0500706f642d31")
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeJoinGroupRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GroupInstanceID != "pod-1" || len(decoded.Topics) != 1 || decoded.Topics[0] != "events" {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestJoinGroupRequestLegacyWithoutInstance(t *testing.T) {
	raw := mustHex(t, "02006731"+"02006d31"+"88130000"+"01000000"+"010074")
	decoded, err := DecodeJoinGroupRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GroupID != "g1" || decoded.MemberID != "m1" || decoded.SessionTimeoutMs != 5000 {
		t.Fatalf("decoded %+v", decoded)
	}
	if len(decoded.Topics) != 1 || decoded.Topics[0] != "t" || decoded.GroupInstanceID != "" {
		t.Fatalf("topics/instance %+v", decoded)
	}
}

func TestJoinGroupResponsePayloadRS(t *testing.T) {
	resp := JoinGroupResponse{
		ErrorCode:  0,
		Generation: 1,
		MemberID:   "uuid-1",
		Assignment: []Assignment{
			{Topic: "events", Partition: 0},
			{Topic: "events", Partition: 1},
		},
		Revoked: []Assignment{{Topic: "events", Partition: 2}},
	}
	raw, err := EncodeJoinGroupResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t,
		"0000"+
			"01000000"+
			"0600"+
			"757569642d31"+
			"02000000"+
			"06006576656e7473"+
			"00000000"+
			"06006576656e7473"+
			"01000000"+
			"01000000"+
			"06006576656e7473"+
			"02000000",
	)
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeJoinGroupResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.MemberID != "uuid-1" || decoded.Generation != 1 || len(decoded.Assignment) != 2 {
		t.Fatalf("decoded %+v", decoded)
	}
	if decoded.Assignment[1].Partition != 1 || decoded.Revoked[0].Partition != 2 {
		t.Fatalf("assignment/revoked %+v", decoded)
	}
	got, err := DecodeResponse(OpJoinGroup, raw)
	if err != nil {
		t.Fatal(err)
	}
	if jr, ok := got.(JoinGroupResponse); !ok || jr.MemberID != "uuid-1" {
		t.Fatalf("dispatch %#v", got)
	}
}

func TestJoinGroupResponseLegacyWithoutRevoked(t *testing.T) {
	raw := mustHex(t, "0000"+"01000000"+"0600757569642d31"+"01000000"+"06006576656e7473"+"00000000")
	decoded, err := DecodeJoinGroupResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.MemberID != "uuid-1" || decoded.Generation != 1 || len(decoded.Assignment) != 1 {
		t.Fatalf("decoded %+v", decoded)
	}
	if decoded.Assignment[0] != (Assignment{Topic: "events", Partition: 0}) {
		t.Fatalf("assignment %+v", decoded.Assignment)
	}
	if len(decoded.Revoked) != 0 {
		t.Fatalf("revoked %+v", decoded.Revoked)
	}
}

func TestHeartbeatRequestPayloadRS(t *testing.T) {
	req := HeartbeatRequest{GroupID: "g1", MemberID: "m1", Generation: 3}
	raw, err := EncodeHeartbeatRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "02006731"+"02006d31"+"03000000")
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeHeartbeatRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != req {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestHeartbeatResponseRebalance(t *testing.T) {
	resp := HeartbeatResponse{ErrorCode: 9}
	raw, err := EncodeHeartbeatResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, mustHex(t, "0900")) {
		t.Fatalf("raw %x", raw)
	}
	decoded, err := DecodeHeartbeatResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != resp {
		t.Fatalf("decoded %+v", decoded)
	}
	got, err := DecodeResponse(OpHeartbeat, raw)
	if err != nil {
		t.Fatal(err)
	}
	if hr, ok := got.(HeartbeatResponse); !ok || hr.ErrorCode != 9 {
		t.Fatalf("dispatch %#v", got)
	}
}

func TestLeaveGroupRequestPayloadRS(t *testing.T) {
	req := LeaveGroupRequest{GroupID: "g1", MemberID: "m1"}
	raw, err := EncodeLeaveGroupRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "02006731"+"02006d31")
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeLeaveGroupRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != req {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestLeaveGroupResponse(t *testing.T) {
	resp := LeaveGroupResponse{ErrorCode: 0}
	raw, err := EncodeLeaveGroupResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, mustHex(t, "0000")) {
		t.Fatalf("raw %x", raw)
	}
	decoded, err := DecodeLeaveGroupResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != resp {
		t.Fatalf("decoded %+v", decoded)
	}
	got, err := DecodeResponse(OpLeaveGroup, raw)
	if err != nil {
		t.Fatal(err)
	}
	if lr, ok := got.(LeaveGroupResponse); !ok || lr.ErrorCode != 0 {
		t.Fatalf("dispatch %#v", got)
	}
}

func TestAuthRequestS3cret(t *testing.T) {
	req := AuthRequest{Token: "s3cret"}
	raw, err := EncodeAuthRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "0600733363726574")
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeAuthRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != req {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestAuthResponseOkAndFailed(t *testing.T) {
	okRaw, err := EncodeAuthResponse(AuthResponse{ErrorCode: 0})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(okRaw, mustHex(t, "0000")) {
		t.Fatalf("ok raw %x", okRaw)
	}
	ok, err := DecodeAuthResponse(okRaw)
	if err != nil {
		t.Fatal(err)
	}
	if ok.ErrorCode != 0 {
		t.Fatalf("ok %+v", ok)
	}
	got, err := DecodeResponse(OpAuthResponse, okRaw)
	if err != nil {
		t.Fatal(err)
	}
	if ar, ok := got.(AuthResponse); !ok || ar.ErrorCode != 0 {
		t.Fatalf("dispatch ok %#v", got)
	}

	failRaw, err := EncodeAuthResponse(AuthResponse{ErrorCode: 17})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(failRaw, mustHex(t, "1100")) {
		t.Fatalf("fail raw %x", failRaw)
	}
	fail, err := DecodeAuthResponse(failRaw)
	if err != nil {
		t.Fatal(err)
	}
	if fail.ErrorCode != 17 {
		t.Fatalf("fail %+v", fail)
	}
	got, err = DecodeResponse(OpAuthResponse, failRaw)
	if err != nil {
		t.Fatal(err)
	}
	if ar, ok := got.(AuthResponse); !ok || ar.ErrorCode != 17 {
		t.Fatalf("dispatch fail %#v", got)
	}
}

func TestScramFirstRequest(t *testing.T) {
	req := ScramFirstRequest{Username: "alice", ClientNonce: "n1"}
	raw, err := EncodeScramFirstRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "0500616c69636502006e31")
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeScramFirstRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != req {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestScramFirstResponse(t *testing.T) {
	resp := ScramFirstResponse{
		ErrorCode:     0,
		CombinedNonce: "n1s1",
		Salt:          []byte{1, 2, 3},
		Iterations:    4096,
	}
	raw, err := EncodeScramFirstResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "000004006e3173310300000001020300100000")
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeScramFirstResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ErrorCode != resp.ErrorCode || decoded.CombinedNonce != resp.CombinedNonce || decoded.Iterations != resp.Iterations || !bytes.Equal(decoded.Salt, resp.Salt) {
		t.Fatalf("decoded %+v", decoded)
	}
	got, err := DecodeResponse(OpScramFirstResponse, raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.(ScramFirstResponse); !ok {
		t.Fatalf("dispatch %#v", got)
	}
}

func TestScramFinalRequest(t *testing.T) {
	req := ScramFinalRequest{
		Username:      "alice",
		CombinedNonce: "n1s1",
		ClientProof:   make([]byte, 32),
	}
	raw, err := EncodeScramFinalRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "0500616c69636504006e31733120000000"+hexZeros(32))
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeScramFinalRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Username != req.Username || decoded.CombinedNonce != req.CombinedNonce || !bytes.Equal(decoded.ClientProof, req.ClientProof) {
		t.Fatalf("decoded %+v", decoded)
	}
}

func TestScramFinalResponse(t *testing.T) {
	sig := bytes.Repeat([]byte{9}, 32)
	resp := ScramFinalResponse{ErrorCode: 0, ServerSignature: sig}
	raw, err := EncodeScramFinalResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustHex(t, "000020000000"+hexNines(32))
	if !bytes.Equal(raw, expected) {
		t.Fatalf("encode:\n got %x\nwant %x", raw, expected)
	}
	decoded, err := DecodeScramFinalResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ErrorCode != 0 || !bytes.Equal(decoded.ServerSignature, sig) {
		t.Fatalf("decoded %+v", decoded)
	}
	got, err := DecodeResponse(OpScramFinalResponse, raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.(ScramFinalResponse); !ok {
		t.Fatalf("dispatch %#v", got)
	}
}

func hexZeros(n int) string {
	out := make([]byte, n*2)
	for i := range out {
		out[i] = '0'
	}
	return string(out)
}

func hexNines(n int) string {
	out := make([]byte, 0, n*2)
	for i := 0; i < n; i++ {
		out = append(out, '0', '9')
	}
	return string(out)
}
