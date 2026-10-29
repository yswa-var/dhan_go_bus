package broker

import (
	"encoding/json"
	"testing"
)

func TestEventKeepsRawPacketAndStableKeyFields(t *testing.T) {
	event := Event{ExchangeCode: "MCX", SecurityID: "100001", RawPacket: "010203"}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]string
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["raw_packet"] != event.RawPacket {
		t.Fatalf("raw packet = %q, want %q", decoded["raw_packet"], event.RawPacket)
	}
	if event.ExchangeCode+":"+event.SecurityID != "MCX:100001" {
		t.Fatal("event key fields changed")
	}
}
