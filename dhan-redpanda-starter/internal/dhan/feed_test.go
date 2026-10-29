package dhan_ops

import (
	"encoding/binary"
	"math"
	"testing"
	"time"
)

func TestDecodeTickerKeepsRawPacketAndMetadata(t *testing.T) {
	packet := make([]byte, tickerSize)
	packet[0] = 2
	binary.LittleEndian.PutUint16(packet[1:3], tickerSize)
	packet[3] = exchangeCode("MCX_COMM")
	binary.LittleEndian.PutUint32(packet[4:8], 12345)
	binary.LittleEndian.PutUint32(packet[8:12], math.Float32bits(71234.5))
	binary.LittleEndian.PutUint32(packet[12:16], 1_700_000_000)

	events, err := decodeFeedMessage(packet, makeInstrumentLookup([]Subscription{{ExchangeSegment: "MCX_COMM", SecurityID: "12345"}}))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].PacketType != PacketTicker {
		t.Fatalf("events = %#v", events)
	}
	if events[0].ExchangeSegment != "MCX_COMM" || events[0].SecurityID != "12345" {
		t.Fatalf("metadata = %s:%s", events[0].ExchangeSegment, events[0].SecurityID)
	}
	if events[0].Tick.LTP != 71234.5 || !events[0].ReceivedAt.Equal(time.Unix(1_700_000_000, 0).UTC()) {
		t.Fatalf("tick = %#v", events[0].Tick)
	}
	if string(events[0].RawPacket) != string(packet) {
		t.Fatal("raw packet was not preserved")
	}
}

func TestDecodeFullEmitsTypedViews(t *testing.T) {
	packet := make([]byte, fullSize)
	packet[0] = 8
	binary.LittleEndian.PutUint16(packet[1:3], fullSize)
	packet[3] = exchangeCode("NSE_FNO")
	binary.LittleEndian.PutUint32(packet[4:8], 49081)
	binary.LittleEndian.PutUint32(packet[8:12], math.Float32bits(368.15))
	binary.LittleEndian.PutUint32(packet[14:18], 1_700_000_000)
	binary.LittleEndian.PutUint32(packet[22:26], 1000)
	binary.LittleEndian.PutUint32(packet[34:38], 2000)

	events, err := decodeFeedMessage(packet, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
	if events[0].Tick.Volume != 1000 || events[0].Tick.OpenInterest != 2000 {
		t.Fatalf("tick = %#v", events[0].Tick)
	}
	if events[0].Quote == nil || len(events[0].Depth.Levels) != depthLevels {
		t.Fatalf("full event = %#v", events[0])
	}
}

func TestValidateSubscriptions(t *testing.T) {
	valid := []Subscription{{ExchangeSegment: "NSE_EQ", SecurityID: "2885"}}
	if err := validateSubscriptions(valid); err != nil {
		t.Fatal(err)
	}
	if err := validateSubscriptions([]Subscription{{ExchangeSegment: "NOPE", SecurityID: "1"}}); err == nil {
		t.Fatal("expected unsupported segment error")
	}
	if err := validateSubscriptions([]Subscription{{ExchangeSegment: "NSE_EQ", SecurityID: "0"}}); err == nil {
		t.Fatal("expected invalid security ID error")
	}
}

func TestEndpointAddsDhanV2Parameters(t *testing.T) {
	reader, err := NewFeedReader(FeedConfig{ClientID: "client", AccessToken: "token"})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := reader.endpoint()
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "wss://api-feed.dhan.co?authType=2&clientId=client&token=token&version=2" {
		t.Fatalf("endpoint = %s", endpoint)
	}
}
