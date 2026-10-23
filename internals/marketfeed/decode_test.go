package marketfeed

import (
	"encoding/binary"
	"math"
	"testing"
	"time"
)

func TestDecodeFullPacket(t *testing.T) {
	packet := make([]byte, fullPacketSize)
	packet[0] = 8
	binary.LittleEndian.PutUint16(packet[1:3], fullPacketSize)
	packet[3] = 5
	binary.LittleEndian.PutUint32(packet[4:8], 571445)
	putFloat32(packet, 8, 123456.5)
	binary.LittleEndian.PutUint16(packet[12:14], 7)
	binary.LittleEndian.PutUint32(packet[14:18], 1_792_755_000)
	putFloat32(packet, 18, 123400.25)
	binary.LittleEndian.PutUint32(packet[22:26], 9001)
	binary.LittleEndian.PutUint32(packet[26:30], 111)
	binary.LittleEndian.PutUint32(packet[30:34], 222)
	binary.LittleEndian.PutUint32(packet[34:38], 3333)
	putFloat32(packet, 46, 123000)
	putFloat32(packet, 50, 122000)
	putFloat32(packet, 54, 124000)
	putFloat32(packet, 58, 121000)
	for i := 0; i < depthLevelCount; i++ {
		offset := 62 + i*depthLevelSize
		binary.LittleEndian.PutUint32(packet[offset:offset+4], uint32(i+1))
		binary.LittleEndian.PutUint32(packet[offset+4:offset+8], uint32(i+11))
		binary.LittleEndian.PutUint16(packet[offset+8:offset+10], uint16(i+2))
		binary.LittleEndian.PutUint16(packet[offset+10:offset+12], uint16(i+3))
		putFloat32(packet, offset+12, float32(i)+1.5)
		putFloat32(packet, offset+16, float32(i)+2.5)
	}

	lookup := makeInstrumentLookup([]Subscription{{
		ExchangeSegment: "MCX_COMM",
		SecurityID:      "571445",
		Symbol:          "GOLDM NOV FUT",
	}})
	events, err := decode(packet, lookup)
	if err != nil {
		t.Fatalf("decode() error = %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("decoded %d events, want tick, quote, and depth", len(events))
	}

	tick, ok := events[0].(Tick)
	if !ok {
		t.Fatalf("event[0] = %T, want Tick", events[0])
	}
	if tick.Symbol != "GOLDM NOV FUT" || tick.SecurityID != "571445" || tick.LTP != 123456.5 || tick.Volume != 9001 || tick.OpenInterest != 3333 {
		t.Fatalf("tick = %#v", tick)
	}
	if !tick.Timestamp.Equal(time.Unix(1_792_755_000, 0).UTC()) {
		t.Fatalf("tick timestamp = %v", tick.Timestamp)
	}

	quote, ok := events[1].(Quote)
	if !ok || quote.Volume != 9001 || quote.OpenInterest != 3333 || quote.High != 124000 {
		t.Fatalf("quote = %#v", events[1])
	}

	depth, ok := events[2].(Depth)
	if !ok || len(depth.Levels) != 5 || depth.Levels[0].BidQuantity != 1 || depth.Levels[4].AskQuantity != 15 {
		t.Fatalf("depth = %#v", events[2])
	}
}

func TestDecodeConcatenatedPackets(t *testing.T) {
	first := make([]byte, tickerPacketSize)
	first[0] = 2
	binary.LittleEndian.PutUint16(first[1:3], tickerPacketSize)
	first[3] = 5
	binary.LittleEndian.PutUint32(first[4:8], 101)
	putFloat32(first, 8, 10.25)
	binary.LittleEndian.PutUint32(first[12:16], 10)

	second := make([]byte, tickerPacketSize)
	second[0] = 2
	binary.LittleEndian.PutUint16(second[1:3], tickerPacketSize)
	second[3] = 5
	binary.LittleEndian.PutUint32(second[4:8], 102)
	putFloat32(second, 8, 20.5)
	binary.LittleEndian.PutUint32(second[12:16], 20)

	events, err := decode(append(first, second...), nil)
	if err != nil {
		t.Fatalf("decode() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("decoded %d events, want 2", len(events))
	}
	if events[0].(Tick).LTP != 10.25 || events[1].(Tick).LTP != 20.5 {
		t.Fatalf("events = %#v", events)
	}
}

func TestDecodeRejectsInvalidPacketLength(t *testing.T) {
	packet := make([]byte, responseHeaderSize)
	packet[0] = 2
	binary.LittleEndian.PutUint16(packet[1:3], tickerPacketSize)
	if _, err := decode(packet, nil); err == nil {
		t.Fatal("decode() succeeded for an incomplete packet")
	}
}

func putFloat32(data []byte, offset int, value float32) {
	binary.LittleEndian.PutUint32(data[offset:offset+4], math.Float32bits(value))
}
