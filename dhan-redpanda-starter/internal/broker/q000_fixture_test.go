package broker

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

type capturedRecord struct {
	Topic     string `json:"topic"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

func TestQ000LiveFixturesMatchPublishedContract(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "..", "fixtures", "q000", "live-records.jsonl")
	file, err := os.Open(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	expectedKind := map[string]string{
		"495213": "GOLD future",
		"581902": "GOLDM CE",
		"582003": "GOLDM PE",
	}
	seen := make(map[string]bool, len(expectedKind))
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record capturedRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("decode broker envelope: %v", err)
		}
		if record.Topic != "dhan.market.raw" || record.Partition != 0 {
			t.Fatalf("unexpected source coordinate: %#v", record)
		}

		var event Event
		if err := json.Unmarshal([]byte(record.Value), &event); err != nil {
			t.Fatalf("decode event at offset %d: %v", record.Offset, err)
		}
		kind, ok := expectedKind[event.SecurityID]
		if !ok {
			t.Fatalf("unexpected security ID %q", event.SecurityID)
		}
		seen[kind] = true
		if record.Key != event.ExchangeCode+":"+event.SecurityID {
			t.Fatalf("key %q does not match event identity", record.Key)
		}

		receivedAt, err := time.Parse(time.RFC3339Nano, event.ReceivedAt)
		if err != nil {
			t.Fatalf("parse received_at at offset %d: %v", record.Offset, err)
		}
		if difference := receivedAt.UnixMilli() - record.Timestamp; difference < 0 || difference > 1 {
			t.Fatalf("Kafka timestamp differs from received_at by %dms", difference)
		}

		raw, err := hex.DecodeString(event.RawPacket)
		if err != nil {
			t.Fatalf("decode raw_packet at offset %d: %v", record.Offset, err)
		}
		if len(raw) != 162 || raw[0] != 8 || binary.LittleEndian.Uint16(raw[1:3]) != 162 {
			t.Fatalf("offset %d is not a 162-byte Dhan full packet", record.Offset)
		}
		if raw[3] != 5 {
			t.Fatalf("offset %d exchange segment code = %d, want MCX 5", record.Offset, raw[3])
		}
		securityID := strconv.FormatUint(uint64(binary.LittleEndian.Uint32(raw[4:8])), 10)
		if securityID != event.SecurityID {
			t.Fatalf("raw security ID %s does not match event %s", securityID, event.SecurityID)
		}
		populatedDepthLevels := 0
		for level := 0; level < 5; level++ {
			offset := 62 + level*20
			bidQuantity := binary.LittleEndian.Uint32(raw[offset : offset+4])
			askQuantity := binary.LittleEndian.Uint32(raw[offset+4 : offset+8])
			bidPrice := math.Float32frombits(binary.LittleEndian.Uint32(raw[offset+12 : offset+16]))
			askPrice := math.Float32frombits(binary.LittleEndian.Uint32(raw[offset+16 : offset+20]))
			if bidQuantity > 0 && askQuantity > 0 && bidPrice > 0 && askPrice > 0 {
				populatedDepthLevels++
			}
		}
		if populatedDepthLevels != 5 {
			t.Fatalf("offset %d has %d populated depth levels, want 5", record.Offset, populatedDepthLevels)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(expectedKind) {
		t.Fatal(fmt.Errorf("fixture coverage = %v, want future, CE, and PE", seen))
	}
}
