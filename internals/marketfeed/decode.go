package marketfeed

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"
)

const (
	responseHeaderSize = 8
	tickerPacketSize   = 16
	quotePacketSize    = 50
	fullPacketSize     = 162
	depthLevelSize     = 20
	depthLevelCount    = 5
)

func decode(data []byte, lookup map[string]instrument) ([]Event, error) {
	var events []Event
	for offset := 0; offset < len(data); {
		if len(data)-offset < responseHeaderSize {
			return nil, fmt.Errorf("short Dhan market-feed header: got %d bytes", len(data)-offset)
		}

		messageLength := int(binary.LittleEndian.Uint16(data[offset+1 : offset+3]))
		if messageLength < responseHeaderSize || messageLength > len(data)-offset {
			return nil, fmt.Errorf("invalid Dhan market-feed message length %d", messageLength)
		}
		packet := data[offset : offset+messageLength]
		packetEvents, err := decodePacket(packet, lookup)
		if err != nil {
			return nil, err
		}
		events = append(events, packetEvents...)
		offset += messageLength
	}
	return events, nil
}

func decodePacket(packet []byte, lookup map[string]instrument) ([]Event, error) {
	code := packet[0]
	segmentCode := packet[3]
	securityID := binary.LittleEndian.Uint32(packet[4:8])
	key := fmt.Sprintf("%d:%d", segmentCode, securityID)
	metadata := lookup[key]
	if metadata.SecurityID == "" {
		metadata = instrument{
			ExchangeSegment: exchangeSegment(segmentCode),
			SecurityID:      strconv.FormatUint(uint64(securityID), 10),
		}
	}

	switch code {
	case 2:
		if len(packet) < tickerPacketSize {
			return nil, fmt.Errorf("short Dhan ticker packet: got %d bytes", len(packet))
		}
		return []Event{Tick{
			ExchangeSegment: metadata.ExchangeSegment,
			SecurityID:      metadata.SecurityID,
			Symbol:          metadata.Symbol,
			LTP:             float32Value(packet, 8),
			Timestamp:       epochTime(binary.LittleEndian.Uint32(packet[12:16])),
		}}, nil
	case 4:
		quote, err := decodeQuote(packet, metadata)
		if err != nil {
			return nil, err
		}
		return []Event{quote}, nil
	case 8:
		return decodeFull(packet, metadata)
	case 50:
		if len(packet) < 10 {
			return nil, fmt.Errorf("short Dhan disconnect packet: got %d bytes", len(packet))
		}
		code := binary.LittleEndian.Uint16(packet[8:10])
		return nil, fmt.Errorf("Dhan market feed disconnected with code %d", code)
	default:
		// Prev-close, OI, status, and index packets are supplementary to the
		// FULL packet used by this reader and do not need separate events.
		return nil, nil
	}
}

func decodeQuote(packet []byte, metadata instrument) (Quote, error) {
	if len(packet) < quotePacketSize {
		return Quote{}, fmt.Errorf("short Dhan quote packet: got %d bytes", len(packet))
	}
	return Quote{
		ExchangeSegment: metadata.ExchangeSegment,
		SecurityID:      metadata.SecurityID,
		Symbol:          metadata.Symbol,
		LTP:             float32Value(packet, 8),
		LastTradeQty:    binary.LittleEndian.Uint16(packet[12:14]),
		Timestamp:       epochTime(binary.LittleEndian.Uint32(packet[14:18])),
		AveragePrice:    float32Value(packet, 18),
		Volume:          binary.LittleEndian.Uint32(packet[22:26]),
		TotalSellQty:    binary.LittleEndian.Uint32(packet[26:30]),
		TotalBuyQty:     binary.LittleEndian.Uint32(packet[30:34]),
		Open:            float32Value(packet, 34),
		Close:           float32Value(packet, 38),
		High:            float32Value(packet, 42),
		Low:             float32Value(packet, 46),
	}, nil
}

func decodeFull(packet []byte, metadata instrument) ([]Event, error) {
	if len(packet) < fullPacketSize {
		return nil, fmt.Errorf("short Dhan full packet: got %d bytes", len(packet))
	}

	timestamp := epochTime(binary.LittleEndian.Uint32(packet[14:18]))
	ltp := float32Value(packet, 8)
	volume := binary.LittleEndian.Uint32(packet[22:26])
	oi := binary.LittleEndian.Uint32(packet[34:38])
	quote := Quote{
		ExchangeSegment: metadata.ExchangeSegment,
		SecurityID:      metadata.SecurityID,
		Symbol:          metadata.Symbol,
		LTP:             ltp,
		LastTradeQty:    binary.LittleEndian.Uint16(packet[12:14]),
		Timestamp:       timestamp,
		AveragePrice:    float32Value(packet, 18),
		Volume:          volume,
		TotalSellQty:    binary.LittleEndian.Uint32(packet[26:30]),
		TotalBuyQty:     binary.LittleEndian.Uint32(packet[30:34]),
		OpenInterest:    oi,
		Open:            float32Value(packet, 46),
		Close:           float32Value(packet, 50),
		High:            float32Value(packet, 54),
		Low:             float32Value(packet, 58),
	}

	depth := Depth{
		ExchangeSegment: metadata.ExchangeSegment,
		SecurityID:      metadata.SecurityID,
		Symbol:          metadata.Symbol,
		LTP:             ltp,
		Levels:          make([]DepthLevel, depthLevelCount),
	}
	for i := range depth.Levels {
		offset := 62 + i*depthLevelSize
		depth.Levels[i] = DepthLevel{
			BidQuantity: binary.LittleEndian.Uint32(packet[offset : offset+4]),
			AskQuantity: binary.LittleEndian.Uint32(packet[offset+4 : offset+8]),
			BidOrders:   binary.LittleEndian.Uint16(packet[offset+8 : offset+10]),
			AskOrders:   binary.LittleEndian.Uint16(packet[offset+10 : offset+12]),
			BidPrice:    float32Value(packet, offset+12),
			AskPrice:    float32Value(packet, offset+16),
		}
	}

	return []Event{
		Tick{
			ExchangeSegment: metadata.ExchangeSegment,
			SecurityID:      metadata.SecurityID,
			Symbol:          metadata.Symbol,
			LTP:             ltp,
			Volume:          volume,
			OpenInterest:    oi,
			Timestamp:       timestamp,
		},
		quote,
		depth,
	}, nil
}

func float32Value(data []byte, offset int) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(data[offset : offset+4]))
}

func epochTime(seconds uint32) time.Time {
	return time.Unix(int64(seconds), 0).UTC()
}

func parseSecurityID(value string) uint32 {
	parsed, _ := strconv.ParseUint(value, 10, 32)
	return uint32(parsed)
}

func exchangeCode(segment string) byte {
	switch segment {
	case "IDX_I":
		return 0
	case "NSE_EQ":
		return 1
	case "NSE_FNO":
		return 2
	case "NSE_CURRENCY":
		return 3
	case "BSE_EQ":
		return 4
	case "MCX_COMM":
		return 5
	case "BSE_CURRENCY":
		return 7
	case "BSE_FNO":
		return 8
	default:
		return 255
	}
}

func exchangeSegment(code byte) string {
	switch code {
	case 0:
		return "IDX_I"
	case 1:
		return "NSE_EQ"
	case 2:
		return "NSE_FNO"
	case 3:
		return "NSE_CURRENCY"
	case 4:
		return "BSE_EQ"
	case 5:
		return "MCX_COMM"
	case 7:
		return "BSE_CURRENCY"
	case 8:
		return "BSE_FNO"
	default:
		return fmt.Sprintf("UNKNOWN_%d", code)
	}
}
