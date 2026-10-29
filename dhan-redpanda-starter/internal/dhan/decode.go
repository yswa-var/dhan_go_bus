package dhan_ops

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"
)

const (
	feedHeaderSize = 8
	tickerSize     = 16
	quoteSize      = 50
	fullSize       = 162
	depthLevelSize = 20
	depthLevels    = 5
)

type instrument struct {
	ExchangeSegment string
	SecurityID      string
}

type Tick struct {
	LTP          float32
	Volume       uint32
	OpenInterest uint32
	Timestamp    time.Time
}

type Quote struct {
	LTP               float32
	LastTradeQuantity uint16
	Timestamp         time.Time
	AveragePrice      float32
	Volume            uint32
	TotalSellQuantity uint32
	TotalBuyQuantity  uint32
	Open              float32
	Close             float32
	High              float32
	Low               float32
}

type DepthLevel struct {
	BidQuantity uint32
	AskQuantity uint32
	BidOrders   uint16
	AskOrders   uint16
	BidPrice    float32
	AskPrice    float32
}

type Depth struct {
	LTP       float32
	Timestamp time.Time
	Levels    []DepthLevel
}

func decodeFeedMessage(data []byte, lookup map[string]instrument) ([]FeedEvent, error) {
	var events []FeedEvent
	for offset := 0; offset < len(data); {
		if len(data)-offset < feedHeaderSize {
			return nil, fmt.Errorf("short Dhan feed header: %d bytes", len(data)-offset)
		}
		length := int(binary.LittleEndian.Uint16(data[offset+1 : offset+3]))
		if length < feedHeaderSize || length > len(data)-offset {
			return nil, fmt.Errorf("invalid Dhan feed packet length %d", length)
		}
		packet := append([]byte(nil), data[offset:offset+length]...)
		decoded, err := decodePacket(packet, lookup)
		if err != nil {
			return nil, err
		}
		events = append(events, decoded...)
		offset += length
	}
	return events, nil
}

func decodePacket(packet []byte, lookup map[string]instrument) ([]FeedEvent, error) {
	if len(packet) < feedHeaderSize {
		return nil, fmt.Errorf("short Dhan feed packet: %d bytes", len(packet))
	}
	packetCode := packet[0]
	segmentCode := packet[3]
	securityIDNumber := binary.LittleEndian.Uint32(packet[4:8])
	key := fmt.Sprintf("%d:%d", segmentCode, securityIDNumber)
	metadata, ok := lookup[key]
	if !ok {
		metadata = instrument{
			ExchangeSegment: exchangeSegment(segmentCode),
			SecurityID:      strconv.FormatUint(uint64(securityIDNumber), 10),
		}
	}

	switch packetCode {
	case 2:
		if len(packet) < tickerSize {
			return nil, fmt.Errorf("short Dhan ticker packet: %d bytes", len(packet))
		}
		tick := Tick{LTP: float32Value(packet, 8), Timestamp: epochTime(binary.LittleEndian.Uint32(packet[12:16]))}
		return []FeedEvent{{PacketType: PacketTicker, ExchangeSegment: metadata.ExchangeSegment, SecurityID: metadata.SecurityID, ReceivedAt: tick.Timestamp, RawPacket: packet, Tick: &tick}}, nil
	case 4:
		quote, err := decodeQuote(packet)
		if err != nil {
			return nil, err
		}
		return []FeedEvent{{PacketType: PacketQuote, ExchangeSegment: metadata.ExchangeSegment, SecurityID: metadata.SecurityID, ReceivedAt: quote.Timestamp, RawPacket: packet, Quote: &quote}}, nil
	case 8:
		return decodeFull(packet, metadata, packet)
	case 50:
		if len(packet) < 10 {
			return nil, fmt.Errorf("short Dhan disconnect packet: %d bytes", len(packet))
		}
		return nil, fmt.Errorf("Dhan market feed disconnected with code %d", binary.LittleEndian.Uint16(packet[8:10]))
	default:
		return nil, nil
	}
}

func decodeQuote(packet []byte) (Quote, error) {
	if len(packet) < quoteSize {
		return Quote{}, fmt.Errorf("short Dhan quote packet: %d bytes", len(packet))
	}
	return Quote{
		LTP:               float32Value(packet, 8),
		LastTradeQuantity: binary.LittleEndian.Uint16(packet[12:14]),
		Timestamp:         epochTime(binary.LittleEndian.Uint32(packet[14:18])),
		AveragePrice:      float32Value(packet, 18),
		Volume:            binary.LittleEndian.Uint32(packet[22:26]),
		TotalSellQuantity: binary.LittleEndian.Uint32(packet[26:30]),
		TotalBuyQuantity:  binary.LittleEndian.Uint32(packet[30:34]),
		Open:              float32Value(packet, 34),
		Close:             float32Value(packet, 38),
		High:              float32Value(packet, 42),
		Low:               float32Value(packet, 46),
	}, nil
}

func decodeFull(packet []byte, metadata instrument, raw []byte) ([]FeedEvent, error) {
	if len(packet) < fullSize {
		return nil, fmt.Errorf("short Dhan full packet: %d bytes", len(packet))
	}
	timestamp := epochTime(binary.LittleEndian.Uint32(packet[14:18]))
	ltp := float32Value(packet, 8)
	quote := Quote{
		LTP:               ltp,
		LastTradeQuantity: binary.LittleEndian.Uint16(packet[12:14]),
		Timestamp:         timestamp,
		AveragePrice:      float32Value(packet, 18),
		Volume:            binary.LittleEndian.Uint32(packet[22:26]),
		TotalSellQuantity: binary.LittleEndian.Uint32(packet[26:30]),
		TotalBuyQuantity:  binary.LittleEndian.Uint32(packet[30:34]),
		Open:              float32Value(packet, 46),
		Close:             float32Value(packet, 50),
		High:              float32Value(packet, 54),
		Low:               float32Value(packet, 58),
	}
	depth := Depth{LTP: ltp, Timestamp: timestamp, Levels: make([]DepthLevel, depthLevels)}
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
	tick := Tick{LTP: ltp, Volume: quote.Volume, OpenInterest: binary.LittleEndian.Uint32(packet[34:38]), Timestamp: timestamp}
	return []FeedEvent{{PacketType: PacketFull, ExchangeSegment: metadata.ExchangeSegment, SecurityID: metadata.SecurityID, ReceivedAt: timestamp, RawPacket: raw, Tick: &tick, Quote: &quote, Depth: &depth}}, nil
}

func float32Value(data []byte, offset int) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(data[offset : offset+4]))
}
func epochTime(seconds uint32) time.Time { return time.Unix(int64(seconds), 0).UTC() }

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
