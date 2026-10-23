package dhan_ops

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"yswa-var/dhan_go_bus/internals"
)

const (
	marketFeedLTPURL = "https://api.dhan.co/v2/marketfeed/ltp"
	quoteInterval    = 1200 * time.Millisecond
	quoteRetryDelay  = time.Second
	maxQuoteAttempts = 3
)

var quoteRateLimiter struct {
	sync.Mutex
	last time.Time
}

// GetQuotePrice returns the latest traded price for one Dhan instrument.
// exchangeSegment must be a Dhan exchange segment such as NSE_EQ, NSE_FNO,
// or MCX_COMM; instrumentCode is the security ID from Dhan's contract master.
func GetQuotePrice(exchangeSegment, instrumentCode string) (float64, error) {
	config, err := internals.NewConfig()
	if err != nil {
		return 0, fmt.Errorf("create Dhan config: %w", err)
	}

	token, err := config.GetAuthToken()
	if err != nil {
		return 0, fmt.Errorf("get Dhan auth token: %w", err)
	}

	for attempt := 0; attempt < maxQuoteAttempts; attempt++ {
		waitForQuoteRateLimit()
		price, err := getQuotePrice(
			http.DefaultClient,
			config.ClientID(),
			token,
			marketFeedLTPURL,
			exchangeSegment,
			instrumentCode,
		)
		if err == nil {
			return price, nil
		}

		var rateLimitErr *quoteRateLimitError
		if !errors.As(err, &rateLimitErr) || attempt == maxQuoteAttempts-1 {
			return 0, err
		}
		time.Sleep(quoteRetryDelay)
	}
	return 0, errors.New("quote request attempts exhausted")
}

func waitForQuoteRateLimit() {
	quoteRateLimiter.Lock()
	defer quoteRateLimiter.Unlock()

	if !quoteRateLimiter.last.IsZero() {
		if wait := quoteInterval - time.Since(quoteRateLimiter.last); wait > 0 {
			time.Sleep(wait)
		}
	}
	quoteRateLimiter.last = time.Now()
}

type ltpResponse struct {
	Status  string `json:"status"`
	Remarks any    `json:"remarks"`
	Data    map[string]map[string]struct {
		LastPrice float64 `json:"last_price"`
	} `json:"data"`
}

type quoteRateLimitError struct {
	message string
}

func (e *quoteRateLimitError) Error() string {
	return e.message
}

func getQuotePrice(client *http.Client, clientID, token, endpoint, exchangeSegment, instrumentCode string) (float64, error) {
	segment := strings.ToUpper(strings.TrimSpace(exchangeSegment))
	if segment == "" {
		return 0, errors.New("exchange segment is required")
	}

	securityID := strings.TrimSpace(instrumentCode)
	if securityID == "" {
		return 0, errors.New("instrument code is required")
	}
	securityIDNumber, err := strconv.ParseInt(securityID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid instrument code %q: %w", instrumentCode, err)
	}

	body, err := json.Marshal(map[string][]int64{segment: {securityIDNumber}})
	if err != nil {
		return 0, fmt.Errorf("encode quote request: %w", err)
	}

	request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("create quote request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("access-token", token)
	request.Header.Set("client-id", clientID)

	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, fmt.Errorf("request quote: %w", err)
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, fmt.Errorf("read quote response: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		message := fmt.Sprintf("Dhan quote request failed (%s): %s", response.Status, strings.TrimSpace(string(responseBody)))
		if response.StatusCode == http.StatusTooManyRequests {
			return 0, &quoteRateLimitError{message: message}
		}
		return 0, errors.New(message)
	}

	var result ltpResponse
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return 0, fmt.Errorf("decode quote response: %w", err)
	}
	if result.Status != "success" {
		return 0, fmt.Errorf("Dhan quote request failed: %v", result.Remarks)
	}

	segmentData, ok := result.Data[segment]
	if !ok {
		return 0, fmt.Errorf("quote response did not contain exchange segment %q", segment)
	}
	quote, ok := segmentData[securityID]
	if !ok {
		return 0, fmt.Errorf("quote response did not contain instrument %q", securityID)
	}
	return quote.LastPrice, nil
}
